package crdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

var (
	sampleString = `{
	"primary_key": "[\"1a7ff641-3e3b-47ee-94fe-a0cadb56cd8f\", 2]", // stringifed JSON array
	"row": "{\"after\": {\"k\": \"1a7ff641-3e3b-47ee-94fe-a0cadb56cd8f\", \"v\": 2}, \"updated\": \"1637953249519902405.0000000000\"}", // stringified JSON object
	"table": "strm_2"
}`
)

func crdbChangefeedInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Integration").
		Summary(fmt.Sprintf("Listens to a CockroachDB Core Changefeed and creates a message for each row received. Each message is a json object looking like: \n```json\n%s\n```", sampleString)).
		Description("Will continue to listen to the change feed until shutdown. If provided, this input will maintain the previous `timestamp` so that in the event of a restart the change feed will resume from where it last processed. This is at-least-once processing, as there is a chance that a timestamp is not successfully stored after being emitted and therefore a row may be sent again on restart.\n\nNote: You must have `SET CLUSTER SETTING kv.rangefeed.enabled = true;` on your CRDB cluster.").
		Field(service.NewStringField("dsn").
			Description(`A Data Source Name to identify the target database.`).
			Example("postgresql://user:password@example.com:26257/defaultdb?sslmode=require")).
		Field(service.NewTLSField("tls")).
		Field(service.NewStringListField("tables").
			Description("CSV of tables to be included in the changefeed").
			Example([]string{"table1", "table2"})).
		Field(service.NewStringListField("options").
			Description("A list of options to be included in the changefeed (WITH X, Y...).\n**NOTE: The CURSOR option here will only be applied if a cached cursor value is not found. If the cursor is cached, it will overwrite this option.**").
			Example([]string{"UPDATE", "CURSOR=1536242855577149065.0000000000"}).
			Optional())
}

type crdbChangefeedInput struct {
	dsn       string
	statement string
	pgConfig  *pgxpool.Config
	pgPool    *pgxpool.Pool
	rows      pgx.Rows
	dbMut     sync.Mutex

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

func newCRDBChangefeedInputFromConfig(conf *service.ParsedConfig, logger *service.Logger) (*crdbChangefeedInput, error) {
	c := &crdbChangefeedInput{
		logger:  logger,
		shutSig: shutdown.NewSignaller(),
	}

	var err error
	if c.dsn, err = conf.FieldString("dsn"); err != nil {
		return nil, err
	}
	if c.pgConfig, err = pgxpool.ParseConfig(c.dsn); err != nil {
		return nil, err
	}

	if c.pgConfig.ConnConfig.TLSConfig, err = conf.FieldTLS("tls"); err != nil {
		return nil, err
	}

	// Setup the query
	tables, err := conf.FieldStringList("tables")
	if err != nil {
		return nil, err
	}

	options, err := conf.FieldStringList("options")
	if err != nil {
		return nil, err
	}

	changeFeedOptions := ""
	if len(options) > 0 {
		changeFeedOptions = fmt.Sprintf(" WITH %s", strings.Join(options, ", "))
	}

	c.statement = fmt.Sprintf("EXPERIMENTAL CHANGEFEED FOR %s%s", strings.Join(tables, ", "), changeFeedOptions)
	logger.Debug("Creating changefeed: " + c.statement)

	go func() {
		<-c.shutSig.CloseNowChan()

		c.dbMut.Lock()
		if c.rows != nil {
			c.rows.Close()
			c.rows = nil
		}
		if c.pgPool != nil {
			c.pgPool.Close()
		}
		c.dbMut.Unlock()
		c.shutSig.ShutdownComplete()
	}()
	return c, nil
}

func init() {
	err := service.RegisterInput(
		"cockroachdb_changefeed", crdbChangefeedInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := newCRDBChangefeedInputFromConfig(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacks(i), nil
		})

	if err != nil {
		panic(err)
	}
}

func (c *crdbChangefeedInput) Connect(ctx context.Context) (err error) {
	c.dbMut.Lock()
	defer c.dbMut.Unlock()

	if c.rows != nil {
		return
	}

	if c.pgPool == nil {
		if c.pgPool, err = pgxpool.ConnectConfig(ctx, c.pgConfig); err != nil {
			return
		}
		defer func() {
			if err != nil {
				c.pgPool.Close()
			}
		}()
	}

	c.logger.Debug(fmt.Sprintf("Running query '%s'", c.statement))
	c.rows, err = c.pgPool.Query(ctx, c.statement) // nolint: gocritic
	return
}

func (c *crdbChangefeedInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	c.dbMut.Lock()
	defer c.dbMut.Unlock()

	if c.rows == nil {
		return nil, nil, service.ErrNotConnected
	}

	if !c.rows.Next() {
		err := c.rows.Err()
		if err == nil {
			err = service.ErrNotConnected
		}
		c.rows.Close()
		c.rows = nil
		return nil, nil, err
	}

	values, err := c.rows.Values()
	if err != nil {
		return nil, nil, err
	}

	// Construct the new JSON
	var jsonBytes []byte
	jsonBytes, err = json.Marshal(map[string]string{
		"table":       values[0].(string),
		"primary_key": string(values[1].([]byte)), // Stringified JSON (Array)
		"row":         string(values[2].([]byte)), // Stringified JSON (Object)
	})
	if err != nil {
		return nil, nil, err
	}

	msg := service.NewMessage(jsonBytes)
	return msg, func(ctx context.Context, err error) error {
		// TODO: Store the current time for the CURSOR offset to cache
		return nil
	}, nil
}

func (c *crdbChangefeedInput) Close(ctx context.Context) error {
	c.shutSig.CloseNow()
	c.dbMut.Lock()
	isNil := c.pgPool == nil
	c.dbMut.Unlock()
	if isNil {
		return nil
	}
	select {
	case <-c.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

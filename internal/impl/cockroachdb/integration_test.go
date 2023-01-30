package crdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestIntegrationCRDB(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "cockroachdb/cockroach",
		Tag:          "latest",
		Cmd:          []string{"start-single-node", "--insecure"},
		ExposedPorts: []string{"8080", "26257"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	port := resource.GetPort("26257/tcp")

	var pgpool *pgxpool.Pool
	require.NoError(t, resource.Expire(900))

	require.NoError(t, pool.Retry(func() error {
		if pgpool == nil {
			if pgpool, err = pgxpool.Connect(context.Background(), fmt.Sprintf("postgresql://root@localhost:%v/defaultdb?sslmode=disable", port)); err != nil {
				return err
			}
		}
		// Enable changefeeds
		if _, err = pgpool.Exec(context.Background(), "SET CLUSTER SETTING kv.rangefeed.enabled = true;"); err != nil {
			return err
		}
		// Create table
		if _, err = pgpool.Exec(context.Background(), "CREATE TABLE foo (a INT PRIMARY KEY);"); err != nil {
			return err
		}
		// Insert a row in
		_, err = pgpool.Exec(context.Background(), "INSERT INTO foo VALUES (0);")
		return err
	}))
	t.Cleanup(func() {
		pgpool.Close()
	})

	template := fmt.Sprintf(`
cockroachdb_changefeed:
  dsn: postgresql://root@localhost:%v/defaultdb?sslmode=disable
  tables:
    - foo
`, port)
	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	runCtx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	var outBatches []string
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
		msgBytes, err := mb[0].AsBytes()
		require.NoError(t, err)
		outBatches = append(outBatches, string(msgBytes))
		cancelFn()
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)
	require.Equal(t, context.Canceled, streamOut.Run(runCtx))

	assert.Contains(t, outBatches, `{"primary_key":"[0]","row":"{\"after\": {\"a\": 0}}","table":"foo"}`)
}

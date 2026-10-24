package storage

import (
	"context"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

func newUnreachableBoltTransport(t *testing.T) *boltTransport {
	t.Helper()
	driver, err := neo4j.NewDriverWithContext(
		"bolt://127.0.0.1:1",
		neo4j.NoAuth(),
		func(c *neo4j.Config) {
			c.MaxTransactionRetryTime = 10 * time.Millisecond
			c.SocketConnectTimeout = 50 * time.Millisecond
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	return &boltTransport{driver: driver, database: "neo4j"}
}

func TestBoltTransport_ErrorPathsWithoutServer(t *testing.T) {
	bt := newUnreachableBoltTransport(t)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := bt.query(ctx, "MATCH (n) RETURN n", nil)
	require.Error(t, err)

	_, _, err = bt.queryWithColumns(ctx, "MATCH (n) RETURN n", nil)
	require.Error(t, err)

	err = bt.queryBatch(ctx, []remoteStatement{{Statement: "CREATE (n:Tmp)", Parameters: nil}})
	require.Error(t, err)

	_, err = bt.beginCypherTx(ctx, true)
	require.Error(t, err)
}

func TestBoltCypherTx_MethodsPanicWhenTxIsNil(t *testing.T) {
	bt := newUnreachableBoltTransport(t)
	sess := bt.driver.NewSession(context.Background(), neo4j.SessionConfig{DatabaseName: bt.database})
	tx := &boltCypherTx{session: sess, tx: nil}

	require.Panics(t, func() {
		_, _, _ = tx.QueryCypher(context.Background(), "RETURN 1", nil)
	})
	require.Panics(t, func() {
		_ = tx.Commit(context.Background())
	})
	require.Panics(t, func() {
		_ = tx.Rollback(context.Background())
	})
}

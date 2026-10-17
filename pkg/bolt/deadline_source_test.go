package bolt

import (
	"context"
	"fmt"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The client status is reported only when the client's tx_timeout is the
// deadline that stopped the statement; a shorter database query time limit
// keeps the server status (review: sol-r1-1392 P2).
func TestClientTimeoutStatusFollowsTheExpiredDeadline(t *testing.T) {
	server, client := nornicerrors.TransactionTimedOut, nornicerrors.TransactionTimedOutClientConfiguration
	for _, test := range []struct {
		name          string
		err           error
		clientTimeout bool
		want          string
	}{
		{"client tx_timeout expired", nornicerrors.MarkStatementTimeout(context.DeadlineExceeded, true), true, client},
		{"database limit expired first", nornicerrors.MarkStatementTimeout(context.DeadlineExceeded, false), true, server},
		{"server statement timeout, no tx_timeout", nornicerrors.MarkStatementTimeout(context.DeadlineExceeded, true), false, server},
	} {
		t.Run(test.name, func(t *testing.T) {
			code, message := nornicerrors.Neo4jStatus(test.err)
			code, _ = clientTimeoutStatus(test.err, code, message, test.clientTimeout)
			require.Equal(t, test.want, code)
		})
	}
}

// End to end on a streamed read with real database limits: whichever deadline
// expires first decides the status.
func TestBoltLazyResultTimeoutSource(t *testing.T) {
	for _, test := range []struct {
		name        string
		limit       time.Duration
		txTimeoutMs int64
		want        string
	}{
		{"database limit shorter than tx_timeout", 150 * time.Millisecond, 10_000, nornicerrors.TransactionTimedOut},
		{"tx_timeout shorter than database limit", 10 * time.Second, 150, nornicerrors.TransactionTimedOutClientConfiguration},
	} {
		t.Run(test.name, func(t *testing.T) {
			mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = mgr.Close() })
			server := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 32, ReadBufferSize: 8192, WriteBufferSize: 8192}, &mockExecutor{}, mgr)
			port := startBoltTestServer(t, server)
			conn := openBoltTestConn(t, port)
			runBoltQueryAndCollectRecords(t, conn, fmt.Sprintf("UNWIND range(0, %d) AS i CREATE (:Item {k: i})", 2999))
			limits := multidb.DefaultLimits()
			limits.Query.MaxQueryTime = test.limit
			require.NoError(t, mgr.SetDatabaseLimits("nornic", limits))
			require.NoError(t, SendRun(t, conn, "MATCH (n:Item) RETURN n.k AS k", nil, map[string]any{"tx_timeout": test.txTimeoutMs}))
			_, err = AssertSuccess(t, conn)
			require.NoError(t, err)
			records, success, failure := pullBatch(t, conn, 10)
			require.Empty(t, failure)
			require.Len(t, records, 10)
			require.Equal(t, true, success["has_more"], "the result is streamed")
			time.Sleep(400 * time.Millisecond)
			_, _, failure = pullBatch(t, conn, -1)
			require.Equal(t, test.want, failure)
		})
	}
}

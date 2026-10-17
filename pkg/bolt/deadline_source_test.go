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

// expiredDatabaseLimit admits a statement under a database query time limit
// that has already ended, and returns only once the RUN's own deadline (its
// tx_timeout) has ended too.
type expiredDatabaseLimit struct{}

func (expiredDatabaseLimit) CheckQueryRate() error       { return nil }
func (expiredDatabaseLimit) GetQueryLimits() interface{} { return nil }
func (expiredDatabaseLimit) CheckQueryLimits(ctx context.Context) (context.Context, context.CancelFunc, error) {
	limited, cancel := context.WithDeadlineCause(ctx, time.Now(), nornicerrors.ErrDatabaseQueryTimeLimit)
	<-ctx.Done()
	return limited, cancel, nil
}

type expiredDatabaseLimitEngine struct{ *storage.NamespacedEngine }

func (engine expiredDatabaseLimitEngine) GetInnerEngine() storage.Engine {
	return engine.NamespacedEngine
}
func (engine expiredDatabaseLimitEngine) GetQueryLimitChecker() interface {
	CheckQueryRate() error
	CheckQueryLimits(context.Context) (context.Context, context.CancelFunc, error)
	GetQueryLimits() interface{}
} {
	return expiredDatabaseLimit{}
}

// The database's limit ended the statement first; by the time it returns,
// the client's tx_timeout has expired as well. The status is the deadline
// that won, the server's, with its message (sol-r2-1392 P2).
func TestBoltDatabaseDeadlineWinsAfterBothExpired(t *testing.T) {
	store := expiredDatabaseLimitEngine{storage.NewNamespacedEngine(storage.NewMemoryEngine(), "deadline_winner")}
	_, port := startBoltIntegrationServer(t, store)
	conn := openBoltTestConn(t, port)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Second)))
	require.NoError(t, SendRun(t, conn, "UNWIND range(1, 100000000) AS x WITH x WHERE x < 0 RETURN count(x) AS c", nil, map[string]any{"tx_timeout": int64(2000)}))
	code, message, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Equal(t, nornicerrors.TransactionTimedOut, code)
	require.Equal(t, nornicerrors.TransactionTimedOutMessage, message)
}

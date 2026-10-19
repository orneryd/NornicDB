package bolt

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

var quotaReadFailure = errors.New("injected quota admission read failure")

// Keep real transactions and wire handling; fail only committed-state reads
// after RUN has staged the write and immediately before COMMIT admission.
type quotaReadFailureEngine struct {
	*storage.MemoryEngine
	mode atomic.Int32
}

func (e *quotaReadFailureEngine) GetInnerEngine() storage.Engine { return e.MemoryEngine }
func (e *quotaReadFailureEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	if e.mode.Load() == 1 {
		return nil, quotaReadFailure
	}
	return e.MemoryEngine.GetNode(id)
}
func (e *quotaReadFailureEngine) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	if e.mode.Load() == 2 {
		return nil, quotaReadFailure
	}
	return e.MemoryEngine.GetEdge(id)
}
func (e *quotaReadFailureEngine) NodeCount() (int64, error) {
	if e.mode.Load() == 3 {
		return 0, quotaReadFailure
	}
	return e.MemoryEngine.NodeCount()
}
func (e *quotaReadFailureEngine) EdgeCount() (int64, error) {
	if e.mode.Load() == 4 {
		return 0, quotaReadFailure
	}
	return e.MemoryEngine.EdgeCount()
}
func (e *quotaReadFailureEngine) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	if e.mode.Load() == 5 {
		return quotaReadFailure
	}
	return e.MemoryEngine.StreamNodesWithOptions(ctx, opts, visit)
}

func TestBoltTransactionQuotaReadFailureKeepsConnection(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode int32
	}{{"node", 1}, {"edge", 2}, {"nodes", 3}, {"edges", 4}, {"bytes", 5}} {
		t.Run(tc.name, func(t *testing.T) {
			base := &quotaReadFailureEngine{MemoryEngine: storage.NewMemoryEngine()}
			manager, err := multidb.NewDatabaseManager(base, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = manager.Close(); _ = base.Close() })
			require.NoError(t, manager.CreateDatabase("quota"))
			limits := &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 100, MaxEdges: 100}}
			if tc.mode == 5 {
				limits = &multidb.Limits{Storage: multidb.StorageLimits{MaxBytes: 100000}}
			}
			require.NoError(t, manager.SetDatabaseLimits("quota", limits))
			server := NewWithDatabaseManager(&Config{Port: 0, ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, manager)
			port := startBoltTestServer(t, server)
			conn := openBoltTestConn(t, port)
			require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))
			beginExplicitTransaction(t, conn, map[string]any{"db": "quota"})
			query := "CREATE (:Quota)"
			if tc.mode == 2 || tc.mode == 4 {
				query = "CREATE (:Quota)-[:LINK]->(:Quota)"
			}
			require.NoError(t, SendRun(t, conn, query, nil, map[string]any{"db": "quota"}))
			require.NoError(t, ReadSuccess(t, conn))
			require.NoError(t, SendPull(t, conn, nil))
			require.NoError(t, ReadSuccess(t, conn))
			base.mode.Store(tc.mode)
			require.NoError(t, SendCommit(t, conn))
			_, message, err := AssertFailure(t, conn)
			require.NoError(t, err)
			require.Contains(t, message, quotaReadFailure.Error(), "the admission read seam must be exercised")
			base.mode.Store(0)
			require.NoError(t, SendReset(t, conn))
			require.NoError(t, ReadSuccess(t, conn), "known rollback must retain this socket")
			require.NoError(t, SendRun(t, conn, "MATCH (n:Quota) RETURN count(n)", nil, map[string]any{"db": "quota"}))
			require.NoError(t, ReadSuccess(t, conn))
			require.NoError(t, SendPull(t, conn, nil))
			record, err := AssertRecord(t, conn)
			require.NoError(t, err)
			require.Equal(t, []any{int64(0)}, record)
			require.NoError(t, ReadSuccess(t, conn))
		})
	}
}

package nornicdb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The storage fault lasts less than the first advertised retry delay. It is
// injected at GetNode, the same public boundary as the committed retry test.
type xhighReadOutage struct {
	storage.Engine
	armed    atomic.Bool
	until    atomic.Int64
	failures atomic.Int32
}

func (e *xhighReadOutage) GetNode(id storage.NodeID) (*storage.Node, error) {
	if id == "a" && e.armed.Load() {
		now := time.Now().UnixNano()
		if e.until.Load() == 0 {
			e.until.CompareAndSwap(0, now+int64(400*time.Millisecond))
		}
		if now < e.until.Load() {
			e.failures.Add(1)
			return nil, errors.New("injected 400ms storage read outage")
		}
	}
	return e.Engine.GetNode(id)
}

func TestXhighTransientOutageDoesNotSpendAllRefreshRetriesImmediately(t *testing.T) {
	db, _ := openConsumerDB(t, false)
	ctx := context.Background()
	tenant := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "retrytenant")
	fault := &xhighReadOutage{Engine: tenant}
	svc, err := db.EnsureSearchIndexes(ctx, "retrytenant", fault, SearchIndexBuildWait)
	require.NoError(t, err)
	_, err = tenant.CreateNode(&storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "beforeoutage"}})
	require.NoError(t, err)
	require.NoError(t, svc.RefreshNode("a"))
	settleQueuedRefreshes()
	require.Equal(t, 1, consumerDBHits(t, svc, "beforeoutage"))
	fault.armed.Store(true)
	require.NoError(t, tenant.UpdateNode(&storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "afteroutage"}}))
	require.Eventually(t, func() bool { return fault.failures.Load() > 0 }, 5*time.Second, 10*time.Millisecond)
	// Correct exponential backoff's first retry is 500ms after the first
	// failed read; it therefore observes recovery. A drain that immediately
	// consumes its retry map burns all eight attempts inside the outage.
	time.Sleep(1500 * time.Millisecond)
	stored, err := tenant.GetNode("a")
	require.NoError(t, err)
	require.Equal(t, "afteroutage", stored.Properties["content"])
	t.Logf("failed storage reads during 400ms outage: %d", fault.failures.Load())
	require.Equal(t, 1, consumerDBHits(t, svc, "afteroutage"), "the retry owner lost a committed update after a short outage")
}

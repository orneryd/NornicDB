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

type retryOwnerReadWindow struct {
	storage.Engine
	edge     bool
	armed    atomic.Bool
	until    atomic.Int64
	failures atomic.Int32
}

func (e *retryOwnerReadWindow) failing() bool {
	if !e.armed.Load() {
		return false
	}
	now := time.Now().UnixNano()
	e.until.CompareAndSwap(0, now+int64(400*time.Millisecond))
	if now < e.until.Load() {
		e.failures.Add(1)
		return true
	}
	return false
}

func (e *retryOwnerReadWindow) GetNode(id storage.NodeID) (*storage.Node, error) {
	if !e.edge && id == "a" && e.failing() {
		return nil, errors.New("400ms node read outage")
	}
	return e.Engine.GetNode(id)
}

func (e *retryOwnerReadWindow) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	if e.edge && id == "r" && e.failing() {
		return nil, errors.New("400ms relationship read outage")
	}
	return e.Engine.GetEdge(id)
}

// The same outage must recover through every refresh entrance. A running drain
// may not consume future retries, and synchronous failures use the same owner.
func TestRefreshRetryOwnerPublicEntrances(t *testing.T) {
	for _, kind := range []string{"node", "relationship"} {
		for _, entrance := range []string{"drain", "synchronous"} {
			t.Run(kind+"/"+entrance, func(t *testing.T) {
				db, _ := openConsumerDB(t, true)
				ctx := context.Background()
				tenant := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "retryowner")
				fault := &retryOwnerReadWindow{Engine: tenant, edge: kind == "relationship"}
				svc, err := db.EnsureSearchIndexes(ctx, "retryowner", fault, SearchIndexBuildWait)
				require.NoError(t, err)
				_, err = tenant.CreateNode(&storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "beforeowneroutage"}})
				require.NoError(t, err)
				_, err = tenant.CreateNode(&storage.Node{ID: "b"})
				require.NoError(t, err)
				vector := make([]float32, db.config.Memory.EmbeddingDimensions)
				vector[0] = 1
				require.NoError(t, tenant.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "BEFORE", Properties: map[string]any{"vec": vector}}))
				require.NoError(t, svc.RefreshNode("a"))
				require.NoError(t, svc.RefreshEdge("r"))
				settleQueuedRefreshes()

				update := func() {
					if kind == "node" {
						require.NoError(t, tenant.UpdateNode(storage.NodeUpdateRequest{After: &storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "afterowneroutage"}}}))
					} else {
						require.NoError(t, tenant.UpdateEdge(storage.EdgeUpdateRequest{After: &storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "AFTER", Properties: map[string]any{"vec": vector}}}))
					}
				}
				visible := func() bool {
					if kind == "node" {
						return consumerDBHits(t, svc, "afterowneroutage") == 1
					}
					return svc.HasRelationshipVectorEntries("AFTER", "vec")
				}
				if entrance == "synchronous" {
					update()
					require.Eventually(t, visible, 5*time.Second, 10*time.Millisecond)
					settleQueuedRefreshes()
					if kind == "node" {
						require.NoError(t, svc.RemoveNode("a"))
					} else {
						require.NoError(t, svc.RemoveEdge("r"))
					}
					require.False(t, visible())
				}
				fault.armed.Store(true)
				if entrance == "drain" {
					update()
				} else if kind == "node" {
					require.Error(t, svc.RefreshNode("a"))
				} else {
					require.Error(t, svc.RefreshEdge("r"))
				}
				require.Eventually(t, func() bool { return fault.failures.Load() > 0 }, 5*time.Second, 5*time.Millisecond)
				time.Sleep(150 * time.Millisecond)
				require.EqualValues(t, 1, fault.failures.Load(), "the first retry must stay ineligible during its backoff")
				require.Eventually(t, visible, 5*time.Second, 10*time.Millisecond, "a committed update was lost")
			})
		}
	}
}

func TestRefreshRetryOwnerSupersededAndRetired(t *testing.T) {
	for _, kind := range []string{"node", "relationship"} {
		for _, retire := range []bool{false, true} {
			t.Run(kind+"/"+map[bool]string{false: "newer_write", true: "reset"}[retire], func(t *testing.T) {
				db, _ := openConsumerDB(t, true)
				tenant := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "retrynewer")
				fault := &retryOwnerReadWindow{Engine: tenant, edge: kind == "relationship"}
				svc, err := db.EnsureSearchIndexes(context.Background(), "retrynewer", fault, SearchIndexBuildWait)
				require.NoError(t, err)
				_, err = tenant.CreateNode(&storage.Node{ID: "a", Properties: map[string]any{"content": "initialownerword"}})
				require.NoError(t, err)
				_, err = tenant.CreateNode(&storage.Node{ID: "b"})
				require.NoError(t, err)
				vector := make([]float32, db.config.Memory.EmbeddingDimensions)
				vector[0] = 1
				require.NoError(t, tenant.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "BEFORE", Properties: map[string]any{"vec": vector}}))
				require.NoError(t, svc.RefreshNode("a"))
				require.NoError(t, svc.RefreshEdge("r"))
				settleQueuedRefreshes()
				fault.armed.Store(true)
				if kind == "node" {
					require.Error(t, svc.RefreshNode("a"))
					require.NoError(t, tenant.UpdateNode(storage.NodeUpdateRequest{After: &storage.Node{ID: "a", Properties: map[string]any{"content": "newestownerword"}}}))
				} else {
					require.Error(t, svc.RefreshEdge("r"))
					require.NoError(t, tenant.UpdateEdge(storage.EdgeUpdateRequest{After: &storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "AFTER", Properties: map[string]any{"vec": vector}}}))
				}
				fault.armed.Store(false)
				if retire {
					db.ResetSearchService("retrynewer")
					require.True(t, svc.Closed())
					svc, err = db.EnsureSearchIndexes(context.Background(), "retrynewer", tenant, SearchIndexBuildWait)
					require.NoError(t, err)
				} else if kind == "node" {
					require.NoError(t, svc.RefreshNode("a"))
				} else {
					require.NoError(t, svc.RefreshEdge("r"))
				}
				visible := func() bool {
					if kind == "node" {
						return consumerDBHits(t, svc, "newestownerword") == 1
					}
					return svc.HasRelationshipVectorEntries("AFTER", "vec")
				}
				require.Eventually(t, visible, 5*time.Second, 10*time.Millisecond)
				time.Sleep(time.Second)
				require.True(t, visible())
				if kind == "node" {
					require.Zero(t, consumerDBHits(t, svc, "initialownerword"))
				} else {
					require.False(t, svc.HasRelationshipVectorEntries("BEFORE", "vec"))
				}
			})
		}
	}
}

package nornicdb

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type retryOwnerReadBarrier struct {
	*retryOwnerReadWindow
	pause   atomic.Bool
	blocked chan struct{}
	release chan struct{}
}

func (e *retryOwnerReadBarrier) wait(err error) {
	if err != nil && e.pause.CompareAndSwap(true, false) {
		close(e.blocked)
		<-e.release
	}
}

func (e *retryOwnerReadBarrier) GetNode(id storage.NodeID) (*storage.Node, error) {
	node, err := e.retryOwnerReadWindow.GetNode(id)
	e.wait(err)
	return node, err
}

func (e *retryOwnerReadBarrier) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	edge, err := e.retryOwnerReadWindow.GetEdge(id)
	e.wait(err)
	return edge, err
}

func TestRefreshRetryOwnerWaitsForAbandonedNewerWork(t *testing.T) {
	for _, kind := range []string{"node", "relationship"} {
		for _, order := range []string{"before_failure", "after_failure"} {
			t.Run(kind+"/"+order, func(t *testing.T) {
				db, _ := openConsumerDB(t, true)
				tenant := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "retryheld")
				fault := &retryOwnerReadBarrier{
					retryOwnerReadWindow: &retryOwnerReadWindow{Engine: tenant, edge: kind == "relationship"},
					blocked:              make(chan struct{}),
					release:              make(chan struct{}),
				}
				t.Cleanup(func() {
					select {
					case <-fault.release:
					default:
						close(fault.release)
					}
				})
				svc, err := db.EnsureSearchIndexesBuilt(context.Background(), "retryheld", fault)
				require.NoError(t, err)
				_, err = tenant.CreateNode(&storage.Node{ID: "a", Properties: map[string]any{"content": "heldownerword"}})
				require.NoError(t, err)
				_, err = tenant.CreateNode(&storage.Node{ID: "b"})
				require.NoError(t, err)
				vector := make([]float32, db.config.Memory.EmbeddingDimensions)
				vector[0] = 1
				require.NoError(t, tenant.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "AFTER", Properties: map[string]any{"vec": vector}}))
				require.NoError(t, svc.RefreshNode("a"))
				require.NoError(t, svc.RefreshEdge("r"))
				settleQueuedRefreshes()
				if kind == "node" {
					require.NoError(t, svc.RemoveNode("a"))
				} else {
					require.NoError(t, svc.RemoveEdge("r"))
				}
				fault.armed.Store(true)
				fault.pause.Store(order == "before_failure")
				readDone := make(chan error, 1)
				go func() {
					if kind == "node" {
						readDone <- svc.RefreshNode("a")
					} else {
						readDone <- svc.RefreshEdge("r")
					}
				}()
				reserve := func() search.RefreshRetry {
					if kind == "node" {
						return search.RefreshRetry{Node: svc.ReserveNodeMutation("a")}
					}
					return search.RefreshRetry{Edge: svc.ReserveEdgeMutation("r")}
				}
				var newer search.RefreshRetry
				if order == "before_failure" {
					select {
					case <-fault.blocked:
					case <-time.After(5 * time.Second):
						t.Fatal("refresh did not reach its read barrier")
					}
					newer = reserve()
					close(fault.release)
				}
				select {
				case err := <-readDone:
					require.Error(t, err)
				case <-time.After(5 * time.Second):
					t.Fatal("failed refresh did not return")
				}
				if order == "after_failure" {
					newer = reserve()
				}
				fault.armed.Store(false)
				visible := func() bool {
					if kind == "node" {
						return consumerDBHits(t, svc, "heldownerword") == 1
					}
					return svc.HasRelationshipVectorEntries("AFTER", "vec")
				}
				time.Sleep(900 * time.Millisecond)
				require.False(t, visible(), "a retry overtook newer live work")
				require.EqualValues(t, 1, fault.failures.Load(), "waiting for newer work spent retry attempts")
				newer.Discard()
				require.Eventually(t, visible, 5*time.Second, 10*time.Millisecond, "discarding the newer work lost the older retry")
			})
		}
	}
}

func TestRefreshRetryOwnerKeepsNewerFailure(t *testing.T) {
	for _, kind := range []string{"node", "relationship"} {
		t.Run(kind, func(t *testing.T) {
			db, _ := openConsumerDB(t, true)
			tenant := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "retrylate")
			fault := &retryOwnerReadBarrier{
				retryOwnerReadWindow: &retryOwnerReadWindow{Engine: tenant, edge: kind == "relationship"},
				blocked:              make(chan struct{}),
				release:              make(chan struct{}),
			}
			t.Cleanup(func() {
				select {
				case <-fault.release:
				default:
					close(fault.release)
				}
			})
			svc, err := db.EnsureSearchIndexesBuilt(context.Background(), "retrylate", fault)
			require.NoError(t, err)
			_, err = tenant.CreateNode(&storage.Node{ID: "a", Properties: map[string]any{"content": "lateownerword"}})
			require.NoError(t, err)
			_, err = tenant.CreateNode(&storage.Node{ID: "b"})
			require.NoError(t, err)
			vector := make([]float32, db.config.Memory.EmbeddingDimensions)
			vector[0] = 1
			require.NoError(t, tenant.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "AFTER", Properties: map[string]any{"vec": vector}}))
			require.NoError(t, svc.RefreshNode("a"))
			require.NoError(t, svc.RefreshEdge("r"))
			settleQueuedRefreshes()
			if kind == "node" {
				require.NoError(t, svc.RemoveNode("a"))
			} else {
				require.NoError(t, svc.RemoveEdge("r"))
			}
			fault.armed.Store(true)
			fault.pause.Store(true)
			olderDone := make(chan error, 1)
			go func() {
				if kind == "node" {
					olderDone <- svc.RefreshNode("a")
				} else {
					olderDone <- svc.RefreshEdge("r")
				}
			}()
			select {
			case <-fault.blocked:
			case <-time.After(5 * time.Second):
				t.Fatal("older refresh did not reach its barrier")
			}
			var newer search.RefreshRetry
			if kind == "node" {
				newer = search.RefreshRetry{Node: svc.ReserveNodeMutation("a")}
				require.Error(t, svc.RefreshNodeWithMutation(newer.Node))
			} else {
				newer = search.RefreshRetry{Edge: svc.ReserveEdgeMutation("r")}
				require.Error(t, svc.RefreshEdgeWithMutation(newer.Edge))
			}
			close(fault.release)
			select {
			case err := <-olderDone:
				require.Error(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("older failed refresh did not return")
			}
			require.True(t, newer.Current(), "an older failure replaced the newer retry's reservation")
			fault.armed.Store(false)
			require.Eventually(t, func() bool {
				if kind == "node" {
					return consumerDBHits(t, svc, "lateownerword") == 1
				}
				return svc.HasRelationshipVectorEntries("AFTER", "vec")
			}, 5*time.Second, 10*time.Millisecond)
		})
	}
}

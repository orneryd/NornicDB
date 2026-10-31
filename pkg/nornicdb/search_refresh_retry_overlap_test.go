package nornicdb

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
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

// retryOwnerOverlapFixture is the shared setup for the retry-owner tests that
// overlap a failed refresh with a newer mutation reservation: an outage window
// over a dedicated namespace, seeded searchable node and relationship state, and
// one stale refresh of "a" (or "r") whose first armed read failure is parked at
// an optional barrier until the test releases it.
type retryOwnerOverlapFixture struct {
	svc      *search.Service
	fault    *retryOwnerReadBarrier
	kind     string
	word     string
	readDone chan error // receives the stale refresh's result
}

// openRetryOwnerOverlapFixture builds the fixture for kind ("node" or
// "relationship"). namespace selects the namespaced tenant and word is the
// content that must become searchable for the node variant.
func openRetryOwnerOverlapFixture(t *testing.T, kind, namespace, word string) *retryOwnerOverlapFixture {
	t.Helper()
	db, _ := openConsumerDB(t, true)
	tenant := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), namespace)
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
	svc, err := db.EnsureSearchIndexes(context.Background(), namespace, fault, SearchIndexBuildWait)
	require.NoError(t, err)
	_, err = tenant.CreateNode(&storage.Node{ID: "a", Properties: map[string]any{"content": word}})
	require.NoError(t, err)
	_, err = tenant.CreateNode(&storage.Node{ID: "b"})
	require.NoError(t, err)
	vector := make([]float32, db.config.Memory.EmbeddingDimensions)
	vector[0] = 1
	require.NoError(t, tenant.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "AFTER", Properties: map[string]any{"vec": vector}}))
	require.NoError(t, svc.RefreshNode("a", storage.CaptureVectorWorkOrigin()))
	require.NoError(t, svc.RefreshEdge("r", storage.CaptureVectorWorkOrigin()))
	settleQueuedRefreshes()
	if kind == "node" {
		require.NoError(t, svc.RemoveNode("a"))
	} else {
		require.NoError(t, svc.RemoveEdge("r"))
	}
	return &retryOwnerOverlapFixture{
		svc:      svc,
		fault:    fault,
		kind:     kind,
		word:     word,
		readDone: make(chan error, 1),
	}
}

// startStaleRefresh arms the outage window and launches the pending refresh of
// the fixture's entity, parking its first read failure when pauseAtBarrier is set.
func (f *retryOwnerOverlapFixture) startStaleRefresh(t *testing.T, pauseAtBarrier bool) {
	t.Helper()
	f.fault.armed.Store(true)
	f.fault.pause.Store(pauseAtBarrier)
	go func() {
		if f.kind == "node" {
			f.readDone <- f.svc.RefreshNode("a", storage.CaptureVectorWorkOrigin())
		} else {
			f.readDone <- f.svc.RefreshEdge("r", storage.CaptureVectorWorkOrigin())
		}
	}()
}

// waitForStaleFailure requires the stale refresh to return an error.
func (f *retryOwnerOverlapFixture) waitForStaleFailure(t *testing.T, what string) {
	t.Helper()
	select {
	case err := <-f.readDone:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal(what + " did not return")
	}
}

// reserveNewer takes a mutation reservation for the fixture's entity.
func (f *retryOwnerOverlapFixture) reserveNewer() search.RefreshRetry {
	if f.kind == "node" {
		return search.RefreshRetry{Node: f.svc.ReserveNodeMutation("a", storage.CaptureVectorWorkOrigin())}
	}
	return search.RefreshRetry{Edge: f.svc.ReserveEdgeMutation("r", storage.CaptureVectorWorkOrigin())}
}

// visible reports whether the fixture's entity is currently searchable.
func (f *retryOwnerOverlapFixture) visible(t *testing.T) func() bool {
	return func() bool {
		if f.kind == "node" {
			return consumerDBHits(t, f.svc, f.word) == 1
		}
		return f.svc.HasRelationshipVectorEntries("AFTER", "vec")
	}
}

func TestRefreshRetryOwnerOverlappingMutations(t *testing.T) {
	t.Run("abandoned newer work", func(t *testing.T) {
		for _, kind := range []string{"node", "relationship"} {
			for _, order := range []string{"before_failure", "after_failure"} {
				t.Run(kind+"/"+order, func(t *testing.T) {
					fixture := openRetryOwnerOverlapFixture(t, kind, "retryheld", "heldownerword")
					fixture.startStaleRefresh(t, order == "before_failure")
					var newer search.RefreshRetry
					if order == "before_failure" {
						runtimeFixtureOwner.WaitForBarrier(t, fixture.fault.blocked, 5*time.Second, nil, "refresh did not reach its read barrier")
						newer = fixture.reserveNewer()
						close(fixture.fault.release)
					}
					fixture.waitForStaleFailure(t, "failed refresh")
					if order == "after_failure" {
						newer = fixture.reserveNewer()
					}
					fixture.fault.armed.Store(false)
					visible := fixture.visible(t)
					time.Sleep(900 * time.Millisecond)
					require.False(t, visible(), "a retry overtook newer live work")
					require.EqualValues(t, 1, fixture.fault.failures.Load(), "waiting for newer work spent retry attempts")
					newer.Discard()
					require.Eventually(t, visible, 5*time.Second, 10*time.Millisecond, "discarding the newer work lost the older retry")
				})
			}
		}
	})
	t.Run("newer failure retained", func(t *testing.T) {
		for _, kind := range []string{"node", "relationship"} {
			t.Run(kind, func(t *testing.T) {
				fixture := openRetryOwnerOverlapFixture(t, kind, "retrylate", "lateownerword")
				fixture.startStaleRefresh(t, true)
				runtimeFixtureOwner.WaitForBarrier(t, fixture.fault.blocked, 5*time.Second, nil, "older refresh did not reach its read barrier")
				newer := fixture.reserveNewer()
				if kind == "node" {
					require.Error(t, fixture.svc.RefreshNodeWithMutation(newer.Node))
				} else {
					require.Error(t, fixture.svc.RefreshEdgeWithMutation(newer.Edge))
				}
				close(fixture.fault.release)
				fixture.waitForStaleFailure(t, "older failed refresh")
				require.True(t, newer.Current(), "an older failure replaced the newer retry's reservation")
				fixture.fault.armed.Store(false)
				require.Eventually(t, fixture.visible(t), 5*time.Second, 10*time.Millisecond)
			})
		}
	})
}

package storage

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestCommitEndpointPointReadRetainsCancellationAtCapture(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	for _, id := range []NodeID{"test:point-a", "test:point-b"} {
		bufferedCreate(t, b, &Node{ID: id})
	}
	require.Positive(t, b.writeBehind.PendingOps(), "real buffered state must require retirement admission")
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	edge := &Edge{ID: "test:point-edge", StartNode: "test:point-a", EndNode: "test:point-b", Type: "LINK"}
	require.NoError(t, tx.CreateEdge(t.Context(), edge))
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	orderWaiting := make(chan struct{}, 1)
	readWaiting := make(chan context.Context, 1)
	t.Cleanup(setAdmissionWaitObserver(func(owner any, seen context.Context) {
		pcs := make([]uintptr, 32)
		n := runtime.Callers(2, pcs)
		frames := runtime.CallersFrames(pcs[:n])
		buffered, endpoint := false, false
		for {
			f, more := frames.Next()
			buffered = buffered || strings.HasSuffix(f.Function, ".commitBufferedLocked")
			endpoint = endpoint || strings.HasSuffix(f.Function, ".revalidateEndpointsLocked")
			if !more {
				break
			}
		}
		if buffered && !endpoint {
			select {
			case orderWaiting <- struct{}{}:
			default:
			}
		}
		if owner == &b.writeBehind.mu && endpoint {
			select {
			case readWaiting <- seen:
			default:
			}
		}
	}))
	require.NoError(t, b.writeBehind.order.lockContext(t.Context(), &allOrderStripes, true))
	var orderOnce, readOnce sync.Once
	releaseOrder := func() { orderOnce.Do(func() { b.writeBehind.order.unlock(&allOrderStripes, true) }) }
	readHeld := false
	releaseRead := func() {
		if readHeld {
			readOnce.Do(b.writeBehind.mu.Unlock)
		}
	}
	done := make(chan error, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		releaseRead()
		releaseOrder()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("endpoint commit did not join")
		}
	})
	go func() { defer close(joined); done <- tx.CommitContext(ctx) }()
	select {
	case <-orderWaiting:
	case <-time.After(5 * time.Second):
		t.Fatal("public commit did not reach actual buffered order admission")
	}
	b.writeBehind.mu.Lock()
	readHeld = true
	releaseOrder()
	var admitted context.Context
	select {
	case admitted = <-readWaiting:
	case <-time.After(5 * time.Second):
		t.Fatal("endpoint revalidation did not reach actual held read capture")
	}
	cancel()
	select {
	case err = <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("public commit discarded ctx at endpoint point-read capture")
	}
	require.True(t, admitted == ctx, "endpoint read capture must receive the original request owner")
	releaseRead()
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("endpoint worker did not join after result")
	}
	_, err = b.GetStoredEdge(t.Context(), edge.ID)
	require.ErrorIs(t, err, ErrNotFound, "cancelled preacceptance must not publish edge")
	retry, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = retry.Rollback() })
	require.NoError(t, retry.SetImplicit(true))
	require.NoError(t, retry.SetNamespace("test"))
	require.NoError(t, retry.CreateEdge(t.Context(), edge))
	require.NoError(t, retry.CommitContext(t.Context()))
	actual, err := b.GetStoredEdge(t.Context(), edge.ID)
	require.NoError(t, err)
	require.Equal(t, edge.ID, actual.ID)
}

// This component control pins the same native point kernel for each private
// transaction read shape. The public commit control above proves reachability
// from request-owned endpoint revalidation; generic Engine point APIs retain
// their separate context-free contract.
func TestNativeTransactionPointShapesRetainCaptureContext(t *testing.T) {
	for _, route := range []string{"constraint_node", "labels", "query_metadata"} {
		t.Run(route, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
			bufferedCreate(t, b, &Node{ID: "test:point-shape", Labels: []string{"Shape"}, Properties: map[string]any{"payload": "retained"}})
			tx, err := b.BeginTransaction(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			type outcome struct {
				node   *Node
				labels []string
				err    error
			}
			read := func(ctx context.Context) outcome {
				tx.mu.Lock()
				defer tx.mu.Unlock()
				switch route {
				case "constraint_node":
					n, e := (txConstraintView{tx}).Node(ctx, "test:point-shape")
					return outcome{node: n, err: e}
				case "labels":
					labels, e := tx.committedNodeLabelsLocked(ctx, "test:point-shape")
					return outcome{labels: labels, err: e}
				default:
					n, e := tx.getCommittedNodeReadLocked(ctx, "test:point-shape", false, storedRead)
					return outcome{node: n, err: e}
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			waiting := make(chan context.Context, 1)
			t.Cleanup(setAdmissionWaitObserver(func(owner any, seen context.Context) {
				if owner == &b.writeBehind.mu {
					select {
					case waiting <- seen:
					default:
					}
				}
			}))
			b.writeBehind.mu.Lock()
			var once sync.Once
			unlock := func() { once.Do(b.writeBehind.mu.Unlock) }
			done := make(chan outcome, 1)
			joined := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				unlock()
				select {
				case <-joined:
				case <-time.After(5 * time.Second):
					t.Error("point shape reader did not join")
				}
			})
			go func() { defer close(joined); done <- read(ctx) }()
			var admitted context.Context
			select {
			case admitted = <-waiting:
			case <-time.After(5 * time.Second):
				t.Fatal("native point shape did not reach held capture")
			}
			cancel()
			select {
			case got := <-done:
				require.ErrorIs(t, got.err, context.Canceled)
				require.Nil(t, got.node)
				require.Nil(t, got.labels)
			case <-time.After(time.Second):
				t.Fatal("native point shape discarded caller cancellation")
			}
			require.True(t, admitted == ctx)
			unlock()
			select {
			case <-joined:
			case <-time.After(5 * time.Second):
				t.Fatal("point shape reader did not join after result")
			}
			healthy := read(t.Context())
			require.NoError(t, healthy.err)
			if route == "labels" {
				require.Equal(t, []string{"Shape"}, healthy.labels)
			} else {
				require.Equal(t, "retained", healthy.node.Properties["payload"])
			}
		})
	}
}

func TestLatestVisibleBatchPointCaptureKeepsRequestContext(t *testing.T) {
	for _, warm := range []bool{false, true} {
		name := "cold"
		if warm {
			name = "cached"
		}
		t.Run(name, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
			target := NodeID("test:latest-target")
			for _, id := range []NodeID{target, "peer:latest-target"} {
				_, err := b.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Latest"}, Properties: map[string]any{"value": int64(7)}})
				require.NoError(t, err)
			}
			bufferedCreate(t, b, &Node{ID: "test:latest-sentinel"})
			if warm {
				_, err := b.GetStoredNode(t.Context(), target)
				require.NoError(t, err)
			} else {
				b.nodeCacheMu.Lock()
				delete(b.nodeCache, target)
				b.nodeCacheMu.Unlock()
			}
			b.nodeCacheMu.RLock()
			_, cached := b.nodeCache[target]
			b.nodeCacheMu.RUnlock()
			require.Equal(t, warm, cached, "fixture must exercise the actual selected point-cache shape")
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			waiting := make(chan context.Context, 1)
			t.Cleanup(setAdmissionWaitObserver(func(owner any, seen context.Context) {
				if owner != &b.writeBehind.mu {
					return
				}
				pcs := make([]uintptr, 32)
				n := runtime.Callers(2, pcs)
				frames := runtime.CallersFrames(pcs[:n])
				point := false
				for {
					f, more := frames.Next()
					point = point || strings.HasSuffix(f.Function, ".getNodeByID")
					if !more {
						break
					}
				}
				if point {
					select {
					case waiting <- seen:
					default:
					}
				}
			}))
			b.writeBehind.mu.Lock()
			var once sync.Once
			unlock := func() { once.Do(b.writeBehind.mu.Unlock) }
			type outcome struct {
				nodes map[NodeID]*Node
				err   error
			}
			done := make(chan outcome, 1)
			joined := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				unlock()
				select {
				case <-joined:
				case <-time.After(5 * time.Second):
					t.Error("latest-visible batch worker did not join")
				}
			})
			ids := []NodeID{target, "test:missing", "peer:latest-target"}
			go func() {
				defer close(joined)
				nodes, err := b.BatchGetNodesLatestVisible(ctx, ids)
				done <- outcome{nodes, err}
			}()
			var admitted context.Context
			select {
			case admitted = <-waiting:
			case <-time.After(5 * time.Second):
				t.Fatal("public latest-visible batch did not reach actual point capture")
			}
			cancel()
			select {
			case got := <-done:
				require.ErrorIs(t, got.err, context.Canceled)
				require.Nil(t, got.nodes, "canceled batch must not return a partial map")
			case <-time.After(time.Second):
				t.Fatal("latest-visible batch discarded request context at point capture")
			}
			require.True(t, admitted == ctx)
			unlock()
			select {
			case <-joined:
			case <-time.After(5 * time.Second):
				t.Fatal("latest-visible batch worker did not join after result")
			}
			nodes, err := b.BatchGetNodesLatestVisible(t.Context(), ids)
			require.NoError(t, err)
			require.Len(t, nodes, 2)
			for _, id := range []NodeID{target, "peer:latest-target"} {
				require.Equal(t, id, nodes[id].ID)
				require.Equal(t, int64(7), nodes[id].Properties["value"])
				require.Equal(t, []string{"Latest"}, nodes[id].Labels)
			}
		})
	}
}

func TestLatestVisibleBatchRetainsNonContextPointErrorSkipping(t *testing.T) {
	b := newTestEngine(t)
	_, err := b.CreateNode(t.Context(), &Node{ID: "test:valid-latest", Properties: map[string]any{"value": int64(9)}})
	require.NoError(t, err)
	require.NoError(t, b.withUpdate(t.Context(), updateRequest{}, func(txn *badger.Txn) error { return txn.Set(nodeKey("test:malformed-latest"), []byte{0xff}) }))
	_, err = b.GetStoredNode(t.Context(), "test:malformed-latest")
	require.Error(t, err, "actual native malformed point read supplies the existing skip condition")
	nodes, err := b.BatchGetNodesLatestVisible(t.Context(), []NodeID{"test:malformed-latest", "test:missing-latest", "test:valid-latest"})
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, int64(9), nodes["test:valid-latest"].Properties["value"])
}

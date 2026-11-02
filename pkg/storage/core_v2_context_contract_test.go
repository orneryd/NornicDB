package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestCoreV2CountAdmissionCancellationAndRetry(t *testing.T) {
	for _, edge := range []bool{false, true} {
		t.Run(map[bool]string{false: "nodes", true: "edges"}[edge], func(t *testing.T) {
			e, _, _ := pointReadFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			waiting := make(chan struct{}, 1)
			restore := setAdmissionWaitObserver(func(owner any, seen context.Context) {
				if owner == &e.totalCountWriteMu {
					select {
					case waiting <- struct{}{}:
					default:
					}
				}
			})
			defer restore()
			e.totalCountWriteMu.Lock()
			var once sync.Once
			unlock := func() { once.Do(e.totalCountWriteMu.Unlock) }
			defer unlock()
			done := make(chan error, 1)
			go func() {
				var err error
				if edge {
					_, err = e.EdgeCount(ctx)
				} else {
					_, err = e.NodeCount(ctx)
				}
				done <- err
			}()
			select {
			case <-waiting:
			case <-time.After(5 * time.Second):
				unlock()
				cancel()
				<-done
				t.Fatal("count did not reach held physical admission")
			}
			cancel()
			var err error
			stoppedBeforeRelease := true
			select {
			case err = <-done:
			case <-time.After(time.Second):
				stoppedBeforeRelease = false
				unlock()
				err = <-done
			}
			unlock()
			require.True(t, stoppedBeforeRelease, "canceled count waited for an unrelated lock owner")
			require.ErrorIs(t, err, context.Canceled)
			if edge {
				count, err := e.EdgeCount(t.Context())
				require.NoError(t, err)
				require.Equal(t, int64(1), count)
			} else {
				count, err := e.NodeCount(t.Context())
				require.NoError(t, err)
				require.Equal(t, int64(2), count)
			}
		})
	}
}

func TestCoreV2PointAndScalarReadsRejectCancelledRequest(t *testing.T) {
	e, _, _ := pointReadFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	reads := map[string]func() error{
		"embedding metadata":  func() error { _, err := e.GetNodeEmbeddingMeta(ctx, "test:a"); return err },
		"entity metadata":     func() error { _, err := e.GetEntityMeta(ctx, "test:a"); return err },
		"node head":           func() error { _, err := e.GetNodeCurrentHead(ctx, "test:a"); return err },
		"edge head":           func() error { _, err := e.GetEdgeCurrentHead(ctx, "test:e"); return err },
		"node effective":      func() error { _, err := NewWALEngine(e, nil).GetNodeLatestEffective(ctx, "test:a"); return err },
		"edge effective":      func() error { _, err := NewWALEngine(e, nil).GetEdgeLatestEffective(ctx, "test:e"); return err },
		"type count":          func() error { _, err := e.EdgeCountByType(ctx, "R"); return err },
		"start-label count":   func() error { _, err := e.EdgeCountByStartLabelInNamespace(ctx, "test", "A", "R"); return err },
		"end-label count":     func() error { _, err := e.EdgeCountByEndLabelInNamespace(ctx, "test", "A", "R"); return err },
		"node prefix":         func() error { _, err := e.NodeCountByPrefix(ctx, "test:"); return err },
		"edge prefix":         func() error { _, err := e.EdgeCountByPrefix(ctx, "test:"); return err },
		"embedding candidate": func() error { _, err := e.FindNodeNeedingEmbedding(ctx); return err },
		"headers":             func() error { _, _, err := e.OutgoingEdgeHeaders(ctx, "test:a"); return err },
		"matching": func() error {
			_, err := e.MatchEdgesBetween(ctx, "test:a", "test:b", "R", nil, func(*Edge) bool { t.Error("canceled match invoked predicate"); return true })
			return err
		},
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) { require.ErrorIs(t, read(), context.Canceled) })
	}
	node, err := e.GetNode(t.Context(), "test:a")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:a"), node.ID)
}

func TestCoreV2EntityCallbacksReceiveOriginalContext(t *testing.T) {
	type key struct{}
	ctx := context.WithValue(t.Context(), key{}, "request")
	e := newTestMemoryEngine(t)
	updateSeen, deleteSeen := false, false
	update := func(seen context.Context, request NodeUpdateRequest) error {
		require.Same(t, ctx, seen)
		require.Equal(t, "request", seen.Value(key{}))
		updateSeen = true
		return nil
	}
	remove := func(seen context.Context, id NodeID) error { require.Same(t, ctx, seen); deleteSeen = true; return nil }
	require.NoError(t, ApplyEntityChange(ctx, e, Operation{Type: OpUpdateNode, Node: &Node{ID: "test:a"}}, update, remove))
	require.NoError(t, ApplyEntityChange(ctx, e, Operation{Type: OpDeleteNode, NodeID: "test:a"}, update, remove))
	require.True(t, updateSeen)
	require.True(t, deleteSeen)
}

func TestCoreV2BeginSnapshotCaptureRetainsRequestCancellation(t *testing.T) {
	e := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	bufferedCreate(t, e, &Node{ID: "test:begin", Properties: map[string]any{"content": "body"}})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waiting := make(chan context.Context, 1)
	restore := setAdmissionWaitObserver(func(owner any, seen context.Context) {
		if owner == &e.writeBehind.mu {
			select {
			case waiting <- seen:
			default:
			}
		}
	})
	defer restore()
	e.writeBehind.mu.Lock()
	var once sync.Once
	unlock := func() { once.Do(e.writeBehind.mu.Unlock) }
	defer unlock()
	type result struct {
		tx  *BadgerTransaction
		err error
	}
	done := make(chan result, 1)
	go func() { tx, err := e.BeginTransaction(ctx); done <- result{tx, err} }()
	select {
	case seen := <-waiting:
		require.Same(t, ctx, seen)
	case <-time.After(5 * time.Second):
		unlock()
		cancel()
		r := <-done
		if r.tx != nil {
			_ = r.tx.Rollback()
		}
		t.Fatal("BEGIN did not reach retirement capture")
	}
	cancel()
	var got result
	returned := true
	select {
	case got = <-done:
	case <-time.After(time.Second):
		returned = false
		unlock()
		got = <-done
	}
	unlock()
	if got.tx != nil {
		_ = got.tx.Rollback()
	}
	require.True(t, returned)
	require.Nil(t, got.tx)
	require.ErrorIs(t, got.err, context.Canceled)
	tx, err := e.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
}

func TestCoreV2CompositeFallbackAndMetadataKeepCallerContext(t *testing.T) {
	e, nodeVersion, edgeVersion := pointReadFixture(t)
	// Deliberately expose only the required Engine contract. The optional MVCC
	// branch is absent, so this tests the actual constituent fallback reads.
	c := NewCompositeEngine(map[string]Engine{"one": struct{ Engine }{e}}, map[string]string{"one": "test"}, map[string]string{"one": "read_write"})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	reads := map[string]func() error{
		"node latest":   func() error { _, err := c.GetNodeLatestVisible(ctx, "test:a"); return err },
		"node version":  func() error { _, err := c.GetNodeVisibleAt(ctx, "test:a", nodeVersion); return err },
		"edge latest":   func() error { _, err := c.GetEdgeLatestVisible(ctx, "test:e"); return err },
		"edge version":  func() error { _, err := c.GetEdgeVisibleAt(ctx, "test:e", edgeVersion); return err },
		"node database": func() error { _, err := c.ConstituentDatabaseForNode(ctx, "test:a"); return err },
		"edge database": func() error { _, err := c.ConstituentDatabaseForEdge(ctx, "test:e"); return err },
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) { require.ErrorIs(t, read(), context.Canceled) })
	}
	name, err := c.ConstituentDatabaseForNode(t.Context(), "test:a")
	require.NoError(t, err)
	require.Equal(t, "test", name)
	name, err = c.ConstituentDatabaseForNode(t.Context(), "test:missing")
	require.NoError(t, err)
	require.Empty(t, name)
}

func TestCoreV2TemporalAndSidecarPreparationKeepRequestContext(t *testing.T) {
	e, _, _ := pointReadFixture(t)
	node, err := e.GetNode(t.Context(), "test:a")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	namespace := NewNamespacedEngine(e, "test")
	reads := map[string]func() error{
		"temporal lookup": func() error {
			_, err := e.GetTemporalNodeAsOfInNamespace(ctx, "test", "A", "id", "x", "from", "to", time.Now())
			return err
		},
		"temporal current": func() error { _, err := e.IsCurrentTemporalNode(ctx, node, time.Now()); return err },
		"namespace temporal lookup": func() error {
			_, err := namespace.GetTemporalNodeAsOf(ctx, "A", "id", "x", "from", "to", time.Now())
			return err
		},
		"namespace current": func() error { _, err := namespace.IsCurrentTemporalNode(ctx, node, time.Now()); return err },
		"native sidecar":    func() error { return e.UpdateNodeEmbeddingSidecar(ctx, node) },
		"namespace writeinput": func() error {
			_, err := namespace.NodeWriteInput(ctx, &Node{ID: "new", Labels: []string{"CancelledToken"}})
			return err
		},
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) { require.ErrorIs(t, read(), context.Canceled) })
	}
	_, err = e.GetNode(t.Context(), "test:new")
	require.ErrorIs(t, err, ErrNotFound)
}

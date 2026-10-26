package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResidualCommitAdmissionCancellation(t *testing.T) {
	for _, route := range []string{"transaction", "merge-preparation", "barrier", "ordinary-gate", "fresh-property-gate", "count", "change-log"} {
		t.Run(route, func(t *testing.T) {
			b, err := NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			if route == "count" {
				for _, id := range []NodeID{"test:a", "test:b"} {
					_, err := b.CreateNode(&Node{ID: id, Labels: []string{"L"}})
					require.NoError(t, err)
				}
			}
			if route == "change-log" {
				b.SetChangeCapture("test", ChangeCaptureDiff)
			}
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			var target sync.Locker
			if route == "count" {
				require.NoError(t, tx.CreateEdge(&Edge{ID: "test:wait-e", StartNode: "test:a", EndNode: "test:b", Type: "WAIT"}))
				require.NotEmpty(t, tx.pendingEdgeTypeLabelCountDeltas)
				target = &b.edgeTypeCountWriteMu
			} else {
				var properties map[string]any
				if route == "fresh-property-gate" {
					properties = map[string]any{"never-persisted-token": int64(1)}
				}
				_, err = tx.CreateNode(&Node{ID: "test:wait-n", Properties: properties})
				require.NoError(t, err)
				switch route {
				case "transaction", "merge-preparation":
					target = &tx.mu
				case "barrier":
					target = &b.writeBarrier
				case "ordinary-gate", "fresh-property-gate":
					target = &b.db.commitGate
				case "change-log":
					target = &b.changeLogFor("test").mu
				}
			}
			target.Lock()
			held := true
			defer func() {
				if held {
					target.Unlock()
				}
			}()
			waited := make(chan context.Context, 1)
			restore := setAdmissionWaitObserver(func(owner any, ctx context.Context) {
				if owner == target {
					select {
					case waited <- ctx:
					default:
					}
				}
			})
			defer restore()
			key := struct{ name string }{"original-write-context"}
			marker := new(int)
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key, marker))
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if route == "merge-preparation" {
					done <- tx.PrepareMergeNode(ctx, nil, nil)
				} else {
					done <- tx.CommitContext(ctx)
				}
			}()
			var admittedContext context.Context
			select {
			case admittedContext = <-waited:
			case <-time.After(2 * time.Second):
				target.Unlock()
				held = false
				cancel()
				<-done
				t.Fatal("commit did not reach the held admission owner")
			}
			cancel()
			returnedBeforeUnlock := false
			var commitErr error
			select {
			case commitErr = <-done:
				returnedBeforeUnlock = true
			case <-time.After(2 * time.Second):
			}
			target.Unlock()
			held = false
			if !returnedBeforeUnlock {
				commitErr = <-done
			}
			require.True(t, returnedBeforeUnlock, "cancellation must return while the peer still owns admission")
			require.ErrorIs(t, commitErr, context.Canceled)
			require.NotNil(t, admittedContext)
			require.Same(t, marker, admittedContext.Value(key))
			require.Equal(t, ctx.Done(), admittedContext.Done())
			if route == "count" {
				_, err = b.GetStoredEdge("test:wait-e")
			} else {
				_, err = b.GetStoredNode("test:wait-n")
			}
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
}

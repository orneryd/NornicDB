package storage

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The request context is the real cancelCtx. The observer only identifies the
// actual schema wait inside the shared composite-index discovery owner.
func TestEvidenceFilledIndexAdmissionCancellation(t *testing.T) {
	for _, validation := range []bool{false, true} {
		t.Run(map[bool]string{false: "indexed MERGE admission", true: "composite transaction validation"}[validation], func(t *testing.T) {
			engine := createTestBadgerEngine(t)
			schema := engine.GetSchemaForNamespace("test")
			constraint := Constraint{Name: "tuple", Type: ConstraintNodeKey, Label: "Tuple", Properties: []string{"tenant", "id"}}
			require.NoError(t, schema.AddConstraint(constraint))
			idx, exists := schema.ConstraintPropertyIndex(constraint.Name)
			require.True(t, exists)
			require.NoError(t, schema.BackfillCompositeIndex(t.Context(), idx.Name, nil))
			properties := map[string]interface{}{"tenant": int64(7), "id": int64(42)}
			keys, err := schema.uniqueMergeKeys(t.Context(), []string{"Tuple"}, properties)
			require.NoError(t, err)
			require.Len(t, keys, 1)
			tx := beginMergeKeyTx(t, engine)
			t.Cleanup(func() { require.NoError(t, tx.Rollback()) })
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			waited := make(chan context.Context, 1)
			restore := setAdmissionWaitObserver(func(owner any, observed context.Context) {
				if owner != &schema.mu {
					return
				}
				pcs := make([]uintptr, 24)
				frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
				for {
					frame, more := frames.Next()
					if strings.HasSuffix(frame.Function, ".filledKeyIndex") {
						select {
						case waited <- observed:
						default:
						}
						return
					}
					if !more {
						return
					}
				}
			})
			t.Cleanup(restore)
			schema.mu.Lock()
			var release sync.Once
			done := make(chan error, 1)
			workerJoined := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				release.Do(schema.mu.Unlock)
				select {
				case <-workerJoined:
				case <-time.After(5 * time.Second):
					t.Error("request worker did not join after schema release")
				}
			})
			go func() {
				defer close(workerJoined)
				if validation {
					done <- tx.checkNodeKeyConstraint(ctx, &Node{ID: "test:new", Labels: []string{"Tuple"}, Properties: properties}, constraint)
					return
				}
				done <- tx.prepareMergeAdmissionKey(ctx, schema, "test", keys[0], properties, false)
			}()
			var admission context.Context
			select {
			case observed := <-waited:
				admission = observed
				t.Logf("actual filledKeyIndex schema waiter received caller context: %v (nil=%v)", observed == ctx, observed == nil)
			case <-time.After(5 * time.Second):
				release.Do(schema.mu.Unlock)
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("request did not join after releasing schema")
				}
				t.Fatal("request never reached filledKeyIndex schema admission")
			}
			cancel()
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Error("canceled request remained blocked behind schema writer in filledKeyIndex")
				release.Do(schema.mu.Unlock)
				select {
				case err := <-done:
					require.ErrorIs(t, err, context.Canceled)
				case <-time.After(5 * time.Second):
					t.Fatal("request did not join after schema release")
				}
			}
			select {
			case <-workerJoined:
			case <-time.After(5 * time.Second):
				t.Fatal("request worker did not join after returning its result")
			}
			require.True(t, admission == ctx, "filled index admission dropped the caller context")
			release.Do(schema.mu.Unlock)
			// A canceled discovery must not poison the healthy indexed route.
			if validation {
				require.NoError(t, tx.checkNodeKeyConstraint(t.Context(), &Node{ID: "test:new", Labels: []string{"Tuple"}, Properties: properties}, constraint))
			} else {
				require.NoError(t, tx.prepareMergeAdmissionKey(t.Context(), schema, "test", keys[0], properties, false))
			}
		})
	}
}

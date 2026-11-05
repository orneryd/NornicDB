package storage

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// This exercises a nonnil public PublishCommit context before publication,
// not context-free staging, detached Commit, or accepted maintenance work.
func TestPublicPublishCommitWarmPendingAdmissionCancellation(t *testing.T) {
	barrier := &writeConflictBarrier{reached: make(chan struct{}), resume: make(chan struct{})}
	engine, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour, Logger: slog.New(barrier),
	}))
	require.NoError(t, err)
	schema := engine.GetSchemaForNamespace("test")
	c := Constraint{Name: "tuple", Type: ConstraintNodeKey, Label: "Tuple", Properties: []string{"tenant", "id"}}
	require.NoError(t, schema.AddConstraint(c))
	idx, exists := schema.ConstraintPropertyIndex(c.Name)
	require.True(t, exists)
	require.NoError(t, schema.BackfillCompositeIndex(t.Context(), idx.Name, nil))
	w := engine.writeBehind
	require.NotNil(t, w)
	w.mu.RLock()
	warm := slices.Contains(w.trackedPairs, nodePropertyKey{label: "Tuple", property: "tenant"})
	w.mu.RUnlock()
	require.True(t, warm, "normal schema attachment must already track this pair")
	tx := beginMergeKeyTx(t, engine)
	t.Cleanup(func() {
		if err := tx.Rollback(); err != nil {
			require.ErrorIs(t, err, ErrTransactionClosed, "publication owns terminal transaction closure")
		}
	})
	require.NoError(t, tx.SetValidationSetting(DeferredConstraintValidation, true))
	require.NoError(t, tx.SetValidationSetting(SkipCreateCommitHeadReload, true))
	id := NodeID("test:" + uuid.NewString())
	_, err = tx.CreateMintedNode(t.Context(), &Node{ID: id, Labels: []string{"Tuple"}, Properties: map[string]interface{}{"tenant": int64(1), "id": int64(2)}})
	require.NoError(t, err)
	require.NoError(t, tx.SetMetadata(map[string]interface{}{"control": "warm pending admission"}))
	barrier.target = tx.ID
	ctx, cancel := context.WithCancel(t.Context())
	waited := make(chan context.Context, 1)
	restore := setAdmissionWaitObserver(func(owner any, observed context.Context) {
		if owner != &w.mu {
			return
		}
		pcs := make([]uintptr, 48)
		frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
		var names []string
		target := false
		for {
			f, more := frames.Next()
			names = append(names, f.Function)
			if strings.HasSuffix(f.Function, ".ensurePendingValueTracked") {
				target = true
			}
			if !more {
				break
			}
		}
		t.Logf("actual buffer admission ctx nil=%v target ensure=%v frames=%s", observed == nil, target, strings.Join(names, " -> "))
		if target {
			select {
			case waited <- observed:
			default:
			}
		}
	})
	defer restore()
	done := make(chan error, 1)
	realDone := make(chan struct{})
	launched := false
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(barrier.resume) }) }
	releaseBuffer := func() {}
	t.Cleanup(func() {
		cancel()
		resume()
		releaseBuffer()
		if launched {
			select {
			case <-realDone:
			case <-time.After(5 * time.Second):
				t.Error("public publication did not actually return during cleanup")
			}
		}
	})
	var published atomic.Bool
	var returnedAt atomic.Int64
	launched = true
	go func() {
		defer close(realDone)
		err := tx.PublishCommit(ctx, CommitPublication{AfterPublication: func() { published.Store(true) }})
		returnedAt.Store(time.Now().UnixNano())
		done <- err
	}()
	select {
	case <-barrier.reached:
	case err := <-done:
		t.Fatalf("public commit did not reach prepublication metadata boundary: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("public commit never reached metadata boundary")
	}
	// Metadata callback follows optimistic validation. Hold the real buffer
	// writer only now, so earlier contextual reads cannot mask the target wait.
	w.mu.Lock()
	releaseBuffer = sync.OnceFunc(w.mu.Unlock)
	resume()
	select {
	case observed := <-waited:
		t.Logf("warm ensurePendingValueTracked waiter carried actual request: %v", observed == ctx)
	case err := <-done:
		t.Fatalf("commit completed before warm pending wait: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not reach targeted warm pending admission")
	}
	canceledAt := time.Now()
	cancel()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	var result error
	select {
	case result = <-done:
		latency := time.Unix(0, returnedAt.Load()).Sub(canceledAt)
		t.Logf("actual cancellation to backend return while buffer writer remains held: %s", latency)
		require.LessOrEqual(t, latency, 100*time.Millisecond, "backend return measured from actual cancellation, writer still held")
	case <-time.After(100 * time.Millisecond):
		t.Error("canceled public prepublication request remained blocked on warm buffer admission")
		releaseBuffer()
		select {
		case result = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("public commit did not join after buffer release")
		}
	}
	releaseBuffer()
	select {
	case <-realDone:
	case <-time.After(5 * time.Second):
		t.Fatal("public commit goroutine did not actually return")
	}
	require.ErrorIs(t, result, context.Canceled)
	require.False(t, published.Load(), "canceled validation cannot publish or invoke callback")
	require.NoError(t, engine.FlushWriteBehind(t.Context()))
	_, err = engine.GetNode(t.Context(), id)
	require.ErrorIs(t, err, ErrNotFound, "canceled request must leave no buffered or backing node")
	retry := beginMergeKeyTx(t, engine)
	t.Cleanup(func() {
		if err := retry.Rollback(); err != nil {
			require.ErrorIs(t, err, ErrTransactionClosed, "successful publication owns terminal transaction closure")
		}
	})
	require.NoError(t, retry.SetValidationSetting(DeferredConstraintValidation, true))
	require.NoError(t, retry.SetValidationSetting(SkipCreateCommitHeadReload, true))
	_, err = retry.CreateMintedNode(t.Context(), &Node{ID: id, Labels: []string{"Tuple"}, Properties: map[string]interface{}{"tenant": int64(1), "id": int64(2)}})
	require.NoError(t, err)
	require.NoError(t, retry.PublishCommit(t.Context(), CommitPublication{}))
	require.NoError(t, engine.FlushWriteBehind(t.Context()))
	node, err := engine.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, id, node.ID)
}

// The cold owner is shared with schema maintenance. Cancellation must leave
// both the live value populations and their tracking metadata unchanged,
// including cancellation after an earlier pair completed in the candidate.
func TestPendingTrackingCancellationPublishesAtomically(t *testing.T) {
	pairs := []nodePropertyKey{{label: "Tuple", property: "a"}, {label: "Tuple", property: "b"}}
	for _, tc := range []struct {
		existing bool
		count    int
	}{{false, 1}, {false, 2}, {true, 1}, {true, 2}} {
		t.Run(fmt.Sprintf("existing=%v/pairs=%d", tc.existing, tc.count), func(t *testing.T) {
			pairs := pairs[:tc.count]
			fresh := func() *WriteBehindBuffer {
				w := &WriteBehindBuffer{active: newCommitBuffer()}
				for i := range 1024 {
					w.active.addNode(&Node{ID: NodeID(fmt.Sprint(i)), Labels: []string{"Tuple"}, Properties: map[string]interface{}{"a": int64(i), "b": int64(i), "unchanged": int64(i)}})
				}
				if tc.existing {
					require.NoError(t, w.trackPendingValues(t.Context(), []nodePropertyKey{{label: "Tuple", property: "unchanged"}}))
				}
				return w
			}
			// Measure this owner's deterministic poll population rather than
			// guessing a late cancellation offset from scheduler timing.
			probe := newIndexPollCancelContext(t, 1<<30)
			require.NoError(t, fresh().trackPendingValues(probe, pairs))
			polls := (1 << 30) - probe.remaining.Load()
			for _, at := range []int64{1, 32, polls - 4} {
				t.Run(fmt.Sprint(at), func(t *testing.T) {
					w := fresh()
					original := w.pendingIndex
					originalPairs := slices.Clone(w.trackedPairs)
					ctx := newIndexPollCancelContext(t, at)
					require.ErrorIs(t, w.trackPendingValues(ctx, pairs), context.Canceled)
					require.ErrorIs(t, ctx.Err(), context.Canceled)
					if original == nil {
						require.Nil(t, w.pendingIndex)
					} else {
						require.Same(t, original, w.pendingIndex)
					}
					require.Equal(t, originalPairs, w.trackedPairs)
					if original != nil {
						require.NotContains(t, original.tracked["Tuple"], "a")
						require.NotContains(t, original.tracked["Tuple"], "b")
						require.Len(t, original.byValue, 1024)
						require.Len(t, original.byProperty, 1)
						require.Len(t, original.byProperty[nodePropertyKey{label: "Tuple", property: "unchanged"}], 1024)
					}
					require.NoError(t, w.trackPendingValues(t.Context(), pairs))
					require.Len(t, w.pendingIndex.byProperty[pairs[0]], 1024)
					if len(pairs) > 1 {
						require.Len(t, w.pendingIndex.byProperty[pairs[1]], 1024)
					}
					key, indexed, err := indexValueKey(t.Context(), int64(17))
					require.NoError(t, err)
					require.True(t, indexed)
					require.Len(t, w.KeyCandidates("Tuple", "a", key), 1)
				})
			}
		})
	}
}

func BenchmarkWarmPendingTrackingAdmission(b *testing.B) {
	w := &WriteBehindBuffer{trackedPairs: []nodePropertyKey{{label: "Tuple", property: "a"}}}
	ctx := b.Context()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := w.ensurePendingValueTracked(ctx, "Tuple", "a"); err != nil {
			b.Fatal(err)
		}
	}
}

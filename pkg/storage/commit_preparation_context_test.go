package storage

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func prepareContextRetry(t *testing.T, b *BadgerEngine, id NodeID, value string) {
	t.Helper()
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	node, err := tx.GetNode(t.Context(), id)
	require.NoError(t, err)
	before := CopyNode(node)
	node.Properties["payload"] = value
	require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: before, After: node, Policy: ClientNodeEdits}))
	prepared, err := tx.PrepareEntityCommit(t.Context(), EntityCommitRequest{LogicalID: "healthy-" + value})
	require.NoError(t, err)
	t.Cleanup(prepared.Abort)
	receipt, err := b.SubmitEntityCommit(t.Context(), prepared, nil)
	require.NoError(t, err)
	outcome := receipt.WaitOutcome(t.Context())
	require.Equal(t, CommitApplied, outcome.State)
	require.NoError(t, outcome.Err)
	node, err = b.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, value, node.Properties["payload"])
}

func TestPrepareEntityCommitStopsCanceledUniqueClassifierAdmission(t *testing.T) {
	for _, plain := range []bool{false, true} {
		t.Run(fmt.Sprintf("plain=%t", plain), func(t *testing.T) {
			b := newTestEngine(t)
			schema := b.GetSchemaForNamespace("prep")
			require.NoError(t, schema.AddUniqueConstraint("doc_key", "Doc", "key"))
			id, err := b.CreateNode(t.Context(), &Node{ID: "prep:doc", Labels: []string{"Doc"}, Properties: map[string]interface{}{"key": "before", "payload": "before"}})
			require.NoError(t, err)
			tx, err := b.BeginTransaction(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			require.NoError(t, tx.SetValidationSetting(DeferredConstraintValidation, true))
			node, err := tx.GetNode(t.Context(), id)
			require.NoError(t, err)
			before := CopyNode(node)
			node.Properties["key"] = "claimed"
			node.Properties["payload"] = "loser"
			require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: before, After: node, Policy: ClientNodeEdits}))
			peer, err := b.GetNode(t.Context(), id)
			require.NoError(t, err)
			peer.Properties["key"] = "claimed"
			peer.Properties["payload"] = "peer"
			require.NoError(t, b.UpdateNode(t.Context(), NodeUpdateRequest{After: peer, Policy: ClientNodeEdits}))
			constraint := schema.uniqueConstraints[nodePropertyKey{label: "Doc", property: "key"}]
			require.NotNil(t, constraint)
			ctx, cancel := context.WithCancel(t.Context())
			type wait struct {
				context    context.Context
				classifier bool
			}
			blocked := make(chan wait, 1)
			t.Cleanup(setAdmissionWaitObserver(func(owner any, seen context.Context) {
				if owner != &constraint.mu {
					return
				}
				pcs := make([]uintptr, 32)
				n := runtime.Callers(2, pcs)
				frames := runtime.CallersFrames(pcs[:n])
				found := false
				for {
					frame, more := frames.Next()
					if strings.HasSuffix(frame.Function, ".classifyUpdateConflictAsUniqueViolation") {
						found = true
					}
					if !more {
						break
					}
				}
				select {
				case blocked <- wait{seen, found}:
				default:
				}
			}))
			constraint.mu.Lock()
			var once sync.Once
			unlock := func() { once.Do(constraint.mu.Unlock) }
			done := make(chan error, 1)
			joined := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				unlock()
				select {
				case <-joined:
				case <-time.After(5 * time.Second):
					t.Error("PrepareEntityCommit worker did not join")
				}
			})
			go func() {
				defer close(joined)
				var p *PreparedEntityCommit
				var err error
				if plain {
					err = tx.CommitContext(ctx)
				} else {
					p, err = tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "canceled-classifier"})
				}
				if p != nil {
					p.Abort()
				}
				done <- err
			}()
			select {
			case event := <-blocked:
				require.True(t, event.classifier, "must reach the real conflict-classification lookup")
			case <-time.After(5 * time.Second):
				t.Fatal("classifier did not reach held metadata admission")
			}
			cancel()
			var result error
			select {
			case result = <-done:
			case <-time.After(5 * time.Second):
				t.Error("canceled preparation stayed blocked at classifier metadata admission")
				unlock()
				select {
				case result = <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("PrepareEntityCommit did not finish after holder released")
				}
			}
			unlock()
			assert.ErrorIs(t, result, context.Canceled)
			actual, err := b.GetNode(t.Context(), id)
			require.NoError(t, err)
			assert.Equal(t, "peer", actual.Properties["payload"])
			assert.Equal(t, "claimed", actual.Properties["key"])
			count, err := b.NodeCount(t.Context())
			require.NoError(t, err)
			assert.Equal(t, int64(1), count)
			prepareContextRetry(t, b, id, "retry-classifier")
		})
	}
}

func TestPrepareEntityCommitStopsChangeImageCaptureAfterCancellation(t *testing.T) {
	b := newTestEngine(t)
	const population = 16
	for i := 0; i < population; i++ {
		_, err := b.CreateNode(t.Context(), &Node{ID: NodeID(fmt.Sprintf("capture:n%d", i)), Properties: map[string]interface{}{"payload": "before"}})
		require.NoError(t, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var reads, publications atomic.Int64
	var armed atomic.Bool
	require.NoError(t, b.InitializeStoredEntityObserver("capture", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
		return func(StoredEntityPublication) (func(), error) { return func() { publications.Add(1) }, nil }, nil
	}, storedReadTestBoundary{read: func(kind StoredEntityReadKind, id string, read func() error) error {
		err := read()
		if armed.Load() && kind == StoredEntityNodePriorRead {
			if reads.Add(1) == 1 {
				cancel()
			}
		}
		return err
	}}))
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	for i := 0; i < population; i++ {
		id := NodeID(fmt.Sprintf("capture:n%d", i))
		node, err := tx.GetNode(t.Context(), id)
		require.NoError(t, err)
		before := CopyNode(node)
		node.Properties["payload"] = "unaccepted"
		require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: before, After: node, Policy: ClientNodeEdits}))
	}
	armed.Store(true)
	prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "capture-canceled"})
	if prepared != nil {
		prepared.Abort()
	}
	armed.Store(false)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, prepared)
	assert.Equal(t, int64(1), reads.Load(), "owned per-key capture must stop after first real native read cancels its request")
	assert.Zero(t, publications.Load(), "canceled preparation must never publish")
	for i := 0; i < population; i++ {
		node, err := b.GetNode(t.Context(), NodeID(fmt.Sprintf("capture:n%d", i)))
		require.NoError(t, err)
		assert.Equal(t, "before", node.Properties["payload"])
	}
	prepareContextRetry(t, b, "capture:n0", "retry-capture")
	assert.Equal(t, int64(1), publications.Load())
}

// The native boundary runs a real prior-image read. Plain CommitContext still
// owns preparation here; a prepared command has already crossed admission.
func TestCommitPreparationChangeCaptureLifetime(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprintf("accepted=%t", accepted), func(t *testing.T) {
			b := newTestEngine(t)
			for i := range 8 {
				_, err := b.CreateNode(t.Context(), &Node{ID: NodeID(fmt.Sprintf("lifetime:n%d", i)), Properties: map[string]any{"payload": "before"}})
				require.NoError(t, err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var armed atomic.Bool
			var reads, publications atomic.Int64
			require.NoError(t, b.InitializeStoredEntityObserver("lifetime", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
				return func(StoredEntityPublication) (func(), error) { return func() { publications.Add(1) }, nil }, nil
			}, storedReadTestBoundary{read: func(kind StoredEntityReadKind, id string, read func() error) error {
				err := read()
				if armed.Load() && kind == StoredEntityNodePriorRead && reads.Add(1) == 1 {
					cancel()
				}
				return err
			}}))
			tx, err := b.BeginTransaction(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			for i := range 8 {
				node, err := tx.GetNode(t.Context(), NodeID(fmt.Sprintf("lifetime:n%d", i)))
				require.NoError(t, err)
				before := CopyNode(node)
				node.Properties["payload"] = "after"
				require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: before, After: node, Policy: ClientNodeEdits}))
			}
			if accepted {
				prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "accepted-lifetime"})
				require.NoError(t, err)
				t.Cleanup(prepared.Abort)
				armed.Store(true)
				receipt, err := b.SubmitEntityCommit(ctx, prepared, nil)
				require.NoError(t, err)
				waitCtx, waitCancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer waitCancel()
				outcome := receipt.WaitOutcome(waitCtx)
				require.Equal(t, CommitApplied, outcome.State)
				require.NoError(t, outcome.Err)
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				require.Equal(t, int64(8), reads.Load())
				require.Equal(t, int64(1), publications.Load())
			} else {
				armed.Store(true)
				err = tx.CommitContext(ctx)
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, int64(1), reads.Load())
				require.Zero(t, publications.Load())
			}
			armed.Store(false)
			for i := range 8 {
				node, err := b.GetNode(t.Context(), NodeID(fmt.Sprintf("lifetime:n%d", i)))
				require.NoError(t, err)
				want := "before"
				if accepted {
					want = "after"
				}
				require.Equal(t, want, node.Properties["payload"])
			}
			prepareContextRetry(t, b, "lifetime:n0", "healthy-lifetime")
		})
	}
}

// Public preparation includes real native validation and immutable handoff, but
// excludes caller staging and Abort. This is preparation cost, not commit throughput.
func BenchmarkPublicEntityCommitPreparation(b *testing.B) {
	for _, size := range []int{1, 64} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngine(b.TempDir()))
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := engine.Close(); err != nil {
					b.Error(err)
				}
			})
			schema := engine.GetSchemaForNamespace("bench")
			if err := schema.AddUniqueConstraint("key", "Doc", "key"); err != nil {
				b.Fatal(err)
			}
			for i := range size {
				if _, err := engine.CreateNode(b.Context(), &Node{ID: NodeID(fmt.Sprintf("bench:n%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"key": i, "payload": "before"}}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tx, err := engine.BeginTransaction(b.Context())
				if err != nil {
					b.Fatal(err)
				}
				for j := range size {
					node, err := tx.GetNode(b.Context(), NodeID(fmt.Sprintf("bench:n%d", j)))
					if err != nil {
						b.Fatal(err)
					}
					before := CopyNode(node)
					node.Properties["payload"] = "after"
					if err := tx.UpdateGraphNode(b.Context(), NodeUpdateRequest{Before: before, After: node, Policy: ClientNodeEdits}); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				prepared, err := tx.PrepareEntityCommit(b.Context(), EntityCommitRequest{LogicalID: fmt.Sprintf("bench-%d", i)})
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				prepared.Abort()
				b.StartTimer()
			}
		})
	}
}

// This observer cancels a real WithCancel context only while its owned phase is
// executing. It witnesses propagation inside the phase; it invents no Err value
// and does not treat a stack sampled later in storage as branch coverage.
type commitPreparationPhaseContext struct {
	context.Context
	cancel context.CancelFunc
	phase  string
	hits   atomic.Int64
}

func (c *commitPreparationPhaseContext) Err() error {
	var pcs [48]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Function, c.phase) {
			c.hits.Add(1)
			c.cancel()
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Err()
}
func TestCommitPreparationTemporalAndChangeKeyContext(t *testing.T) {
	for _, phase := range []string{".bufferTemporalIndexWrites", ".ofNode"} {
		for _, plain := range []bool{false, true} {
			if phase == ".ofNode" && !plain {
				continue
			}
			t.Run(fmt.Sprintf("%s/plain=%t", phase, plain), func(t *testing.T) {
				b := newTestEngine(t)
				schema := b.GetSchemaForNamespace("phase")
				base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
				require.NoError(t, schema.AddConstraint(Constraint{Name: "span", Type: ConstraintTemporal, EntityType: ConstraintEntityNode, Label: "Span", Properties: []string{"key", "from", "to"}}))
				if phase == ".ofNode" {
					b.SetChangeCapture("phase", ChangeCaptureFull)
				}
				_, err := b.CreateNode(t.Context(), &Node{ID: "phase:n", Labels: []string{"Span"}, Properties: map[string]any{"key": "n", "from": base, "to": base.Add(time.Hour), "payload": "before"}})
				require.NoError(t, err)
				tx, err := b.BeginTransaction(t.Context())
				require.NoError(t, err)
				t.Cleanup(func() { _ = tx.Rollback() })
				node, err := tx.GetNode(t.Context(), "phase:n")
				require.NoError(t, err)
				before := CopyNode(node)
				node.Properties["payload"] = "unaccepted"
				require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: before, After: node, Policy: ClientNodeEdits}))
				real, cancel := context.WithCancel(t.Context())
				defer cancel()
				ctx := &commitPreparationPhaseContext{Context: real, cancel: cancel, phase: phase}
				if plain {
					err = tx.CommitContext(ctx)
				} else {
					var p *PreparedEntityCommit
					p, err = tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "phase-canceled"})
					if p != nil {
						p.Abort()
					}
				}
				require.Positive(t, ctx.hits.Load(), "public preparation must reach actual owned phase with caller context")
				require.ErrorIs(t, err, context.Canceled)
				require.ErrorIs(t, real.Err(), context.Canceled)
				actual, err := b.GetNode(t.Context(), "phase:n")
				require.NoError(t, err)
				require.Equal(t, "before", actual.Properties["payload"])
				prepareContextRetry(t, b, "phase:n", "healthy-phase")
			})
		}
	}
}

func TestPrepareEntityCommitCaptureOverlayAdmissionCancellation(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	_, err := b.CreateNode(t.Context(), &Node{ID: "overlay:n", Properties: map[string]any{"payload": "before"}})
	require.NoError(t, err)
	var arm atomic.Bool
	held := make(chan struct{})
	var lockOnce, unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(b.writeBehind.mu.Unlock) }
	require.NoError(t, b.InitializeStoredEntityObserver("overlay", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
		return func(StoredEntityPublication) (func(), error) { return func() {}, nil }, nil
	}, storedReadTestBoundary{read: func(kind StoredEntityReadKind, id string, read func() error) error {
		if arm.Load() && kind == StoredEntityNodePriorRead {
			lockOnce.Do(func() { b.writeBehind.mu.Lock(); close(held) })
		}
		return read()
	}}))
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	node, err := tx.GetNode(t.Context(), "overlay:n")
	require.NoError(t, err)
	before := CopyNode(node)
	node.Properties["payload"] = "unaccepted"
	require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: before, After: node, Policy: ClientNodeEdits}))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	joined := make(chan struct{})
	entered := make(chan struct{}, 1)
	t.Cleanup(setAdmissionWaitObserver(func(owner any, seen context.Context) {
		if owner == &b.writeBehind.mu {
			select {
			case entered <- struct{}{}:
			default:
			}
		}
	}))
	// Before starting the worker, register release and its actual join. The boundary
	// owns the lock only once; cleanup must not unlock a lock never acquired.
	t.Cleanup(func() {
		cancel()
		arm.Store(false)
		select {
		case <-held:
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("capture did not reach release or join")
		}
		select {
		case <-held:
			unlock()
		default:
		}
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("capture worker did not join")
		}
	})
	arm.Store(true)
	go func() {
		defer close(joined)
		p, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "overlay-canceled"})
		if p != nil {
			p.Abort()
		}
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("capture did not reach held acknowledged overlay admission")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("capture overlay admission ignored cancellation")
	}
	arm.Store(false)
	unlock()
	actual, err := b.GetNode(t.Context(), "overlay:n")
	require.NoError(t, err)
	require.Equal(t, "before", actual.Properties["payload"])
	prepareContextRetry(t, b, "overlay:n", "healthy-overlay")
}

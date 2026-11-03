package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Temporal index maintenance belongs to the phase that owns it. A direct node
// write maintains the index inside its publishing transaction, before the
// commit finishes and before that write's publication is decided, so its
// constraint discovery and current-pointer refresh are preparation: they carry
// the write's own preparation lifetime (the context of the write
// context), exactly as a transaction commit's temporal preparation carries its
// commit's preparation context. A rebuild or label backfill prepares each
// stored node the same way, on the maintenance write's own lifetime. A canceled
// operation therefore stops at the admission it is waiting on instead of
// continuing that work on a lifetime nobody can cancel.

// temporalPreparationFixture opens an engine whose namespace schema carries the
// one temporal constraint these controls use.
func temporalPreparationFixture(t *testing.T) (*BadgerEngine, *NamespacedEngine, time.Time) {
	t.Helper()
	inner := openBackfillTestEngine(t, t.TempDir(), false)
	engine := NewNamespacedEngine(inner, "test")
	c := Constraint{Name: "period_history", Type: ConstraintTemporal, EntityType: ConstraintEntityNode, Label: "Period", Properties: []string{"key", "from", "to"}}
	require.NoError(t, engine.GetSchema().AddConstraint(c))
	return inner, engine, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
}

// periodNode is one stored interval of the fixture's temporal key.
func periodNode(id NodeID, start time.Time) *Node {
	return &Node{ID: id, Labels: []string{"Period"}, Properties: map[string]any{"key": "k", "from": start, "to": start.Add(24 * time.Hour)}}
}

// temporalVersionAt reports the node the temporal index holds for the fixture's
// key at the given instant.
func temporalVersionAt(t *testing.T, inner *BadgerEngine, asOf time.Time) NodeID {
	t.Helper()
	node, err := inner.GetTemporalNodeAsOfInNamespace(t.Context(), "test", "Period", "key", "k", "from", "to", asOf)
	require.NoError(t, err)
	require.NotNil(t, node, "the temporal index holds no version at %s", asOf)
	return node.ID
}

// canceledTemporalPreparation cancels the real request context only while the
// direct write's temporal index preparation is executing: the existing
// phase-context observer witnesses propagation inside that owned phase and
// invents no error value.
func canceledTemporalPreparation(t *testing.T) *commitPreparationPhaseContext {
	t.Helper()
	real, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return &commitPreparationPhaseContext{Context: real, cancel: cancel, phase: ".applyTemporalIndexesForNodeChangeInTxn"}
}

// TestTemporalRebuildCancellationStopsNodeIndexPreparation pins that a canceled
// rebuild stops at the real schema admission its node preparation waits on.
// The schedule is deterministic and uses the owners that already exist: the
// rebuild pauses at pauseTemporalRebuildNode, after the initial reset and
// before it indexes the node it just read; the control then holds the actual
// schema write lock and lets the node preparation reach its constraint
// discovery, cancels the original request and requires the rebuild to return
// its own cause while the lock is still held. The index stays unready with no
// format marker, and a later healthy rebuild publishes the stored nodes.
func TestTemporalRebuildCancellationStopsNodeIndexPreparation(t *testing.T) {
	inner, engine, from := temporalPreparationFixture(t)
	_, err := engine.CreateNode(t.Context(), periodNode("p1", from))
	require.NoError(t, err)
	schema := inner.GetSchemaForNamespace("test")

	// The rebuild pauses once, after it read a stored node and before that
	// node's index preparation.
	atPause, resume := make(chan struct{}), make(chan struct{})
	var pauseOnce, resumeOnce sync.Once
	resumeRebuild := func() { resumeOnce.Do(func() { close(resume) }) }
	defer resumeRebuild()
	restorePause := setMaintenancePauseHook(func(point string) {
		if point != pauseTemporalRebuildNode {
			return
		}
		pauseOnce.Do(func() {
			close(atPause)
			<-resume
		})
	})
	defer restorePause()

	// The observer reports the actual admission boundary each wait blocks on.
	waits := make(chan context.Context, 4)
	restoreObserver := setAdmissionWaitObserver(func(owner any, wait context.Context) {
		if owner != &schema.mu {
			return
		}
		select {
		case waits <- wait:
		default:
		}
	})
	defer restoreObserver()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- inner.RebuildTemporalIndexes(ctx) }()

	select {
	case <-atPause:
	case <-time.After(30 * time.Second):
		cancel()
		<-done
		t.Fatal("the rebuild never reached its node index preparation pause")
	}
	require.Zero(t, len(waits), "a schema admission was already waited on before the node preparation")
	// Hold the real schema write lock: the paused node's constraint discovery
	// must wait here.
	schema.mu.Lock()
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(schema.mu.Unlock) }
	defer unlock()
	resumeRebuild()

	var wait context.Context
	select {
	case wait = <-waits:
	case <-time.After(30 * time.Second):
		unlock()
		<-done
		t.Fatal("the paused node's constraint discovery never reached the schema admission")
	}
	cancel()

	var rebuildErr error
	returned := false
	select {
	case rebuildErr = <-done:
		returned = true
	case <-time.After(5 * time.Second):
	}
	unlock()
	if !returned {
		<-done
		t.Fatal("a canceled rebuild kept waiting on a schema admission that carried no caller lifetime")
	}
	require.ErrorIs(t, rebuildErr, context.Canceled)
	require.True(t, wait == ctx, "the node preparation waited on the schema admission with a lifetime that was not the caller's")

	// A canceled rebuild publishes neither the index nor its format marker: a
	// later clean shutdown must not certify the cleared index as current.
	require.False(t, inner.temporalIndexReady.Load(), "a canceled rebuild reported its index ready")
	current, err := inner.temporalIndexFormatIsCurrent()
	require.NoError(t, err)
	require.False(t, current, "a canceled rebuild certified the temporal index format")

	// A healthy rebuild over the same store publishes the stored nodes.
	require.NoError(t, inner.RebuildTemporalIndexes(context.Background()))
	require.True(t, inner.temporalIndexReady.Load())
	current, err = inner.temporalIndexFormatIsCurrent()
	require.NoError(t, err)
	require.True(t, current)
	require.NotEmpty(t, indexKeys(t, inner, prefixTemporalIndex, prefixTemporalHead))
	require.Equal(t, NodeID("test:p1"), temporalVersionAt(t, inner, from.Add(time.Hour)),
		"the healthy rebuild did not index the stored temporal node")
}

// TestTemporalNodeWritePreparationCancellationStopsPublicWrite pins that the
// three public direct node writers reach their temporal index preparation with
// the caller's lifetime. The request context is canceled only while that owned
// phase executes, so a canceled write must report its own cause and publish
// nothing, and a healthy retry must publish.
func TestTemporalNodeWritePreparationCancellationStopsPublicWrite(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		inner, engine, from := temporalPreparationFixture(t)
		ctx := canceledTemporalPreparation(t)
		_, err := engine.CreateNode(ctx, periodNode("p1", from))
		require.Positive(t, ctx.hits.Load(), "the canceled create never reached its temporal index preparation with the caller's lifetime")
		require.ErrorIs(t, err, context.Canceled)
		_, err = engine.GetNode(t.Context(), "p1")
		require.ErrorIs(t, err, ErrNotFound, "a canceled create published the node")

		_, err = engine.CreateNode(t.Context(), periodNode("p1", from))
		require.NoError(t, err)
		require.Equal(t, NodeID("test:p1"), temporalVersionAt(t, inner, from.Add(time.Hour)))
	})
	t.Run("update", func(t *testing.T) {
		inner, engine, from := temporalPreparationFixture(t)
		_, err := engine.CreateNode(t.Context(), periodNode("p1", from))
		require.NoError(t, err)
		ctx := canceledTemporalPreparation(t)
		err = engine.UpdateNode(ctx, NodeUpdateRequest{After: periodNode("p1", from.Add(48*time.Hour)), Policy: ClientNodeEdits})
		require.Positive(t, ctx.hits.Load(), "the canceled update never reached its temporal index preparation with the caller's lifetime")
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, NodeID("test:p1"), temporalVersionAt(t, inner, from.Add(time.Hour)),
			"a canceled update published its new interval")
		moved, err := inner.GetTemporalNodeAsOfInNamespace(t.Context(), "test", "Period", "key", "k", "from", "to", from.Add(49*time.Hour))
		require.NoError(t, err)
		require.Nil(t, moved, "a canceled update published its new interval")

		require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: periodNode("p1", from.Add(48*time.Hour)), Policy: ClientNodeEdits}))
		require.Equal(t, NodeID("test:p1"), temporalVersionAt(t, inner, from.Add(49*time.Hour)))
	})
	t.Run("delete", func(t *testing.T) {
		inner, engine, from := temporalPreparationFixture(t)
		_, err := engine.CreateNode(t.Context(), periodNode("p1", from))
		require.NoError(t, err)
		ctx := canceledTemporalPreparation(t)
		err = engine.DeleteNode(ctx, "p1")
		require.Positive(t, ctx.hits.Load(), "the canceled delete never reached its temporal index preparation with the caller's lifetime")
		require.ErrorIs(t, err, context.Canceled)
		_, err = engine.GetNode(t.Context(), "p1")
		require.NoError(t, err, "a canceled delete removed the node")
		require.Equal(t, NodeID("test:p1"), temporalVersionAt(t, inner, from.Add(time.Hour)),
			"a canceled delete removed the temporal index entry")

		require.NoError(t, engine.DeleteNode(t.Context(), "p1"))
		_, err = engine.GetNode(t.Context(), "p1")
		require.ErrorIs(t, err, ErrNotFound)
	})
}

// TestTemporalNodeWritePreparationPublishesAcceptedWrite pins the other
// direction: a direct node write that is not canceled still maintains the
// temporal index through the same preparation, so create, update and delete
// leave the history and current pointer exactly as a healthy rebuild does.
func TestTemporalNodeWritePreparationPublishesAcceptedWrite(t *testing.T) {
	inner, engine, from := temporalPreparationFixture(t)
	_, err := engine.CreateNode(t.Context(), periodNode("p1", from))
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), periodNode("p2", from.Add(48*time.Hour)))
	require.NoError(t, err)

	require.Equal(t, NodeID("test:p2"), temporalVersionAt(t, inner, from.Add(49*time.Hour)), "the later interval is not the current version")
	require.Equal(t, NodeID("test:p1"), temporalVersionAt(t, inner, from.Add(time.Hour)))

	require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{
		After:  periodNode("p2", from.Add(72*time.Hour)),
		Policy: ClientNodeEdits,
	}))
	require.Equal(t, NodeID("test:p2"), temporalVersionAt(t, inner, from.Add(73*time.Hour)), "the accepted update did not move the temporal index entry")

	require.NoError(t, engine.DeleteNode(t.Context(), "p2"))
	node, err := inner.GetTemporalNodeAsOfInNamespace(t.Context(), "test", "Period", "key", "k", "from", "to", from.Add(73*time.Hour))
	require.NoError(t, err)
	require.Nil(t, node, "the accepted delete left the temporal index entry behind")
	require.Equal(t, NodeID("test:p1"), temporalVersionAt(t, inner, from.Add(time.Hour)))

	// The write-maintained index matches the one a rebuild from the stored
	// records writes.
	published := indexKeys(t, inner, prefixTemporalIndex, prefixTemporalHead)
	require.NoError(t, inner.RebuildTemporalIndexes(context.Background()))
	require.Equal(t, published, indexKeys(t, inner, prefixTemporalIndex, prefixTemporalHead),
		"the write-maintained temporal index disagrees with the rebuilt one")
}

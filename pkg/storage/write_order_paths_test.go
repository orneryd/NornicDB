package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

// newDecayWriteBehindEngine is a write-behind engine whose "Doc" label decays
// below visibility within a second (labelChangeDecayFixture's binding).
func newDecayWriteBehindEngine(t *testing.T, dir string) *BadgerEngine {
	t.Helper()
	engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	engine.writeBehind.SetDelayBounds(time.Hour, time.Hour)
	engine.SetDecayEnabled(true)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name: "doc_decay", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds: 1, VisibilityThreshold: 0.10, ScoreFrom: knowledgepolicy.ScoreFromCreated,
		Enabled: true, DecayEnabled: true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name: "doc_binding", ProfileRef: "doc_decay", TargetLabels: []string{"Doc"},
	}))
	return engine
}

// The decay suppression rewrite rescores the newest body of the entity: run
// right after an acknowledged, unflushed relabel, it decides and stores what
// it decides after the flush.
func TestDecaySuppressionRescoresTheNewestBufferedBody(t *testing.T) {
	old := time.Now().Add(-72 * time.Hour)
	run := func(flushFirst bool) (bool, bool) {
		engine := newDecayWriteBehindEngine(t, t.TempDir())
		t.Cleanup(func() { _ = engine.Close() })
		_, err := engine.CreateNode(&Node{ID: "test:n", Labels: []string{"Untargeted"}, Properties: map[string]any{"v": "x"}, CreatedAt: old, UpdatedAt: old})
		require.NoError(t, err)
		require.NoError(t, engine.FlushWriteBehind())
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetImplicit(true))
		require.NoError(t, tx.UpdateGraphNode(&Node{ID: "test:n", Labels: []string{"Doc"}, Properties: map[string]any{"v": "x"}, CreatedAt: old, UpdatedAt: old}, ClientNodeEdits))
		require.NoError(t, tx.Commit())
		if flushFirst {
			require.NoError(t, engine.FlushWriteBehind())
		}
		became, err := engine.EnqueueDeindexIfSuppressed("test:n", false)
		require.NoError(t, err)
		require.NoError(t, engine.FlushWriteBehind())
		engine.invalidateCachesAfterRestore()
		stored, err := engine.GetStoredNode("test:n")
		require.NoError(t, err)
		require.Equal(t, []string{"Doc"}, stored.Labels)
		return became, stored.RuntimeMetadata().VisibilitySuppressed
	}
	becameAfterFlush, suppressedAfterFlush := run(true)
	became, suppressed := run(false)
	require.Equal(t, becameAfterFlush, became, "the rescore decides on the newest body")
	require.Equal(t, suppressedAfterFlush, suppressed, "and stores that decision")
}

// An explicit transaction ordered after an acknowledged commit of the same
// node moves onto Badger transactions opened after the drain; a property name
// it is the first to use moves with it and survives a restart.
func TestDrainedTransactionKeepsItsNewPropertyKeys(t *testing.T) {
	dir := t.TempDir()
	engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	engine.writeBehind.SetDelayBounds(time.Hour, time.Hour)
	_, err = engine.CreateNode(&Node{ID: "test:n", Properties: map[string]any{"v": "initial"}})
	require.NoError(t, err)
	require.NoError(t, engine.FlushWriteBehind())
	buffered, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, buffered.SetImplicit(true))
	require.NoError(t, buffered.UpdateGraphNode(&Node{ID: "test:n", Properties: map[string]any{"v": "acknowledged"}}, ClientNodeEdits))
	require.NoError(t, buffered.Commit())

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.UpdateGraphNode(&Node{ID: "test:n", Properties: map[string]any{"v": "explicit", "first_seen_here": "kept"}}, ClientNodeEdits))
	require.NoError(t, tx.Commit())
	require.NoError(t, engine.Close())

	reopened, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	stored, err := reopened.GetStoredNode("test:n")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"v": "explicit", "first_seen_here": "kept"}, stored.Properties)
}

// When the drain it needs fails, a write ordered after the acknowledged
// commits of its entities fails with it and changes nothing: no direct write,
// explicit transaction or suppression rewrite lands ahead of a commit that
// has not reached storage.
func TestOrderedWriteFailsWhenItsDrainFails(t *testing.T) {
	engine := newDecayWriteBehindEngine(t, t.TempDir())
	// The buffer's apply is wrapped once, before any commit signals the
	// flusher; failing is then switched atomically.
	injected := errors.New("injected flush failure")
	var failing atomic.Bool
	apply := engine.writeBehind.apply
	engine.writeBehind.apply = func(buf *CommitBuffer) error {
		if failing.Load() {
			return injected
		}
		return apply(buf)
	}
	t.Cleanup(func() {
		failing.Store(false)
		_ = engine.Close()
	})
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: id, Properties: map[string]any{"v": "initial"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"v": "initial"}}))
	require.NoError(t, engine.FlushWriteBehind())

	failing.Store(true)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.UpdateGraphNode(&Node{ID: "test:a", Properties: map[string]any{"v": "acknowledged"}}, ClientNodeEdits))
	require.NoError(t, tx.UpdateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"v": "acknowledged"}}))
	require.NoError(t, tx.DeleteGraphNode("test:b", CascadeEdges))
	require.NoError(t, tx.Commit())

	writes := map[string]func() error{
		"create node": func() error { _, err := engine.CreateNode(&Node{ID: "test:b"}); return err },
		"update node": func() error { return engine.UpdateNode(&Node{ID: "test:a", Properties: map[string]any{"v": "direct"}}) },
		"delete node": func() error { return engine.DeleteNode("test:a") },
		"create edge": func() error {
			return engine.CreateEdge(&Edge{ID: "test:s", StartNode: "test:a", EndNode: "test:a", Type: "LINK"})
		},
		"update edge": func() error {
			return engine.UpdateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK"})
		},
		"delete edge": func() error { return engine.DeleteEdge("test:r") },
		"suppression": func() error { _, err := engine.EnqueueDeindexIfSuppressed("test:a", false); return err },
		"explicit tx commit": func() error {
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			if err := tx.UpdateGraphNode(&Node{ID: "test:a", Properties: map[string]any{"v": "explicit"}}, ClientNodeEdits); err != nil {
				_ = tx.Rollback()
				return err
			}
			return tx.Commit()
		},
	}
	for name, write := range writes {
		require.ErrorIs(t, write(), injected, name)
	}
	stored, err := engine.GetStoredNode("test:a")
	require.NoError(t, err)
	require.Equal(t, "acknowledged", stored.Properties["v"], "no failed write landed")
	_, err = engine.GetStoredEdge("test:s")
	require.ErrorIs(t, err, ErrNotFound)

	failing.Store(false)
	require.NoError(t, engine.FlushWriteBehind())
	engine.invalidateCachesAfterRestore()
	stored, err = engine.GetStoredNode("test:a")
	require.NoError(t, err)
	require.Equal(t, "acknowledged", stored.Properties["v"])
	_, err = engine.GetStoredNode("test:b")
	require.ErrorIs(t, err, ErrNotFound)
}

// The write order's drain returns once the generations rotated before it
// started are retired; a generation rotated while it runs is left to later
// drains, so a writer holding stripes never chases newer commits. A public
// flush still drains everything.
func TestOrderDrainStopsAtItsFrontier(t *testing.T) {
	var applied atomic.Int32
	var later sync.Once
	var buf *WriteBehindBuffer
	buf = NewWriteBehindBuffer(time.Hour, 0, func(*CommitBuffer) error {
		later.Do(func() {
			buf.AppendNode(&Node{ID: "test:later"})
			buf.rotate()
		})
		applied.Add(1)
		return nil
	})
	// No background flusher: only these drains apply generations.
	buf.Close()
	buf.AppendNode(&Node{ID: "test:first"})
	require.NoError(t, buf.flush(false, true))
	require.Equal(t, int32(1), applied.Load())
	_, found, _ := buf.GetNode("test:later")
	require.True(t, found, "the later generation is still buffered")
	require.NoError(t, buf.Flush())
	require.Equal(t, int32(2), applied.Load())
	require.NoError(t, buf.flush(false, true), "nothing to drain")
}

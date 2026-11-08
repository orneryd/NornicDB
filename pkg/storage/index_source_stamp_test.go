package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func indexSourceStamp(t *testing.T, source IndexSourceStampedView) string {
	t.Helper()
	stamp, err := source.IndexSourceStamp(t.Context())
	require.NoError(t, err)
	require.Len(t, stamp, 64)
	return stamp
}

// Every source representation consumed by population must invalidate a saved
// proof, even when its entity count and vector dimensions stay unchanged.
func TestIndexSourceStampCommittedInputs(t *testing.T) {
	engine := newReadSnapshotTestEngine(t, false)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": "old"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}))
	mutations := []struct {
		name  string
		apply func() error
	}{
		{"body", func() error {
			node, err := engine.GetNode(t.Context(), "test:a")
			if err != nil {
				return err
			}
			node.Properties["text"] = "new"
			return engine.UpdateNode(t.Context(), NodeUpdateRequest{After: node})
		}},
		{"labels", func() error {
			node, err := engine.GetNode(t.Context(), "test:a")
			if err != nil {
				return err
			}
			node.Labels = []string{"Other"}
			return engine.UpdateNode(t.Context(), NodeUpdateRequest{After: node})
		}},
		{"embedding-sidecar", func() error {
			node, err := engine.GetNode(t.Context(), "test:a")
			if err != nil {
				return err
			}
			node.ChunkEmbeddings = [][]float32{{0, 1, 0, 0}}
			return engine.UpdateNodeEmbeddingSidecar(t.Context(), node)
		}},
		{"relationship-body", func() error {
			edge, err := engine.GetEdge(t.Context(), "test:r")
			if err != nil {
				return err
			}
			edge.Properties = map[string]any{"text": "changed"}
			return engine.UpdateEdge(t.Context(), EdgeUpdateRequest{After: edge})
		}},
		{"delete-and-recreate-same-count", func() error {
			if err := engine.DeleteNode(t.Context(), "test:b"); err != nil {
				return err
			}
			_, err := engine.CreateNode(t.Context(), &Node{ID: "test:b", Labels: []string{"Doc"}, Properties: map[string]any{"text": "replacement"}})
			return err
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			before := indexSourceStamp(t, engine)
			require.NoError(t, mutation.apply())
			require.NotEqual(t, before, indexSourceStamp(t, engine))
		})
	}
	first, second := NewNamespacedEngine(engine, "first"), NewNamespacedEngine(engine, "second")
	require.NotEqual(t, indexSourceStamp(t, first), indexSourceStamp(t, second), "proofs cannot cross database scopes")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := engine.IndexSourceStamp(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestIndexSourceStampCaptureReopenRestore(t *testing.T) {
	dir := t.TempDir()
	open := func() *BadgerEngine {
		engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true}))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, engine.Close()) })
		return engine
	}
	engine := open()
	_, err := engine.CreateNode(t.Context(), &Node{ID: "test:a", Properties: map[string]any{"text": "baseline"}})
	require.NoError(t, err)
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{Name: "doc_key", Type: ConstraintUnique, Label: "Doc", Properties: []string{"key"}}))
	before := indexSourceStamp(t, engine)
	view, err := engine.OpenReadSnapshot(t.Context())
	require.NoError(t, err)
	stamped := view.(IndexSourceStampedView)
	require.Equal(t, before, indexSourceStamp(t, stamped))
	node, err := engine.GetNode(t.Context(), "test:a")
	require.NoError(t, err)
	node.Properties["text"] = "after"
	require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: node}))
	require.Equal(t, before, indexSourceStamp(t, stamped), "the proof belongs to the captured committed view")
	after := indexSourceStamp(t, engine)
	require.NotEqual(t, before, after)
	view.Close()
	for range 3 {
		require.NoError(t, engine.Close())
		engine = open()
		require.Equal(t, after, indexSourceStamp(t, engine), "opening with a nonempty schema and no source writes keeps its proof")
	}
	backup := filepath.Join(t.TempDir(), "backup.bin")
	require.NoError(t, engine.Backup(backup))
	require.NoError(t, engine.Restore(backup))
	require.NotEqual(t, after, indexSourceStamp(t, engine), "restore rotates source authority even at equal contents")
}

func TestIndexSourceStampPendingAndIndependentWriters(t *testing.T) {
	t.Run("uncommitted-overlay", func(t *testing.T) {
		engine := newReadSnapshotTestEngine(t, true)
		bufferedCreate(t, engine, &Node{ID: "test:a", Properties: map[string]any{"text": "buffered"}})
		_, err := engine.IndexSourceStamp(t.Context())
		require.ErrorIs(t, err, ErrReadSnapshotUnsupported, "an acknowledged overlay cannot be certified by physical versions")
		require.NoError(t, engine.FlushWriteBehind(t.Context()))
		_ = indexSourceStamp(t, engine)
	})
	t.Run("independent-writes", func(t *testing.T) {
		engine := newReadSnapshotTestEngine(t, false)
		const writers = 8
		var start sync.WaitGroup
		start.Add(1)
		errors := make(chan error, writers)
		for worker := range writers {
			go func() {
				start.Wait()
				_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(fmt.Sprintf("test:writer-%d", worker)), Properties: map[string]any{"text": "independent"}})
				errors <- err
			}()
		}
		start.Done()
		for range writers {
			require.NoError(t, <-errors, "the blind source identity must not introduce cross-writer conflicts")
		}
		count, err := engine.NodeCount(t.Context())
		require.NoError(t, err)
		require.Equal(t, int64(writers), count)
		_ = indexSourceStamp(t, engine)
	})
}

// The same derived maintenance must retain its publication policy when the
// lifecycle supplies a raw DB or a held restore read instead of engine admission.
func TestIndexSourceStampDerivedMaintenanceLifecycle(t *testing.T) {
	for _, lifecycle := range []string{"engine", "owned-db", "held-db"} {
		t.Run(lifecycle, func(t *testing.T) {
			engine := newReadSnapshotTestEngine(t, false)
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": "baseline"}})
				require.NoError(t, err)
			}
			require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:r", Type: "REL", StartNode: "test:a", EndNode: "test:b"}))
			for _, kind := range []byte{countDeltaKindLabel, countDeltaKindEdgeType} {
				pending, err := engine.hasAnyCountDeltasInDB(kind)
				require.NoError(t, err)
				require.True(t, pending, "the fixture must enter a real maintenance transaction")
			}
			before := indexSourceStamp(t, engine)
			source := countFoldSource{}
			release := func() {}
			if lifecycle != "engine" {
				source.db = engine.db
			}
			if lifecycle == "held-db" {
				require.True(t, engine.db.oracle.holdReads(time.Second))
				source.held = true
				release = sync.OnceFunc(engine.db.oracle.releaseReads)
				defer release()
			}
			require.NoError(t, engine.foldCountDeltasOn(t.Context(), source, countDeltaKindLabel, labelCountKey, "", ""))
			require.NoError(t, engine.foldCountDeltasOn(t.Context(), source, countDeltaKindEdgeType, edgeTypeCountKey, "", ""))
			release()
			require.Equal(t, before, indexSourceStamp(t, engine), "derived count maintenance must not change graph-source identity")
			for _, kind := range []byte{countDeltaKindLabel, countDeltaKindEdgeType} {
				pending, err := engine.hasAnyCountDeltasInDB(kind)
				require.NoError(t, err)
				require.False(t, pending, "both count kinds must actually have been folded")
			}
			namespaced := NewNamespacedEngine(engine, "test")
			nodes, err := namespaced.GetNodesByLabel(t.Context(), "Doc")
			require.NoError(t, err)
			require.Len(t, nodes, 2)
			edges, err := namespaced.GetEdgesByType(t.Context(), "REL")
			require.NoError(t, err)
			require.Len(t, edges, 1)
			node, err := engine.GetNode(t.Context(), "test:a")
			require.NoError(t, err)
			node.Properties["text"] = "real source change"
			require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: node}))
			require.NotEqual(t, before, indexSourceStamp(t, engine), "ordinary graph writes remain admission-relevant")
		})
	}
}

// Embedding Engine must not implicitly certify a transformed view just because
// its backing store has a certificate. The capability is optional and explicit.
type unprovenIndexSourceView struct{ Engine }

func TestIndexSourceStampRequiresExplicitViewCapability(t *testing.T) {
	engine := newReadSnapshotTestEngine(t, false)
	_, err := engine.CreateNode(t.Context(), &Node{ID: "test:capability"})
	require.NoError(t, err)
	stamp, err := ReadIndexSourceStamp(t.Context(), engine)
	require.NoError(t, err)
	require.Len(t, stamp, 64)
	_, err = ReadIndexSourceStamp(t.Context(), unprovenIndexSourceView{Engine: engine})
	require.ErrorIs(t, err, ErrReadSnapshotUnsupported)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = ReadIndexSourceStamp(ctx, engine)
	require.ErrorIs(t, err, context.Canceled)
}

func TestIndexSourceStampNativeMetadataChanges(t *testing.T) {
	engine := newReadSnapshotTestEngine(t, false)
	_, err := engine.CreateNode(t.Context(), &Node{ID: "test:source", Labels: []string{"Doc"}})
	require.NoError(t, err)
	before := indexSourceStamp(t, engine)
	require.NoError(t, engine.SetManualSuppression(t.Context(), StoredEntityNodeKind, "test:source", true))
	_, err = engine.IndexSourceStamp(t.Context())
	require.ErrorIs(t, err, ErrReadSnapshotUnsupported, "live reveal can change suppressed rows without a stored write")
	require.NoError(t, engine.SetManualSuppression(t.Context(), StoredEntityNodeKind, "test:source", false))
	require.NotEqual(t, before, indexSourceStamp(t, engine), "native visibility changes invalidate population proof")
	before = indexSourceStamp(t, engine)
	require.NoError(t, engine.persistSchemaDefinition("test", &SchemaDefinition{}))
	require.NotEqual(t, before, indexSourceStamp(t, engine), "direct schema callbacks invalidate population proof")
}

func TestIndexSourceStampSuppressionABA(t *testing.T) {
	engine := newReadSnapshotTestEngine(t, false)
	_, err := engine.CreateNode(t.Context(), &Node{ID: "test:source"})
	require.NoError(t, err)
	snapshot, err := engine.OpenReadSnapshot(t.Context())
	require.NoError(t, err)
	defer snapshot.Close()
	before := indexSourceStamp(t, snapshot.(IndexSourceStampedView))
	require.NoError(t, engine.SetManualSuppression(t.Context(), StoredEntityNodeKind, "test:source", true))
	require.NoError(t, engine.SetManualSuppression(t.Context(), StoredEntityNodeKind, "test:source", false))
	require.Equal(t, before, indexSourceStamp(t, snapshot.(IndexSourceStampedView)))
	require.NotEqual(t, before, indexSourceStamp(t, engine), "restored visibility must not hide an intervening suppression")
}

func TestIndexSourceStampDeclinesDynamicVisibility(t *testing.T) {
	engine := newReadSnapshotTestEngine(t, false)
	engine.SetDecayEnabled(true)
	_, err := engine.IndexSourceStamp(t.Context())
	require.ErrorIs(t, err, ErrReadSnapshotUnsupported)
	engine.SetDecayEnabled(false)
	engine.SetRevealAll(true)
	_, err = engine.IndexSourceStamp(t.Context())
	require.ErrorIs(t, err, ErrReadSnapshotUnsupported)
	engine.SetRevealAll(false)
	require.NotEmpty(t, indexSourceStamp(t, engine))
}

func TestIndexSourceStampPhysicalPurge(t *testing.T) {
	engine := newReadSnapshotTestEngine(t, false)
	_, err := engine.CreateNode(t.Context(), &Node{ID: "test:source"})
	require.NoError(t, err)
	before := indexSourceStamp(t, engine)
	require.NoError(t, engine.db.DropPrefix(t.Context(), prefixDropOptions{mode: purgePrefixVersions}, []byte{prefixNode}))
	require.NotEqual(t, before, indexSourceStamp(t, engine))
}

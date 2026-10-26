package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

// A snapshot holds what storage holds: the relationships that on-demand
// suppression or knowledge-policy decay hide from reads are stored data too,
// so recovering from the snapshot (after corruption, the WAL truncated)
// restores them with everything else. Both writers are covered: the
// WAL-compaction stream (SaveStreamingSnapshot) and the materialized snapshot
// (WAL.CreateSnapshot, SaveSnapshot).
func TestSnapshotRecoveryKeepsHiddenRelationships(t *testing.T) {
	source := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = source.Close() })
	source.SetDecayEnabled(true)
	bindings, err := knowledgepolicy.BuildBindingTable(
		map[string]*knowledgepolicy.DecayProfileBundle{"stale": {
			Name: "stale", Scope: knowledgepolicy.ScopeEdge, Function: knowledgepolicy.DecayFunctionExponential,
			HalfLifeSeconds: 3600, VisibilityThreshold: 0.5, ScoreFrom: knowledgepolicy.ScoreFromCreated,
			Enabled: true, DecayEnabled: true,
		}},
		map[string]*knowledgepolicy.DecayProfileBinding{"stale_edges": {
			Name: "stale_edges", ProfileRef: "stale", TargetEdgeType: "DECAYED", IsEdge: true,
		}},
		nil, nil,
	)
	require.NoError(t, err)
	source.GetSchemaForNamespace("test").SetBindingTable(bindings)

	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := source.CreateNode(&Node{ID: id, Labels: []string{"Doc"}})
		require.NoError(t, err)
	}
	old := time.Now().Add(-30 * 24 * time.Hour)
	for _, edge := range []*Edge{
		{ID: "test:suppressed", StartNode: "test:a", EndNode: "test:b", Type: "CITES"},
		{ID: "test:decayed", StartNode: "test:b", EndNode: "test:a", Type: "DECAYED", CreatedAt: old, UpdatedAt: old},
		{ID: "test:visible", StartNode: "test:a", EndNode: "test:b", Type: "LINKS"},
	} {
		require.NoError(t, source.CreateEdge(edge))
	}
	require.NoError(t, source.SetManualSuppression(StoredEntityEdgeKind, "test:suppressed", true))
	for _, id := range []EdgeID{"test:suppressed", "test:decayed"} {
		_, err := source.GetEdge(id)
		require.ErrorIs(t, err, ErrNotFound, "%s is hidden from reads", id)
	}

	dir := t.TempDir()
	streaming := filepath.Join(dir, "streaming.nds")
	require.NoError(t, SaveStreamingSnapshot(context.Background(), source, streaming, SnapshotOptions{Sequence: 1}))
	wal, err := NewWAL(filepath.Join(dir, "wal"), &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	snapshot, err := wal.CreateSnapshot(source)
	require.NoError(t, err)
	require.NoError(t, wal.Close())
	materialized := filepath.Join(dir, "materialized.nds")
	require.NoError(t, SaveSnapshot(snapshot, materialized))

	for name, path := range map[string]string{"streaming": streaming, "materialized": materialized} {
		t.Run(name, func(t *testing.T) {
			recovered := newTestMemoryEngine(t)
			t.Cleanup(func() { _ = recovered.Close() })
			_, _, err := RecoverIntoEngine(recovered, t.TempDir(), path)
			require.NoError(t, err)
			for _, id := range []EdgeID{"test:suppressed", "test:decayed", "test:visible"} {
				_, err := recovered.GetEdge(id)
				require.NoError(t, err, "%s survives recovery from the snapshot", id)
			}
			count, err := recovered.EdgeCount()
			require.NoError(t, err)
			require.EqualValues(t, 3, count)
		})
	}
}

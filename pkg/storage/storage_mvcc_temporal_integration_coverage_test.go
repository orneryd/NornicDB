package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStorage_MVCCTemporal_IntegrationCoverageScenario(t *testing.T) {
	engine := newTestEngine(t)

	// Enable temporal paths.
	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(Constraint{
		Name:       "role_temporal",
		Type:       ConstraintTemporal,
		EntityType: ConstraintEntityNode,
		Label:      "Role",
		Properties: []string{"entity_id", "valid_from", "valid_to"},
	}))

	// Tx create phase.
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(t.Context(), &Node{ID: "test:a", Labels: []string{"User", "Role"}, Properties: map[string]any{"entity_id": "u1", "valid_from": time.Now().Add(-2 * time.Hour), "valid_to": time.Now().Add(2 * time.Hour), "name": "Alice"}})
	require.NoError(t, err)
	_, err = tx.CreateNode(t.Context(), &Node{ID: "test:b", Labels: []string{"User"}, Properties: map[string]any{"name": "Bob"}})
	require.NoError(t, err)
	_, err = tx.CreateNode(t.Context(), &Node{ID: "test:c", Labels: []string{"User"}, Properties: map[string]any{"name": "Carol"}})
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(t.Context(), &Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "KNOWS", Properties: map[string]any{"w": int64(1)}}))
	require.NoError(t, tx.CreateEdge(t.Context(), &Edge{ID: "test:e2", StartNode: "test:b", EndNode: "test:c", Type: "KNOWS", Properties: map[string]any{"w": int64(2)}}))
	require.NoError(t, tx.Commit())

	// Force archive path for updates/deletes.
	engine.activeMVCCSnapshotReaders.Store(1)

	// Tx update phase.
	tx2, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx2.SetNamespace("test"))
	nodeA, err := tx2.GetNode(t.Context(), "test:a")
	require.NoError(t, err)
	nodeA.Properties["name"] = "Alice-updated"
	require.NoError(t, tx2.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: nodeA, Policy: ClientNodeEdits}))
	edge1, err := tx2.GetEdge(t.Context(), "test:e1")
	require.NoError(t, err)
	edge1.Properties["w"] = int64(3)
	require.NoError(t, tx2.UpdateEdge(t.Context(), EdgeUpdateRequest{After: edge1}))
	require.NoError(t, tx2.Commit())

	// Tx delete phase.
	tx3, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx3.SetNamespace("test"))
	require.NoError(t, tx3.DeleteEdge(t.Context(), "test:e2"))
	require.NoError(t, tx3.DeleteGraphNode(t.Context(), "test:c", CascadeEdges))
	require.NoError(t, tx3.Commit())

	engine.activeMVCCSnapshotReaders.Store(0)

	// Exercise visibility APIs.
	_, _ = engine.GetNodeLatestVisible(t.Context(), "test:a")
	_, _ = engine.GetNodeVisibleAt(t.Context(), "test:a", MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 999})
	_, _ = engine.GetEdgeLatestVisible(t.Context(), "test:e1")
	_, _ = engine.GetEdgeVisibleAt(t.Context(), "test:e1", MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 999})
	_, _ = engine.GetNodesByLabelVisibleAt(context.Background(), "User", MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 999})
	_, _ = engine.GetEdgesByTypeVisibleAt(context.Background(), "KNOWS", MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 999})
	_, _ = engine.GetEdgesBetweenVisibleAt(context.Background(), "test:a", "test:b", MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 999})

	_, _ = engine.BatchGetNodesLatestVisible(context.Background(), []NodeID{"test:a", "test:b", "test:c"})
	require.NoError(t, engine.IterateLatestVisibleNodes(context.Background(), func(n *Node) error { return nil }))
	require.NoError(t, engine.IterateLatestVisibleEdges(func(e *Edge) error { return nil }))

	// Rebuild/prune paths.
	require.NoError(t, engine.RebuildMVCCHeads(context.Background()))
	_, err = engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1})
	require.NoError(t, err)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = engine.PruneMVCCVersions(cancelled, MVCCPruneOptions{MaxVersionsPerKey: 1})
	require.Error(t, err)

	// Temporal index rebuild and prune.
	require.NoError(t, engine.RebuildTemporalIndexes(context.Background()))
	_, err = engine.GetTemporalNodeAsOfInNamespace(context.Background(), "test", "Role", "entity_id", "u1", "valid_from", "valid_to", time.Now().UTC())
	require.NoError(t, err)
	_, err = engine.PruneTemporalHistory(context.Background(), TemporalPruneOptions{MaxVersionsPerKey: 1, MinRetentionAge: time.Nanosecond})
	require.NoError(t, err)

	// Conflict helper calls.
	tx4, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx4.SetNamespace("test"))
	tx4.readTS = MVCCVersion{CommitTimestamp: time.Unix(0, 0).UTC(), CommitSequence: 0}
	_ = tx4.checkNodeCreateConflict(context.Background(), "test:a")
	_ = tx4.checkNodeWriteConflict(t.Context(), "test:a")
	_ = tx4.checkEdgeWriteConflict(context.Background(), "test:e1", true)
	_ = tx4.checkEdgeWriteConflict(context.Background(), "test:e1", true)
	_ = tx4.checkEdgeEndpointConflicts(context.Background(), &Edge{ID: "test:new", StartNode: "test:a", EndNode: "test:b", Type: "KNOWS"})
	_ = tx4.Rollback()
}

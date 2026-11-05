package storage

import (
	"fmt"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

func TestPreparedPolicyRewriteConflictBeforeAcceptance(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, implicit := range []bool{false, true} {
			for _, operation := range []string{"node-update", "node-delete", "node-cascade", "edge-update", "edge-delete", "edge-create"} {
				t.Run(fmt.Sprintf("buffered=%v/implicit=%v/%s", buffered, implicit, operation), func(t *testing.T) {
					testPreparedPolicyRewriteConflict(t, buffered, implicit, operation)
				})
			}
		}
	}
}

func testPreparedPolicyRewriteConflict(t *testing.T, buffered, implicit bool, operation string) {
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	b.SetDecayEnabled(true)
	schema := b.GetSchemaForNamespace("test")
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name: "stale", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds: 1, VisibilityThreshold: .1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "stale", ProfileRef: "stale", TargetLabels: []string{"Doc"}}))
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name: "stale_edge", Scope: knowledgepolicy.ScopeEdge, Function: knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds: 1, VisibilityThreshold: .1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "stale_edge", ProfileRef: "stale_edge", TargetEdgeType: "REL", IsEdge: true}))
	old := time.Now().Add(-72 * time.Hour)
	_, err = b.CreateNode(t.Context(), &Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"payload": "original"}, CreatedAt: old, UpdatedAt: old})
	require.NoError(t, err)
	_, err = b.CreateNode(t.Context(), &Node{ID: "test:b"})
	require.NoError(t, err)
	edge := &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "REL", Properties: map[string]any{"payload": "original"}, CreatedAt: old, UpdatedAt: old}
	if operation != "node-delete" && operation != "node-update" && operation != "edge-create" {
		require.NoError(t, b.CreateEdge(t.Context(), edge))
	}
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetImplicit(implicit))
	switch operation {
	case "node-update":
		node, err := b.GetStoredNode(t.Context(), "test:a")
		require.NoError(t, err)
		node.Properties["payload"] = "changed"
		require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
	case "node-delete", "node-cascade":
		require.NoError(t, tx.DeleteGraphNode(t.Context(), "test:a", CascadeEdges))
	case "edge-update":
		edge.Properties["payload"] = "changed"
		require.NoError(t, tx.UpdateEdge(t.Context(), EdgeUpdateRequest{After: edge}))
	case "edge-delete":
		require.NoError(t, tx.DeleteEdge(t.Context(), edge.ID))
	case "edge-create":
		require.NoError(t, tx.CreateEdge(t.Context(), edge))
	}
	isEdge := operation == "edge-update" || operation == "edge-delete" || operation == "node-cascade"
	entityID := "test:a"
	if isEdge {
		entityID = "test:r"
	}
	beforeHead, err := b.GetNodeCurrentHead(t.Context(), "test:a")
	require.NoError(t, err)
	changed, err := b.EnqueueDeindexIfSuppressed(t.Context(), entityID, isEdge)
	require.NoError(t, err)
	require.True(t, changed)
	afterHead, err := b.GetNodeCurrentHead(t.Context(), "test:a")
	require.NoError(t, err)
	require.Equal(t, beforeHead, afterHead, "policy changes the physical body, not the logical user head")
	prepared, err := tx.PrepareEntityCommit(t.Context(), EntityCommitRequest{LogicalID: tx.ID})
	if prepared != nil {
		defer prepared.Abort()
	}
	if operation == "edge-create" || buffered && implicit {
		require.NoError(t, err, "body-only policy changes do not invalidate untouched endpoints or buffered ACKs")
		require.NotNil(t, prepared)
		require.NoError(t, prepared.Commit(nil), "accepted preparation must not discover the prior physical policy conflict")
		require.Equal(t, buffered && implicit, tx.AcknowledgedBuffered())
		if operation == "edge-create" {
			stored, err := b.GetStoredEdge(t.Context(), edge.ID)
			require.NoError(t, err)
			require.Equal(t, "original", stored.Properties["payload"])
			node, err := b.GetStoredNode(t.Context(), "test:a")
			require.NoError(t, err)
			require.True(t, node.RuntimeMetadata().VisibilitySuppressed, "creating an edge must not overwrite endpoint policy metadata")
		}
		return
	}
	require.ErrorIs(t, err, ErrConflict, "the native physical conflict must be rejected before durable acceptance")
	require.Nil(t, prepared)
	require.Equal(t, TxStatusRolledBack, tx.Status)
	node, err := b.GetStoredNode(t.Context(), "test:a")
	require.NoError(t, err)
	require.Equal(t, "original", node.Properties["payload"], "rejected preparation publishes no user mutation")
	next, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer next.Rollback()
	require.NoError(t, next.SetNamespace("test"))
	require.NoError(t, next.DeleteGraphNode(t.Context(), "test:a", CascadeEdges))
	healthy, err := next.PrepareEntityCommit(t.Context(), EntityCommitRequest{LogicalID: next.ID})
	require.NoError(t, err, "rejected preparation must release authority for the next cleanup")
	defer healthy.Abort()
	require.NoError(t, healthy.Commit(nil))
	_, err = b.GetStoredNode(t.Context(), "test:a")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = b.GetStoredEdge(t.Context(), "test:r")
	require.ErrorIs(t, err, ErrNotFound)
	count, err := b.NodeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	count, err = b.EdgeCount(t.Context())
	require.NoError(t, err)
	require.Zero(t, count)
}

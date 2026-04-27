package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func setupOnAccessE2E(t *testing.T, bundles map[string]*knowledgepolicy.DecayProfileBundle, bindings map[string]*knowledgepolicy.DecayProfileBinding, policies map[string]*knowledgepolicy.PromotionPolicyDef) (*storage.BadgerEngine, *knowledgepolicy.AccessFlusher, *StorageExecutor, context.Context) {
	t.Helper()
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })

	be.SetDecayEnabled(true)
	bt, err := knowledgepolicy.BuildBindingTable(bundles, bindings, nil, policies)
	require.NoError(t, err)
	be.GetSchemaForNamespace("test").SetBindingTable(bt)

	acc := knowledgepolicy.NewAccessAccumulator(true, 0)
	be.SetAccessAccumulator(acc)
	flusher := knowledgepolicy.NewAccessFlusher(acc, be, time.Hour)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(be, "test"))
	flusher.SetPropertySuppression(
		func(namespace string) *knowledgepolicy.Scorer { return be.ScorerForNamespace(namespace) },
		be,
		func(entityID string) { be.AddToPendingEmbeddings(storage.NodeID(entityID)) },
	)
	flusher.SetSuppressionRecheck(func(entityID string, meta knowledgepolicy.EntityMeta) {
		becameSuppressed, err := be.EnqueueDeindexIfSuppressed(entityID, meta.Scope == knowledgepolicy.ScopeEdge)
		if err == nil && becameSuppressed {
			tokens := append([]string(nil), meta.Labels...)
			if meta.EdgeType != "" {
				tokens = append(tokens, meta.EdgeType)
			}
			exec.InvalidateEntityCaches(entityID, tokens)
		}
	})
	return be, flusher, exec, context.Background()
}

func TestE2E_OnAccessAcceleratesDecayUntilSuppressed(t *testing.T) {
	bundles := map[string]*knowledgepolicy.DecayProfileBundle{
		"node_decay": {
			Name:                "node_decay",
			Scope:               knowledgepolicy.ScopeNode,
			Function:            knowledgepolicy.DecayFunctionExponential,
			HalfLifeSeconds:     2,
			VisibilityThreshold: 0.10,
			ScoreFrom:           knowledgepolicy.ScoreFromCustom,
			ScoreFromProperty:   "degradeAt",
			Enabled:             true,
		},
		"edge_decay": {
			Name:                "edge_decay",
			Scope:               knowledgepolicy.ScopeEdge,
			Function:            knowledgepolicy.DecayFunctionExponential,
			HalfLifeSeconds:     2,
			VisibilityThreshold: 0.10,
			ScoreFrom:           knowledgepolicy.ScoreFromCustom,
			ScoreFromProperty:   "edgePenaltyAt",
			Enabled:             true,
		},
	}
	bindings := map[string]*knowledgepolicy.DecayProfileBinding{
		"node_bind": {
			Name:         "node_bind",
			ProfileRef:   "node_decay",
			TargetLabels: []string{"DecaySource"},
		},
		"target_bind": {
			Name:         "target_bind",
			ProfileRef:   "node_decay",
			TargetLabels: []string{"DecayTarget"},
		},
		"edge_bind": {
			Name:           "edge_bind",
			ProfileRef:     "edge_decay",
			TargetEdgeType: "DECAYS_TO",
			IsEdge:         true,
		},
	}
	policies := map[string]*knowledgepolicy.PromotionPolicyDef{
		"node_policy": {
			Name:         "node_policy",
			TargetLabels: []string{"DecaySource"},
			Enabled:      true,
			OnAccess: &knowledgepolicy.PromotionPolicyOnAccess{Mutations: []knowledgepolicy.OnAccessMutation{
				{Expression: "n.accessCount = coalesce(n.accessCount, 0) + 1"},
				{Expression: "n.degradeAt = timestamp() - (n.accessCount * 700000000)"},
			}},
		},
		"edge_policy": {
			Name:           "edge_policy",
			TargetEdgeType: "DECAYS_TO",
			IsEdge:         true,
			Enabled:        true,
			OnAccess: &knowledgepolicy.PromotionPolicyOnAccess{Mutations: []knowledgepolicy.OnAccessMutation{
				{Expression: "r.accessCount = coalesce(r.accessCount, 0) + 1"},
				{Expression: "r.edgePenaltyAt = timestamp() - (r.accessCount * 700000000)"},
			}},
		},
	}

	be, flusher, exec, ctx := setupOnAccessE2E(t, bundles, bindings, policies)

	now := time.Now()
	_, err := be.CreateNode(&storage.Node{ID: "test:source-1", Labels: []string{"DecaySource"}, Properties: map[string]interface{}{"title": "source", "body": "volatile"}, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)
	_, err = be.CreateNode(&storage.Node{ID: "test:target-1", Labels: []string{"DecayTarget"}, Properties: map[string]interface{}{"title": "target"}, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)
	err = be.CreateEdge(&storage.Edge{ID: "test:edge-1", StartNode: "test:source-1", EndNode: "test:target-1", Type: "DECAYS_TO", Properties: map[string]interface{}{"summary": "edge text", "weight": int64(7)}, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)

	query := `MATCH (s:DecaySource)-[r:DECAYS_TO]->(t:DecayTarget) RETURN s, r, t`
	for i := 0; i < 10; i++ {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "access %d should still be visible before post-access flush", i+1)
		flusher.Flush()
	}

	nodes, err := be.GetNodesByLabel("DecaySource")
	require.NoError(t, err)
	require.Len(t, nodes, 0, "storage node reads should already honor suppression after 10 accesses")
	edges, err := be.GetEdgesByType("DECAYS_TO")
	require.NoError(t, err)
	require.Len(t, edges, 0, "storage edge reads should already honor suppression after 10 accesses")

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 0, "node+edge should be suppressed after 10 accesses")
}

func TestE2E_OnAccessPropertyPreservationKeepsOnlyAllowedFields(t *testing.T) {
	bundles := map[string]*knowledgepolicy.DecayProfileBundle{
		"node_decay": {
			Name:                "node_decay",
			Scope:               knowledgepolicy.ScopeNode,
			Function:            knowledgepolicy.DecayFunctionExponential,
			HalfLifeSeconds:     10,
			VisibilityThreshold: 0.10,
			ScoreFrom:           knowledgepolicy.ScoreFromCustom,
			ScoreFromProperty:   "degradeAt",
			Enabled:             true,
		},
		"edge_decay": {
			Name:                "edge_decay",
			Scope:               knowledgepolicy.ScopeEdge,
			Function:            knowledgepolicy.DecayFunctionExponential,
			HalfLifeSeconds:     10,
			VisibilityThreshold: 0.10,
			ScoreFrom:           knowledgepolicy.ScoreFromCustom,
			ScoreFromProperty:   "edgePenaltyAt",
			Enabled:             true,
		},
	}
	bindings := map[string]*knowledgepolicy.DecayProfileBinding{
		"node_bind": {
			Name:         "node_bind",
			ProfileRef:   "node_decay",
			TargetLabels: []string{"PropertySource"},
			PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{
				{PropertyPath: "body", HalfLifeSeconds: 1},
				{PropertyPath: "title", NoDecay: true},
			},
		},
		"target_bind": {
			Name:         "target_bind",
			ProfileRef:   "node_decay",
			TargetLabels: []string{"PropertyTarget"},
		},
		"edge_bind": {
			Name:           "edge_bind",
			ProfileRef:     "edge_decay",
			TargetEdgeType: "PRESERVES",
			IsEdge:         true,
			PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{
				{PropertyPath: "summary", HalfLifeSeconds: 1},
				{PropertyPath: "weight", NoDecay: true},
			},
		},
	}
	policies := map[string]*knowledgepolicy.PromotionPolicyDef{
		"node_policy": {
			Name:         "node_policy",
			TargetLabels: []string{"PropertySource"},
			Enabled:      true,
			OnAccess: &knowledgepolicy.PromotionPolicyOnAccess{Mutations: []knowledgepolicy.OnAccessMutation{
				{Expression: "n.accessCount = coalesce(n.accessCount, 0) + 1"},
				{Expression: "n.degradeAt = timestamp() - (n.accessCount * 700000000)"},
			}},
		},
		"edge_policy": {
			Name:           "edge_policy",
			TargetEdgeType: "PRESERVES",
			IsEdge:         true,
			Enabled:        true,
			OnAccess: &knowledgepolicy.PromotionPolicyOnAccess{Mutations: []knowledgepolicy.OnAccessMutation{
				{Expression: "r.accessCount = coalesce(r.accessCount, 0) + 1"},
				{Expression: "r.edgePenaltyAt = timestamp() - (r.accessCount * 700000000)"},
			}},
		},
	}

	be, flusher, exec, ctx := setupOnAccessE2E(t, bundles, bindings, policies)

	now := time.Now()
	_, err := be.CreateNode(&storage.Node{ID: "test:prop-source-1", Labels: []string{"PropertySource"}, Properties: map[string]interface{}{"title": "keep-me", "body": "hide-me"}, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)
	_, err = be.CreateNode(&storage.Node{ID: "test:prop-target-1", Labels: []string{"PropertyTarget"}, Properties: map[string]interface{}{"title": "target"}, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)
	err = be.CreateEdge(&storage.Edge{ID: "test:prop-edge-1", StartNode: "test:prop-source-1", EndNode: "test:prop-target-1", Type: "PRESERVES", Properties: map[string]interface{}{"summary": "drop-me", "weight": int64(42)}, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)

	query := `MATCH (s:PropertySource)-[r:PRESERVES]->(t:PropertyTarget) RETURN s, r`
	for i := 0; i < 10; i++ {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		flusher.Flush()
	}

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1, "entity should remain visible while only selected properties suppress")

	nodes, err := be.GetNodesByLabel("PropertySource")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	nodeProps, ok := exec.nodeToMap(nodes[0])["properties"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "keep-me", nodeProps["title"])
	_, hasBody := nodeProps["body"]
	require.False(t, hasBody, "body should be suppressed after repeated access")

	edges, err := be.GetEdgesByType("PRESERVES")
	require.NoError(t, err)
	require.Len(t, edges, 1)
	edgeProps, ok := exec.edgeToMap(edges[0])["properties"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, int64(42), edgeProps["weight"])
	_, hasSummary := edgeProps["summary"]
	require.False(t, hasSummary, "summary should be suppressed after repeated access")
}

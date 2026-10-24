package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func setupOnAccessE2E(t *testing.T, bundles map[string]*knowledgepolicy.DecayProfileBundle, bindings map[string]*knowledgepolicy.DecayProfileBinding, policies map[string]*knowledgepolicy.PromotionPolicyDef) (*storage.BadgerEngine, *knowledgepolicy.AccessAccumulator, *knowledgepolicy.AccessFlusher, *StorageExecutor, context.Context) {
	t.Helper()
	be, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
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
	return be, acc, flusher, exec, context.Background()
}

func requireBufferedAccessCounts(t *testing.T, acc *knowledgepolicy.AccessAccumulator, expected map[string]int64) {
	t.Helper()
	for entityID, want := range expected {
		got := acc.ReadThrough(entityID, "accessCount", 0)
		require.Equalf(t, want, got, "unexpected buffered accessCount for %s", entityID)
	}
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
			DecayEnabled:        true,
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
			DecayEnabled:        true,
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

	be, acc, flusher, exec, ctx := setupOnAccessE2E(t, bundles, bindings, policies)

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
		if i == 0 {
			requireBufferedAccessCounts(t, acc, map[string]int64{
				"test:source-1": 1,
				"test:edge-1":   1,
				"test:target-1": 1,
			})
		}
		flusher.Flush()
		if i == 0 {
			requireBufferedAccessCounts(t, acc, map[string]int64{
				"test:source-1": 0,
				"test:edge-1":   0,
				"test:target-1": 0,
			})
		}
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
			DecayEnabled:        true,
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
			DecayEnabled:        true,
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

	be, acc, flusher, exec, ctx := setupOnAccessE2E(t, bundles, bindings, policies)

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
		if i == 0 {
			requireBufferedAccessCounts(t, acc, map[string]int64{
				"test:prop-source-1": 1,
				"test:prop-edge-1":   1,
				"test:prop-target-1": 0,
			})
		}
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

func TestE2E_OnAccessDoesNotTriggerForWhereOnlyEvaluation(t *testing.T) {
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
			DecayEnabled:        true,
		},
	}
	bindings := map[string]*knowledgepolicy.DecayProfileBinding{
		"node_bind": {
			Name:         "node_bind",
			ProfileRef:   "node_decay",
			TargetLabels: []string{"WhereOnlySource"},
		},
	}
	policies := map[string]*knowledgepolicy.PromotionPolicyDef{
		"node_policy": {
			Name:         "node_policy",
			TargetLabels: []string{"WhereOnlySource"},
			Enabled:      true,
			OnAccess: &knowledgepolicy.PromotionPolicyOnAccess{Mutations: []knowledgepolicy.OnAccessMutation{
				{Expression: "n.accessCount = coalesce(n.accessCount, 0) + 1"},
				{Expression: "n.degradeAt = timestamp() - (n.accessCount * 700000000)"},
			}},
		},
	}

	be, acc, flusher, exec, ctx := setupOnAccessE2E(t, bundles, bindings, policies)

	now := time.Now()
	_, err := be.CreateNode(&storage.Node{
		ID:         "test:where-only-1",
		Labels:     []string{"WhereOnlySource"},
		Properties: map[string]interface{}{"title": "candidate", "kind": "ignore-me"},
		CreatedAt:  now,
		UpdatedAt:  now,
	})
	require.NoError(t, err)
	_, err = be.CreateNode(&storage.Node{
		ID:         "test:where-only-2",
		Labels:     []string{"WhereOnlySource"},
		Properties: map[string]interface{}{"title": "other", "kind": "other-kind"},
		CreatedAt:  now,
		UpdatedAt:  now,
	})
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `MATCH (n:WhereOnlySource) WHERE n.kind = 'missing-kind' RETURN n`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 0, "WHERE-only evaluation should not materialize the node")
	requireBufferedAccessCounts(t, acc, map[string]int64{
		"test:where-only-1": 0,
		"test:where-only-2": 0,
	})

	flusher.Flush()

	entry, err := be.GetAccessMeta("test:where-only-1")
	require.NoError(t, err)
	require.Nil(t, entry, "WHERE evaluation must not trigger ON ACCESS mutations")
	entry, err = be.GetAccessMeta("test:where-only-2")
	require.NoError(t, err)
	require.Nil(t, entry, "non-materialized peers must not be queued for ON ACCESS")

	result, err = exec.Execute(ctx, `MATCH (n:WhereOnlySource) WHERE n.kind = 'ignore-me' RETURN n`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1, "returning the node should materialize it")
	materialized, ok := result.Rows[0][0].(*storage.Node)
	require.True(t, ok, "RETURN n should materialize a storage node")
	require.Equal(t, storage.NodeID("where-only-1"), materialized.ID, "namespaced executor should expose the user-facing node ID")
	requireBufferedAccessCounts(t, acc, map[string]int64{
		"test:where-only-1": 1,
		"test:where-only-2": 0,
	})

	flusher.Flush()

	entry, err = be.GetAccessMeta("test:where-only-1")
	require.NoError(t, err)
	require.NotNil(t, entry, "materialized RETURN should trigger ON ACCESS")
	require.Equal(t, int64(1), entry.Fixed.AccessCount)
	entry, err = be.GetAccessMeta("test:where-only-2")
	require.NoError(t, err)
	require.Nil(t, entry, "only the materialized node should be flushed")
}

func TestE2E_OnAccessDoesNotTriggerForScalarProjection(t *testing.T) {
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
			DecayEnabled:        true,
		},
	}
	bindings := map[string]*knowledgepolicy.DecayProfileBinding{
		"node_bind": {
			Name:         "node_bind",
			ProfileRef:   "node_decay",
			TargetLabels: []string{"WhereOnlySource"},
		},
	}
	policies := map[string]*knowledgepolicy.PromotionPolicyDef{
		"node_policy": {
			Name:         "node_policy",
			TargetLabels: []string{"WhereOnlySource"},
			Enabled:      true,
			OnAccess: &knowledgepolicy.PromotionPolicyOnAccess{Mutations: []knowledgepolicy.OnAccessMutation{
				{Expression: "n.accessCount = coalesce(n.accessCount, 0) + 1"},
				{Expression: "n.degradeAt = timestamp() - (n.accessCount * 700000000)"},
			}},
		},
	}

	be, acc, flusher, exec, ctx := setupOnAccessE2E(t, bundles, bindings, policies)

	now := time.Now()
	_, err := be.CreateNode(&storage.Node{
		ID:         "test:scalar-only-1",
		Labels:     []string{"WhereOnlySource"},
		Properties: map[string]interface{}{"title": "candidate", "kind": "ignore-me"},
		CreatedAt:  now,
		UpdatedAt:  now,
	})
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `MATCH (n:WhereOnlySource) WHERE n.kind = 'ignore-me' RETURN n.kind`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1, "scalar projection should still return one row")
	require.Len(t, result.Rows[0], 1)
	require.Equal(t, "ignore-me", result.Rows[0][0])
	requireBufferedAccessCounts(t, acc, map[string]int64{
		"test:scalar-only-1": 0,
	})

	flusher.Flush()

	entry, err := be.GetAccessMeta("test:scalar-only-1")
	require.NoError(t, err)
	require.Nil(t, entry, "scalar projection must not count as materializing the node")
}

func TestE2E_OnAccessOnlyTriggersAfterScoringGate(t *testing.T) {
	bundles := map[string]*knowledgepolicy.DecayProfileBundle{
		"blocked_reverse": {
			Name:                "blocked_reverse",
			Scope:               knowledgepolicy.ScopeNode,
			Function:            knowledgepolicy.DecayFunctionExponential,
			HalfLifeSeconds:     -3600,
			VisibilityThreshold: 0.10,
			ScoreFloor:          0.05,
			ScoreFrom:           knowledgepolicy.ScoreFromCreated,
			Enabled:             true,
			DecayEnabled:        true,
		},
		"passing_reverse": {
			Name:                "passing_reverse",
			Scope:               knowledgepolicy.ScopeNode,
			Function:            knowledgepolicy.DecayFunctionExponential,
			HalfLifeSeconds:     -3600,
			VisibilityThreshold: 0.10,
			ScoreFloor:          0.20,
			ScoreFrom:           knowledgepolicy.ScoreFromCreated,
			Enabled:             true,
			DecayEnabled:        true,
		},
	}
	bindings := map[string]*knowledgepolicy.DecayProfileBinding{
		"blocked_bind": {
			Name:         "blocked_bind",
			ProfileRef:   "blocked_reverse",
			TargetLabels: []string{"BlockedReverse"},
		},
		"passing_bind": {
			Name:         "passing_bind",
			ProfileRef:   "passing_reverse",
			TargetLabels: []string{"PassingReverse"},
		},
	}
	policies := map[string]*knowledgepolicy.PromotionPolicyDef{
		"blocked_policy": {
			Name:         "blocked_policy",
			TargetLabels: []string{"BlockedReverse"},
			Enabled:      true,
			OnAccess: &knowledgepolicy.PromotionPolicyOnAccess{Mutations: []knowledgepolicy.OnAccessMutation{
				{Expression: "n.accessCount = coalesce(n.accessCount, 0) + 1"},
			}},
		},
		"passing_policy": {
			Name:         "passing_policy",
			TargetLabels: []string{"PassingReverse"},
			Enabled:      true,
			OnAccess: &knowledgepolicy.PromotionPolicyOnAccess{Mutations: []knowledgepolicy.OnAccessMutation{
				{Expression: "n.accessCount = coalesce(n.accessCount, 0) + 1"},
			}},
		},
	}

	be, acc, flusher, exec, ctx := setupOnAccessE2E(t, bundles, bindings, policies)
	now := time.Now()
	_, err := be.CreateNode(&storage.Node{ID: "test:blocked-reverse", Labels: []string{"BlockedReverse"}, Properties: map[string]interface{}{"name": "blocked"}, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)
	_, err = be.CreateNode(&storage.Node{ID: "test:passing-reverse", Labels: []string{"PassingReverse"}, Properties: map[string]interface{}{"name": "passing"}, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `MATCH (n:BlockedReverse) RETURN n`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 0, "scoreFloor below threshold should fail the scoring gate")
	requireBufferedAccessCounts(t, acc, map[string]int64{"test:blocked-reverse": 0})
	flusher.Flush()
	entry, err := be.GetAccessMeta("test:blocked-reverse")
	require.NoError(t, err)
	require.Nil(t, entry, "ON ACCESS must not run for rows rejected by the scoring gate")

	result, err = exec.Execute(ctx, `MATCH (n:PassingReverse) RETURN n`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1, "scoreFloor above threshold should pass the scoring gate")
	requireBufferedAccessCounts(t, acc, map[string]int64{"test:passing-reverse": 1})
	flusher.Flush()
	entry, err = be.GetAccessMeta("test:passing-reverse")
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, int64(1), entry.Fixed.AccessCount)
}

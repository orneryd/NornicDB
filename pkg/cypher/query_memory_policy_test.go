package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func queryMemoryPolicyFixture(t testing.TB) (*StorageExecutor, *storage.BadgerEngine) {
	t.Helper()
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	base.SetDecayEnabled(true)
	schema := base.GetSchemaForNamespace("policy")
	// The traversal differential executes 108 queries against this fixture.
	// Keep fresh pending properties stable throughout that comparison; the
	// 72-hour-old controls still exercise suppression and promotion.
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name: "age", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds: 3600, VisibilityThreshold: 0.10, ScoreFrom: knowledgepolicy.ScoreFromCreated,
		Enabled: true, DecayEnabled: true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name: "facts", TargetLabels: []string{"Fact"}, ProfileRef: "age",
		PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "secret", HalfLifeSeconds: 1}},
	}))
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name: "relationships", Scope: knowledgepolicy.ScopeEdge, Function: knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds: 3600 * 24 * 365, VisibilityThreshold: 0.10, ScoreFrom: knowledgepolicy.ScoreFromCreated,
		Enabled: true, DecayEnabled: true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name: "relationships", IsEdge: true, TargetEdgeType: "REL", ProfileRef: "relationships",
		PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "secret", HalfLifeSeconds: 1}},
	}))
	require.NoError(t, schema.CreatePromotionProfile(knowledgepolicy.PromotionProfileDef{
		Name: "keep", Scope: knowledgepolicy.ScopeNode, Multiplier: 10, ScoreFloor: 0.5, ScoreCap: 1, Enabled: true,
	}))
	require.NoError(t, schema.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{
		Name: "promote", TargetLabels: []string{"Fact"}, Enabled: true,
		WhenClauses: []knowledgepolicy.PromotionPolicyWhenClause{{Predicate: "n.promote = true", ProfileRef: "keep"}},
	}))
	old := time.Now().Add(-72 * time.Hour)
	for _, node := range []*storage.Node{
		{ID: "policy:a", Labels: []string{"Fact"}, Properties: map[string]interface{}{"promote": true, "display": "a"}},
		{ID: "policy:b", Labels: []string{"Fact"}, Properties: map[string]interface{}{"promote": true, "display": "b"}},
		{ID: "policy:expired", Labels: []string{"Fact"}, Properties: map[string]interface{}{"promote": false, "display": "expired"}},
		{ID: "policy:internal", Labels: []string{"Fact", "_meta"}, Properties: map[string]interface{}{"promote": true, "display": "internal"}},
	} {
		node.CreatedAt, node.UpdatedAt = old, old
		node.Properties["secret"] = "hidden"
		_, err := base.CreateNode(node)
		require.NoError(t, err)
	}
	for _, start := range []storage.NodeID{"policy:a", "policy:expired", "policy:internal"} {
		require.NoError(t, base.CreateEdge(&storage.Edge{ID: storage.EdgeID(string(start) + "-edge"), StartNode: start, EndNode: "policy:b", Type: "REL", Properties: map[string]interface{}{"secret": "hidden", "marker": "visible"}, CreatedAt: old, UpdatedAt: old}))
	}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "policy"))
	exec.cache = nil
	return exec, base
}

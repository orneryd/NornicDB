package nornicdb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type knowledgePolicyBootstrapProvider struct {
	storage.Engine
	schemas map[string]*storage.SchemaManager
}

func (p *knowledgePolicyBootstrapProvider) GetSchemaForNamespace(namespace string) *storage.SchemaManager {
	return p.schemas[namespace]
}

func TestKnowledgePolicyBootstrapBranches(t *testing.T) {
	require.NoError(t, maybeBootstrapDefaultKnowledgePolicy(nil, "tenant"))
	require.NoError(t, maybeBootstrapDefaultKnowledgePolicy(storage.NewMemoryEngine(), "tenant"))

	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	provider := &knowledgePolicyBootstrapProvider{
		Engine:  base,
		schemas: map[string]*storage.SchemaManager{"nornic": storage.NewSchemaManager(), "custom": storage.NewSchemaManager(), "missing": nil},
	}

	require.NoError(t, maybeBootstrapDefaultKnowledgePolicy(provider, ""))
	bundles, bindings := provider.schemas["nornic"].ShowDecayProfiles()
	require.NotEmpty(t, bundles)
	require.NotEmpty(t, bindings)
	require.NotEmpty(t, provider.schemas["nornic"].ShowPromotionProfiles())
	policies := provider.schemas["nornic"].ShowPromotionPolicies()
	require.Len(t, policies, 3)
	expected := map[string]struct{ mutations, whenClauses int }{
		"memory_episode_consolidation": {5, 2},
		"wisdom_directive_stability":   {2, 3},
		"evidence_traversal_tiering":   {2, 1},
	}
	for _, policy := range policies {
		require.Contains(t, expected, policy.Name)
		require.NotNil(t, policy.OnAccess)
		require.Len(t, policy.OnAccess.Mutations, expected[policy.Name].mutations)
		require.Len(t, policy.WhenClauses, expected[policy.Name].whenClauses)
		if policy.Name == "memory_episode_consolidation" {
			require.NotNil(t, policy.OnAccess.Mutations[3].Kalman)
		}
	}
	require.False(t, knowledgePolicySchemaEmpty(provider.schemas["nornic"]))

	require.NoError(t, maybeBootstrapDefaultKnowledgePolicy(provider, "custom"))
	customBundles, _ := provider.schemas["custom"].ShowDecayProfiles()
	require.NotEmpty(t, customBundles)
	require.NoError(t, maybeBootstrapDefaultKnowledgePolicy(provider, "missing"))

	beforeBundles := len(bundles)
	require.NoError(t, maybeBootstrapDefaultKnowledgePolicy(provider, "nornic"))
	afterBundles, _ := provider.schemas["nornic"].ShowDecayProfiles()
	require.Len(t, afterBundles, beforeBundles)
}

func TestApplyKnowledgePolicyDDLBranches(t *testing.T) {
	schema := storage.NewSchemaManager()
	require.True(t, knowledgePolicySchemaEmpty(schema))

	require.ErrorContains(t, applyKnowledgePolicyDDL(schema, "MATCH (n) RETURN n"), "not a knowledge-policy DDL statement")
	require.Error(t, applyKnowledgePolicyDDL(schema, "CREATE DECAY PROFILE OPTIONS { function: 'none' }"))

	require.NoError(t, applyKnowledgePolicyDDL(schema, `CREATE DECAY PROFILE unit_decay OPTIONS {
		decayEnabled: false,
		visibilityThreshold: 0.0,
		function: 'none',
		scoreFrom: 'CREATED'
	}`))
	require.NoError(t, applyKnowledgePolicyDDL(schema, `CREATE DECAY PROFILE unit_binding FOR (n:Unit) APPLY { DECAY PROFILE 'unit_decay' }`))
	require.NoError(t, applyKnowledgePolicyDDL(schema, `CREATE PROMOTION PROFILE unit_promo OPTIONS { multiplier: 1.0, scoreFloor: 0.0, scoreCap: 1.0 }`))
	require.NoError(t, applyKnowledgePolicyDDL(schema, `CREATE PROMOTION POLICY unit_policy FOR (n:Unit) APPLY { WHEN n.score >= 1 APPLY PROFILE 'unit_promo' }`))
	require.NoError(t, applyKnowledgePolicyDDL(schema, `CREATE PROMOTION POLICY IF NOT EXISTS unit_policy FOR (n:Unit) APPLY { WHEN n.score >= 1 APPLY PROFILE 'unit_promo' }`))

	bundles, bindings := schema.ShowDecayProfiles()
	require.Len(t, bundles, 1)
	require.Len(t, bindings, 1)
	require.Len(t, schema.ShowPromotionProfiles(), 1)
	require.Len(t, schema.ShowPromotionPolicies(), 1)
}

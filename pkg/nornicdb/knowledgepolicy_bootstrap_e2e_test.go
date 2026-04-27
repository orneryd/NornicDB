package nornicdb

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen_BootstrapsDefaultKnowledgePolicyWhenDecayEnabledAndSchemaEmpty(t *testing.T) {
	config := DefaultConfig()
	config.Memory.DecayEnabled = true

	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	defer db.Close()

	base := db.GetBaseStorageForManager()
	be := unwrapToBadgerEngine(base)
	require.NotNil(t, be)
	provider, ok := base.(storage.NamespaceSchemaProvider)
	require.True(t, ok)

	namespace := db.defaultDatabaseName()
	schema := provider.GetSchemaForNamespace(namespace)
	bundles, bindings := schema.ShowDecayProfiles()
	profiles := schema.ShowPromotionProfiles()
	policies := schema.ShowPromotionPolicies()
	require.Len(t, bundles, 5)
	require.Len(t, bindings, 8)
	require.Len(t, profiles, 6)
	require.Len(t, policies, 3)

	scorer := be.ScorerForNamespace(namespace)
	require.NotNil(t, scorer)
	now := time.Now().UnixNano()
	res := scorer.ScoreNode("n1", []string{"KnowledgeFact"}, nil, now-int64(365*24*time.Hour), 0, now)
	assert.True(t, res.NoDecay)
	assert.Equal(t, 1.0, res.FinalScore)
	assert.Equal(t, "knowledge_fact_retention", res.ResolvedDecayProfileID)
}

func TestOpen_DoesNotBootstrapDefaultKnowledgePolicyWhenDecayDisabledOrPoliciesExist(t *testing.T) {
	t.Run("decay disabled", func(t *testing.T) {
		config := DefaultConfig()
		config.Memory.DecayEnabled = false

		db, err := Open(t.TempDir(), config)
		require.NoError(t, err)
		defer db.Close()

		provider, ok := db.GetBaseStorageForManager().(storage.NamespaceSchemaProvider)
		require.True(t, ok)
		schema := provider.GetSchemaForNamespace(db.defaultDatabaseName())
		bundles, bindings := schema.ShowDecayProfiles()
		assert.Len(t, bundles, 0)
		assert.Len(t, bindings, 0)
		assert.Len(t, schema.ShowPromotionProfiles(), 0)
		assert.Len(t, schema.ShowPromotionPolicies(), 0)
	})

	t.Run("existing policy present", func(t *testing.T) {
		config := DefaultConfig()
		config.Memory.DecayEnabled = true
		dataDir := t.TempDir()

		eng, err := storage.NewBadgerEngine(dataDir)
		require.NoError(t, err)
		schema := eng.GetSchemaForNamespace("nornic")
		require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
			Name:                "custom_decay",
			Scope:               knowledgepolicy.ScopeNode,
			Function:            knowledgepolicy.DecayFunctionExponential,
			HalfLifeSeconds:     60,
			VisibilityThreshold: 0.1,
			ScoreFrom:           knowledgepolicy.ScoreFromCreated,
			Enabled:             true,
			DecayEnabled:        true,
		}))
		require.NoError(t, eng.Close())

		db, err := Open(dataDir, config)
		require.NoError(t, err)
		defer db.Close()

		provider, ok := db.GetBaseStorageForManager().(storage.NamespaceSchemaProvider)
		require.True(t, ok)
		schema = provider.GetSchemaForNamespace(db.defaultDatabaseName())
		bundles, bindings := schema.ShowDecayProfiles()
		assert.Len(t, bundles, 1)
		assert.Len(t, bindings, 0)
		assert.Len(t, schema.ShowPromotionProfiles(), 0)
		assert.Len(t, schema.ShowPromotionPolicies(), 0)
	})
}

func TestE2E_AlterUnsuppressionInvalidatesCachedCypherReads(t *testing.T) {
	config := DefaultConfig()
	config.Memory.DecayEnabled = true
	config.Database.AsyncWritesEnabled = false

	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	base := db.GetBaseStorageForManager()
	be := unwrapToBadgerEngine(base)
	require.NotNil(t, be)
	provider, ok := base.(storage.NamespaceSchemaProvider)
	require.True(t, ok)
	schema := provider.GetSchemaForNamespace(db.defaultDatabaseName())
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name:                "short_doc",
		Scope:               knowledgepolicy.ScopeNode,
		Function:            knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds:     1,
		VisibilityThreshold: 0.10,
		ScoreFrom:           knowledgepolicy.ScoreFromCreated,
		Enabled:             true,
		DecayEnabled:        true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name:         "short_doc_binding",
		ProfileRef:   "short_doc",
		TargetLabels: []string{"CachedDoc"},
	}))

	node, err := base.CreateNode(&storage.Node{
		ID:         storage.NodeID("nornic:cached-doc-1"),
		Labels:     []string{"CachedDoc"},
		Properties: map[string]any{"title": "cached"},
		CreatedAt:  time.Now().Add(-72 * time.Hour),
		UpdatedAt:  time.Now().Add(-72 * time.Hour),
	})
	require.NoError(t, err)
	require.NotNil(t, node)
	becameSuppressed, err := be.EnqueueDeindexIfSuppressed(string(node), false)
	require.NoError(t, err)
	require.True(t, becameSuppressed)

	result, err := db.ExecuteCypher(ctx, `MATCH (n:CachedDoc) RETURN count(n) AS cnt`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.EqualValues(t, 0, result.Rows[0][0])

	err = schema.AlterDecayProfile("short_doc", map[string]interface{}{"decayEnabled": false})
	require.NoError(t, err)

	result, err = db.ExecuteCypher(ctx, `MATCH (n:CachedDoc) RETURN count(n) AS cnt`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.EqualValues(t, 1, result.Rows[0][0])
}

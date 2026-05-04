package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2E_NornicDbKnowledgePolicyInfoReflectsSchemaCounts(t *testing.T) {
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })

	be.SetDecayEnabled(true)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(be, "test"))
	ctx := context.Background()

	stmts := []string{
		"CREATE DECAY PROFILE profile_alpha OPTIONS { halfLifeSeconds: 3600, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', visibilityThreshold: 0.3 }",
		"CREATE DECAY PROFILE binding_alpha FOR (n:MemoryEpisode) APPLY { DECAY PROFILE 'profile_alpha' }",
		"CREATE PROMOTION PROFILE promo_alpha OPTIONS { multiplier: 1.25, scoreFloor: 0.4, scoreCap: 0.95, scope: 'NODE' }",
		"CREATE PROMOTION POLICY policy_alpha FOR (n:MemoryEpisode) APPLY { WHEN n.accessCount >= 3 APPLY PROFILE promo_alpha }",
	}
	for _, stmt := range stmts {
		_, err := exec.Execute(ctx, stmt, nil)
		require.NoError(t, err, stmt)
	}

	result, err := exec.Execute(ctx, "CALL nornicdb.knowledgepolicy.info()", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, []string{"enabled", "system", "decayProfiles", "decayBindings", "promotionProfiles", "promotionPolicies", "configuredVia"}, result.Columns)
	require.Equal(t, true, result.Rows[0][0])
	require.Equal(t, 1, result.Rows[0][2])
	require.Equal(t, 1, result.Rows[0][3])
	require.Equal(t, 1, result.Rows[0][4])
	require.Equal(t, 1, result.Rows[0][5])
	configuredVia, ok := result.Rows[0][6].(string)
	require.True(t, ok)
	require.Contains(t, configuredVia, "CREATE DECAY PROFILE")
	require.Contains(t, configuredVia, "CREATE PROMOTION PROFILE")
	require.Contains(t, configuredVia, "CREATE PROMOTION POLICY")
}

func TestE2E_ShowDecayProfiles_RowShapes(t *testing.T) {
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })

	exec := NewStorageExecutor(storage.NewNamespacedEngine(be, "test"))
	ctx := context.Background()

	stmts := []string{
		"CREATE DECAY PROFILE profile_alpha OPTIONS { halfLifeSeconds: 3600, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', visibilityThreshold: 0.3 }",
		"CREATE DECAY PROFILE binding_alpha FOR (n:MemoryEpisode) APPLY { DECAY PROFILE 'profile_alpha' }",
	}
	for _, stmt := range stmts {
		_, err := exec.Execute(ctx, stmt, nil)
		require.NoError(t, err, stmt)
	}

	result, err := exec.Execute(ctx, "SHOW DECAY PROFILES", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"kind", "name", "scope", "target", "profileRef", "enabled"}, result.Columns)
	require.Len(t, result.Rows, 2)

	rowsByName := make(map[string][]interface{}, len(result.Rows))
	for _, row := range result.Rows {
		require.Len(t, row, 6)
		name, ok := row[1].(string)
		require.True(t, ok)
		rowsByName[name] = row
	}

	bundleRow, ok := rowsByName["profile_alpha"]
	require.True(t, ok)
	assert.Equal(t, "bundle", bundleRow[0])
	assert.Equal(t, "NODE", bundleRow[2])
	assert.Equal(t, "", bundleRow[3])
	assert.Equal(t, "", bundleRow[4])
	assert.Equal(t, true, bundleRow[5])

	bindingRow, ok := rowsByName["binding_alpha"]
	require.True(t, ok)
	assert.Equal(t, "binding", bindingRow[0])
	assert.Equal(t, "NODE", bindingRow[2])
	assert.Equal(t, "MemoryEpisode", bindingRow[3])
	assert.Equal(t, "profile_alpha", bindingRow[4])
	assert.Equal(t, true, bindingRow[5])
}

func TestE2E_ShowPromotionPolicies_RowShapes(t *testing.T) {
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })

	exec := NewStorageExecutor(storage.NewNamespacedEngine(be, "test"))
	ctx := context.Background()

	stmts := []string{
		"CREATE PROMOTION PROFILE promo_alpha OPTIONS { multiplier: 1.25, scoreFloor: 0.4, scoreCap: 0.95, scope: 'NODE' }",
		"CREATE PROMOTION POLICY policy_alpha FOR (n:MemoryEpisode) APPLY { ON ACCESS { SET n.accessCount = coalesce(n.accessCount, 0) + 1 } WHEN n.accessCount >= 3 APPLY PROFILE promo_alpha }",
	}
	for _, stmt := range stmts {
		_, err := exec.Execute(ctx, stmt, nil)
		require.NoError(t, err, stmt)
	}

	result, err := exec.Execute(ctx, "SHOW PROMOTION POLICIES", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"name", "scope", "target", "enabled", "whenClauses", "onAccessMutations"}, result.Columns)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 6)

	row := result.Rows[0]
	assert.Equal(t, "policy_alpha", row[0])
	assert.Equal(t, "NODE", row[1])
	assert.Equal(t, "MemoryEpisode", row[2])
	assert.Equal(t, true, row[3])
	assert.Equal(t, 1, row[4])
	assert.Equal(t, 1, row[5])
}

func TestE2E_CallNornicDbKnowledgePolicyProfilesAndPolicies(t *testing.T) {
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })

	be.SetDecayEnabled(true)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(be, "test"))
	ctx := context.Background()

	stmts := []string{
		"CREATE DECAY PROFILE profile_alpha OPTIONS { halfLifeSeconds: 3600, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', visibilityThreshold: 0.3, scoreFloor: 0.1 }",
		"CREATE DECAY PROFILE binding_alpha FOR (n:MemoryEpisode) APPLY { DECAY PROFILE 'profile_alpha' }",
		"CREATE PROMOTION PROFILE promo_alpha OPTIONS { multiplier: 1.25, scoreFloor: 0.4, scoreCap: 0.95, scope: 'NODE' }",
		"CREATE PROMOTION POLICY policy_alpha FOR (n:MemoryEpisode) APPLY { WHEN n.accessCount >= 3 APPLY PROFILE promo_alpha }",
	}
	for _, stmt := range stmts {
		_, err := exec.Execute(ctx, stmt, nil)
		require.NoError(t, err, stmt)
	}

	profiles, err := exec.Execute(ctx, "CALL nornicdb.knowledgepolicy.profiles()", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"kind", "Name", "HalfLifeSeconds", "VisibilityThreshold", "ScoreFloor", "Function", "Scope", "DecayEnabled", "ScoreFrom", "ScoreFromProperty", "Enabled", "TargetLabels", "TargetEdgeType", "IsWildcard", "IsEdge", "ProfileRef", "NoDecay", "Order"}, profiles.Columns)
	require.Len(t, profiles.Rows, 2)

	rowsByName := make(map[string][]interface{}, len(profiles.Rows))
	for _, row := range profiles.Rows {
		rowsByName[row[1].(string)] = row
	}
	assert.Equal(t, "bundle", rowsByName["profile_alpha"][0])
	assert.Equal(t, int64(3600), rowsByName["profile_alpha"][2])
	assert.Equal(t, 0.3, rowsByName["profile_alpha"][3])
	assert.Equal(t, "binding", rowsByName["binding_alpha"][0])
	assert.Equal(t, "profile_alpha", rowsByName["binding_alpha"][15])
	assert.Equal(t, 0, rowsByName["binding_alpha"][17])

	policies, err := exec.Execute(ctx, "CALL nornicdb.knowledgepolicy.policies()", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"kind", "Name", "Scope", "Multiplier", "ScoreFloor", "ScoreCap", "Enabled", "TargetLabels", "TargetEdgeType", "IsWildcard", "IsEdge"}, policies.Columns)
	require.Len(t, policies.Rows, 2)

	policyRowsByName := make(map[string][]interface{}, len(policies.Rows))
	for _, row := range policies.Rows {
		policyRowsByName[row[1].(string)] = row
	}
	assert.Equal(t, "profile", policyRowsByName["promo_alpha"][0])
	assert.Equal(t, 1.25, policyRowsByName["promo_alpha"][3])
	assert.Equal(t, "policy", policyRowsByName["policy_alpha"][0])
	assert.Equal(t, true, policyRowsByName["policy_alpha"][6])
	assert.Equal(t, []string{"MemoryEpisode"}, policyRowsByName["policy_alpha"][7])
}

func TestE2E_CallNornicDbKnowledgePolicyResolve(t *testing.T) {
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })

	be.SetDecayEnabled(true)
	exec := NewStorageExecutor(be)
	ctx := context.Background()

	_, err = exec.Execute(ctx, "CREATE DECAY PROFILE profile_alpha OPTIONS { halfLifeSeconds: 3600, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', visibilityThreshold: 0.3 }", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE DECAY PROFILE binding_alpha FOR (n:MemoryEpisode) APPLY { DECAY PROFILE 'profile_alpha' }", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE PROMOTION PROFILE promo_alpha OPTIONS { multiplier: 1.25, scoreFloor: 0.4, scoreCap: 0.95, scope: 'NODE' }", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE PROMOTION POLICY policy_alpha FOR (n:MemoryEpisode) APPLY { WHEN n.accessCount >= 3 APPLY PROFILE promo_alpha }", nil)
	require.NoError(t, err)

	nodeID, err := be.CreateNode(&storage.Node{
		ID:        storage.NodeID("nornic:episode-1"),
		Labels:    []string{"MemoryEpisode"},
		CreatedAt: time.Unix(0, storage.DecayScoringTime()-6*3600*1e9),
	})
	require.NoError(t, err)
	require.NoError(t, be.PutAccessMeta(string(nodeID), &knowledgepolicy.AccessMetaEntry{
		TargetID:    string(nodeID),
		TargetScope: knowledgepolicy.ScopeNode,
		Fixed: knowledgepolicy.AccessMetaFixedFields{
			AccessCount: 3,
		},
	}))

	result, err := exec.Execute(ctx, fmt.Sprintf("CALL nornicdb.knowledgepolicy.resolve('%s', '', '')", nodeID), nil)
	require.NoError(t, err)
	require.Equal(t, []string{"TargetID", "TargetScope", "ResolvedDecayProfileID", "ResolvedScoreFrom", "ResolutionSourceChain", "AppliedDecayProfileNames", "AppliedPromotionPolicyName", "AppliedPromotionProfileName", "EffectiveRate", "EffectiveThreshold", "EffectiveMultiplier", "BaseScore", "FinalScore", "NoDecay", "SuppressionEligible", "Explanation"}, result.Columns)
	require.Len(t, result.Rows, 1)
	row := result.Rows[0]
	assert.Equal(t, string(nodeID), row[0])
	assert.Equal(t, "NODE", row[1])
	assert.Equal(t, "profile_alpha", row[2])
	assert.Equal(t, "CREATED", row[3])
	assert.Equal(t, "policy_alpha", row[6])
	assert.Equal(t, "promo_alpha", row[7])
	assert.Equal(t, false, row[13])
}

func TestE2E_CallNornicDbKnowledgePolicyDeindexStatus(t *testing.T) {
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })

	item := &storage.DeindexWorkItem{
		WorkItemID:  "work-1",
		TargetID:    "node-1",
		TargetScope: "NODE",
		EnqueuedAt:  1715000000000,
		Status:      "pending",
	}
	require.NoError(t, be.PutDeindexWorkItem(item))

	exec := NewStorageExecutor(storage.NewNamespacedEngine(be, "test"))
	ctx := context.Background()

	result, err := exec.Execute(ctx, "CALL nornicdb.knowledgepolicy.deindexStatus()", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"pending_count", "supported", "message", "workItemId", "targetId", "targetScope", "enqueuedAt", "status"}, result.Columns)
	require.Len(t, result.Rows, 1)
	row := result.Rows[0]
	assert.Equal(t, 1, row[0])
	assert.Equal(t, true, row[1])
	assert.Equal(t, "work-1", row[3])
	assert.Equal(t, "node-1", row[4])
	assert.Equal(t, "NODE", row[5])
	assert.Equal(t, int64(1715000000000), row[6])
	assert.Equal(t, "pending", row[7])
}

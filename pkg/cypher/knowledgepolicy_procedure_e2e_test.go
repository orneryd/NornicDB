package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2E_DecayProfileOptionParity(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			require.Equal(t, parser, config.GetParserType())
			for _, options := range []string{
				"halfLifeSeconds: 1, visibilityThreshold: 1, scoreFloor: 0, function: 'exponential', scope: 'NODE', decayEnabled: 0, enabled: 1, scoreFrom: 'CREATED'",
				"HALFLIFESECONDS: 1, visibilitythreshold: 0.5, scorefloor: 0.5, FUNCTION: 'exponential', SCOPE: 'NODE', DECAYENABLED: false, ENABLED: true, SCOREFROM: 'CREATED'",
			} {
				t.Run(options, func(t *testing.T) {
					store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "decay_options")
					executor := NewStorageExecutor(store)
					ctx := context.Background()
					_, err := executor.Execute(ctx, "CREATE DECAY PROFILE expected OPTIONS {"+options+"}", nil)
					require.NoError(t, err)
					_, err = executor.Execute(ctx, "CREATE DECAY PROFILE altered OPTIONS {halfLifeSeconds: 3600, function: 'none', scope: 'NODE', scoreFrom: 'CREATED'}", nil)
					require.NoError(t, err)
					_, err = executor.Execute(ctx, "ALTER DECAY PROFILE altered SET OPTIONS {"+options+"}", nil)
					require.NoError(t, err)
					bundles, _ := store.GetSchema().ShowDecayProfiles()
					require.Len(t, bundles, 2)
					byName := make(map[string]knowledgepolicy.DecayProfileBundle)
					for _, bundle := range bundles {
						name := bundle.Name
						bundle.Name = "comparison"
						byName[name] = bundle
					}
					expected, ok := byName["expected"]
					require.True(t, ok)
					altered, ok := byName["altered"]
					require.True(t, ok)
					require.Equal(t, expected, altered)
					for _, invalid := range []string{"halfLifeSeconds: 'abc'", "visibilityThreshold: true", "scoreFloor: false", "function: 5", "scope: false", "decayEnabled: 'abc'", "enabled: 'abc'", "scoreFrom: false", "unknown: 1"} {
						_, err = executor.Execute(ctx, "ALTER DECAY PROFILE altered SET OPTIONS {"+invalid+"}", nil)
						require.Error(t, err, invalid)
						current, _ := store.GetSchema().ShowDecayProfiles()
						require.Len(t, current, 2)
						found := false
						for _, unchanged := range current {
							if unchanged.Name == "altered" {
								found = true
								unchanged.Name = "comparison"
								require.Equal(t, altered, unchanged, invalid)
							}
						}
						require.True(t, found, invalid)
					}
				})
			}
		})
	}
}

func TestE2E_KnowledgePolicyProcedureTransactionVisibility(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			require.Equal(t, parser, config.GetParserType())
			for _, commit := range []bool{false, true} {
				t.Run(fmt.Sprintf("commit=%v", commit), func(t *testing.T) {
					base := newTestMemoryEngine(t)
					store := storage.NewNamespacedEngine(base, "policy_procedure")
					executor := NewStorageExecutor(store)
					peer := NewStorageExecutor(store)
					ctx := context.Background()
					_, err := executor.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					for _, statement := range []string{
						"CREATE DECAY PROFILE decay OPTIONS {halfLifeSeconds: 3600, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', visibilityThreshold: 0.3}",
						"CREATE DECAY PROFILE binding FOR (n:Account) APPLY {DECAY PROFILE decay}",
						"CREATE PROMOTION PROFILE promotion OPTIONS {multiplier: 1.25, scope: 'NODE', scoreFloor: 0.4, scoreCap: 0.95}",
						"CREATE PROMOTION POLICY policy FOR (n:Account) APPLY {WHEN n.count >= 3 APPLY PROFILE promotion}",
					} {
						_, err := executor.Execute(ctx, statement, nil)
						require.NoError(t, err, statement)
					}
					staged, err := executor.Execute(ctx, "CALL nornicdb.knowledgepolicy.info()", nil)
					require.NoError(t, err)
					visible, err := peer.Execute(ctx, "CALL nornicdb.knowledgepolicy.info()", nil)
					require.NoError(t, err)
					profiles, err := executor.Execute(ctx, "CALL nornicdb.knowledgepolicy.profiles()", nil)
					require.NoError(t, err)
					policies, err := executor.Execute(ctx, "CALL nornicdb.knowledgepolicy.policies()", nil)
					require.NoError(t, err)
					_, err = executor.Execute(ctx, map[bool]string{false: "ROLLBACK", true: "COMMIT"}[commit], nil)
					require.NoError(t, err)
					require.Equal(t, []interface{}{1, 1, 1, 1}, staged.Rows[0][2:6])
					require.Equal(t, []interface{}{0, 0, 0, 0}, visible.Rows[0][2:6])
					require.Len(t, profiles.Rows, 2)
					require.Len(t, policies.Rows, 2)
					stored, err := peer.Execute(ctx, "CALL nornicdb.knowledgepolicy.info()", nil)
					require.NoError(t, err)
					if commit {
						require.Equal(t, staged.Rows[0][2:6], stored.Rows[0][2:6])
					} else {
						require.Equal(t, visible.Rows[0][2:6], stored.Rows[0][2:6])
					}
				})
			}
		})
	}
}

func TestE2E_NornicDbKnowledgePolicyInfoReflectsSchemaCounts(t *testing.T) {
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })

	be.SetDecayEnabled(true)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(be, "test"))
	ctx := context.Background()

	stmts := []string{
		"CREATE DECAY PROFILE profile_alpha OPTIONS { halfLifeSeconds: 3600, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', visibilityThreshold: 0.3 }",
		"CREATE DECAY PROFILE binding_alpha FOR (n:MemoryEpisode) APPLY { DECAY PROFILE 'profile_alpha' n.summary NO DECAY }",
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

func TestE2E_ShowPromotionProfiles_RowShapes(t *testing.T) {
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })

	exec := NewStorageExecutor(storage.NewNamespacedEngine(be, "test"))
	ctx := context.Background()

	_, err = exec.Execute(ctx, "CREATE PROMOTION PROFILE promo_alpha OPTIONS { multiplier: 1.25, scoreFloor: 0.4, scoreCap: 0.95, scope: 'NODE' }", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE PROMOTION PROFILE promo_beta OPTIONS { multiplier: 2.0, scoreFloor: 0.2, scoreCap: 0.99, scope: 'EDGE' }", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "SHOW PROMOTION PROFILES", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"name", "scope", "multiplier", "scoreFloor", "scoreCap", "enabled"}, result.Columns)
	require.Len(t, result.Rows, 2)

	rows := make(map[string][]interface{}, len(result.Rows))
	for _, row := range result.Rows {
		require.Len(t, row, 6)
		name, ok := row[0].(string)
		require.True(t, ok)
		rows[name] = row
	}

	require.Contains(t, rows, "promo_alpha")
	assert.Equal(t, "NODE", rows["promo_alpha"][1])
	assert.Equal(t, 1.25, rows["promo_alpha"][2])
	assert.Equal(t, 0.4, rows["promo_alpha"][3])
	assert.Equal(t, 0.95, rows["promo_alpha"][4])
	assert.Equal(t, true, rows["promo_alpha"][5])

	require.Contains(t, rows, "promo_beta")
	assert.Equal(t, "EDGE", rows["promo_beta"][1])
	assert.Equal(t, 2.0, rows["promo_beta"][2])
	assert.Equal(t, 0.2, rows["promo_beta"][3])
	assert.Equal(t, 0.99, rows["promo_beta"][4])
	assert.Equal(t, true, rows["promo_beta"][5])
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
		"CREATE DECAY PROFILE binding_alpha FOR (n:MemoryEpisode) APPLY { DECAY PROFILE 'profile_alpha' n.summary NO DECAY }",
		"CREATE PROMOTION PROFILE promo_alpha OPTIONS { multiplier: 1.25, scoreFloor: 0.4, scoreCap: 0.95, scope: 'NODE' }",
		"CREATE PROMOTION POLICY policy_alpha FOR (n:MemoryEpisode) APPLY { WHEN n.accessCount >= 3 APPLY PROFILE promo_alpha }",
	}
	for _, stmt := range stmts {
		_, err := exec.Execute(ctx, stmt, nil)
		require.NoError(t, err, stmt)
	}

	profiles, err := exec.Execute(ctx, "CALL nornicdb.knowledgepolicy.profiles()", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"kind", "Name", "HalfLifeSeconds", "VisibilityThreshold", "ScoreFloor", "Function", "Scope", "DecayEnabled", "ScoreFrom", "ScoreFromProperty", "Enabled", "TargetLabels", "TargetEdgeType", "IsWildcard", "IsEdge", "ProfileRef", "NoDecay", "Order", "Apply"}, profiles.Columns)
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
	assert.Equal(t, "DECAY PROFILE 'profile_alpha'\nn.summary NO DECAY", rowsByName["binding_alpha"][18])

	policies, err := exec.Execute(ctx, "CALL nornicdb.knowledgepolicy.policies()", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"kind", "Name", "Scope", "Multiplier", "ScoreFloor", "ScoreCap", "Enabled", "TargetLabels", "TargetEdgeType", "IsWildcard", "IsEdge", "Apply"}, policies.Columns)
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
	assert.Equal(t, "WHEN n.accessCount >= 3 APPLY PROFILE 'promo_alpha'", policyRowsByName["policy_alpha"][11])
}

func TestKnowledgePolicyApplyFormattingRoundTrips(t *testing.T) {
	visibilityThreshold := 0.25
	binding := knowledgepolicy.DecayProfileBinding{
		ProfileRef:          "decay_base",
		HalfLifeSeconds:     7200,
		ScoreFloor:          0.15,
		VisibilityThreshold: &visibilityThreshold,
		PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{
			{PropertyPath: "summary", NoDecay: true, Order: 0},
			{PropertyPath: "confidence", ProfileRef: "decay_slow", HalfLifeSeconds: 14400, ScoreFloor: 0.4, Order: 1},
		},
	}
	var parsedBinding knowledgepolicy.DecayProfileBinding
	require.NoError(t, parseBindingApplyBlock(formatDecayBindingApply(binding), &parsedBinding))
	assert.Equal(t, binding.ProfileRef, parsedBinding.ProfileRef)
	assert.Equal(t, binding.HalfLifeSeconds, parsedBinding.HalfLifeSeconds)
	assert.Equal(t, binding.ScoreFloor, parsedBinding.ScoreFloor)
	require.NotNil(t, parsedBinding.VisibilityThreshold)
	assert.Equal(t, *binding.VisibilityThreshold, *parsedBinding.VisibilityThreshold)
	assert.Equal(t, binding.PropertyRules, parsedBinding.PropertyRules)

	policy := knowledgepolicy.PromotionPolicyDef{
		OnAccess: &knowledgepolicy.PromotionPolicyOnAccess{
			Mutations: []knowledgepolicy.OnAccessMutation{
				{Expression: "n.accessCount = coalesce(n.accessCount, 0) + 1"},
				{
					Expression: "n.confidence = $evaluatedConfidence",
					Kalman: &knowledgepolicy.KalmanConfig{
						Mode: knowledgepolicy.KalmanModeManual, Q: 0.05, R: 50, VarianceScale: 5, WindowSize: 64,
					},
				},
			},
		},
		WhenClauses: []knowledgepolicy.PromotionPolicyWhenClause{
			{Predicate: "n.accessCount >= 5", ProfileRef: "boost", Order: 0},
		},
	}
	var parsedPolicy knowledgepolicy.PromotionPolicyDef
	require.NoError(t, parsePolicyApplyBlock(formatPromotionPolicyApply(policy), &parsedPolicy))
	assert.Equal(t, policy.OnAccess, parsedPolicy.OnAccess)
	assert.Equal(t, policy.WhenClauses, parsedPolicy.WhenClauses)
}

func TestE2E_KnowledgePolicyTargetAdmissionAcrossParsers(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			exec, ctx := newUnitExecutor(t)
			require.Equal(t, parser, config.GetParserType())
			for _, statement := range []string{
				"CREATE PROMOTION POLICY nofor",
				"CREATE PROMOTION POLICY nofor APPLY { ON ACCESS { SET n.c = 1 } }",
				"CREATE PROMOTION POLICY IF NOT EXISTS nofor APPLY { ON ACCESS { SET n.c = 1 } }",
				"CREATE PROMOTION POLICY nofor IF NOT EXISTS APPLY { ON ACCESS { SET n.c = 1 } }",
				"CREATE PROMOTION POLICY p FOR",
				"CREATE PROMOTION POLICY p FOR APPLY { ON ACCESS { SET n.c = 1 } }",
				"ALTER PROMOTION POLICY p FOR",
				"CREATE DECAY PROFILE d FOR",
				"ALTER DECAY PROFILE d FOR",
			} {
				_, err := exec.Execute(ctx, statement, nil)
				require.ErrorContains(t, err, "after", statement)
			}
			result, err := exec.Execute(ctx, "SHOW PROMOTION POLICIES", nil)
			require.NoError(t, err)
			require.Empty(t, result.Rows)
			for _, statement := range []string{
				"CREATE PROMOTION POLICY node_target FOR (n:Fact) APPLY { ON ACCESS { SET n.c = 1 } }",
				"CREATE PROMOTION POLICY edge_target FOR ()-[r:REFERENCES]-() APPLY { ON ACCESS { SET r.c = 1 } }",
				"CREATE PROMOTION POLICY wildcard_target FOR (n) APPLY { ON ACCESS { SET n.c = 1 } }",
				"CREATE PROMOTION POLICY IF NOT EXISTS wildcard_target FOR (n) APPLY { ON ACCESS { SET n.c = 1 } }",
				"CREATE PROMOTION POLICY wildcard_target IF NOT EXISTS FOR (n) APPLY { ON ACCESS { SET n.c = 1 } }",
			} {
				_, err := exec.Execute(ctx, statement, nil)
				require.NoError(t, err, statement)
			}
			result, err = exec.Execute(ctx, "SHOW PROMOTION POLICIES", nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 3)
			for _, statement := range []string{
				"BEGIN",
				"CREATE PROMOTION POLICY rolled_back FOR (n:Fact) APPLY { ON ACCESS { SET n.c = 1 } }",
				"ROLLBACK",
			} {
				_, err := exec.Execute(ctx, statement, nil)
				require.NoError(t, err, statement)
			}
			result, err = exec.Execute(ctx, "SHOW PROMOTION POLICIES", nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 3)
		})
	}
}

func TestE2E_KnowledgeProfileTrailingAdmissionAcrossParsers(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			exec, ctx := newUnitExecutor(t)
			require.Equal(t, parser, config.GetParserType())
			for _, statement := range []string{
				"CREATE DECAY PROFILE d OPTIONS { halfLifeSeconds: 3600, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED' } GARBAGE",
				"CREATE DECAY PROFILE binding FOR (n:Fact) APPLY { DECAY PROFILE 'd' } GARBAGE",
				"ALTER DECAY PROFILE binding FOR (n:Fact) APPLY { DECAY PROFILE 'd' } GARBAGE",
				"CREATE PROMOTION PROFILE p OPTIONS { multiplier: 1.5 } GARBAGE",
				"ALTER PROMOTION PROFILE p SET OPTIONS { multiplier: 1.5 } GARBAGE",
				"ALTER DECAY PROFILE d SET OPTIONS { halfLifeSeconds: 3600 } GARBAGE",
			} {
				_, err := exec.Execute(ctx, statement, nil)
				require.ErrorContains(t, err, "expected end of statement", statement)
			}
			for _, statement := range []string{"SHOW DECAY PROFILES", "SHOW PROMOTION PROFILES", "SHOW PROMOTION POLICIES"} {
				result, err := exec.Execute(ctx, statement, nil)
				require.NoError(t, err)
				require.Empty(t, result.Rows)
			}
		})
	}
}

func TestE2E_PromotionProfileOptionParity(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			require.Equal(t, parser, config.GetParserType())
			for _, options := range []string{
				"{ multiplier: 'abc' }",
				"{ scoreFloor: 'abc' }",
				"{ scoreCap: 'abc' }",
				"{ enabled: 'no' }",
				"{ scope: 'UNKNOWN' }",
				"{ nonsense: 5 }",
				"{ multiplier: 2.0, scoreFloor: 'abc' }",
			} {
				t.Run("reject "+options, func(t *testing.T) {
					exec, ctx := newUnitExecutor(t)
					_, err := exec.Execute(ctx, "CREATE PROMOTION PROFILE invalid OPTIONS "+options, nil)
					require.Error(t, err)
					_, err = exec.Execute(ctx, "CREATE PROMOTION PROFILE boost OPTIONS { multiplier: 1.5 }", nil)
					require.NoError(t, err)
					before, err := exec.Execute(ctx, "SHOW PROMOTION PROFILES", nil)
					require.NoError(t, err)
					_, err = exec.Execute(ctx, "ALTER PROMOTION PROFILE boost SET OPTIONS "+options, nil)
					require.Error(t, err)
					after, err := exec.Execute(ctx, "SHOW PROMOTION PROFILES", nil)
					require.NoError(t, err)
					require.Equal(t, before.Rows, after.Rows)
				})
			}
			for _, options := range []string{
				"{ scorefloor: 0.5 }",
				"{ scoreFloor: 1 }",
				"{ scope: 'NODE' }",
				"{ enabled: 0 }",
			} {
				t.Run("accept "+options, func(t *testing.T) {
					exec, ctx := newUnitExecutor(t)
					_, err := exec.Execute(ctx, "CREATE PROMOTION PROFILE boost OPTIONS { multiplier: 1.5 }", nil)
					require.NoError(t, err)
					_, err = exec.Execute(ctx, "CREATE PROMOTION PROFILE expected OPTIONS "+options, nil)
					require.NoError(t, err)
					_, err = exec.Execute(ctx, "ALTER PROMOTION PROFILE boost SET OPTIONS "+options, nil)
					require.NoError(t, err)
					result, err := exec.Execute(ctx, "SHOW PROMOTION PROFILES YIELD name, scoreFloor, scope, enabled RETURN name, scoreFloor, scope, enabled ORDER BY name", nil)
					require.NoError(t, err)
					require.Len(t, result.Rows, 2)
					require.Equal(t, result.Rows[1][1:], result.Rows[0][1:])
				})
			}
		})
	}
}

func TestE2E_KnowledgePolicyDDLAcrossParsers(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			exec, ctx := newUnitExecutor(t)
			require.Equal(t, parser, config.GetParserType())
			for _, statement := range []string{
				"CREATE DECAY PROFILE decay OPTIONS { halfLifeSeconds: 3600, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED' }",
				"CREATE PROMOTION PROFILE boost OPTIONS { multiplier: 1.5 }",
				"CREATE PROMOTION POLICY pg FOR (n:Fact) APPLY { ON ACCESS { SET n.c = 1 } }",
				"ALTER DECAY PROFILE decay SET OPTIONS { halfLifeSeconds: 7200 }",
				"ALTER PROMOTION PROFILE boost SET OPTIONS { multiplier: 2.0 }",
				"ALTER PROMOTION POLICY pg SET OPTIONS { enabled: true } DISABLE",
				"SHOW DECAY PROFILES",
				"SHOW PROMOTION PROFILES",
				"SHOW PROMOTION POLICIES YIELD name, enabled WHERE name = 'pg' RETURN enabled",
				"DROP PROMOTION POLICY pg",
				"DROP PROMOTION PROFILE boost",
				"DROP DECAY PROFILE decay",
			} {
				_, err := exec.Execute(ctx, statement, nil)
				require.NoError(t, err, statement)
			}
			_, err := exec.Execute(ctx, "ALTER PROMOTION POLICY pg DISABEL", nil)
			require.Error(t, err)
		})
	}
}

func TestE2E_AlterPromotionPolicyOptionAdmission(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	_, err := exec.Execute(ctx, "CREATE PROMOTION POLICY pg FOR (n:Fact) APPLY { ON ACCESS { SET n.c = 1 } }", nil)
	require.NoError(t, err)
	assertEnabled := func(t *testing.T, expected bool) {
		result, err := exec.Execute(ctx, "SHOW PROMOTION POLICIES YIELD name, enabled WHERE name = 'pg' RETURN enabled", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{expected}}, result.Rows)
	}
	for _, options := range []string{
		"{ nonsense: 5 }",
		"{ enabled: 'no' }",
		"{ enabled: null }",
		"{ enabled: 'no' } ENABLE",
		"{ enabled: null } DISABLE",
		"{ enabled: false, nonsense: 5 }",
	} {
		t.Run(options, func(t *testing.T) {
			_, err := exec.Execute(ctx, "ALTER PROMOTION POLICY pg SET OPTIONS "+options, nil)
			require.Error(t, err)
			assertEnabled(t, true)
		})
	}
	for _, edit := range []struct {
		clause  string
		enabled bool
	}{
		{"SET OPTIONS { enabled: true } DISABLE", false},
		{"SET OPTIONS { enabled: false } ENABLE", true},
	} {
		_, err := exec.Execute(ctx, "ALTER PROMOTION POLICY pg "+edit.clause, nil)
		require.NoError(t, err)
		assertEnabled(t, edit.enabled)
	}
}

func TestE2E_AlterKnowledgePolicyDefinitions(t *testing.T) {
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })

	exec := NewStorageExecutor(storage.NewNamespacedEngine(be, "test"))
	ctx := context.Background()
	statements := []string{
		"CREATE DECAY PROFILE decay_a OPTIONS { halfLifeSeconds: 3600, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED' }",
		"CREATE DECAY PROFILE decay_b OPTIONS { halfLifeSeconds: 7200, function: 'linear', scope: 'NODE', scoreFrom: 'CREATED' }",
		"CREATE DECAY PROFILE editable_binding FOR (n:Original) APPLY { DECAY PROFILE 'decay_a' }",
		"CREATE PROMOTION PROFILE boost OPTIONS { multiplier: 1.5, scoreFloor: 0.1, scoreCap: 1.0, scope: 'NODE' }",
		"CREATE PROMOTION POLICY editable_policy FOR (n:Original) APPLY { WHEN n.accessCount > 1 APPLY PROFILE 'boost' }",
		"ALTER PROMOTION POLICY editable_policy DISABLE",
		"ALTER DECAY PROFILE editable_binding FOR (n:KnowledgeFact:Reviewed) APPLY { DECAY PROFILE 'decay_b' n.summary NO DECAY }",
		"ALTER PROMOTION POLICY editable_policy FOR ()-[r:REFERENCES]-() APPLY { ON ACCESS { SET r.accessCount = coalesce(r.accessCount, 0) + 1 } WHEN r.accessCount >= 5 APPLY PROFILE 'boost' }",
	}
	for _, statement := range statements {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}

	profiles, err := exec.Execute(ctx, "CALL nornicdb.knowledgepolicy.profiles()", nil)
	require.NoError(t, err)
	var bindingRow []interface{}
	for _, row := range profiles.Rows {
		if row[1] == "editable_binding" {
			bindingRow = row
		}
	}
	require.NotNil(t, bindingRow)
	assert.Equal(t, []string{"KnowledgeFact", "Reviewed"}, bindingRow[11])
	assert.Equal(t, "decay_b", bindingRow[15])
	assert.Contains(t, bindingRow[18], "n.summary NO DECAY")

	policyResult, err := exec.Execute(ctx, "CALL nornicdb.knowledgepolicy.policies()", nil)
	require.NoError(t, err)
	var policyRow []interface{}
	for _, row := range policyResult.Rows {
		if row[1] == "editable_policy" {
			policyRow = row
		}
	}
	require.NotNil(t, policyRow)
	assert.Equal(t, "REFERENCES", policyRow[8])
	assert.Equal(t, false, policyRow[6])
	assert.Contains(t, policyRow[11], "ON ACCESS")
	assert.Contains(t, policyRow[11], "WHEN r.accessCount >= 5")
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

func TestE2E_CallNornicDbKnowledgePolicyResolve_AcceptsNeo4jElementIDs(t *testing.T) {
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })

	store := storage.NewNamespacedEngine(be, "nornic")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	_, err = exec.Execute(ctx, "CREATE DECAY PROFILE profile_alpha OPTIONS { halfLifeSeconds: 3600, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED' }", nil)
	require.NoError(t, err)

	createdAt := time.Unix(1700000000, 0)
	for _, nodeID := range []storage.NodeID{"node-1", "node-2"} {
		_, err = store.CreateNode(&storage.Node{ID: nodeID, Labels: []string{"MemoryEpisode"}, CreatedAt: createdAt})
		require.NoError(t, err)
	}
	edgeID := storage.EdgeID("edge-1")
	require.NoError(t, store.CreateEdge(&storage.Edge{
		ID: edgeID, StartNode: "node-1", EndNode: "node-2", Type: "REFERENCES", CreatedAt: createdAt,
	}))

	tests := []struct {
		name      string
		entityID  string
		wantID    string
		wantScope string
	}{
		{name: "raw node ID", entityID: "node-1", wantID: "node-1", wantScope: "NODE"},
		{name: "node element ID", entityID: "4:nornicdb:node-1", wantID: "4:nornicdb:node-1", wantScope: "NODE"},
		{name: "raw relationship ID", entityID: "edge-1", wantID: "edge-1", wantScope: "EDGE"},
		{name: "relationship element ID", entityID: "5:nornicdb:edge-1", wantID: "5:nornicdb:edge-1", wantScope: "EDGE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, resolveErr := exec.Execute(ctx, fmt.Sprintf("CALL nornicdb.knowledgepolicy.resolve('%s', '', '')", tt.entityID), nil)
			require.NoError(t, resolveErr)
			require.Len(t, result.Rows, 1)
			assert.Equal(t, tt.wantID, result.Rows[0][0])
			assert.Equal(t, tt.wantScope, result.Rows[0][1])
		})
	}

	_, err = exec.Execute(ctx, "CALL nornicdb.knowledgepolicy.resolve('4:nornicdb:edge-1', '', '')", nil)
	require.EqualError(t, err, "Failed to invoke procedure `nornicdb.knowledgepolicy.resolve`: Caused by: entity not found: 4:nornicdb:edge-1")
	_, err = exec.Execute(ctx, "CALL nornicdb.knowledgepolicy.resolve('5:nornicdb:node-1', '', '')", nil)
	require.EqualError(t, err, "Failed to invoke procedure `nornicdb.knowledgepolicy.resolve`: Caused by: entity not found: 5:nornicdb:node-1")
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

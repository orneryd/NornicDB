package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestKnowledgePolicySchemaLookupErrorBoundaries(t *testing.T) {
	base := newTestMemoryEngine(t)
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("lookup"))
	executor := NewStorageExecutor(storage.NewNamespacedEngine(base, "lookup"))
	executor.txContext = &TransactionContext{tx: tx, active: true}
	view, err := executor.knowledgePolicySchema()
	require.NoError(t, err)
	require.NotNil(t, view)
	wrongNamespace := NewStorageExecutor(&transactionStorageWrapper{tx: tx, underlying: base, namespace: "other"})
	_, err = wrongNamespace.knowledgePolicySchema()
	require.Error(t, err)
	require.NoError(t, tx.Rollback())
	for _, procedure := range []func() (*ExecuteResult, error){
		executor.callNornicDbKnowledgePolicyInfo,
		executor.callNornicDbKnowledgePolicyProfiles,
		executor.callNornicDbKnowledgePolicyPolicies,
	} {
		_, err := procedure()
		require.Error(t, err, "closed transaction introspection must not silently fall back to committed schema")
	}
}

func TestSchemaMutationHelperErrorBoundaries(t *testing.T) {
	base := newTestMemoryEngine(t)
	require.NoError(t, NewStorageExecutor(base).prepareSchemaTransaction(context.Background()))
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("mutation"))
	wrongNamespace := NewStorageExecutor(&transactionStorageWrapper{tx: tx, underlying: base, namespace: "other"})
	ctx := context.Background()
	called := false
	err = wrongNamespace.mutateSchema(ctx, func(*storage.SchemaManager) error {
		called = true
		return nil
	})
	require.Error(t, err)
	require.False(t, called)
	executor := NewStorageExecutor(&transactionStorageWrapper{tx: tx, underlying: base, namespace: "mutation"})
	err = executor.mutateSchema(ctx, func(*storage.SchemaManager) error { return storage.ErrInvalidData })
	require.ErrorIs(t, err, storage.ErrInvalidData)
	err = executor.mutateSchema(ctx, func(*storage.SchemaManager) error { return tx.Rollback() })
	require.Error(t, err, "staging must refuse a transaction closed during its schema callback")
	require.Error(t, executor.prepareSchemaTransaction(ctx))
	tx, err = base.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&storage.Node{ID: "mutation:data"})
	require.NoError(t, err)
	executor = NewStorageExecutor(&transactionStorageWrapper{tx: tx, underlying: base, namespace: "mutation"})
	require.ErrorContains(t, executor.prepareSchemaTransaction(ctx), "ForbiddenDueToTransactionType")
	require.NoError(t, tx.Rollback())
}

func TestKnowledgePolicyExecuteDDL_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	fixtureAdmission1236Context, fixtureAdmission1236 := statementTestContext(ctx, "MATCH (n) RETURN n")
	_ = fixtureAdmission1236Context
	_, err := exec.executeKnowledgePolicyDDL(fixtureAdmission1236, fixtureAdmission1236Context, "MATCH (n) RETURN n")
	require.EqualError(t, err, "unsupported knowledge policy command: MATCH (n) RETURN n")

	fixtureAdmission1237Context, fixtureAdmission1237 := statementTestContext(ctx, `CREATE DECAY PROFILE bad OPTIONS { function: 'bogus', scope: 'NODE', scoreFrom: 'CREATED' }`)
	_ = fixtureAdmission1237Context
	_, err = exec.executeKnowledgePolicyDDL(fixtureAdmission1237, fixtureAdmission1237Context, `CREATE DECAY PROFILE bad OPTIONS { function: 'bogus', scope: 'NODE', scoreFrom: 'CREATED' }`)
	require.ErrorContains(t, err, "invalid decay function")

	fixtureAdmission1238Context, fixtureAdmission1238 := statementTestContext(ctx, `CREATE DECAY PROFILE base OPTIONS {
		halfLifeSeconds: 3600,
		function: 'none',
		scope: 'NODE',
		scoreFrom: 'CREATED'
	}`)
	_ = fixtureAdmission1238Context
	_, err = exec.executeKnowledgePolicyDDL(fixtureAdmission1238, fixtureAdmission1238Context, `CREATE DECAY PROFILE base OPTIONS {
		halfLifeSeconds: 3600,
		function: 'none',
		scope: 'NODE',
		scoreFrom: 'CREATED'
	}`)
	require.NoError(t, err)

	fixtureAdmission1239Context, fixtureAdmission1239 := statementTestContext(ctx, `CREATE DECAY PROFILE base_binding FOR (n:Fact) APPLY {
		DECAY PROFILE 'base'
	}`)
	_ = fixtureAdmission1239Context
	_, err = exec.executeKnowledgePolicyDDL(fixtureAdmission1239, fixtureAdmission1239Context, `CREATE DECAY PROFILE base_binding FOR (n:Fact) APPLY {
		DECAY PROFILE 'base'
	}`)
	require.NoError(t, err)

	fixtureAdmission1240Context, fixtureAdmission1240 := statementTestContext(ctx, `ALTER DECAY PROFILE base SET OPTIONS { enabled: false }`)
	_ = fixtureAdmission1240Context
	_, err = exec.executeKnowledgePolicyDDL(fixtureAdmission1240, fixtureAdmission1240Context, `ALTER DECAY PROFILE base SET OPTIONS { enabled: false }`)
	require.NoError(t, err)

	fixtureAdmission1241Context, fixtureAdmission1241 := statementTestContext(ctx, `SHOW DECAY PROFILES`)
	_ = fixtureAdmission1241Context
	showDecay, err := exec.executeKnowledgePolicyDDL(fixtureAdmission1241, fixtureAdmission1241Context, `SHOW DECAY PROFILES`)
	require.NoError(t, err)
	require.Equal(t, []string{"kind", "name", "scope", "target", "profileRef", "enabled"}, showDecay.Columns)
	require.Len(t, showDecay.Rows, 2)

	rowsByName := map[string][]interface{}{}
	for _, row := range showDecay.Rows {
		rowsByName[row[1].(string)] = row
	}
	require.Equal(t, "bundle", rowsByName["base"][0])
	require.Equal(t, "NODE", rowsByName["base"][2])
	require.Equal(t, "", rowsByName["base"][3])
	require.Equal(t, "binding", rowsByName["base_binding"][0])
	require.Equal(t, "Fact", rowsByName["base_binding"][3])
	require.Equal(t, "base", rowsByName["base_binding"][4])

	fixtureAdmission1242Context, fixtureAdmission1242 := statementTestContext(ctx, `CREATE PROMOTION PROFILE boost OPTIONS {
		scope: 'NODE',
		multiplier: 1.5,
		scoreFloor: 0.1,
		scoreCap: 0.9
	}`)
	_ = fixtureAdmission1242Context
	_, err = exec.executeKnowledgePolicyDDL(fixtureAdmission1242, fixtureAdmission1242Context, `CREATE PROMOTION PROFILE boost OPTIONS {
		scope: 'NODE',
		multiplier: 1.5,
		scoreFloor: 0.1,
		scoreCap: 0.9
	}`)
	require.NoError(t, err)
	fixtureAdmission1243Context, fixtureAdmission1243 := statementTestContext(ctx, `ALTER PROMOTION PROFILE boost SET OPTIONS { multiplier: 1.2, enabled: false }`)
	_ = fixtureAdmission1243Context
	_, err = exec.executeKnowledgePolicyDDL(fixtureAdmission1243, fixtureAdmission1243Context, `ALTER PROMOTION PROFILE boost SET OPTIONS { multiplier: 1.2, enabled: false }`)
	require.NoError(t, err)

	fixtureAdmission1244Context, fixtureAdmission1244 := statementTestContext(ctx, `CREATE PROMOTION POLICY fact_promote FOR (n:Fact) APPLY {
		ON ACCESS {
			SET n.accessCount = n.accessCount + 1
		}
	}`)
	_ = fixtureAdmission1244Context
	_, err = exec.executeKnowledgePolicyDDL(fixtureAdmission1244, fixtureAdmission1244Context, `CREATE PROMOTION POLICY fact_promote FOR (n:Fact) APPLY {
		ON ACCESS {
			SET n.accessCount = n.accessCount + 1
		}
	}`)
	require.NoError(t, err)
	fixtureAdmission1245Context, fixtureAdmission1245 := statementTestContext(ctx, `ALTER PROMOTION POLICY fact_promote SET OPTIONS { enabled: false }`)
	_ = fixtureAdmission1245Context
	_, err = exec.executeKnowledgePolicyDDL(fixtureAdmission1245, fixtureAdmission1245Context, `ALTER PROMOTION POLICY fact_promote SET OPTIONS { enabled: false }`)
	require.NoError(t, err)

	fixtureAdmission1246Context, fixtureAdmission1246 := statementTestContext(ctx, `SHOW PROMOTION PROFILES`)
	_ = fixtureAdmission1246Context
	showPromotionProfiles, err := exec.executeKnowledgePolicyDDL(fixtureAdmission1246, fixtureAdmission1246Context, `SHOW PROMOTION PROFILES`)
	require.NoError(t, err)
	require.Equal(t, []string{"name", "scope", "multiplier", "scoreFloor", "scoreCap", "enabled"}, showPromotionProfiles.Columns)
	require.Len(t, showPromotionProfiles.Rows, 1)
	require.Equal(t, "boost", showPromotionProfiles.Rows[0][0])
	require.Equal(t, "NODE", showPromotionProfiles.Rows[0][1])

	fixtureAdmission1247Context, fixtureAdmission1247 := statementTestContext(ctx, `SHOW PROMOTION POLICIES`)
	_ = fixtureAdmission1247Context
	showPolicies, err := exec.executeKnowledgePolicyDDL(fixtureAdmission1247, fixtureAdmission1247Context, `SHOW PROMOTION POLICIES`)
	require.NoError(t, err)
	require.Equal(t, []string{"name", "scope", "target", "enabled", "whenClauses", "onAccessMutations"}, showPolicies.Columns)
	require.Len(t, showPolicies.Rows, 1)
	require.Equal(t, "fact_promote", showPolicies.Rows[0][0])
	require.Equal(t, "Fact", showPolicies.Rows[0][2])
	require.EqualValues(t, int64(1), showPolicies.Rows[0][5])

	fixtureAdmission1248Context, fixtureAdmission1248 := statementTestContext(ctx, `DROP PROMOTION POLICY IF EXISTS fact_promote`)
	_ = fixtureAdmission1248Context
	_, err = exec.executeKnowledgePolicyDDL(fixtureAdmission1248, fixtureAdmission1248Context, `DROP PROMOTION POLICY IF EXISTS fact_promote`)
	require.NoError(t, err)
	fixtureAdmission1249Context, fixtureAdmission1249 := statementTestContext(ctx, `DROP PROMOTION PROFILE IF EXISTS boost`)
	_ = fixtureAdmission1249Context
	_, err = exec.executeKnowledgePolicyDDL(fixtureAdmission1249, fixtureAdmission1249Context, `DROP PROMOTION PROFILE IF EXISTS boost`)
	require.NoError(t, err)
	fixtureAdmission1250Context, fixtureAdmission1250 := statementTestContext(ctx, `DROP DECAY PROFILE IF EXISTS base_binding`)
	_ = fixtureAdmission1250Context
	_, err = exec.executeKnowledgePolicyDDL(fixtureAdmission1250, fixtureAdmission1250Context, `DROP DECAY PROFILE IF EXISTS base_binding`)
	require.NoError(t, err)
	fixtureAdmission1251Context, fixtureAdmission1251 := statementTestContext(ctx, `DROP DECAY PROFILE IF EXISTS base`)
	_ = fixtureAdmission1251Context
	_, err = exec.executeKnowledgePolicyDDL(fixtureAdmission1251, fixtureAdmission1251Context, `DROP DECAY PROFILE IF EXISTS base`)
	require.NoError(t, err)
}

func TestKnowledgePolicyCreatePromotionPolicy_RepeatedDDLDoesNotBlockWrites(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	create := "CREATE PROMOTION POLICY repeat_policy FOR (n:Fact) APPLY { ON ACCESS { SET n.accessCount = n.accessCount + 1 } }"
	_, err := exec.Execute(ctx, create, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, create, nil)
	require.Error(t, err)
	for _, stmt := range []string{
		"CREATE PROMOTION POLICY IF NOT EXISTS repeat_policy FOR (n:Fact) APPLY { ON ACCESS { SET n.accessCount = n.accessCount + 1 } }",
		"CREATE PROMOTION POLICY repeat_policy IF NOT EXISTS FOR (n:Fact) APPLY { ON ACCESS { SET n.accessCount = n.accessCount + 1 } }",
	} {
		_, err = exec.Execute(ctx, stmt, nil)
		require.NoError(t, err)
	}
	policies := exec.storage.GetSchema().ShowPromotionPolicies()
	require.Len(t, policies, 1)
	require.Equal(t, "repeat_policy", policies[0].Name)
	_, err = exec.Execute(ctx, "CREATE (:Other {name: 'after bootstrap'})", nil)
	require.NoError(t, err)
}

func TestKnowledgePolicyProcedure_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	res, err := exec.callNornicDbKnowledgePolicyDeindexStatus()
	require.NoError(t, err)
	require.Equal(t, []string{"pending_count", "supported", "message", "workItemId", "targetId", "targetScope", "enqueuedAt", "status"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, false, res.Rows[0][1])
	require.Equal(t, "deindex status requires BadgerDB storage backend", res.Rows[0][2])

	_, err = exec.callNornicDbKnowledgePolicyResolve(nil)
	require.EqualError(t, err, "kp.resolve requires entityId, labels, or edgeType")

	_, err = exec.callNornicDbKnowledgePolicyResolve([]interface{}{123})
	require.EqualError(t, err, "argument 1 must be a string")

	_, err = exec.callNornicDbKnowledgePolicyResolve([]interface{}{"missing-entity"})
	require.EqualError(t, err, "knowledge policy binding table unavailable")

	fixtureAdmission1252Context, fixtureAdmission1252 := statementTestContext(ctx, `CREATE DECAY PROFILE base OPTIONS {
		halfLifeSeconds: 3600,
		function: 'none',
		scope: 'NODE',
		scoreFrom: 'CREATED'
	}`)
	_ = fixtureAdmission1252Context
	_, err = exec.executeKnowledgePolicyDDL(fixtureAdmission1252, fixtureAdmission1252Context, `CREATE DECAY PROFILE base OPTIONS {
		halfLifeSeconds: 3600,
		function: 'none',
		scope: 'NODE',
		scoreFrom: 'CREATED'
	}`)
	require.NoError(t, err)
	fixtureAdmission1253Context, fixtureAdmission1253 := statementTestContext(ctx, `CREATE DECAY PROFILE base_binding FOR (n:Fact) APPLY { DECAY PROFILE 'base' }`)
	_ = fixtureAdmission1253Context
	_, err = exec.executeKnowledgePolicyDDL(fixtureAdmission1253, fixtureAdmission1253Context, `CREATE DECAY PROFILE base_binding FOR (n:Fact) APPLY { DECAY PROFILE 'base' }`)
	require.NoError(t, err)
	fixtureAdmission1254Context, fixtureAdmission1254 := statementTestContext(ctx, `CREATE PROMOTION PROFILE boost OPTIONS { scope: 'NODE', multiplier: 1.1, scoreFloor: 0.1, scoreCap: 0.9 }`)
	_ = fixtureAdmission1254Context
	_, err = exec.executeKnowledgePolicyDDL(fixtureAdmission1254, fixtureAdmission1254Context, `CREATE PROMOTION PROFILE boost OPTIONS { scope: 'NODE', multiplier: 1.1, scoreFloor: 0.1, scoreCap: 0.9 }`)
	require.NoError(t, err)
	fixtureAdmission1255Context, fixtureAdmission1255 := statementTestContext(ctx, `CREATE PROMOTION POLICY fact_promote FOR (n:Fact) APPLY { ON ACCESS { SET n.accessCount = n.accessCount + 1 } }`)
	_ = fixtureAdmission1255Context
	_, err = exec.executeKnowledgePolicyDDL(fixtureAdmission1255, fixtureAdmission1255Context, `CREATE PROMOTION POLICY fact_promote FOR (n:Fact) APPLY { ON ACCESS { SET n.accessCount = n.accessCount + 1 } }`)
	require.NoError(t, err)

	profiles, err := exec.callNornicDbKnowledgePolicyProfiles()
	require.NoError(t, err)
	require.Equal(t, []string{"kind", "Name", "HalfLifeSeconds", "VisibilityThreshold", "ScoreFloor", "Function", "Scope", "DecayEnabled", "ScoreFrom", "ScoreFromProperty", "Enabled", "TargetLabels", "TargetEdgeType", "IsWildcard", "IsEdge", "ProfileRef", "NoDecay", "Order", "Apply"}, profiles.Columns)
	require.Len(t, profiles.Rows, 2)

	policies, err := exec.callNornicDbKnowledgePolicyPolicies()
	require.NoError(t, err)
	require.Equal(t, []string{"kind", "Name", "Scope", "Multiplier", "ScoreFloor", "ScoreCap", "Enabled", "TargetLabels", "TargetEdgeType", "IsWildcard", "IsEdge", "Apply"}, policies.Columns)
	require.Len(t, policies.Rows, 2)

	_, err = exec.callNornicDbKnowledgePolicyResolve([]interface{}{"missing-entity"})
	require.EqualError(t, err, "entity not found: missing-entity")
	resolveByLabels, err := exec.callNornicDbKnowledgePolicyResolve([]interface{}{"", "Fact"})
	require.NoError(t, err)
	require.Len(t, resolveByLabels.Rows, 1)
	require.Equal(t, "dry-run", resolveByLabels.Rows[0][0])
	resolveByEdge, err := exec.callNornicDbKnowledgePolicyResolve([]interface{}{"", "", "LINKS"})
	require.NoError(t, err)
	require.Len(t, resolveByEdge.Rows, 1)
	require.Equal(t, "dry-run", resolveByEdge.Rows[0][0])

	s, err := optionalStringArg([]interface{}{"  x  "}, 0)
	require.NoError(t, err)
	require.Equal(t, "x", s)

	empty, err := optionalStringArg([]interface{}{nil}, 0)
	require.NoError(t, err)
	require.Empty(t, empty)

	_, err = optionalStringArg([]interface{}{1.25}, 0)
	require.EqualError(t, err, "argument 1 must be a string")
}

func TestKnowledgePolicyFunctionHelpers_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	now := time.Now().UTC()
	node := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Fact"}, CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-1 * time.Hour)}
	edge := &storage.Edge{ID: storage.EdgeID("e1"), Type: "REL", CreatedAt: now.Add(-30 * time.Minute)}

	ent, ok := exec.resolveEntityForDecay("n", map[string]*storage.Node{"n": node}, nil)
	require.True(t, ok)
	require.Equal(t, "test:n1", ent.entityID)
	require.Equal(t, []string{"Fact"}, ent.labels)
	require.False(t, ent.isEdge)

	ent, ok = exec.resolveEntityForDecay("r", nil, map[string]*storage.Edge{"r": edge})
	require.True(t, ok)
	require.Equal(t, "test:e1", ent.entityID)
	require.True(t, ent.isEdge)
	require.Equal(t, "REL", ent.edgeType)

	_, ok = exec.resolveEntityForDecay("missing", nil, nil)
	require.False(t, ok)

	be, nowNanos, ok := exec.getDecayContext(context.Background())
	require.False(t, ok)
	require.Nil(t, be)
	require.Zero(t, nowNanos)

	require.Equal(t, map[string]interface{}{"score": 1.0, "applies": false, "reason": "because"}, decayDisabledMap("because"))

	mapped := resolutionToMap(knowledgepolicy.ScoringResolution{NoDecay: true, FinalScore: 0.8, TargetScope: knowledgepolicy.ScopeNode})
	require.Equal(t, "no decay", mapped["reason"])
	require.Equal(t, false, mapped["applies"])

	mapped = resolutionToMap(knowledgepolicy.ScoringResolution{ResolvedDecayProfileID: "bundle1", FinalScore: 0.9, TargetScope: knowledgepolicy.ScopeEdge})
	require.Equal(t, "", mapped["reason"])
	require.Equal(t, true, mapped["applies"])
	require.Equal(t, "EDGE", mapped["scope"])

	metaMap := accessMetaToMap(&knowledgepolicy.AccessMetaEntry{TargetID: "node-1", TargetScope: knowledgepolicy.ScopeNode, Fixed: knowledgepolicy.AccessMetaFixedFields{AccessCount: 4, TraversalCount: 2, LastAccessedAt: 33}, LastMutatedAt: 44, MutationCount: 5})
	require.Equal(t, "node-1", metaMap["targetId"])
	require.EqualValues(t, int64(4), metaMap["accessCount"])

	require.Equal(t, map[string]interface{}{"targetId": "abc", "targetScope": "NODE"}, minimalPolicyMap("abc"))

	fixtureAdmission1897Context, fixtureAdmission1897 := expressionTestContext(context.Background(), "{property: 'title'}")
	_ = fixtureAdmission1897Context
	property, err := validateDecayOptions(fixtureAdmission1897, fixtureAdmission1897Context, "{property: 'title'}", nil, nil, exec)
	require.NoError(t, err)
	require.Equal(t, "title", property)

	fixtureAdmission1898Context, fixtureAdmission1898 := expressionTestContext(context.Background(), "{other: 'x'}")
	_ = fixtureAdmission1898Context
	_, err = validateDecayOptions(fixtureAdmission1898, fixtureAdmission1898Context, "{other: 'x'}", nil, nil, exec)
	require.EqualError(t, err, `unknown decay option key: "other"`)

	fixtureAdmission1899Context, fixtureAdmission1899 := expressionTestContext(context.Background(), "42")
	_ = fixtureAdmission1899Context
	_, err = validateDecayOptions(fixtureAdmission1899, fixtureAdmission1899Context, "42", nil, nil, exec)
	require.ErrorContains(t, err, "decayScore/decay options must be a map")

	require.Equal(t, "NODE", bindingScope(knowledgepolicy.DecayProfileBinding{}))
	require.Equal(t, "EDGE", bindingScope(knowledgepolicy.DecayProfileBinding{IsEdge: true}))
	require.Equal(t, "*", bindingTarget(knowledgepolicy.DecayProfileBinding{IsWildcard: true}))
	require.Equal(t, "REL", bindingTarget(knowledgepolicy.DecayProfileBinding{IsEdge: true, TargetEdgeType: "REL"}))
	require.Equal(t, "A:B", bindingTarget(knowledgepolicy.DecayProfileBinding{TargetLabels: []string{"A", "B"}}))

	require.Equal(t, "NODE", promotionPolicyScope(knowledgepolicy.PromotionPolicyDef{}))
	require.Equal(t, "EDGE", promotionPolicyScope(knowledgepolicy.PromotionPolicyDef{IsEdge: true}))
	require.Equal(t, "*", promotionPolicyTarget(knowledgepolicy.PromotionPolicyDef{IsWildcard: true}))
	require.Equal(t, "REL", promotionPolicyTarget(knowledgepolicy.PromotionPolicyDef{IsEdge: true, TargetEdgeType: "REL"}))
	require.Equal(t, "A:B", promotionPolicyTarget(knowledgepolicy.PromotionPolicyDef{TargetLabels: []string{"A", "B"}}))

	// Unknown function call branch.
	fixtureAdmission1641Context, fixtureAdmission1641 := expressionTestContext(context.Background(), "noop(n)")
	_ = fixtureAdmission1641Context
	v, handled := exec.evaluateKnowledgePolicyFunction(fixtureAdmission1641, fixtureAdmission1641Context, "noop(n)", "noop(n)", map[string]*storage.Node{"n": node}, map[string]*storage.Edge{"r": edge})
	require.False(t, handled)
	require.Nil(t, v)

	// Memory engine has no decay context, so decay helpers return neutral fallback values.
	fixtureAdmission1642Context, fixtureAdmission1642 := expressionTestContext(context.Background(), "decayScore(n, {unknown:'x'})")
	_ = fixtureAdmission1642Context
	decayScore := exec.evalDecayScore(fixtureAdmission1642, fixtureAdmission1642Context, "decayScore(n, {unknown:'x'})", map[string]*storage.Node{"n": node}, nil)
	require.Equal(t, 1.0, decayScore)
	fixtureAdmission1643Context, fixtureAdmission1643 := expressionTestContext(context.Background(), "decay(n, {unknown:'x'})")
	_ = fixtureAdmission1643Context
	decay := exec.evalDecay(fixtureAdmission1643, fixtureAdmission1643Context, "decay(n, {unknown:'x'})", map[string]*storage.Node{"n": node}, nil)
	require.Equal(t, map[string]interface{}{"score": 1.0, "applies": false, "reason": "no BadgerEngine"}, decay)

	// Policy fallbacks on missing entity and missing argument.
	policy := exec.evalPolicy(context.Background(), "policy()", map[string]*storage.Node{"n": node}, nil)
	require.Equal(t, map[string]interface{}{"targetId": "", "targetScope": "NODE"}, policy)
	policy = exec.evalPolicy(context.Background(), "policy(missing)", map[string]*storage.Node{"n": node}, nil)
	require.Equal(t, map[string]interface{}{"targetId": "", "targetScope": "NODE"}, policy)
}

package cypher

import (
	"context"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The error and edge branches of the dynamic-token owner (dynamic_tokens.go)
// and its callers, reached directly (#907).
func TestDynamicTokenBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dynamic_branches"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:DB {id: 1})-[:DBR]->(:DB {id: 2})", nil)
	require.NoError(t, err)
	store := exec.getStorage(ctx)
	nodes, err := store.GetNodesByLabel("DB")
	require.NoError(t, err)
	edges, err := store.GetEdgesByType("DBR")
	require.NoError(t, err)
	node, edge := nodes[0], edges[0]
	requireCode := func(t *testing.T, err error, code string) {
		t.Helper()
		require.Error(t, err)
		got, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, code, got, err.Error())
	}

	// A parameter that wasn't supplied, in every reader of a dynamic token.
	withParams := withParams(ctx, map[string]interface{}{"other": 1})
	fixtureAdmission1140Context, fixtureAdmission1140 := expressionTestContext(withParams, "$missing")
	_ = fixtureAdmission1140Context
	_, err = exec.dynamicTokenValue(fixtureAdmission1140, fixtureAdmission1140Context, "$missing", nil, nil)
	require.Error(t, err)
	fixtureAdmission1881Context, fixtureAdmission1881 := statementTestContext(withParams, "RETURN "+("$missing"))
	_ = fixtureAdmission1881Context
	_, err = exec.chainLabelNames(fixtureAdmission1881, fixtureAdmission1881Context, []labelChainItem{{expression: "$missing"}}, nil, nil)
	require.Error(t, err)
	fixtureAdmission1141Context, fixtureAdmission1141 := expressionTestContext(withParams, "$missing")
	_ = fixtureAdmission1141Context
	_, err = exec.dynamicPropertyKeyOf(fixtureAdmission1141, fixtureAdmission1141Context, "$missing", nil, nil, false)
	require.Error(t, err)
	fixtureAdmission1142Context, fixtureAdmission1142 := traversalTestContext(withParams, "(n:$($missing))", "", nil)
	_ = fixtureAdmission1142Context
	_, err = exec.resolveRowDynamicTokens(fixtureAdmission1142, fixtureAdmission1142Context, "(n:$($missing))", nil, nil)
	require.Error(t, err)
	fixtureAdmission1143Context, fixtureAdmission1143 := traversalTestContext(ctx, "(n:$(null))", "", nil)
	_ = fixtureAdmission1143Context
	_, err = exec.resolveRowDynamicTokens(fixtureAdmission1143, fixtureAdmission1143Context, "(n:$(null))", nil, nil)
	requireCode(t, err, "Neo.ClientError.Statement.TypeError")

	// The pattern resolver leaves quoted text, parameters and unbalanced text.
	fixtureAdmission1144Context, fixtureAdmission1144 := traversalTestContext(ctx, "(`n`:$('A') $props)-[:$('R')]->(m:X)", "", nil)
	_ = fixtureAdmission1144Context
	resolved, err := exec.resolveRowDynamicTokens(fixtureAdmission1144, fixtureAdmission1144Context, "(`n`:$('A') $props)-[:$('R')]->(m:X)", nil, nil)
	require.NoError(t, err)
	require.Equal(t, "(`n`:A $props)-[:R]->(m:X)", resolved)
	fixtureAdmission1145Context, fixtureAdmission1145 := traversalTestContext(ctx, "(n:$('A')", "", nil)
	_ = fixtureAdmission1145Context
	resolved, err = exec.resolveRowDynamicTokens(fixtureAdmission1145, fixtureAdmission1145Context, "(n:$('A')", nil, nil)
	require.NoError(t, err)
	require.Equal(t, "(n:A", resolved)
	fixtureAdmission1146Context, fixtureAdmission1146 := traversalTestContext(ctx, "(n:$(", "", nil)
	_ = fixtureAdmission1146Context
	resolved, err = exec.resolveRowDynamicTokens(fixtureAdmission1146, fixtureAdmission1146Context, "(n:$(", nil, nil)
	require.NoError(t, err)
	require.Equal(t, "(n:$(", resolved)
	fixtureAdmission1147Context, fixtureAdmission1147 := traversalTestContext(ctx, "(n:Fixed: $([]))", "", nil)
	_ = fixtureAdmission1147Context
	resolved, err = exec.resolveRowDynamicTokens(fixtureAdmission1147, fixtureAdmission1147Context, "(n:Fixed: $([]))", nil, nil)
	require.NoError(t, err)
	require.Equal(t, "(n:Fixed)", resolved)
	fixtureAdmission1148Context, fixtureAdmission1148 := traversalTestContext(ctx, "(n {k: 1})", "", nil)
	_ = fixtureAdmission1148Context
	resolved, err = exec.resolveRowDynamicTokens(fixtureAdmission1148, fixtureAdmission1148Context, "(n {k: 1})", nil, nil)
	require.NoError(t, err)
	require.Equal(t, "(n {k: 1})", resolved)

	// A label item on a relationship, where its kind isn't known statically.
	fixtureAdmission1585Context, fixtureAdmission1585 := statementTestContext(ctx, "r:$('X')")
	_ = fixtureAdmission1585Context
	_, err = exec.applySetToRelationshipWithContext(fixtureAdmission1585, fixtureAdmission1585Context, edge, "r", "r:$('X')", nil, nil)
	requireCode(t, err, "Neo.ClientError.Statement.TypeError")
	fixtureAdmission1586Context, fixtureAdmission1586 := statementTestContext(ctx, "r[$missing] = 1")
	_ = fixtureAdmission1586Context
	_, err = exec.applySetToRelationshipWithContext(fixtureAdmission1586, fixtureAdmission1586Context, edge, "r", "r[$missing] = 1", nil, nil)
	require.Error(t, err)
	fixtureAdmission1587Context, fixtureAdmission1587 := statementTestContext(ctx, "r['k'] = {a: 1}")
	_ = fixtureAdmission1587Context
	_, err = exec.applySetToRelationshipWithContext(fixtureAdmission1587, fixtureAdmission1587Context, edge, "r", "r['k'] = {a: 1}", nil, nil)
	require.Error(t, err)
	fixtureAdmission1588Context, fixtureAdmission1588 := statementTestContext(ctx, "n:$(1)")
	_ = fixtureAdmission1588Context
	_, err = exec.applySetToNodeWithContext(fixtureAdmission1588, fixtureAdmission1588Context, node, "n", "n:$(1)", nil, nil)
	requireCode(t, err, "Neo.ClientError.Statement.TypeError")
	fixtureAdmission1589Context, fixtureAdmission1589 := statementTestContext(ctx, "n:Bad Label")
	_ = fixtureAdmission1589Context
	_, err = exec.applySetToNodeWithContext(fixtureAdmission1589, fixtureAdmission1589Context, node, "n", "n:Bad Label", nil, nil)
	require.Error(t, err)
	fixtureAdmission1590Context, fixtureAdmission1590 := statementTestContext(ctx, "n['k'] = {a: 1}")
	_ = fixtureAdmission1590Context
	_, err = exec.applySetToNodeWithContext(fixtureAdmission1590, fixtureAdmission1590Context, node, "n", "n['k'] = {a: 1}", nil, nil)
	require.Error(t, err)
	result := &ExecuteResult{Stats: &QueryStats{}}
	fixtureAdmission1149Context, fixtureAdmission1149 := statementTestContext(ctx, "REMOVE r:$('X')")
	_ = fixtureAdmission1149Context
	err = exec.pipelineApplyRemove(fixtureAdmission1149, fixtureAdmission1149Context, []pipelineRow{{"r": edge}}, "REMOVE r:$('X')", result)
	requireCode(t, err, "Neo.ClientError.Statement.TypeError")
	fixtureAdmission1150Context, fixtureAdmission1150 := statementTestContext(ctx, "REMOVE r[null]")
	_ = fixtureAdmission1150Context
	err = exec.pipelineApplyRemove(fixtureAdmission1150, fixtureAdmission1150Context, []pipelineRow{{"r": edge}}, "REMOVE r[null]", result)
	requireCode(t, err, "Neo.ClientError.Statement.TypeError")
	fixtureAdmission1151Context, fixtureAdmission1151 := statementTestContext(ctx, "REMOVE n:$(null)")
	_ = fixtureAdmission1151Context
	err = exec.pipelineApplyRemove(fixtureAdmission1151, fixtureAdmission1151Context, []pipelineRow{{"n": node}}, "REMOVE n:$(null)", result)
	requireCode(t, err, "Neo.ClientError.Statement.TypeError")
	fixtureAdmission1152Context, fixtureAdmission1152 := statementTestContext(ctx, "REMOVE n[null]")
	_ = fixtureAdmission1152Context
	err = exec.pipelineApplyRemove(fixtureAdmission1152, fixtureAdmission1152Context, []pipelineRow{{"n": node}}, "REMOVE n[null]", result)
	requireCode(t, err, "Neo.ClientError.Statement.TypeError")
	fixtureAdmission1153Context, fixtureAdmission1153 := statementTestContext(ctx, "REMOVE n.k, r.k")
	_ = fixtureAdmission1153Context
	err = exec.pipelineApplyRemove(fixtureAdmission1153, fixtureAdmission1153Context, []pipelineRow{{"n": (*storage.Node)(nil), "r": (*storage.Edge)(nil)}}, "REMOVE n.k, r.k", result)
	require.NoError(t, err)
	fixtureAdmission1154Context, fixtureAdmission1154 := statementTestContext(ctx, "REMOVE n:")
	_ = fixtureAdmission1154Context
	err = exec.pipelineApplyRemove(fixtureAdmission1154, fixtureAdmission1154Context, []pipelineRow{{"n": node}}, "REMOVE n:", result)
	require.Error(t, err)
	fixtureAdmission1155Context, fixtureAdmission1155 := statementTestContext(ctx, "REMOVE n")
	_ = fixtureAdmission1155Context
	err = exec.pipelineApplyRemove(fixtureAdmission1155, fixtureAdmission1155Context, []pipelineRow{{"n": node}}, "REMOVE n", result)
	require.Error(t, err)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	fixtureAdmission1156Context, fixtureAdmission1156 := statementTestContext(cancelled, "REMOVE n.k")
	_ = fixtureAdmission1156Context
	err = exec.pipelineApplyRemove(fixtureAdmission1156, fixtureAdmission1156Context, []pipelineRow{{"n": node}}, "REMOVE n.k", result)
	require.ErrorIs(t, err, context.Canceled)
	items, err := parseRemoveItems("n.a, , n:A")
	require.NoError(t, err)
	require.Len(t, items, 2)
	_, err = parseRemoveItems("n[ ]")
	require.Error(t, err)

	// The internal label test: its arguments, an evaluation error, a value
	// that isn't an entity.
	fnContext := func(values map[string]interface{}, fail error) cypherfn.Context {
		return cypherfn.Context{Eval: func(expr string) (interface{}, error) {
			if fail != nil {
				return nil, fail
			}
			return values[expr], nil
		}}
	}
	_, err = fnDynamicLabelTest(fnContext(nil, nil), []string{"n"})
	require.Error(t, err)
	_, err = fnDynamicLabelTest(fnContext(nil, context.Canceled), []string{"n", "l", "a"})
	require.ErrorIs(t, err, context.Canceled)
	value, err := fnDynamicLabelTest(fnContext(map[string]interface{}{"l": "A", "a": false}, nil), []string{"n", "l", "a"})
	require.NoError(t, err)
	require.Nil(t, value)
	value, err = fnDynamicLabelTest(fnContext(map[string]interface{}{"n": edge, "l": "DBR", "a": false}, nil), []string{"n", "l", "a"})
	require.NoError(t, err)
	require.Equal(t, true, value)
	_, err = fnDynamicLabelTest(fnContext(map[string]interface{}{"n": node, "l": int64(1), "a": false}, nil), []string{"n", "l", "a"})
	requireCode(t, err, "Neo.ClientError.Statement.TypeError")

	// Static checks: MERGE actions, REMOVE label items on a relationship, a
	// write pattern's row-dependent term, an unbalanced predicate call.
	scope := staticTypeScope{kinds: matchSemanticScope{"n": matchBindingNode, "r": matchBindingRelationship}, values: map[string]string{"i": "Integer"}}
	requireCode(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseMerge, text: "MERGE (n:A) ON CREATE SET n:$(1)"}, scope), "Neo.ClientError.Statement.SyntaxError")
	requireCode(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseMerge, text: "MERGE (n:A) ON MATCH SET n[1] = 1"}, scope), "Neo.ClientError.Statement.SyntaxError")
	requireCode(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseRemove, text: "REMOVE r:$('X')"}, scope), "Neo.ClientError.Statement.SyntaxError")
	requireCode(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseRemove, text: "REMOVE n:$(i)"}, scope), "Neo.ClientError.Statement.SyntaxError")
	// No label item, key or non-entity target: nothing to check here (the
	// REMOVE scope check reports an item of no form).
	require.NoError(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseRemove, text: "REMOVE n"}, scope))
	require.Error(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseRemove, text: "REMOVE i"}, scope))
	require.NoError(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseSet, text: "SET n.p = 1"}, staticTypeScope{}))
	require.NoError(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseSet, text: "SET n.p = i, n.q = i.x"}, scope))
	require.NoError(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseMerge, text: "MERGE (n:A {k: 1}) ON CREATE SET n.p = i"}, scope))
	require.Error(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseMerge, text: "MERGE (n:A) ON CREATE SET n.p = 1, i.q = 2"}, scope))
	require.Error(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseSet, text: "SET p.k = 1"}, staticTypeScope{kinds: matchSemanticScope{"p": matchBindingPath}}))
	requireCode(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseSet, text: "SET n:Bad Label"}, scope), "Neo.ClientError.Statement.SyntaxError")
	requireCode(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseCreate, text: "CREATE (m:$(i))"}, scope), "Neo.ClientError.Statement.SyntaxError")
	require.NoError(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseCreate, text: "CREATE (m:$(i"}, scope))
	require.NoError(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseCreate, text: "CREATE (m {k: '$(1)', v: $p})"}, scope))
	require.NoError(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseMatch, text: "MATCH (m) WHERE " + dynamicLabelTestFunction + "(m, i"}, scope))
	requireCode(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseMatch, text: "MATCH (m) WHERE " + dynamicLabelTestFunction + "(m, i, false)"}, scope), "Neo.ClientError.Statement.SyntaxError")
	require.NoError(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseMatch, text: "MATCH (m) WHERE m.__x = 1"}, scope))

	// The label-chain reader: malformed dynamic items.
	for _, chain := range []string{"$(x", "$(x) y", "`A` y", "A:$()"} {
		_, err := setLabelChainItems(chain)
		require.Error(t, err, chain)
	}
	items2, err := setLabelChainItems("A: $(x) :B")
	require.NoError(t, err)
	require.Equal(t, []labelChainItem{{name: "A"}, {expression: "x"}, {name: "B"}}, items2)

	// Readers of the new item forms.
	readText := deletedEntityReadText(pipelineClause{kind: pipelineClauseSet, text: "SET n[m.k] = m.v, n:$(m.l), n:Fixed"})
	require.Contains(t, readText, "m.k")
	require.Contains(t, readText, "m.v")
	require.Contains(t, readText, "$(m.l)")
	require.NotContains(t, readText, "Fixed")
	var reads, writes stretchTokens
	analyzeStretchClause(context.Background(), pipelineClause{kind: pipelineClauseRemove, text: "REMOVE n:$(m.l):A, n[m.k], n.p"}, &reads, &writes)
	require.True(t, writes.anyNode)
	require.True(t, writes.anyKey)
	require.Contains(t, writes.labels, "A")
	require.Contains(t, reads.keys, "l")
	require.Contains(t, reads.keys, "k")
	reads, writes = stretchTokens{}, stretchTokens{}
	analyzeStretchClause(context.Background(), pipelineClause{kind: pipelineClauseRemove, text: "REMOVE n"}, &reads, &writes)
	require.True(t, writes.everything)

	// Label expressions: the predicate of %, a nested term's error.
	require.Equal(t, "n:%", (&labelExpression{kind: labelExpressionAny}).predicate("n"))
	negated := &labelExpression{kind: labelExpressionNot, operands: []*labelExpression{{kind: labelExpressionDynamic, expression: "x"}}}
	_, _, err = negated.resolveDynamic(func(string) (interface{}, bool, error) { return nil, true, nil })
	requireCode(t, err, "Neo.ClientError.Statement.TypeError")
	r := &labelExpressionRewriter{query: "n:$(x)", writeItems: true}
	value2, constant, err := r.resolveConstant("$1")
	require.NoError(t, err)
	require.False(t, constant)
	require.Nil(t, value2)
	require.False(t, r.writeItemHead(0))
	got, _, err := desugarLabelExpressions("MATCH (n) SET n IS $(x RETURN n", nil, false)
	require.NoError(t, err)
	require.Equal(t, "MATCH (n) SET n IS $(x RETURN n", got)
	var references []string
	scanParameterReferences("RETURN $all(x), $p", func(dollar, start, end int) {
		references = append(references, "RETURN $all(x), $p"[start:end])
	})
	require.Equal(t, []string{"p"}, references)

	// Statements: a relationship's dynamic type, a MERGE's row value.
	for query, code := range map[string]string{
		"MATCH ()-[r:$(null)]->() RETURN r":                  "Neo.ClientError.Statement.SyntaxError",
		"WITH null AS l MERGE (n:$(l) {k: 1}) RETURN n":      "Neo.ClientError.Statement.TypeError",
		"WITH 'A' AS l MATCH (n:DB) SET n[1 +] = 1 RETURN n": "Neo.ClientError.Statement.SyntaxError",
	} {
		_, err := exec.Execute(ctx, query, nil)
		requireCode(t, err, code)
	}

	// The scope checks, called directly as the MERGE validator calls them.
	scope2 := newSemanticBindingScope()
	scope2.bind("n")
	require.Error(t, exec.validateSetClauseScope(scope2, "SET n[1 +] = 1"))
	require.Error(t, exec.validateSetClauseScope(scope2, "SET n:$(missing)"))
	require.Error(t, exec.validateSetClauseScope(scope2, "SET n:$()"))
	require.Error(t, validateRemoveClauseScope(scope2, "REMOVE n"))
	require.Error(t, validateRemoveClauseScope(scope2, "REMOVE n[missing]"))
	require.Error(t, validateRemoveClauseScope(scope2, "REMOVE n:$(missing)"))
	require.Error(t, validateRemoveClauseScope(scope2, "REMOVE m.p"))
	require.NoError(t, validateRemoveClauseScope(scope2, "REMOVE n:$('A'), n['k'], n.p"))

	requireCode(t, staticWriteTokenError(pipelineClause{kind: pipelineClauseMerge, text: "MERGE (n:A) ON CREATE SET n[1] = 1"}, scope), "Neo.ClientError.Statement.SyntaxError")
	fixtureAdmission1157Context, fixtureAdmission1157 := traversalTestContext(cancelled, "(n:$(l) {k: 1})", "", nil)
	_ = fixtureAdmission1157Context
	_, _, err = exec.pipelineApplyRowDynamicMerge(fixtureAdmission1157, fixtureAdmission1157Context, []pipelineRow{{"l": "L"}}, "MERGE (n:$(l) {k: 1})", "(n:$(l) {k: 1})")
	require.ErrorIs(t, err, context.Canceled)
	fixtureAdmission1158Context, fixtureAdmission1158 := traversalTestContext(ctx, "(n:$(l) {k: 1})", "", nil)
	_ = fixtureAdmission1158Context
	_, _, err = exec.pipelineApplyRowDynamicMerge(fixtureAdmission1158, fixtureAdmission1158Context, []pipelineRow{{"l": int64(1)}}, "MERGE (n:$(l) {k: 1})", "(n:$(l) {k: 1})")
	requireCode(t, err, "Neo.ClientError.Statement.TypeError")

	// Removing an absent property is a no-op even when the store rejects writes.
	failing := NewStorageExecutor(&updateErrorEngine{Engine: store, nodeErr: context.DeadlineExceeded, edgeErr: context.DeadlineExceeded})
	fixtureAdmission1159Context, fixtureAdmission1159 := statementTestContext(ctx, "REMOVE n.k")
	_ = fixtureAdmission1159Context
	err = failing.pipelineApplyRemove(fixtureAdmission1159, fixtureAdmission1159Context, []pipelineRow{{"n": node}}, "REMOVE n.k", result)
	require.NoError(t, err)
	fixtureAdmission1160Context, fixtureAdmission1160 := statementTestContext(ctx, "REMOVE r.k")
	_ = fixtureAdmission1160Context
	err = failing.pipelineApplyRemove(fixtureAdmission1160, fixtureAdmission1160Context, []pipelineRow{{"r": edge}}, "REMOVE r.k", result)
	require.NoError(t, err)

	// Seed real persisted properties so REMOVE must write and report rejection.
	node, err = store.GetNode(node.ID)
	require.NoError(t, err)
	node.Properties["k"] = int64(1)
	require.NoError(t, store.UpdateNode(node))
	edge, err = store.GetEdge(edge.ID)
	require.NoError(t, err)
	if edge.Properties == nil {
		edge.Properties = make(map[string]interface{})
	}
	edge.Properties["k"] = int64(1)
	require.NoError(t, store.UpdateEdge(edge))
	fixtureAdmission1161Context, fixtureAdmission1161 := statementTestContext(ctx, "REMOVE n.k")
	_ = fixtureAdmission1161Context
	err = failing.pipelineApplyRemove(fixtureAdmission1161, fixtureAdmission1161Context, []pipelineRow{{"n": node}}, "REMOVE n.k", result)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	fixtureAdmission1162Context, fixtureAdmission1162 := statementTestContext(ctx, "REMOVE r.k")
	_ = fixtureAdmission1162Context
	err = failing.pipelineApplyRemove(fixtureAdmission1162, fixtureAdmission1162Context, []pipelineRow{{"r": edge}}, "REMOVE r.k", result)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

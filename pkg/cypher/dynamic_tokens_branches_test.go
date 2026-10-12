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
	_, err = exec.dynamicTokenValue(withParams, "$missing", nil, nil)
	require.Error(t, err)
	_, err = exec.chainLabelNames(withParams, []labelChainItem{{expression: "$missing"}}, nil, nil)
	require.Error(t, err)
	_, err = exec.dynamicPropertyKeyOf(withParams, "$missing", nil, nil, false)
	require.Error(t, err)
	_, err = exec.resolveRowDynamicTokens(withParams, "(n:$($missing))", nil, nil)
	require.Error(t, err)
	_, err = exec.resolveRowDynamicTokens(ctx, "(n:$(null))", nil, nil)
	requireCode(t, err, "Neo.ClientError.Statement.TypeError")

	// The pattern resolver leaves quoted text, parameters and unbalanced text.
	resolved, err := exec.resolveRowDynamicTokens(ctx, "(`n`:$('A') $props)-[:$('R')]->(m:X)", nil, nil)
	require.NoError(t, err)
	require.Equal(t, "(`n`:A $props)-[:R]->(m:X)", resolved)
	resolved, err = exec.resolveRowDynamicTokens(ctx, "(n:$('A')", nil, nil)
	require.NoError(t, err)
	require.Equal(t, "(n:A", resolved)
	resolved, err = exec.resolveRowDynamicTokens(ctx, "(n:$(", nil, nil)
	require.NoError(t, err)
	require.Equal(t, "(n:$(", resolved)
	resolved, err = exec.resolveRowDynamicTokens(ctx, "(n:Fixed: $([]))", nil, nil)
	require.NoError(t, err)
	require.Equal(t, "(n:Fixed)", resolved)
	resolved, err = exec.resolveRowDynamicTokens(ctx, "(n {k: 1})", nil, nil)
	require.NoError(t, err)
	require.Equal(t, "(n {k: 1})", resolved)

	// A label item on a relationship, where its kind isn't known statically.
	_, err = exec.applySetToRelationshipWithContext(ctx, edge, "r", "r:$('X')", nil, nil)
	requireCode(t, err, "Neo.ClientError.Statement.TypeError")
	_, err = exec.applySetToRelationshipWithContext(ctx, edge, "r", "r[$missing] = 1", nil, nil)
	require.Error(t, err)
	_, err = exec.applySetToRelationshipWithContext(ctx, edge, "r", "r['k'] = {a: 1}", nil, nil)
	require.Error(t, err)
	_, err = exec.applySetToNodeWithContext(ctx, node, "n", "n:$(1)", nil, nil)
	requireCode(t, err, "Neo.ClientError.Statement.TypeError")
	_, err = exec.applySetToNodeWithContext(ctx, node, "n", "n:Bad Label", nil, nil)
	require.Error(t, err)
	_, err = exec.applySetToNodeWithContext(ctx, node, "n", "n['k'] = {a: 1}", nil, nil)
	require.Error(t, err)
	result := &ExecuteResult{Stats: &QueryStats{}}
	err = exec.pipelineApplyRemove(ctx, []pipelineRow{{"r": edge}}, "REMOVE r:$('X')", result)
	requireCode(t, err, "Neo.ClientError.Statement.TypeError")
	err = exec.pipelineApplyRemove(ctx, []pipelineRow{{"r": edge}}, "REMOVE r[null]", result)
	requireCode(t, err, "Neo.ClientError.Statement.TypeError")
	err = exec.pipelineApplyRemove(ctx, []pipelineRow{{"n": node}}, "REMOVE n:$(null)", result)
	requireCode(t, err, "Neo.ClientError.Statement.TypeError")
	err = exec.pipelineApplyRemove(ctx, []pipelineRow{{"n": node}}, "REMOVE n[null]", result)
	requireCode(t, err, "Neo.ClientError.Statement.TypeError")
	err = exec.pipelineApplyRemove(ctx, []pipelineRow{{"n": (*storage.Node)(nil), "r": (*storage.Edge)(nil)}}, "REMOVE n.k, r.k", result)
	require.NoError(t, err)
	err = exec.pipelineApplyRemove(ctx, []pipelineRow{{"n": node}}, "REMOVE n:", result)
	require.Error(t, err)
	err = exec.pipelineApplyRemove(ctx, []pipelineRow{{"n": node}}, "REMOVE n", result)
	require.Error(t, err)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	err = exec.pipelineApplyRemove(cancelled, []pipelineRow{{"n": node}}, "REMOVE n.k", result)
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
	analyzeStretchClause(pipelineClause{kind: pipelineClauseRemove, text: "REMOVE n:$(m.l):A, n[m.k], n.p"}, &reads, &writes)
	require.True(t, writes.anyNode)
	require.True(t, writes.anyKey)
	require.Contains(t, writes.labels, "A")
	require.Contains(t, reads.keys, "l")
	require.Contains(t, reads.keys, "k")
	reads, writes = stretchTokens{}, stretchTokens{}
	analyzeStretchClause(pipelineClause{kind: pipelineClauseRemove, text: "REMOVE n"}, &reads, &writes)
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
		"MATCH ()-[r:$(null)]->() RETURN r":                 "Neo.ClientError.Statement.SyntaxError",
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
	_, _, err = exec.pipelineApplyRowDynamicMerge(cancelled, []pipelineRow{{"l": "L"}}, "MERGE (n:$(l) {k: 1})", "(n:$(l) {k: 1})")
	require.ErrorIs(t, err, context.Canceled)
	_, _, err = exec.pipelineApplyRowDynamicMerge(ctx, []pipelineRow{{"l": int64(1)}}, "MERGE (n:$(l) {k: 1})", "(n:$(l) {k: 1})")
	requireCode(t, err, "Neo.ClientError.Statement.TypeError")

	// A store that can't write: REMOVE reports it.
	failing := NewStorageExecutor(&updateErrorEngine{Engine: store, nodeErr: context.DeadlineExceeded, edgeErr: context.DeadlineExceeded})
	err = failing.pipelineApplyRemove(ctx, []pipelineRow{{"n": node}}, "REMOVE n.k", result)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	err = failing.pipelineApplyRemove(ctx, []pipelineRow{{"r": edge}}, "REMOVE r.k", result)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

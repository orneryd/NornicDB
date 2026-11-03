package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A label test on any expression, a call or a parenthesized expression,
// written expr:Labels or expr IS Labels, is the same test in every clause:
// RETURN, WHERE, a pattern element's own WHERE and a quantified path
// pattern's relationship predicate (#1181). Expected values are Neo4j
// 5.26.30's.
func TestLabelTestOnAnExpression(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "expression_label_test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:Doc {id: 'a'})-[:R {id: 'r1'}]->(:Def {id: 'd'})", nil)
	require.NoError(t, err)
	for query, want := range map[string][][]interface{}{
		"MATCH ()-[r:R]->() RETURN endNode(r):Def AS v":                                                                                     {{true}},
		"MATCH ()-[r:R]->() RETURN endNode(r) IS Def AS v":                                                                                  {{true}},
		"MATCH ()-[r:R]->() RETURN endNode(r):Def|Doc AS v, (endNode(r)):Def AS w":                                                          {{true, true}},
		"MATCH ()-[r:R]->() RETURN endNode(r):!Doc AS v, startNode(r):Doc&Def AS w":                                                         {{true, false}},
		"MATCH ()-[r:R]->() RETURN endNode(r):% AS v":                                                                                       {{true}},
		"MATCH p = (:Doc)-[r:R]->() RETURN head(relationships(p)):% AS v, head([]):% AS w":                                                  {{true, nil}},
		"MATCH ()-[r:R]->() RETURN endNode(r) : Def AS v, endNode(r)  :Def|Doc AS w":                                                        {{true, true}},
		"MATCH ()-[r:R]->() RETURN endNode(r):`%` AS v, endNode(r):% AS w":                                                                  {{false, true}},
		"RETURN head([]):Def AS v":                                                                                                          {{nil}},
		"MATCH p = (:Doc)-[r:R]->() RETURN head(relationships(p)):R AS v, last(nodes(p)):Def|Doc AS w":                                      {{true, true}},
		"MATCH (a:Doc {id: 'a'})-[r:R]-(t) WHERE endNode(r):Def RETURN t.id AS v":                                                           {{"d"}},
		"MATCH (a:Doc {id: 'a'})-[r:R]-(t) WHERE NOT (startNode(r):Def OR endNode(r):Def) RETURN t.id AS v":                                 {},
		"MATCH (a:Doc {id: 'a'})-[r:R WHERE endNode(r):Def]->(t) RETURN t.id AS v":                                                          {{"d"}},
		"MATCH (a:Doc {id: 'a'})-[r:R WHERE NOT endNode(r):Def]->(t) RETURN t.id AS v":                                                      {},
		"MATCH (a:Doc {id: 'a'}) MATCH p = SHORTEST 1 (a)(()-[r:R WHERE NOT (startNode(r):Def OR endNode(r):Def)]-())+(t) RETURN t.id AS v": {},
		// A label test on a variable, and a dynamic label, are unchanged.
		"MATCH ()-[r:R]->() WITH endNode(r) AS e RETURN e:Def AS v":        {{true}},
		"MATCH (n:Doc) SET n:$('Dyn'):Other RETURN n:Dyn AND n:Other AS v": {{true}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		if len(want) == 0 {
			require.Empty(t, result.Rows, query)
			continue
		}
		require.Equal(t, want, result.Rows, query)
	}
}

// The rewriter's edges for a label test on an expression: the pre-check
// past a type cast, a dynamic term and an empty conjunction in the
// predicate, forms left alone, and errors inside the expression or a moved
// element predicate (#1181).
func TestExpressionLabelTestRewriterEdges(t *testing.T) {
	require.True(t, mayTestExpressionLabels("RETURN f(x)::INTEGER, g(y):A"))
	require.True(t, mayTestExpressionLabels("RETURN f(x) : A"))
	require.False(t, mayTestExpressionLabels("RETURN f(x)::INTEGER"))
	require.False(t, mayTestExpressionLabels("RETURN f(x) AS v"))

	dynamic := &labelExpression{kind: labelExpressionDynamic, expression: "$names"}
	require.Equal(t, dynamicLabelTestFunction+"((f(x)), ($names), false)", dynamic.entityPredicate("f(x)"))
	require.Equal(t, "true", (&labelExpression{kind: labelExpressionAnd}).entityPredicate("f(x)"))
	require.Equal(t, "false", (&labelExpression{kind: labelExpressionOr}).entityPredicate("f(x)"))

	for _, query := range []string{
		"RETURN endNode(r) IS NOT Def AS v",    // IS NOT takes no label: left for its own error
		"RETURN f(x): 1 AS v",                  // no label chain after the colon
		"RETURN (exists((a)-->(:A|B))):C AS v", // the expression holds a rewrite of its own
	} {
		out, _, err := desugarLabelExpressions(t.Context(), query, nil, false)
		require.NoError(t, err, query)
		require.NotContains(t, out, dynamicLabelTestFunction+"((", query)
	}
	_, _, err := desugarLabelExpressions(t.Context(), "RETURN size((a)-[:R]->+(b)):A AS v", nil, false)
	require.Error(t, err)
	_, _, err = desugarLabelExpressions(t.Context(), "MATCH (a)-[r WHERE size((a)-[:R]->+(b)) > 0]->(b) RETURN a", nil, false)
	require.Error(t, err)
}

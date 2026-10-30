package cypher

import (
	"context"
	"sort"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// MATCH, CREATE and MERGE take dynamic labels and types ($(e), $all(e),
// $any(e)) as Neo4j 5.26.30 does: a literal or parameter value is resolved
// before the statement runs, a value that depends on the row per row (#907).
// Results are Neo4j's on the same graph.
func TestDynamicLabelsAndTypesInPatterns(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dynamic_patterns"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:PA:PB {id: 1})-[:PR {w: 1}]->(b:PA {id: 2})-[:PS]->(c:PC {id: 3}), (a)-[:PR]->(c), (:Bare {id: 4})", nil)
	require.NoError(t, err)
	inTransaction := func(query string, params map[string]interface{}) (*ExecuteResult, error) {
		_, err := exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		defer func() {
			_, err := exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}()
		return exec.Execute(ctx, query, params)
	}
	params := map[string]interface{}{"label": "PA", "labels": []interface{}{"PA", "PB"}, "type": "PR"}

	for query, want := range map[string]int64{
		// Constant values.
		"MATCH (n:$('PA')) RETURN count(n) AS c":                                         2,
		"MATCH (n:$(['PA', 'PB'])) RETURN count(n) AS c":                                 1,
		"MATCH (n:$all(['PA', 'PB'])) RETURN count(n) AS c":                              1,
		"MATCH (n:$any(['PA', 'PC'])) RETURN count(n) AS c":                              3,
		"MATCH (n:$all([])) RETURN count(n) AS c":                                        4,
		"MATCH (n:$([])) RETURN count(n) AS c":                                           4,
		"MATCH (n:$any([])) RETURN count(n) AS c":                                        0,
		"MATCH (n:$('PA')&!$('PB')) RETURN count(n) AS c":                                1,
		"MATCH (n:!$('PA')) RETURN count(n) AS c":                                        2,
		"MATCH (n:$('PA')|$('PC')) RETURN count(n) AS c":                                 3,
		"MATCH (n:$($label)) RETURN count(n) AS c":                                       2,
		"MATCH (n:$($labels)) RETURN count(n) AS c":                                      1,
		"MATCH (n:$('PA') {id: 2}) RETURN count(n) AS c":                                 1,
		"MATCH ()-[r:$('PR')]->() RETURN count(r) AS c":                                  2,
		"MATCH ()-[r:$($type)]->() RETURN count(r) AS c":                                 2,
		"MATCH ()-[r:$any(['PR', 'PS'])]->() RETURN count(r) AS c":                       3,
		"MATCH ()-[r:$(['PR', 'PS'])]->() RETURN count(r) AS c":                          0,
		"MATCH ()-[r:$([])]->() RETURN count(r) AS c":                                    3,
		"MATCH ()-[r:$('PR')|PS]->() RETURN count(r) AS c":                               3,
		"MATCH (a:PA {id: 1})-[r:$('PR')*1..2]->(m) RETURN count(*) AS c":                2,
		"MATCH (a:PA {id: 1}) RETURN COUNT { (a)-->(:$any(['PA', 'PC'])) } AS c":         2,
		"MATCH (a:PA {id: 1}) WHERE EXISTS { (a)-[:$('PR')]->(:$('PA')) } RETURN 1 AS c": 1,
		"MATCH (a:PA {id: 1}) RETURN size([(a)-[:$('PR')]->(m) | m.id]) AS c":            2,
		// Values that depend on the row.
		"WITH 'PA' AS l MATCH (n:$(l)) RETURN count(n) AS c":                          2,
		"WITH ['PA', 'PB'] AS l MATCH (n:$(l)) RETURN count(n) AS c":                  1,
		"WITH ['PA', 'PC'] AS l MATCH (n:$any(l)) RETURN count(n) AS c":               3,
		"WITH [] AS l MATCH (n:$(l)) RETURN count(n) AS c":                            4,
		"WITH 'PB' AS l MATCH (n:PA&!$(l)) RETURN count(n) AS c":                      1,
		"WITH 'P' + 'R' AS t MATCH ()-[r:$(t)]->() RETURN count(r) AS c":              2,
		"WITH 'PR' AS t MATCH (a:PA {id: 1})-[r:$(t)*1..2]->(m) RETURN count(*) AS c": 2,
		"UNWIND ['PA', 'PC'] AS l MATCH (n:$(l)) RETURN count(n) AS c":                3,
	} {
		result, err := inTransaction(query, params)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}

	sorted := func(value interface{}) []string {
		var out []string
		for _, item := range value.([]interface{}) {
			out = append(out, item.(string))
		}
		sort.Strings(out)
		return out
	}
	for query, want := range map[string][]string{
		"CREATE (n:$('DynA')) RETURN labels(n) AS l":                                 {"DynA"},
		"CREATE (n:$(['C1', 'C2']) {k: 1}) RETURN labels(n) AS l":                    {"C1", "C2"},
		"CREATE (n:$all(['C1', 'C2'])) RETURN labels(n) AS l":                        {"C1", "C2"},
		"CREATE (n:$([]) {k: 1}) RETURN labels(n) AS l":                              nil,
		"CREATE (n:Fixed:$('A B')) RETURN labels(n) AS l":                            {"A B", "Fixed"},
		"CREATE (n:$('Dyn' + 'C')) RETURN labels(n) AS l":                            {"DynC"},
		"CREATE (n:$($labels)) RETURN labels(n) AS l":                                {"PA", "PB"},
		"WITH ['W1', 'W2'] AS ls CREATE (n:$(ls)) RETURN labels(n) AS l":             {"W1", "W2"},
		"WITH [] AS ls CREATE (n:$(ls) {k: 2}) RETURN labels(n) AS l":                nil,
		"MERGE (n:$('PA') {id: 2}) RETURN labels(n) AS l":                            {"PA"},
		"MERGE (n:$('NEWL') {id: 9}) ON CREATE SET n.c = true RETURN labels(n) AS l": {"NEWL"},
		"WITH 'PA' AS l MERGE (n:$(l) {id: 2}) RETURN labels(n) AS l":                {"PA"},
	} {
		result, err := inTransaction(query, params)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1, query)
		got := sorted(result.Rows[0][0])
		require.Equal(t, want, got, query)
	}

	for query, want := range map[string]interface{}{
		"MATCH (a:PA {id: 1}), (c:PC) CREATE (a)-[r:$('NEW')]->(c) RETURN type(r) AS t":              "NEW",
		"MATCH (a:PA {id: 1}), (c:PC) CREATE (a)-[r:$(['NEW'])]->(c) RETURN type(r) AS t":            "NEW",
		"WITH 'NEWT' AS t MATCH (a:PA {id: 1}), (c:PC) CREATE (a)-[r:$(t)]->(c) RETURN type(r) AS t": "NEWT",
		"MATCH (a:PA {id: 1}), (c:PC) MERGE (a)-[r:$('PR')]->(c) RETURN type(r) AS t":                "PR",
		"WITH 'PR' AS t MATCH (a:PA {id: 1}), (c:PC) MERGE (a)-[r:$(t)]->(c) RETURN type(r) AS t":    "PR",
		"MATCH (a:PA {id: 1}) MERGE (a)-[r:$('PT')]->(b:$('PQ') {id: 5}) RETURN type(r) AS t":        "PT",
	} {
		result, err := inTransaction(query, params)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}

	// MERGE sees what the rows before it merged.
	result, err := inTransaction("UNWIND ['M1', 'M1', 'M2'] AS l MERGE (n:$(l) {k: 9}) RETURN count(DISTINCT n) AS c", params)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
	result, err = inTransaction("UNWIND ['L1', 'L2'] AS l CREATE (n:$(l)) RETURN count(*) AS c", params)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
	require.Equal(t, 2, result.Stats.LabelsAdded)

	for query, code := range map[string]string{
		"CREATE (n:$any(['X', 'Y'])) RETURN n":                                                "Neo.ClientError.Statement.SyntaxError",
		"MERGE (n:$any(['X']) {k: 1}) RETURN n":                                               "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:PA) WHERE n:$('PB') RETURN n":                                               "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:PA) RETURN n:$('PB') AS b":                                                  "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:PA) WITH n WHERE (n:$('PB')) RETURN n":                                      "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:PA) WHERE n IS $('PB') RETURN n":                                            "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:PA) SET n.p = n:$('PB') RETURN n":                                           "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:$(null)) RETURN n":                                                          "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:$('')) RETURN n":                                                            "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:$(1)) RETURN n":                                                             "Neo.ClientError.Statement.SyntaxError",
		"CREATE (n:$(['A', null])) RETURN n":                                                  "Neo.ClientError.Statement.SyntaxError",
		"MATCH (a:PA {id: 1}), (c:PC) CREATE (a)-[r:$([])]->(c) RETURN r":                     "Neo.ClientError.Statement.SyntaxError",
		"MATCH (a:PA {id: 1}), (c:PC) MERGE (a)-[r:$(['A', 'B'])]->(c) RETURN r":              "Neo.ClientError.Statement.SyntaxError",
		"WITH null AS l MATCH (n:$(l)) RETURN n":                                              "Neo.ClientError.Statement.TypeError",
		"WITH 1 AS l MATCH (n:$(l)) RETURN n":                                                 "Neo.ClientError.Statement.SyntaxError",
		"WITH ['A', 'B'] AS t MATCH (a:PA {id: 1}), (c:PC) CREATE (a)-[r:$(t)]->(c) RETURN r": "Neo.DatabaseError.Statement.ExecutionFailed",
		"WITH ['A', 'B'] AS t MATCH (a:PA {id: 1}), (c:PC) MERGE (a)-[r:$(t)]->(c) RETURN r":  "Neo.DatabaseError.Statement.ExecutionFailed",
		"WITH null AS l CREATE (n:$(l)) RETURN n":                                             "Neo.ClientError.Statement.TypeError",
		"WITH '' AS l CREATE (n:$(l)) RETURN n":                                               "Neo.ClientError.Schema.TokenNameError",
		"MATCH (n:$($missing)) RETURN n":                                                      "Neo.ClientError.Statement.ParameterMissing",
	} {
		_, err := inTransaction(query, params)
		require.Error(t, err, query)
		got, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, code, got, "%s: %v", query, err)
	}
}

// A statement whose pattern has a dynamic label invalidates cached reads of
// every label, as SET n:$(e) does.
func TestDynamicPatternLabelInvalidatesCachedReads(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dynamic_pattern_cache"))
	ctx := context.Background()
	count := func() int64 {
		result, err := exec.Execute(ctx, "MATCH (n:Made) RETURN count(n) AS c", nil)
		require.NoError(t, err)
		return result.Rows[0][0].(int64)
	}
	require.Equal(t, int64(0), count())
	_, err := exec.Execute(ctx, "WITH 'Made' AS l CREATE (:$(l))", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), count())
}

func TestLabelExpressionDynamicTerms(t *testing.T) {
	expr, ok := parseLabelExpression("A&$any(x)|!$(y)")
	require.True(t, ok)
	require.Equal(t, "(A&$any(x))|!$(y)", expr.String())
	require.Nil(t, expr.requiredLabels())
	require.False(t, (&labelExpression{kind: labelExpressionDynamic}).matches([]string{"A"}))
	require.Equal(t, "((n:A AND __nornic_haslabels(n, (x), true)) OR NOT (__nornic_haslabels(n, (y), false)))", expr.predicate("n"))
	empty := &labelExpression{kind: labelExpressionAnd}
	none := &labelExpression{kind: labelExpressionOr}
	require.Equal(t, "(%|!%)", empty.String())
	require.Equal(t, "(%&!%)", none.String())
	require.Equal(t, "true", empty.predicate("n"))
	require.Equal(t, "false", none.predicate("n"))
	require.True(t, empty.matches(nil))
	require.False(t, none.matches([]string{"A"}))
	_, ok = none.alternatives()
	require.False(t, ok)
	require.Nil(t, none.requiredLabels())
	_, ok = parseLabelExpression("$(x")
	require.False(t, ok)
	_, ok = parseLabelExpression("$x")
	require.False(t, ok)

	require.True(t, labelsSatisfyNames([]string{"A", "B"}, []string{"A", "B"}, false))
	require.False(t, labelsSatisfyNames([]string{"A"}, []string{"A", "B"}, false))
	require.True(t, labelsSatisfyNames([]string{"A"}, []string{"B", "A"}, true))
	require.False(t, labelsSatisfyNames([]string{"A"}, nil, true))
	require.True(t, labelsSatisfyNames(nil, nil, false))
	require.True(t, hasDynamicToken("(n:$(x))"))
	require.False(t, hasDynamicToken("(n {k: '$(x)'})"))
	require.False(t, hasDynamicToken("(n {k: $v})"))
}

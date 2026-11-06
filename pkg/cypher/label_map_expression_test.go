package cypher

// NOR27: a map literal's key colon is not a label test's colon, and the
// value beside it is an expression like any other: {valid: (null):A} is the
// map whose value is the label test (null):A (#1181), not a label chain
// (n:(null):A) and not a free variable. The rewrite's expression scanner
// reads map literals entry by entry for it (#860's label expressions).
//
// The statements were checked on neo4j:5.26.30-community's answers where the
// answer is a value: {valid: (null):A} is {valid: null}, head([null]):A is
// null, and a label test on a number, list or null is null.

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestLabelExpressionRewriterReadsMapEntries pins the lexical classification
// itself: the rewrite's text for a map entry's value, in both Cypher
// normalizer settings, at a key followed directly by its colon, with a
// backtick-quoted key, and across block and line comments at the key, the
// colon and the call-to-colon gap.
func TestLabelExpressionRewriterReadsMapEntries(t *testing.T) {
	hasLabels := func(subject string) string {
		return "(" + dynamicLabelTestFunction + "((" + subject + "), 'A', false))"
	}
	for _, tc := range []struct{ in, want string }{
		// The NOR27 fixture: an aggregate map entry, then a label test on
		// null written without a space after its colon.
		{
			"UNWIND [1,2] AS x RETURN {c:count(x),valid:(null):A} AS v",
			"UNWIND [1,2] AS x RETURN {c:count(x),valid:" + hasLabels("(null)") + "} AS v",
		},
		// A call's result, in a map entry and at a projection's start.
		{"RETURN {valid:head([null]):A} AS v", "RETURN {valid:" + hasLabels("head([null])") + "} AS v"},
		{"RETURN head([null]):A AS v", "RETURN " + hasLabels("head([null])") + " AS v"},
		{"RETURN head /* gap */ ([null]):A AS v", "RETURN " + hasLabels("head /* gap */ ([null])") + " AS v"},
		{"RETURN `head`([null]):A AS v", "RETURN " + hasLabels("`head`([null])") + " AS v"},
		{"RETURN duration /* ns */ . `between`(date('2020-01-01'),date('2020-01-02')):A AS v", "RETURN " + hasLabels("duration /* ns */ . `between`(date('2020-01-01'),date('2020-01-02'))") + " AS v"},
		// A backtick-quoted key.
		{"RETURN {`odd key`:head([null]):A} AS v", "RETURN {`odd key`:" + hasLabels("head([null])") + "} AS v"},
		// Block comments around the key, its colon and the call-to-colon gap.
		{
			"RETURN {`odd key` /*c*/ : /*d*/ head([null]) /*e*/ :A} AS v",
			"RETURN {`odd key` /*c*/ : /*d*/ " + hasLabels("head([null])") + "} AS v",
		},
		{"RETURN {k /*c*/ : /*d*/ (null):A} AS v", "RETURN {k /*c*/ : /*d*/ " + hasLabels("(null)") + "} AS v"},
		// A line comment ends at its line break; the colon test follows it.
		{"RETURN {k: head([null]) //c\n :A} AS v", "RETURN {k: " + hasLabels("head([null])") + "} AS v"},
		// An aggregate family's argument list keeps its comma inside the call.
		{"RETURN {k: percentileCont(x, 0.5):A} AS v", "RETURN {k: " + hasLabels("percentileCont(x, 0.5)") + "} AS v"},
		// A map entry's value is an expression like any other: its pattern
		// comprehension is still rewritten.
		{
			"RETURN {a: 1, b: [(1)-->(m:!A) | m]} AS v",
			"RETURN {a: 1, b: [(1)-->(m) WHERE m:!A | m]} AS v",
		},
	} {
		for _, cypher25 := range []bool{false, true} {
			got, rewrite, err := desugarLabelExpressions(t.Context(), tc.in, nil, cypher25)
			require.NoError(t, err, "cypher25=%v %s", cypher25, tc.in)
			require.Equal(t, tc.want, got, "cypher25=%v %s", cypher25, tc.in)
			require.NotNil(t, rewrite, "cypher25=%v %s", cypher25, tc.in)
			require.Equal(t, tc.want, rewrite.Text, "cypher25=%v %s", cypher25, tc.in)
		}
	}
}

// TestLabelExpressionRewriterLeavesMapKeysAlone pins the statements whose text
// stays as written: a map entry that holds no label test, a map key that names
// a label, and a colon test on a variable that follows a comma in a
// projection, which is no map key.
func TestLabelExpressionRewriterLeavesMapKeysAlone(t *testing.T) {
	for _, query := range []string{
		"RETURN {a: 1} AS m",
		"RETURN {a: 1, b: 2} AS m",
		"RETURN {`a`: 1, /*c*/ b /*d*/ : 2} AS m",
		"MATCH (n:A) RETURN {l:n:A} AS v",
		"MATCH (n:A) RETURN {A: 1, end: 2, count: 3} AS v",
		"MATCH (n:A) RETURN n {.id, extra: 1} AS m",
		// A label test after a comma is still a label test, not a map value.
		"MATCH (n:A) RETURN 1 AS x, n:A AS v",
	} {
		for _, cypher25 := range []bool{false, true} {
			got, rewrite, err := desugarLabelExpressions(t.Context(), query, nil, cypher25)
			require.NoError(t, err, "cypher25=%v %s", cypher25, query)
			require.Equal(t, query, got, "cypher25=%v %s", cypher25, query)
			require.Nil(t, rewrite, "cypher25=%v %s", cypher25, query)
		}
	}
	// IS after a comma is the same label test, written as the colon test.
	got, _, err := desugarLabelExpressions(t.Context(), "MATCH (n:A) RETURN 1 AS x, n IS A AS v", nil, false)
	require.NoError(t, err)
	require.Equal(t, "MATCH (n:A) RETURN 1 AS x, n:A AS v", got)
}

// TestLabelExpressionRewriterKeepsMixingRejections pins the statements Neo4j
// rejects for mixing colon conjunctions with label expression symbols: a
// MATCH pattern, a colon test after a comma in a projection, and a map entry's
// value.
func TestLabelExpressionRewriterKeepsMixingRejections(t *testing.T) {
	for _, query := range []string{
		"MATCH (n:A|B:C) RETURN n",
		"MATCH (n:A:B|C) RETURN n",
		"RETURN 1 AS x, n:A|B:C AS v",
		"MATCH (n) RETURN {l:n:A|B:C} AS v",
		"MATCH (n) WHERE n:(A|B):C RETURN n",
	} {
		for _, cypher25 := range []bool{false, true} {
			_, _, err := desugarLabelExpressions(t.Context(), query, nil, cypher25)
			require.ErrorContains(t, err, "Mixing label expression symbols", "cypher25=%v %s", cypher25, query)
		}
	}
}

// TestLabelTestInMapEntriesPublic runs the same statements through both
// parsers' public execution path: one row per statement, with the map a label
// test's subject is read from. A label test on a number, a list or null is
// null (hasLabels' answer for a value that is no entity), so an aggregate
// entry keeps its key and gains no error.
func TestLabelTestInMapEntriesPublic(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		ctx := t.Context()
		parser := config.GetParserType()
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor27_label_map"))
		_, err := exec.Execute(ctx, "CREATE (:A {id: 'a'})", nil)
		require.NoError(t, err, parser)
		for _, tc := range []struct {
			query string
			want  [][]interface{}
		}{
			{
				"UNWIND [1,2] AS x RETURN {c:count(x),valid:(null):A} AS v",
				[][]interface{}{{map[string]interface{}{"c": int64(2), "valid": nil}}},
			},
			{"RETURN head([null]):A AS v", [][]interface{}{{nil}}},
			{"MATCH (n:A) RETURN `head`([n]):A AS v", [][]interface{}{{true}}},
			{"RETURN duration /* namespace */ . /* component */ between(date('2020-01-01'),date('2020-01-02')):A AS v", [][]interface{}{{nil}}},
			{"RETURN duration . between(date('2020-01-01'),date('2020-01-02')):A AS v", [][]interface{}{{nil}}},
			{"RETURN duration.`between`(date('2020-01-01'),date('2020-01-02')):A AS v", [][]interface{}{{nil}}},
			{"MATCH (n:A) RETURN head /* gap */ ([n]):A AS v", [][]interface{}{{true}}},
			{"RETURN {valid:head([null]):A} AS v", [][]interface{}{{map[string]interface{}{"valid": nil}}}},
			{"RETURN {`odd key`:head([null]):A} AS v", [][]interface{}{{map[string]interface{}{"odd key": nil}}}},
			{
				"RETURN {valid: /*c*/ head([null]) /*d*/ :A} AS v",
				[][]interface{}{{map[string]interface{}{"valid": nil}}},
			},
			{"RETURN {valid: head([null]) //c\n :A} AS v", [][]interface{}{{map[string]interface{}{"valid": nil}}}},
			// A label test on a bound node, in a map entry and after a comma.
			{"MATCH (n:A) RETURN {l:n:A} AS v", [][]interface{}{{map[string]interface{}{"l": true}}}},
			{"MATCH (n:A) RETURN {l:n:A|B} AS v", [][]interface{}{{map[string]interface{}{"l": true}}}},
			{"MATCH (n:A) RETURN 1 AS x, n:A AS v", [][]interface{}{{int64(1), true}}},
			{"MATCH (n:A) RETURN 1 AS x, n IS A AS v", [][]interface{}{{int64(1), true}}},
			// Keys that name a label, a keyword and an aggregate stay keys.
			{
				"MATCH (n:A) RETURN {A: 1, end: 2, count: 3} AS v",
				[][]interface{}{{map[string]interface{}{"A": int64(1), "end": int64(2), "count": int64(3)}}},
			},
			// The numeric aggregate families beside a map key's colon.
			{
				"UNWIND [1,2] AS x RETURN {c:count(x):A} AS c, {c:sum(x):A} AS s, {c:avg(x):A} AS a, " +
					"{c:collect(x):A} AS l, {c:min(x):A} AS mi, {c:max(x):A} AS ma, {c:percentileCont(x, 0.5):A} AS p",
				[][]interface{}{{
					map[string]interface{}{"c": nil}, map[string]interface{}{"c": nil}, map[string]interface{}{"c": nil},
					map[string]interface{}{"c": nil}, map[string]interface{}{"c": nil}, map[string]interface{}{"c": nil},
					map[string]interface{}{"c": nil},
				}},
			},
		} {
			result, err := exec.Execute(ctx, tc.query, nil)
			require.NoError(t, err, parser+" "+tc.query)
			require.Equal(t, tc.want, result.Rows, parser+" "+tc.query)
		}
		// The MATCH pattern's own rejection is unchanged.
		_, err = exec.Execute(ctx, "MATCH (n:A|B:C) RETURN n", nil)
		require.ErrorContains(t, err, "Mixing label expression symbols", parser)
	})
}

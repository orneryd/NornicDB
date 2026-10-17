package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A variable named end inside a CASE is a variable where an operand is
// expected, and END the keyword after a complete operand, as Neo4j 5.26.30
// reads them (#1136).
func TestCaseReadsVariableNamedEnd(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "caseend"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:N {id: 1})", nil)
	require.NoError(t, err)

	for statement, want := range map[string][][]interface{}{
		"WITH {id: 1} AS end, null AS node RETURN CASE WHEN node IS NOT NULL THEN true ELSE end.id IN [1] AND end.id > 0 END AS r": {{true}},
		"WITH {id: 1} AS end RETURN CASE WHEN end.id = 1 THEN end.id ELSE 0 END AS r":                                              {{int64(1)}},
		"WITH {id: 1} AS end RETURN CASE WHEN false THEN 0 ELSE end END.id AS r":                                                   {{int64(1)}},
		"WITH 1 AS end RETURN CASE end WHEN 1 THEN 'one' ELSE 'other' END AS r":                                                    {{"one"}},
		"WITH 2 AS end RETURN CASE WHEN true THEN 1 + end END AS r":                                                                {{int64(3)}},
		"RETURN CASE WHEN true THEN {a: 1} END.a AS r":                                                                             {{int64(1)}},
		"MATCH (end:N) WHERE CASE WHEN end.id = 1 THEN true ELSE false END RETURN end.id AS r":                                     {{int64(1)}},
		"MATCH (n:N) WITH n AS end WHERE CASE WHEN false THEN false ELSE end.id IN [1] AND end.id > 0 END RETURN end.id AS r":      {{int64(1)}},
		"MATCH (n:N) RETURN CASE WHEN n.id = 1 THEN n.end ELSE {end: 2}.end END AS r":                                              {{nil}},
		// A map value after its key's colon is an operand (#1136 follow-up).
		"WITH [1] AS l, 30 AS end RETURN l + CASE WHEN 1 < end THEN [{s: 1, end: end}] ELSE [] END AS r": {{[]interface{}{int64(1), map[string]interface{}{"s": int64(1), "end": int64(30)}}}},
		"WITH 2 AS end RETURN CASE WHEN true THEN {a: end} END.a AS r":                                   {{int64(2)}},
	} {
		result, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		require.Equal(t, want, result.Rows, statement)
	}
}

// caseKeywordAt reads END as the keyword only after a complete operand.
func TestCaseKeywordAtReadsEndAfterAnOperand(t *testing.T) {
	for text, keyword := range map[string]bool{
		"CASE WHEN x THEN 1 END":        true,
		"CASE WHEN x THEN n END":        true,
		"CASE WHEN x THEN f(n) END":     true,
		"CASE WHEN x THEN 'a' END":      true,
		"CASE WHEN x THEN n.else END":   true,
		"CASE WHEN x THEN end END":      false,
		"CASE WHEN end.id THEN 1 END":   false,
		"CASE WHEN x THEN end.id END":   false,
		"CASE WHEN x ELSE end END":      false,
		"CASE WHEN x AND end END":       false,
		"CASE WHEN x THEN [end] END":    false,
		"CASE WHEN x THEN 1 + end END":  false,
		"CASE WHEN x THEN {a: end} END": false,
	} {
		// The END under test is the first after CASE's own words.
		index := -1
		for i := len("CASE"); i < len(text); i++ {
			if matchKeywordAt(text, i, "END") {
				index = i
				break
			}
		}
		require.GreaterOrEqual(t, index, 0, text)
		require.Equal(t, keyword, caseKeywordAt(text, index, "END"), text)
	}
}

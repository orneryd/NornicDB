package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A variable named case, when, then, else or end works inside CASE, in both
// forms and every branch, in a WHERE and a comprehension, as Neo4j 5.26.30
// reads it (a CASE over a variable named case, CASE case WHEN …, reads as a
// nested CASE and is left out).
func TestCaseReadsVariablesNamedLikeItsKeywords(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "casekw"))
	ctx := context.Background()
	shapes := map[string][][]interface{}{
		"WITH {id: 1} AS V RETURN CASE WHEN V.id = 1 THEN V.id ELSE 0 END AS r":                  {{int64(1)}},
		"WITH 1 AS V RETURN CASE V WHEN 1 THEN V ELSE 0 END AS r":                                {{int64(1)}},
		"WITH 2 AS V RETURN CASE WHEN true THEN V END AS r":                                      {{int64(2)}},
		"WITH 2 AS V RETURN CASE WHEN false THEN 0 ELSE V END AS r":                              {{int64(2)}},
		"WITH 2 AS V RETURN CASE WHEN V > 1 AND V < 3 THEN V * 2 ELSE 0 - V END AS r":            {{int64(4)}},
		"UNWIND [1, 2] AS V WITH V WHERE CASE WHEN V > 1 THEN true ELSE false END RETURN V AS r": {{int64(2)}},
		"WITH 1 AS V RETURN [x IN [1, 2] | CASE WHEN x = V THEN V ELSE x END] AS r":              {{[]interface{}{int64(1), int64(2)}}},
	}
	for _, name := range []string{"case", "when", "then", "else", "end", "WHEN", "Then"} {
		for shape, want := range shapes {
			if strings.EqualFold(name, "case") && strings.Contains(shape, "CASE V WHEN") {
				continue
			}
			statement := strings.ReplaceAll(shape, "V", name)
			result, err := exec.Execute(ctx, statement, nil)
			require.NoError(t, err, statement)
			require.Equal(t, want, result.Rows, statement)
		}
	}
	// Variables named like infix and prefix keywords, where a CASE keyword
	// follows them or they open a simple CASE's test.
	for statement, want := range map[string][][]interface{}{
		"WITH 1 AS and RETURN CASE and WHEN 1 THEN and ELSE 0 END AS r":                                  {{int64(1)}},
		"WITH 1 AS where RETURN CASE where WHEN 1 THEN where ELSE 0 END AS r":                            {{int64(1)}},
		"WITH 2 AS in RETURN CASE WHEN in > 1 THEN 1 ELSE 0 END AS r":                                    {{int64(1)}},
		"WITH 1 AS distinct RETURN [x IN [1, 2] | CASE WHEN x = distinct THEN distinct ELSE x END] AS r": {{[]interface{}{int64(1), int64(2)}}},
		"WITH 1 AS not RETURN CASE WHEN 1 = not THEN not ELSE 0 END AS r":                                {{int64(1)}},
		"MATCH (n:Q) RETURN CASE WHEN n:and THEN 1 ELSE 0 END AS r":                                      {},
		"RETURN CASE WHEN NOT false THEN 1 END AS r":                                                     {{int64(1)}},
		"RETURN CASE WHEN 'ab' STARTS WITH 'a' AND 1 IN [1] THEN 1 END AS r":                             {{int64(1)}},
	} {
		result, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		if len(want) == 0 {
			require.Empty(t, result.Rows, statement)
			continue
		}
		require.Equal(t, want, result.Rows, statement)
	}
	// CASE expressions without such variables, nested ones included, read as before.
	for statement, want := range map[string][][]interface{}{
		"RETURN CASE WHEN true THEN CASE WHEN false THEN 1 ELSE 2 END END AS r":              {{int64(2)}},
		"WITH 1 AS x RETURN CASE x WHEN 1 THEN CASE x WHEN 1 THEN 'a' END ELSE 'b' END AS r": {{"a"}},
		"RETURN CASE CASE WHEN true THEN 1 END WHEN 1 THEN 'one' END AS r":                   {{"one"}},
		"RETURN CASE WHEN true THEN {a: 1} END.a AS r":                                       {{int64(1)}},
	} {
		result, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		require.Equal(t, want, result.Rows, statement)
	}
}

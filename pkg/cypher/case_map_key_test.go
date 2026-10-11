package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A name that is a CASE keyword (case, when, then, else, end) used as a map
// key, property, label or parameter inside a CASE doesn't open, split or close
// it. Recorded on Neo4j 5.26.30.
func TestCaseKeywordNamesInsideCase(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "case_map_key"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:End {end: 5, case: 6})", nil)
	require.NoError(t, err)
	for query, want := range map[string]interface{}{
		"RETURN CASE WHEN true THEN {end: 1} ELSE null END AS v":                     map[string]interface{}{"end": int64(1)},
		"RETURN CASE WHEN true THEN {case: 1, end: 2} ELSE null END AS v":            map[string]interface{}{"case": int64(1), "end": int64(2)},
		"RETURN CASE WHEN false THEN null ELSE {start: 1, end : 2} END AS v":         map[string]interface{}{"start": int64(1), "end": int64(2)},
		"MATCH (n:End) RETURN CASE WHEN n.end > 1 THEN n.case ELSE 0 END AS v":       int64(6),
		"MATCH (n) WHERE CASE WHEN n:End THEN true ELSE false END RETURN n.end AS v": int64(5),
		"RETURN CASE WHEN $end = 1 THEN 'one' ELSE 'other' END AS v":                 "one",
		"RETURN CASE WHEN true THEN {a: CASE WHEN true THEN {end: 3} END} END AS v":  map[string]interface{}{"a": map[string]interface{}{"end": int64(3)}},
		"RETURN CASE WHEN true THEN 1 END :: INTEGER AS v":                           true,
		"WITH {end: 7} AS m RETURN CASE WHEN m.end = 7 THEN m.end END AS v":          int64(7),
	} {
		result, err := exec.Execute(ctx, query, map[string]interface{}{"end": int64(1)})
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	for _, keyword := range []string{"case", "when", "then", "else", "end"} {
		for _, query := range []string{
			"WITH {" + keyword + ": 3} AS m RETURN CASE WHEN m." + keyword + " = 3 THEN m." + keyword + " ELSE 0 END AS v",
			"WITH {" + keyword + ": 3} AS m RETURN CASE m." + keyword + " WHEN 3 THEN m." + keyword + " ELSE 0 END AS v",
			"RETURN CASE WHEN true THEN {" + keyword + ": 3}." + keyword + " ELSE {" + keyword + ": 0} END AS v",
		} {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows, query)
		}
	}
}

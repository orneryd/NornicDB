package cypher

import (
	"context"
	"sort"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// SET and REMOVE take dynamic labels ($(expr)) and property keys (n[expr]) as
// Neo4j 5.26.30 does, in every clause that writes them: SET, REMOVE, MERGE
// actions, FOREACH (#907). Results and counters are Neo4j's.
func TestDynamicLabelsAndKeysInWrites(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dynamic_writes"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:DW {id: 1, p1: 0})-[:DR {w: 1}]->(:DW {id: 2})", nil)
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
	sortedStrings := func(value interface{}) []string {
		var out []string
		switch typed := value.(type) {
		case []string:
			out = append(out, typed...)
		case []interface{}:
			for _, item := range typed {
				out = append(out, item.(string))
			}
		}
		sort.Strings(out)
		return out
	}
	params := map[string]interface{}{"p": "PV", "list": []interface{}{"L1", "L2"}, "key": "pk"}

	labelCases := map[string][]string{
		"MATCH (n:DW {id: 1}) SET n:$('DynA') RETURN labels(n) AS l":                                      {"DW", "DynA"},
		"MATCH (n:DW {id: 1}) SET n:$('Dyn' + 'B') RETURN labels(n) AS l":                                 {"DW", "DynB"},
		"MATCH (n:DW {id: 1}) SET n:$(['C1', 'C2']) RETURN labels(n) AS l":                                {"C1", "C2", "DW"},
		"MATCH (n:DW {id: 1}) SET n:$([]) RETURN labels(n) AS l":                                          {"DW"},
		"WITH 'V' AS lbl MATCH (n:DW {id: 1}) SET n:$(lbl) RETURN labels(n) AS l":                         {"DW", "V"},
		"MATCH (n:DW {id: 1}) SET n:$($p) RETURN labels(n) AS l":                                          {"DW", "PV"},
		"MATCH (n:DW {id: 1}) SET n:$($list) RETURN labels(n) AS l":                                       {"DW", "L1", "L2"},
		"MATCH (n:DW {id: 1}) SET n:$('A B') RETURN labels(n) AS l":                                       {"A B", "DW"},
		"MATCH (n:DW {id: 1}) SET n:$('DynA'):DynB RETURN labels(n) AS l":                                 {"DW", "DynA", "DynB"},
		"MATCH (n:DW {id: 1}) SET n:Fixed:$('DynA') RETURN labels(n) AS l":                                {"DW", "DynA", "Fixed"},
		"MATCH (n:DW {id: 1}) SET n IS $('DynA') RETURN labels(n) AS l":                                   {"DW", "DynA"},
		"MATCH (n:DW {id: 1}) SET n:A:B:C REMOVE n:$('A') RETURN labels(n) AS l":                          {"B", "C", "DW"},
		"MATCH (n:DW {id: 1}) SET n:A:B:C REMOVE n:$(['A', 'B']) RETURN labels(n) AS l":                   {"C", "DW"},
		"MATCH (n:DW {id: 1}) SET n:A:B:C REMOVE n:$([]) RETURN labels(n) AS l":                           {"A", "B", "C", "DW"},
		"MATCH (n:DW {id: 1}) SET n:A:B:C REMOVE n:$('A'):B RETURN labels(n) AS l":                        {"C", "DW"},
		"MATCH (n:DW {id: 1}) SET n:A REMOVE n IS $('A') RETURN labels(n) AS l":                           {"DW"},
		"MATCH (n:DW {id: 1}) SET n:A REMOVE n:$('DW') RETURN labels(n) AS l":                             {"A"},
		"MERGE (n:DW {id: 1}) ON MATCH SET n:$('OnMatch') RETURN labels(n) AS l":                          {"DW", "OnMatch"},
		"MATCH (n:DW {id: 1}) FOREACH (label IN ['F1', 'F2'] | SET n:$(label)) RETURN labels(n) AS l":     {"DW", "F1", "F2"},
		"MATCH (n:DW {id: 1}) WITH n, 'W' + toString(n.id) AS name SET n:$(name) RETURN labels(n) AS l":   {"DW", "W1"},
		"MATCH (n:DW {id: 1}) SET n:A, n:$('B') REMOVE n:$(['A']) SET n:$('C') RETURN labels(n) AS l":     {"B", "C", "DW"},
		"UNWIND ['U1', 'U2'] AS label MATCH (n:DW {id: 1}) SET n:$(label) RETURN DISTINCT labels(n) AS l": nil,
	}
	for query, want := range labelCases {
		result, err := inTransaction(query, params)
		require.NoError(t, err, query)
		if want == nil {
			continue
		}
		require.Len(t, result.Rows, 1, query)
		require.Equal(t, want, sortedStrings(result.Rows[0][0]), query)
	}

	for query, want := range map[string]map[string]interface{}{
		"MATCH (n:DW {id: 1}) SET n['k1'] = 5 RETURN n {.*} AS v":                                                                 {"id": int64(1), "p1": int64(0), "k1": int64(5)},
		"MATCH (n:DW {id: 1}) SET n['k' + toString(2)] = 5 RETURN n {.*} AS v":                                                    {"id": int64(1), "p1": int64(0), "k2": int64(5)},
		"MATCH (n:DW {id: 1}) SET n[$key] = 5 RETURN n {.*} AS v":                                                                 {"id": int64(1), "p1": int64(0), "pk": int64(5)},
		"MATCH (n:DW {id: 1}) SET (n)['k'] = 1 RETURN n {.*} AS v":                                                                {"id": int64(1), "p1": int64(0), "k": int64(1)},
		"MATCH (n:DW {id: 1}) SET n['A B'] = 1 RETURN n {.*} AS v":                                                                {"id": int64(1), "p1": int64(0), "A B": int64(1)},
		"MATCH (n:DW {id: 1}) SET n['p1'] = null RETURN n {.*} AS v":                                                              {"id": int64(1)},
		"MATCH (n:DW {id: 1}) SET n.p1 = 7, n['p1'] = n.p1 + 1 RETURN n {.*} AS v":                                                {"id": int64(1), "p1": int64(1)},
		"MATCH (n:DW {id: 1}) SET n[CASE WHEN n.id = 1 THEN 'one' END] = 1 RETURN n {.*} AS v":                                    {"id": int64(1), "p1": int64(0), "one": int64(1)},
		"MATCH (n:DW {id: 1}) REMOVE n['p1'] RETURN n {.*} AS v":                                                                  {"id": int64(1)},
		"WITH 'p1' AS k MATCH (n:DW {id: 1}) REMOVE n[k] RETURN n {.*} AS v":                                                      {"id": int64(1)},
		"WITH '' AS k MATCH (n:DW {id: 1}) REMOVE n[k] RETURN n {.*} AS v":                                                        {"id": int64(1), "p1": int64(0)},
		"MATCH (n:DW {id: 1}) REMOVE n['missing'], n.p1 RETURN n {.*} AS v":                                                       {"id": int64(1)},
		"MERGE (n:DW {id: 1}) ON MATCH SET n['m'] = 3 RETURN n {.*} AS v":                                                         {"id": int64(1), "p1": int64(0), "m": int64(3)},
		"MERGE (n:DWNew {id: 9}) ON CREATE SET n['c'] = 3 RETURN n {.*} AS v":                                                     {"id": int64(9), "c": int64(3)},
		"MATCH (n:DW {id: 1}) FOREACH (k IN ['f1', 'f2'] | SET n[k] = 1) RETURN n {.*} AS v":                                      {"id": int64(1), "p1": int64(0), "f1": int64(1), "f2": int64(1)},
		"MATCH (:DW {id: 1})-[r:DR]->() SET r['x'] = 2 RETURN r {.*} AS v":                                                        {"w": int64(1), "x": int64(2)},
		"MATCH (:DW {id: 1})-[r:DR]->() REMOVE r['w'] RETURN r {.*} AS v":                                                         {},
		"MATCH (:DW {id: 1})-[r:DR]->() WITH r, 'w' AS k REMOVE r[k] RETURN r {.*} AS v":                                          {},
		"MATCH (n:DW {id: 1}) WITH n, ['a', 'b'] AS ks UNWIND ks AS k SET n[k] = size(k) RETURN DISTINCT n.id AS id, n {.*} AS v": nil,
	} {
		result, err := inTransaction(query, params)
		require.NoError(t, err, query)
		if want == nil {
			continue
		}
		require.Len(t, result.Rows, 1, query)
		require.Equal(t, want, result.Rows[0][len(result.Rows[0])-1], query)
	}

	// Counters: a dynamic label counts as a label, a dynamic key as a
	// property (Neo4j's counters).
	result, err := inTransaction("MATCH (n:DW {id: 1}) SET n:$(['X', 'Y']), n['k'] = 1 RETURN 1 AS v", params)
	require.NoError(t, err)
	require.Equal(t, 2, result.Stats.LabelsAdded)
	require.Equal(t, 1, result.Stats.PropertiesSet)
	result, err = inTransaction("MATCH (n:DW {id: 1}) SET n:X:Y REMOVE n:$(['X', 'Nope']), n['p1'], n['nope'] RETURN 1 AS v", params)
	require.NoError(t, err)
	require.Equal(t, 1, result.Stats.LabelsRemoved)
	require.Equal(t, 1, result.Stats.PropertiesSet)

	for query, code := range map[string]string{
		// Known when compiling: SyntaxError.
		"MATCH (n:DW {id: 1}) SET n:$(null) RETURN n":              "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:DW {id: 1}) SET n:$('') RETURN n":                "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:DW {id: 1}) SET n:$(1) RETURN n":                 "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:DW {id: 1}) SET n:$(['A', null]) RETURN n":       "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:DW {id: 1}) REMOVE n:$(null) RETURN n":           "Neo.ClientError.Statement.SyntaxError",
		"UNWIND [1] AS x MATCH (n:DW {id: 1}) SET n:$(x) RETURN n": "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:DW {id: 1}) SET n[null] = 1 RETURN n":            "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:DW {id: 1}) SET n[''] = 1 RETURN n":              "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:DW {id: 1}) SET n[1] = 1 RETURN n":               "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:DW {id: 1}) SET n[['k']] = 1 RETURN n":           "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:DW {id: 1}) REMOVE n[''] RETURN n":               "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:DW {id: 1}) REMOVE n[null] RETURN n":             "Neo.ClientError.Statement.SyntaxError",
		"MATCH (:DW {id: 1})-[r:DR]->() SET r:$('X') RETURN r":     "Neo.ClientError.Statement.SyntaxError",
		"MATCH (:DW {id: 1})-[r:DR]->() REMOVE r:$('X') RETURN r":  "Neo.ClientError.Statement.SyntaxError",
		"WITH {a: 1} AS map SET map['b'] = 2 RETURN map":           "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:DW {id: 1}) SET n IS $('A'):B RETURN n":          "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:DW {id: 1}) SET n['k'] += 1 RETURN n":            "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:DW {id: 1}) SET n[missing] = 1 RETURN n":         "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:DW {id: 1}) SET n:$(missing) RETURN n":           "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:DW {id: 1}) REMOVE n[missing] RETURN n":          "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:DW {id: 1}) REMOVE n:$(missing) RETURN n":        "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:DW {id: 1}) REMOVE m:$('A') RETURN n":            "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:DW {id: 1}) REMOVE n:$() RETURN n":               "Neo.ClientError.Statement.SyntaxError",
		// Known only at run time.
		"UNWIND [null] AS x MATCH (n:DW {id: 1}) SET n:$(x) RETURN n":          "Neo.ClientError.Statement.TypeError",
		"MATCH (n:DW {id: 1}) SET n:$(n.id) RETURN n":                          "Neo.ClientError.Statement.TypeError",
		"MATCH (n:DW {id: 1}) SET n:$([n.id]) RETURN n":                        "Neo.ClientError.Statement.TypeError",
		"UNWIND [null] AS x MATCH (n:DW {id: 1}) REMOVE n:$(x) RETURN n":       "Neo.ClientError.Statement.TypeError",
		"UNWIND [''] AS x MATCH (n:DW {id: 1}) SET n:$(x) RETURN n":            "Neo.ClientError.Schema.TokenNameError",
		"UNWIND [''] AS x MATCH (n:DW {id: 1}) SET n[x] = 1 RETURN n":          "Neo.ClientError.Schema.TokenNameError",
		"UNWIND [null] AS x MATCH (n:DW {id: 1}) SET n[x] = 1 RETURN n":        "Neo.ClientError.Statement.TypeError",
		"UNWIND [null] AS x MATCH (n:DW {id: 1}) REMOVE n[x] RETURN n":         "Neo.ClientError.Statement.TypeError",
		"MATCH (n:DW {id: 1}) SET n[n.id] = 1 RETURN n":                        "Neo.ClientError.Statement.TypeError",
		"MATCH (n:DW {id: 1})-[r:DR]->() WITH r, 'X' AS l SET r:$(l) RETURN r": "Neo.ClientError.Statement.SyntaxError",
	} {
		_, err := inTransaction(query, params)
		require.Error(t, err, query)
		got, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, code, got, "%s: %v", query, err)
	}
	_, err = inTransaction("MATCH (n:DW {id: 1}) SET n[$missing] = 1 RETURN n", nil)
	require.Error(t, err)
}

// A statement that sets a dynamic label invalidates cached reads of any
// label: its labels are known only at run time (dynamicLabelStartsAt).
func TestDynamicLabelWriteInvalidatesCachedReads(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dynamic_cache"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:DC {id: 1})", nil)
	require.NoError(t, err)
	count := func() int64 {
		result, err := exec.Execute(ctx, "MATCH (n:Tagged) RETURN count(n) AS c", nil)
		require.NoError(t, err)
		return result.Rows[0][0].(int64)
	}
	require.Equal(t, int64(0), count())
	_, err = exec.Execute(ctx, "MATCH (n:DC) SET n:$('Tag' + 'ged')", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), count())
	_, err = exec.Execute(ctx, "MATCH (n:DC) REMOVE n:$(['Tagged'])", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), count())
}

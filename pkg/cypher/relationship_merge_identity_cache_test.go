package cypher

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestRelationshipMergeIdentityCacheMatchesDirectLookup runs multi-row
// relationship MERGEs with the per-statement identity cache and with a
// storage read per row, and requires the same rows, counters and stored
// relationships: every cache update and drop (created relationships, SET,
// ON CREATE SET and ON MATCH SET of identity properties, unbound endpoints)
// keeps the cache's answer the lookup's.
func TestRelationshipMergeIdentityCacheMatchesDirectLookup(t *testing.T) {
	setup := []string{
		"CREATE (:S {id: 1}), (:T {id: 2}), (:T {id: 3})",
		"MATCH (s:S), (t:T {id: 2}) CREATE (s)-[:R {k: 'a', n: 1}]->(t), (s)-[:R {k: 'a', n: 2}]->(t), (s)-[:R {k: 'b', n: 3}]->(t)",
	}
	statements := []string{
		// Duplicates, repeated identities, a created one reused by a later row.
		"UNWIND ['a', 'a', 'b', 'c', 'c'] AS k MATCH (s:S {id: 1}) MATCH (t:T {id: 2}) MERGE (s)-[r:R {k: k}]->(t) SET r.seen = coalesce(r.seen, 0) + 1 RETURN count(*) AS c",
		"UNWIND ['a', 'a', 'b', 'c', 'c'] AS k MATCH (s:S {id: 1}) MATCH (t:T {id: 2}) MERGE (s)-[r:R {k: k}]->(t) RETURN k, r.n AS n ORDER BY k, n",
		// SET changes the identity: a later row with the old identity creates.
		"UNWIND ['a', 'a', 'x', 'x'] AS k MATCH (s:S {id: 1}) MATCH (t:T {id: 2}) MERGE (s)-[r:R {k: k}]->(t) SET r.k = k + '2' RETURN count(*) AS c",
		// ON CREATE SET changes the created relationship's identity.
		"UNWIND ['x', 'x', 'x2'] AS k MATCH (s:S {id: 1}) MATCH (t:T {id: 2}) MERGE (s)-[r:R {k: k}]->(t) ON CREATE SET r.k = k + '2' RETURN k, r.k AS rk ORDER BY k, rk",
		// ON MATCH SET changes a matched relationship's identity.
		"UNWIND ['a', 'a', 'b'] AS k MATCH (s:S {id: 1}) MATCH (t:T {id: 2}) MERGE (s)-[r:R {k: k}]->(t) ON MATCH SET r.k = 'b' RETURN k, r.n AS n ORDER BY k, n",
		// Other endpoint pairs in the same statement.
		"UNWIND [2, 3, 2, 3] AS id MATCH (s:S {id: 1}) MATCH (t:T {id: id}) MERGE (s)-[r:R {k: 'a'}]->(t) RETURN id, r.n AS n ORDER BY id, n",
		// An endpoint the MERGE binds itself.
		"UNWIND ['a', 'a', 'z'] AS k MATCH (s:S {id: 1}) MERGE (s)-[r:R {k: k}]->(t:T {id: 2}) RETURN k, count(*) AS c ORDER BY k",
		// Values that compare equal: 1 and 1.0; NaN matches nothing.
		"UNWIND [1, 1.0, 1] AS v MATCH (s:S {id: 1}) MATCH (t:T {id: 3}) MERGE (s)-[r:N {v: v}]->(t) RETURN count(*) AS c",
		"UNWIND [0.0 / 0.0, 0.0 / 0.0] AS v MATCH (s:S {id: 1}) MATCH (t:T {id: 3}) MERGE (s)-[r:NaN {v: v}]->(t) RETURN count(*) AS c",
		// Lists and maps as identity values.
		"UNWIND [[1, 2], [1, 2], [2, 1]] AS v MATCH (s:S {id: 1}) MATCH (t:T {id: 3}) MERGE (s)-[r:L {v: v}]->(t) RETURN count(*) AS c",
		// An undirected pattern reads both directions.
		"UNWIND ['a', 'a'] AS k MATCH (s:S {id: 1}) MATCH (t:T {id: 2}) MERGE (t)-[r:R {k: k}]-(s) RETURN count(*) AS c",
	}
	for _, statement := range statements {
		var outcomes [2]string
		for mode, disabled := range []bool{false, true} {
			mergeIdentityCacheDisabled = disabled
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cache"))
			ctx := context.Background()
			for _, query := range setup {
				_, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err)
			}
			result, err := exec.Execute(ctx, statement, nil)
			var out strings.Builder
			if err != nil {
				fmt.Fprintf(&out, "error: %v\n", err)
			} else {
				fmt.Fprintf(&out, "columns %v rows %v created %d set %d\n", result.Columns, result.Rows, result.Stats.RelationshipsCreated, result.Stats.PropertiesSet)
			}
			graph, err := exec.Execute(ctx, "MATCH (a)-[r]->(b) RETURN a.id AS a, type(r) AS t, properties(r) AS p, b.id AS b", nil)
			require.NoError(t, err)
			lines := make([]string, 0, len(graph.Rows))
			for _, row := range graph.Rows {
				lines = append(lines, fmt.Sprint(row))
			}
			sort.Strings(lines)
			out.WriteString(strings.Join(lines, "\n"))
			outcomes[mode] = out.String()
		}
		mergeIdentityCacheDisabled = false
		require.Equal(t, outcomes[1], outcomes[0], statement)
	}
}

// TestRelationshipMergeIdentityKeyIsTheComparison: two identity values get
// the same key exactly when relationshipMergeValuesEqual holds.
func TestRelationshipMergeIdentityKeyIsTheComparison(t *testing.T) {
	values := []interface{}{
		"a", "b", "", "1", true, false, int64(0), int64(1), int64(-1), 0.0, -0.0, 1.0, 1.5, -1.5,
		float32(0), float32(-0.0) * -1, float32(1.5), int(1), int32(1),
		[]interface{}{int64(1), int64(2)}, []interface{}{1.0, 2.0}, []interface{}{int64(2), int64(1)}, []interface{}{},
		[]string{"a"}, []interface{}{"a"}, map[string]interface{}{"k": "v"}, map[string]interface{}{"k": 1.0}, map[string]interface{}{"k": int64(1)},
	}
	negativeZero := float32(0)
	negativeZero = -negativeZero
	values = append(values, negativeZero, []interface{}{negativeZero})
	for _, left := range values {
		for _, right := range values {
			leftKey, leftOK := relationshipMergeIdentityValuesKey(map[string]interface{}{"p": left}, []string{"p"})
			rightKey, rightOK := relationshipMergeIdentityValuesKey(map[string]interface{}{"p": right}, []string{"p"})
			require.True(t, leftOK && rightOK)
			require.Equal(t, relationshipMergeValuesEqual(left, right), leftKey == rightKey, "%#v vs %#v", left, right)
		}
	}
}

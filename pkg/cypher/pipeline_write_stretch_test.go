package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A clause reads what every row of an earlier clause wrote, and nothing a
// later row will write, as in Neo4j 5.26.30 (#907).
func TestWriteStretchReadsAreClauseByClause(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "write_stretch"))
	ctx := context.Background()
	inRolledBackTransaction := func(query string) [][]interface{} {
		_, err := exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		defer func() {
			_, err := exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	_, err := exec.Execute(ctx, "CREATE (:A {n: 1}), (:Done {flag: false})", nil)
	require.NoError(t, err)

	require.Equal(t, [][]interface{}{{int64(1), int64(3)}, {int64(2), int64(3)}, {int64(3), int64(3)}},
		inRolledBackTransaction("UNWIND [1, 2, 3] AS k CREATE (w:W {k: k}) WITH w MATCH (v:W) RETURN w.k AS k, count(v) AS c ORDER BY k"))
	// Every row's MATCH runs before any row's CREATE: 2 rows, 2 nodes made.
	require.Equal(t, [][]interface{}{{int64(2)}},
		inRolledBackTransaction("UNWIND [1, 2] AS k MATCH (a:A) CREATE (:A) RETURN count(*) AS c"))
	require.Equal(t, [][]interface{}{{int64(3)}},
		inRolledBackTransaction("UNWIND [1, 2] AS k MATCH (a:A) CREATE (:A) WITH 1 AS one MATCH (x:A) RETURN count(DISTINCT x) AS c"))
	// A property a later clause matches on: every row sees every SET.
	require.Equal(t, [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(1)}},
		inRolledBackTransaction("UNWIND [1, 2] AS k MATCH (d:Done) SET d.flag = (k = 2) WITH k MATCH (x:Done {flag: true}) RETURN k, count(x) AS c ORDER BY k"))
	// MERGE sees the rows before it within its own clause.
	require.Equal(t, [][]interface{}{{int64(1)}},
		inRolledBackTransaction("UNWIND [1, 1, 1] AS k MERGE (m:M {k: k}) WITH DISTINCT m RETURN count(m) AS c"))
}

func TestPipelineWriteStretchConflicts(t *testing.T) {
	conflicts := func(query string) bool {
		clauses, ok, _ := parsePipelineClauses(query)
		require.True(t, ok, query)
		return pipelineWriteStretchConflicts(clauses)
	}
	// Bulk-load shapes stay row by row: nothing read is written.
	for _, query := range []string{
		"MATCH (a:Person {id: row.a}) MATCH (b:Person {id: row.b}) CREATE (a)-[:KNOWS]->(b)",
		"MERGE (n:Person {id: row.id}) SET n.name = row.name, n.age = row.age",
		"CREATE (n:Doc {id: row.id}) SET n.title = row.title",
		"MATCH (a:Person {id: row.a}) CREATE (a)-[:WROTE]->(:Post {title: row.t})",
	} {
		require.False(t, conflicts(query), query)
	}
	for _, query := range []string{
		"CREATE (w:W {k: k}) WITH w MATCH (v:W)",
		"MATCH (a:A) CREATE (:A)",
		"MATCH (a) CREATE (:B)",
		"MATCH (a)-[:R]->(b) CREATE (b)-[:R]->(:C)",
		"MATCH ()-[r]->() CREATE (:C)-[:T]->(:D)",
		"MATCH (n:Done {flag: false}) SET n.flag = true WITH n MATCH (x {flag: true})",
		"MERGE (n:Person {id: row.id}) SET n += row.props",
		"MATCH (n:A) SET n:B WITH n MATCH (m:B)",
		"MATCH (n:A) REMOVE n:A WITH n MATCH (m:A)",
		"MATCH (n:A) REMOVE n.k WITH n MATCH (m {k: 1})",
		"MATCH (n:A) DELETE n",
		"MATCH (n:A) SET n:$(row.label) WITH n MATCH (m:B)",
		"MERGE (a:P {id: row.a}) MERGE (b:P {id: row.b})",
		"MATCH (n:A) SET n.k = 1 WITH n, properties(n) AS p",
	} {
		require.True(t, conflicts(query), query)
	}
}

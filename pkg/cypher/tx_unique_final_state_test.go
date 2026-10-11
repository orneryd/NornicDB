package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A uniqueness constraint is checked against the transaction's final state:
// a value an entity gives up in the transaction, by being deleted or by
// changing it, is free for another entity in the same transaction. Recorded
// on Neo4j 5.26.30. Reported by the Personal Documents integration (I25).
func TestUniquenessIsCheckedAgainstTheTransactionsFinalState(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	run := func(t *testing.T, query string) [][]interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	run(t, "CREATE CONSTRAINT i25_rel FOR ()-[r:I25_REL]-() REQUIRE r.id IS UNIQUE")
	run(t, "CREATE CONSTRAINT i25_node FOR (n:I25N) REQUIRE n.id IS UNIQUE")
	run(t, "CREATE (:I25A {id: 'a'})-[:I25_REL {id: 'link'}]->(:I25A {id: 'b'})")

	t.Run("relationship deleted, then recreated with its value", func(t *testing.T) {
		rows := run(t, "MATCH (a:I25A {id: 'a'})-[r:I25_REL {id: 'link'}]->(b:I25A {id: 'b'}) DELETE r WITH a, b CREATE (a)-[n:I25_REL {id: 'link', kind: 'changed'}]->(b) RETURN n.id, n.kind")
		require.Equal(t, [][]interface{}{{"link", "changed"}}, rows)
		require.Equal(t, [][]interface{}{{"link", "changed"}}, run(t, "MATCH (:I25A)-[r:I25_REL]->(:I25A) RETURN r.id, r.kind"))
	})
	t.Run("relationship value changed, then taken by a new one", func(t *testing.T) {
		run(t, "MATCH (a:I25A {id: 'a'})-[r:I25_REL {id: 'link'}]->(b:I25A {id: 'b'}) SET r.id = 'moved' WITH a, b CREATE (a)-[:I25_REL {id: 'link'}]->(b)")
		require.Equal(t, [][]interface{}{{"link"}, {"moved"}}, run(t, "MATCH (:I25A)-[r:I25_REL]->(:I25A) RETURN r.id ORDER BY r.id"))
	})
	t.Run("node deleted, then recreated with its value", func(t *testing.T) {
		run(t, "CREATE (:I25N {id: 'x'})")
		require.Equal(t, [][]interface{}{{"x"}}, run(t, "MATCH (n:I25N {id: 'x'}) DELETE n WITH 1 AS one CREATE (m:I25N {id: 'x'}) RETURN m.id"))
	})
	t.Run("node value changed, then taken by a new one", func(t *testing.T) {
		require.Equal(t, [][]interface{}{{"x"}}, run(t, "MATCH (n:I25N {id: 'x'}) SET n.id = 'y' WITH 1 AS one CREATE (m:I25N {id: 'x'}) RETURN m.id"))
		require.Equal(t, [][]interface{}{{"x"}, {"y"}}, run(t, "MATCH (n:I25N) RETURN n.id ORDER BY n.id"))
	})
	t.Run("a value still held is a violation", func(t *testing.T) {
		_, err := exec.Execute(ctx, "MATCH (a:I25A {id: 'a'})-[r:I25_REL {id: 'link'}]->(b) CREATE (a)-[:I25_REL {id: 'link'}]->(b)", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")
		_, err = exec.Execute(ctx, "CREATE (:I25N {id: 'y'})", nil)
		require.Error(t, err)
	})
}

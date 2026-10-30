package nornicdb

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A relationship MERGEd in an explicit transaction that rolls back, then in
// one that commits, is still there after the database is reopened. MERGE
// derives the relationship's ID from its endpoints and properties, so the
// retry reuses the rolled-back attempt's ID. Reported by the Personal
// Documents integration (#907, I29).
func TestMergedRelationshipSurvivesReopenAfterRolledBackAttempt(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	// A session executor, as the Bolt and HTTP servers give each transaction.
	session := func(db *DB) *cypher.StorageExecutor {
		return cypher.NewStorageExecutor(storage.NewNamespacedEngine(db.baseStorage, "nornic"))
	}
	db, err := Open(dir, nil)
	require.NoError(t, err)
	_, err = session(db).Execute(ctx, `CREATE CONSTRAINT r_id FOR ()-[r:R]-() REQUIRE r.id IS UNIQUE`, nil)
	require.NoError(t, err)
	_, err = session(db).Execute(ctx, `CREATE (:N {id: "a"}), (:N {id: "b"})`, nil)
	require.NoError(t, err)
	merge := `MATCH (a:N {id: "a"}), (b:N {id: "b"}) MERGE (a)-[r:R {id: "new"}]->(b)`
	for _, terminal := range []string{"ROLLBACK", "COMMIT"} {
		exec := session(db)
		_, err := exec.Execute(cypher.WithTransactionControl(ctx), "BEGIN", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, merge, nil)
		require.NoError(t, err)
		_, err = exec.Execute(cypher.WithTransactionControl(ctx), terminal, nil)
		require.NoError(t, err)
	}
	read := `MATCH (a:N {id: "a"})-[r:R]->(b:N {id: "b"}) RETURN r.id`
	result, err := session(db).Execute(ctx, read, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"new"}}, result.Rows)
	require.NoError(t, db.Close())

	db, err = Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	result, err = session(db).Execute(ctx, read, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"new"}}, result.Rows, "after reopen")
}

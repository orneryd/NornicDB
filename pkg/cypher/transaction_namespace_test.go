package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func countFromResult(t *testing.T, result *ExecuteResult) int64 {
	t.Helper()
	if result == nil || len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		t.Fatalf("missing count result: %+v", result)
	}
	switch v := result.Rows[0][0].(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	default:
		t.Fatalf("unexpected count type %T (%v)", v, v)
		return 0
	}
}

func executeExplicitTransactionQueryForTrace(t *testing.T, exec *StorageExecutor, ctx context.Context, query string) (*ExecuteResult, HotPathTrace) {
	t.Helper()
	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	exec.resetHotPathTrace()
	result, err := exec.Execute(ctx, strings.TrimSpace(query), nil)
	require.NoError(t, err)
	trace := exec.LastHotPathTrace()

	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	return result, trace
}

func TestExplicitTransaction_NamespacedCreateCommit(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}
	if _, err := exec.Execute(ctx, "CREATE (n:TxNs {name: 'commit'})", nil); err != nil {
		t.Fatalf("CREATE in tx failed: %v", err)
	}
	if _, err := exec.Execute(ctx, "COMMIT", nil); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	result, err := exec.Execute(ctx, "MATCH (n:TxNs {name: 'commit'}) RETURN count(n) AS c", nil)
	if err != nil {
		t.Fatalf("verification query failed: %v", err)
	}
	if got := countFromResult(t, result); got != 1 {
		t.Fatalf("expected 1 committed node, got %d", got)
	}
}

func TestExplicitTransaction_NamespacedCreateRollback(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}
	if _, err := exec.Execute(ctx, "CREATE (n:TxNs {name: 'rollback'})", nil); err != nil {
		t.Fatalf("CREATE in tx failed: %v", err)
	}
	if _, err := exec.Execute(ctx, "ROLLBACK", nil); err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}

	result, err := exec.Execute(ctx, "MATCH (n:TxNs {name: 'rollback'}) RETURN count(n) AS c", nil)
	if err != nil {
		t.Fatalf("verification query failed: %v", err)
	}
	if got := countFromResult(t, result); got != 0 {
		t.Fatalf("expected 0 rolled-back nodes, got %d", got)
	}
}

func TestExplicitTransaction_RelationshipConstraintUsesFinalStateAtCommit(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE CONSTRAINT follows_since_exists FOR ()-[r:FOLLOWS]-() REQUIRE r.since IS NOT NULL`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `CREATE (:Person {id: 'tx-rel-a'}), (:Person {id: 'tx-rel-b'})`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
		MATCH (a:Person {id: 'tx-rel-a'}), (b:Person {id: 'tx-rel-b'})
		CREATE (a)-[:FOLLOWS {note: 'set later in tx'}]->(b)
	`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
		MATCH (:Person {id: 'tx-rel-a'})-[r:FOLLOWS]->(:Person {id: 'tx-rel-b'})
		SET r.since = 2026
		RETURN r.since
	`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
		MATCH (:Person {id: 'tx-rel-a'})-[r:FOLLOWS]->(:Person {id: 'tx-rel-b'})
		RETURN r.since, r.note
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(2026), result.Rows[0][0])
	assert.Equal(t, "set later in tx", result.Rows[0][1])
}

func TestTransactionStatementRoutingAndErrors(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	// Unknown statement returns nil,nil (not a tx command).
	res, err := exec.parseTransactionStatement("MATCH (n) RETURN n")
	require.NoError(t, err)
	assert.Nil(t, res)

	// BEGIN route.
	res, err = exec.parseTransactionStatement("BEGIN TRANSACTION")
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, "Transaction started", res.Rows[0][0])

	// Active transaction guard on BEGIN.
	_, err = exec.parseTransactionStatement("BEGIN")
	require.Error(t, err)

	// COMMIT route.
	res, err = exec.parseTransactionStatement("COMMIT TRANSACTION")
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, "Transaction committed", res.Rows[0][0])

	// No active transaction for COMMIT and ROLLBACK.
	_, err = exec.parseTransactionStatement("COMMIT")
	require.Error(t, err)
	_, err = exec.parseTransactionStatement("ROLLBACK")
	require.Error(t, err)
}

func TestTransactionUnknownTypeBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	require.NotNil(t, exec.txContext)

	// Force unknown transaction type branch in executeInTransaction.
	exec.txContext.tx = "not-a-transaction"
	_, err = exec.executeInTransaction(ctx, "MATCH (n) RETURN n", "MATCH (N) RETURN N")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown transaction type")

	// Force unknown type branches in commit/rollback handlers.
	exec.txContext.active = true
	exec.txContext.tx = 123
	_, err = exec.handleCommit()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown transaction type")

	exec.txContext.active = true
	exec.txContext.tx = 123
	_, err = exec.handleRollback()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown transaction type")
}

func TestExecuteQueryAgainstStorage_DispatchCoverage(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:         "dispatch-node",
		Labels:     []string{"Dispatch"},
		Properties: map[string]interface{}{"name": "n1"},
	})
	require.NoError(t, err)

	testCases := []struct {
		name      string
		cypher    string
		expectErr bool
		errLike   string
	}{
		{name: "create", cypher: "CREATE (:Dispatch {name:'c1'})", expectErr: false},
		{name: "match", cypher: "MATCH (n:Dispatch) RETURN count(n)", expectErr: false},
		{name: "optional_match", cypher: "OPTIONAL MATCH (n:Dispatch) RETURN n", expectErr: false},
		{name: "merge", cypher: "MERGE (n:Dispatch {name:'m1'}) RETURN n", expectErr: false},
		{name: "delete_prefix", cypher: "DELETE n", expectErr: true},
		{name: "set_prefix", cypher: "SET n.name = 'x'", expectErr: true},
		{name: "match_delete_clause", cypher: "MATCH (n:Dispatch) DELETE n", expectErr: false},
		{name: "match_detach_delete_clause", cypher: "MATCH (n:Dispatch) DETACH DELETE n", expectErr: false},
		{name: "match_remove_clause", cypher: "MATCH (n:Dispatch) REMOVE n.name RETURN count(n)", expectErr: false},
		{name: "create_with_delete_compound", cypher: "CREATE (n:Dispatch {name:'tmp_del'}) WITH n DELETE n", expectErr: false},
		{name: "match_create_with_delete_compound", cypher: "MATCH (n:Dispatch {name:'n1'}) CREATE (m:Dispatch {name:'tmp_del2'}) WITH m DELETE m", expectErr: false},
		{name: "merge_on_create_set", cypher: "MERGE (n:Dispatch {name:'merge_set'}) ON CREATE SET n.age = 1 RETURN n", expectErr: false},
		{name: "match_merge_set_compound", cypher: "MATCH (n:Dispatch {name:'n1'}) MERGE (m:Dispatch {name:'merge_compound'}) SET m.flag = true RETURN m", expectErr: false},
		{name: "return", cypher: "RETURN 1", expectErr: false},
		{name: "call", cypher: "CALL db.labels()", expectErr: false},
		{name: "show_indexes", cypher: "SHOW INDEXES", expectErr: false},
		{name: "show_fulltext_indexes", cypher: "SHOW FULLTEXT INDEXES", expectErr: false},
		{name: "show_constraints", cypher: "SHOW CONSTRAINTS", expectErr: false},
		{name: "show_procedures", cypher: "SHOW PROCEDURES", expectErr: false},
		{name: "show_functions", cypher: "SHOW FUNCTIONS", expectErr: false},
		{name: "show_databases_requires_manager", cypher: "SHOW DATABASES", expectErr: true, errLike: "SHOW DATABASES requires multi-database support"},
		{name: "show_aliases_requires_manager", cypher: "SHOW ALIASES", expectErr: true, errLike: "SHOW ALIASES requires multi-database support"},
		{name: "show_limits_requires_manager", cypher: "SHOW LIMITS FOR DATABASE nornic", expectErr: true, errLike: "SHOW LIMITS requires multi-database support"},
		{name: "show_unsupported", cypher: "SHOW FOO", expectErr: true, errLike: "unsupported SHOW command in transaction"},
		{name: "drop_index_noop", cypher: "DROP INDEX idx_any", expectErr: false},
		{name: "unwind", cypher: "UNWIND [1,2] AS x RETURN x", expectErr: false},
		{name: "with", cypher: "WITH 1 AS x RETURN x", expectErr: false},
		{name: "foreach", cypher: "FOREACH (x IN [1] | CREATE (:Dispatch {name:'f'}))", expectErr: false},
		{name: "unsupported", cypher: "BLAH COMMAND", expectErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := exec.executeQueryAgainstStorage(ctx, tc.cypher, strings.ToUpper(tc.cypher))
			if tc.expectErr {
				require.Error(t, err)
				if tc.errLike != "" {
					require.Contains(t, err.Error(), tc.errLike)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestExplicitTransaction_MatchMergeOnCreateRoutesToMerge(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, stmt := range []string{
		"CREATE CONSTRAINT dispatch_source_uid_unique IF NOT EXISTS FOR (n:DispatchSource) REQUIRE n.uid IS UNIQUE",
		"CREATE CONSTRAINT dispatch_target_uid_unique IF NOT EXISTS FOR (n:DispatchTarget) REQUIRE n.uid IS UNIQUE",
	} {
		_, err := exec.Execute(ctx, stmt, nil)
		require.NoError(t, err)
	}

	_, err := exec.Execute(ctx, "CREATE (:DispatchSource {uid: 'source-1'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:DispatchTarget {uid: 'target-existing', created: false})", nil)
	require.NoError(t, err)

	result, trace := executeExplicitTransactionQueryForTrace(t, exec, ctx, `
		MATCH (s:DispatchSource {uid: 'source-1'})
		MERGE (t:DispatchTarget {uid: 'target-1'})
		ON CREATE SET t.created = true
		MERGE (s)-[rel:DISPATCHES_TO]->(t)
		ON CREATE SET rel.created = true
		RETURN t.uid
	`)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "target-1", result.Rows[0][0])
	require.True(t, trace.MergeSchemaLookupUsed,
		"MATCH...MERGE with ON CREATE SET must route through merge handling and use schema lookup")
	require.False(t, trace.MergeScanFallbackUsed,
		"DispatchTarget.uid has a unique constraint; label scan fallback is not justified")

	verifyCreated, err := exec.Execute(ctx, `
		MATCH (s:DispatchSource {uid: 'source-1'})-[rel:DISPATCHES_TO]->(t:DispatchTarget {uid: 'target-1'})
		RETURN t.created, count(rel)
	`, nil)
	require.NoError(t, err)
	require.Len(t, verifyCreated.Rows, 1)
	require.Equal(t, true, verifyCreated.Rows[0][0])
	require.Equal(t, int64(1), verifyCreated.Rows[0][1])

	result, trace = executeExplicitTransactionQueryForTrace(t, exec, ctx, `
		MATCH (s:DispatchSource {uid: 'source-1'})
		MERGE (t:DispatchTarget {uid: 'target-existing'})
		ON CREATE SET t.created = true
		ON MATCH SET t.matched = true
		SET t.lastSeen = 'tx'
		MERGE (s)-[rel:DISPATCHES_TO]->(t)
		ON CREATE SET rel.created = true
		RETURN t.uid, t.matched, t.lastSeen
	`)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "target-existing", result.Rows[0][0])
	require.Equal(t, true, result.Rows[0][1])
	require.Equal(t, "tx", result.Rows[0][2])
	require.True(t, trace.MergeSchemaLookupUsed,
		"MATCH...MERGE with ON MATCH SET and standalone SET must stay on merge handling")
	require.False(t, trace.MergeScanFallbackUsed,
		"matched DispatchTarget.uid has a unique constraint; label scan fallback is not justified")
}

func TestExecuteQueryAgainstStorage_ShowDispatchWithDatabaseManager(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	mockDBM := newMockDatabaseManager()
	require.NoError(t, mockDBM.CreateDatabase("nornic"))
	require.NoError(t, mockDBM.CreateDatabase("tenant_a"))
	exec.SetDatabaseManager(mockDBM)

	queries := []struct {
		query   string
		minRows int
	}{
		{query: "SHOW DATABASES", minRows: 2},
		{query: "SHOW ALIASES", minRows: 0},
		{query: "SHOW LIMITS FOR DATABASE nornic", minRows: 0},
	}

	for _, tc := range queries {
		res, err := exec.executeQueryAgainstStorage(ctx, tc.query, strings.ToUpper(tc.query))
		require.NoError(t, err, "query should dispatch and succeed: %s", tc.query)
		require.NotNil(t, res)
		require.NotEmpty(t, res.Columns)
		require.GreaterOrEqual(t, len(res.Rows), tc.minRows)
	}
}

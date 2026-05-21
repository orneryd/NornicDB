package cypher

import (
	"context"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionScriptBeginTransactionCommit(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:User {id: 'u-10', age: 21, last_seen: null})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
CREATE OR REPLACE PROCEDURE nornic.touchUser($id, $ts)
MODE WRITE
AS
MATCH (u:User {id: $id})
SET u.last_seen = $ts
RETURN u
`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
BEGIN TRANSACTION
CALL nornic.touchUser('u-10', datetime())
YIELD u
RETURN u.id, u.last_seen
COMMIT
`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "u-10", result.Rows[0][0])
	require.NotNil(t, result.Rows[0][1])
}

func TestTransactionScriptBeginShorthandEquivalent(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:User {id: 'u-11', age: 21, last_seen: null})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
CREATE OR REPLACE PROCEDURE nornic.touchUser($id, $ts)
MODE WRITE
AS
MATCH (u:User {id: $id})
SET u.last_seen = $ts
RETURN u
`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
BEGIN
CALL nornic.touchUser('u-11', datetime())
YIELD u
RETURN u.id, u.last_seen
COMMIT
`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "u-11", result.Rows[0][0])
	require.NotNil(t, result.Rows[0][1])
}

func TestTransactionScriptCaseRollback(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:User {id: 'u-12', age: 10, last_seen: null})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
CREATE OR REPLACE PROCEDURE nornic.touchUser($id, $ts)
MODE WRITE
AS
MATCH (u:User {id: $id})
SET u.last_seen = $ts
RETURN u
`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
BEGIN TRANSACTION
CALL nornic.touchUser('u-12', datetime())
YIELD u
CASE
  WHEN u.age < 18 THEN ROLLBACK
  ELSE
    RETURN u.id, u.last_seen
COMMIT
`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"status"}, result.Columns)
	require.Equal(t, "Transaction rolled back", result.Rows[0][0])

	check, err := exec.Execute(ctx, "MATCH (u:User {id: 'u-12'}) RETURN u.last_seen", nil)
	require.NoError(t, err)
	require.Len(t, check.Rows, 1)
	require.Nil(t, check.Rows[0][0])
}

func TestTransactionScriptBeginTransactionCommit_ANTLRMode(t *testing.T) {
	cleanup := config.WithANTLRParser()
	defer cleanup()

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
BEGIN TRANSACTION
CREATE (u:User {id: 'u-antlr', age: 21, last_seen: datetime()})
RETURN u.id, u.last_seen
COMMIT
`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "u-antlr", result.Rows[0][0])
	require.NotNil(t, result.Rows[0][1])

	check, err := exec.Execute(ctx, "MATCH (u:User {id: 'u-antlr'}) RETURN count(u)", nil)
	require.NoError(t, err)
	require.EqualValues(t, int64(1), check.Rows[0][0])
}

func TestTransactionScriptHelpers_ParseAndMapBuilders(t *testing.T) {
	body, ok := stripBeginTransactionPrefix("BEGIN TRANSACTION RETURN 1 COMMIT")
	require.True(t, ok)
	assert.Equal(t, "RETURN 1 COMMIT", body)

	body, ok = stripBeginTransactionPrefix("BEGIN RETURN 1 COMMIT")
	require.True(t, ok)
	assert.Equal(t, "RETURN 1 COMMIT", body)

	_, ok = stripBeginTransactionPrefix("MATCH (n) RETURN n")
	assert.False(t, ok)

	q, action, ok := splitTransactionScriptTailAction("RETURN 1 COMMIT")
	require.True(t, ok)
	assert.Equal(t, "RETURN 1", q)
	assert.Equal(t, "COMMIT", action)

	q, action, ok = splitTransactionScriptTailAction("RETURN 1 ROLLBACK")
	require.True(t, ok)
	assert.Equal(t, "RETURN 1", q)
	assert.Equal(t, "ROLLBACK", action)

	_, _, ok = splitTransactionScriptTailAction("COMMIT")
	assert.False(t, ok)

	n := &storage.Node{ID: "n1", Labels: []string{"Node"}, Properties: map[string]interface{}{"x": 1}}
	e := &storage.Edge{ID: "e1", StartNode: "n1", EndNode: "n1", Type: "LOOP"}
	nodes, rels := buildRowGraphContext([]string{"n", "r", "value"}, []interface{}{n, e, 42})
	assert.Contains(t, nodes, "n")
	assert.Contains(t, rels, "r")
	assert.Equal(t, n, nodes["n"])
	assert.Equal(t, e, rels["r"])

	vals := buildRowValueMap([]string{"a", "b", "c"}, []interface{}{1, "two"})
	assert.Equal(t, 1, vals["a"])
	assert.Equal(t, "two", vals["b"])
	_, exists := vals["c"]
	assert.False(t, exists)
}

func TestTransactionScript_AdditionalBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// executeTransactionScript non-BEGIN and bare BEGIN paths.
	res, err := exec.executeTransactionScript(ctx, "MATCH (n) RETURN n")
	require.NoError(t, err)
	assert.Nil(t, res)
	res, err = exec.executeTransactionScript(ctx, "BEGIN")
	require.NoError(t, err)
	assert.Nil(t, res)

	// Missing COMMIT/ROLLBACK action should return nil,nil.
	res, err = exec.executeTransactionScript(ctx, "BEGIN RETURN 1")
	require.NoError(t, err)
	assert.Nil(t, res)

	// executeSimpleTransactionScript invalid action rolls back and returns error.
	_, err = exec.executeSimpleTransactionScript(ctx, "RETURN 1 AS x", "INVALID")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid transaction script action")

	// executeSimpleTransactionScript query error path rolls back.
	_, err = exec.executeSimpleTransactionScript(ctx, "BLAH COMMAND", "COMMIT")
	require.Error(t, err)

	// evaluateConditionExpression non-boolean branch.
	ok, err := exec.evaluateConditionExpression(ctx, "'x'", map[string]*storage.Node{}, map[string]*storage.Edge{})
	require.Error(t, err)
	assert.False(t, ok)
}

func TestTransactionStatementHandlers_AdditionalBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)

	res, err := exec.parseTransactionStatement("MATCH (n) RETURN n")
	require.NoError(t, err)
	assert.Nil(t, res)

	res, err = exec.parseTransactionStatement("BEGIN")
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, []string{"status"}, res.Columns)
	require.NotNil(t, exec.txContext)
	assert.True(t, exec.txContext.active)

	_, err = exec.handleBegin()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transaction already active")

	commitRes, err := exec.handleCommit()
	require.NoError(t, err)
	require.Equal(t, []string{"status"}, commitRes.Columns)
	assert.Equal(t, "Transaction committed", commitRes.Rows[0][0])
	assert.Nil(t, exec.txContext)

	_, err = exec.handleCommit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active transaction")

	exec.txContext = &TransactionContext{tx: struct{}{}, active: true}
	_, err = exec.handleCommit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown transaction type")

	exec.txContext = &TransactionContext{tx: struct{}{}, active: true}
	_, err = exec.handleRollback()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown transaction type")

	exec.txContext = nil
	_, err = exec.handleRollback()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active transaction")
}

func TestTransactionStatementHandlers_WithWALAsyncEngine(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cypher-transaction-wal-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	badger, err := storage.NewBadgerEngine(tmpDir)
	require.NoError(t, err)
	defer badger.Close()

	wal, err := storage.NewWAL(tmpDir+"/wal", nil)
	require.NoError(t, err)
	defer wal.Close()

	walEngine := storage.NewWALEngine(badger, wal)
	asyncEngine := storage.NewAsyncEngine(walEngine, nil)
	defer asyncEngine.Close()

	store := storage.NewNamespacedEngine(asyncEngine, "test")
	exec := NewStorageExecutor(store)

	beginRes, err := exec.parseTransactionStatement("BEGIN TRANSACTION")
	require.NoError(t, err)
	require.Equal(t, "Transaction started", beginRes.Rows[0][0])
	require.NotNil(t, exec.txContext)
	assert.NotNil(t, exec.txContext.wal)
	assert.Greater(t, exec.txContext.walSeqStart, uint64(0))

	commitRes, err := exec.parseTransactionStatement("COMMIT TRANSACTION")
	require.NoError(t, err)
	require.Equal(t, "Transaction committed", commitRes.Rows[0][0])
	require.NotNil(t, commitRes.Metadata)
	require.NotNil(t, commitRes.Metadata["receipt"])
}

func TestTransactionHandleCommit_ErrorBranchAfterClosedTx(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)

	_, err := exec.handleBegin()
	require.NoError(t, err)
	require.NotNil(t, exec.txContext)

	tx, ok := exec.txContext.tx.(*storage.BadgerTransaction)
	require.True(t, ok)
	require.NoError(t, tx.Rollback())

	_, err = exec.handleCommit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit failed")
	assert.Nil(t, exec.txContext)
}

func TestTransactionHandleCommit_ErrorBranchAfterClosedTx_WithWAL(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cypher-transaction-commit-fail-wal-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	badger, err := storage.NewBadgerEngine(tmpDir)
	require.NoError(t, err)
	defer badger.Close()

	wal, err := storage.NewWAL(tmpDir+"/wal", nil)
	require.NoError(t, err)
	defer wal.Close()

	walEngine := storage.NewWALEngine(badger, wal)
	store := storage.NewNamespacedEngine(walEngine, "test")
	exec := NewStorageExecutor(store)

	_, err = exec.handleBegin()
	require.NoError(t, err)
	require.NotNil(t, exec.txContext)
	require.NotNil(t, exec.txContext.wal)
	require.Greater(t, exec.txContext.walSeqStart, uint64(0))

	tx, ok := exec.txContext.tx.(*storage.BadgerTransaction)
	require.True(t, ok)
	require.NoError(t, tx.Rollback())

	_, err = exec.handleCommit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit failed")
	assert.Nil(t, exec.txContext)
}

func TestTransactionHandleRollback_ErrorBranchAfterClosedTx(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)

	_, err := exec.handleBegin()
	require.NoError(t, err)
	require.NotNil(t, exec.txContext)

	tx, ok := exec.txContext.tx.(*storage.BadgerTransaction)
	require.True(t, ok)
	require.NoError(t, tx.Rollback())

	_, err = exec.handleRollback()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rollback failed")
	assert.Nil(t, exec.txContext)
}

func TestExecuteInTransaction_SuccessPath(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.handleBegin()
	require.NoError(t, err)
	require.NotNil(t, exec.txContext)
	defer func() {
		if exec.txContext != nil {
			_, _ = exec.handleRollback()
		}
	}()

	res, err := exec.executeInTransaction(ctx, "RETURN 1 AS x", "RETURN 1 AS X")
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, []string{"x"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(1), res.Rows[0][0])
}

func TestExecuteInTransaction_SuccessPath_NoNamespace(t *testing.T) {
	base := newTestMemoryEngine(t)
	exec := NewStorageExecutor(base)
	ctx := context.Background()

	_, err := exec.handleBegin()
	require.NoError(t, err)
	require.NotNil(t, exec.txContext)
	defer func() {
		if exec.txContext != nil {
			_, _ = exec.handleRollback()
		}
	}()

	res, err := exec.executeInTransaction(ctx, "RETURN 2 AS y", "RETURN 2 AS Y")
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, []string{"y"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(2), res.Rows[0][0])
}

func TestTransactionHandleBegin_NoTransactionEngine(t *testing.T) {
	exec := &StorageExecutor{} // no storage engine configured
	_, err := exec.handleBegin()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "engine does not support transactions")
}

func TestTransactionScript_CaseRollbackAdditionalErrorBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.executeCaseRollbackTransactionScript(ctx, "CALL db.labels()")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing CASE block")

	_, err = exec.executeCaseRollbackTransactionScript(ctx, "CALL db.labels() CASE WHEN true THEN ROLLBACK")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid transaction CASE syntax")

	_, err = exec.executeCaseRollbackTransactionScript(ctx, "CALL bad.proc() CASE WHEN true THEN ROLLBACK ELSE RETURN 1 COMMIT")
	require.Error(t, err)

	_, err = exec.executeCaseRollbackTransactionScript(ctx, "CALL dbms.info() YIELD id CASE WHEN 'x' THEN ROLLBACK ELSE RETURN id COMMIT")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not evaluate to boolean")
}

func TestTransactionScript_ProjectAndConditionAdditionalBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Empty return expression should not error and should produce deterministic output.
	out, err := exec.projectTransactionReturn(ctx, &ExecuteResult{
		Columns: []string{"x"},
		Rows:    [][]interface{}{{1}},
	}, "")
	require.NoError(t, err)
	require.NotNil(t, out)
	require.NotEmpty(t, out.Columns)

	// evaluateConditionExpression nil branch.
	ok, err := exec.evaluateConditionExpression(ctx, "", map[string]*storage.Node{}, map[string]*storage.Edge{})
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestTransactionScript_SimpleAndCaseCommitBranches(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:User {id: 'u-commit-tx', age: 30, last_seen: null})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
CREATE OR REPLACE PROCEDURE nornic.touchUserTx($id, $ts)
MODE WRITE
AS
MATCH (u:User {id: $id})
SET u.last_seen = $ts
RETURN u
`, nil)
	require.NoError(t, err)

	// executeSimpleTransactionScript: explicit ROLLBACK action branch.
	rollbackRes, err := exec.executeSimpleTransactionScript(ctx, "RETURN 1 AS x", "ROLLBACK")
	require.NoError(t, err)
	require.Equal(t, []string{"status"}, rollbackRes.Columns)
	require.Len(t, rollbackRes.Rows, 1)
	assert.Equal(t, "Transaction rolled back", rollbackRes.Rows[0][0])

	// executeCaseRollbackTransactionScript: commit projection branch (condition false).
	caseCommitRes, err := exec.executeCaseRollbackTransactionScript(ctx, `
CALL nornic.touchUserTx('u-commit-tx', datetime())
YIELD u
CASE WHEN u.age < 18 THEN ROLLBACK ELSE RETURN u.id AS id, u.last_seen AS seen COMMIT`)
	require.NoError(t, err)
	require.Equal(t, []string{"id", "seen"}, caseCommitRes.Columns)
	require.Len(t, caseCommitRes.Rows, 1)
	assert.Equal(t, "u-commit-tx", caseCommitRes.Rows[0][0])
	assert.NotNil(t, caseCommitRes.Rows[0][1])

	// projectTransactionReturn: direct row map fallback (non-expression column alias).
	projected, err := exec.projectTransactionReturn(ctx, &ExecuteResult{
		Columns: []string{"plain"},
		Rows:    [][]interface{}{{int64(7)}},
	}, "plain AS p")
	require.NoError(t, err)
	require.Equal(t, []string{"p"}, projected.Columns)
	require.Len(t, projected.Rows, 1)
	assert.Equal(t, int64(7), projected.Rows[0][0])
}

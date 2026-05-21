// Package cypher provides tests for the Cypher executor.
package cypher

import (
	"context"
	"errors"
	"math"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sequenceEmbedder struct {
	embs  [][]float32
	errs  []error
	calls int
}

type beginFailEngine struct {
	storage.Engine
}

func (e *beginFailEngine) BeginTransaction() (*storage.BadgerTransaction, error) {
	return nil, errors.New("begin failed")
}

type nonTransactionalEngine struct {
	storage.Engine
}

func (s *sequenceEmbedder) Embed(_ context.Context, _ string) ([]float32, error) {
	i := s.calls
	s.calls++
	if i < len(s.errs) && s.errs[i] != nil {
		return nil, s.errs[i]
	}
	if i < len(s.embs) {
		return s.embs[i], nil
	}
	return []float32{1, 0}, nil
}

func (s *sequenceEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return chunkTestText(text, maxTokens, overlap)
}

func TestCallDbLabelsWithError(t *testing.T) {
	// This is tricky - MemoryEngine doesn't error on AllNodes
	// Just verify normal behavior
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "label-test",
		Labels:     []string{"TestLabel", "SecondLabel"},
		Properties: map[string]interface{}{},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "CALL db.labels()", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2)
}

func TestResolveReturnItemWithCount(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "count-ri",
		Labels:     []string{"CountRI"},
		Properties: map[string]interface{}{},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// This triggers resolveReturnItem with COUNT prefix in non-aggregation path
	result, err := exec.Execute(ctx, "MATCH (n:CountRI) RETURN count(*)", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.Rows[0][0])
}

// Tests for toFloat64 type coverage
func TestToFloat64TypeCoverage(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test float32 through comparison
	node1 := &storage.Node{
		ID:         "f32-test",
		Labels:     []string{"Float32Test"},
		Properties: map[string]interface{}{"val": float32(3.14)},
	}
	_, err := store.CreateNode(node1)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:Float32Test) WHERE n.val > 3.0 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Test int through SUM aggregation
	node2 := &storage.Node{
		ID:         "int-test",
		Labels:     []string{"IntTest"},
		Properties: map[string]interface{}{"val": int(100)},
	}
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	result, err = exec.Execute(ctx, "MATCH (n:IntTest) RETURN sum(n.val)", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(100), result.Rows[0][0])

	// Test int32 through AVG
	node3 := &storage.Node{
		ID:         "i32-test",
		Labels:     []string{"Int32Test"},
		Properties: map[string]interface{}{"val": int32(50)},
	}
	_, err = store.CreateNode(node3)
	require.NoError(t, err)

	result, err = exec.Execute(ctx, "MATCH (n:Int32Test) RETURN avg(n.val)", nil)
	require.NoError(t, err)
	assert.Equal(t, float64(50), result.Rows[0][0])

	// Test string value - Neo4j ignores non-numeric values in SUM
	node4 := &storage.Node{
		ID:         "str-num",
		Labels:     []string{"StrNumTest"},
		Properties: map[string]interface{}{"val": "42.5"},
	}
	_, err = store.CreateNode(node4)
	require.NoError(t, err)

	result, err = exec.Execute(ctx, "MATCH (n:StrNumTest) RETURN sum(n.val)", nil)
	require.NoError(t, err)
	// String values are not numeric, so SUM ignores them and returns 0
	assert.Equal(t, int64(0), result.Rows[0][0])
}

// Test Parser MERGE clause
func TestParseMerge(t *testing.T) {
	parser := NewParser()
	query, err := parser.Parse("MERGE (n:Person {name: 'Alice'})")
	require.NoError(t, err)
	assert.NotNil(t, query)
	// MERGE is currently parsed but treated as CREATE internally
}

// Test all WHERE operators exercise evaluateWhere fully
func TestEvaluateWhereFullCoverage(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:     "where-full",
		Labels: []string{"WhereFull"},
		Properties: map[string]interface{}{
			"name":   "Alice Smith",
			"age":    float64(30),
			"active": true,
			"score":  float64(85.5),
		},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// Test >= operator
	result, err := exec.Execute(ctx, "MATCH (n:WhereFull) WHERE n.age >= 30 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Test <= operator
	result, err = exec.Execute(ctx, "MATCH (n:WhereFull) WHERE n.age <= 30 RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

// Test edge cases in splitNodePatterns
func TestSplitNodePatternsEdgeCases(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Empty pattern after splitting
	result, err := exec.Execute(ctx, "CREATE (a:A)", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
}

// Test evaluateStringOp edge cases
func TestEvaluateStringOpEdgeCases(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "str-edge",
		Labels:     []string{"StrEdge"},
		Properties: map[string]interface{}{"text": "hello world"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// CONTAINS that matches
	result, err := exec.Execute(ctx, "MATCH (n:StrEdge) WHERE n.text CONTAINS 'world' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// CONTAINS that doesn't match
	result, err = exec.Execute(ctx, "MATCH (n:StrEdge) WHERE n.text CONTAINS 'xyz' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)

	// STARTS WITH match
	result, err = exec.Execute(ctx, "MATCH (n:StrEdge) WHERE n.text STARTS WITH 'hello' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// ENDS WITH match
	result, err = exec.Execute(ctx, "MATCH (n:StrEdge) WHERE n.text ENDS WITH 'world' RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

// Test evaluateInOp edge cases
func TestEvaluateInOpMatch(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "in-match",
		Labels:     []string{"InMatch"},
		Properties: map[string]interface{}{"status": "pending"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	// IN with matching value
	result, err := exec.Execute(ctx, "MATCH (n:InMatch) WHERE n.status IN ['active', 'pending'] RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// IN with literal on the left and list property on the right
	node2 := &storage.Node{
		ID:     "in-list-prop",
		Labels: []string{"InMatch"},
		Properties: map[string]interface{}{
			"file_tags": []interface{}{"github", "argoCD"},
		},
	}
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	result, err = exec.Execute(ctx, "MATCH (n:InMatch) WHERE 'github' IN n.file_tags RETURN n", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestEvaluateInOpMatch_UsesFabricRecordBindingList(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Doc {id:'a', textKey128:'h1'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Doc {id:'b', textKey128:'h2'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Doc {id:'c', textKey128:'h3'})", nil)
	require.NoError(t, err)

	exec.fabricRecordBindings = map[string]interface{}{
		"keys": []interface{}{"h1", "h2"},
	}
	t.Cleanup(func() { exec.fabricRecordBindings = nil })

	res, err := exec.Execute(ctx, "MATCH (n:Doc) WHERE n.textKey128 IN keys RETURN n.textKey128 AS k ORDER BY k", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"k"}, res.Columns)
	require.Len(t, res.Rows, 2)
	assert.ElementsMatch(t, []interface{}{"h1", "h2"}, []interface{}{res.Rows[0][0], res.Rows[1][0]})
}

func TestExecuteUnwind_WithCollectDistinctProjection(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.WithValue(context.Background(), paramsKey, map[string]interface{}{
		"rows": []interface{}{
			map[string]interface{}{"textKey128": "h1"},
			map[string]interface{}{"textKey128": "h2"},
			map[string]interface{}{"textKey128": "h1"},
		},
	})

	res, err := exec.executeUnwind(ctx, `
UNWIND $rows AS r
WITH collect(DISTINCT r.textKey128) AS keys
RETURN keys`)
	require.NoError(t, err)
	require.Equal(t, []string{"keys"}, res.Columns)
	require.Len(t, res.Rows, 1)
	keys, ok := res.Rows[0][0].([]interface{})
	require.True(t, ok)
	require.ElementsMatch(t, []interface{}{"h1", "h2"}, keys)
}

// Test Parser default case in Parse
func TestParserDefaultCase(t *testing.T) {
	parser := NewParser()

	// Query with tokens that aren't recognized keywords
	query, err := parser.Parse("MATCH (n) RETURN n")
	require.NoError(t, err)
	assert.NotNil(t, query)
}

func TestExecuteInternal_Branches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Empty query guard.
	_, err := exec.executeInternal(ctx, "   ", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty query")

	// Non-transaction path delegates to executeWithoutTransaction.
	res, err := exec.executeInternal(ctx, "RETURN 1 AS x", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"x"}, res.Columns)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, int64(1), res.Rows[0][0])

	// Active transaction path delegates to executeInTransaction.
	_, err = exec.parseTransactionStatement("BEGIN")
	require.NoError(t, err)
	res, err = exec.executeInternal(ctx, "CREATE (:InternalBranch {id:'ib-1'})", nil)
	require.NoError(t, err)
	_, err = exec.parseTransactionStatement("COMMIT")
	require.NoError(t, err)

	verify, err := exec.Execute(ctx, "MATCH (n:InternalBranch {id:'ib-1'}) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	assert.Equal(t, int64(1), verify.Rows[0][0])
}

func TestApocDynamicRunAndRunMany_Direct(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:         "apoc-dyn-1",
		Labels:     []string{"Dyn"},
		Properties: map[string]interface{}{"name": "a"},
	})
	require.NoError(t, err)

	_, err = exec.callApocCypherRun(ctx, "CALL apoc.cypher.bogus('RETURN 1', {})")
	require.Error(t, err)
	_, err = exec.callApocCypherRun(ctx, "CALL apoc.cypher.run")
	require.Error(t, err)
	_, err = exec.callApocCypherRun(ctx, "CALL apoc.cypher.run('RETURN 1', {}")
	require.Error(t, err)

	res, err := exec.callApocCypherRun(ctx, "CALL apoc.cypher.run('MATCH (n:Dyn) RETURN count(n) AS c', {})")
	require.NoError(t, err)
	require.Equal(t, []string{"value"}, res.Columns)
	require.Len(t, res.Rows, 1)
	valueMap, ok := res.Rows[0][0].(map[string]interface{})
	require.True(t, ok)
	_, hasC := valueMap["c"]
	require.True(t, hasC)

	_, err = exec.callApocCypherRunMany(ctx, "CALL apoc.cypher.bogusMany('RETURN 1', {})")
	require.Error(t, err)
	_, err = exec.callApocCypherRunMany(ctx, "CALL apoc.cypher.runMany")
	require.Error(t, err)
	_, err = exec.callApocCypherRunMany(ctx, "CALL apoc.cypher.runMany('RETURN 1', {}")
	require.Error(t, err)

	res, err = exec.callApocCypherRunMany(ctx, "CALL apoc.cypher.runMany('RETURN 1 AS n; INVALID CYPHER; RETURN 2 AS n', {})")
	require.NoError(t, err)
	require.Equal(t, []string{"row", "result"}, res.Columns)
	require.NotEmpty(t, res.Rows)
	// Ensure row index and error payload branches are deterministic.
	require.Len(t, res.Rows, 3)
	require.Equal(t, int64(0), res.Rows[0][0])
	require.Equal(t, int64(1), res.Rows[1][0])
	require.Equal(t, int64(2), res.Rows[2][0])
	errMap, ok := res.Rows[1][1].(map[string]interface{})
	require.True(t, ok)
	_, hasErr := errMap["error"]
	require.True(t, hasErr)

	// Stats accumulation branch: runMany with CREATE should increment NodesCreated.
	createRes, err := exec.callApocCypherRunMany(ctx, "CALL apoc.cypher.runMany('CREATE (:Dyn {name:\"x\"}); CREATE (:Dyn {name:\"y\"})', {})")
	require.NoError(t, err)
	require.NotNil(t, createRes.Stats)
	assert.GreaterOrEqual(t, createRes.Stats.NodesCreated, 2)
}

func TestApocPeriodicIterateAndCommit_Direct(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.callApocPeriodicIterate(ctx, "CALL apoc.periodic.nope('RETURN 1','RETURN 1',{})")
	require.Error(t, err)
	_, err = exec.callApocPeriodicIterate(ctx, "CALL apoc.periodic.iterate")
	require.Error(t, err)
	_, err = exec.callApocPeriodicIterate(ctx, "CALL apoc.periodic.iterate('RETURN 1','RETURN 1',{}")
	require.Error(t, err)

	res, err := exec.callApocPeriodicIterate(ctx, "CALL apoc.periodic.iterate('UNWIND [1,2,3] AS i RETURN i','CREATE (:Iter {v: i})',{batchSize:2})")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(3), res.Rows[0][1]) // total

	res, err = exec.callApocPeriodicIterate(ctx, "CALL apoc.periodic.rock_n_roll('UNWIND [4,5] AS i RETURN i','CREATE (:Iter {v: i})',{batchSize:1})")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)

	_, err = exec.callApocPeriodicCommit(ctx, "CALL apoc.periodic.nope('MATCH (n) RETURN n', {})")
	require.Error(t, err)
	_, err = exec.callApocPeriodicCommit(ctx, "CALL apoc.periodic.commit")
	require.Error(t, err)
	_, err = exec.callApocPeriodicCommit(ctx, "CALL apoc.periodic.commit('MATCH (n) RETURN n', {}")
	require.Error(t, err)

	res, err = exec.callApocPeriodicCommit(ctx, "CALL apoc.periodic.commit('MATCH (n:Nothing) RETURN n', {limit: 10})")
	require.NoError(t, err)
	require.Equal(t, []string{"updates", "executions", "runtime", "batches"}, res.Columns)
	require.Len(t, res.Rows, 1)
}

func TestDbTxlogProcedures_ErrorBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.callDbTxlogEntries(ctx, "CALL db.txlog.bad(1,2)")
	require.Error(t, err)
	_, err = exec.callDbTxlogEntries(ctx, "CALL db.txlog.entries(1,2")
	require.Error(t, err)
	_, err = exec.callDbTxlogEntries(ctx, "CALL db.txlog.entries(abc,2)")
	require.Error(t, err)
	_, err = exec.callDbTxlogEntries(ctx, "CALL db.txlog.entries(0,2)")
	require.Error(t, err)
	_, err = exec.callDbTxlogEntries(ctx, "CALL db.txlog.entries(2,1)")
	require.Error(t, err)
	_, err = exec.callDbTxlogEntries(ctx, "CALL db.txlog.entries(1,2)")
	require.Error(t, err) // memory engine -> WAL not available

	_, err = exec.callDbTxlogByTxID(ctx, "CALL db.txlog.bad('x',10)")
	require.Error(t, err)
	_, err = exec.callDbTxlogByTxID(ctx, "CALL db.txlog.byTxId('x',10")
	require.Error(t, err)
	_, err = exec.callDbTxlogByTxID(ctx, "CALL db.txlog.byTxId('',10)")
	require.Error(t, err)
	_, err = exec.callDbTxlogByTxID(ctx, "CALL db.txlog.byTxId('tx-1',10)")
	require.Error(t, err) // memory engine -> WAL not available
}

func TestDbTxlogProcedures_WithWALStack(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cypher-txlog-test-*")
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
	ctx := context.Background()

	beginSeq, err := wal.AppendTxBegin("test", "tx-test-1", map[string]string{"app": "cypher-test"})
	require.NoError(t, err)
	_, err = wal.AppendTxCommit("test", "tx-test-1", 2)
	require.NoError(t, err)
	require.Greater(t, beginSeq, uint64(0))
	require.NoError(t, wal.Sync())

	_, err = exec.Execute(ctx, `CREATE (n:TxLog {name: "one"})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (n:TxLog {name: "two"})`, nil)
	require.NoError(t, err)

	entriesRes, err := exec.callDbTxlogEntries(ctx, "CALL db.txlog.entries(1, 0)")
	require.NoError(t, err)
	require.NotEmpty(t, entriesRes.Rows)
	var txID string
	for _, row := range entriesRes.Rows {
		if len(row) > 3 {
			if s, ok := row[3].(string); ok && s != "" {
				txID = s
				break
			}
		}
	}
	require.NotEmpty(t, txID)

	byTxIDRes, err := exec.callDbTxlogByTxID(ctx, "CALL db.txlog.byTxId('tx-test-1', 10)")
	require.NoError(t, err)
	require.NotEmpty(t, byTxIDRes.Rows)
	for _, row := range byTxIDRes.Rows {
		require.GreaterOrEqual(t, len(row), 4)
		assert.Equal(t, "tx-test-1", row[3])
	}
}

func TestExecuteWithImplicitTransaction_Branches(t *testing.T) {
	ctx := context.Background()

	t.Run("fallback when tx unsupported", func(t *testing.T) {
		base := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(base, "test")
		exec := NewStorageExecutor(&nonTransactionalEngine{Engine: store})

		result, err := exec.executeWithImplicitTransaction(ctx, "CREATE (:FallbackTx {id:'1'})", "CREATE (:FALLBACKTX {ID:'1'})")
		require.NoError(t, err)
		require.NotNil(t, result)

		readBack, err := exec.Execute(ctx, "MATCH (n:FallbackTx {id:'1'}) RETURN count(n)", nil)
		require.NoError(t, err)
		require.Equal(t, int64(1), readBack.Rows[0][0])
	})

	t.Run("begin transaction failure", func(t *testing.T) {
		base := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(base, "test")
		exec := NewStorageExecutor(&beginFailEngine{Engine: store})

		_, err := exec.executeWithImplicitTransaction(ctx, "CREATE (:BeginFail {id:'1'})", "CREATE (:BEGINFAIL {ID:'1'})")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to start implicit transaction")
	})

	t.Run("execution error rolls back", func(t *testing.T) {
		base := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(base, "test")
		exec := NewStorageExecutor(store)

		_, err := exec.executeWithImplicitTransaction(ctx, "CALL definitely.unknown.procedure()", "CALL DEFINITELY.UNKNOWN.PROCEDURE()")
		require.Error(t, err)

		readBack, readErr := exec.Execute(ctx, "MATCH (n:RollbackMe {id:'1'}) RETURN count(n)", nil)
		require.NoError(t, readErr)
		require.Equal(t, int64(0), readBack.Rows[0][0])
	})

	t.Run("commit failure wraps error", func(t *testing.T) {
		base := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(base, "test")
		exec := NewStorageExecutor(store)

		_, err := exec.Execute(ctx, "CREATE CONSTRAINT c_commit_unique IF NOT EXISTS FOR (n:CommitFail) REQUIRE n.id IS UNIQUE", nil)
		require.NoError(t, err)

		_, err = exec.executeWithImplicitTransaction(
			ctx,
			"CREATE (:CommitFail {id:'dup'}), (:CommitFail {id:'dup'})",
			"CREATE (:COMMITFAIL {ID:'DUP'}), (:COMMITFAIL {ID:'DUP'})",
		)
		require.Error(t, err)
		// Wire contract: see consumer-pinned-error-contract-plan.md §2.1.
		// Aligns with pkg/cypher/transaction.go:181 wrapper.
		require.Contains(t, err.Error(), "commit failed")
	})
}

func TestExecuteWithImplicitTransaction_WALBeginError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cypher-implicit-wal-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	badger, err := storage.NewBadgerEngine(tmpDir)
	require.NoError(t, err)
	defer badger.Close()

	wal, err := storage.NewWAL(tmpDir+"/wal", nil)
	require.NoError(t, err)

	walEngine := storage.NewWALEngine(badger, wal)
	store := storage.NewNamespacedEngine(walEngine, "test")
	exec := NewStorageExecutor(store)

	require.NoError(t, wal.Close())

	_, err = exec.executeWithImplicitTransaction(
		context.Background(),
		"CREATE (:WalBegin {id:'1'})",
		"CREATE (:WALBEGIN {ID:'1'})",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to write WAL tx begin")
}

func TestExecuteReturn_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// RETURN missing branch.
	_, err := exec.executeReturn(ctx, "MATCH (n)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RETURN clause not found")

	// Parameter substitution + aliases + null + int/float/bool/string fallback.
	ctxWithParams := context.WithValue(ctx, paramsKey, map[string]interface{}{"x": int64(7)})
	res, err := exec.executeReturn(ctxWithParams, "RETURN $x AS x, null AS n, 1 AS one, 0 AS zero, 3.14 AS pi, 's' AS s, bareword AS b")
	require.NoError(t, err)
	require.Equal(t, []string{"x", "n", "one", "zero", "pi", "s", "b"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 7)
	assert.Equal(t, int64(7), res.Rows[0][0])
	assert.Nil(t, res.Rows[0][1])
	assert.Equal(t, int64(1), res.Rows[0][2])
	assert.Equal(t, int64(0), res.Rows[0][3])
	assert.Equal(t, 3.14, res.Rows[0][4])
	assert.Equal(t, "s", res.Rows[0][5])
	assert.Equal(t, "bareword", res.Rows[0][6])

	// Expression evaluation branch (evaluateExpressionWithContext result != nil).
	exprRes, err := exec.executeReturn(ctx, "RETURN toUpper('abc') AS up")
	require.NoError(t, err)
	require.Len(t, exprRes.Rows, 1)
	require.Len(t, exprRes.Rows[0], 1)
	assert.Equal(t, "ABC", exprRes.Rows[0][0])
}

func TestExecuteQueryAgainstStorage_DispatchBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	expectNoError := []string{
		"FOREACH (x IN [1] | CREATE (:Q {v:x}))",
		"CREATE RANGE INDEX idx_tx_dispatch FOR (n:Dispatch) ON (n.age)",
		"SHOW INDEXES",
		"SHOW FULLTEXT INDEXES",
		"SHOW CONSTRAINTS",
		"SHOW PROCEDURES",
		"SHOW FUNCTIONS",
		"SHOW DATABASE nornic",
		"SHOW DATABASE",
		"DROP INDEX idx_missing IF EXISTS",
		"DROP CONSTRAINT c_missing IF EXISTS",
	}

	for _, q := range expectNoError {
		_, err := exec.executeQueryAgainstStorage(ctx, q, strings.ToUpper(q))
		require.NoError(t, err, "query should succeed: %s", q)
	}

	expectError := []struct {
		query       string
		errContains string
	}{
		{query: "SHOW DATABASES", errContains: "SHOW DATABASES requires multi-database support"},
		{query: "SHOW ALIASES", errContains: "SHOW ALIASES requires multi-database support"},
		{query: "SHOW LIMITS FOR DATABASE nornic", errContains: "SHOW LIMITS requires multi-database support"},
		{query: "SHOW WHATEVER", errContains: "unsupported SHOW command in transaction"},
		{query: "LOAD CSV FROM 'file:///tmp/missing.csv' AS row RETURN row", errContains: "unsupported query type"},
		{query: "ALTER COMPOSITE DATABASE cdb ADD CONSTITUENT db1", errContains: "unsupported query type"},
		{query: "WHATEVER 1", errContains: "unsupported query type"},
	}

	for _, tc := range expectError {
		_, err := exec.executeQueryAgainstStorage(ctx, tc.query, strings.ToUpper(tc.query))
		require.Error(t, err, "query should fail: %s", tc.query)
		assert.Contains(t, err.Error(), tc.errContains, "query should fail with expected reason: %s", tc.query)
	}
}

// =============================================================================
// Tests for Parameter Substitution (substituteParams and valueToLiteral)
// =============================================================================

func TestSubstituteParamsBasic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		name     string
		query    string
		params   map[string]interface{}
		expected string
	}{
		{
			name:     "string parameter",
			query:    "MATCH (n {name: $name}) RETURN n",
			params:   map[string]interface{}{"name": "Alice"},
			expected: "MATCH (n {name: 'Alice'}) RETURN n",
		},
		{
			name:     "integer parameter",
			query:    "MATCH (n) WHERE n.age = $age RETURN n",
			params:   map[string]interface{}{"age": 25},
			expected: "MATCH (n) WHERE n.age = 25 RETURN n",
		},
		{
			name:     "float parameter",
			query:    "MATCH (n) WHERE n.score > $score RETURN n",
			params:   map[string]interface{}{"score": 85.5},
			expected: "MATCH (n) WHERE n.score > 85.5 RETURN n",
		},
		{
			name:     "boolean parameter true",
			query:    "MATCH (n) WHERE n.active = $active RETURN n",
			params:   map[string]interface{}{"active": true},
			expected: "MATCH (n) WHERE n.active = true RETURN n",
		},
		{
			name:     "boolean parameter false",
			query:    "MATCH (n) WHERE n.active = $active RETURN n",
			params:   map[string]interface{}{"active": false},
			expected: "MATCH (n) WHERE n.active = false RETURN n",
		},
		{
			name:     "null parameter",
			query:    "MATCH (n) WHERE n.value = $value RETURN n",
			params:   map[string]interface{}{"value": nil},
			expected: "MATCH (n) WHERE n.value = null RETURN n",
		},
		{
			name:     "multiple parameters",
			query:    "MATCH (n {name: $name, age: $age}) RETURN n",
			params:   map[string]interface{}{"name": "Bob", "age": 30},
			expected: "MATCH (n {name: 'Bob', age: 30}) RETURN n",
		},
		{
			name:     "missing parameter unchanged",
			query:    "MATCH (n {name: $name}) RETURN n",
			params:   map[string]interface{}{},
			expected: "MATCH (n {name: $name}) RETURN n",
		},
		{
			name:     "empty params",
			query:    "MATCH (n) RETURN n",
			params:   nil,
			expected: "MATCH (n) RETURN n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.substituteParams(tt.query, tt.params)
			assert.Equal(t, tt.expected, result)
		})
	}

	// Verify queries execute correctly after substitution
	_, err := exec.Execute(ctx, "CREATE (n:ParamTest {name: 'Test'})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:ParamTest {name: $name}) RETURN n", map[string]interface{}{"name": "Test"})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestSubstituteParamsStringEscaping(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		value    string
		expected string
	}{
		{
			name:     "single quote escaping",
			value:    "O'Connor",
			expected: "'O''Connor'",
		},
		{
			name:     "backslash escaping",
			value:    "path\\to\\file",
			expected: "'path\\\\to\\\\file'",
		},
		{
			name:     "both quotes and backslashes",
			value:    "It's a\\path",
			expected: "'It''s a\\\\path'",
		},
		{
			name:     "empty string",
			value:    "",
			expected: "''",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.valueToLiteral(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValueToLiteralArrays(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{
			name:     "string array",
			value:    []string{"a", "b", "c"},
			expected: "['a', 'b', 'c']",
		},
		{
			name:     "int array",
			value:    []int{1, 2, 3},
			expected: "[1, 2, 3]",
		},
		{
			name:     "int64 array",
			value:    []int64{100, 200, 300},
			expected: "[100, 200, 300]",
		},
		{
			name:     "float64 array",
			value:    []float64{1.5, 2.5, 3.5},
			expected: "[1.5, 2.5, 3.5]",
		},
		{
			name:     "interface array",
			value:    []interface{}{"hello", 42, true},
			expected: "['hello', 42, true]",
		},
		{
			name:     "empty array",
			value:    []interface{}{},
			expected: "[]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.valueToLiteral(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValueToLiteralMaps(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	// Map with single key (deterministic)
	result := exec.valueToLiteral(map[string]interface{}{"name": "Alice"})
	assert.Equal(t, "{name: 'Alice'}", result)

	// Empty map
	result = exec.valueToLiteral(map[string]interface{}{})
	assert.Equal(t, "{}", result)
}

func TestValueToLiteralIntegerTypes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"int", int(42), "42"},
		{"int8", int8(8), "8"},
		{"int16", int16(16), "16"},
		{"int32", int32(32), "32"},
		{"int64", int64(64), "64"},
		{"uint", uint(100), "100"},
		{"uint8", uint8(8), "8"},
		{"uint16", uint16(16), "16"},
		{"uint32", uint32(32), "32"},
		{"uint64", uint64(64), "64"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.valueToLiteral(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValueToLiteralFloats(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	// float32
	result := exec.valueToLiteral(float32(3.14))
	assert.Contains(t, result, "3.14")

	// float64
	result = exec.valueToLiteral(float64(2.718281828))
	assert.Contains(t, result, "2.718")
}

// =============================================================================
// Tests for RETURN Clause Parsing
// =============================================================================

func TestParseReturnClauseBasic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	node := &storage.Node{
		ID:         "ret-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": float64(30)},
	}

	tests := []struct {
		name            string
		returnClause    string
		varName         string
		expectedCols    []string
		expectedValFunc func([]interface{}) bool
	}{
		{
			name:         "return star",
			returnClause: "*",
			varName:      "n",
			expectedCols: []string{"n"},
			expectedValFunc: func(vals []interface{}) bool {
				return len(vals) == 1 && vals[0] != nil
			},
		},
		{
			name:         "return property with alias",
			returnClause: "n.name AS personName",
			varName:      "n",
			expectedCols: []string{"personName"},
			expectedValFunc: func(vals []interface{}) bool {
				return len(vals) == 1 && vals[0] == "Alice"
			},
		},
		{
			name:         "return property without alias",
			returnClause: "n.age",
			varName:      "n",
			expectedCols: []string{"age"},
			expectedValFunc: func(vals []interface{}) bool {
				return len(vals) == 1 && vals[0] == float64(30)
			},
		},
		{
			name:         "return id function",
			returnClause: "id(n) AS node_id",
			varName:      "n",
			expectedCols: []string{"node_id"},
			expectedValFunc: func(vals []interface{}) bool {
				return len(vals) == 1 && vals[0] == "ret-1"
			},
		},
		{
			name:         "return multiple expressions",
			returnClause: "n.name AS name, n.age AS age, id(n) AS id",
			varName:      "n",
			expectedCols: []string{"name", "age", "id"},
			expectedValFunc: func(vals []interface{}) bool {
				return len(vals) == 3 && vals[0] == "Alice" && vals[1] == float64(30) && vals[2] == "ret-1"
			},
		},
		{
			name:         "return variable only",
			returnClause: "n",
			varName:      "n",
			expectedCols: []string{"n"},
			expectedValFunc: func(vals []interface{}) bool {
				return len(vals) == 1 && vals[0] != nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			cols, vals := exec.parseReturnClause(ctx, tt.returnClause, tt.varName, node)
			assert.Equal(t, tt.expectedCols, cols)
			assert.True(t, tt.expectedValFunc(vals), "Value validation failed for %v", vals)
		})
	}
}

func TestSplitReturnExpressions(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		clause   string
		expected []string
	}{
		{
			name:     "single expression",
			clause:   "n.name",
			expected: []string{"n.name"},
		},
		{
			name:     "multiple simple expressions",
			clause:   "n.name, n.age, n.city",
			expected: []string{"n.name", " n.age", " n.city"},
		},
		{
			name:     "expression with function",
			clause:   "id(n), n.name",
			expected: []string{"id(n)", " n.name"},
		},
		{
			name:     "nested parentheses",
			clause:   "count(n), sum(n.age)",
			expected: []string{"count(n)", " sum(n.age)"},
		},
		{
			name:     "complex function call",
			clause:   "collect(n.name), count(*)",
			expected: []string{"collect(n.name)", " count(*)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.splitReturnExpressions(tt.clause)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExpressionToAlias(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		expr     string
		expected string
	}{
		{"property access", "n.name", "name"},
		{"nested property", "n.address.city", "city"},
		{"function call", "id(n)", "id(n)"},
		{"simple variable", "n", "n"},
		{"literal", "'hello'", "'hello'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.expressionToAlias(tt.expr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluateExpression(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	node := &storage.Node{
		ID:     "eval-1",
		Labels: []string{"Test"},
		Properties: map[string]interface{}{
			"name":   "Test Node",
			"count":  float64(42),
			"active": true,
		},
	}

	tests := []struct {
		name     string
		expr     string
		varName  string
		expected interface{}
	}{
		{"id function", "id(n)", "n", "eval-1"},
		{"id function with spaces", "id( n )", "n", "eval-1"},
		{"property access", "n.name", "n", "Test Node"},
		{"numeric property", "n.count", "n", float64(42)},
		{"boolean property", "n.active", "n", true},
		{"missing property", "n.missing", "n", nil},
		{"string literal", "'hello'", "n", "hello"},
		{"integer literal", "42", "n", int64(42)},
		{"float literal", "3.14", "n", float64(3.14)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			result := exec.evaluateExpression(ctx, tt.expr, tt.varName, node)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// Tests for MERGE with ON CREATE SET / ON MATCH SET
// =============================================================================

func TestMergeWithOnCreateSet(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// MERGE a new node with ON CREATE SET
	result, err := exec.Execute(ctx, `
		MERGE (n:Person {name: 'Alice'})
		ON CREATE SET n.created = 'yes', n.age = 25
		RETURN n
	`, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Len(t, result.Rows, 1)

	// Verify node was created with properties
	matchResult, err := exec.Execute(ctx, "MATCH (n:Person {name: 'Alice'}) RETURN n.created, n.age", nil)
	require.NoError(t, err)
	assert.Len(t, matchResult.Rows, 1)
}

func TestMergeWithOnMatchSet(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// First create a node
	_, err := exec.Execute(ctx, "CREATE (n:Person {name: 'Bob', visits: 0})", nil)
	require.NoError(t, err)

	// MERGE existing node with ON MATCH SET
	result, err := exec.Execute(ctx, `
		MERGE (n:Person {name: 'Bob'})
		ON MATCH SET n.visits = 1
		RETURN n
	`, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Stats.NodesCreated) // Should not create new node
}

func TestMergeRouting(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// MERGE with ON CREATE SET should NOT be routed to executeSet
	result, err := exec.Execute(ctx, `
		MERGE (n:File {path: '/test/file.txt'})
		ON CREATE SET n.created = 'true'
		RETURN n.path AS path
	`, nil)
	require.NoError(t, err)
	assert.Len(t, result.Columns, 1)
	assert.Equal(t, "path", result.Columns[0])
}

func TestMergeWithParameterSubstitution(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	params := map[string]interface{}{
		"path":      "/app/docs/README.md",
		"name":      "README.md",
		"size":      int64(1024),
		"extension": ".md",
	}

	result, err := exec.Execute(ctx, `
		MERGE (f:File {path: $path})
		ON CREATE SET f.name = $name, f.size = $size, f.extension = $extension
		RETURN f.path AS path, f.name AS name
	`, params)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Stats.NodesCreated)
	assert.Len(t, result.Columns, 2)
	assert.Contains(t, result.Columns, "path")
	assert.Contains(t, result.Columns, "name")
}

func TestMergeReturnIdFunction(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
		MERGE (f:File {path: '/test.txt'})
		RETURN f.path AS path, id(f) AS node_id
	`, nil)
	require.NoError(t, err)
	assert.Len(t, result.Columns, 2)
	assert.Equal(t, "path", result.Columns[0])
	assert.Equal(t, "node_id", result.Columns[1])

	// node_id should be a string
	if len(result.Rows) > 0 {
		assert.IsType(t, "", result.Rows[0][1])
	}
}

// =============================================================================
// Tests for Extract Helper Functions
// =============================================================================

func TestExtractVarName(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		pattern  string
		expected string
	}{
		{"simple var with label", "(n:Person)", "n"},
		{"var with multiple labels", "(f:File:Node)", "f"},
		{"var with properties", "(n:Person {name: 'Alice'})", "n"},
		{"var only", "(n)", "n"},
		{"no var, label only", "(:Person)", "n"}, // Default
		{"empty pattern", "()", "n"},             // Default
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.extractVarName(tt.pattern)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractLabels(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name     string
		pattern  string
		expected []string
	}{
		{"single label", "(n:Person)", []string{"Person"}},
		{"multiple labels", "(f:File:Node)", []string{"File", "Node"}},
		{"label with properties", "(n:Person {name: 'Alice'})", []string{"Person"}},
		{"no label", "(n)", []string{}},
		{"no var, label only", "(:Person)", []string{"Person"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.extractLabels(tt.pattern)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// Tests for DROP INDEX (No-op)
// =============================================================================

func TestDropIndexNoOp(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// DROP INDEX should be treated as no-op (returns empty result, no error)
	result, err := exec.Execute(ctx, "DROP INDEX file_path IF EXISTS", nil)
	require.NoError(t, err)
	assert.Empty(t, result.Columns)
	assert.Empty(t, result.Rows)
}

// =============================================================================
// Tests for Edge Cases in Parameter Substitution
// =============================================================================

func TestSubstituteParamsEdgeCases(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	tests := []struct {
		name   string
		query  string
		params map[string]interface{}
	}{
		{
			name:   "parameter at start",
			query:  "$name is the name",
			params: map[string]interface{}{"name": "test"},
		},
		{
			name:   "parameter at end",
			query:  "The name is $name",
			params: map[string]interface{}{"name": "test"},
		},
		{
			name:   "adjacent parameters",
			query:  "Values: $a$b",
			params: map[string]interface{}{"a": "x", "b": "y"},
		},
		{
			name:   "underscore in param name",
			query:  "Path: $host_path",
			params: map[string]interface{}{"host_path": "/app/docs"},
		},
		{
			name:   "number in param name",
			query:  "Value: $param123",
			params: map[string]interface{}{"param123": "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.substituteParams(tt.query, tt.params)
			// Should not contain the original parameter placeholders
			for key := range tt.params {
				assert.NotContains(t, result, "$"+key)
			}
		})
	}
}

func TestSubstituteParamsComplexQuery(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Test with a complex MERGE query pattern
	query := `
		MERGE (f:File:Node {path: $path})
		ON CREATE SET f.id = 'file-123',
			f.host_path = $host_path,
			f.name = $name,
			f.extension = $extension,
			f.size_bytes = $size_bytes,
			f.content = $content
		RETURN f.path AS path, f.size_bytes AS size_bytes, id(f) AS node_id
	`

	params := map[string]interface{}{
		"path":       "/app/docs/README.md",
		"host_path":  "/Users/dev/docs/README.md",
		"name":       "README.md",
		"extension":  ".md",
		"size_bytes": int64(2048),
		"content":    "# Hello World\n\nThis is a test file.",
	}

	result, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	assert.Len(t, result.Columns, 3)
	assert.Contains(t, result.Columns, "path")
	assert.Contains(t, result.Columns, "size_bytes")
	assert.Contains(t, result.Columns, "node_id")

	if len(result.Rows) > 0 {
		assert.Equal(t, "/app/docs/README.md", result.Rows[0][0])
	}
}

// TestRelationshipCountAggregation tests that COUNT(r) properly aggregates
// all relationships instead of returning 1 (the bug that was fixed)
func TestRelationshipCountAggregation(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a small graph with multiple relationships
	// 3 products, 2 categories, 2 suppliers
	// Each product has 2 relationships (PART_OF category, SUPPLIES from supplier)
	// Total: 6 relationships

	// Create categories
	cat1 := &storage.Node{ID: "cat1", Labels: []string{"Category"}, Properties: map[string]interface{}{"categoryID": int64(1), "name": "Beverages"}}
	cat2 := &storage.Node{ID: "cat2", Labels: []string{"Category"}, Properties: map[string]interface{}{"categoryID": int64(2), "name": "Condiments"}}
	_, err := store.CreateNode(cat1)
	require.NoError(t, err)
	_, err = store.CreateNode(cat2)
	require.NoError(t, err)

	// Create suppliers
	sup1 := &storage.Node{ID: "sup1", Labels: []string{"Supplier"}, Properties: map[string]interface{}{"supplierID": int64(1), "name": "Exotic Liquids"}}
	sup2 := &storage.Node{ID: "sup2", Labels: []string{"Supplier"}, Properties: map[string]interface{}{"supplierID": int64(2), "name": "New Orleans"}}
	_, err = store.CreateNode(sup1)
	require.NoError(t, err)
	_, err = store.CreateNode(sup2)
	require.NoError(t, err)

	// Create products
	prod1 := &storage.Node{ID: "prod1", Labels: []string{"Product"}, Properties: map[string]interface{}{"productID": int64(1), "name": "Chai"}}
	prod2 := &storage.Node{ID: "prod2", Labels: []string{"Product"}, Properties: map[string]interface{}{"productID": int64(2), "name": "Chang"}}
	prod3 := &storage.Node{ID: "prod3", Labels: []string{"Product"}, Properties: map[string]interface{}{"productID": int64(3), "name": "Aniseed Syrup"}}
	_, err = store.CreateNode(prod1)
	require.NoError(t, err)
	_, err = store.CreateNode(prod2)
	require.NoError(t, err)
	_, err = store.CreateNode(prod3)
	require.NoError(t, err)

	// Create relationships
	// Product 1: Beverages category, Supplier 1
	edge1 := &storage.Edge{ID: "e1", StartNode: "prod1", EndNode: "cat1", Type: "PART_OF"}
	edge2 := &storage.Edge{ID: "e2", StartNode: "sup1", EndNode: "prod1", Type: "SUPPLIES"}
	require.NoError(t, store.CreateEdge(edge1))
	require.NoError(t, store.CreateEdge(edge2))

	// Product 2: Beverages category, Supplier 1
	edge3 := &storage.Edge{ID: "e3", StartNode: "prod2", EndNode: "cat1", Type: "PART_OF"}
	edge4 := &storage.Edge{ID: "e4", StartNode: "sup1", EndNode: "prod2", Type: "SUPPLIES"}
	require.NoError(t, store.CreateEdge(edge3))
	require.NoError(t, store.CreateEdge(edge4))

	// Product 3: Condiments category, Supplier 2
	edge5 := &storage.Edge{ID: "e5", StartNode: "prod3", EndNode: "cat2", Type: "PART_OF"}
	edge6 := &storage.Edge{ID: "e6", StartNode: "sup2", EndNode: "prod3", Type: "SUPPLIES"}
	require.NoError(t, store.CreateEdge(edge5))
	require.NoError(t, store.CreateEdge(edge6))

	// Test 1: Count all relationships
	t.Run("count all relationships", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r]->() RETURN count(r) as count", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1, "Should return single aggregated row")
		require.Len(t, result.Rows[0], 1, "Should return single column")

		count := result.Rows[0][0]
		assert.Equal(t, int64(6), count, "Should count all 6 relationships, not return 1")
	})

	// Test 2: Count relationships by type
	t.Run("count PART_OF relationships", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r:PART_OF]->() RETURN count(r) as count", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		count := result.Rows[0][0]
		assert.Equal(t, int64(3), count, "Should count 3 PART_OF relationships")
	})

	t.Run("count SUPPLIES relationships", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r:SUPPLIES]->() RETURN count(r) as count", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		count := result.Rows[0][0]
		assert.Equal(t, int64(3), count, "Should count 3 SUPPLIES relationships")
	})

	// Test 3: Count with wildcard (COUNT(*))
	t.Run("count with wildcard", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r]->() RETURN count(*) as count", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		count := result.Rows[0][0]
		assert.Equal(t, int64(6), count, "COUNT(*) should count all 6 relationships")
	})

	// Test 4: Verify non-aggregation still works (should return all rows)
	t.Run("non-aggregation returns all relationships", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r]->() RETURN type(r) as relType", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Len(t, result.Rows, 6, "Non-aggregation should return all 6 relationship rows")
	})

	// Test 5: Count with GROUP BY (implicit grouping by type)
	t.Run("count grouped by type", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r]->() RETURN type(r) as relType, count(*) as count", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		// Should group by type: PART_OF (3) and SUPPLIES (3) = 2 groups
		assert.Len(t, result.Rows, 2, "Should return 2 groups (PART_OF and SUPPLIES)")

		// Verify counts
		for _, row := range result.Rows {
			relType := row[0].(string)
			count := row[1].(int64)
			assert.Equal(t, int64(3), count, "Each type should have count of 3")
			assert.Contains(t, []string{"PART_OF", "SUPPLIES"}, relType)
		}
	})

	// Test 6: Empty result aggregation
	t.Run("count with no matches", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH ()-[r:NONEXISTENT]->() RETURN count(r) as count", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)

		count := result.Rows[0][0]
		assert.Equal(t, int64(0), count, "COUNT should return 0 for no matches")
	})
}

// ========================================
// SET Label Tests - SET n:Label syntax
// ========================================

func TestSetLabelSyntax(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node without the target label
	node := &storage.Node{
		ID:         "label-test-1",
		Labels:     []string{"File"},
		Properties: map[string]interface{}{"path": "/test/file.txt"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	t.Run("SET single label", func(t *testing.T) {
		// Add Node label using SET n:Label syntax
		result, err := exec.Execute(ctx, `
			MATCH (f:File {path: '/test/file.txt'})
			SET f:Node
			RETURN f
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, result.Stats.LabelsAdded)

		// Verify the node now has both labels
		updatedNode, err := store.GetNode("label-test-1")
		require.NoError(t, err)
		assert.Contains(t, updatedNode.Labels, "File")
		assert.Contains(t, updatedNode.Labels, "Node")
	})

	t.Run("SET label idempotent", func(t *testing.T) {
		// Adding same label again should not increase count
		result, err := exec.Execute(ctx, `
			MATCH (f:File {path: '/test/file.txt'})
			SET f:Node
			RETURN f
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 0, result.Stats.LabelsAdded, "Should not add duplicate label")
	})
}

func TestSetLabelWithPropertyAssignment(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node
	node := &storage.Node{
		ID:         "combo-test-1",
		Labels:     []string{"Document"},
		Properties: map[string]interface{}{"name": "readme"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	t.Run("SET label and property together", func(t *testing.T) {
		// SET f:Node, f.type = 'file' - combining label and property
		result, err := exec.Execute(ctx, `
			MATCH (d:Document {name: 'readme'})
			SET d:Indexed, d.type = 'file'
			RETURN d
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, result.Stats.LabelsAdded)
		assert.Equal(t, 1, result.Stats.PropertiesSet)

		// Verify both label and property were set
		updatedNode, err := store.GetNode("combo-test-1")
		require.NoError(t, err)
		assert.Contains(t, updatedNode.Labels, "Document")
		assert.Contains(t, updatedNode.Labels, "Indexed")
		assert.Equal(t, "file", updatedNode.Properties["type"])
	})
}

func TestSetMultipleLabels(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node
	node := &storage.Node{
		ID:         "multi-label-1",
		Labels:     []string{"Base"},
		Properties: map[string]interface{}{},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, err)

	t.Run("SET multiple labels in sequence", func(t *testing.T) {
		// First add one label
		_, err := exec.Execute(ctx, `MATCH (n:Base) SET n:First RETURN n`, nil)
		require.NoError(t, err)

		// Then add another
		result, err := exec.Execute(ctx, `MATCH (n:Base) SET n:Second RETURN n`, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, result.Stats.LabelsAdded)

		// Verify all labels present
		updatedNode, err := store.GetNode("multi-label-1")
		require.NoError(t, err)
		assert.Contains(t, updatedNode.Labels, "Base")
		assert.Contains(t, updatedNode.Labels, "First")
		assert.Contains(t, updatedNode.Labels, "Second")
	})
}

// ========================================
// WHERE Label Check Tests - WHERE n:Label and WHERE NOT n:Label
// ========================================

func TestWhereLabelCheck(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test nodes with different labels
	nodes := []*storage.Node{
		{ID: "person-1", Labels: []string{"Person", "Employee"}, Properties: map[string]interface{}{"name": "Alice"}},
		{ID: "person-2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}},
		{ID: "company-1", Labels: []string{"Company"}, Properties: map[string]interface{}{"name": "Acme"}},
	}
	for _, n := range nodes {
		_, err := store.CreateNode(n)
		require.NoError(t, err)
	}

	t.Run("WHERE n:Label filters correctly", func(t *testing.T) {
		// Match only Person nodes that are also Employees
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)
			WHERE p:Employee
			RETURN p.name
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "Alice", result.Rows[0][0])
	})

	t.Run("WHERE NOT n:Label excludes correctly", func(t *testing.T) {
		// Match Person nodes that are NOT Employees
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)
			WHERE NOT p:Employee
			RETURN p.name
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "Bob", result.Rows[0][0])
	})
}

func TestWhereNotLabelMigrationPattern(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// This tests the exact pattern from schema initialization:
	// MATCH (f:File) WHERE NOT f:Node SET f:Node, f.type = 'file'

	// Create File nodes - some with Node label, some without
	nodes := []*storage.Node{
		{ID: "file-1", Labels: []string{"File"}, Properties: map[string]interface{}{"path": "/a.txt"}},
		{ID: "file-2", Labels: []string{"File", "Node"}, Properties: map[string]interface{}{"path": "/b.txt"}},
		{ID: "file-3", Labels: []string{"File"}, Properties: map[string]interface{}{"path": "/c.txt"}},
	}
	for _, n := range nodes {
		_, err := store.CreateNode(n)
		require.NoError(t, err)
	}

	t.Run("migration adds label only to nodes missing it", func(t *testing.T) {
		// Run the migration pattern
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WHERE NOT f:Node
			SET f:Node, f.type = 'file'
		`, nil)
		require.NoError(t, err)
		// Should add Node label to file-1 and file-3, not file-2
		assert.Equal(t, 2, result.Stats.LabelsAdded)
		assert.Equal(t, 2, result.Stats.PropertiesSet)

		// Verify file-1 now has Node label
		file1, err := store.GetNode("file-1")
		require.NoError(t, err)
		assert.Contains(t, file1.Labels, "Node")
		assert.Equal(t, "file", file1.Properties["type"])

		// Verify file-2 unchanged (already had Node label)
		file2, err := store.GetNode("file-2")
		require.NoError(t, err)
		assert.Contains(t, file2.Labels, "Node")
		assert.Nil(t, file2.Properties["type"]) // type not set because it wasn't matched

		// Verify file-3 now has Node label
		file3, err := store.GetNode("file-3")
		require.NoError(t, err)
		assert.Contains(t, file3.Labels, "Node")
		assert.Equal(t, "file", file3.Properties["type"])
	})

	t.Run("running migration again has no effect", func(t *testing.T) {
		// Second run should do nothing since all File nodes now have Node label
		result, err := exec.Execute(ctx, `
			MATCH (f:File)
			WHERE NOT f:Node
			SET f:Node, f.type = 'file'
		`, nil)
		require.NoError(t, err)
		assert.Equal(t, 0, result.Stats.LabelsAdded)
		assert.Equal(t, 0, result.Stats.PropertiesSet)
	})
}

func TestWhereLabelWithAndCondition(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test nodes
	nodes := []*storage.Node{
		{ID: "emp-1", Labels: []string{"Person", "Employee"}, Properties: map[string]interface{}{"name": "Alice", "age": int64(30)}},
		{ID: "emp-2", Labels: []string{"Person", "Employee"}, Properties: map[string]interface{}{"name": "Bob", "age": int64(25)}},
		{ID: "cust-1", Labels: []string{"Person", "Customer"}, Properties: map[string]interface{}{"name": "Charlie", "age": int64(35)}},
	}
	for _, n := range nodes {
		_, err := store.CreateNode(n)
		require.NoError(t, err)
	}

	t.Run("WHERE label AND property condition", func(t *testing.T) {
		// Match Employees over 28
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)
			WHERE p:Employee AND p.age > 28
			RETURN p.name
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "Alice", result.Rows[0][0])
	})

	t.Run("WHERE NOT label AND property condition", func(t *testing.T) {
		// Match non-Employees over 30
		result, err := exec.Execute(ctx, `
			MATCH (p:Person)
			WHERE NOT p:Employee AND p.age > 30
			RETURN p.name
		`, nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "Charlie", result.Rows[0][0])
	})
}

// TestUseCommand tests :USE command handling (Neo4j browser/shell compatibility)
func TestUseCommand(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run(":USE command with CREATE statements", func(t *testing.T) {
		// Test :USE command followed by multiple CREATE statements
		// This mimics Neo4j browser behavior where :USE switches database context
		query := `:USE test_db_a
CREATE (alice:Person {name: "Alice", id: "a1", db: "test_db_a"})
CREATE (bob:Person {name: "Bob", id: "a2", db: "test_db_a"})
CREATE (company:Company {name: "Acme Corp", id: "a3", db: "test_db_a"})
CREATE (alice)-[:WORKS_FOR]->(company)
CREATE (bob)-[:WORKS_FOR]->(company)
RETURN alice, bob, company`

		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, ":USE command should be stripped and query should execute")
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1, "should return one row with alice, bob, company")

		// Verify nodes were created
		countResult, err := exec.Execute(ctx, "MATCH (n) RETURN count(n) as count", nil)
		require.NoError(t, err)
		require.Len(t, countResult.Rows, 1)
		assert.Equal(t, int64(3), countResult.Rows[0][0], "should have 3 nodes (alice, bob, company)")

		// Verify relationships were created
		relResult, err := exec.Execute(ctx, "MATCH ()-[r:WORKS_FOR]->() RETURN count(r) as count", nil)
		require.NoError(t, err)
		require.Len(t, relResult.Rows, 1)
		assert.Equal(t, int64(2), relResult.Rows[0][0], "should have 2 WORKS_FOR relationships")
	})

	t.Run(":USE command alone returns success", func(t *testing.T) {
		// :USE without any query should return success (database switching handled at API layer)
		result, err := exec.Execute(ctx, ":USE test_db", nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, []string{"database"}, result.Columns)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "switched", result.Rows[0][0])
	})

	t.Run(":USE with lowercase", func(t *testing.T) {
		// Test :use (lowercase) is also recognized
		result, err := exec.Execute(ctx, `:use test_db
CREATE (n:Test {name: "test"})
RETURN n.name`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "test", result.Rows[0][0])
	})

	t.Run(":USE with whitespace", func(t *testing.T) {
		// Test :USE with extra whitespace
		result, err := exec.Execute(ctx, `:USE  test_db  
CREATE (n:Test {name: "test2"})
RETURN n.name`, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "test2", result.Rows[0][0])
	})
}

// TestPropertyAccessInMatch verifies that property access works correctly in MATCH queries
func TestPropertyAccessInMatch(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// First, test parseReturnItems directly
	returnClause := "n.order_id as order_id, n.amount as amount"
	returnItems := exec.parseReturnItems(returnClause)
	t.Logf("parseReturnItems returned %d items for %q", len(returnItems), returnClause)
	for i, item := range returnItems {
		t.Logf("  Item %d: expr=%q, alias=%q", i, item.expr, item.alias)
	}
	require.Len(t, returnItems, 2, "should parse 2 return items")
	require.Equal(t, "n.order_id", returnItems[0].expr, "first item expression should be n.order_id")
	require.Equal(t, "order_id", returnItems[0].alias, "first item alias should be order_id")
	require.Equal(t, "n.amount", returnItems[1].expr, "second item expression should be n.amount")
	require.Equal(t, "amount", returnItems[1].alias, "second item alias should be amount")

	// Create a node with properties
	_, err := exec.Execute(ctx, `CREATE (order:Order {order_id: "ORD-001", amount: 1000, db: "test_db"}) RETURN order`, nil)
	require.NoError(t, err)

	// Verify node was created with properties
	verifyResult, err := exec.Execute(ctx, `MATCH (n:Order) RETURN n, properties(n) as props`, nil)
	require.NoError(t, err)
	require.NotNil(t, verifyResult)
	require.Len(t, verifyResult.Rows, 1, "should find the Order node")
	if len(verifyResult.Rows) > 0 && len(verifyResult.Rows[0]) >= 2 {
		if props, ok := verifyResult.Rows[0][1].(map[string]interface{}); ok {
			t.Logf("Order node properties: %+v", props)
			require.Contains(t, props, "order_id", "Order node should have order_id property")
			require.Contains(t, props, "amount", "Order node should have amount property")
		}
	}

	// Test property access in MATCH query
	query := `MATCH (n:Order) RETURN n.order_id as order_id, n.amount as amount`
	t.Logf("Testing query: %q", query)
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	t.Logf("Result columns: %+v", result.Columns)
	t.Logf("Result rows: %+v", result.Rows)
	require.Len(t, result.Columns, 2, "should have 2 columns: order_id and amount")
	require.Len(t, result.Rows, 1, "should have 1 row")
	if len(result.Rows) > 0 {
		require.Len(t, result.Rows[0], 2, "row should have 2 values")
		t.Logf("Returned row: %+v", result.Rows[0])
	}

	// Verify properties are accessible
	orderID, ok := result.Rows[0][0].(string)
	require.True(t, ok, "order_id should be a string")
	assert.Equal(t, "ORD-001", orderID, "order_id should be ORD-001")

	// Amount can be int64 or float64 depending on how it was parsed
	var amountValue interface{}
	var amountFloat float64
	var amountInt int64
	if f, ok := result.Rows[0][1].(float64); ok {
		amountValue = f
		amountFloat = f
	} else if i, ok := result.Rows[0][1].(int64); ok {
		amountValue = i
		amountInt = i
	} else {
		t.Fatalf("amount should be int64 or float64, got %T: %v", result.Rows[0][1], result.Rows[0][1])
	}
	require.NotNil(t, amountValue, "amount should not be nil")
	if amountFloat > 0 {
		assert.Equal(t, float64(1000), amountFloat, "amount should be 1000")
	} else {
		assert.Equal(t, int64(1000), amountInt, "amount should be 1000")
	}
}

func TestParseReturnItems_OrderByAfterAliasWithOrderPrefix(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	exec := NewStorageExecutor(store)

	items := exec.parseReturnItems("person_id, person_name, order_ids, order_count ORDER BY person_id")
	require.Len(t, items, 4)
	assert.Equal(t, "person_id", items[0].expr)
	assert.Equal(t, "person_name", items[1].expr)
	assert.Equal(t, "order_ids", items[2].expr)
	assert.Equal(t, "order_count", items[3].expr)
}

func TestParseReturnItems_OrderByOnNewline(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	exec := NewStorageExecutor(store)

	items := exec.parseReturnItems("textKey128, texts\nORDER BY textKey128")
	require.Len(t, items, 2)
	assert.Equal(t, "textKey128", items[0].expr)
	assert.Equal(t, "texts", items[1].expr)
}

func TestProcessCallSubqueryReturn_OrderByOnNewline(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	exec := NewStorageExecutor(store)

	inner := &ExecuteResult{
		Columns: []string{"textKey128", "texts"},
		Rows: [][]interface{}{
			{"a1", []interface{}{"ORD-001"}},
			{"a2", []interface{}{}},
		},
	}

	ctx := context.Background()

	out, err := exec.processCallSubqueryReturn(ctx, inner, "RETURN textKey128, texts\nORDER BY textKey128")
	require.NoError(t, err)
	require.NotNil(t, out)
	require.Len(t, out.Columns, 2)
	require.Equal(t, "textKey128", out.Columns[0])
	require.Equal(t, "texts", out.Columns[1])
	require.Len(t, out.Rows, 2)
	require.Len(t, out.Rows[0], 2)
	require.Len(t, out.Rows[1], 2)
}

func TestExecuteReturn_IgnoresOrderByInProjection(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	exec := NewStorageExecutor(store)
	exec.fabricRecordBindings = map[string]interface{}{
		"textKey128": "a1",
		"texts":      []interface{}{"ORD-001"},
	}

	res, err := exec.executeReturn(context.Background(), "RETURN textKey128, texts\nORDER BY textKey128")
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Len(t, res.Columns, 2)
	require.Equal(t, []string{"textKey128", "texts"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 2)
	require.Equal(t, "a1", res.Rows[0][0])
}

// TestMultipleCreatesPropertyAccess verifies that properties are correctly accessible after multiple CREATE statements
func TestMultipleCreatesPropertyAccess(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes with multiple CREATE statements (like user's query)
	_, err := exec.Execute(ctx, `
		CREATE (charlie:Person {name: "Charlie", id: "b1", db: "test_db_b"})
		CREATE (diana:Person {name: "Diana", id: "b2", db: "test_db_b"})
		CREATE (order:Order {order_id: "ORD-001", amount: 1000, db: "test_db_b"})
		RETURN charlie, diana, order
	`, nil)
	require.NoError(t, err)

	// Query nodes and verify properties are accessible
	result, err := exec.Execute(ctx, `
		MATCH (n)
		RETURN n.name as name, n.order_id as order_id, labels(n) as labels, n.db as db
		ORDER BY n.name
	`, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 3, "should have 3 nodes")

	// Verify properties are set correctly
	foundCharlie := false
	foundDiana := false
	foundOrder := false

	for _, row := range result.Rows {
		require.Len(t, row, 4, "each row should have 4 columns: name, order_id, labels, db")

		name := row[0]
		orderID := row[1]
		db := row[3]

		// Check db property
		if dbVal, ok := db.(string); ok {
			assert.Equal(t, "test_db_b", dbVal, "db property should be test_db_b")
		}

		// Check Person nodes
		if nameVal, ok := name.(string); ok && nameVal == "Charlie" {
			foundCharlie = true
			assert.Nil(t, orderID, "Charlie should not have order_id property")
		} else if nameVal, ok := name.(string); ok && nameVal == "Diana" {
			foundDiana = true
			assert.Nil(t, orderID, "Diana should not have order_id property")
		}

		// Check Order node
		if orderIDVal, ok := orderID.(string); ok && orderIDVal == "ORD-001" {
			foundOrder = true
			assert.Nil(t, name, "Order should not have name property")
		}
	}

	assert.True(t, foundCharlie, "should find Charlie node")
	assert.True(t, foundDiana, "should find Diana node")
	assert.True(t, foundOrder, "should find Order node")
}

func TestMatchMultiAndUnwindBranchCoverage(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice", "age": int64(30), "items": []interface{}{"x", "y", "z"}}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "p2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob", "age": int64(40), "items": []interface{}{"x", "x"}}})
	require.NoError(t, err)

	_, err = exec.executeMultiMatch(ctx, "MATCH (a:Person) MATCH (b:Person) WHERE a <> b")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires RETURN")

	_, err = exec.executeMultiMatch(ctx, "MATCH (a:Person) RETURN a")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected multiple MATCH clauses")

	aggRes, err := exec.executeMultiMatch(ctx, "MATCH (a:Person) MATCH (b:Person) WHERE a <> b RETURN count(*) AS c, sum(a.age) AS s, avg(a.age) AS av, min(a.age) AS mn, max(a.age) AS mx, collect(b.name) AS names")
	require.NoError(t, err)
	require.Len(t, aggRes.Rows, 1)
	assert.Equal(t, int64(2), aggRes.Rows[0][0])
	assert.Equal(t, float64(70), aggRes.Rows[0][1])
	assert.Equal(t, float64(35), aggRes.Rows[0][2])
	assert.Equal(t, int64(30), aggRes.Rows[0][3])
	assert.Equal(t, int64(40), aggRes.Rows[0][4])
	require.Len(t, aggRes.Rows[0][5].([]interface{}), 2)

	b := binding{
		"a": &storage.Node{ID: "p1", Properties: map[string]interface{}{"age": int64(30)}},
		"b": &storage.Node{ID: "p2", Properties: map[string]interface{}{"age": int64(40)}},
	}
	assert.True(t, exec.evaluateBindingWhere(ctx, b, "a <> b AND a.age < b.age", nil))
	assert.True(t, exec.evaluateBindingWhere(ctx, b, "NOT a.age > b.age", nil))
	assert.False(t, exec.evaluateBindingWhere(ctx, b, "a.age > b.age OR a = b", nil))
	assert.False(t, exec.evaluateBindingWhere(ctx, b, "a.name", nil))
	assert.True(t, exec.evaluateWhereForContext(ctx, "a.age < b.age", map[string]*storage.Node{"a": b["a"], "b": b["b"]}))
	assert.False(t, exec.evaluateWhereForContext(ctx, "a.name", map[string]*storage.Node{"a": b["a"]}))
	assert.False(t, isSystemNode(nil))
	assert.True(t, isSystemNode(&storage.Node{Labels: []string{"_meta"}}))
	assert.False(t, isSystemNode(&storage.Node{Labels: []string{"Person"}}))

	_, err = exec.executeMatchUnwind(ctx, "MATCH (n:Person) UNWIND n.items RETURN n")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "UNWIND requires AS clause")

	unwindRes, err := exec.executeMatchUnwind(ctx, "MATCH (n:Person) UNWIND n.items AS item WHERE item <> 'y' RETURN item ORDER BY item DESC SKIP 1 LIMIT 2")
	require.NoError(t, err)
	require.Len(t, unwindRes.Rows, 2)
	assert.Equal(t, "y", unwindRes.Rows[0][0])

	aggUnwindRes, err := exec.executeMatchUnwind(ctx, "MATCH (n:Person {name:'alice'}) UNWIND n.items AS item RETURN count(*) AS c, collect(item) AS allItems")
	require.NoError(t, err)
	require.Len(t, aggUnwindRes.Rows, 1)
	assert.Equal(t, int64(3), aggUnwindRes.Rows[0][0])
	require.Len(t, aggUnwindRes.Rows[0][1].([]interface{}), 3)
}

func TestCypherUtilityConstructorsAndProcedureDDLBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// findStandaloneWithIndex should ignore STARTS/ENDS WITH and find standalone WITH.
	s := "RETURN n WHERE n.name STARTS WITH 'a' WITH n RETURN n"
	withIdx := findStandaloneWithIndex(s)
	require.Greater(t, withIdx, 0)
	assert.Equal(t, "WITH", s[withIdx:withIdx+4])
	assert.Equal(t, -1, findStandaloneWithIndex("RETURN n WHERE n.name ENDS WITH 'z'"))

	// Query analyzer constructor default branch.
	qa := NewQueryAnalyzer(0)
	require.NotNil(t, qa)
	assert.Equal(t, 1000, qa.maxSize)

	// Query plan cache constructor/default and LRU branches.
	pc := NewQueryPlanCache(0)
	require.NotNil(t, pc)
	assert.Equal(t, 500, pc.maxSize)
	pc.Put("MATCH (n) RETURN n", nil, QueryMatch)
	_, qt, found := pc.Get("MATCH   (n)\nRETURN n")
	require.True(t, found)
	assert.Equal(t, QueryMatch, qt)

	pcSmall := NewQueryPlanCache(1)
	pcSmall.Put("RETURN 1", nil, QueryReturn)
	pcSmall.Put("RETURN 2", nil, QueryReturn)
	_, _, found = pcSmall.Get("RETURN 1")
	assert.False(t, found)
	_, _, found = pcSmall.Get("RETURN 2")
	assert.True(t, found)

	// Worker pool constructor/default + execution branches.
	pool := NewWorkerPool(0)
	require.NotNil(t, pool)
	require.Greater(t, pool.numWorkers, 0)
	pool.Start()
	var jobsRun atomic.Int32
	for i := 0; i < 5; i++ {
		pool.Submit(func() { jobsRun.Add(1) })
	}
	pool.Wait()
	assert.Equal(t, int32(5), jobsRun.Load())
	pool.Stop()
	pool.Stop() // idempotent stop

	// Flush on non-async engine is a no-op nil.
	require.NoError(t, exec.Flush())

	// queryDeletesNodes helper behavior.
	assert.True(t, queryDeletesNodes("MATCH (n) DETACH DELETE n"))
	assert.False(t, queryDeletesNodes("MATCH (a)-[r]->(b) DELETE r"))
	assert.True(t, queryDeletesNodes("MATCH (n) DELETE n"))

	// CREATE PROCEDURE: active tx disallowed.
	exec.txContext = &TransactionContext{active: true}
	_, err := exec.executeCreateProcedure(ctx, "CREATE PROCEDURE p_cov() MODE READ AS RETURN 1 AS v")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not allowed inside an active transaction")
	exec.txContext = nil

	// CREATE PROCEDURE: syntax + arg parsing error branches.
	_, err = exec.executeCreateProcedure(ctx, "CREATE PROCEDURE bad syntax")
	require.Error(t, err)
	_, err = exec.executeCreateProcedure(ctx, "CREATE PROCEDURE p_cov_dup(a,a) MODE READ AS RETURN 1 AS v")
	require.Error(t, err)

	// CREATE PROCEDURE: create, duplicate without replace, and replace.
	_, err = exec.executeCreateProcedure(ctx, "CREATE PROCEDURE p_cov(v) MODE READ AS RETURN v AS value")
	require.NoError(t, err)
	_, err = exec.executeCreateProcedure(ctx, "CREATE PROCEDURE p_cov(v) MODE READ AS RETURN v AS value")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
	_, err = exec.executeCreateProcedure(ctx, "CREATE OR REPLACE PROCEDURE p_cov(v) MODE READ AS RETURN v AS value")
	require.NoError(t, err)
}

func TestRunSearchRequestBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.runSearchRequest(ctx, map[string]interface{}{}, false, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query is required")

	_, err = store.CreateNode(&storage.Node{
		ID:         "doc1",
		Labels:     []string{"Doc"},
		Properties: map[string]interface{}{"name": "alpha", "text": "alpha beta"},
	})
	require.NoError(t, err)

	req := map[string]interface{}{
		"query":          "alpha",
		"limit":          int64(5),
		"types":          []interface{}{"Doc"},
		"minSimilarity":  0.01,
		"rerankTopK":     int64(3),
		"rerankMinScore": 0.0,
		"embedding":      []interface{}{0.1, 0.2, 0.3},
	}
	res, err := exec.runSearchRequest(ctx, req, true, true)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, []string{"node", "score", "rrf_score", "vector_rank", "bm25_rank", "search_method", "fallback_triggered"}, res.Columns)
	require.NotNil(t, exec.searchService)
}

func TestExecuteMatchRelationshipsWithClauseBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "a1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "a2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "a3", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "carl"}})
	require.NoError(t, err)
	err = store.CreateEdge(&storage.Edge{ID: "e1", StartNode: "a1", EndNode: "a2", Type: "KNOWS"})
	require.NoError(t, err)
	err = store.CreateEdge(&storage.Edge{ID: "e2", StartNode: "a1", EndNode: "a3", Type: "KNOWS"})
	require.NoError(t, err)
	err = store.CreateEdge(&storage.Edge{ID: "e3", StartNode: "a2", EndNode: "a3", Type: "KNOWS"})
	require.NoError(t, err)

	_, err = exec.executeMatchRelationshipsWithClause(ctx, "not-a-pattern", "", "WITH a RETURN a")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid traversal pattern")

	_, err = exec.executeMatchRelationshipsWithClause(ctx, "(a:Person)-[r:KNOWS]->(b:Person)", "", "WITH a")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RETURN clause required")

	pathRes, err := exec.executeMatchRelationshipsWithClause(
		ctx,
		"p=(a:Person)-[r:KNOWS]->(b:Person)",
		"a.name = 'alice'",
		"WITH p AS path, b AS connected RETURN length(path) AS l, size(relationships(path)) AS relCount, labels(connected) AS labs ORDER BY l DESC SKIP 0 LIMIT 5",
	)
	require.NoError(t, err)
	require.NotEmpty(t, pathRes.Rows)
	require.Len(t, pathRes.Rows[0], 3)
	assert.EqualValues(t, 1, pathRes.Rows[0][0])
	assert.EqualValues(t, 1, pathRes.Rows[0][1])

	withAggRes, err := exec.executeMatchRelationshipsWithClause(
		ctx,
		"(a:Person)-[r:KNOWS]->(b:Person)",
		"",
		"WITH b.name AS friend, count(*) AS c WHERE c >= 1 RETURN friend, c ORDER BY friend SKIP 0 LIMIT 10",
	)
	require.NoError(t, err)
	require.NotEmpty(t, withAggRes.Rows)

	returnAggRes, err := exec.executeMatchRelationshipsWithClause(
		ctx,
		"(a:Person)-[r:KNOWS]->(b:Person)",
		"",
		"WITH b AS connected RETURN count(*) AS total, collect(DISTINCT connected.name) AS names",
	)
	require.NoError(t, err)
	require.Len(t, returnAggRes.Rows, 1)
	assert.Equal(t, int64(3), returnAggRes.Rows[0][0])
}

func TestCreateSetAndSetMergeBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// executeCreateSet: missing SET
	_, err := exec.executeCreateSet(ctx, "CREATE (n:Person)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SET clause not found")

	// parameter branches
	_, err = exec.executeCreateSet(ctx, "CREATE (n:Person) SET n.age = $age RETURN n")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires parameters to be provided")

	ctxWithParams := context.WithValue(ctx, paramsKey, map[string]interface{}{"x": int64(1)})
	_, err = exec.executeCreateSet(ctxWithParams, "CREATE (n:Person) SET n.age = $age RETURN n")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found in provided parameters")

	// property replacement must use map
	_, err = exec.executeCreateSet(ctx, "CREATE (n:Person) SET n = 1 RETURN n")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be a map")

	// unknown variable branches
	_, err = exec.executeCreateSet(ctx, "CREATE (n:Person) SET m.age = 1 RETURN n")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown variable in SET clause")

	_, err = exec.executeCreateSet(ctx, "CREATE (n:Person)-[r:KNOWS]->(m:Person) SET z += {x:1} RETURN n")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown variable in SET +=")

	// invalid label name in SET label assignment
	_, err = exec.executeCreateSet(ctx, "CREATE (n:Person) SET n:bad-label RETURN n")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid label name")

	// successful node + edge property and merge updates
	okRes, err := exec.executeCreateSet(
		ctx,
		"CREATE (a:Person {name:'a'})-[r:KNOWS]->(b:Person {name:'b'}) SET a.age = 30, r.weight = 2, a += {city:'X'}, b:Employee RETURN a.age AS aAge, type(r) AS rt",
	)
	require.NoError(t, err)
	require.Len(t, okRes.Rows, 1)
	assert.EqualValues(t, 30, okRes.Rows[0][0])
	assert.Equal(t, "KNOWS", okRes.Rows[0][1])

	noReturnRes, err := exec.executeCreateSet(ctx, "CREATE (n:DefaultNode {name:'d'}) SET n.flag = true")
	require.NoError(t, err)
	require.NotEmpty(t, noReturnRes.Rows)
	require.Equal(t, "node", noReturnRes.Columns[0])

	// executeSetMerge branches
	target := &storage.Node{ID: "sm1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "merge"}}
	_, err = store.CreateNode(target)
	require.NoError(t, err)

	matchResult := &ExecuteResult{
		Columns: []string{"n", "props"},
		Rows: [][]interface{}{
			{target, map[string]interface{}{"k": int64(9)}},
		},
	}

	mergeOut := &ExecuteResult{Stats: &QueryStats{}}
	_, err = exec.executeSetMerge(ctx, matchResult, "n props", mergeOut, "MATCH (n) SET n props", -1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected += operator")

	_, err = exec.executeSetMerge(ctx, matchResult, "n += ", mergeOut, "MATCH (n) SET n += ", -1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires a map or parameter")

	_, err = exec.executeSetMerge(ctx, matchResult, "n += $props", mergeOut, "MATCH (n) SET n += $props", -1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires parameters to be provided")

	ctxParam := context.WithValue(ctx, paramsKey, map[string]interface{}{"props": map[interface{}]interface{}{1: "x"}})
	_, err = exec.executeSetMerge(ctxParam, matchResult, "n += $props", mergeOut, "MATCH (n) SET n += $props", -1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "string keys")

	_, err = exec.executeSetMerge(ctx, matchResult, "n += missingMap", mergeOut, "MATCH (n) SET n += missingMap", -1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing \"missingMap\"")

	_, err = exec.executeSetMerge(ctx, matchResult, "n += {invalid}", mergeOut, "MATCH (n) SET n += {invalid}", -1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse properties in SET +=")

	_, err = exec.executeSetMerge(ctx, matchResult, "n += {a: 1,}", mergeOut, "MATCH (n) SET n += {a: 1,}", -1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse properties in SET +=")

	badMapResult := &ExecuteResult{Columns: []string{"n", "props"}, Rows: [][]interface{}{{target, true}}}
	_, err = exec.executeSetMerge(ctx, badMapResult, "n += props", mergeOut, "MATCH (n) SET n += props", -1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be a map")

	okSetMergeResult := &ExecuteResult{Stats: &QueryStats{}}
	ctxGoodParam := context.WithValue(ctx, paramsKey, map[string]interface{}{"props": map[string]interface{}{"age": int64(44)}})
	retQuery := "MATCH (n) SET n += $props RETURN n.age AS age"
	retIdx := strings.Index(strings.ToUpper(retQuery), "RETURN")
	got, err := exec.executeSetMerge(ctxGoodParam, matchResult, "n += $props", okSetMergeResult, retQuery, retIdx)
	require.NoError(t, err)
	require.NotEmpty(t, got.Rows)
	assert.EqualValues(t, 44, got.Rows[0][0])

	noReturnSetMerge := &ExecuteResult{Stats: &QueryStats{}}
	got, err = exec.executeSetMerge(ctx, matchResult, "n += {active: true}", noReturnSetMerge, "MATCH (n) SET n += {active: true}", -1)
	require.NoError(t, err)
	require.Equal(t, []string{"matched"}, got.Columns)
	require.EqualValues(t, 1, got.Rows[0][0])
}

func TestEmbedQueryChunkedAndVectorQueryNodeBranches(t *testing.T) {
	ctx := context.Background()

	_, err := embedQueryChunked(ctx, nil, "hello")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no embedder configured")

	oneChunkEmbedder := &sequenceEmbedder{embs: [][]float32{{3, 4}}}
	emb, err := embedQueryChunked(ctx, oneChunkEmbedder, "short")
	require.NoError(t, err)
	assert.Equal(t, []float32{3, 4}, emb)

	multiChunkText := strings.Repeat("chunk ", 800)
	multiEmbedder := &sequenceEmbedder{
		embs: [][]float32{
			{1, 0},
			{0, 1},
			{1, 1},
		},
		errs: []error{
			errors.New("embed failed"),
			nil,
			nil,
		},
	}
	emb, err = embedQueryChunked(ctx, multiEmbedder, multiChunkText)
	require.NoError(t, err)
	require.Len(t, emb, 2)
	norm := math.Sqrt(float64(emb[0]*emb[0] + emb[1]*emb[1]))
	assert.InDelta(t, 1.0, norm, 0.01)

	errOnlyEmbedder := &sequenceEmbedder{errs: []error{errors.New("always fails"), errors.New("still fails"), errors.New("still fails")}}
	_, err = embedQueryChunked(ctx, errOnlyEmbedder, multiChunkText)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "always fails")

	emptyEmbedder := &sequenceEmbedder{embs: [][]float32{{}, {}, {}}}
	_, err = embedQueryChunked(ctx, emptyEmbedder, multiChunkText)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no embeddings produced")

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)

	_, err = exec.callDbIndexVectorQueryNodes(ctx, "CALL db.index.vector.queryNodes('idx', 2, [0.1,0.2])")
	require.NoError(t, err)

	_, err = exec.callDbIndexVectorQueryNodes(ctx, "CALL db.index.vector.queryNodes('idx', 2, 'hello')")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no embedder configured")

	res, err := exec.callDbIndexVectorQueryNodes(ctx, "CALL db.index.vector.queryNodes('idx', 2, $q)")
	require.NoError(t, err)
	assert.Empty(t, res.Rows)

	ctxMissing := context.WithValue(ctx, paramsKey, map[string]interface{}{"x": []float32{0.1, 0.2}})
	_, err = exec.callDbIndexVectorQueryNodes(ctxMissing, "CALL db.index.vector.queryNodes('idx', 2, $q)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parameter $q not provided")

	ctxBadType := context.WithValue(ctx, paramsKey, map[string]interface{}{"q": true})
	_, err = exec.callDbIndexVectorQueryNodes(ctxBadType, "CALL db.index.vector.queryNodes('idx', 2, $q)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported type")

	_, err = exec.callDbIndexVectorQueryNodes(ctxBadType, "CALL db.index.vector.queryNodes('idx', 2)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported type")
}

func TestVectorParsingAndEmbedProcedureBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, _, _, err := exec.parseVectorQueryParams("CALL db.labels()")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "procedure not found")

	_, _, _, err = exec.parseVectorQueryParams("CALL db.index.vector.queryNodes 'idx', 2, [1,2]")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing parameters")

	_, _, _, err = exec.parseVectorQueryParams("CALL db.index.vector.queryNodes('idx', 2, [1,2]")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmatched parenthesis")

	indexName, k, input, err := exec.parseVectorQueryParams("CALL db.index.vector.queryNodes('idx', 3, [0.1, 0.2])")
	require.NoError(t, err)
	assert.Equal(t, "idx", indexName)
	assert.Equal(t, 3, k)
	assert.Equal(t, []float32{0.1, 0.2}, input.vector)

	_, _, input, err = exec.parseVectorQueryParams("CALL db.index.vector.queryRelationships('idx2', 5, 'hello')")
	require.NoError(t, err)
	assert.Equal(t, "hello", input.stringQuery)

	_, _, input, err = exec.parseVectorQueryParams("CALL db.index.vector.queryNodes('idx3', 7, $q)")
	require.NoError(t, err)
	assert.Equal(t, "q", input.paramName)

	parts := splitParamsCarefully("'a,b',[1,2],{\"k\":\"v\"},\"x,y\"")
	require.Len(t, parts, 4)
	assert.Equal(t, "'a,b'", strings.TrimSpace(parts[0]))
	assert.Equal(t, "[1,2]", strings.TrimSpace(parts[1]))

	assert.Equal(t, []float32{1.5, -2}, parseInlineVector("[1.5, -2, nope]"))

	_, err = exec.callDbIndexVectorEmbed(ctx, "CALL db.index.vector.embed('x')")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no embedder configured")

	exec.embedder = &sequenceEmbedder{embs: [][]float32{{1, 2}}}

	_, err = exec.callDbIndexVectorEmbed(ctx, "CALL db.index.vector.embed")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires one argument")

	_, err = exec.callDbIndexVectorEmbed(ctx, "CALL db.index.vector.embed(")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmatched parenthesis")

	_, err = exec.callDbIndexVectorEmbed(ctx, "CALL db.index.vector.embed(123)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires STRING text")

	_, err = exec.callDbIndexVectorEmbed(ctx, "CALL db.index.vector.embed('   ')")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires non-empty text")

	_, err = exec.callDbIndexVectorEmbed(ctx, "CALL db.index.vector.embed($q)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parameter $q not provided")

	ctxParams := context.WithValue(ctx, paramsKey, map[string]interface{}{"q": 1})
	_, err = exec.callDbIndexVectorEmbed(ctxParams, "CALL db.index.vector.embed($q)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be STRING")

	res, err := exec.callDbIndexVectorEmbed(context.WithValue(ctx, paramsKey, map[string]interface{}{"q": "hello"}), "CALL db.index.vector.embed($q)")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
}

func TestCollectSubqueryBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	p := &storage.Node{ID: "p1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}}
	f1 := &storage.Node{ID: "f1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob"}}
	f2 := &storage.Node{ID: "f2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "carl"}}
	_, err := store.CreateNode(p)
	require.NoError(t, err)
	_, err = store.CreateNode(f1)
	require.NoError(t, err)
	_, err = store.CreateNode(f2)
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e1", StartNode: p.ID, EndNode: f1.ID, Type: "KNOWS"}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e2", StartNode: p.ID, EndNode: f2.ID, Type: "KNOWS"}))

	_, err = exec.evaluateCollectSubquery(ctx, p, "p", "COLLECT bad")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid COLLECT subquery syntax")

	_, err = exec.evaluateCollectSubquery(ctx, p, "p", "COLLECT { MATCH (p)-[:KNOWS]->(f) }")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must have a RETURN clause")

	collected, err := exec.evaluateCollectSubquery(ctx, p, "p", "COLLECT { MATCH (p)-[:KNOWS]->(f) RETURN f.name }")
	require.NoError(t, err)
	require.Len(t, collected, 2)

	collected, err = exec.evaluateCollectSubquery(ctx, p, "p", "COLLECT { MATCH (p)-[:KNOWS]->(f) WHERE f.name <> 'none' RETURN f.name }")
	require.NoError(t, err)
	require.Len(t, collected, 2)

	// subquery execution error branch
	_, err = exec.evaluateCollectSubquery(ctx, p, "p", "COLLECT { MATCH (p)-[:KNOWS]->(f) RETURN }")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "execution failed")
}

func TestExecuteMatchCreateBlock_AdditionalBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "a1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "b1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob"}})
	require.NoError(t, err)

	// No CREATE in block: branch returns empty result without error.
	res, err := exec.executeMatchCreateBlock(ctx, "MATCH (a:Person {name: 'alice'})", map[string]*storage.Node{}, map[string]*storage.Edge{})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Empty(t, res.Rows)

	// Relationship creation and edge-property return path.
	nodeVars := map[string]*storage.Node{}
	edgeVars := map[string]*storage.Edge{}
	res, err = exec.executeMatchCreateBlock(
		ctx,
		"MATCH (a:Person {name: 'alice'}), (b:Person {name: 'bob'}) CREATE (a)-[r:KNOWS {since: 2020}]->(b) RETURN r.since AS since",
		nodeVars,
		edgeVars,
	)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(2020), res.Rows[0][0])
	require.Contains(t, edgeVars, "r")

	// Unknown variable in SET clause should error deterministically.
	_, err = exec.executeMatchCreateBlock(
		ctx,
		"MATCH (a:Person {name: 'alice'}) CREATE (t:Temp {name:'tmp'}) SET missing.flag = true",
		map[string]*storage.Node{},
		map[string]*storage.Edge{},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown variable in SET clause")

	// CREATE ... WITH ... DELETE ... RETURN count() path.
	res, err = exec.executeMatchCreateBlock(
		ctx,
		"MATCH (a:Person {name: 'alice'}) CREATE (t:TempDel {name:'x'}) WITH t DELETE t RETURN count(t) AS deleted",
		map[string]*storage.Node{},
		map[string]*storage.Edge{},
	)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(1), res.Rows[0][0])
}

func TestExecuteMatchRelationshipsWithClause_AggregationBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "n2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "n3", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "carol"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "n4", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "dave"}})
	require.NoError(t, err)

	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e1", StartNode: "n1", EndNode: "n3", Type: "KNOWS", Properties: map[string]interface{}{"weight": int64(2)}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e2", StartNode: "n1", EndNode: "n4", Type: "KNOWS", Properties: map[string]interface{}{"weight": int64(3)}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e3", StartNode: "n2", EndNode: "n4", Type: "KNOWS", Properties: map[string]interface{}{"weight": int64(5)}}))

	pattern := "(a:Person)-[r:KNOWS]->(b:Person)"
	withAndReturn := "WITH a.name AS person, count(*) AS c, sum(r.weight) AS s, avg(r.weight) AS av, min(r.weight) AS mn, max(r.weight) AS mx, collect(b.name) AS names, collect(DISTINCT b.name) AS dnames WHERE c >= 2 RETURN person, c, s, av, mn, mx, size(names) AS n ORDER BY person ASC SKIP 0 LIMIT 10"
	res, err := exec.executeMatchRelationshipsWithClause(ctx, pattern, "", withAndReturn)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "alice", res.Rows[0][0])
	require.Equal(t, int64(2), res.Rows[0][1])
	require.Equal(t, int64(5), res.Rows[0][2])
	require.InDelta(t, 2.5, res.Rows[0][3], 0.001)
	require.Equal(t, int64(2), res.Rows[0][4])
	require.Equal(t, int64(3), res.Rows[0][5])
	require.Equal(t, int64(2), res.Rows[0][6])

	// RETURN aggregation branch over computed rows.
	res, err = exec.executeMatchRelationshipsWithClause(
		ctx,
		pattern,
		"",
		"WITH a.name AS person, r.weight AS w RETURN count(*), sum(w), avg(w), min(w), max(w), collect(person), collect(DISTINCT person)",
	)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(3), res.Rows[0][0])
	require.InDelta(t, 10.0, res.Rows[0][1], 0.001)
	require.InDelta(t, 10.0/3.0, res.Rows[0][2], 0.001)
	require.Equal(t, int64(2), res.Rows[0][3])
	require.Equal(t, int64(5), res.Rows[0][4])
}

func TestEvaluateExpressionFromValues_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)

	n := &storage.Node{
		ID:         "n1",
		Labels:     []string{"Person", "Employee"},
		Properties: map[string]interface{}{"name": "alice", "age": int64(30)},
	}
	path := map[string]interface{}{
		"length": int64(2),
		"rels": []*storage.Edge{
			{ID: "e1", Type: "KNOWS", Properties: map[string]interface{}{"since": int64(2020)}},
			{ID: "e2", Type: "WORKS_WITH", Properties: map[string]interface{}{"since": int64(2021)}},
		},
	}
	values := map[string]interface{}{
		"n":    n,
		"path": path,
		"xs":   []interface{}{"a", "b", "c"},
		"m": map[string]interface{}{
			"properties": map[string]interface{}{"title": "eng"},
		},
	}

	require.Equal(t, "alice", exec.evaluateExpressionFromValues("n.name", values))
	require.Equal(t, "eng", exec.evaluateExpressionFromValues("m.title", values))
	require.Equal(t, int64(2), exec.evaluateExpressionFromValues("length(path)", values))
	require.Equal(t, int64(3), exec.evaluateExpressionFromValues("size(xs)", values))
	require.Equal(t, int64(6), exec.evaluateExpressionFromValues("size('abcd')", values))

	labels := exec.evaluateExpressionFromValues("labels(n)", values)
	require.NotNil(t, labels)

	rels := exec.evaluateExpressionFromValues("relationships(path)", values)
	require.Len(t, rels.([]interface{}), 2)

	listComp := exec.evaluateExpressionFromValues("[r IN relationships(path) | type(r)]", values)
	require.Equal(t, []interface{}{"KNOWS", "WORKS_WITH"}, listComp)

	mapLit := exec.evaluateExpressionFromValues("{name: n.name, relCount: size(relationships(path))}", values)
	require.NotNil(t, mapLit)
}

func TestExecuteMatchWithClause_DelegationAndErrorBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "w1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice", "age": int64(31)}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "w2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob", "age": int64(29)}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "w3", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "carol", "age": int64(22)}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "w4", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "nobody"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "wr1", StartNode: "w1", EndNode: "w2", Type: "KNOWS", Properties: map[string]interface{}{"weight": int64(3)}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "wr2", StartNode: "w1", EndNode: "w3", Type: "KNOWS", Properties: map[string]interface{}{"weight": int64(5)}}))

	// Missing WITH/RETURN must fail deterministically.
	_, err = exec.executeMatchWithClause(ctx, "MATCH (n:Person) RETURN n.name")
	require.Error(t, err)
	require.Contains(t, err.Error(), "WITH and RETURN clauses required")

	// WITH + UNWIND branch delegates to executeMatchWithUnwind.
	res, err := exec.executeMatchWithClause(ctx, "MATCH (n:Person) WITH collect(n.name) AS names UNWIND names AS name RETURN name")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(res.Rows), 3)

	// WITH + OPTIONAL MATCH branch delegates to executeMatchWithOptionalMatch.
	res, err = exec.executeMatchWithClause(ctx, "MATCH (n:Person) WITH n OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN n.name, m.name")
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)
	require.Equal(t, "alice", res.Rows[0][0])

	// Relationship-pattern path delegates to executeMatchRelationshipsWithClause.
	res, err = exec.executeMatchWithClause(ctx, "MATCH (a:Person)-[r:KNOWS]->(b:Person) WITH a.name AS who, sum(r.weight) AS total RETURN who, total")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "alice", res.Rows[0][0])
	require.Equal(t, int64(8), res.Rows[0][1])

	// Explicit relationship query with no matches still returns empty rows.
	res, err = exec.executeMatchWithClause(ctx, "MATCH (a:Person)-[r:LIKES]->(b:Person) WITH a, b RETURN a, b")
	require.NoError(t, err)
	require.Len(t, res.Rows, 0)
}

func TestEvaluateExpressionFromValues_MaterializesTemporalExpressions(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	values := map[string]interface{}{
		"row": map[string]interface{}{
			"valid_from_iso":  "2026-03-20T20:22:20Z",
			"valid_to_iso":    nil,
			"asserted_at_iso": "2026-03-21T01:02:03Z",
		},
	}

	require.Equal(t, "2026-03-20T20:22:20Z", exec.evaluateExpressionFromValues("datetime(row.valid_from_iso)", values))
	require.Nil(t, exec.evaluateExpressionFromValues("CASE WHEN row.valid_to_iso IS NULL THEN null ELSE datetime(row.valid_to_iso) END", values))
	require.Equal(t, "2026-03-21T01:02:03Z", exec.evaluateExpressionFromValues("CASE WHEN row.asserted_at_iso IS NULL THEN null ELSE datetime(row.asserted_at_iso) END", values))
}

func TestEvaluateCoalesceInContext_ResolvesComputedMapProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	values := map[string]interface{}{
		"row": map[string]interface{}{
			"name":      "BackendRegex",
			"file_path": "internal/parser/parser.go",
			"entity_id": "symbol::git-to-graph::symbol::internal/parser/parser.go::constant::BackendRegex",
		},
	}

	require.Equal(
		t,
		"BackendRegex",
		exec.evaluateCoalesceInContext("coalesce(row.name, row.path, row.file_path, row.entity_id)", nil, nil, values),
	)
}

func TestExecuteMatchWithClause_ChainedWithAndStorageFailureBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "cw1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "cw2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob"}})
	require.NoError(t, err)

	// Chained WITH parsing: first WITH has WHERE, second WITH has projection.
	res, err := exec.executeMatchWithClause(
		ctx,
		"MATCH (n:Person) WITH n.name AS name WHERE name <> 'bob' WITH name WHERE name STARTS WITH 'a' RETURN name",
	)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "alice", res.Rows[0][0])

	// Exercise storage error path from GetNodesByLabel.
	failing := &failingNodeLookupEngine{
		Engine:     store,
		byLabelErr: errors.New("forced-label-error"),
	}
	execFail := NewStorageExecutor(failing)
	_, err = execFail.executeMatchWithClause(ctx, "MATCH (n:Person) WITH n RETURN n")
	require.Error(t, err)
	require.Contains(t, err.Error(), "forced-label-error")

	// Exercise storage error path from AllNodes.
	failingAll := &failingNodeLookupEngine{
		Engine:      store,
		allNodesErr: errors.New("forced-allnodes-error"),
	}
	execFailAll := NewStorageExecutor(failingAll)
	_, err = execFailAll.executeMatchWithClause(ctx, "MATCH (n) WITH n RETURN n")
	require.Error(t, err)
	require.Contains(t, err.Error(), "forced-allnodes-error")
}

func TestExecuteMatchWithOptionalMatch_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "om1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice", "age": int64(31)}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "om2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob", "age": int64(29)}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "om3", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "carol"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "omr1", StartNode: "om1", EndNode: "om2", Type: "KNOWS", Properties: map[string]interface{}{}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "omr2", StartNode: "om1", EndNode: "om3", Type: "KNOWS", Properties: map[string]interface{}{}}))

	// Validation branch: required clauses.
	_, err = exec.executeMatchWithOptionalMatch(ctx, "MATCH (n:Person) WITH n RETURN n")
	require.Error(t, err)
	require.Contains(t, err.Error(), "WITH, OPTIONAL MATCH, and RETURN clauses required")

	// sourceNode=nil branch (WITH projection drops node object) + ORDER/SKIP/LIMIT branch.
	noNodeRes, err := exec.executeMatchWithOptionalMatch(
		ctx,
		"MATCH (n:Person) WITH n.name AS name OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN name, m.name ORDER BY name SKIP 1 LIMIT 1",
	)
	require.NoError(t, err)
	require.Len(t, noNodeRes.Rows, 1)
	require.NotNil(t, noNodeRes.Rows[0][0])
	require.Nil(t, noNodeRes.Rows[0][1], "optional side should be nil when WITH removed source node")

	// Optional WHERE filters all related nodes -> left-join null row retained.
	filteredRes, err := exec.executeMatchWithOptionalMatch(
		ctx,
		"MATCH (n:Person {name:'alice'}) WITH n OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) WHERE m.age > 100 RETURN n.name, m.name",
	)
	require.NoError(t, err)
	require.Len(t, filteredRes.Rows, 1)
	require.Equal(t, "alice", filteredRes.Rows[0][0])
	require.Nil(t, filteredRes.Rows[0][1])
}

func TestExecuteMatchWithClause_AggregationAndWindowBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seed := []*storage.Node{
		{ID: "mwc1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice", "dept": "eng", "age": int64(30)}},
		{ID: "mwc2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob", "dept": "eng", "age": int64(20)}},
		{ID: "mwc3", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "cara", "dept": "sales", "age": int64(25)}},
	}
	for _, n := range seed {
		_, err := store.CreateNode(n)
		require.NoError(t, err)
	}

	// Aggregated WITH path + post-WITH filtering + ORDER/LIMIT.
	aggRes, err := exec.executeMatchWithClause(
		ctx,
		"MATCH (n:Person) WITH n.dept AS dept, count(n) AS c, collect(n.name) AS names WHERE c >= 1 RETURN dept, c, names ORDER BY dept LIMIT 2",
	)
	require.NoError(t, err)
	require.Len(t, aggRes.Rows, 2)
	require.Equal(t, "eng", aggRes.Rows[0][0])
	require.Equal(t, int64(2), aggRes.Rows[0][1])
	require.NotEmpty(t, aggRes.Rows[0][2])

	// Non-aggregation WITH path + expression evaluation + ORDER/SKIP/LIMIT windowing.
	windowRes, err := exec.executeMatchWithClause(
		ctx,
		"MATCH (n:Person) WITH n.name AS name, n.age AS age RETURN name, age + 1 ORDER BY name SKIP 1 LIMIT 1",
	)
	require.NoError(t, err)
	require.Len(t, windowRes.Rows, 1)
	require.NotNil(t, windowRes.Rows[0][0])
	require.NotNil(t, windowRes.Rows[0][1])
}

func TestExecuteMatchWithClause_MoreAggregationBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seed := []*storage.Node{
		{ID: "mwa1", Labels: []string{"Emp"}, Properties: map[string]interface{}{"name": "alice", "dept": "eng", "score": int64(10)}},
		{ID: "mwa2", Labels: []string{"Emp"}, Properties: map[string]interface{}{"name": "bob", "dept": "eng", "score": float64(15.5)}},
		{ID: "mwa3", Labels: []string{"Emp"}, Properties: map[string]interface{}{"name": "cara", "dept": "ops", "score": int64(7)}},
	}
	for _, n := range seed {
		_, err := store.CreateNode(n)
		require.NoError(t, err)
	}

	// Distinct/non-distinct COUNT and COLLECT plus mixed SUM types.
	res, err := exec.executeMatchWithClause(
		ctx,
		"MATCH (n:Emp) WITH n.dept AS d, count(DISTINCT n.name) AS uniq, count(n.name) AS cnt, sum(n.score) AS total, collect(DISTINCT n.name) AS names RETURN d, uniq, cnt, total, names ORDER BY d",
	)
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "eng", res.Rows[0][0])
	require.Equal(t, int64(2), res.Rows[0][1])
	require.Equal(t, int64(2), res.Rows[0][2])
	require.Equal(t, float64(25.5), res.Rows[0][3])
	require.Len(t, res.Rows[0][4].([]interface{}), 2)

	// Property access on projected node with missing property should resolve to nil.
	missing, err := exec.executeMatchWithClause(
		ctx,
		"MATCH (n:Emp) WITH n AS node RETURN node.unknown ORDER BY node.name LIMIT 1",
	)
	require.NoError(t, err)
	require.Len(t, missing.Rows, 1)
	assert.Nil(t, missing.Rows[0][0])

	// Scalar substitution/evaluation fallback on projected value.
	exprRes, err := exec.executeMatchWithClause(
		ctx,
		"MATCH (n:Emp) WITH n.name AS nm RETURN nm + '-x' ORDER BY nm LIMIT 1",
	)
	require.NoError(t, err)
	require.Len(t, exprRes.Rows, 1)
	assert.Equal(t, "alice-x", exprRes.Rows[0][0])

	// ORDER BY DESC + SKIP + LIMIT windowing path.
	windowed, err := exec.executeMatchWithClause(
		ctx,
		"MATCH (n:Emp) WITH n.dept AS d RETURN d ORDER BY d DESC SKIP 1 LIMIT 1",
	)
	require.NoError(t, err)
	require.Len(t, windowed.Rows, 1)
	assert.Equal(t, "eng", windowed.Rows[0][0])
}

func TestExecuteDeleteAndSetAdditionalBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (a:P {id:'a'})-[:REL]->(b:P {id:'b'})", nil)
	require.NoError(t, err)

	// DETACH without DELETE should fail with deterministic syntax error.
	_, err = exec.executeDelete(ctx, "MATCH (n:P) DETACH n")
	require.Error(t, err)
	assert.Contains(t, strings.ToUpper(err.Error()), "DETACH DELETE")

	// Relationship delete path (row map with _edgeId) and count(*) branch.
	relDelete, err := exec.Execute(ctx, "MATCH (a:P)-[r:REL]->(b:P) DELETE r RETURN count(*) AS c", nil)
	require.NoError(t, err)
	require.Len(t, relDelete.Rows, 1)
	assert.Equal(t, int64(1), relDelete.Rows[0][0])

	// ExecuteSet buildEvalNodes default-scalar branch.
	setScalar, err := exec.Execute(ctx, "MATCH (n:P) WITH n, 5 AS five SET n.score = five RETURN n.score", nil)
	require.NoError(t, err)
	require.Len(t, setScalar.Rows, 2)
	for _, row := range setScalar.Rows {
		assert.Equal(t, int64(5), row[0])
	}

	// ExecuteSet buildEvalNodes map branch.
	setMap, err := exec.Execute(ctx, "MATCH (n:P) WITH n, {bonus: 2} AS m SET n.score = m.bonus RETURN n.score", nil)
	require.NoError(t, err)
	require.Len(t, setMap.Rows, 2)
	for _, row := range setMap.Rows {
		assert.Equal(t, int64(2), row[0])
	}
}

func TestCreateDeleteAdditionalNoReturnAndEdgeCleanupBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:         "alice-create-delete",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "alice"},
	})
	require.NoError(t, err)

	// Direct DELETE-without-WITH/RETURN branch in executeMatchCreateBlock.
	res, err := exec.executeMatchCreateBlock(
		ctx,
		"MATCH (a:Person {name:'alice'}) CREATE (t:Tmp {id:'tmp-no-return'}) DELETE t",
		map[string]*storage.Node{},
		map[string]*storage.Edge{},
	)
	require.NoError(t, err)
	require.NotNil(t, res.Stats)
	assert.Equal(t, 1, res.Stats.NodesCreated)
	assert.Equal(t, 1, res.Stats.NodesDeleted)

	// executeCompoundCreateWithDelete branch where deleting created node also removes created edge.
	compound, err := exec.executeCompoundCreateWithDelete(
		ctx,
		"CREATE (a:Tmp {id:'edge-a'})-[:REL]->(b:Tmp {id:'edge-b'}) WITH a DELETE a RETURN count(a)",
	)
	require.NoError(t, err)
	require.NotNil(t, compound.Stats)
	assert.Equal(t, 2, compound.Stats.NodesCreated)
	assert.Equal(t, 1, compound.Stats.NodesDeleted)
	assert.Equal(t, 1, compound.Stats.RelationshipsCreated)
	assert.Equal(t, 1, compound.Stats.RelationshipsDeleted)
	require.Len(t, compound.Rows, 1)
	assert.Equal(t, int64(1), compound.Rows[0][0])
}

func TestExecuteSet_AdditionalMapAndLabelValidationBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:         "set-branch-node",
		Labels:     []string{"P"},
		Properties: map[string]interface{}{"name": "set-branch"},
	})
	require.NoError(t, err)

	// executeMatch error path while evaluating SET.
	_, err = exec.executeSet(ctx, "MATCH (n:P SET n.value = 1")
	require.Error(t, err)

	// Empty assignment list should fail fast.
	_, err = exec.executeSet(ctx, "MATCH (n:P) SET    ")
	require.Error(t, err)
	assert.Contains(t, strings.ToUpper(err.Error()), "SET CLAUSE")

	// Map-variable merge path inside executeSet (SET n += props).
	merged, err := exec.Execute(ctx, "MATCH (n:P) WITH n, {level: 3} AS props SET n += props RETURN n.level", nil)
	require.NoError(t, err)
	require.Len(t, merged.Rows, 1)
	assert.Equal(t, int64(3), merged.Rows[0][0])

	// Missing map variable should return explicit scope error.
	_, err = exec.Execute(ctx, "MATCH (n:P) SET n += props RETURN n", nil)
	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "map variable")

	// Escaped-label normalization branch (`Quoted``Label` -> Quoted`Label) still fails identifier validation.
	_, err = exec.Execute(ctx, "MATCH (n:P) SET n:`Quoted``Label` RETURN n", nil)
	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "invalid label name")

	// Unescaped keyword label currently accepted by parser/runtime.
	_, err = exec.Execute(ctx, "MATCH (n:P) SET n:RETURN RETURN n", nil)
	require.NoError(t, err)
	kwLabelRes, err := exec.Execute(ctx, "MATCH (n:RETURN) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Len(t, kwLabelRes.Rows, 1)
	assert.Equal(t, int64(1), kwLabelRes.Rows[0][0])

	_, err = exec.Execute(ctx, "MATCH (n:P) SET n:1bad RETURN n", nil)
	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "invalid label name")
}

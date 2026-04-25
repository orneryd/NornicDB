package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/vectorspace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type typedResultFixture struct {
	Name      string    `cypher:"name"`
	Age       int       `json:"age"`
	CreatedAt time.Time `json:"created_at"`
	Score     float64
}

type testTimeStringer string

func (ts testTimeStringer) String() string { return string(ts) }

type failingEmbedder struct {
	err error
}

type fastPathFailEngine struct {
	storage.Engine
	batchErr error
	edgesErr error
}

func (f *fastPathFailEngine) BatchGetNodes(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	if f.batchErr != nil {
		return nil, f.batchErr
	}
	return f.Engine.BatchGetNodes(ids)
}

func (f *fastPathFailEngine) GetEdgesByType(edgeType string) ([]*storage.Edge, error) {
	if f.edgesErr != nil {
		return nil, f.edgesErr
	}
	return f.Engine.GetEdgesByType(edgeType)
}

func (f *failingEmbedder) Embed(context.Context, string) ([]float32, error) {
	if f.err == nil {
		f.err = fmt.Errorf("embed failed")
	}
	return nil, f.err
}

func (f *failingEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return chunkTestText(text, maxTokens, overlap)
}

func TestCypherHelpers_DecodeMapAndAssignValue(t *testing.T) {
	m := map[string]interface{}{
		"name":       "alice",
		"age":        float64(32),
		"created_at": "2024-01-02T03:04:05Z",
		"score":      int64(7),
	}
	var out typedResultFixture
	err := decodeMap(m, reflect.ValueOf(&out).Elem())
	require.NoError(t, err)
	assert.Equal(t, "alice", out.Name)
	assert.Equal(t, 32, out.Age)
	assert.WithinDuration(t, time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), out.CreatedAt, time.Second)
	assert.Equal(t, 7.0, out.Score)

	// assignValue error branch (unsupported conversion)
	var i int
	err = assignValue(reflect.ValueOf(&i).Elem(), "not-a-number")
	assert.Error(t, err)
}

func TestCypherHelpers_ExtractorsAndEnsureLabel(t *testing.T) {
	assert.Equal(t, "g_cov", extractStringArg("CALL gds.graph.project('g_cov', ['A'], ['R'])", "gds.graph.project"))
	assert.Equal(t, "A", extractStringArg("CALL gds.graph.project(g_cov, ['A'], ['R'])", "gds.graph.project"))
	assert.Equal(t, "", extractStringArg("RETURN 1", "gds.graph.project"))
	assert.Equal(t, "", extractStringArg("CALL gds.graph.project('unterminated)", "gds.graph.project"))

	assert.Equal(t, "myGraph", extractGraphNameFromReturn("RETURN gds.graph.project('myGraph', ['A'], ['R'])"))
	assert.Equal(t, "", extractGraphNameFromReturn("RETURN 1"))
	assert.Equal(t, 0.75, extractFloatArg("{dampingFactor: 0.75, iterations: 20}", "dampingFactor"))
	assert.Equal(t, 0.0, extractFloatArg("{iterations: 20}", "dampingFactor"))

	labels := ensureLabel([]string{"A"}, "B")
	assert.ElementsMatch(t, []string{"A", "B"}, labels)
	labels2 := ensureLabel([]string{"A", "B"}, "B")
	assert.ElementsMatch(t, []string{"A", "B"}, labels2)
}

func TestCypherHelpers_ProcedureCatalogNodeID(t *testing.T) {
	id := procedureCatalogNodeID("  Db.Labels  ")
	assert.Equal(t, storage.NodeID(procedureCatalogPrefix+"db.labels"), id)
}

func TestCypherHelpers_NodeMapAndProperties(t *testing.T) {
	exec := &StorageExecutor{}

	nodeNoEmb := &storage.Node{
		ID:         "n-pending",
		Labels:     []string{"Doc"},
		Properties: map[string]interface{}{"name": "pending"},
	}
	pending := exec.nodeToMap(nodeNoEmb)
	require.Equal(t, "n-pending", pending["_nodeId"])
	require.Equal(t, "n-pending", pending["id"]) // fallback to storage ID when user id absent
	// embedding is not injected — only present if user stored it
	_, hasEmb := pending["embedding"]
	assert.False(t, hasEmb, "embedding should not be injected into node map")

	nodeWithUserEmb := &storage.Node{
		ID:     "n-ready",
		Labels: []string{"Doc"},
		Properties: map[string]interface{}{
			"id":        "user-id",
			"name":      "ready",
			"embedding": []float32{1, 2, 3, 4},
		},
	}
	ready := exec.nodeToMap(nodeWithUserEmb)
	require.Equal(t, "user-id", ready["id"]) // preserve user-provided id
	embVal, ok := ready["embedding"].([]float32)
	require.True(t, ok, "user embedding property should be returned as-is")
	assert.Len(t, embVal, 4)
}

func TestCypherHelpers_NodeVersionAtNanosFallsBackToCreatedAt(t *testing.T) {
	createdAt := int64(123456789)
	node := &storage.Node{}

	got := nodeVersionAtNanos(node, createdAt)
	assert.Equal(t, createdAt, got)
}

func TestCypherHelpers_ProcedureRegistryAndPatternNames(t *testing.T) {
	reg := NewProcedureRegistry()
	err := reg.RegisterBuiltIn(ProcedureSpec{Name: "db.labels", MinArgs: 0, MaxArgs: 0}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{}, nil
	})
	require.NoError(t, err)
	err = reg.RegisterUser(ProcedureSpec{Name: "custom.proc", MinArgs: 0, MaxArgs: 1}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{}, nil
	})
	require.NoError(t, err)

	builtins := reg.ListBuiltIns()
	require.Len(t, builtins, 1)
	assert.Equal(t, "db.labels", builtins[0].Name)

	assert.Equal(t, "Generic", PatternGeneric.String())
	assert.Equal(t, "MutualRelationship", PatternMutualRelationship.String())
	assert.Equal(t, "IncomingCountAgg", PatternIncomingCountAgg.String())
	assert.Equal(t, "OutgoingCountAgg", PatternOutgoingCountAgg.String())
	assert.Equal(t, "EdgePropertyAgg", PatternEdgePropertyAgg.String())
	assert.Equal(t, "LargeResultSet", PatternLargeResultSet.String())
}

func TestCypherHelpers_ProcedureRegistryAndArgParsing_Errors(t *testing.T) {
	reg := NewProcedureRegistry()

	err := reg.RegisterBuiltIn(ProcedureSpec{Name: "db.bad", MinArgs: 0, MaxArgs: 0}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil handler")

	err = reg.RegisterUser(ProcedureSpec{Name: "", MinArgs: 0, MaxArgs: 0}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{}, nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name cannot be empty")

	err = reg.RegisterUser(ProcedureSpec{Name: "custom.bad", MinArgs: 2, MaxArgs: 1}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{}, nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MaxArgs")

	// parseProcedureArgLiteral branches.
	v, err := parseProcedureArgLiteral("")
	require.NoError(t, err)
	assert.Equal(t, "", v)
	v, err = parseProcedureArgLiteral("\"abc\"")
	require.NoError(t, err)
	assert.Equal(t, "abc", v)
	v, err = parseProcedureArgLiteral("$param")
	require.NoError(t, err)
	assert.Equal(t, "$param", v)
	v, err = parseProcedureArgLiteral("{a:1}")
	require.NoError(t, err)
	assert.Equal(t, "{a:1}", v)
	v, err = parseProcedureArgLiteral("3.25")
	require.NoError(t, err)
	assert.Equal(t, 3.25, v)
}

func TestCypherHelpers_ExtractCallArguments_IgnoresTailFunctionParens(t *testing.T) {
	args, err := extractCallArguments(
		"CALL db.index.vector.queryNodes('idx_original_text', $topK, $text) " +
			"YIELD node, score " +
			"RETURN elementId(node) AS id, labels(node) AS labels, left(node.originalText,40) AS txt, score LIMIT 5",
	)
	require.NoError(t, err)
	require.Len(t, args, 3)
	assert.Equal(t, "idx_original_text", args[0])
	assert.Equal(t, "$topK", args[1])
	assert.Equal(t, "$text", args[2])
}

func TestCypherHelpers_ExecuteCall_DoesNotMiscountArgsWithTailFunctions(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	origRegistry := globalProcedureRegistry
	origOnce := builtinProcedureRegistryOnce
	doneOnce := sync.Once{}
	doneOnce.Do(func() {})
	globalProcedureRegistry = NewProcedureRegistry()
	builtinProcedureRegistryOnce = doneOnce
	defer func() {
		globalProcedureRegistry = origRegistry
		builtinProcedureRegistryOnce = origOnce
	}()

	err := globalProcedureRegistry.RegisterUser(
		ProcedureSpec{Name: "custom.echo", MinArgs: 3, MaxArgs: 3},
		func(_ context.Context, _ *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
			if len(args) != 3 {
				return nil, fmt.Errorf("unexpected arg count: %d", len(args))
			}
			return &ExecuteResult{
				Columns: []string{"value"},
				Rows:    [][]interface{}{{"ok"}},
			}, nil
		},
	)
	require.NoError(t, err)

	res, err := exec.executeCall(ctx,
		"CALL custom.echo('idx_original_text', 10, 'needle') "+
			"YIELD value "+
			"RETURN left('abcdef', 2) AS prefix, value",
	)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotEmpty(t, res.Rows)
}

func TestCypherHelpers_LooksNumericAndSkipString(t *testing.T) {
	assert.True(t, looksNumeric("42"))
	assert.True(t, looksNumeric("3.1415"))
	assert.False(t, looksNumeric("x42"))
	assert.Equal(t, "17", ExtractSkipString("MATCH (n) RETURN n SKIP 17 LIMIT 5"))
	assert.Equal(t, "", ExtractSkipString("MATCH (n) RETURN n"))
}

func TestCypherHelpers_ClauseAndMapLiterals(t *testing.T) {
	assert.True(t, prevWordEqualsIgnoreCase("name STARTS WITH 'a'", strings.Index("name STARTS WITH 'a'", "WITH"), "STARTS"))
	assert.True(t, prevWordEqualsIgnoreCase("name ENDS WITH 'z'", strings.Index("name ENDS WITH 'z'", "WITH"), "ENDS"))
	assert.False(t, prevWordEqualsIgnoreCase("WITH n", 0, "STARTS"))

	m := normalizeInterfaceMap(map[interface{}]interface{}{"k": 1, 2: "v"})
	assert.Equal(t, 1, m["k"])
	assert.Equal(t, "v", m["2"])

	assert.Equal(t, "'x'", valueToCypherLiteral("x"))
	assert.Equal(t, "true", valueToCypherLiteral(true))
	assert.Equal(t, "3.5", valueToCypherLiteral(3.5))
	assert.Equal(t, "null", valueToCypherLiteral(nil))
	assert.Equal(t, "[1, 'a']", valueToCypherLiteral([]interface{}{1, "a"}))
	assert.Contains(t, valueToCypherLiteral(map[string]interface{}{"a": 1}), "a: 1")
}

func TestCypherHelpers_DurationAndFunctionMatcherHelpers(t *testing.T) {
	d := &CypherDuration{Days: 1, Hours: 2, Minutes: 3, Seconds: 4, Nanos: 5}
	want := 24*time.Hour + 2*time.Hour + 3*time.Minute + 4*time.Second + 5*time.Nanosecond
	assert.Equal(t, want, d.ToTimeDuration())

	args, idx := extractFuncArgsLen("COUNT (n)", "count")
	assert.Equal(t, "n", args)
	assert.GreaterOrEqual(t, idx, 0)

	args, idx = extractFuncArgsLen("count(n) + 1", "count")
	assert.Equal(t, "", args)
	assert.Equal(t, -1, idx)
}

func TestCypherHelpers_CreatePipelineAndCreateHelpers(t *testing.T) {
	assert.True(t, containsString([]string{"a", "b"}, "a"))
	assert.False(t, containsString([]string{"a", "b"}, "c"))

	nodes := []*storage.Node{
		{ID: "2", Properties: map[string]interface{}{"orderId": 2}},
		{ID: "1", Properties: map[string]interface{}{"orderId": 1}},
		{ID: "3", Properties: map[string]interface{}{"other": 9}},
		nil,
	}
	sortNodesByProperty(nodes, "orderId")
	assert.Equal(t, "1", string(nodes[0].ID))
	assert.Equal(t, 1, getNodeProp(nodes[0], "orderId"))
	assert.Nil(t, getNodeProp(nil, "orderId"))
	assert.Nil(t, getNodeProp(&storage.Node{}, "orderId"))

	keys := getKeys(map[string]*storage.Node{"b": {}, "a": {}})
	assert.Len(t, keys, 2)
	assert.Contains(t, keys, "a")
	assert.Contains(t, keys, "b")
}

func TestCypherHelpers_NodeLookupCacheHelpers(t *testing.T) {
	exec := &StorageExecutor{
		nodeLookupCache: make(map[string]*storage.Node, 1000),
	}
	key := makeLookupKey("Person", map[string]interface{}{"name": "alice", "age": 30})
	assert.True(t, strings.HasPrefix(key, "Person:"))
	assert.Contains(t, key, "name=alice")
	assert.Contains(t, key, "age=30")
	assert.Equal(t, "Person", makeLookupKey("Person", nil))

	n := &storage.Node{ID: "n1"}
	exec.cacheNodeLookup("Person", map[string]interface{}{"name": "alice"}, n)
	got := exec.lookupCachedNode("Person", map[string]interface{}{"name": "alice"})
	require.NotNil(t, got)
	assert.Equal(t, storage.NodeID("n1"), got.ID)

	// eviction branch
	exec.nodeLookupCache = make(map[string]*storage.Node, 10002)
	for i := 0; i < 10002; i++ {
		exec.nodeLookupCache[makeLookupKey("X", map[string]interface{}{"i": i})] = &storage.Node{ID: storage.NodeID("x")}
	}
	exec.cacheNodeLookup("Person", map[string]interface{}{"name": "bob"}, &storage.Node{ID: "n2"})
	assert.LessOrEqual(t, len(exec.nodeLookupCache), 1001)
	exec.invalidateNodeLookupCache()
	assert.Len(t, exec.nodeLookupCache, 0)
}

func TestCypherHelpers_UnwindMergeChainPlanCacheHelpers(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	mutation := "MERGE (t:TranslatedText {translationId: row.translationId, language: row.language}) ON CREATE SET t.translatedText = row.translatedText ON MATCH SET t.translatedText = row.translatedText"

	plan := exec.cachedUnwindMergeChainPlan(mutation)
	require.True(t, plan.supported)
	require.NotNil(t, exec.unwindMergeChainPlanCache)
	assert.Len(t, exec.unwindMergeChainPlanCache.plans, 1)

	clone := exec.cloneWithStorage(exec.storage)
	require.Same(t, exec.unwindMergeChainPlanCache, clone.unwindMergeChainPlanCache)

	clonedPlan := clone.cachedUnwindMergeChainPlan(mutation)
	assert.Equal(t, plan, clonedPlan)
	assert.Len(t, exec.unwindMergeChainPlanCache.plans, 1)
	assert.Len(t, clone.unwindMergeChainPlanCache.plans, 1)
}

func TestCypherHelpers_MatchRowsAndTransactionProjection(t *testing.T) {
	exec := &StorageExecutor{}
	nodes := map[string]*storage.Node{
		"n": {ID: "n1", Properties: map[string]interface{}{"name": "alice", "age": int64(42)}},
	}
	co := exec.evaluateCoalesceInContext("COALESCE(n.missing, n.name, 'fallback')", nodes, nil, map[string]interface{}{})
	assert.Equal(t, "alice", co)
	co2 := exec.evaluateCoalesceInContext("COALESCE(missing, 'fallback')", nodes, nil, map[string]interface{}{})
	assert.Equal(t, "fallback", co2)
	assert.Nil(t, exec.evaluateCoalesceInContext("COALESCE(", nodes, nil, nil))

	assert.True(t, exec.nodeMatchesWhereClause(nodes["n"], "n.age >= 40", "n"))
	assert.False(t, exec.nodeMatchesWhereClause(nodes["n"], "n.age < 40", "n"))

	input := &ExecuteResult{
		Columns: []string{"x", "n"},
		Rows: [][]interface{}{
			{int64(7), nodes["n"]},
		},
	}
	out, err := exec.projectTransactionReturn(input, "x AS val, n.name AS name")
	require.NoError(t, err)
	require.Equal(t, []string{"val", "name"}, out.Columns)
	require.Len(t, out.Rows, 1)
	require.Equal(t, int64(7), out.Rows[0][0])
	require.Equal(t, "alice", out.Rows[0][1])

	empty, err := exec.projectTransactionReturn(input, "")
	require.NoError(t, err)
	require.Equal(t, []string{"*"}, empty.Columns)
	require.Len(t, empty.Rows, 1)
	require.Equal(t, "*", empty.Rows[0][0])
}

func TestCypherHelpers_ToStringAnyMapAndSubstringSet(t *testing.T) {
	m, ok := toStringAnyMap(map[string]interface{}{"a": 1})
	require.True(t, ok)
	assert.Equal(t, 1, m["a"])

	m, ok = toStringAnyMap(map[interface{}]interface{}{"a": 1})
	require.True(t, ok)
	assert.Equal(t, 1, m["a"])

	_, ok = toStringAnyMap(map[interface{}]interface{}{1: "x"})
	assert.False(t, ok)
	_, ok = toStringAnyMap([]int{1, 2})
	assert.False(t, ok)

	exec := &StorageExecutor{}
	assert.Equal(t, "bc", exec.evaluateSubstringForSet("substring('abc', 1, 2)"))
	assert.Equal(t, "c", exec.evaluateSubstringForSet("substring('abc', 2)"))
	assert.Equal(t, "", exec.evaluateSubstringForSet("substring('abc', 9, 1)"))
	assert.Equal(t, "ab", exec.evaluateSubstringForSet("substring('abc', bad, 2)"))
	assert.Equal(t, "", exec.evaluateSubstringForSet("substring('abc')"))
}

func TestCypherHelpers_RagToFloat64Branches(t *testing.T) {
	v, ok := ragToFloat64(float64(1.5))
	require.True(t, ok)
	assert.Equal(t, float64(1.5), v)

	v, ok = ragToFloat64(float32(2.5))
	require.True(t, ok)
	assert.Equal(t, float64(2.5), v)

	v, ok = ragToFloat64(3)
	require.True(t, ok)
	assert.Equal(t, float64(3), v)

	v, ok = ragToFloat64(int64(4))
	require.True(t, ok)
	assert.Equal(t, float64(4), v)

	v, ok = ragToFloat64(" 5.25 ")
	require.True(t, ok)
	assert.Equal(t, float64(5.25), v)

	_, ok = ragToFloat64("bad")
	assert.False(t, ok)
	_, ok = ragToFloat64(true)
	assert.False(t, ok)
}

func TestCypherHelpers_EvaluateExpressionWithContextFullOperators_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	nodes := map[string]*storage.Node{
		"n": {
			ID:     "n1",
			Labels: []string{"Person"},
			Properties: map[string]interface{}{
				"name": "alice",
				"age":  int64(30),
			},
		},
	}

	eval := func(expr string) interface{} {
		return exec.evaluateExpressionWithContextFullOperators(expr, strings.ToLower(expr), nodes, nil, nil, nil, nil, 0)
	}

	assert.Equal(t, false, eval("NOT true"))
	assert.Equal(t, true, eval("5 BETWEEN 1 AND 10"))
	// Invalid BETWEEN shape falls through to generic evaluation path.
	assert.Equal(t, "5 BETWEEN 1", eval("5 BETWEEN 1"))
	assert.Equal(t, true, eval("1 = 1 AND 2 = 2"))
	assert.Equal(t, true, eval("1 = 2 OR 2 = 2"))
	assert.Equal(t, true, eval("true XOR false"))
	assert.Equal(t, true, eval("1 <> 2"))
	assert.Equal(t, int64(3), eval("1 + 2"))
	assert.Equal(t, float64(-1.5), eval("-1.5"))
	assert.Equal(t, "ab", eval("'a' + 'b'"))
	assert.Equal(t, int64(-5), eval("-5"))
	assert.Equal(t, true, eval("n.missing IS NULL"))
	assert.Equal(t, true, eval("n.name IS NOT NULL"))
	assert.Equal(t, true, eval("n.name STARTS WITH 'al'"))
	assert.Equal(t, true, eval("n.name ENDS WITH 'ce'"))
	assert.Equal(t, true, eval("n.name CONTAINS 'lic'"))
	assert.Equal(t, false, eval("n.age STARTS WITH '3'"))
	assert.Equal(t, true, eval("'a' IN ['a','b']"))
	assert.Equal(t, true, eval("30 IN [20,30]"))
	assert.Equal(t, false, eval("'a' IN 42"))
	assert.Equal(t, true, eval("'z' NOT IN ['a','b']"))
	assert.Equal(t, false, eval("30 NOT IN [10,30]"))
	assert.Equal(t, true, eval("'z' NOT IN 42"))
	assert.Equal(t, "alice", eval("n.name"))
}

func TestCypherHelpers_CallTxSetMetadata_SyntaxBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	// No active transaction.
	_, err := exec.callTxSetMetadata("CALL tx.setMetaData({app:'x'})")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires an active transaction")

	// Active tx with unsupported tx implementation.
	exec.txContext = &TransactionContext{active: true, tx: struct{}{}}
	_, err = exec.callTxSetMetadata("CALL tx.setMetaData({app:'x'})")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transaction type not supported")

	// Invalid syntax / missing payload branches.
	exec.txContext = &TransactionContext{active: true, tx: struct{}{}}
	_, err = exec.callTxSetMetadata("CALL tx.meta({app:'x'})")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid tx.setMetaData syntax")

	_, err = exec.callTxSetMetadata("CALL tx.setMetaData")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing parentheses")

	_, err = exec.callTxSetMetadata("CALL tx.setMetaData()")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires a metadata object")

	_, err = exec.callTxSetMetadata("CALL tx.setMetaData({})")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one key-value pair")
}

func TestCypherHelpers_FindNodeByProperties_AndRangeIndex(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice", "age": int64(30)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "p2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob", "age": int64(40)}})
	require.NoError(t, err)

	n := exec.findNodeByProperties(map[string]interface{}{"name": "alice"})
	require.NotNil(t, n)
	assert.Equal(t, storage.NodeID("p1"), n.ID)
	assert.Nil(t, exec.findNodeByProperties(map[string]interface{}{"name": "nobody"}))

	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX idx_age IF NOT EXISTS FOR (n:Person) ON (n.age)")
	require.NoError(t, err)
	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX FOR (n:Person) ON (n.age)")
	require.NoError(t, err)
	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX idx_bad FOR (n:Person) ON (n.a, n.b)")
	require.Error(t, err)
	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX nonsense")
	require.Error(t, err)
}

func TestCypherHelpers_ExecuteMatchWithPipelineToRows(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	// Seed OrderStatus and Pharmacy data expected by pipeline helper.
	_, err := eng.CreateNode(&storage.Node{ID: "o1", Labels: []string{"OrderStatus"}, Properties: map[string]interface{}{"orderId": int64(1)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "o2", Labels: []string{"OrderStatus"}, Properties: map[string]interface{}{"orderId": int64(2)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "ph1", Labels: []string{"Pharmacy"}, Properties: map[string]interface{}{"id": int64(10)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "ph2", Labels: []string{"Pharmacy"}, Properties: map[string]interface{}{"id": int64(20)}})
	require.NoError(t, err)

	matchPart := "MATCH (o:OrderStatus) WITH collect(o) AS orders UNWIND range(0, size(orders)-1) AS i WITH orders[i] AS o, i MATCH (ph:Pharmacy) WITH o, i, ph ORDER BY ph.id WITH o, i, collect(ph) AS pharmacies WITH o, pharmacies[i % size(pharmacies)] AS pharmacy"
	rows, err := exec.executeMatchWithPipelineToRows(ctx, matchPart, []string{"o", "pharmacy"}, eng)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	for _, row := range rows {
		_, okO := row["o"].(*storage.Node)
		_, okP := row["pharmacy"].(*storage.Node)
		assert.True(t, okO)
		assert.True(t, okP)
	}

	// Missing WITH should error.
	_, err = exec.executeMatchWithPipelineToRows(ctx, "MATCH (o:OrderStatus)", []string{"o"}, eng)
	require.Error(t, err)
}

func TestCypherHelpers_ParserMarkersAndUnwindHelpers(t *testing.T) {
	// Clause marker coverage.
	var _ Clause = &MatchClause{}
	var _ Clause = &CreateClause{}
	var _ Clause = &ReturnClause{}
	var _ Clause = &WhereClause{}
	var _ Clause = &SetClause{}
	var _ Clause = &DeleteClause{}
	for _, c := range []Clause{
		&MatchClause{}, &CreateClause{}, &ReturnClause{}, &WhereClause{}, &SetClause{}, &DeleteClause{},
	} {
		c.clauseMarker()
	}

	// Expression marker coverage.
	var _ Expression = &PropertyAccess{}
	var _ Expression = &Comparison{}
	var _ Expression = &Literal{}
	var _ Expression = &Parameter{}
	var _ Expression = &FunctionCall{}
	for _, ex := range []Expression{
		&PropertyAccess{}, &Comparison{}, &Literal{}, &Parameter{}, &FunctionCall{},
	} {
		ex.exprMarker()
	}

	assert.True(t, hasOuterParens("(a)"))
	assert.True(t, hasOuterParens("((a+b))"))
	assert.False(t, hasOuterParens("(a)+b"))
	assert.False(t, hasOuterParens("a"))
	assert.False(t, hasOuterParens("(a"))
	assert.False(t, hasOuterParens("a)"))
	assert.False(t, hasOuterParens("(a) + (b)"))
	assert.True(t, hasOuterParens("(\"(\")"))
	assert.True(t, hasOuterParens("(')')"))
	assert.False(t, hasOuterParens("('x') + ('y')"))
	assert.True(t, hasOuterParens("((\"(\") + (')'))"))
	assert.Equal(t, "$x", normalizeUnwindExpression(" ( ($x) ) "))

	assert.Nil(t, coerceToUnwindItems(nil))
	assert.Equal(t, []interface{}{"a", "b"}, coerceToUnwindItems([]string{"a", "b"}))
	assert.Equal(t, []interface{}{1, 2}, coerceToUnwindItems([]int{1, 2}))
	assert.Equal(t, []interface{}{int64(1), int64(2)}, coerceToUnwindItems([]int64{1, 2}))
	assert.Equal(t, []interface{}{"x"}, coerceToUnwindItems("x"))
}

func TestCypherHelpers_CartesianMatchAndAggregation(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{ID: "a1", Labels: []string{"A"}, Properties: map[string]interface{}{"name": "a1"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "a2", Labels: []string{"A"}, Properties: map[string]interface{}{"name": "a2"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "b1", Labels: []string{"B"}, Properties: map[string]interface{}{"name": "b1"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "b2", Labels: []string{"B"}, Properties: map[string]interface{}{"name": "b2"}})
	require.NoError(t, err)

	result := &ExecuteResult{
		Columns: []string{"aName", "bName"},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	items := []returnItem{
		{expr: "a.name", alias: "aName"},
		{expr: "b.name", alias: "bName"},
	}
	_, err = exec.executeCartesianProductMatch(
		ctx,
		"MATCH (a:A), (b:B) RETURN a.name AS aName, b.name AS bName ORDER BY aName SKIP 1 LIMIT 2",
		"(a:A), (b:B)",
		[]string{"(a:A)", "(b:B)"},
		-1,
		strings.Index("MATCH (a:A), (b:B) RETURN a.name AS aName, b.name AS bName ORDER BY aName SKIP 1 LIMIT 2", "RETURN"),
		items,
		false,
		false,
		result,
	)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)

	// Aggregation without grouping.
	aggResult := &ExecuteResult{Columns: []string{"cnt"}, Rows: [][]interface{}{}, Stats: &QueryStats{}}
	aggItems := []returnItem{{expr: "COUNT(*)", alias: "cnt"}}
	_, err = exec.executeCartesianProductMatch(
		ctx,
		"MATCH (a:A), (b:B) RETURN COUNT(*) AS cnt",
		"(a:A), (b:B)",
		[]string{"(a:A)", "(b:B)"},
		-1,
		strings.Index("MATCH (a:A), (b:B) RETURN COUNT(*) AS cnt", "RETURN"),
		aggItems,
		true,
		false,
		aggResult,
	)
	require.NoError(t, err)
	require.Len(t, aggResult.Rows, 1)
	assert.Equal(t, int64(4), aggResult.Rows[0][0])

	// Aggregation with grouping path.
	allMatches := []map[string]*storage.Node{
		{"a": {ID: "a1", Properties: map[string]interface{}{"name": "x"}}},
		{"a": {ID: "a2", Properties: map[string]interface{}{"name": "x"}}},
		{"a": {ID: "a3", Properties: map[string]interface{}{"name": "y"}}},
	}
	groupRes := &ExecuteResult{Columns: []string{"name", "cnt"}, Rows: [][]interface{}{}, Stats: &QueryStats{}}
	_, err = exec.executeCartesianAggregation(allMatches, []returnItem{{expr: "a.name", alias: "name"}, {expr: "COUNT(*)", alias: "cnt"}}, groupRes)
	require.NoError(t, err)
	require.Len(t, groupRes.Rows, 2)
}

func TestCypherHelpers_TraversalAndShortestPathHelpers(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	_, err := eng.CreateNode(&storage.Node{ID: "a1", Labels: []string{"A"}, Properties: map[string]interface{}{"name": "alpha"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "a2", Labels: []string{"A"}, Properties: map[string]interface{}{"name": "beta"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "b1", Labels: []string{"B"}, Properties: map[string]interface{}{"name": "bee"}})
	require.NoError(t, err)
	err = eng.CreateEdge(&storage.Edge{
		ID:        "e1",
		StartNode: "a1",
		EndNode:   "b1",
		Type:      "KNOWS",
	})
	require.NoError(t, err)
	err = eng.CreateEdge(&storage.Edge{
		ID:        "e2",
		StartNode: "a2",
		EndNode:   "b1",
		Type:      "KNOWS",
	})
	require.NoError(t, err)

	// Invalid pattern branch for executeMatchWithRelationships.
	_, err = exec.executeMatchWithRelationships(context.Background(), "this is not a pattern", "", []returnItem{{expr: "a", alias: "a"}})
	require.Error(t, err)

	// Valid traversal branch.
	r, err := exec.executeMatchWithRelationships(context.Background(), "(a:A)-[r:KNOWS]->(b:B)", "", []returnItem{
		{expr: "a.name", alias: "aName"},
		{expr: "b.name", alias: "bName"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, r.Rows)

	// Cover traverseGraphParallel directly with explicit worker config.
	match := exec.parseTraversalPattern("(a:A)-[r:KNOWS]->(b:B)")
	require.NotNil(t, match)
	startNodes, err := eng.GetNodesByLabel("A")
	require.NoError(t, err)
	paths := exec.traverseGraphParallel(match, startNodes, ParallelConfig{
		Enabled:      true,
		MaxWorkers:   2,
		MinBatchSize: 1,
	}, TemporalViewport{}, nil)
	require.Len(t, paths, 2)

	// Cover evaluatePathExpression helper.
	path := PathResult{
		Nodes:  []*storage.Node{{ID: "s1", Properties: map[string]interface{}{"name": "start"}}, {ID: "e1", Properties: map[string]interface{}{"name": "end"}}},
		Length: 1,
	}
	q := &ShortestPathQuery{
		startNode: nodePatternInfo{variable: "s"},
		endNode:   nodePatternInfo{variable: "e"},
	}
	assert.Equal(t, "start", exec.evaluatePathExpression("s.name", path, q))

	// Sanity to ensure shortest path helper still identifies syntax.
	assert.True(t, isShortestPathQuery("MATCH p=shortestPath((a)-[*]->(b)) RETURN p"))
	assert.True(t, isShortestPathQuery("MATCH p=allShortestPaths((a)-[*]->(b)) RETURN p"))
	assert.False(t, isShortestPathQuery("MATCH (a) RETURN a"))
}

func TestCypherHelpers_ExecuteCallFallbackDispatch(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{ID: "n1", Labels: []string{"L1"}, Properties: map[string]interface{}{"name": "x"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "n2", Labels: []string{"L2"}, Properties: map[string]interface{}{"name": "y"}})
	require.NoError(t, err)
	err = eng.CreateEdge(&storage.Edge{
		ID:         "rel1",
		StartNode:  "n1",
		EndNode:    "n2",
		Type:       "KNOWS",
		Properties: map[string]interface{}{"text": "hello world"},
	})
	require.NoError(t, err)

	// Force executeCall to use legacy switch fallback (instead of registry-first dispatch)
	// so we can validate and cover those branches while preserving production behavior.
	origRegistry := globalProcedureRegistry
	origOnce := builtinProcedureRegistryOnce
	doneOnce := sync.Once{}
	doneOnce.Do(func() {})
	globalProcedureRegistry = NewProcedureRegistry()
	builtinProcedureRegistryOnce = doneOnce
	defer func() {
		globalProcedureRegistry = origRegistry
		builtinProcedureRegistryOnce = origOnce
	}()

	cases := []struct {
		query     string
		expectErr bool
	}{
		{query: "CALL db.labels()", expectErr: false},
		{query: "CALL db.relationshipTypes()", expectErr: false},
		{query: "CALL db.schema.visualization()", expectErr: false},
		{query: "CALL db.schema.nodeProperties()", expectErr: false},
		{query: "CALL db.schema.relProperties()", expectErr: false},
		{query: "CALL db.indexes()", expectErr: false},
		{query: "CALL db.index.stats()", expectErr: false},
		{query: "CALL db.constraints()", expectErr: false},
		{query: "CALL db.propertyKeys()", expectErr: false},
		{query: "CALL db.info()", expectErr: false},
		{query: "CALL db.ping()", expectErr: false},
		{query: "CALL dbms.info()", expectErr: false},
		{query: "CALL dbms.listConfig()", expectErr: false},
		{query: "CALL dbms.clientConfig()", expectErr: false},
		{query: "CALL dbms.listConnections()", expectErr: false},
		{query: "CALL dbms.components()", expectErr: false},
		{query: "CALL dbms.procedures()", expectErr: false},
		{query: "CALL dbms.functions()", expectErr: false},
		{query: "CALL db.index.fulltext.listAvailableAnalyzers()", expectErr: false},
		{query: "CALL db.index.fulltext.queryRelationships('idx','hello')", expectErr: false},
		{query: "CALL db.stats.status()", expectErr: false},
		{query: "CALL db.clearQueryCaches()", expectErr: false},
		{query: "CALL tx.setMetaData({app:'x'})", expectErr: true}, // requires active tx
		{query: "CALL db.notARealProcedure()", expectErr: true},
	}

	for _, tc := range cases {
		res, err := exec.executeCall(ctx, tc.query)
		if tc.expectErr {
			require.Error(t, err, tc.query)
		} else {
			require.NoError(t, err, tc.query)
			require.NotNil(t, res, tc.query)
		}
	}

	expectSuccess := []string{
		"CALL nornicdb.version()",
		"CALL nornicdb.stats()",
		"CALL nornicdb.decay.info()",
		"CALL apoc.path.subgraphNodes('n1', {maxLevel: 1})",
		"CALL apoc.path.expand('n1', 'KNOWS>', '', 1, 2)",
		"CALL apoc.algo.pageRank({iterations: 2})",
		"CALL apoc.algo.louvain()",
		"CALL apoc.algo.betweenness()",
		"CALL apoc.algo.closeness()",
		"CALL apoc.algo.labelPropagation()",
		"CALL apoc.algo.wcc()",
		"CALL apoc.neighbors.tohop('n1','KNOWS',1)",
		"CALL apoc.neighbors.byhop('n1','KNOWS',2)",
		"CALL gds.version()",
		"CALL gds.graph.list()",
		"CALL apoc.periodic.iterate('RETURN 1 AS n','RETURN n',{})",
		"CALL apoc.periodic.commit('RETURN 1')",
		"CALL apoc.periodic.rock_n_roll('RETURN 1 AS n','RETURN n',{})",
		"CALL apoc.export.csv.all('file:///missing.csv',{})",
		"CALL apoc.export.csv.query('MATCH (n) RETURN n','file:///missing.csv',{})",
		"CALL apoc.export.json.all('file:///missing.json',{})",
		"CALL apoc.export.json.query('MATCH (n) RETURN n','file:///missing.json',{})",
		"CALL apoc.load.jsonarray('file:///missing.json')",
		"CALL apoc.load.json('file:///missing.json')",
		"CALL apoc.load.csv('file:///missing.csv')",
		"CALL apoc.import.json('file:///missing.json')",
		"CALL gds.graph.project('g_cov', ['L1'], ['KNOWS'])",
		"CALL gds.fastRP.stream('g_cov')",
		"CALL gds.fastRP.stats('g_cov')",
		"CALL db.index.vector.queryNodes('idx', 2, [0.1,0.2])",
		"CALL db.index.vector.createNodeIndex('idx_cov','L1','embedding',2,'cosine')",
		"CALL db.index.vector.createRelationshipIndex('idx_cov_rel','KNOWS','embedding',2,'cosine')",
		"CALL db.index.fulltext.createNodeIndex('idx_txt_cov',['L1'],['name'])",
		"CALL db.index.fulltext.createRelationshipIndex('idx_rel_cov',['KNOWS'],['text'])",
		"CALL db.index.fulltext.drop('idx_txt_cov')",
		"CALL db.index.vector.drop('idx_cov')",
		"CALL db.create.setNodeVectorProperty('n1','embedding',[0.1,0.2])",
		"CALL db.create.setRelationshipVectorProperty('rel1','embedding',[0.1,0.2])",
		"CALL apoc.cypher.run('RETURN 1', {})",
		"CALL apoc.cypher.runMany('RETURN 1; RETURN 2', {})",
		"CALL db.retrieve({query:'x'})",
		"CALL db.rretrieve({query:'x'})",
		"CALL db.awaitIndexes()",
		"CALL db.awaitIndex('idx')",
		"CALL db.resampleIndex('idx')",
		"CALL db.stats.retrieveAllAnTheStats()",
		"CALL db.stats.retrieve('QUERIES')",
		"CALL db.stats.collect('QUERIES')",
		"CALL db.stats.clear()",
		"CALL db.stats.stop()",
	}
	for _, q := range expectSuccess {
		res, err := exec.executeCall(ctx, q)
		require.NoErrorf(t, err, "expected success for query: %s", q)
		require.NotNilf(t, res, "expected non-nil result for query: %s", q)
	}

	expectError := []string{
		"CALL apoc.algo.dijkstra()",
		"CALL apoc.algo.astar()",
		"CALL apoc.algo.allSimplePaths()",
		"CALL apoc.path.spanningTree('n1', {maxLevel: 1})",
		"CALL gds.graph.drop('missing')",
		"CALL gds.linkprediction.adamicAdar.stream('g_cov',{sourceNode:'n1',targetNode:'n2'})",
		"CALL gds.linkprediction.commonNeighbors.stream('g_cov',{sourceNode:'n1',targetNode:'n2'})",
		"CALL gds.linkprediction.resourceAllocation.stream('g_cov',{sourceNode:'n1',targetNode:'n2'})",
		"CALL gds.linkprediction.preferentialAttachment.stream('g_cov',{sourceNode:'n1',targetNode:'n2'})",
		"CALL gds.linkprediction.jaccard.stream('g_cov',{sourceNode:'n1',targetNode:'n2'})",
		"CALL gds.linkprediction.predict.stream('g_cov',{sourceNode:'n1',targetNode:'n2'})",
		"CALL db.index.fulltext.queryNodes('idx','hello')",
		"CALL db.index.vector.embed('hello')",
		"CALL db.rerank({query:'x', candidates: []})",
		"CALL db.infer({prompt:'x'})",
		"CALL db.txlog.entries(1, 10)",
		"CALL db.txlog.byTxId('tx-1', 10)",
	}
	for _, q := range expectError {
		_, err := exec.executeCall(ctx, q)
		require.Errorf(t, err, "expected error for query: %s", q)
	}

}

func TestCypherHelpers_CallCompatRelationshipQueries(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "a"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "n2", Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "b"}})
	require.NoError(t, err)
	err = eng.CreateEdge(&storage.Edge{
		ID:         "e1",
		StartNode:  "n1",
		EndNode:    "n2",
		Type:       "RELATED",
		Properties: map[string]interface{}{"content": "searchable text"},
	})
	require.NoError(t, err)

	// Fulltext relationship query: empty query branch.
	res, err := exec.callDbIndexFulltextQueryRelationships("CALL db.index.fulltext.queryRelationships('idx','')")
	require.NoError(t, err)
	require.Empty(t, res.Rows)

	// Fulltext relationship query: match path.
	res, err = exec.callDbIndexFulltextQueryRelationships("CALL db.index.fulltext.queryRelationships('idx','searchable')")
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)

	// Vector relationship query: parse error.
	_, err = exec.callDbIndexVectorQueryRelationships(ctx, "CALL db.index.vector.queryRelationships('idx', 2)")
	require.Error(t, err)

	// Vector relationship query: string input without embedder.
	_, err = exec.callDbIndexVectorQueryRelationships(ctx, "CALL db.index.vector.queryRelationships('idx', 2, 'hello')")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no embedder configured")

	// Vector relationship query: parameter without params context => empty rows.
	res, err = exec.callDbIndexVectorQueryRelationships(ctx, "CALL db.index.vector.queryRelationships('idx', 2, $q)")
	require.NoError(t, err)
	require.Empty(t, res.Rows)

	// Vector relationship query: missing parameter.
	ctxMissing := context.WithValue(ctx, paramsKey, map[string]interface{}{"x": []float32{0.1, 0.2}})
	_, err = exec.callDbIndexVectorQueryRelationships(ctxMissing, "CALL db.index.vector.queryRelationships('idx', 2, $q)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "parameter $q not provided")

	// Vector relationship query: unsupported parameter element type.
	ctxBadElem := context.WithValue(ctx, paramsKey, map[string]interface{}{"q": []interface{}{1, "bad"}})
	_, err = exec.callDbIndexVectorQueryRelationships(ctxBadElem, "CALL db.index.vector.queryRelationships('idx', 2, $q)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-numeric value")

	// Vector relationship query: unsupported parameter type and missing query vector branch.
	ctxBadType := context.WithValue(ctx, paramsKey, map[string]interface{}{"q": true})
	_, err = exec.callDbIndexVectorQueryRelationships(ctxBadType, "CALL db.index.vector.queryRelationships('idx', 2, $q)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported type")
}

func TestCypherHelpers_ComparisonConversionAndTemporalHelpers(t *testing.T) {
	assert.False(t, compareForSort(nil, nil))
	assert.True(t, compareForSort(nil, "x"))
	assert.False(t, compareForSort("x", nil))
	assert.True(t, compareForSort(int64(1), int64(2)))
	assert.True(t, compareForSort(int64(1), float64(2)))
	assert.True(t, compareForSort(1, 2))
	assert.True(t, compareForSort(1, int64(2)))
	assert.True(t, compareForSort(float64(1.5), float64(2.5)))
	assert.True(t, compareForSort(float64(1.5), int64(2)))
	assert.True(t, compareForSort("a", "b"))
	assert.True(t, compareForSort(struct{ X int }{X: 1}, struct{ X int }{X: 2}))

	assert.Equal(t, int64(3), toInt64(3))
	assert.Equal(t, int64(4), toInt64(int64(4)))
	assert.Equal(t, int64(5), toInt64(float64(5.9)))
	assert.Equal(t, int64(6), toInt64("6"))
	assert.Equal(t, int64(0), toInt64("bad"))
	assert.Equal(t, int64(0), toInt64(true))

	ctx := context.WithValue(context.Background(), paramsKey, map[string]interface{}{"p": "v", "n": int64(7)})
	assert.Nil(t, resolveTemporalArg(ctx, ""))
	assert.Nil(t, resolveTemporalArg(ctx, "NULL"))
	assert.Equal(t, "v", resolveTemporalArg(ctx, "$p"))
	assert.Nil(t, resolveTemporalArg(ctx, "$missing"))
	assert.Equal(t, "s", resolveTemporalArg(ctx, "'s'"))
	assert.Equal(t, "d", resolveTemporalArg(ctx, "\"d\""))
	assert.Equal(t, int64(9), resolveTemporalArg(ctx, "9"))
	assert.Equal(t, float64(3.5), resolveTemporalArg(ctx, "3.5"))
	assert.Equal(t, "raw", resolveTemporalArg(ctx, "raw"))

	_, err := coerceStringArg(nil, "arg")
	require.Error(t, err)
	_, err = coerceStringArg("   ", "arg")
	require.Error(t, err)
	s, err := coerceStringArg(12, "arg")
	require.NoError(t, err)
	assert.Equal(t, "12", s)

	now := time.Now().UTC().Truncate(time.Second)
	tm, ok := coerceDateTime(now)
	require.True(t, ok)
	assert.Equal(t, now, tm)
	tm, ok = coerceDateTime("2024-01-02T03:04:05Z")
	require.True(t, ok)
	assert.Equal(t, int64(1704164645), tm.Unix())
	tm, ok = coerceDateTime(int64(1700000000))
	require.True(t, ok)
	assert.Equal(t, int64(1700000000), tm.Unix())
	tm, ok = coerceDateTime(float64(1700000001))
	require.True(t, ok)
	assert.Equal(t, int64(1700000001), tm.Unix())
	tm, ok = coerceDateTime(testTimeStringer("2024-01-02T03:04:05Z"))
	require.True(t, ok)
	assert.Equal(t, int64(1704164645), tm.Unix())
	_, ok = coerceDateTime(struct{}{})
	assert.False(t, ok)

	_, ok = coerceDateTimeOptional(nil)
	assert.False(t, ok)

	aStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	aEnd := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	bStart := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	bEnd := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, intervalsOverlap(aStart, aEnd, true, bStart, bEnd, true))
	assert.False(t, intervalsOverlap(time.Time{}, aEnd, true, bStart, bEnd, true))
	assert.False(t, intervalsOverlap(aStart, aEnd, true, bEnd, bEnd, true))
	assert.False(t, intervalsOverlap(bEnd, bEnd, true, aStart, aEnd, true))

	assert.True(t, valuesEqual(int64(1), "1"))
	assert.False(t, valuesEqual(1, 2))
	assert.False(t, isTruthy(nil))
	assert.False(t, isTruthy(false))
	assert.False(t, isTruthy(int64(0)))
	assert.False(t, isTruthy(""))
	assert.True(t, isTruthy(true))
	assert.True(t, isTruthy(2))
	assert.True(t, isTruthy("x"))
	assert.True(t, isTruthy(struct{}{}))
}

func TestCypherHelpers_DatabaseNameAndRemoveNodeFromSearch(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "tenant_cov")
	exec := NewStorageExecutor(ns)

	assert.Equal(t, "tenant_cov", exec.databaseName())

	execNoNS := NewStorageExecutor(base)
	t.Setenv("NORNICDB_DEFAULT_DATABASE", "db_env_cov")
	assert.Equal(t, "db_env_cov", execNoNS.databaseName())
	t.Setenv("NORNICDB_DEFAULT_DATABASE", "")
	assert.Equal(t, "nornic", execNoNS.databaseName())

	// removeNodeFromSearch should early-return on empty id / nil search.
	execNoNS.removeNodeFromSearch("")
	execNoNS.removeNodeFromSearch("plain-id")

	// With search service configured, prefixed IDs should be unprefixed before removal.
	svc := search.NewService(base)
	execNoNS.SetSearchService(svc)
	execNoNS.removeNodeFromSearch("tenant_cov:node-1")
	execNoNS.removeNodeFromSearch("node-2")
}

func TestCypherHelpers_VectorRegistryRegisterUnregister(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "tenant_cov")
	exec := NewStorageExecutor(ns)

	// Successful registration with default vector name and cosine distance.
	exec.registerVectorSpace("idx_cov", "Person", "", 3, "cosine")
	key, ok := exec.vectorIndexSpaces["idx_cov"]
	require.True(t, ok)
	assert.Equal(t, "tenant_cov", key.DB)
	assert.Equal(t, "person", key.Type)
	assert.Equal(t, vectorspace.DefaultVectorName, key.VectorName)
	assert.Equal(t, vectorspace.DistanceCosine, key.Distance)

	space, exists := exec.GetVectorRegistry().GetSpace(key)
	require.True(t, exists)
	require.NotNil(t, space)

	// Invalid similarity should be rejected without map insert.
	exec.registerVectorSpace("idx_bad", "Person", "embedding", 3, "chebyshev")
	_, ok = exec.vectorIndexSpaces["idx_bad"]
	assert.False(t, ok)

	// dims <= 0 is ignored.
	exec.registerVectorSpace("idx_zero", "Person", "embedding", 0, "cosine")
	_, ok = exec.vectorIndexSpaces["idx_zero"]
	assert.False(t, ok)

	// unregister should remove both registry and local mapping.
	exec.unregisterVectorSpace("idx_cov")
	_, ok = exec.vectorIndexSpaces["idx_cov"]
	assert.False(t, ok)
	_, exists = exec.GetVectorRegistry().GetSpace(key)
	assert.False(t, exists)

	// Missing index and nil registry are safe no-op branches.
	exec.unregisterVectorSpace("missing")
	exec.vectorRegistry = nil
	exec.registerVectorSpace("idx_nil", "Person", "embedding", 3, "cosine")
	_, ok = exec.vectorIndexSpaces["idx_nil"]
	assert.False(t, ok)
}

func TestCypherHelpers_SchemaVectorAndApocPathHelpers(t *testing.T) {
	// parsePropertyType coverage.
	pt, err := parsePropertyType("STRING")
	require.NoError(t, err)
	assert.Equal(t, storage.PropertyTypeString, pt)
	pt, err = parsePropertyType("INT")
	require.NoError(t, err)
	assert.Equal(t, storage.PropertyTypeInteger, pt)
	pt, err = parsePropertyType("FLOAT")
	require.NoError(t, err)
	assert.Equal(t, storage.PropertyTypeFloat, pt)
	pt, err = parsePropertyType("BOOL")
	require.NoError(t, err)
	assert.Equal(t, storage.PropertyTypeBoolean, pt)
	pt, err = parsePropertyType("DATE")
	require.NoError(t, err)
	assert.Equal(t, storage.PropertyTypeDate, pt)
	pt, err = parsePropertyType("ZONED DATETIME")
	require.NoError(t, err)
	assert.Equal(t, storage.PropertyTypeZonedDateTime, pt)
	pt, err = parsePropertyType("LOCALDATETIME")
	require.NoError(t, err)
	assert.Equal(t, storage.PropertyTypeLocalDateTime, pt)
	_, err = parsePropertyType("UNSUPPORTED")
	require.Error(t, err)

	// toDistanceMetric coverage.
	dist, err := toDistanceMetric("")
	require.NoError(t, err)
	assert.Equal(t, vectorspace.DistanceCosine, dist)
	dist, err = toDistanceMetric("dot")
	require.NoError(t, err)
	assert.Equal(t, vectorspace.DistanceDot, dist)
	dist, err = toDistanceMetric("euclidean")
	require.NoError(t, err)
	assert.Equal(t, vectorspace.DistanceEuclidean, dist)
	_, err = toDistanceMetric("chebyshev")
	require.Error(t, err)

	// isTerminateNode coverage.
	node := &storage.Node{ID: "n1", Labels: []string{"A", "B"}}
	assert.True(t, isTerminateNode(node, []string{"B"}))
	assert.False(t, isTerminateNode(node, []string{"C"}))
	assert.False(t, isTerminateNode(node, nil))
}

func TestCypherHelpers_CreateAndDropConstraintVariants(t *testing.T) {
	validCreate := []string{
		"CREATE CONSTRAINT c_nodekey IF NOT EXISTS FOR (n:Person) REQUIRE (n.id, n.tenant) IS NODE KEY",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:Person) REQUIRE (n.id, n.tenant) IS NODE KEY",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT (n.id, n.tenant) IS NODE KEY",
		"CREATE CONSTRAINT c_temporal IF NOT EXISTS FOR (n:Fact) REQUIRE (n.key, n.valid_from, n.valid_to) IS TEMPORAL",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:Fact) REQUIRE (n.key, n.valid_from, n.valid_to) IS TEMPORAL",
		"CREATE CONSTRAINT c_exists IF NOT EXISTS FOR (n:Person) REQUIRE n.name IS NOT NULL",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:Person) REQUIRE n.name IS NOT NULL",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT exists(n.name)",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT n.name IS NOT NULL",
		"CREATE CONSTRAINT c_type IF NOT EXISTS FOR (n:Person) REQUIRE n.age IS :: INTEGER",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:Person) REQUIRE n.age IS :: INTEGER",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT n.age IS :: INTEGER",
		"CREATE CONSTRAINT c_unique IF NOT EXISTS FOR (n:Person) REQUIRE n.email IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:Person) REQUIRE n.email IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT n.email IS UNIQUE",
	}

	for i, q := range validCreate {
		t.Run(fmt.Sprintf("create_variant_%d", i), func(t *testing.T) {
			base := newTestMemoryEngine(t)
			eng := storage.NewNamespacedEngine(base, "test")
			exec := NewStorageExecutor(eng)
			_, err := exec.executeCreateConstraint(context.Background(), q)
			require.NoError(t, err, q)
		})
	}

	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	_, err := exec.executeCreateConstraint(context.Background(), "CREATE CONSTRAINT bad syntax")
	require.Error(t, err)

	_, err = exec.executeCreateConstraint(context.Background(), "CREATE CONSTRAINT c_type_bad IF NOT EXISTS FOR (n:Person) REQUIRE n.age IS :: BOGUS")
	require.Error(t, err)

	// Drop existing constraint path.
	_, err = exec.executeCreateConstraint(context.Background(), "CREATE CONSTRAINT c_drop IF NOT EXISTS FOR (n:Person) REQUIRE n.email IS UNIQUE")
	require.NoError(t, err)
	_, err = exec.executeDropConstraint(context.Background(), "DROP CONSTRAINT c_drop")
	require.NoError(t, err)

	// IF EXISTS swallow path.
	_, err = exec.executeDropConstraint(context.Background(), "DROP CONSTRAINT c_missing IF EXISTS")
	require.NoError(t, err)

	// Invalid drop syntax.
	_, err = exec.executeDropConstraint(context.Background(), "DROP CONSTRAINT")
	require.Error(t, err)

	t.Run("constraint validation errors on existing data", func(t *testing.T) {
		base := newTestMemoryEngine(t)
		eng := storage.NewNamespacedEngine(base, "test")
		exec := NewStorageExecutor(eng)
		ctx := context.Background()

		// Duplicate values violate UNIQUE constraint creation.
		_, err := exec.Execute(ctx, "CREATE (:Person {email:'dup@example.com'})", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, "CREATE (:Person {email:'dup@example.com'})", nil)
		require.NoError(t, err)
		_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT c_dup IF NOT EXISTS FOR (n:Person) REQUIRE n.email IS UNIQUE")
		require.Error(t, err)

		// Wrong property type violates type constraint creation.
		_, err = exec.Execute(ctx, "CREATE (:Typed {age:'bad'})", nil)
		require.NoError(t, err)
		_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT c_type_err IF NOT EXISTS FOR (n:Typed) REQUIRE n.age IS :: INTEGER")
		require.Error(t, err)
	})

	t.Run("temporal constraint malformed property count", func(t *testing.T) {
		base := newTestMemoryEngine(t)
		eng := storage.NewNamespacedEngine(base, "test")
		exec := NewStorageExecutor(eng)

		_, err := exec.executeCreateConstraint(
			context.Background(),
			"CREATE CONSTRAINT c_temporal_bad IF NOT EXISTS FOR (n:Fact) REQUIRE (n.key, n.valid_from) IS TEMPORAL",
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "TEMPORAL constraint requires 3 properties")
	})
}

func TestCypherHelpers_CallPluginHandlerSignatures(t *testing.T) {
	type tc struct {
		name      string
		handler   interface{}
		args      []interface{}
		expect    interface{}
		expectErr bool
	}

	cases := []tc{
		{name: "nil", handler: nil, expectErr: true},
		{name: "noargs_interface", handler: func() interface{} { return "ok" }, expect: "ok"},
		{name: "noargs_string", handler: func() string { return "s" }, expect: "s"},
		{name: "noargs_int64", handler: func() int64 { return 7 }, expect: int64(7)},
		{name: "noargs_float64", handler: func() float64 { return 1.5 }, expect: 1.5},
		{name: "one_interface", handler: func(v interface{}) interface{} { return v }, args: []interface{}{"x"}, expect: "x"},
		{name: "one_list_interface", handler: func(v []interface{}) interface{} { return len(v) }, args: []interface{}{[]interface{}{1, 2}}, expect: 2},
		{name: "one_list_float", handler: func(v []interface{}) float64 { return float64(len(v)) }, args: []interface{}{[]interface{}{1, 2, 3}}, expect: 3.0},
		{name: "one_string", handler: func(s string) string { return s + "!" }, args: []interface{}{"a"}, expect: "a!"},
		{name: "one_float", handler: func(f float64) float64 { return f * 2 }, args: []interface{}{int64(3)}, expect: 6.0},
		{name: "one_float_slice", handler: func(v []float64) []float64 { return append(v, 9) }, args: []interface{}{[]interface{}{1.0, 2.0}}, expect: []float64{1, 2, 9}},
		{name: "two_interface", handler: func(a, b interface{}) interface{} { return fmt.Sprintf("%v-%v", a, b) }, args: []interface{}{"a", "b"}, expect: "a-b"},
		{name: "two_list_item", handler: func(v []interface{}, x interface{}) bool { return len(v) == 1 && x == "k" }, args: []interface{}{[]interface{}{1}, "k"}, expect: true},
		{name: "two_lists", handler: func(a, b []interface{}) []interface{} { return append(a, b...) }, args: []interface{}{[]interface{}{1}, []interface{}{2}}, expect: []interface{}{1, 2}},
		{name: "two_strings", handler: func(a, b string) string { return a + b }, args: []interface{}{"a", "b"}, expect: "ab"},
		{name: "two_strings_int", handler: func(a, b string) int { return len(a) + len(b) }, args: []interface{}{"a", "bb"}, expect: 3},
		{name: "two_strings_float", handler: func(a, b string) float64 { return float64(len(a) * len(b)) }, args: []interface{}{"aa", "bbb"}, expect: 6.0},
		{name: "two_float_slices", handler: func(a, b []float64) float64 { return a[0] + b[0] }, args: []interface{}{[]interface{}{1.0}, []interface{}{2.0}}, expect: 3.0},
		{name: "three_strings", handler: func(a, b, c string) string { return a + b + c }, args: []interface{}{"a", "b", "c"}, expect: "abc"},
		{name: "unsupported", handler: func(int) int { return 1 }, args: []interface{}{1}, expectErr: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := callPluginHandler(c.handler, c.args)
			if c.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, c.expect, got)
		})
	}
}

func TestCypherHelpers_ProcedureSpecValidation(t *testing.T) {
	err := validateProcedureSpec(ProcedureSpec{Name: "", MinArgs: 0, MaxArgs: 0})
	require.Error(t, err)

	err = validateProcedureSpec(ProcedureSpec{Name: "db.test", MinArgs: 2, MaxArgs: 1})
	require.Error(t, err)

	err = validateProcedureSpec(ProcedureSpec{Name: "db.test", MinArgs: 0, MaxArgs: 0})
	require.NoError(t, err)
}

func TestCypherHelpers_ExtractPolygonPoints_AllBranches(t *testing.T) {
	fromPoints := extractPolygonPoints(map[string]interface{}{
		"points": []interface{}{map[string]interface{}{"x": 1, "y": 2}},
	})
	require.Len(t, fromPoints, 1)

	fromCoordinates := extractPolygonPoints(map[string]interface{}{
		"coordinates": []interface{}{[]interface{}{1.0, 2.0}},
	})
	require.Len(t, fromCoordinates, 1)

	none := extractPolygonPoints(map[string]interface{}{"points": "bad"})
	assert.Nil(t, none)
}

func TestCypherHelpers_ParseRagProcedureRequest(t *testing.T) {
	base := newTestMemoryEngine(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "test"))
	ctx := context.Background()

	req, err := exec.parseRagProcedureRequest(ctx, "CALL db.retrieve({query:'alpha', limit: 5})", "DB.RETRIEVE")
	require.NoError(t, err)
	assert.Equal(t, "alpha", req["query"])

	req, err = exec.parseRagProcedureRequest(ctx, "CALL db.retrieve('alpha')", "DB.RETRIEVE")
	require.NoError(t, err)
	assert.Equal(t, "alpha", req["query"])

	req, err = exec.parseRagProcedureRequest(ctx, "CALL db.retrieve()", "DB.RETRIEVE")
	require.NoError(t, err)
	assert.Empty(t, req)

	ctxWithParams := context.WithValue(ctx, paramsKey, map[string]interface{}{
		"r": map[string]interface{}{"query": "beta", "limit": int64(2)},
	})
	req, err = exec.parseRagProcedureRequest(ctxWithParams, "CALL db.retrieve($r)", "DB.RETRIEVE")
	require.NoError(t, err)
	assert.Equal(t, "beta", req["query"])

	_, err = exec.parseRagProcedureRequest(ctx, "CALL db.retrieve($missing)", "DB.RETRIEVE")
	require.Error(t, err)
	_, err = exec.parseRagProcedureRequest(ctx, "CALL db.retrieve(123)", "DB.RETRIEVE")
	require.Error(t, err)
	_, err = exec.parseRagProcedureRequest(ctx, "CALL db.retrieve(", "DB.RETRIEVE")
	require.Error(t, err)
	_, err = exec.parseRagProcedureRequest(ctx, "CALL other.proc({})", "DB.RETRIEVE")
	require.Error(t, err)
}

func TestCypherHelpers_CallDbIndexVectorEmbed_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "test"))
	ctx := context.Background()

	_, err := exec.callDbIndexVectorEmbed(ctx, "CALL db.index.vector.embed('x')")
	require.Error(t, err)

	exec.SetEmbedder(&mockQueryEmbedder{embedding: []float32{1, 0, 0}})

	_, err = exec.callDbIndexVectorEmbed(ctx, "CALL db.index.vector.bad('x')")
	require.Error(t, err)
	_, err = exec.callDbIndexVectorEmbed(ctx, "CALL db.index.vector.embed")
	require.Error(t, err)
	_, err = exec.callDbIndexVectorEmbed(ctx, "CALL db.index.vector.embed(")
	require.Error(t, err)
	_, err = exec.callDbIndexVectorEmbed(ctx, "CALL db.index.vector.embed('   ')")
	require.Error(t, err)
	_, err = exec.callDbIndexVectorEmbed(ctx, "CALL db.index.vector.embed(123)")
	require.Error(t, err)
	_, err = exec.callDbIndexVectorEmbed(ctx, "CALL db.index.vector.embed($q)")
	require.Error(t, err)

	ctxWithBadParam := context.WithValue(ctx, paramsKey, map[string]interface{}{"q": 123})
	_, err = exec.callDbIndexVectorEmbed(ctxWithBadParam, "CALL db.index.vector.embed($q)")
	require.Error(t, err)

	ctxWithParam := context.WithValue(ctx, paramsKey, map[string]interface{}{"q": "hello"})
	res, err := exec.callDbIndexVectorEmbed(ctxWithParam, "CALL db.index.vector.embed($q)")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)

	res, err = exec.callDbIndexVectorEmbed(ctx, "CALL db.index.vector.embed('hello')")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
}

func TestCypherHelpers_SubstituteBoundVariablesInCall(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	node := &storage.Node{
		ID:              "n1",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{0.1, 0.2}},
		Properties: map[string]interface{}{
			"id":        "doc-1",
			"score":     float64(1.5),
			"intScore":  int64(7),
			"published": true,
			"embedding": []interface{}{1.0, 2.0, int64(3)},
		},
	}
	ctxNodes := map[string]*storage.Node{"n": node}

	// Regular property replacement.
	out := exec.substituteBoundVariablesInCall("CALL proc(n.id, n.score, n.intScore, n.published)", ctxNodes)
	assert.Contains(t, out, "'doc-1'")
	assert.Contains(t, out, "1.5")
	assert.Contains(t, out, "7")
	assert.Contains(t, out, "true")

	// Embedding from Properties (stored as regular property, no special routing).
	out = exec.substituteBoundVariablesInCall("CALL db.index.vector.queryNodes('idx', 5, n.embedding)", ctxNodes)
	assert.Contains(t, out, "[1, 2, 3]")

	// Embedding from []float64 property.
	nodeFloat64 := &storage.Node{
		ID:     "n2",
		Labels: []string{"Doc"},
		Properties: map[string]interface{}{
			"embedding": []float64{0.3, 0.4},
		},
	}
	out = exec.substituteBoundVariablesInCall(
		"CALL db.index.vector.queryNodes('idx', 5, n.embedding)",
		map[string]*storage.Node{"n": nodeFloat64},
	)
	assert.Contains(t, out, "[0.3, 0.4]")

	// Embedding from []interface{} property.
	nodeIface := &storage.Node{
		ID:     "n3",
		Labels: []string{"Doc"},
		Properties: map[string]interface{}{
			"embedding": []interface{}{1.0, int64(2), float32(3)},
		},
	}
	out = exec.substituteBoundVariablesInCall(
		"CALL db.index.vector.queryNodes('idx', 5, n.embedding)",
		map[string]*storage.Node{"n": nodeIface},
	)
	assert.Contains(t, out, "[1, 2, 3]")

	// Unknown variable/property should remain unchanged.
	orig := "CALL proc(missing.value)"
	out = exec.substituteBoundVariablesInCall(orig, ctxNodes)
	assert.Equal(t, orig, out)

	// Patterns inside quoted strings should not be replaced.
	inQuoted := "CALL proc('n.id', \"n.score\")"
	out = exec.substituteBoundVariablesInCall(inQuoted, ctxNodes)
	assert.Equal(t, inQuoted, out)
}

func TestCypherHelpers_PrefixNodeIDIfNeeded(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	assert.Equal(t, storage.NodeID("n1"), exec.prefixNodeIDIfNeeded("n1", ""))
	assert.Equal(t, storage.NodeID("tenant:n1"), exec.prefixNodeIDIfNeeded("n1", "tenant:"))
	assert.Equal(t, storage.NodeID("tenant:n1"), exec.prefixNodeIDIfNeeded("tenant:n1", "tenant:"))
}

func TestCypherHelpers_EvaluateWhereOnComputedRow(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	values := map[string]interface{}{
		"score": float64(9.5),
		"name":  "alice",
		"age":   int64(30),
	}

	assert.True(t, exec.evaluateWhereOnComputedRow("score >= 9 AND age = 30", values))
	assert.True(t, exec.evaluateWhereOnComputedRow("name = 'alice' OR age < 10", values))
	assert.True(t, exec.evaluateWhereOnComputedRow("age <> 99", values))
	assert.True(t, exec.evaluateWhereOnComputedRow("age != 99", values))
	assert.True(t, exec.evaluateWhereOnComputedRow("score > 9", values))
	assert.True(t, exec.evaluateWhereOnComputedRow("score < 10", values))
	assert.True(t, exec.evaluateWhereOnComputedRow("age <= 30", values))
	assert.False(t, exec.evaluateWhereOnComputedRow("score > 99", values))
	assert.True(t, exec.evaluateWhereOnComputedRow("unsupported-clause", values))
}

func TestCypherHelpers_EvaluateInnerWhereBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	node := &storage.Node{
		ID:     "n-123",
		Labels: []string{"Person"},
		Properties: map[string]interface{}{
			"name": "alice",
			"age":  int64(30),
			"bio":  "hello world",
			"tags": []interface{}{"a", "b"},
		},
	}

	assert.True(t, exec.evaluateInnerWhere(node, "n", "(n.age = 30)"))
	assert.True(t, exec.evaluateInnerWhere(node, "n", "n.age = 30 AND n.name = 'alice'"))
	assert.True(t, exec.evaluateInnerWhere(node, "n", "n.age = 99 OR n.name = 'alice'"))
	assert.True(t, exec.evaluateInnerWhere(node, "n", "NOT n.age = 99"))
	assert.True(t, exec.evaluateInnerWhere(node, "n", "n.bio CONTAINS 'hello'"))
	assert.True(t, exec.evaluateInnerWhere(node, "n", "n.name STARTS WITH 'ali'"))
	assert.True(t, exec.evaluateInnerWhere(node, "n", "n.name ENDS WITH 'ice'"))
	assert.True(t, exec.evaluateInnerWhere(node, "n", "'a' IN n.tags"))
	assert.True(t, exec.evaluateInnerWhere(node, "n", "n.name IS NOT NULL"))
	assert.False(t, exec.evaluateInnerWhere(node, "n", "n.missing IS NOT NULL"))
	assert.True(t, exec.evaluateInnerWhere(node, "n", "n.missing IS NULL"))
	assert.True(t, exec.evaluateInnerWhere(node, "n", "id(n) = 'n-123'"))
	assert.True(t, exec.evaluateInnerWhere(node, "n", "elementId(n) = 'n-123'"))
	assert.True(t, exec.evaluateInnerWhere(node, "n", "n.age >= 30"))
	assert.True(t, exec.evaluateInnerWhere(node, "n", "n.age <= 30"))
	assert.True(t, exec.evaluateInnerWhere(node, "n", "n.age > 29"))
	assert.True(t, exec.evaluateInnerWhere(node, "n", "n.age < 31"))
	assert.True(t, exec.evaluateInnerWhere(node, "n", "n.name =~ 'a.*'"))
	assert.False(t, exec.evaluateInnerWhere(node, "n", "n.age = 99"))
	assert.False(t, exec.evaluateInnerWhere(node, "n", "x.age = 30")) // wrong variable branch
	assert.True(t, exec.evaluateInnerWhere(node, "n", ""))            // empty where clause includes
	assert.False(t, exec.evaluateInnerWhere(node, "n", "MALFORMED"))  // malformed non-empty clause excludes
}

func TestCypherHelpers_NormalizePropValueAndMap(t *testing.T) {
	assert.Equal(t, int64(1), normalizePropValue(int(1)))
	assert.Equal(t, int64(2), normalizePropValue(int8(2)))
	assert.Equal(t, int64(3), normalizePropValue(int16(3)))
	assert.Equal(t, int64(4), normalizePropValue(int32(4)))
	assert.Equal(t, int64(5), normalizePropValue(int64(5)))
	assert.Equal(t, int64(6), normalizePropValue(uint(6)))
	assert.Equal(t, int64(7), normalizePropValue(uint8(7)))
	assert.Equal(t, int64(8), normalizePropValue(uint16(8)))
	assert.Equal(t, int64(9), normalizePropValue(uint32(9)))
	assert.Equal(t, int64(10), normalizePropValue(uint64(10)))
	assert.Equal(t, float64(1.25), normalizePropValue(float32(1.25)))

	overMax := uint64(math.MaxInt64) + 1
	_, isFloat := normalizePropValue(overMax).(float64)
	assert.True(t, isFloat)

	nested := normalizePropValue([]interface{}{int(1), map[string]interface{}{"x": uint8(2)}})
	require.Equal(t, []interface{}{int64(1), map[string]interface{}{"x": int64(2)}}, nested)

	props, err := normalizePropsMap(map[string]interface{}{"a": int(1), "b": float32(2)}, "$p")
	require.NoError(t, err)
	assert.Equal(t, int64(1), props["a"])
	assert.Equal(t, float64(2), props["b"])

	props, err = normalizePropsMap(map[interface{}]interface{}{"a": int8(1)}, "props")
	require.NoError(t, err)
	assert.Equal(t, int64(1), props["a"])

	_, err = normalizePropsMap(map[interface{}]interface{}{1: "bad"}, "props")
	require.Error(t, err)
	_, err = normalizePropsMap("not-map", "props")
	require.Error(t, err)
}

func TestCypherHelpers_FindNodeByVariableInMatch(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)

	_, err := eng.CreateNode(&storage.Node{ID: "id-node", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "other-node", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob"}})
	require.NoError(t, err)

	// ID pattern branch.
	n := exec.findNodeByVariableInMatch("MATCH (a:Person {id: 'id-node'}) RETURN a", "a")
	require.NotNil(t, n)
	assert.Equal(t, storage.NodeID("id-node"), n.ID)

	// Non-id pattern branch currently does not resolve a node from MATCH text.
	n = exec.findNodeByVariableInMatch("MATCH (a:Person) RETURN a", "a")
	assert.Nil(t, n)

	// No match branch.
	n = exec.findNodeByVariableInMatch("MATCH (x:Thing) RETURN x", "a")
	assert.Nil(t, n)
}

func TestCypherHelpers_ApocLouvainBasic(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)

	_, err := eng.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "a"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "n2", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "b"}})
	require.NoError(t, err)
	err = eng.CreateEdge(&storage.Edge{
		ID:         "e1",
		StartNode:  "n1",
		EndNode:    "n2",
		Type:       "LINK",
		Properties: map[string]interface{}{"weight": float64(2)},
	})
	require.NoError(t, err)

	// Basic call with explicit label filter.
	res, err := exec.callApocAlgoLouvain(context.Background(), "CALL apoc.algo.louvain(['Node']) YIELD node, community")
	require.NoError(t, err)
	assert.Equal(t, []string{"node", "community"}, res.Columns)

	// weightProperty parsing branch.
	res, err = exec.callApocAlgoLouvain(context.Background(), "CALL apoc.algo.louvain(['Node'], {weightProperty: 'weight'}) YIELD node, community")
	require.NoError(t, err)
	assert.Equal(t, []string{"node", "community"}, res.Columns)
}

func TestCypherHelpers_ParseFulltextQueryAndShowDatabase(t *testing.T) {
	terms, excludes, must := parseFulltextQuery(`alpha AND beta OR "exact phrase" +must -drop NOT skip`)
	assert.Contains(t, terms, "alpha")
	assert.Contains(t, terms, "beta")
	assert.Contains(t, excludes, "drop")
	assert.Contains(t, excludes, "skip")
	assert.Contains(t, must, "exact phrase")
	assert.Contains(t, must, "must")

	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "tenant_show")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{ID: "n1", Labels: []string{"X"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	err = eng.CreateEdge(&storage.Edge{ID: "e1", StartNode: "n1", EndNode: "n1", Type: "LOOP"})
	require.NoError(t, err)

	// Without dbManager, default fallback branch.
	res, err := exec.executeShowDatabase(ctx, "SHOW DATABASE")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, "nornic", res.Rows[0][0])
	assert.Equal(t, 1, res.Stats.NodesCreated)
	assert.Equal(t, 1, res.Stats.RelationshipsCreated)

	// :USE context branch should override namespace.
	ctxUse := context.WithValue(ctx, ctxKeyUseDatabase, "ctx_db")
	res, err = exec.executeShowDatabase(ctxUse, "SHOW DATABASE")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, "ctx_db", res.Rows[0][0])
}

func TestCypherHelpers_EvaluateSetExpressionAndArraySuffix(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	assert.Nil(t, exec.evaluateSetExpression("null"))
	assert.Equal(t, "x", exec.evaluateSetExpression("'x'"))
	assert.Equal(t, "y", exec.evaluateSetExpression("\"y\""))
	assert.Equal(t, true, exec.evaluateSetExpression("true"))
	assert.Equal(t, false, exec.evaluateSetExpression("false"))
	assert.Equal(t, int64(42), exec.evaluateSetExpression("42"))
	assert.Equal(t, float64(3.5), exec.evaluateSetExpression("3.5"))
	assert.Equal(t, []interface{}{}, exec.evaluateSetExpression("[]"))
	assert.Equal(t, []interface{}{int64(1), "a", true}, exec.evaluateSetExpression("[1, 'a', true]"))
	assert.IsType(t, int64(0), exec.evaluateSetExpression("timestamp()"))
	assert.IsType(t, "", exec.evaluateSetExpression("datetime()"))
	assert.IsType(t, "", exec.evaluateSetExpression("randomUUID()"))

	collected := []interface{}{int64(1), int64(2), int64(3), int64(4)}
	assert.Equal(t, collected, exec.applyArraySuffix(collected, ""))
	assert.Equal(t, collected, exec.applyArraySuffix(collected, "bad"))
	assert.Equal(t, []interface{}{int64(1), int64(2)}, exec.applyArraySuffix(collected, "[..2]"))
	assert.Equal(t, []interface{}{int64(2), int64(3)}, exec.applyArraySuffix(collected, "[1..3]"))
	assert.Equal(t, []interface{}{int64(3), int64(4)}, exec.applyArraySuffix(collected, "[-2..]"))
	assert.Equal(t, []interface{}{}, exec.applyArraySuffix(collected, "[3..1]"))
	assert.Equal(t, int64(2), exec.applyArraySuffix(collected, "[1]"))
	assert.Equal(t, int64(4), exec.applyArraySuffix(collected, "[-1]"))
	assert.Nil(t, exec.applyArraySuffix(collected, "[99]"))
}

func TestCypherHelpers_AssignValueAdditionalCases(t *testing.T) {
	// time conversion branches
	var ts time.Time
	err := assignValue(reflect.ValueOf(&ts).Elem(), int64(1700000000))
	require.NoError(t, err)
	assert.Equal(t, int64(1700000000), ts.Unix())

	var ts2 time.Time
	err = assignValue(reflect.ValueOf(&ts2).Elem(), "2024-01-02 03:04:05")
	require.NoError(t, err)
	assert.Equal(t, 2024, ts2.Year())

	// bool conversion branches
	var b bool
	err = assignValue(reflect.ValueOf(&b).Elem(), int(1))
	require.NoError(t, err)
	assert.True(t, b)
	err = assignValue(reflect.ValueOf(&b).Elem(), int64(0))
	require.NoError(t, err)
	assert.False(t, b)

	// slice conversion branch
	var ints []int64
	err = assignValue(reflect.ValueOf(&ints).Elem(), []interface{}{float64(1), int64(2)})
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2}, ints)

	// unsupported assignment branch
	var dst struct{ A int }
	err = assignValue(reflect.ValueOf(&dst).Elem(), "bad")
	require.Error(t, err)
}

func TestCypherHelpers_ModuloAndMapLiteralFullBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	// modulo: integer coercion, divide-by-zero, and invalid operand branches.
	assert.Equal(t, int64(1), exec.modulo(int64(10), int64(3)))
	assert.Nil(t, exec.modulo(int64(10), int64(0)))
	assert.Nil(t, exec.modulo("bad", int64(2)))

	nodes := map[string]*storage.Node{
		"n": {ID: "n1", Properties: map[string]interface{}{"name": "alice", "age": int64(30)}},
	}
	rels := map[string]*storage.Edge{}

	// map-literal parser should normalize quoted/backticked keys and evaluate known values.
	out := exec.evaluateMapLiteralFull("{`k1`: n.name, 'k2': 3, \"k3\": true, badPair}", nodes, rels, nil, nil, nil, 0)
	assert.Equal(t, "alice", out["k1"])
	assert.Equal(t, int64(3), out["k2"])
	assert.Equal(t, true, out["k3"])

	// Non-map shapes return deterministic empty map.
	assert.Empty(t, exec.evaluateMapLiteralFull("not-a-map", nodes, rels, nil, nil, nil, 0))
}

func TestCypherHelpers_QueryPatternAndFastPathHelpers(t *testing.T) {
	// extractRelationshipType branches.
	assert.Equal(t, "KNOWS", extractRelationshipType("(a)-[:KNOWS]-(b)"))
	assert.Equal(t, "RATED", extractRelationshipType("(a)-[r:RATED]-(b)"))
	assert.Equal(t, "", extractRelationshipType("(a)-->(b)"))

	// isReturnEdgePropertyAggNameShape strictness.
	assert.True(t, isReturnEdgePropertyAggNameShape(
		"MATCH (c)-[r:REVIEWED]->(p) RETURN p.name AS name, avg(r.rating) AS score, count(r) AS cnt",
		"r",
		"rating",
	))
	assert.False(t, isReturnEdgePropertyAggNameShape(
		"MATCH (c)-[r:REVIEWED]->(p) RETURN p.id AS id, avg(r.rating) AS score",
		"r",
		"rating",
	))
	assert.False(t, isReturnEdgePropertyAggNameShape(
		"MATCH (c)-[r:REVIEWED]->(p) RETURN p.name AS name, avg(r.other) AS score",
		"r",
		"rating",
	))
	assert.False(t, isReturnEdgePropertyAggNameShape("RETURN p.name", "r", "rating"))

	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "tenant_fast_cov")
	exec := NewStorageExecutor(ns)

	_, err := ns.CreateNode(&storage.Node{ID: "n1", Labels: []string{"X"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	_, err = ns.CreateNode(&storage.Node{ID: "n2", Labels: []string{"X"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	require.NoError(t, ns.CreateEdge(&storage.Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "REL"}))

	// getEdgesByTypeFast: namespace filter branch.
	edges, prefix, err := exec.getEdgesByTypeFast("REL")
	require.NoError(t, err)
	assert.Equal(t, "tenant_fast_cov:", prefix)
	for _, e := range edges {
		assert.True(t, strings.HasPrefix(string(e.ID), prefix))
	}

	// batchGetNodesFast happy path.
	nodes, gotPrefix, err := exec.batchGetNodesFast([]storage.NodeID{"tenant_fast_cov:n1"})
	require.NoError(t, err)
	assert.Equal(t, "tenant_fast_cov:", gotPrefix)
	require.NotNil(t, nodes["tenant_fast_cov:n1"])

	// error branches from underlying engine.
	failExec := &StorageExecutor{
		storage: &fastPathFailEngine{
			Engine:   base,
			batchErr: fmt.Errorf("batch fail"),
			edgesErr: fmt.Errorf("edges fail"),
		},
		analyzer: NewQueryAnalyzer(64),
	}
	_, _, err = failExec.batchGetNodesFast([]storage.NodeID{"n1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "batch fail")
	_, _, err = failExec.getEdgesByTypeFast("REL")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "edges fail")
}

func TestExtractMatchOrderByClause_IgnoresOrderLabelsAndRelationshipTypes(t *testing.T) {
	query := `
		MATCH (c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)<-[:SUPPLIES]-(s:Supplier)
		RETURN c.companyName, s.companyName, count(DISTINCT o) as orders
		ORDER BY orders DESC
		LIMIT 10
	`
	returnIdx := findKeywordNotInBrackets(strings.ToUpper(query), " RETURN ")
	if returnIdx < 0 {
		returnIdx = findKeywordIndex(query, "RETURN")
	}
	require.Equal(t, "orders DESC", extractMatchOrderByClause(query, returnIdx))
	queryNoOrder := `MATCH (o:Order)-[:ORDERS]->(p:Product) RETURN p`
	returnIdx = findKeywordIndex(queryNoOrder, "RETURN")
	require.Equal(t, "", extractMatchOrderByClause(queryNoOrder, returnIdx))
}

func TestCypherHelpers_ExecuteCreateConstraint_TypeAndErrorBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	// Named Neo4j 5 style property type constraint.
	_, err := exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT person_age_type IF NOT EXISTS FOR (n:Person) REQUIRE n.age IS :: INTEGER")
	require.NoError(t, err)

	// Unsupported type should error deterministically.
	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT person_bad_type IF NOT EXISTS FOR (n:Person) REQUIRE n.age IS :: UUID")
	require.Error(t, err)
	assert.Contains(t, strings.ToUpper(err.Error()), "UNSUPPORTED PROPERTY TYPE")

	// Completely malformed constraint command should error.
	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT nonsense")
	require.Error(t, err)
}

func TestCypherHelpers_UnregisterVectorSpace_StandaloneBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "vreg_cov"))
	exec.SetVectorRegistry(vectorspace.NewIndexRegistry())

	// Non-existing key with non-nil map.
	exec.vectorIndexSpaces = map[string]vectorspace.VectorSpaceKey{}
	exec.unregisterVectorSpace("missing")

	// Existing key removal branch.
	k := vectorspace.VectorSpaceKey{
		DB:         "vreg_cov",
		Type:       "doc",
		VectorName: "embedding",
		Dims:       3,
		Distance:   vectorspace.DistanceCosine,
	}
	canonical, err := k.Canonical()
	require.NoError(t, err)
	_, err = exec.vectorRegistry.CreateSpace(canonical, vectorspace.BackendAuto)
	require.NoError(t, err)
	exec.vectorIndexSpaces["idx1"] = canonical
	exec.unregisterVectorSpace("idx1")
	_, ok := exec.vectorIndexSpaces["idx1"]
	assert.False(t, ok)
	_, exists := exec.vectorRegistry.GetSpace(canonical)
	assert.False(t, exists)

	// Nil registry early-return branch.
	exec.vectorRegistry = nil
	exec.unregisterVectorSpace("idx1")
}

func TestCypherHelpers_ExecuteCreateConstraint_MultiSyntaxCoverage(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	valid := []string{
		"CREATE CONSTRAINT c_named_unique IF NOT EXISTS FOR (n:Person) REQUIRE n.email IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:Person) REQUIRE n.username IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT n.legacy IS UNIQUE",
		"CREATE CONSTRAINT c_named_notnull IF NOT EXISTS FOR (n:Person) REQUIRE n.name IS NOT NULL",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:Person) REQUIRE n.display IS NOT NULL",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT exists(n.bio)",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT n.alias IS NOT NULL",
		"CREATE CONSTRAINT c_named_nodekey IF NOT EXISTS FOR (n:Person) REQUIRE (n.tenant, n.external) IS NODE KEY",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:Person) REQUIRE (n.region, n.account) IS NODE KEY",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT (n.org, n.localId) IS NODE KEY",
		"CREATE CONSTRAINT c_named_temporal IF NOT EXISTS FOR (n:Versioned) REQUIRE (n.key, n.valid_from, n.valid_to) IS TEMPORAL NO OVERLAP",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:Versioned) REQUIRE (n.key, n.valid_from, n.valid_to) IS TEMPORAL",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:Person) REQUIRE n.score IS TYPED FLOAT",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT n.age IS :: INTEGER",
	}
	for _, q := range valid {
		_, err := exec.executeCreateConstraint(ctx, q)
		require.NoError(t, err, q)
	}

	// Temporal constraint requires exactly 3 properties.
	_, err := exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT bad_temporal FOR (n:Versioned) REQUIRE (n.key, n.valid_from) IS TEMPORAL")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TEMPORAL constraint requires 3 properties")
}

func TestCypherHelpers_ExecuteCreateConstraint_ValidationFailureBranches(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Duplicate unique value should fail validation on CREATE CONSTRAINT ... IS UNIQUE.
	_, err := store.CreateNode(&storage.Node{
		ID:         "u1",
		Labels:     []string{"User"},
		Properties: map[string]interface{}{"email": "dup@example.com"},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:         "u2",
		Labels:     []string{"User"},
		Properties: map[string]interface{}{"email": "dup@example.com"},
	})
	require.NoError(t, err)
	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT uq_user_email FOR (n:User) REQUIRE n.email IS UNIQUE")
	require.Error(t, err)

	// Existing invalid type should fail property-type validation.
	_, err = store.CreateNode(&storage.Node{
		ID:         "p1",
		Labels:     []string{"Profile"},
		Properties: map[string]interface{}{"age": "not-an-int"},
	})
	require.NoError(t, err)
	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT profile_age_type FOR (n:Profile) REQUIRE n.age IS :: INTEGER")
	require.Error(t, err)
}

func TestCypherHelpers_MatchRowsHelpers_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	nodes := []*storage.Node{
		{ID: "n1", Labels: []string{"X"}, Properties: map[string]interface{}{"a": int64(3), "b": int64(4)}},
		{ID: "n2", Labels: []string{"X"}, Properties: map[string]interface{}{"a": float64(1.5), "b": int64(2)}},
	}

	// SUM arithmetic branches: mixed SUM terms, numeric literal, unary negative.
	assert.Equal(t, 10.5, exec.evaluateSumArithmetic("SUM(n.a) + SUM(n.b)", nodes, "n"))
	assert.Equal(t, 2.5, exec.evaluateSumArithmetic("SUM(n.a) + 4 - SUM(n.b)", nodes, "n"))
	assert.Equal(t, -2.0, exec.evaluateSumArithmetic("-2", nodes, "n"))
	assert.Equal(t, 0.0, exec.evaluateSumArithmetic("SUM(n.missing)", nodes, "n"))

	// filterNodesByWhereClause branches.
	all := exec.filterNodesByWhereClause(nodes, "", "n")
	require.Len(t, all, 2)
	filtered := exec.filterNodesByWhereClause(nodes, "n.a >= 3", "n")
	require.Len(t, filtered, 1)
	assert.Equal(t, storage.NodeID("n1"), filtered[0].ID)
}

func TestCypherHelpers_EvaluateMapLiteralFromValues_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	values := map[string]interface{}{
		"name": "alice",
		"age":  int64(30),
		"rels": []interface{}{
			map[string]interface{}{"type": "KNOWS"},
			map[string]interface{}{"type": "WORKS_WITH"},
		},
	}

	// Normal map literal with value substitution and list-comprehension transform.
	out := exec.evaluateMapLiteralFromValues("{person: name, age: age, relTypes: [r IN rels | type(r)]}", values)
	require.Equal(t, "alice", out["person"])
	require.Equal(t, int64(30), out["age"])
	require.Equal(t, []interface{}{"KNOWS", "WORKS_WITH"}, out["relTypes"])

	// Invalid/non-map/empty shapes are deterministic empties.
	assert.Empty(t, exec.evaluateMapLiteralFromValues("not-a-map", values))
	assert.Empty(t, exec.evaluateMapLiteralFromValues("{}", values))
	// Pair without colon should be skipped, not panic.
	out = exec.evaluateMapLiteralFromValues("{good: age, badpair}", values)
	require.Equal(t, int64(30), out["good"])
}

func TestCypherHelpers_KalmanPredict_Branches(t *testing.T) {
	state := KalmanState{
		X:     10.0,
		LastX: 8.0,
		P:     1.0,
		Q:     0.01,
		R:     0.1,
		K:     0.5,
	}
	blob, err := json.Marshal(state)
	require.NoError(t, err)

	// velocity = 2; predict 3 steps => 16
	assert.Equal(t, 16.0, kalmanPredict(string(blob), 3))
	// invalid state JSON branch
	assert.Equal(t, 0.0, kalmanPredict("{bad-json", 5))
}

func TestCypherHelpers_CompareValuesForSort(t *testing.T) {
	assert.Equal(t, 0, compareValuesForSort(nil, nil))
	assert.Equal(t, -1, compareValuesForSort(nil, 1))
	assert.Equal(t, 1, compareValuesForSort(1, nil))
	assert.Equal(t, -1, compareValuesForSort(1, 2))
	assert.Equal(t, 1, compareValuesForSort(2, 1))
	assert.Equal(t, 0, compareValuesForSort(2, 2))
	assert.Equal(t, -1, compareValuesForSort(int64(1), int64(2)))
	assert.Equal(t, 0, compareValuesForSort(float64(1.5), float64(1.5)))
	assert.Equal(t, -1, compareValuesForSort("a", "b"))
	assert.Equal(t, 1, compareValuesForSort("b", "a"))
	assert.Equal(t, -1, compareValuesForSort(struct{ X int }{1}, struct{ X int }{2}))
}

func TestCypherHelpers_SubstituteNodeAndExecuteSetMerge(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	node := &storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}}
	_, err := eng.CreateNode(node)
	require.NoError(t, err)

	// substituteNodeInSubquery
	sub := exec.substituteNodeInSubquery("MATCH (n)-[:KNOWS]->(m) RETURN n.name", "n", node)
	assert.Contains(t, sub, "(n1)-[:KNOWS]->")
	sub = exec.substituteNodeInSubquery("MATCH (n:Person)-[:KNOWS]->(m) RETURN n.name", "n", node)
	assert.Contains(t, sub, "(n1:Person)-[:KNOWS]->")

	matchResult := &ExecuteResult{
		Columns: []string{"n", "props"},
		Rows:    [][]interface{}{{node, map[string]interface{}{"city": "NYC"}}},
	}
	out := &ExecuteResult{Stats: &QueryStats{}}
	_, err = exec.executeSetMerge(ctx, matchResult, "n += {country: 'US'}", out, "", -1)
	require.NoError(t, err)
	assert.Equal(t, "US", node.Properties["country"])

	// map variable path
	out = &ExecuteResult{Stats: &QueryStats{}}
	_, err = exec.executeSetMerge(ctx, matchResult, "n += props", out, "", -1)
	require.NoError(t, err)
	assert.Equal(t, "NYC", node.Properties["city"])

	// parameter map path
	ctxWithParams := context.WithValue(ctx, paramsKey, map[string]interface{}{"p": map[string]interface{}{"age": int(41)}})
	out = &ExecuteResult{Stats: &QueryStats{}}
	_, err = exec.executeSetMerge(ctxWithParams, matchResult, "n += $p", out, "", -1)
	require.NoError(t, err)
	assert.Equal(t, int64(41), node.Properties["age"])

	// Error branches.
	_, err = exec.executeSetMerge(ctx, matchResult, "n = {x:1}", &ExecuteResult{Stats: &QueryStats{}}, "", -1)
	require.Error(t, err)
	_, err = exec.executeSetMerge(ctx, matchResult, "n += $", &ExecuteResult{Stats: &QueryStats{}}, "", -1)
	require.Error(t, err)
	_, err = exec.executeSetMerge(ctx, matchResult, "n += $missing", &ExecuteResult{Stats: &QueryStats{}}, "", -1)
	require.Error(t, err)
	_, err = exec.executeSetMerge(ctx, &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{node}}}, "n += props", &ExecuteResult{Stats: &QueryStats{}}, "", -1)
	require.Error(t, err)

	// normalizePropsMap / normalizePropValue branches
	props, err := normalizePropsMap(map[interface{}]interface{}{"a": int(1), "b": uint8(2), "c": float32(3.5), "d": []interface{}{int8(1), uint16(2)}}, "var props")
	require.NoError(t, err)
	assert.Equal(t, int64(1), props["a"])
	assert.Equal(t, int64(2), props["b"])
	assert.Equal(t, float64(3.5), props["c"])
	_, err = normalizePropsMap(map[interface{}]interface{}{1: "x"}, "bad")
	require.Error(t, err)
	_, err = normalizePropsMap("not-map", "bad")
	require.Error(t, err)
}

func TestCypherHelpers_TypedResultDecodeAndAssignBranches(t *testing.T) {
	// decodeRow pointer validation.
	var scalar int
	err := decodeRow([]string{"x"}, []interface{}{1}, scalar)
	require.Error(t, err)
	err = decodeRow([]string{"x"}, []interface{}{1}, (*int)(nil))
	require.Error(t, err)

	// scalar assignment path.
	err = decodeRow([]string{"x"}, []interface{}{int64(7)}, &scalar)
	require.NoError(t, err)
	assert.Equal(t, 7, scalar)

	// unsupported destination type branch.
	var unsupported map[string]int
	err = decodeRow([]string{"x", "y"}, []interface{}{1, 2}, &unsupported)
	require.Error(t, err)

	// decodeMap and decodeStruct error propagation branches.
	type badTimeStruct struct {
		When time.Time `cypher:"when"`
	}
	var bad badTimeStruct
	err = decodeMap(map[string]interface{}{"when": "not-a-time"}, reflect.ValueOf(&bad).Elem())
	require.Error(t, err)

	type badIntStruct struct {
		Age int `cypher:"age"`
	}
	var badInt badIntStruct
	err = decodeStruct([]string{"age"}, []interface{}{"not-int"}, reflect.ValueOf(&badInt).Elem())
	require.Error(t, err)

	// assignValue additional conversion branches.
	var s string
	err = assignValue(reflect.ValueOf(&s).Elem(), 123)
	require.NoError(t, err)
	assert.Equal(t, "{", s)
	err = assignValue(reflect.ValueOf(&s).Elem(), struct{ V int }{V: 9})
	require.NoError(t, err)
	assert.Contains(t, s, "9")

	var b bool
	err = assignValue(reflect.ValueOf(&b).Elem(), true)
	require.NoError(t, err)
	assert.True(t, b)

	var f32 float32
	err = assignValue(reflect.ValueOf(&f32).Elem(), int64(9))
	require.NoError(t, err)
	assert.Equal(t, float32(9), f32)

	var i8 int8
	err = assignValue(reflect.ValueOf(&i8).Elem(), float32(4.0))
	require.NoError(t, err)
	assert.Equal(t, int8(4), i8)

	var times []time.Time
	err = assignValue(reflect.ValueOf(&times).Elem(), []interface{}{"2024-01-01T00:00:00Z"})
	require.NoError(t, err)
	require.Len(t, times, 1)
	assert.Equal(t, 2024, times[0].Year())

	var tm time.Time
	err = assignValue(reflect.ValueOf(&tm).Elem(), "not-a-time")
	require.Error(t, err)
}

func TestCypherHelpers_CallEvaluationHelpers(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	node := &storage.Node{
		ID:         "n1",
		Labels:     []string{"Doc"},
		Properties: map[string]interface{}{"name": "alice", "id": "user-1"},
	}
	ctxVals := map[string]interface{}{
		"score": 0.9,
		"n":     node,
		"m": map[string]interface{}{
			"name": "bob",
			"properties": map[string]interface{}{
				"city": "NYC",
			},
		},
	}

	assert.Equal(t, 0.9, exec.evaluateReturnExprInContext("score", ctxVals))
	assert.Equal(t, "alice", exec.evaluateReturnExprInContext("n.name", ctxVals))
	assert.Equal(t, "user-1", exec.evaluateReturnExprInContext("n.id", ctxVals))
	assert.Equal(t, "bob", exec.evaluateReturnExprInContext("m.name", ctxVals))
	assert.Equal(t, "NYC", exec.evaluateReturnExprInContext("m.city", ctxVals))
	assert.Nil(t, exec.evaluateReturnExprInContext("missing.field", ctxVals))

	ok, err := exec.evaluateYieldWhere("", map[string]interface{}{"x": 1})
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = exec.evaluateYieldWhere("n.value > 0", map[string]interface{}{"n": map[string]interface{}{"value": int64(2)}})
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = exec.evaluateYieldWhere("x.value > 1", map[string]interface{}{"x": int64(0)})
	require.NoError(t, err)
	assert.False(t, ok)

	ok, err = exec.evaluateYieldWhere("1 + 1", map[string]interface{}{})
	require.Error(t, err)
	assert.False(t, ok)
}

func TestCypherHelpers_VectorParsingAndQueryBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	_, _, _, err := exec.parseVectorQueryParams("CALL db.labels()")
	require.Error(t, err)
	_, _, _, err = exec.parseVectorQueryParams("CALL db.index.vector.queryNodes")
	require.Error(t, err)
	_, _, _, err = exec.parseVectorQueryParams("CALL db.index.vector.queryNodes('idx', 2, [1,2]")
	require.Error(t, err)

	indexName, k, input, err := exec.parseVectorQueryParams("CALL db.index.vector.queryNodes('idx', 5, [1.0, 2.5])")
	require.NoError(t, err)
	assert.Equal(t, "idx", indexName)
	assert.Equal(t, 5, k)
	assert.Equal(t, []float32{1.0, 2.5}, input.vector)

	_, _, input, err = exec.parseVectorQueryParams("CALL db.index.vector.queryNodes('idx', 3, 'hello')")
	require.NoError(t, err)
	assert.Equal(t, "hello", input.stringQuery)

	_, _, input, err = exec.parseVectorQueryParams("CALL db.index.vector.queryRelationships('idx', 4, $q)")
	require.NoError(t, err)
	assert.Equal(t, "q", input.paramName)

	parts := splitParamsCarefully(`'idx', 5, "a,b", [1,2,{x:3}]`)
	require.Len(t, parts, 4)
	assert.Equal(t, "\"a,b\"", strings.TrimSpace(parts[2]))
	assert.Equal(t, []float32{1.5, 2}, parseInlineVector("[1.5, bad, 2]"))

	// String query branch without embedder should error.
	_, err = exec.callDbIndexVectorQueryNodes(ctx, "CALL db.index.vector.queryNodes('idx', 2, 'search text')")
	require.Error(t, err)

	// Parameter branch with no params should return empty result.
	res, err := exec.callDbIndexVectorQueryNodes(ctx, "CALL db.index.vector.queryNodes('idx', 2, $query)")
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Empty(t, res.Rows)

	// Parameter missing in map should error.
	ctxMissing := context.WithValue(ctx, paramsKey, map[string]interface{}{"other": []float64{1, 2}})
	_, err = exec.callDbIndexVectorQueryNodes(ctxMissing, "CALL db.index.vector.queryNodes('idx', 2, $query)")
	require.Error(t, err)

	// Unsupported parameter type should error.
	ctxBadType := context.WithValue(ctx, paramsKey, map[string]interface{}{"query": true})
	_, err = exec.callDbIndexVectorQueryNodes(ctxBadType, "CALL db.index.vector.queryNodes('idx', 2, $query)")
	require.Error(t, err)

	// []interface{} with non-numeric value should error.
	ctxNonNumeric := context.WithValue(ctx, paramsKey, map[string]interface{}{"query": []interface{}{1, "x"}})
	_, err = exec.callDbIndexVectorQueryNodes(ctxNonNumeric, "CALL db.index.vector.queryNodes('idx', 2, $query)")
	require.Error(t, err)

	// No recognized input plus unsupported params path.
	ctxUnsupported := context.WithValue(ctx, paramsKey, map[string]interface{}{"bad": true})
	_, err = exec.callDbIndexVectorQueryNodes(ctxUnsupported, "CALL db.index.vector.queryNodes('idx', 2, 123)")
	require.Error(t, err)
}

func TestCypherHelpers_VectorAndFulltextRelationshipQueryBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	// Fulltext relationships: empty query and match query branches.
	res, err := exec.callDbIndexFulltextQueryRelationships("CALL db.index.fulltext.queryRelationships('idx', '')")
	require.NoError(t, err)
	require.Equal(t, []string{"relationship", "score"}, res.Columns)

	// Populate one relationship with searchable property.
	_, err = exec.storage.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "a"}})
	require.NoError(t, err)
	_, err = exec.storage.CreateNode(&storage.Node{ID: "b", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "b"}})
	require.NoError(t, err)
	err = exec.storage.CreateEdge(&storage.Edge{
		ID:        "r1",
		StartNode: "a",
		EndNode:   "b",
		Type:      "LINKS",
		Properties: map[string]interface{}{
			"text": "hello world",
		},
	})
	require.NoError(t, err)
	res, err = exec.callDbIndexFulltextQueryRelationships("CALL db.index.fulltext.queryRelationships('idx', 'hello')")
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)

	// Vector relationships: same error/param branches as node vector query.
	_, err = exec.callDbIndexVectorQueryRelationships(ctx, "CALL db.index.vector.queryRelationships('idx', 2, 'search text')")
	require.Error(t, err)

	res, err = exec.callDbIndexVectorQueryRelationships(ctx, "CALL db.index.vector.queryRelationships('idx', 2, $query)")
	require.NoError(t, err)
	assert.Empty(t, res.Rows)

	ctxMissing := context.WithValue(ctx, paramsKey, map[string]interface{}{"other": []float64{1, 2}})
	_, err = exec.callDbIndexVectorQueryRelationships(ctxMissing, "CALL db.index.vector.queryRelationships('idx', 2, $query)")
	require.Error(t, err)

	ctxBadType := context.WithValue(ctx, paramsKey, map[string]interface{}{"query": true})
	_, err = exec.callDbIndexVectorQueryRelationships(ctxBadType, "CALL db.index.vector.queryRelationships('idx', 2, $query)")
	require.Error(t, err)

	ctxNonNumeric := context.WithValue(ctx, paramsKey, map[string]interface{}{"query": []interface{}{1, "x"}})
	_, err = exec.callDbIndexVectorQueryRelationships(ctxNonNumeric, "CALL db.index.vector.queryRelationships('idx', 2, $query)")
	require.Error(t, err)

	ctxUnsupported := context.WithValue(ctx, paramsKey, map[string]interface{}{"bad": true})
	_, err = exec.callDbIndexVectorQueryRelationships(ctxUnsupported, "CALL db.index.vector.queryRelationships('idx', 2, 123)")
	require.Error(t, err)
}

func TestCypherHelpers_VectorRelationshipQuery_AdditionalBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	// Setup small graph with relationship embeddings.
	_, err := exec.storage.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "a"}})
	require.NoError(t, err)
	_, err = exec.storage.CreateNode(&storage.Node{ID: "b", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "b"}})
	require.NoError(t, err)
	_, err = exec.storage.CreateNode(&storage.Node{ID: "c", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "c"}})
	require.NoError(t, err)
	require.NoError(t, exec.storage.CreateEdge(&storage.Edge{
		ID:        "r1",
		StartNode: "a",
		EndNode:   "b",
		Type:      "LINKS",
		Properties: map[string]interface{}{
			"emb": []float32{1, 0},
		},
	}))
	require.NoError(t, exec.storage.CreateEdge(&storage.Edge{
		ID:        "r2",
		StartNode: "a",
		EndNode:   "c",
		Type:      "LINKS",
		Properties: map[string]interface{}{
			"emb": []float64{0, 1},
		},
	}))
	// Different type should be filtered by index relType.
	require.NoError(t, exec.storage.CreateEdge(&storage.Edge{
		ID:        "r3",
		StartNode: "b",
		EndNode:   "c",
		Type:      "OTHER",
		Properties: map[string]interface{}{
			"emb": []float32{1, 0},
		},
	}))
	// Dimension mismatch edge should be skipped.
	require.NoError(t, exec.storage.CreateEdge(&storage.Edge{
		ID:        "r4",
		StartNode: "c",
		EndNode:   "a",
		Type:      "LINKS",
		Properties: map[string]interface{}{
			"emb": []float32{1, 0, 0},
		},
	}))

	schema := exec.storage.GetSchema()
	require.NoError(t, schema.AddVectorIndex("idx_euclid", "LINKS", "emb", 2, "euclidean"))
	require.NoError(t, schema.AddVectorIndex("idx_dot", "LINKS", "emb", 2, "dot"))

	// Euclidean branch + k limiting branch.
	res, err := exec.callDbIndexVectorQueryRelationships(ctx, "CALL db.index.vector.queryRelationships('idx_euclid', 1, [1,0])")
	require.NoError(t, err)
	require.Equal(t, []string{"relationship", "score"}, res.Columns)
	require.Len(t, res.Rows, 1)

	// Dot branch.
	res, err = exec.callDbIndexVectorQueryRelationships(ctx, "CALL db.index.vector.queryRelationships('idx_dot', 5, [1,0])")
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)

	// Parameter []float64 branch.
	ctxF64 := context.WithValue(ctx, paramsKey, map[string]interface{}{"q": []float64{1, 0}})
	res, err = exec.callDbIndexVectorQueryRelationships(ctxF64, "CALL db.index.vector.queryRelationships('idx_dot', 5, $q)")
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)

	// String parameter with embedder branch.
	exec.SetEmbedder(&mockQueryEmbedder{embedding: []float32{1, 0}})
	ctxString := context.WithValue(ctx, paramsKey, map[string]interface{}{"q": "hello"})
	res, err = exec.callDbIndexVectorQueryRelationships(ctxString, "CALL db.index.vector.queryRelationships('idx_dot', 5, $q)")
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)

	// No-input + params provided but all values have supported types => generic no-input error branch.
	ctxSupported := context.WithValue(ctx, paramsKey, map[string]interface{}{"q": []float32{1, 0}})
	_, err = exec.callDbIndexVectorQueryRelationships(ctxSupported, "CALL db.index.vector.queryRelationships('idx_dot', 5, 123)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no query vector or search text provided")

	// Missing vector/text with no params branch.
	_, err = exec.callDbIndexVectorQueryRelationships(context.Background(), "CALL db.index.vector.queryRelationships('idx_dot', 5, 123)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no query vector or search text provided")

	// []float32 parameter branch.
	ctxF32 := context.WithValue(ctx, paramsKey, map[string]interface{}{"q": []float32{1, 0}})
	res, err = exec.callDbIndexVectorQueryRelationships(ctxF32, "CALL db.index.vector.queryRelationships('idx_dot', 5, $q)")
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)

	// []interface{} numeric branch.
	ctxIface := context.WithValue(ctx, paramsKey, map[string]interface{}{"q": []interface{}{1, float64(0)}})
	res, err = exec.callDbIndexVectorQueryRelationships(ctxIface, "CALL db.index.vector.queryRelationships('idx_dot', 5, $q)")
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)

	// String parameter with failing embedder branch.
	exec.SetEmbedder(&failingEmbedder{err: fmt.Errorf("forced embed error")})
	ctxStringFail := context.WithValue(ctx, paramsKey, map[string]interface{}{"q": "hello"})
	_, err = exec.callDbIndexVectorQueryRelationships(ctxStringFail, "CALL db.index.vector.queryRelationships('idx_dot', 5, $q)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to embed parameter")

	// Direct string query with failing embedder branch.
	_, err = exec.callDbIndexVectorQueryRelationships(ctx, "CALL db.index.vector.queryRelationships('idx_dot', 5, 'hello')")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to embed query")

	// Parse error branch.
	_, err = exec.callDbIndexVectorQueryRelationships(ctx, "CALL db.index.vector.queryRelationships('idx_dot', 5")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "vector query parse error")

	// Index missing branch: no property filter means no embeddings picked from relationships.
	res, err = exec.callDbIndexVectorQueryRelationships(ctx, "CALL db.index.vector.queryRelationships('idx_missing', 5, [1,0])")
	require.NoError(t, err)
	assert.Empty(t, res.Rows)
}

func TestCypherHelpers_ExecuteCallDispatchAssertions(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	expectSuccess := []string{
		"CALL db.info()",
		"CALL db.ping()",
		"CALL db.labels()",
		"CALL db.relationshipTypes()",
		"CALL db.propertyKeys()",
		"CALL db.indexes()",
		"CALL db.constraints()",
		"CALL db.index.stats()",
		"CALL db.schema.visualization()",
		"CALL db.schema.nodeProperties()",
		"CALL db.schema.relProperties()",
		"CALL dbms.info()",
		"CALL dbms.listConfig()",
		"CALL dbms.clientConfig()",
		"CALL dbms.listConnections()",
		"CALL dbms.components()",
		"CALL dbms.procedures()",
		"CALL dbms.functions()",
		"CALL db.index.fulltext.listAvailableAnalyzers()",
		"CALL db.index.fulltext.queryRelationships('idx', 'nothing')",
		"CALL db.index.vector.queryRelationships('idx', 2, [0.1, 0.2])",
		"CALL db.index.vector.queryNodes('idx', 2, [0.1, 0.2])",
		"CALL gds.version()",
		"CALL gds.graph.list()",
		"CALL nornicdb.version()",
		"CALL nornicdb.stats()",
		"CALL nornicdb.decay.info()",
		"CALL db.retrieve('x')",
		"CALL db.rretrieve('x')",
		"CALL db.stats.status()",
		"CALL db.stats.stop()",
		"CALL db.stats.clear()",
		"CALL db.stats.collect()",
		"CALL db.stats.retrieve()",
		"CALL db.stats.retrieveAllAnTheStats()",
		"CALL db.awaitIndexes()",
		"CALL db.awaitIndex('idx', 1)",
		"CALL db.resampleIndex('idx')",
		"CALL apoc.algo.pageRank()",
		"CALL apoc.algo.betweenness()",
		"CALL apoc.algo.closeness()",
		"CALL apoc.algo.louvain()",
		"CALL apoc.algo.labelPropagation()",
		"CALL apoc.algo.wcc()",
		"CALL db.clearQueryCaches()",
	}
	for _, q := range expectSuccess {
		t.Run("success_"+q, func(t *testing.T) {
			res, err := exec.executeCall(ctx, q)
			require.NoError(t, err, q)
			require.NotNil(t, res, q)
			require.NotEmpty(t, res.Columns, q)
		})
	}

	expectError := []string{
		"CALL db.index.vector.queryRelationships('idx', 2, 'search text')", // no embedder
		"CALL db.index.vector.queryNodes('idx', 2, 'search text')",         // no embedder
		"CALL db.index.fulltext.queryNodes('idx', 'nothing')",              // missing fulltext index
		"CALL db.txlog.entries(1, 2)",                                      // no WAL on memory engine
		"CALL db.txlog.byTxId('x', 2)",                                     // no WAL on memory engine
		"CALL db.index.vector.embed('hello')",                              // no embedder
		"CALL tx.setMetaData({app:'test'})",                                // no active tx
		"CALL db.rerank('x')",                                              // missing/invalid args
		"CALL db.infer('x')",                                               // missing/invalid args
		"CALL db.temporal.assertNoOverlap()",                               // malformed
		"CALL db.temporal.asOf()",                                          // malformed
		"CALL apoc.path.expand()",                                          // malformed
		"CALL apoc.path.spanningTree()",                                    // malformed
		"CALL apoc.algo.dijkstra()",                                        // malformed
		"CALL apoc.algo.aStar()",                                           // malformed
		"CALL apoc.algo.allSimplePaths()",                                  // malformed
		"CALL apoc.neighbors.tohop()",                                      // malformed
		"CALL apoc.neighbors.byhop()",                                      // malformed
		"CALL apoc.load.json()",                                            // malformed
		"CALL apoc.load.csv()",                                             // malformed
		"CALL apoc.import.json()",                                          // malformed
		"CALL apoc.periodic.commit()",                                      // malformed
		"CALL apoc.periodic.iterate()",                                     // malformed
		"CALL apoc.cypher.run()",                                           // malformed
		"CALL apoc.cypher.runMany()",                                       // malformed
		"CALL apoc.cypher.doitAll()",                                       // malformed alias
		"CALL apoc.periodic.rock_n_roll()",                                 // malformed alias
		"CALL apoc.load.jsonArray()",                                       // malformed
		"CALL apoc.export.json.all()",                                      // malformed
		"CALL apoc.export.json.query()",                                    // malformed
		"CALL apoc.export.csv.all()",                                       // malformed
		"CALL apoc.export.csv.query()",                                     // malformed
		"CALL gds.graph.drop()",                                            // malformed
		"CALL gds.graph.project()",                                         // malformed
		"CALL gds.fastRP.stream()",                                         // malformed
		"CALL gds.fastRP.stats()",                                          // malformed
		"CALL db.index.vector.createNodeIndex()",                           // malformed
		"CALL db.index.vector.createRelationshipIndex()",                   // malformed
		"CALL db.index.fulltext.createNodeIndex()",                         // malformed
		"CALL db.index.fulltext.createRelationshipIndex()",                 // malformed
		"CALL db.create.setNodeVectorProperty()",                           // malformed
		"CALL db.create.setRelationshipVectorProperty()",                   // malformed
	}
	for _, q := range expectError {
		t.Run("error_"+q, func(t *testing.T) {
			res, err := exec.executeCall(ctx, q)
			require.Error(t, err, q)
			assert.Nil(t, res, q)
		})
	}
}

func TestCypherHelpers_ExecuteCartesianProductMatch_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice", "age": int64(30)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "p2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob", "age": int64(20)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "a1", Labels: []string{"Area"}, Properties: map[string]interface{}{"code": "X"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "a2", Labels: []string{"Area"}, Properties: map[string]interface{}{"code": "Y"}})
	require.NoError(t, err)

	// Non-aggregation path with WHERE, DISTINCT, ORDER BY, SKIP, LIMIT.
	query := "MATCH (p:Person), (a:Area) WHERE p.age >= 20 RETURN p.name AS name, a.code AS code ORDER BY name SKIP 1 LIMIT 2"
	retItems := []returnItem{{expr: "p.name", alias: "name"}, {expr: "a.code", alias: "code"}}
	res := &ExecuteResult{Columns: []string{"name", "code"}, Rows: [][]interface{}{}, Stats: &QueryStats{}}
	out, err := exec.executeCartesianProductMatch(
		ctx,
		query,
		"MATCH (p:Person), (a:Area)",
		[]string{"(p:Person)", "(a:Area)"},
		strings.Index(strings.ToUpper(query), "WHERE"),
		strings.Index(strings.ToUpper(query), "RETURN"),
		retItems,
		false,
		true,
		res,
	)
	require.NoError(t, err)
	require.Len(t, out.Rows, 2)

	// Aggregation path.
	aggQuery := "MATCH (p:Person), (a:Area) RETURN count(*) AS c, collect(p.name) AS names"
	aggItems := []returnItem{{expr: "count(*)", alias: "c"}, {expr: "collect(p.name)", alias: "names"}}
	aggRes := &ExecuteResult{Columns: []string{"c", "names"}, Rows: [][]interface{}{}, Stats: &QueryStats{}}
	aggOut, err := exec.executeCartesianProductMatch(
		ctx,
		aggQuery,
		"MATCH (p:Person), (a:Area)",
		[]string{"(p:Person)", "(a:Area)"},
		-1,
		strings.Index(strings.ToUpper(aggQuery), "RETURN"),
		aggItems,
		true,
		false,
		aggRes,
	)
	require.NoError(t, err)
	require.Len(t, aggOut.Rows, 1)
	assert.Equal(t, int64(4), aggOut.Rows[0][0]) // 2x2 cartesian product
}

func TestCypherHelpers_ExecuteCartesianProductMatch_LabelAndAnonymousBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Person", "Employee"}, Properties: map[string]interface{}{"name": "alice"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "n2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob"}})
	require.NoError(t, err)

	// Additional-label filtering path (:Person:Employee).
	retItems := []returnItem{{expr: "p.name", alias: "name"}}
	res := &ExecuteResult{Columns: []string{"name"}, Rows: [][]interface{}{}, Stats: &QueryStats{}}
	out, err := exec.executeCartesianProductMatch(
		ctx,
		"MATCH (p:Person:Employee) RETURN p.name AS name",
		"MATCH (p:Person:Employee)",
		[]string{"(p:Person:Employee)"},
		-1,
		strings.Index(strings.ToUpper("MATCH (p:Person:Employee) RETURN p.name AS name"), "RETURN"),
		retItems,
		false,
		false,
		res,
	)
	require.NoError(t, err)
	require.Len(t, out.Rows, 1)
	assert.Equal(t, "alice", out.Rows[0][0])

	// Anonymous pattern (no variable) results in no pattern matches and empty rows.
	anon := &ExecuteResult{Columns: []string{"x"}, Rows: [][]interface{}{}, Stats: &QueryStats{}}
	anonOut, err := exec.executeCartesianProductMatch(
		ctx,
		"MATCH (:Person) RETURN 1 AS x",
		"MATCH (:Person)",
		[]string{"(:Person)"},
		-1,
		strings.Index(strings.ToUpper("MATCH (:Person) RETURN 1 AS x"), "RETURN"),
		[]returnItem{{expr: "1", alias: "x"}},
		false,
		false,
		anon,
	)
	require.NoError(t, err)
	assert.Empty(t, anonOut.Rows)
}

func TestCypherHelpers_MutationRelationshipPatternHelpers(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)

	_, err := eng.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"age": int64(40)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "n2", Labels: []string{"Person"}, Properties: map[string]interface{}{"age": int64(30)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "n3", Labels: []string{"Person"}, Properties: map[string]interface{}{"age": int64(20)}})
	require.NoError(t, err)
	err = eng.CreateEdge(&storage.Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"})
	require.NoError(t, err)
	err = eng.CreateEdge(&storage.Edge{ID: "e2", StartNode: "n2", EndNode: "n3", Type: "LIKES"})
	require.NoError(t, err)

	n1, err := eng.GetNode("n1")
	require.NoError(t, err)
	n2, err := eng.GetNode("n2")
	require.NoError(t, err)
	n3, err := eng.GetNode("n3")
	require.NoError(t, err)

	assert.False(t, exec.evaluateRelationshipPatternInWhere(n1, "n", "(x)-[:KNOWS]->()"))
	assert.True(t, exec.evaluateRelationshipPatternInWhere(n1, "n", "(n)-[:KNOWS]->()"))
	assert.False(t, exec.evaluateRelationshipPatternInWhere(n1, "n", "(n)-[:LIKES]->()"))
	assert.True(t, exec.evaluateRelationshipPatternInWhere(n2, "n", "(n)<-[:KNOWS]-()"))
	assert.True(t, exec.evaluateRelationshipPatternInWhere(n2, "n", "(n)-[:ANY]-()"))
	assert.True(t, exec.evaluateRelationshipPatternInWhere(n1, "n", "(n)-[:KNOWS]->()-[:LIKES]->()"))

	assert.False(t, exec.checkChainedPattern(n1, "x", "(n)-[:KNOWS]->()-[:LIKES]->()", ""))
	assert.True(t, exec.checkChainedPattern(n1, "n", "(n:Person)-[:KNOWS]->()-[:LIKES]->()", ""))

	hops := exec.parseRelationshipHops("(n)-[:KNOWS|FRIEND]->()-[:LIKES]->()", "n")
	require.Len(t, hops, 2)
	assert.True(t, hops[0].outgoing)
	assert.ElementsMatch(t, []string{"KNOWS", "FRIEND"}, hops[0].relTypes)
	assert.True(t, hops[1].outgoing)
	assert.ElementsMatch(t, []string{"LIKES"}, hops[1].relTypes)
	assert.Empty(t, exec.parseRelationshipHops("(n)-[:BROKEN->()", "n"))
	incomingHops := exec.parseRelationshipHops("(n)<-[:LIKES|FOLLOWS]-()<-[]-()", "n")
	require.Len(t, incomingHops, 2)
	assert.False(t, incomingHops[0].outgoing)
	assert.ElementsMatch(t, []string{"LIKES", "FOLLOWS"}, incomingHops[0].relTypes)
	assert.False(t, incomingHops[1].outgoing)
	assert.Empty(t, incomingHops[1].relTypes)
	assert.Empty(t, exec.parseRelationshipHops("(n)<-[:BROKEN-()", "n"))

	assert.True(t, exec.traverseChain(n1, []relationshipHop{
		{relTypes: []string{"KNOWS"}, outgoing: true},
		{relTypes: []string{"LIKES"}, outgoing: true},
	}, 0))
	assert.False(t, exec.traverseChain(n1, []relationshipHop{
		{relTypes: []string{"KNOWS"}, outgoing: true},
		{relTypes: []string{"MISSING"}, outgoing: true},
	}, 0))

	// Incoming traversal branch.
	assert.True(t, exec.traverseChain(n3, []relationshipHop{
		{relTypes: []string{"LIKES"}, outgoing: false},
		{relTypes: []string{"KNOWS"}, outgoing: false},
	}, 0))
	assert.False(t, exec.traverseChain(n3, []relationshipHop{
		{relTypes: []string{"MISSING"}, outgoing: false},
	}, 0))

	// No type filter branch.
	assert.True(t, exec.traverseChain(n1, []relationshipHop{
		{relTypes: nil, outgoing: true},
	}, 0))
}

func TestCypherHelpers_ExecuteMatchRelationshipsWithClause_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{
		ID:         "pa",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "alice"},
	})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{
		ID:         "pb",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "bob"},
	})
	require.NoError(t, err)
	err = eng.CreateEdge(&storage.Edge{
		ID:        "kn-1",
		StartNode: "pa",
		EndNode:   "pb",
		Type:      "KNOWS",
	})
	require.NoError(t, err)

	out, err := exec.executeMatchRelationshipsWithClause(
		ctx,
		"p = (a:Person)-[r:KNOWS]->(b:Person)",
		"",
		"WITH a, b, r, p WHERE a.name = 'alice' RETURN a.name AS fromName, b.name AS toName ORDER BY fromName SKIP 0 LIMIT 10",
	)
	require.NoError(t, err)
	require.NotEmpty(t, out.Rows)
	assert.Equal(t, "alice", out.Rows[0][0])
	assert.Equal(t, "bob", out.Rows[0][1])

	_, err = exec.executeMatchRelationshipsWithClause(
		ctx,
		"(a:Person)-[r:KNOWS]->(b:Person)",
		"",
		"WITH a, b, r",
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RETURN clause required")

	_, err = exec.executeMatchRelationshipsWithClause(
		ctx,
		"this is not a traversal pattern",
		"",
		"WITH a RETURN a",
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid traversal pattern")
}

func TestCypherHelpers_CountSubqueryAndComparison_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)

	_, err := eng.CreateNode(&storage.Node{ID: "a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "a"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "b", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "b"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "c", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "c"}})
	require.NoError(t, err)
	require.NoError(t, eng.CreateEdge(&storage.Edge{ID: "e1", Type: "KNOWS", StartNode: "a", EndNode: "b"}))
	require.NoError(t, eng.CreateEdge(&storage.Edge{ID: "e2", Type: "LIKES", StartNode: "c", EndNode: "a"}))

	a, err := eng.GetNode("a")
	require.NoError(t, err)

	assert.EqualValues(t, 0, exec.countSubqueryMatches(a, "n", "RETURN 1"))
	assert.EqualValues(t, 0, exec.countSubqueryMatches(a, "n", "MATCH (x)-[:KNOWS]->()"))
	assert.EqualValues(t, 1, exec.countSubqueryMatches(a, "n", "MATCH (n)-[:KNOWS]->()"))
	assert.EqualValues(t, 1, exec.countSubqueryMatches(a, "n", "MATCH ()-[:LIKES]->(n)"))
	assert.EqualValues(t, 1, exec.countSubqueryMatches(a, "n", "MATCH ()-[r]->(n)"))

	assert.True(t, exec.evaluateCountSubqueryComparison(a, "n", "COUNT { MATCH (n)-[:KNOWS]->() }"))
	assert.True(t, exec.evaluateCountSubqueryComparison(a, "n", "COUNT { MATCH (n)-[:KNOWS]->() } = 1"))
	assert.True(t, exec.evaluateCountSubqueryComparison(a, "n", "COUNT { MATCH (n)-[:KNOWS]->() } != 2"))
	assert.True(t, exec.evaluateCountSubqueryComparison(a, "n", "COUNT { MATCH (n)-[:KNOWS]->() } <= 1"))
	assert.False(t, exec.evaluateCountSubqueryComparison(a, "n", "COUNT { MATCH (n)-[:KNOWS]->() } > 1"))
	assert.False(t, exec.evaluateCountSubqueryComparison(a, "n", "COUNT { MATCH (n)-[:KNOWS]->() } = nope"))
	assert.False(t, exec.evaluateCountSubqueryComparison(a, "n", "COUNT { MATCH (n)-[:KNOWS]->() "))
}

func TestCypherHelpers_ExtractionHelpers_Branches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	assert.Equal(t, "", extractVariableNameFromReturnItem(""))
	assert.Equal(t, "n", extractVariableNameFromReturnItem("n"))
	assert.Equal(t, "n", extractVariableNameFromReturnItem("n.name"))
	assert.Equal(t, "n", extractVariableNameFromReturnItem("id(n)"))
	assert.Equal(t, "n", extractVariableNameFromReturnItem("id(n.name)"))

	assert.Equal(t, "b", exec.extractTargetVariable("(a)-[:KNOWS]->(b:Person)", "a"))
	assert.Equal(t, "a", exec.extractTargetVariable("(a:Person)<-[:KNOWS]-(b)", "b"))
	assert.Equal(t, "", exec.extractTargetVariable("(a)-[:KNOWS]-", "a"))

	got := dedupeNonEmpty([]string{"a", "", "a", " b ", "b"}, []string{"", "c", "a"})
	assert.Equal(t, []string{"a", "b", "c"}, got)
}

func TestCypherHelpers_EvaluateWhereAsBooleanBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	node := &storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"age": int64(21), "name": "alice"}}

	assert.True(t, exec.evaluateWhereAsBoolean("n.age >= 21", "n", node))
	assert.False(t, exec.evaluateWhereAsBoolean("n.missing", "n", node))
	assert.True(t, exec.evaluateWhereAsBoolean("1", "n", node))
	assert.False(t, exec.evaluateWhereAsBoolean("0", "n", node))
	assert.True(t, exec.evaluateWhereAsBoolean("1.5", "n", node))
	assert.False(t, exec.evaluateWhereAsBoolean("0.0", "n", node))
	assert.True(t, exec.evaluateWhereAsBoolean("'x'", "n", node))
}

func TestCypherHelpers_ComparisonHelpers_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	node := &storage.Node{
		ID:              "n1",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{0.1, 0.2}},
		EmbedMeta: map[string]interface{}{
			"source":        "model-a",
			"nullable_meta": nil,
		},
		Properties: map[string]interface{}{
			"name": "alice",
			"age":  int64(42),
			"nick": nil,
		},
	}

	items, ok := toInterfaceSlice([]string{"a", "b"})
	require.True(t, ok)
	assert.Equal(t, []interface{}{"a", "b"}, items)
	items, ok = toInterfaceSlice([]interface{}{1, "x"})
	require.True(t, ok)
	assert.Equal(t, []interface{}{1, "x"}, items)
	_, ok = toInterfaceSlice(nil)
	assert.False(t, ok)
	_, ok = toInterfaceSlice(123)
	assert.False(t, ok)

	// Variable mismatch keeps historical pass-through semantics.
	assert.True(t, exec.evaluateIsNull(node, "n", "other.name IS NULL", false))
	assert.True(t, exec.evaluateIsNull(node, "n", "other.name IS NOT NULL", true))

	// Standard property null checks.
	assert.True(t, exec.evaluateIsNull(node, "n", "n.nick IS NULL", false))
	assert.False(t, exec.evaluateIsNull(node, "n", "n.nick IS NOT NULL", true))
	assert.True(t, exec.evaluateIsNull(node, "n", "n.name IS NOT NULL", true))
	assert.False(t, exec.evaluateIsNull(node, "n", "n.missing IS NOT NULL", true))

	// EmbedMeta-backed properties.
	assert.True(t, exec.evaluateIsNull(node, "n", "n.source IS NOT NULL", true))
	assert.True(t, exec.evaluateIsNull(node, "n", "n.nullable_meta IS NULL", false))

	// embedding is a regular property — no special routing.
	assert.True(t, exec.evaluateIsNull(node, "n", "n.embedding IS NULL", false)) // not in Properties
	node.Properties["embedding"] = []float32{0.1, 0.2}
	assert.True(t, exec.evaluateIsNull(node, "n", "n.embedding IS NOT NULL", true)) // now in Properties
}

func TestCypherHelpers_ExecuteUnwind_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	_, err := exec.executeUnwind(ctx, "MATCH (n) RETURN n")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "UNWIND clause not found")

	_, err = exec.executeUnwind(ctx, "UNWIND [1,2,3] RETURN x")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires AS clause")

	_, err = exec.executeUnwind(ctx, "UNWIND keys({a:1}) AS k RETURN k")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "keys() function")

	aggRes, err := exec.executeUnwind(ctx, "UNWIND [1,2,3] AS x RETURN sum(x) AS s, count(x) AS c, avg(x) AS a, min(x) AS mn, max(x) AS mx, collect(x)[..2] AS cs")
	require.NoError(t, err)
	require.Len(t, aggRes.Rows, 1)
	assert.Equal(t, int64(6), aggRes.Rows[0][0])
	assert.Equal(t, int64(3), aggRes.Rows[0][1])
	assert.Equal(t, 2.0, aggRes.Rows[0][2])
	assert.Equal(t, 1.0, aggRes.Rows[0][3])
	assert.Equal(t, 3.0, aggRes.Rows[0][4])
	assert.Equal(t, []interface{}{int64(1), int64(2)}, aggRes.Rows[0][5])

	rowRes, err := exec.executeUnwind(ctx, "UNWIND ['a','b'] AS x RETURN x AS value")
	require.NoError(t, err)
	require.Len(t, rowRes.Rows, 2)
	assert.Equal(t, "a", rowRes.Rows[0][0])
	assert.Equal(t, "b", rowRes.Rows[1][0])

	nullAgg, err := exec.executeUnwind(ctx, "UNWIND null AS x RETURN count(x) AS c")
	require.NoError(t, err)
	require.Len(t, nullAgg.Rows, 1)
	assert.Equal(t, int64(0), nullAgg.Rows[0][0])

	createRes, err := exec.executeUnwind(ctx, "UNWIND [1,2] AS x CREATE (n:UnwindNode {v: x}) RETURN n.v AS v")
	require.NoError(t, err)
	assert.Equal(t, 2, createRes.Stats.NodesCreated)
	require.Len(t, createRes.Rows, 2)
	assert.Equal(t, int64(1), createRes.Rows[0][0])
	assert.Equal(t, int64(2), createRes.Rows[1][0])

	// Regression: UNWIND row-map + CREATE ... SET n = row must never panic when
	// downstream execution returns a nil Stats pointer.
	mapCreateRes, err := exec.executeUnwind(ctx, "UNWIND [{a: 1, b: 'x'}, {a: 2, b: 'y'}] AS row CREATE (n:UnwindSetNode) SET n = row RETURN n.a AS a, n.b AS b")
	require.NoError(t, err)
	require.Len(t, mapCreateRes.Rows, 2)
	assert.Equal(t, int64(1), mapCreateRes.Rows[0][0])
	assert.Equal(t, "x", mapCreateRes.Rows[0][1])
	assert.Equal(t, int64(2), mapCreateRes.Rows[1][0])
	assert.Equal(t, "y", mapCreateRes.Rows[1][1])
}

func TestCypherHelpers_ExecuteMatchWithPipelineToRows_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{
		ID:     "o-2",
		Labels: []string{"OrderStatus"},
		Properties: map[string]interface{}{
			"orderId": int64(2),
			"state":   "ready",
			"active":  true,
		},
	})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{
		ID:     "o-1",
		Labels: []string{"OrderStatus"},
		Properties: map[string]interface{}{
			"orderId": int64(1),
			"state":   "ready",
			"active":  true,
		},
	})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{
		ID:     "o-3",
		Labels: []string{"OrderStatus"},
		Properties: map[string]interface{}{
			"orderId": int64(3),
			"state":   "ignored",
			"active":  false,
		},
	})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{
		ID:     "ph-2",
		Labels: []string{"Pharmacy"},
		Properties: map[string]interface{}{
			"id": "B",
		},
	})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{
		ID:     "ph-1",
		Labels: []string{"Pharmacy"},
		Properties: map[string]interface{}{
			"id": "A",
		},
	})
	require.NoError(t, err)

	rows, err := exec.executeMatchWithPipelineToRows(
		ctx,
		"MATCH (o:OrderStatus {state: 'ready'}) WHERE o.active = true WITH o",
		[]string{"o", "pharmacy"},
		eng,
	)
	require.NoError(t, err)
	require.Len(t, rows, 2)

	firstOrder, ok := rows[0]["o"].(*storage.Node)
	require.True(t, ok)
	firstPharmacy, ok := rows[0]["pharmacy"].(*storage.Node)
	require.True(t, ok)
	secondOrder, ok := rows[1]["o"].(*storage.Node)
	require.True(t, ok)
	secondPharmacy, ok := rows[1]["pharmacy"].(*storage.Node)
	require.True(t, ok)

	// Orders are sorted by orderId and pharmacy assignment follows modulo index.
	assert.Equal(t, storage.NodeID("o-1"), firstOrder.ID)
	assert.Equal(t, storage.NodeID("ph-1"), firstPharmacy.ID)
	assert.Equal(t, storage.NodeID("o-2"), secondOrder.ID)
	assert.Equal(t, storage.NodeID("ph-2"), secondPharmacy.ID)

	_, err = exec.executeMatchWithPipelineToRows(ctx, "MATCH (o:OrderStatus)", []string{"o"}, eng)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pipeline requires WITH")

	_, err = exec.executeMatchWithPipelineToRows(ctx, "MATCH (:OrderStatus) WITH o", []string{"o"}, eng)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must have a variable")
}

func TestCypherHelpers_YieldParsingAndFiltering_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	// Keyword detection should ignore quoted occurrences.
	assert.Equal(t, -1, findKeywordIndexInContext("x = 'ORDER BY' and y = 'LIMIT'", "ORDER"))
	assert.NotEqual(t, -1, findKeywordIndexInContext(" score > 0.1 ORDER BY score DESC", "ORDER"))

	parsed := parseYieldClause("CALL proc() YIELD node, score RETURN node.id AS id, score ORDER BY score DESC SKIP 1 LIMIT 2")
	require.NotNil(t, parsed)
	require.Len(t, parsed.items, 2)
	assert.Equal(t, "node", parsed.items[0].name)
	assert.Equal(t, "score", parsed.items[1].name)
	assert.True(t, parsed.hasReturn)
	assert.Equal(t, "node.id AS id, score", parsed.returnExpr)
	assert.Equal(t, "score DESC", parsed.orderBy)
	assert.Equal(t, 1, parsed.skip)
	assert.Equal(t, 2, parsed.limit)

	parsedNoReturn := parseYieldClause("CALL proc() YIELD score ORDER BY score DESC SKIP 2 LIMIT 1")
	require.NotNil(t, parsedNoReturn)
	assert.False(t, parsedNoReturn.hasReturn)
	assert.Equal(t, "score DESC", parsedNoReturn.orderBy)
	assert.Equal(t, 2, parsedNoReturn.skip)
	assert.Equal(t, 1, parsedNoReturn.limit)

	node1 := &storage.Node{ID: "n1", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "user-1"}}
	node2 := &storage.Node{ID: "n2", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "user-2"}}
	node3 := &storage.Node{ID: "n3", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "user-3"}}
	baseResult := &ExecuteResult{
		Columns: []string{"node", "score"},
		Rows: [][]interface{}{
			{node1, float64(0.2)},
			{node2, float64(0.9)},
			{node3, float64(0.5)},
		},
	}

	yield := &yieldClause{
		yieldAll:   true,
		hasReturn:  true,
		returnExpr: "node.id AS id, score AS score",
		orderBy:    "ORDER BY score DESC",
		skip:       1,
		limit:      1,
	}
	filtered, err := exec.applyYieldFilter(baseResult, yield)
	require.NoError(t, err)
	require.Len(t, filtered.Rows, 1)
	assert.Equal(t, []string{"id", "score"}, filtered.Columns)
	assert.Equal(t, "user-3", filtered.Rows[0][0])
	assert.Equal(t, float64(0.5), filtered.Rows[0][1])

	// WHERE filtering + projected aliases.
	whereResult := &ExecuteResult{
		Columns: []string{"score"},
		Rows: [][]interface{}{
			{map[string]interface{}{"value": float64(0.2)}},
			{map[string]interface{}{"value": float64(0.9)}},
		},
	}
	yieldWhere := &yieldClause{
		items: []yieldItem{
			{name: "score", alias: "score"},
		},
		where: "score.value > 0.5",
	}
	whereFiltered, err := exec.applyYieldFilter(whereResult, yieldWhere)
	require.NoError(t, err)
	// Current evaluator semantics: map-valued YIELD WHERE does not resolve to true here.
	require.Empty(t, whereFiltered.Rows)

	// Unknown column should be rejected.
	_, err = exec.applyYieldFilter(&ExecuteResult{
		Columns: []string{"a"},
		Rows:    [][]interface{}{{int64(1)}},
	}, &yieldClause{
		items: []yieldItem{{name: "missing"}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown YIELD column")
}

func TestCypherHelpers_TryFastRevenueByProduct_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)

	// Rejection branches.
	rejectCases := []struct {
		name    string
		matches *TraversalMatch
		with    string
		ret     string
	}{
		{name: "nil_matches", matches: nil, with: "p, sum(p.unitPrice * r.quantity) as revenue", ret: "p.productName, revenue"},
		{name: "chained", matches: &TraversalMatch{IsChained: true}, with: "p, sum(p.unitPrice * r.quantity) as revenue", ret: "p.productName, revenue"},
		{name: "wrong_hops", matches: &TraversalMatch{StartNode: nodePatternInfo{variable: "p", labels: []string{"Product"}}, Relationship: RelationshipPattern{Variable: "r", Types: []string{"ORDERS"}, Direction: "incoming", MinHops: 2, MaxHops: 2}}, with: "p, sum(p.unitPrice * r.quantity) as revenue", ret: "p.productName, revenue"},
		{name: "wrong_type", matches: &TraversalMatch{StartNode: nodePatternInfo{variable: "p", labels: []string{"Product"}}, Relationship: RelationshipPattern{Variable: "r", Types: []string{"LIKES"}, Direction: "incoming", MinHops: 1, MaxHops: 1}}, with: "p, sum(p.unitPrice * r.quantity) as revenue", ret: "p.productName, revenue"},
		{name: "wrong_direction", matches: &TraversalMatch{StartNode: nodePatternInfo{variable: "p", labels: []string{"Product"}}, Relationship: RelationshipPattern{Variable: "r", Types: []string{"ORDERS"}, Direction: "outgoing", MinHops: 1, MaxHops: 1}}, with: "p, sum(p.unitPrice * r.quantity) as revenue", ret: "p.productName, revenue"},
		{name: "missing_vars", matches: &TraversalMatch{StartNode: nodePatternInfo{labels: []string{"Product"}}, Relationship: RelationshipPattern{Types: []string{"ORDERS"}, Direction: "incoming", MinHops: 1, MaxHops: 1}}, with: "p, sum(p.unitPrice * r.quantity) as revenue", ret: "p.productName, revenue"},
		{name: "bad_with_expr", matches: &TraversalMatch{StartNode: nodePatternInfo{variable: "p", labels: []string{"Product"}}, Relationship: RelationshipPattern{Variable: "r", Types: []string{"ORDERS"}, Direction: "incoming", MinHops: 1, MaxHops: 1}}, with: "p, sum(p.unitPrice + r.quantity) as revenue", ret: "p.productName, revenue"},
	}
	for _, tc := range rejectCases {
		t.Run(tc.name, func(t *testing.T) {
			res, ok, err := exec.tryFastRevenueByProduct(tc.matches, tc.with, tc.ret, "", 0, 0)
			require.NoError(t, err)
			assert.False(t, ok)
			assert.Nil(t, res)
		})
	}

	_, err := eng.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "A", "unitPrice": float64(10)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "p2", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "B", "unitPrice": "20"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "o1", Labels: []string{"Order"}, Properties: map[string]interface{}{"orderID": int64(1)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "o2", Labels: []string{"Order"}, Properties: map[string]interface{}{"orderID": int64(2)}})
	require.NoError(t, err)
	require.NoError(t, eng.CreateEdge(&storage.Edge{ID: "e1", StartNode: "o1", EndNode: "p1", Type: "ORDERS", Properties: map[string]interface{}{"quantity": int64(3)}}))
	require.NoError(t, eng.CreateEdge(&storage.Edge{ID: "e2", StartNode: "o2", EndNode: "p2", Type: "ORDERS", Properties: map[string]interface{}{"quantity": float64(2)}}))

	matches := &TraversalMatch{
		StartNode: nodePatternInfo{variable: "p", labels: []string{"Product"}},
		Relationship: RelationshipPattern{
			Variable:  "r",
			Types:     []string{"ORDERS"},
			Direction: "incoming",
			MinHops:   1,
			MaxHops:   1,
		},
	}
	res, ok, err := exec.tryFastRevenueByProduct(
		matches,
		"p, sum(p.unitPrice * r.quantity) as revenue",
		"p.productName, revenue",
		"revenue DESC",
		0,
		10,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []string{"p.productName", "revenue"}, res.Columns)
	require.Len(t, res.Rows, 2)
	assert.Equal(t, "B", res.Rows[0][0])
	assert.Equal(t, float64(40), res.Rows[0][1])
	assert.Equal(t, "A", res.Rows[1][0])
	assert.Equal(t, float64(30), res.Rows[1][1])
}

func TestCypherHelpers_TryFastRevenueByProduct_TypeAndPaginationBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)

	// Products with different unitPrice types.
	_, err := eng.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "A", "unitPrice": float32(10)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "p2", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "B", "unitPrice": int(2)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "p3", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "C", "unitPrice": int64(3)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "p4", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "D", "unitPrice": "bad-number"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "p5", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "E", "unitPrice": true}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "o1", Labels: []string{"Order"}, Properties: map[string]interface{}{"id": int64(1)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "o2", Labels: []string{"Order"}, Properties: map[string]interface{}{"id": int64(2)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "o3", Labels: []string{"Order"}, Properties: map[string]interface{}{"id": int64(3)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "o4", Labels: []string{"Order"}, Properties: map[string]interface{}{"id": int64(4)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "o5", Labels: []string{"Order"}, Properties: map[string]interface{}{"id": int64(5)}})
	require.NoError(t, err)

	require.NoError(t, eng.CreateEdge(&storage.Edge{ID: "e1", StartNode: "o1", EndNode: "p1", Type: "ORDERS", Properties: map[string]interface{}{"quantity": int(3)}}))
	require.NoError(t, eng.CreateEdge(&storage.Edge{ID: "e2", StartNode: "o2", EndNode: "p2", Type: "ORDERS", Properties: map[string]interface{}{"quantity": float32(5)}}))
	require.NoError(t, eng.CreateEdge(&storage.Edge{ID: "e3", StartNode: "o3", EndNode: "p3", Type: "ORDERS", Properties: map[string]interface{}{"quantity": int64(7)}}))
	// Unusable unitPrice type/string should be skipped.
	require.NoError(t, eng.CreateEdge(&storage.Edge{ID: "e4", StartNode: "o4", EndNode: "p4", Type: "ORDERS", Properties: map[string]interface{}{"quantity": int64(1)}}))
	require.NoError(t, eng.CreateEdge(&storage.Edge{ID: "e5", StartNode: "o5", EndNode: "p5", Type: "ORDERS", Properties: map[string]interface{}{"quantity": "bad-qty"}}))

	matches := &TraversalMatch{
		StartNode: nodePatternInfo{variable: "p", labels: []string{"Product"}},
		Relationship: RelationshipPattern{
			Variable:  "r",
			Types:     []string{"ORDERS"},
			Direction: "incoming",
			MinHops:   1,
			MaxHops:   1,
		},
	}

	res, ok, err := exec.tryFastRevenueByProduct(
		matches,
		"p, sum(p.unitPrice * r.quantity) as revenue",
		"p.productName, revenue",
		"revenue DESC",
		1,
		2,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []string{"p.productName", "revenue"}, res.Columns)
	// Pagination branch should retain at least one row and keep numeric revenue values.
	require.NotEmpty(t, res.Rows)
	for _, row := range res.Rows {
		require.Len(t, row, 2)
		_, okName := row[0].(string)
		require.True(t, okName)
		_, okRev := row[1].(float64)
		require.True(t, okRev)
	}

	// Skip beyond row count yields empty rows.
	res, ok, err = exec.tryFastRevenueByProduct(
		matches,
		"p, sum(p.unitPrice * r.quantity) as revenue",
		"p.productName, revenue",
		"revenue DESC",
		100,
		0,
	)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Empty(t, res.Rows)
}

func TestCypherHelpers_TryFastCompoundOptionalMatchCount_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)

	// Rejection branches.
	res, ok, err := exec.tryFastCompoundOptionalMatchCount(nil, nodePatternInfo{variable: "p"}, optionalRelPattern{direction: "in", relType: "ORDERS", targetVar: "o"}, "RETURN p.productName, count(o)")
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, res)

	nodes := []*storage.Node{{ID: "p0", Properties: map[string]interface{}{"productName": "X"}}}
	res, ok, err = exec.tryFastCompoundOptionalMatchCount(nodes, nodePatternInfo{variable: ""}, optionalRelPattern{direction: "in", relType: "ORDERS", targetVar: "o"}, "RETURN p.productName, count(o)")
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, res)

	res, ok, err = exec.tryFastCompoundOptionalMatchCount(nodes, nodePatternInfo{variable: "p"}, optionalRelPattern{direction: "out", relType: "ORDERS", targetVar: "o"}, "RETURN p.productName, count(o)")
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, res)

	res, ok, err = exec.tryFastCompoundOptionalMatchCount(nodes, nodePatternInfo{variable: "p"}, optionalRelPattern{direction: "in", relType: "LIKES", targetVar: "o"}, "RETURN p.productName, count(o)")
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, res)

	res, ok, err = exec.tryFastCompoundOptionalMatchCount(nodes, nodePatternInfo{variable: "p"}, optionalRelPattern{direction: "in", relType: "ORDERS", targetVar: "o"}, "WITH p RETURN p")
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, res)

	// Happy path with ORDER BY/SKIP/LIMIT.
	_, err = eng.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "A"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "p2", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "B"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "p3", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "C"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "o1", Labels: []string{"Order"}, Properties: map[string]interface{}{"id": int64(1)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "o2", Labels: []string{"Order"}, Properties: map[string]interface{}{"id": int64(2)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "o3", Labels: []string{"Order"}, Properties: map[string]interface{}{"id": int64(3)}})
	require.NoError(t, err)
	require.NoError(t, eng.CreateEdge(&storage.Edge{ID: "e1", StartNode: "o1", EndNode: "p1", Type: "ORDERS"}))
	require.NoError(t, eng.CreateEdge(&storage.Edge{ID: "e2", StartNode: "o2", EndNode: "p1", Type: "ORDERS"}))
	require.NoError(t, eng.CreateEdge(&storage.Edge{ID: "e3", StartNode: "o3", EndNode: "p2", Type: "ORDERS"}))

	initial := []*storage.Node{
		{ID: "p1", Properties: map[string]interface{}{"productName": "A"}},
		{ID: "p2", Properties: map[string]interface{}{"productName": "B"}},
		{ID: "p3", Properties: map[string]interface{}{"productName": "C"}},
	}
	res, ok, err = exec.tryFastCompoundOptionalMatchCount(
		initial,
		nodePatternInfo{variable: "p"},
		optionalRelPattern{direction: "in", relType: "ORDERS", targetVar: "o"},
		"RETURN p.productName, count(o) AS orderCount ORDER BY orderCount DESC SKIP 1 LIMIT 1",
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []string{"p.productName", "orderCount"}, res.Columns)
	require.Len(t, res.Rows, 1)
	// Counts are A=2, B=1, C=0 => after SKIP 1 LIMIT 1 => B.
	assert.Equal(t, "B", res.Rows[0][0])
	assert.Equal(t, int64(1), res.Rows[0][1])
}

func TestCypherHelpers_MergeRelationshipContextHelpers_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	// extractVariableNamesFromPattern basic behavior.
	vars := exec.extractVariableNamesFromPattern("(p:Person)<-[:REL]-(poc:POC)-[:BELONGS_TO]->(a:Area)")
	assert.ElementsMatch(t, []string{"p", "poc", "a"}, vars)
	assert.Empty(t, exec.extractVariableNamesFromPattern("()-[:REL]->()"))

	// No variable names branch.
	matches, rels, err := exec.executeMatchForContextWithRelationships(ctx, "MATCH ()-[:REL]->()", "()-[:REL]->()")
	require.NoError(t, err)
	assert.Empty(t, matches)
	assert.Empty(t, rels)

	_, err = eng.CreateNode(&storage.Node{ID: "pa", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "pb", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob"}})
	require.NoError(t, err)
	require.NoError(t, eng.CreateEdge(&storage.Edge{ID: "k1", StartNode: "pa", EndNode: "pb", Type: "KNOWS"}))

	// Successful extraction branch.
	matches, rels, err = exec.executeMatchForContextWithRelationships(
		ctx,
		"MATCH (a:Person)-[:KNOWS]->(b:Person)",
		"(a:Person)-[:KNOWS]->(b:Person)",
	)
	require.NoError(t, err)
	require.Len(t, matches, 1)
	assert.Equal(t, storage.NodeID("pa"), matches[0]["a"].ID)
	assert.Equal(t, storage.NodeID("pb"), matches[0]["b"].ID)
	assert.Empty(t, rels)

	// Malformed pattern should fail fast.
	_, _, err = exec.executeMatchForContextWithRelationships(ctx, "MATCH (a:Person)-[:KNOWS]->(b:Person", "(a:Person)-[:KNOWS]->(b:Person")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "malformed relationship pattern")
}

func TestCypherHelpers_ExplainInferenceAndCostHelpers(t *testing.T) {
	exec := &StorageExecutor{}

	cols := exec.inferExplainColumns("CALL db.labels() YIELD label AS lbl RETURN lbl")
	require.Equal(t, []string{"lbl"}, cols)
	cols = exec.inferExplainColumns("MATCH (n) RETURN n.name AS name, n.age")
	require.Equal(t, []string{"name", "n.age"}, cols)
	cols = exec.inferExplainColumns("MATCH (n)")
	require.Empty(t, cols)

	limit := exec.analyzeLimitSkip("MATCH (n) RETURN n SKIP 5 LIMIT 10")
	require.Equal(t, "10", limit.Arguments["limit"])
	require.Equal(t, "5", limit.Arguments["skip"])
	assert.Equal(t, int64(10), limit.EstimatedRows)
	assert.Contains(t, limit.Description, "Limit to 10 rows")
	assert.Contains(t, limit.Description, "skip 5")

	skipOnly := exec.analyzeLimitSkip("MATCH (n) RETURN n SKIP 2")
	assert.Equal(t, int64(100), skipOnly.EstimatedRows)
	assert.Equal(t, "2", skipOnly.Arguments["skip"])

	plan := &PlanOperator{
		OperatorType:  "NodeByLabelScan",
		EstimatedRows: 3,
		Children: []*PlanOperator{
			{OperatorType: "Expand", EstimatedRows: 2},
			{OperatorType: "CustomOperator", EstimatedRows: 4},
		},
	}
	hits := exec.estimateDBHits(plan)
	require.Equal(t, int64(16), hits)
	assert.Equal(t, int64(16), plan.DBHits)

	res := &ExecuteResult{
		Rows: [][]interface{}{{1}, {2}},
	}
	exec.updatePlanWithStats(plan, res)
	assert.Equal(t, int64(2), plan.ActualRows)
	require.Len(t, plan.Children, 2)
	assert.Equal(t, int64(2), plan.Children[0].ActualRows)
	// Nil-plan branch should be a no-op.
	exec.updatePlanWithStats(nil, res)

	attached := exec.attachPlanMetadata(&ExecuteResult{}, &ExecutionPlan{Mode: ModeExplain, Root: plan})
	require.NotNil(t, attached.Metadata["planString"])
	require.NotNil(t, attached.Metadata["plan"])
	require.Equal(t, string(ModeExplain), attached.Metadata["planType"])
}

func TestCypherHelpers_ExtractCreateVariableRefsBranches(t *testing.T) {
	vars := extractCreateVariableRefs("CREATE (a)-[:KNOWS]->(b)")
	assert.ElementsMatch(t, []string{"a", "b"}, vars)

	vars = extractCreateVariableRefs("CREATE (a)<-[:KNOWS]-(b)")
	assert.ElementsMatch(t, []string{"a", "b"}, vars)

	// Inline node definitions are not simple variable refs.
	vars = extractCreateVariableRefs("CREATE (:Person {name:'x'})-[:KNOWS]->(:Person {name:'y'})")
	assert.Empty(t, vars)

	// Mixed CREATE clauses deduplicate refs.
	vars = extractCreateVariableRefs("CREATE (a)-[:R]->(b) CREATE (a)-[:R]->(c)")
	assert.ElementsMatch(t, []string{"a", "b", "c"}, vars)
}

func TestCypherHelpers_ExecuteSetTrailingPipelinesAndHelpers(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (n:Person {name:'alice'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (n:Person {name:'bob'})", nil)
	require.NoError(t, err)

	// SET + trailing UNWIND + RETURN path.
	unwindRes, err := exec.Execute(ctx, "MATCH (n:Person) SET n.score = 7 UNWIND [1,2] AS x RETURN n.name AS name, x", nil)
	require.NoError(t, err)
	require.Len(t, unwindRes.Columns, 2)
	require.Len(t, unwindRes.Rows, 4)
	for _, row := range unwindRes.Rows {
		require.Len(t, row, 2)
		name, ok := row[0].(string)
		require.True(t, ok)
		require.Contains(t, []string{"alice", "bob"}, name)
		require.Contains(t, []interface{}{int64(1), int64(2), 1, 2}, row[1])
	}

	// SET + trailing WITH/RETURN follow-query path should expose updated property directly.
	withRes, err := exec.Execute(ctx, "MATCH (n:Person) SET n.flag = true WITH n RETURN n.flag AS flag", nil)
	require.NoError(t, err)
	require.Len(t, withRes.Rows, 2)
	for _, row := range withRes.Rows {
		require.Len(t, row, 1)
		assert.Equal(t, true, row[0])
	}
	verifyRes, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n", nil)
	require.NoError(t, err)
	require.Len(t, verifyRes.Rows, 2)
	for _, row := range verifyRes.Rows {
		require.Len(t, row, 1)
		switch n := row[0].(type) {
		case map[string]interface{}:
			assert.Equal(t, true, n["flag"])
		case *storage.Node:
			assert.Equal(t, true, n.Properties["flag"])
		default:
			t.Fatalf("unexpected row type: %T", row[0])
		}
	}

	// Chained SET collapse path (SET ... SET ...).
	chainedRes, err := exec.Execute(ctx, "MATCH (n:Person {name:'alice'}) SET n += {city:'phx'} SET n.age = 30 RETURN n.city AS city, n.age AS age", nil)
	require.NoError(t, err)
	require.Len(t, chainedRes.Rows, 1)
	assert.Equal(t, "phx", chainedRes.Rows[0][0])
	assert.Equal(t, int64(30), chainedRes.Rows[0][1])

	matchResult := &ExecuteResult{
		Columns: []string{"n"},
		Rows: [][]interface{}{{
			&storage.Node{
				ID:         "set-trailing",
				Labels:     []string{"Person"},
				Properties: map[string]interface{}{"name": "zed"},
			},
		}},
	}

	// executeSetTrailingUnwind syntax validation branches.
	_, err = exec.executeSetTrailingUnwind(ctx, "UNWIND [1,2]", matchResult, &ExecuteResult{Stats: &QueryStats{}})
	require.Error(t, err)
	assert.Contains(t, strings.ToUpper(err.Error()), "AS")

	_, err = exec.executeSetTrailingUnwind(ctx, "UNWIND [1,2] AS x", matchResult, &ExecuteResult{Stats: &QueryStats{}})
	require.Error(t, err)
	assert.Contains(t, strings.ToUpper(err.Error()), "RETURN")

	// Helper branches.
	assert.Equal(t, "n += $props, n.x = 1, n.y = 2", collapseChainedSetClauses("n += $props SET n.x = 1 SET n.y = 2"))
	assert.Equal(t, -1, firstPostSetClauseIndex("n.x = 1, n.y = 2"))
	assert.GreaterOrEqual(t, firstPostSetClauseIndex("n.x = 1 RETURN n"), 0)
	assert.GreaterOrEqual(t, firstPostSetClauseIndex("n.x = 1 UNWIND [1] AS x RETURN x"), 0)
}

func TestCypherHelpers_ValueToCypherLiteralAndPipelineRowsBranches(t *testing.T) {
	// valueToCypherLiteral/mapToCypherLiteral branches.
	assert.Equal(t, "'x'", valueToCypherLiteral("x"))
	assert.Equal(t, "true", valueToCypherLiteral(true))
	assert.Equal(t, "null", valueToCypherLiteral(nil))
	assert.Equal(t, "9", valueToCypherLiteral(9))
	assert.Equal(t, "[1, 'a', [2, false]]", valueToCypherLiteral([]interface{}{1, "a", []interface{}{2, false}}))

	mixed := valueToCypherLiteral(map[interface{}]interface{}{"a": 1, "b": "x"})
	assert.True(t, strings.HasPrefix(mixed, "{") && strings.HasSuffix(mixed, "}"))
	assert.Contains(t, mixed, "a: 1")
	assert.Contains(t, mixed, "b: 'x'")

	// executeMatchWithPipelineToRows branches for multi-label/property/where filtering.
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "o1",
		Labels: []string{"OrderStatus", "Routable"},
		Properties: map[string]interface{}{
			"orderId": "A-1",
			"status":  "open",
		},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     "o2",
		Labels: []string{"OrderStatus", "Routable"},
		Properties: map[string]interface{}{
			"orderId": "A-2",
			"status":  "closed",
		},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     "ph1",
		Labels: []string{"Pharmacy"},
		Properties: map[string]interface{}{
			"id": "P-1",
		},
	})
	require.NoError(t, err)

	matchPart := "MATCH (o:OrderStatus:Routable {status:'open'}) WHERE o.orderId IS NOT NULL WITH collect(o) AS orders UNWIND range(0, size(orders)-1) AS i WITH orders[i] AS o, i MATCH (ph:Pharmacy) WITH o, i, ph ORDER BY ph.id WITH o, i, collect(ph) AS pharmacies WITH o, pharmacies[i % size(pharmacies)] AS pharmacy"
	rows, err := exec.executeMatchWithPipelineToRows(ctx, matchPart, []string{"o", "pharmacy"}, store)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	_, ok := rows[0]["o"].(*storage.Node)
	require.True(t, ok)
	_, ok = rows[0]["pharmacy"].(*storage.Node)
	require.True(t, ok)

	// No pharmacies path should deterministically return empty output rows.
	emptyStore := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	_, err = emptyStore.CreateNode(&storage.Node{
		ID:     "o-empty",
		Labels: []string{"OrderStatus"},
		Properties: map[string]interface{}{
			"orderId": "Z-1",
		},
	})
	require.NoError(t, err)
	rows, err = exec.executeMatchWithPipelineToRows(
		ctx,
		"MATCH (o:OrderStatus) WITH collect(o) AS orders UNWIND range(0, size(orders)-1) AS i WITH orders[i] AS o, i MATCH (ph:Pharmacy) WITH o, i, ph ORDER BY ph.id WITH o, i, collect(ph) AS pharmacies WITH o, pharmacies[i % size(pharmacies)] AS pharmacy",
		[]string{"o", "pharmacy"},
		emptyStore,
	)
	require.NoError(t, err)
	assert.Empty(t, rows)
}

func TestCypherHelpers_SetTrailingWithReturnAndRowNormalizationBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{
		ID:         "norm-node",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"flag": true, "name": "norm"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)

	// normalizeSetMatchRowsToNodes: map with _nodeId is converted; non-map and map without _nodeId stay untouched.
	mr := &ExecuteResult{
		Columns: []string{"n", "literal", "raw"},
		Rows: [][]interface{}{{
			map[string]interface{}{"_nodeId": "norm-node"},
			"keep",
			map[string]interface{}{"name": "unchanged"},
		}, {
			map[string]interface{}{"_nodeId": 123}, // invalid node ID type branch
			map[string]interface{}{"_nodeId": "missing-node"},
			map[string]interface{}{"_nodeId": ""}, // empty node ID branch
		}},
	}
	exec.normalizeSetMatchRowsToNodes(mr, store)
	require.Len(t, mr.Rows, 2)
	require.Len(t, mr.Rows[0], 3)
	n, ok := mr.Rows[0][0].(*storage.Node)
	require.True(t, ok)
	assert.Equal(t, "norm-node", string(n.ID))
	assert.Equal(t, "keep", mr.Rows[0][1])
	raw, ok := mr.Rows[0][2].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "unchanged", raw["name"])
	for _, v := range mr.Rows[1] {
		_, mapOK := v.(map[string]interface{})
		require.True(t, mapOK)
	}

	// resolveSetTrailingValue: direct column hit.
	row := []interface{}{"vcol", node}
	colIndex := map[string]int{"col": 0, "n": 1}
	val, resolved := resolveSetTrailingValue("col", row, colIndex, map[string]*storage.Node{"n": node})
	require.True(t, resolved)
	assert.Equal(t, "vcol", val)

	// resolveSetTrailingValue: property hit through node scope.
	val, resolved = resolveSetTrailingValue("n.flag", row, colIndex, map[string]*storage.Node{"n": node})
	require.True(t, resolved)
	assert.Equal(t, true, val)

	// resolveSetTrailingValue: unresolved branch.
	val, resolved = resolveSetTrailingValue("missing.prop", row, colIndex, map[string]*storage.Node{})
	require.False(t, resolved)
	assert.Nil(t, val)

	// executeSetTrailingWithReturn: non-WITH trailing text => handled=false.
	out, handled, err := exec.executeSetTrailingWithReturn(ctx, "UNWIND [1] AS x RETURN x", mr, &ExecuteResult{Stats: &QueryStats{}})
	require.NoError(t, err)
	assert.False(t, handled)
	assert.Nil(t, out)

	// executeSetTrailingWithReturn: WITH without RETURN => handled=false.
	out, handled, err = exec.executeSetTrailingWithReturn(ctx, "WITH n", mr, &ExecuteResult{Stats: &QueryStats{}})
	require.NoError(t, err)
	assert.False(t, handled)
	assert.Nil(t, out)

	// executeSetTrailingWithReturn: empty WITH body => handled=true with error.
	_, handled, err = exec.executeSetTrailingWithReturn(ctx, "WITH   RETURN n", mr, &ExecuteResult{Stats: &QueryStats{}})
	require.Error(t, err)
	assert.True(t, handled)

	// executeSetTrailingWithReturn: malformed WITH item => handled=true with error.
	_, handled, err = exec.executeSetTrailingWithReturn(ctx, "WITH n AS RETURN n", mr, &ExecuteResult{Stats: &QueryStats{}})
	require.Error(t, err)
	assert.True(t, handled)

	// executeSetTrailingWithReturn: unsupported additional clause in WITH => falls back (handled=false).
	out, handled, err = exec.executeSetTrailingWithReturn(ctx, "WITH n MATCH (m) RETURN n", mr, &ExecuteResult{Stats: &QueryStats{}})
	require.NoError(t, err)
	assert.False(t, handled)
	assert.Nil(t, out)

	// executeSetTrailingWithReturn: handled path, projection through WITH alias and property access.
	matchResult := &ExecuteResult{
		Columns: []string{"n"},
		Rows: [][]interface{}{{
			node,
		}},
	}
	res := &ExecuteResult{Stats: &QueryStats{}}
	out, handled, err = exec.executeSetTrailingWithReturn(ctx, "WITH n AS person RETURN person.flag AS flag, person.name AS name", matchResult, res)
	require.NoError(t, err)
	require.True(t, handled)
	require.NotNil(t, out)
	require.Equal(t, []string{"flag", "name"}, out.Columns)
	require.Len(t, out.Rows, 1)
	require.Len(t, out.Rows[0], 2)
	assert.Equal(t, true, out.Rows[0][0])
	assert.Equal(t, "norm", out.Rows[0][1])

	// executeSetTrailingWithReturn: map-property projection branch from WITH alias value.
	mapOut, handled, err := exec.executeSetTrailingWithReturn(
		ctx,
		"WITH {flag: true, name: 'map'} AS person RETURN person.flag AS flag, person.name AS name",
		matchResult,
		&ExecuteResult{Stats: &QueryStats{}},
	)
	require.NoError(t, err)
	require.True(t, handled)
	require.Len(t, mapOut.Rows, 1)
	assert.Equal(t, true, mapOut.Rows[0][0])
	assert.Equal(t, "map", mapOut.Rows[0][1])
}

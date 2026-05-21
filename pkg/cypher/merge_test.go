// Tests for MERGE clause functionality, including relationship error handling
// and idempotent operations.
// Based on Neo4j's MERGE semantics: if MATCH finds results → use those, else CREATE

package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type staleMergeLookupEngine struct {
	storage.Engine
	hiddenLabel string
}

func (s *staleMergeLookupEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if label == s.hiddenLabel {
		return nil, nil
	}
	return s.Engine.GetNodesByLabel(label)
}

type noScanMergeLookupEngine struct {
	storage.Engine
}

func (n *noScanMergeLookupEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	return nil, assert.AnError
}

func (n *noScanMergeLookupEngine) AllNodes() ([]*storage.Node, error) {
	return nil, assert.AnError
}

// ========================================
// Basic MERGE Node Tests
// ========================================

func TestMergeNode_CreateWhenEmpty(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// MERGE on empty store should create
	result, err := e.Execute(ctx, `
		MERGE (n:TestNode {id: 'test-1'})
		RETURN n.id
	`, nil)

	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	// Verify node was created
	verifyResult, err := e.Execute(ctx, `
		MATCH (n:TestNode {id: 'test-1'})
		RETURN n.id
	`, nil)
	require.NoError(t, err)
	require.Len(t, verifyResult.Rows, 1)
}

func TestMergeNode_MatchWhenExists(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node first
	_, err := e.Execute(ctx, `
		CREATE (n:Person {name: 'Alice', age: 30})
	`, nil)
	require.NoError(t, err)

	// MERGE should find existing node, not create new one
	result, err := e.Execute(ctx, `
		MERGE (n:Person {name: 'Alice'})
		RETURN n.name, n.age
	`, nil)

	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	// Verify only one Person node exists
	countResult, err := e.Execute(ctx, `
		MATCH (n:Person)
		RETURN count(n) as cnt
	`, nil)
	require.NoError(t, err)
	require.Len(t, countResult.Rows, 1)
	assert.Equal(t, int64(1), countResult.Rows[0][0])
}

func TestMergeNode_FindMergeNodeIgnoresStaleCacheEntry(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	actual := &storage.Node{
		ID:         "actual-person",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	_, err := store.CreateNode(actual)
	require.NoError(t, err)

	labels := []string{"Person"}
	props := map[string]interface{}{"name": "Alice"}
	exec.cacheMergeNode(labels, props, &storage.Node{
		ID:         "stale-person",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	})

	found, err := exec.findMergeNode(store, labels, props)
	require.NoError(t, err)
	require.NotNil(t, found)
	assert.Equal(t, storage.NodeID("actual-person"), found.ID)

	exec.nodeLookupCacheMu.RLock()
	cached := exec.nodeLookupCache[mergeLookupCacheKey(labels, "name", "Alice")]
	exec.nodeLookupCacheMu.RUnlock()
	require.NotNil(t, cached)
	assert.Equal(t, storage.NodeID("actual-person"), cached.ID)
}

func TestCloneWithStorageSharesNodeLookupCacheAndLock(t *testing.T) {
	parent := NewStorageExecutor(storage.NewMemoryEngine())
	clone := parent.cloneWithStorage(storage.NewMemoryEngine())

	if parent.nodeLookupCacheMu != clone.nodeLookupCacheMu {
		t.Fatal("cloneWithStorage must share node lookup cache lock with parent")
	}

	parent.cacheMergeNode([]string{"Person"}, map[string]interface{}{"id": "parent"}, &storage.Node{
		ID:         "parent-node",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"id": "parent"},
	})
	clone.cacheMergeNode([]string{"Person"}, map[string]interface{}{"id": "clone"}, &storage.Node{
		ID:         "clone-node",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"id": "clone"},
	})

	if got := parent.findMergeNodeInCache(nil, []string{"Person"}, map[string]interface{}{"id": "clone"}); got == nil {
		t.Fatal("parent cache did not observe clone node")
	}
	if got := clone.findMergeNodeInCache(nil, []string{"Person"}, map[string]interface{}{"id": "parent"}); got == nil {
		t.Fatal("clone cache did not observe parent node")
	}
}

// TestCloneWithStorageIsolatesNodeLookupCacheForTxClones guards the fix for a
// concurrent-MERGE bug where two writers MERGE-ing the same (label, prop, value)
// would race through the executor's nodeLookupCache: the first writer wrote its
// uncommitted node into the shared cache, the peer read that node ID via
// store.GetNode(...) inside its own tx.badgerTx, and Badger SSI then rejected
// the loser with a generic "Transaction Conflict" instead of the consumer-pinned
// commit-time UNIQUE shape. See docs/plans/consumer-pinned-error-contract-plan.md
// §2.1. Transactional clones must therefore get their own cache + mutex.
func TestCloneWithStorageIsolatesNodeLookupCacheForTxClones(t *testing.T) {
	parent := NewStorageExecutor(storage.NewMemoryEngine())
	parent.ensureNodeLookupCache()

	tx, err := parent.storage.(*storage.MemoryEngine).BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	txWrapper := &transactionStorageWrapper{
		tx:             tx,
		underlying:     parent.storage,
		mutatedNodeIDs: make(map[string]struct{}),
	}
	clone := parent.cloneWithStorage(txWrapper)

	if parent.nodeLookupCacheMu == clone.nodeLookupCacheMu {
		t.Fatal("transactional clone must NOT share the lookup-cache mutex with parent")
	}
	if &parent.nodeLookupCache == &clone.nodeLookupCache {
		t.Fatal("transactional clone must NOT share the lookup-cache map with parent")
	}

	parent.cacheMergeNode([]string{"Person"}, map[string]interface{}{"id": "abc"}, &storage.Node{
		ID:         "parent-node",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"id": "abc"},
	})
	if got := clone.findMergeNodeInCache(nil, []string{"Person"}, map[string]interface{}{"id": "abc"}); got != nil {
		t.Fatalf("transactional clone leaked parent's pre-commit cache entry: %+v", got)
	}
}

// ========================================
// MERGE with ON CREATE/ON MATCH Tests
// ========================================

func TestMergeNode_OnCreateSet(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// ON CREATE SET should run when creating
	result, err := e.Execute(ctx, `
		MERGE (n:Counter {name: 'hits'})
		ON CREATE SET n.count = 1
		RETURN n.name, n.count
	`, nil)

	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	// Verify the node was created with ON CREATE properties
	verifyResult, err := e.Execute(ctx, `
		MATCH (n:Counter {name: 'hits'})
		RETURN n.count
	`, nil)
	require.NoError(t, err)
	require.Len(t, verifyResult.Rows, 1)
	assert.Equal(t, int64(1), verifyResult.Rows[0][0])
}

func TestMergeNode_OnMatchSet(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create initial node
	_, err := e.Execute(ctx, `
		CREATE (n:Counter {name: 'hits', count: 1})
	`, nil)
	require.NoError(t, err)

	// ON MATCH SET should run when finding existing
	_, err = e.Execute(ctx, `
		MERGE (n:Counter {name: 'hits'})
		ON MATCH SET n.count = n.count + 1
		RETURN n.count
	`, nil)
	require.NoError(t, err)

	// Verify count was incremented
	verifyResult, err := e.Execute(ctx, `
		MATCH (n:Counter {name: 'hits'})
		RETURN n.count
	`, nil)
	require.NoError(t, err)
	require.Len(t, verifyResult.Rows, 1)
	assert.Equal(t, int64(2), verifyResult.Rows[0][0])
}

// ========================================
// MERGE Idempotency Tests
// ========================================

func TestMergeNode_Idempotent(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// First MERGE - should create
	_, err := e.Execute(ctx, `
		MERGE (n:Singleton {key: 'unique-key'})
		SET n.value = 'first'
	`, nil)
	require.NoError(t, err)

	// Second MERGE - should NOT create, should match
	_, err = e.Execute(ctx, `
		MERGE (n:Singleton {key: 'unique-key'})
		SET n.value = 'second'
	`, nil)
	require.NoError(t, err)

	// Third MERGE - still idempotent
	_, err = e.Execute(ctx, `
		MERGE (n:Singleton {key: 'unique-key'})
		SET n.value = 'third'
	`, nil)
	require.NoError(t, err)

	// Verify only ONE node exists
	countResult, err := e.Execute(ctx, `
		MATCH (n:Singleton {key: 'unique-key'})
		RETURN count(n) as cnt
	`, nil)
	require.NoError(t, err)
	require.Len(t, countResult.Rows, 1)
	assert.Equal(t, int64(1), countResult.Rows[0][0])

	// Verify it has the last value
	valueResult, err := e.Execute(ctx, `
		MATCH (n:Singleton {key: 'unique-key'})
		RETURN n.value
	`, nil)
	require.NoError(t, err)
	require.Len(t, valueResult.Rows, 1)
	assert.Equal(t, "third", valueResult.Rows[0][0])
}

func TestMergeNode_StandaloneSetAfterMerge(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := e.Execute(ctx, `
		MERGE (fv:FactVersion {version_id: 'fv-14e79330'})
		SET fv.fact_key = 'repo_fact|import|internal/gitreader/gitreader.go->import (
			"bufio"
			"bytes"
		)',
		    fv.tx_id = 'tx-5671c64f-000001',
		    fv.commit_hash = '5671c64fcba850a6fd01ef68f2b9d592389f41c1',
		    fv.valid_from_iso = '2026-03-20T20:22:20Z',
		    fv.valid_from = datetime('2026-03-20T20:22:20Z'),
		    fv.value_json = '{"repo":"git-to-graph","source":"internal/gitreader/gitreader.go"}',
		    fv.valid_to = CASE WHEN null IS NULL THEN null ELSE datetime(null) END,
		    fv.asserted_at = datetime('2026-03-20T20:22:20Z'),
		    fv.asserted_by = 'TJ Sweet',
		    fv.semantic_type = 'ImportEdgeVersion'
	`, nil)
	require.NoError(t, err)

	res, err := e.Execute(ctx, `
		MATCH (fv:FactVersion {version_id: 'fv-14e79330'})
		RETURN fv.semantic_type, fv.asserted_by, fv.valid_to, fv.tx_id
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "ImportEdgeVersion", res.Rows[0][0])
	require.Equal(t, "TJ Sweet", res.Rows[0][1])
	require.NotNil(t, res.Rows[0][2])
	require.Equal(t, "tx-5671c64f-000001", res.Rows[0][3])
}

func TestMergeChain_StandaloneSetThenRelationshipMerge(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := e.Execute(ctx, `
		MERGE (fk:FactKey {subject_entity_id: 'file::internal/parser/parser.go', predicate: 'calls'})
		MERGE (fv:FactVersion {version_id: 'fv-ad70fda8'})
		SET fv.fact_key = 'repo_fact|calls|file::internal/parser/parser.go->symbol::internal/parser/parser.go::function::parseTreeSitter',
		    fv.semantic_type = 'CallEdgeVersion',
		    fv.asserted_by = 'TJ Sweet'
		MERGE (fk)-[:HAS_VERSION]->(fv)
	`, nil)
	require.NoError(t, err)

	res, err := e.Execute(ctx, `
		MATCH (fk:FactKey {subject_entity_id: 'file::internal/parser/parser.go', predicate: 'calls'})-[:HAS_VERSION]->(fv:FactVersion {version_id: 'fv-ad70fda8'})
		RETURN fv.semantic_type, fv.asserted_by
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "CallEdgeVersion", res.Rows[0][0])
	require.Equal(t, "TJ Sweet", res.Rows[0][1])
}

// ========================================
// MERGE Relationship Tests
// ========================================

func TestMergeRelationship_Basic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes first
	_, err := e.Execute(ctx, "CREATE (a:Person {name: 'Alice'})", nil)
	require.NoError(t, err)
	_, err = e.Execute(ctx, "CREATE (b:Person {name: 'Bob'})", nil)
	require.NoError(t, err)

	// MERGE relationship
	_, err = e.Execute(ctx, `
		MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
		MERGE (a)-[r:KNOWS]->(b)
	`, nil)
	require.NoError(t, err)

	// Verify relationship exists
	verifyResult, err := e.Execute(ctx, `
		MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'})
		RETURN count(r) as cnt
	`, nil)
	require.NoError(t, err)
	require.Len(t, verifyResult.Rows, 1)
	assert.Equal(t, int64(1), verifyResult.Rows[0][0])
}

func TestMergeRelationship_Idempotent(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	_, err := e.Execute(ctx, "CREATE (a:Node {id: 'a'})", nil)
	require.NoError(t, err)
	_, err = e.Execute(ctx, "CREATE (b:Node {id: 'b'})", nil)
	require.NoError(t, err)

	// First MERGE relationship
	_, err = e.Execute(ctx, `
		MATCH (a:Node {id: 'a'}), (b:Node {id: 'b'})
		MERGE (a)-[r:CONNECTED]->(b)
	`, nil)
	require.NoError(t, err)

	// Second MERGE - should not create duplicate
	_, err = e.Execute(ctx, `
		MATCH (a:Node {id: 'a'}), (b:Node {id: 'b'})
		MERGE (a)-[r:CONNECTED]->(b)
	`, nil)
	require.NoError(t, err)

	// Verify only one relationship
	countResult, err := e.Execute(ctx, `
		MATCH (a:Node {id: 'a'})-[r:CONNECTED]->(b:Node {id: 'b'})
		RETURN count(r) as cnt
	`, nil)
	require.NoError(t, err)
	require.Len(t, countResult.Rows, 1)
	assert.Equal(t, int64(1), countResult.Rows[0][0])
}

func TestMergeHelpers_ParseReturnAndClauseSplitBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)

	a := &storage.Node{ID: "n-a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}}
	b := &storage.Node{ID: "n-b", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob"}}
	_, err := store.CreateNode(a)
	require.NoError(t, err)
	_, err = store.CreateNode(b)
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e-ab", StartNode: a.ID, EndNode: b.ID, Type: "KNOWS"}))

	nodeCtx := map[string]*storage.Node{"a": a, "b": b}
	relCtx := map[string]*storage.Edge{"r": {ID: "e-ab", StartNode: a.ID, EndNode: b.ID, Type: "KNOWS"}}
	ctx := context.Background()

	cols, vals := e.parseReturnClauseWithContext(ctx, "*", nodeCtx, relCtx)
	require.Len(t, cols, 2)
	require.Len(t, vals, 2)

	cols, vals = e.parseReturnClauseWithContext(ctx, "a.name AS name, id(a) AS aid", nodeCtx, relCtx)
	require.Equal(t, []string{"name", "aid"}, cols)
	require.Len(t, vals, 2)
	require.Equal(t, "alice", vals[0])

	assert.Nil(t, splitMergeChainClauseBlock(""))
	parts := splitMergeChainClauseBlock("junk OPTIONAL MATCH (a) FOREACH (x IN [1] | SET a.v = x) RETURN a")
	require.GreaterOrEqual(t, len(parts), 3)

	collapsed := collapseConsecutiveDuplicateWithClauses(`
MERGE (o:OriginalText {textKey: $lookupValue})
WITH o, $targetLang AS targetLang
WITH o, $targetLang AS targetLang
WHERE o IS NOT NULL
RETURN o
`)
	require.Equal(t, 1, strings.Count(collapsed, "WITH o, $targetLang AS targetLang"))
}

func TestFindMergeNode_UsesPropertyIndexLookup(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	schema := store.GetSchema()
	require.NotNil(t, schema)
	require.NoError(t, schema.AddPropertyIndex("idx_original_textkey", "OriginalText", []string{"textKey"}))
	nodeID, err := store.CreateNode(&storage.Node{
		ID:     "orig-idx-1",
		Labels: []string{"OriginalText"},
		Properties: map[string]interface{}{
			"textKey": "k-1",
		},
	})
	require.NoError(t, err)
	require.NoError(t, schema.PropertyIndexInsert("OriginalText", "textKey", nodeID, "k-1"))

	found, err := exec.findMergeNode(store, []string{"OriginalText"}, map[string]interface{}{"textKey": "k-1"})
	require.NoError(t, err)
	require.NotNil(t, found)
	require.Equal(t, storage.NodeID("orig-idx-1"), found.ID)
}

func TestFindMergeNode_UsesUniqueConstraintLookupWithoutScan(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")

	schema := store.GetSchema()
	require.NotNil(t, schema)
	require.NoError(t, schema.AddUniqueConstraint("function_uid_unique", "Function", "uid"))
	_, err := store.CreateNode(&storage.Node{
		ID:     "function-1",
		Labels: []string{"Function"},
		Properties: map[string]interface{}{
			"uid":  "content-entity:e_hot",
			"name": "hotPath",
		},
	})
	require.NoError(t, err)

	noScanStore := &noScanMergeLookupEngine{Engine: store}
	exec := NewStorageExecutor(noScanStore)

	found, err := exec.findMergeNode(noScanStore, []string{"Function"}, map[string]interface{}{"uid": "content-entity:e_hot"})
	require.NoError(t, err)
	require.NotNil(t, found)
	require.Equal(t, storage.NodeID("function-1"), found.ID)
}

func TestFindMergeNode_IgnoresNonComparableUniqueLookupValue(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")

	schema := store.GetSchema()
	require.NotNil(t, schema)
	require.NoError(t, schema.AddUniqueConstraint("function_uid_unique", "Function", "uid"))
	exec := NewStorageExecutor(store)

	require.NotPanics(t, func() {
		found, err := exec.findMergeNode(store, []string{"Function"}, map[string]interface{}{"uid": []string{"bad"}})
		require.NoError(t, err)
		require.Nil(t, found)
	})
}

func TestFindMergeNode_RequiresFullMultiLabelMatch(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	_, err := store.CreateNode(&storage.Node{
		ID:     "single-label",
		Labels: []string{"FileChunk"},
		Properties: map[string]interface{}{
			"id": "chunk-1",
		},
	})
	require.NoError(t, err)

	found, err := exec.findMergeNode(store, []string{"FileChunk", "Node"}, map[string]interface{}{"id": "chunk-1"})
	require.NoError(t, err)
	require.Nil(t, found)

	dualID, err := store.CreateNode(&storage.Node{
		ID:     "dual-label",
		Labels: []string{"FileChunk", "Node"},
		Properties: map[string]interface{}{
			"id": "chunk-1",
		},
	})
	require.NoError(t, err)

	found, err = exec.findMergeNode(store, []string{"FileChunk", "Node"}, map[string]interface{}{"id": "chunk-1"})
	require.NoError(t, err)
	require.NotNil(t, found)
	require.Equal(t, dualID, found.ID)
}

func TestExecuteCompoundMatchMerge_OptionalAndContextRelationshipBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := e.Execute(ctx, "CREATE (a:Person {name:'alice'})", nil)
	require.NoError(t, err)
	_, err = e.Execute(ctx, "CREATE (b:Person {name:'bob'})", nil)
	require.NoError(t, err)
	_, err = e.Execute(ctx, "MATCH (a:Person {name:'alice'}), (b:Person {name:'bob'}) CREATE (a)-[:KNOWS]->(b)", nil)
	require.NoError(t, err)

	// OPTIONAL MATCH with no matches still executes MERGE path.
	res, err := e.executeCompoundMatchMerge(ctx, "OPTIONAL MATCH (m:Missing) MERGE (t:Target {name:'created'}) RETURN t.name")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)

	// Relationship-context matcher error path via malformed match clause.
	matches, rels, err := e.executeMatchForContextWithRelationships(
		ctx,
		"MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN",
		"(a:Person)-[:KNOWS]->(b:Person)",
	)
	require.NoError(t, err)
	require.NotNil(t, matches)
	require.NotNil(t, rels)
}

func TestExecuteMatchForContextWithRelationships_MapResolutionBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := e.Execute(ctx, "CREATE (a:Person {name:'alice'}), (b:Person {name:'bob'}), (a)-[:KNOWS]->(b)", nil)
	require.NoError(t, err)

	// Map with explicit id key branch.
	matches, _, err := e.executeMatchForContextWithRelationships(
		ctx,
		"MATCH (a:Person)-[:KNOWS]->(b:Person) WITH {id:'a'} AS a, b",
		"(a:Person)-[:KNOWS]->(b:Person)",
	)
	require.NoError(t, err)
	require.NotNil(t, matches)

	// Map without id/_id branch should fall back to findNodeByProperties.
	matches, _, err = e.executeMatchForContextWithRelationships(
		ctx,
		"MATCH (a:Person)-[:KNOWS]->(b:Person) WITH {name:'alice'} AS a, b",
		"(a:Person)-[:KNOWS]->(b:Person)",
	)
	require.NoError(t, err)
	require.NotEmpty(t, matches)
}

// ========================================
// FileIndexer Pattern Tests
// ========================================

func TestMerge_FileIndexerPattern(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("create file and chunk nodes", func(t *testing.T) {
		// Create file node (like FileIndexer does)
		_, err := e.Execute(ctx, `
			CREATE (f:File:Node {
				path: '/app/docs/README.md',
				name: 'README.md',
				type: 'file'
			})
		`, nil)
		require.NoError(t, err)

		// Verify file was created
		fileResult, err := e.Execute(ctx, `
			MATCH (f:File {path: '/app/docs/README.md'})
			RETURN f.name
		`, nil)
		require.NoError(t, err)
		require.Len(t, fileResult.Rows, 1)
		assert.Equal(t, "README.md", fileResult.Rows[0][0])
	})

	t.Run("merge chunk with file relationship", func(t *testing.T) {
		// Get file node ID
		fileResult, err := e.Execute(ctx, `
			MATCH (f:File {path: '/app/docs/README.md'})
			RETURN id(f) as fileId
		`, nil)
		require.NoError(t, err)
		require.Len(t, fileResult.Rows, 1)
		fileNodeId := fileResult.Rows[0][0]

		// MERGE chunk (like FileIndexer does)
		_, err = e.Execute(ctx, `
			MATCH (f:File) WHERE id(f) = $fileNodeId
			MERGE (c:FileChunk:Node {id: $chunkId})
			SET c.chunk_index = $chunkIndex, c.text = $text, c.parent_file_id = $fileNodeId
			MERGE (f)-[:HAS_CHUNK {index: $chunkIndex}]->(c)
		`, map[string]interface{}{
			"fileNodeId": fileNodeId,
			"chunkId":    "chunk-readme-0-abc123",
			"chunkIndex": 0,
			"text":       "# Introduction",
		})
		require.NoError(t, err)

		// Verify chunk was created
		chunkResult, err := e.Execute(ctx, `
			MATCH (c:FileChunk {id: 'chunk-readme-0-abc123'})
			RETURN c.text, c.chunk_index
		`, nil)
		require.NoError(t, err)
		require.Len(t, chunkResult.Rows, 1)
		assert.Equal(t, "# Introduction", chunkResult.Rows[0][0])
	})

	t.Run("re-merge same chunk is idempotent", func(t *testing.T) {
		// Get file node ID
		fileResult, err := e.Execute(ctx, `
			MATCH (f:File {path: '/app/docs/README.md'})
			RETURN id(f) as fileId
		`, nil)
		require.NoError(t, err)
		fileNodeId := fileResult.Rows[0][0]

		// MERGE same chunk again with updated text
		_, err = e.Execute(ctx, `
			MATCH (f:File) WHERE id(f) = $fileNodeId
			MERGE (c:FileChunk:Node {id: $chunkId})
			SET c.text = $text
		`, map[string]interface{}{
			"fileNodeId": fileNodeId,
			"chunkId":    "chunk-readme-0-abc123",
			"text":       "# Updated Introduction",
		})
		require.NoError(t, err)

		// Verify only one chunk exists with that ID
		countResult, err := e.Execute(ctx, `
			MATCH (c:FileChunk {id: 'chunk-readme-0-abc123'})
			RETURN count(c) as cnt
		`, nil)
		require.NoError(t, err)
		require.Len(t, countResult.Rows, 1)
		assert.Equal(t, int64(1), countResult.Rows[0][0])

		// Verify text was updated
		textResult, err := e.Execute(ctx, `
			MATCH (c:FileChunk {id: 'chunk-readme-0-abc123'})
			RETURN c.text
		`, nil)
		require.NoError(t, err)
		require.Len(t, textResult.Rows, 1)
		assert.Equal(t, "# Updated Introduction", textResult.Rows[0][0])
	})
}

// Test exact FileIndexer query format with SET on separate line
func TestMerge_FileIndexerExactFormat(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create file node
	_, err := e.Execute(ctx, `
		CREATE (f:File:Node {
			path: '/app/docs/TEST.md',
			name: 'TEST.md',
			type: 'file'
		})
	`, nil)
	require.NoError(t, err)

	// Get file node ID
	fileResult, err := e.Execute(ctx, `
		MATCH (f:File {path: '/app/docs/TEST.md'})
		RETURN id(f) as fileId
	`, nil)
	require.NoError(t, err)
	require.Len(t, fileResult.Rows, 1)
	fileNodeId := fileResult.Rows[0][0]

	// Use exact query format with SET on separate line
	_, err = e.Execute(ctx, `
		MATCH (f:File) WHERE id(f) = $fileNodeId
		MERGE (c:FileChunk:Node {id: $chunkId})
		SET
			c.chunk_index = $chunkIndex,
			c.text = $text,
			c.type = 'file_chunk',
			c.parent_file_id = $parentFileId
		MERGE (f)-[:HAS_CHUNK {index: $chunkIndex}]->(c)
	`, map[string]interface{}{
		"fileNodeId":   fileNodeId,
		"chunkId":      "chunk-test-0-xyz",
		"chunkIndex":   0,
		"text":         "Test content",
		"parentFileId": fileNodeId,
	})
	require.NoError(t, err)

	// Verify chunk was created with ALL properties including type
	chunkResult, err := e.Execute(ctx, `
		MATCH (c:FileChunk {id: 'chunk-test-0-xyz'})
		RETURN c.text, c.chunk_index, c.type, c.parent_file_id
	`, nil)
	require.NoError(t, err)
	require.Len(t, chunkResult.Rows, 1, "FileChunk should exist")
	assert.Equal(t, "Test content", chunkResult.Rows[0][0], "text should match")
	assert.Equal(t, int64(0), chunkResult.Rows[0][1], "chunk_index should match")
	assert.Equal(t, "file_chunk", chunkResult.Rows[0][2], "type MUST be set to 'file_chunk'")
	assert.Equal(t, fileNodeId, chunkResult.Rows[0][3], "parent_file_id should match")
}

// ========================================
// MERGE with Parameters Tests
// ========================================

func TestMerge_WithParameters(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	params := map[string]interface{}{
		"nodeId":   "param-node-1",
		"nodeName": "Test Node",
	}

	// MERGE with parameters
	_, err := e.Execute(ctx, `
		MERGE (n:ParamTest {id: $nodeId})
		SET n.name = $nodeName
	`, params)
	require.NoError(t, err)

	// Verify node was created with correct values
	result, err := e.Execute(ctx, `
		MATCH (n:ParamTest {id: 'param-node-1'})
		RETURN n.name
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "Test Node", result.Rows[0][0])
}

func TestExecuteMergeRelSegment_ErrorBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	a := &storage.Node{ID: "a1", Labels: []string{"A"}, Properties: map[string]interface{}{}}
	b := &storage.Node{ID: "b1", Labels: []string{"B"}, Properties: map[string]interface{}{}}
	_, err := store.CreateNode(a)
	require.NoError(t, err)
	_, err = store.CreateNode(b)
	require.NoError(t, err)

	// Missing start node closing paren.
	err = e.executeMergeRelSegment(ctx, "(a-[:REL]->(b)", map[string]*storage.Node{"a": a, "b": b})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "start node variable")

	// Missing relationship brackets.
	err = e.executeMergeRelSegment(ctx, "(a)-REL->(b)", map[string]*storage.Node{"a": a, "b": b})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing relationship brackets")

	// Missing start var in context.
	err = e.executeMergeRelSegment(ctx, "(a)-[:REL]->(b)", map[string]*storage.Node{"b": b})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "start node variable")

	// Missing end var in context.
	err = e.executeMergeRelSegment(ctx, "(a)-[:REL]->(b)", map[string]*storage.Node{"a": a})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "end node variable")
}

func TestSplitMergeChainClauseBlock_Branches(t *testing.T) {
	assert.Nil(t, splitMergeChainClauseBlock("   "))

	// No recognized keyword -> returns whole block as one clause.
	one := splitMergeChainClauseBlock("x = 1")
	require.Len(t, one, 1)
	assert.Equal(t, "x = 1", one[0])

	// Leading noise should be skipped to first recognized keyword.
	clauses := splitMergeChainClauseBlock("foo bar MATCH (n) RETURN n")
	require.Len(t, clauses, 2)
	assert.Equal(t, "MATCH (n)", clauses[0])
	assert.Equal(t, "RETURN n", clauses[1])

	// Mixed clause chain with intermediate text: parser should still split at known clause starts.
	clauses = splitMergeChainClauseBlock("MATCH (n) junk OPTIONAL MATCH (m) FOREACH (x IN [1] | SET m.v = x) RETURN m")
	require.Len(t, clauses, 4)
	assert.Equal(t, "MATCH (n) junk", clauses[0])
	assert.Equal(t, "OPTIONAL MATCH (m)", clauses[1])
	assert.Equal(t, "FOREACH (x IN [1] | SET m.v = x)", clauses[2])
	assert.Equal(t, "RETURN m", clauses[3])

	// Unknown tail after first clause is kept as part of that clause when no next keyword exists.
	clauses = splitMergeChainClauseBlock("MATCH (n) trailing noise")
	require.Len(t, clauses, 1)
	assert.Equal(t, "MATCH (n) trailing noise", clauses[0])
}

func TestApplyWithProjection_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	n := &storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{}}
	r := &storage.Edge{ID: "e1", Type: "KNOWS", StartNode: "n1", EndNode: "n1", Properties: map[string]interface{}{}}
	nodeCtx := map[string]*storage.Node{"n": n}
	relCtx := map[string]*storage.Edge{"r": r}
	scalarCtx := map[string]interface{}{"answer": int64(42)}
	ctx := context.Background()

	remaining, keptNodes, keptRels, keptScalars := exec.applyWithProjection(ctx, "* MATCH (n) RETURN n", nodeCtx, relCtx, scalarCtx)
	assert.Equal(t, "MATCH (n) RETURN n", remaining)
	assert.Equal(t, nodeCtx, keptNodes)
	assert.Equal(t, relCtx, keptRels)
	assert.Equal(t, scalarCtx, keptScalars)

	remaining, keptNodes, keptRels, keptScalars = exec.applyWithProjection(ctx, "n RETURN n", nodeCtx, relCtx, scalarCtx)
	assert.Equal(t, "RETURN n", remaining)
	require.Contains(t, keptNodes, "n")
	assert.Empty(t, keptRels)
	assert.Empty(t, keptScalars)

	remaining, keptNodes, keptRels, keptScalars = exec.applyWithProjection(ctx, "answer AS projected RETURN projected", nodeCtx, relCtx, scalarCtx)
	assert.Equal(t, "RETURN projected", remaining)
	assert.Empty(t, keptNodes)
	assert.Empty(t, keptRels)
	assert.Equal(t, map[string]interface{}{"projected": int64(42)}, keptScalars)

	// Non-matching projection drops context keys not explicitly projected.
	remaining, keptNodes, keptRels, keptScalars = exec.applyWithProjection(ctx, "n + 1", nodeCtx, relCtx, scalarCtx)
	assert.Equal(t, "", remaining)
	assert.Empty(t, keptNodes)
	assert.Empty(t, keptRels)
	assert.Empty(t, keptScalars)
}

func TestExecuteMergeWithChain_AdditionalBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	e := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := e.Execute(ctx, "CREATE (c:Category {name:'catA'}), (a:TypeA {name:'A1'})", nil)
	require.NoError(t, err)

	// Chain-break branch: MATCH miss in middle should produce 0 rows while prior MERGE still succeeds.
	brokenRes, err := e.executeMergeWithChain(ctx, `
		MERGE (ent:Entry {id:'entry-break'})
		ON CREATE SET ent.created = true
		WITH ent
		MATCH (c:Category {name:'missing'})
		MERGE (ent)-[:IN_CATEGORY]->(c)
		RETURN ent.id
	`)
	require.NoError(t, err)
	assert.Equal(t, []string{"ent.id"}, brokenRes.Columns)
	assert.Empty(t, brokenRes.Rows)

	verifyBroken, err := e.Execute(ctx, "MATCH (e:Entry {id:'entry-break'}) RETURN count(e)", nil)
	require.NoError(t, err)
	require.Len(t, verifyBroken.Rows, 1)
	assert.Equal(t, int64(1), verifyBroken.Rows[0][0])

	// OPTIONAL MATCH + FOREACH clause handling in chain segments.
	okRes, err := e.executeMergeWithChain(ctx, `
		MERGE (ent:Entry {id:'entry-ok'})
		ON CREATE SET ent.created = true
		WITH ent
		OPTIONAL MATCH (a:TypeA {name:'A1'})
		FOREACH (_ IN CASE WHEN a IS NULL THEN [] ELSE [1] END | MERGE (ent)-[:HAS_A]->(a))
		RETURN ent.id
	`)
	require.NoError(t, err)
	require.Len(t, okRes.Rows, 1)
	assert.Equal(t, "entry-ok", okRes.Rows[0][0])
}

func TestSplitMergeChainSegments_AdditionalBranches(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	segments := e.splitMergeChainSegments(`
		MERGE (n:Word {name:'x'})
		WITH n
		MATCH (m:Word) WHERE m.name STARTS WITH 'x'
		RETURN n.name
	`)
	require.GreaterOrEqual(t, len(segments), 3)
	assert.True(t, strings.HasPrefix(strings.TrimSpace(segments[0]), "MERGE"))
	assert.Contains(t, strings.ToUpper(segments[len(segments)-1]), "RETURN")

	one := e.splitMergeChainSegments("MERGE (n:Solo {id:'1'}) RETURN n.id")
	require.Len(t, one, 1)
	assert.Contains(t, one[0], "MERGE (n:Solo")
}

func TestSplitMultipleMerges_WithScalarProjectionTail(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	segments := e.splitMultipleMerges(strings.TrimSpace(`
WITH 'entity-single' AS entity_id, 'calls' AS relation_type, 'state-single' AS state_id, 'commit-single-row' AS commit_hash
MATCH (ck:CodeKey {entity_id: entity_id, relation_type: relation_type})
MATCH (cs:CodeState {state_id: state_id})
MATCH (c:Commit {hash: commit_hash})
MERGE (ck)-[:HAS_STATE]->(cs)
MERGE (c)-[:CHANGED]->(cs)
MERGE (c)-[:TOUCHED]->(ck)
`))
	require.Equal(t, []string{
		"WITH 'entity-single' AS entity_id, 'calls' AS relation_type, 'state-single' AS state_id, 'commit-single-row' AS commit_hash",
		"MATCH (ck:CodeKey {entity_id: entity_id, relation_type: relation_type})",
		"MATCH (cs:CodeState {state_id: state_id})",
		"MATCH (c:Commit {hash: commit_hash})",
		"MERGE (ck)-[:HAS_STATE]->(cs)",
		"MERGE (c)-[:CHANGED]->(cs)",
		"MERGE (c)-[:TOUCHED]->(ck)",
	}, segments)
}

func TestSplitMultipleMerges_FullFallbackRowShape(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	segments := e.splitMultipleMerges(strings.TrimSpace(`
MERGE (ck:CodeKey {entity_id: 'entity-single', relation_type: 'calls'})
MERGE (cs:CodeState {state_id: 'state-single'})
SET cs.code_key = 'repo_fact|calls|single',
    cs.tx_id = 'tx-single',
    cs.commit_hash = 'commit-single-row',
    cs.valid_from_iso = '2026-03-20T20:22:20Z',
    cs.valid_from = datetime('2026-03-20T20:22:20Z'),
    cs.value_json = '{"repo":"git-to-graph","source":"single-a","target":"single-b"}',
    cs.valid_to = CASE WHEN null IS NULL THEN null ELSE datetime(null) END,
    cs.asserted_at = datetime('2026-03-20T20:22:20Z'),
    cs.asserted_by = 'TJ Sweet',
    cs.semantic_type = 'CallEdgeVersion'
MERGE (c:Commit {hash: 'commit-single-row'})
ON CREATE SET c.timestamp = datetime('2026-03-20T20:22:20Z'), c.tx_id = 'tx-single', c.actor = 'TJ Sweet'
WITH 'entity-single' AS entity_id, 'calls' AS relation_type, 'state-single' AS state_id, 'commit-single-row' AS commit_hash
MATCH (ck:CodeKey {entity_id: entity_id, relation_type: relation_type})
MATCH (cs:CodeState {state_id: state_id})
MATCH (c:Commit {hash: commit_hash})
MERGE (ck)-[:HAS_STATE]->(cs)
MERGE (c)-[:CHANGED]->(cs)
MERGE (c)-[:TOUCHED]->(ck)
`))
	require.Equal(t, []string{
		"MERGE (ck:CodeKey {entity_id: 'entity-single', relation_type: 'calls'})",
		"MERGE (cs:CodeState {state_id: 'state-single'})\nSET cs.code_key = 'repo_fact|calls|single',\n    cs.tx_id = 'tx-single',\n    cs.commit_hash = 'commit-single-row',\n    cs.valid_from_iso = '2026-03-20T20:22:20Z',\n    cs.valid_from = datetime('2026-03-20T20:22:20Z'),\n    cs.value_json = '{\"repo\":\"git-to-graph\",\"source\":\"single-a\",\"target\":\"single-b\"}',\n    cs.valid_to = CASE WHEN null IS NULL THEN null ELSE datetime(null) END,\n    cs.asserted_at = datetime('2026-03-20T20:22:20Z'),\n    cs.asserted_by = 'TJ Sweet',\n    cs.semantic_type = 'CallEdgeVersion'",
		"MERGE (c:Commit {hash: 'commit-single-row'})\nON CREATE SET c.timestamp = datetime('2026-03-20T20:22:20Z'), c.tx_id = 'tx-single', c.actor = 'TJ Sweet'",
		"WITH 'entity-single' AS entity_id, 'calls' AS relation_type, 'state-single' AS state_id, 'commit-single-row' AS commit_hash",
		"MATCH (ck:CodeKey {entity_id: entity_id, relation_type: relation_type})",
		"MATCH (cs:CodeState {state_id: state_id})",
		"MATCH (c:Commit {hash: commit_hash})",
		"MERGE (ck)-[:HAS_STATE]->(cs)",
		"MERGE (c)-[:CHANGED]->(cs)",
		"MERGE (c)-[:TOUCHED]->(ck)",
	}, segments)
}

func TestProjectWithContext_PreservesScalarAliases(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	nodes, rels, scalars := exec.projectWithContext(ctx,
		`'entity-single' AS entity_id, 'calls' AS relation_type, 'state-single' AS state_id, 'commit-single-row' AS commit_hash`,
		map[string]*storage.Node{},
		map[string]*storage.Edge{},
		nil,
	)
	assert.Empty(t, nodes)
	assert.Empty(t, rels)
	assert.Equal(t, map[string]interface{}{
		"entity_id":     "entity-single",
		"relation_type": "calls",
		"state_id":      "state-single",
		"commit_hash":   "commit-single-row",
	}, scalars)
}

func TestExecuteMatchSegment_ResolvesScalarBindings(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "ck-1", Labels: []string{"CodeKey"}, Properties: map[string]interface{}{"entity_id": "entity-single", "relation_type": "calls"}})
	require.NoError(t, err)

	exec.fabricRecordBindings = map[string]interface{}{
		"entity_id":     "entity-single",
		"relation_type": "calls",
	}
	t.Cleanup(func() { exec.fabricRecordBindings = nil })

	node, varName, err := exec.executeMatchSegment(ctx, `MATCH (ck:CodeKey {entity_id: entity_id, relation_type: relation_type})`, map[string]*storage.Node{})
	require.NoError(t, err)
	require.NotNil(t, node)
	require.Equal(t, "ck", varName)
	require.Equal(t, "entity-single", node.Properties["entity_id"])
}

func TestExecuteMerge_UnsubstitutedParamAndFallbackPatternBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Unsubstituted parameter branch should proceed without panicking.
	res, err := exec.executeMerge(ctx, "MERGE (n:Doc {path: $path}) RETURN n")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)

	// Fallback parse branch for degenerate MERGE pattern.
	fallback, err := exec.executeMerge(ctx, "MERGE () RETURN n")
	require.NoError(t, err)
	require.Equal(t, []string{"n"}, fallback.Columns)
	require.Len(t, fallback.Rows, 1)
	require.NotNil(t, fallback.Rows[0][0])
}

func TestExecuteMerge_RecoversFromDuplicateCreateForParameterizedRepositoryMerge(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	require.NoError(t, store.GetSchema().AddUniqueConstraint("repo_path", "Repository", "path"))

	_, err := store.CreateNode(&storage.Node{
		ID:     "repo-1",
		Labels: []string{"Repository"},
		Properties: map[string]interface{}{
			"path":          "/Users/timothysweet/src/my-CodeGraphContext",
			"name":          "old-name",
			"is_dependency": true,
		},
	})
	require.NoError(t, err)

	exec := NewStorageExecutor(&staleMergeLookupEngine{
		Engine:      store,
		hiddenLabel: "Repository",
	})
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
		MERGE (r:Repository {path: $path})
		SET r.name = $name, r.is_dependency = $is_dependency
		RETURN r.path, r.name, r.is_dependency
	`, map[string]interface{}{
		"path":          "/Users/timothysweet/src/my-CodeGraphContext",
		"name":          "my-CodeGraphContext",
		"is_dependency": false,
	})

	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "/Users/timothysweet/src/my-CodeGraphContext", result.Rows[0][0])
	assert.Equal(t, "my-CodeGraphContext", result.Rows[0][1])
	assert.Equal(t, false, result.Rows[0][2])

	nodes, err := store.GetNodesByLabel("Repository")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.Equal(t, "/Users/timothysweet/src/my-CodeGraphContext", nodes[0].Properties["path"])
	assert.Equal(t, "my-CodeGraphContext", nodes[0].Properties["name"])
	assert.Equal(t, false, nodes[0].Properties["is_dependency"])
}

func TestExecuteMerge_CreatesNodeWhenLegacySequentialIDWouldCollide(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	require.NoError(t, store.GetSchema().AddUniqueConstraint("file_path", "File", "path"))

	_, err := store.CreateNode(&storage.Node{
		ID:     "node-1",
		Labels: []string{"Scratch"},
		Properties: map[string]interface{}{
			"name": "occupied-legacy-id",
		},
	})
	require.NoError(t, err)

	idCounter = 0

	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
		MERGE (f:File {path: $path})
		SET f.name = $name, f.relative_path = $relative_path, f.is_dependency = $is_dependency
		RETURN f.path, f.name
	`, map[string]interface{}{
		"path":          "/Users/timothysweet/src/my-CodeGraphContext/cgc_entry.py",
		"name":          "cgc_entry.py",
		"relative_path": "cgc_entry.py",
		"is_dependency": false,
	})

	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "/Users/timothysweet/src/my-CodeGraphContext/cgc_entry.py", result.Rows[0][0])
	assert.Equal(t, "cgc_entry.py", result.Rows[0][1])

	files, err := store.GetNodesByLabel("File")
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.Equal(t, "/Users/timothysweet/src/my-CodeGraphContext/cgc_entry.py", files[0].Properties["path"])
	assert.NotEqual(t, storage.NodeID("node-1"), files[0].ID)
}

func TestExecuteCompoundMatchMerge_RecoversFromDuplicateCreateForParameterizedParameterMerge(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	require.NoError(t, store.GetSchema().AddConstraint(storage.Constraint{
		Name:       "parameter_unique",
		Type:       storage.ConstraintNodeKey,
		Label:      "Parameter",
		Properties: []string{"name", "path", "function_line_number"},
	}))

	_, err := store.CreateNode(&storage.Node{
		ID:     "fn-1",
		Labels: []string{"Function"},
		Properties: map[string]interface{}{
			"name":        "run",
			"path":        "/Users/timothysweet/src/my-CodeGraphContext/tests/unit/tools/test_indexing_scalability.py",
			"line_number": int64(365),
		},
	})
	require.NoError(t, err)

	_, err = store.CreateNode(&storage.Node{
		ID:     "param-1",
		Labels: []string{"Parameter"},
		Properties: map[string]interface{}{
			"name":                 "query",
			"path":                 "/Users/timothysweet/src/my-CodeGraphContext/tests/unit/tools/test_indexing_scalability.py",
			"function_line_number": int64(365),
		},
	})
	require.NoError(t, err)

	exec := NewStorageExecutor(&staleMergeLookupEngine{
		Engine:      store,
		hiddenLabel: "Parameter",
	})
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
		MATCH (fn:Function {name: $func_name, path: $path, line_number: $function_line_number})
		MERGE (p:Parameter {name: $name, path: $path, function_line_number: $function_line_number})
		MERGE (fn)-[:HAS_PARAMETER]->(p)
		RETURN p.name, p.path, p.function_line_number
	`, map[string]interface{}{
		"func_name":            "run",
		"path":                 "/Users/timothysweet/src/my-CodeGraphContext/tests/unit/tools/test_indexing_scalability.py",
		"function_line_number": int64(365),
		"name":                 "query",
	})

	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "query", result.Rows[0][0])
	assert.Equal(t, "/Users/timothysweet/src/my-CodeGraphContext/tests/unit/tools/test_indexing_scalability.py", result.Rows[0][1])
	assert.Equal(t, int64(365), result.Rows[0][2])

	params, err := store.GetNodesByLabel("Parameter")
	require.NoError(t, err)
	require.Len(t, params, 1)
	assert.Equal(t, "query", params[0].Properties["name"])
	assert.Equal(t, int64(365), params[0].Properties["function_line_number"])

	fn, err := store.GetFirstNodeByLabel("Function")
	require.NoError(t, err)
	param := params[0]
	edges, err := store.GetEdgesBetween(fn.ID, param.ID)
	require.NoError(t, err)
	require.Len(t, edges, 1)
	assert.Equal(t, "HAS_PARAMETER", edges[0].Type)
}

func TestExecuteCompoundMatchMerge_SecondMergeAndErrorBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:         "match-merge-seed",
		Labels:     []string{"P"},
		Properties: map[string]interface{}{"name": "seed"},
	})
	require.NoError(t, err)

	// First MERGE before MATCH forces "find second MERGE after MATCH" branch.
	res, err := exec.executeCompoundMatchMerge(
		ctx,
		"MERGE (pre:Scratch {id:'pre'}) MATCH (a:P) MERGE (m:Merged {id:'ok'}) RETURN m",
	)
	require.NoError(t, err)
	require.NotNil(t, res)

	// MATCH parse failure path should bubble up deterministic error.
	_, err = exec.executeCompoundMatchMerge(ctx, "MATCH (a:P MERGE (m:Broken {id:'x'})")
	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "match")

	// OPTIONAL MATCH with empty result should still execute MERGE branch.
	opt, err := exec.executeCompoundMatchMerge(ctx, "OPTIONAL MATCH (a:Missing) MERGE (m:Merged {id:'opt'}) RETURN m")
	require.NoError(t, err)
	require.NotNil(t, opt)
}

func TestExecuteMergeNodeAndMatchSegment_AdditionalBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seed := &storage.Node{
		ID:         "merge-seed",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "alice", "age": int64(30)},
	}
	_, err := store.CreateNode(seed)
	require.NoError(t, err)

	// Existing node + ON MATCH SET branch.
	node, varName, err := exec.executeMergeNodeSegment(ctx, "MERGE (p:Person {name:'alice'}) ON MATCH SET p.age = 31")
	require.NoError(t, err)
	require.Equal(t, "p", varName)
	require.NotNil(t, node)
	assert.Equal(t, int64(31), node.Properties["age"])

	// Create node + ON CREATE SET branch.
	created, createdVar, err := exec.executeMergeNodeSegment(ctx, "MERGE (q:Person {name:'bob'}) ON CREATE SET q.city = 'phx'")
	require.NoError(t, err)
	require.Equal(t, "q", createdVar)
	require.NotNil(t, created)
	assert.Equal(t, "phx", created.Properties["city"])

	// Syntax guard branches.
	_, _, err = exec.executeMergeNodeSegment(ctx, "MATCH (n)")
	require.Error(t, err)
	_, _, err = exec.executeMergeNodeSegment(ctx, "MERGE bad-pattern")
	require.Error(t, err)

	// executeMatchSegment: missing MATCH keyword.
	_, _, err = exec.executeMatchSegment(ctx, "(n:Person)", map[string]*storage.Node{})
	require.Error(t, err)

	// Bound-variable fast return.
	bound := &storage.Node{ID: "bound-1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bound"}}
	got, gotVar, err := exec.executeMatchSegment(ctx, "MATCH (n:Person {name:'alice'})", map[string]*storage.Node{"n": bound})
	require.NoError(t, err)
	assert.Equal(t, "n", gotVar)
	assert.Equal(t, bound, got)

	// AllNodes path (no label) and no-match branch.
	got, gotVar, err = exec.executeMatchSegment(ctx, "MATCH (x {name:'alice'})", map[string]*storage.Node{})
	require.NoError(t, err)
	assert.Equal(t, "x", gotVar)
	require.NotNil(t, got)

	got, gotVar, err = exec.executeMatchSegment(ctx, "MATCH (z:Person {name:'missing'})", map[string]*storage.Node{})
	require.NoError(t, err)
	assert.Equal(t, "z", gotVar)
	assert.Nil(t, got)
}

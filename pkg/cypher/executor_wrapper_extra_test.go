package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionStorageWrapper_PrefixHelpers(t *testing.T) {
	w := &transactionStorageWrapper{namespace: "tenant", separator: ":"}
	assert.Equal(t, storage.NodeID("tenant:n1"), w.prefixNodeID("n1"))
	assert.Equal(t, storage.NodeID("tenant:n1"), w.prefixNodeID("tenant:n1"))
	assert.Equal(t, storage.NodeID("n1"), w.unprefixNodeID("tenant:n1"))
	assert.Equal(t, storage.NodeID("n1"), w.unprefixNodeID("n1"))
	assert.Equal(t, storage.EdgeID("tenant:e1"), w.prefixEdgeID("e1"))
	assert.Equal(t, storage.EdgeID("tenant:e1"), w.prefixEdgeID("tenant:e1"))
	assert.Equal(t, storage.EdgeID("e1"), w.unprefixEdgeID("tenant:e1"))
	assert.Equal(t, storage.EdgeID("e1"), w.unprefixEdgeID("e1"))

	w2 := &transactionStorageWrapper{}
	assert.Equal(t, storage.NodeID("n1"), w2.prefixNodeID("n1"))
	assert.Equal(t, storage.EdgeID("e1"), w2.prefixEdgeID("e1"))
}

func TestTransactionStorageWrapper_CreateGetDelete_WithNamespace(t *testing.T) {
	eng := newTestMemoryEngine(t)
	defer eng.Close()
	tx, err := eng.BeginTransaction()
	require.NoError(t, err)

	w := &transactionStorageWrapper{tx: tx, underlying: eng, namespace: "tenant", separator: ":"}

	n := &storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}}
	createdID, err := w.CreateNode(n)
	require.NoError(t, err)
	assert.Equal(t, storage.NodeID("n1"), createdID)

	got, err := w.GetNode("n1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, storage.NodeID("n1"), got.ID)

	err = w.UpdateNode(&storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "bob"}})
	require.NoError(t, err)

	err = w.DeleteNode("n1")
	require.NoError(t, err)
}

func TestTransactionStorageWrapper_BulkOps_AndCounts(t *testing.T) {
	eng := newTestMemoryEngine(t)
	defer eng.Close()

	// Seed underlying storage first so the transaction snapshot can read/delete them.
	_, err := eng.CreateNode(&storage.Node{ID: "test:r1", Labels: []string{"Person"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "test:r2", Labels: []string{"Person"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	err = eng.CreateEdge(&storage.Edge{ID: "test:re1", StartNode: "test:r1", EndNode: "test:r2", Type: "REL", Properties: map[string]interface{}{}})
	require.NoError(t, err)

	tx, err := eng.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	w := &transactionStorageWrapper{tx: tx, underlying: eng, namespace: "", separator: ":"}

	// Cover bulk-create transaction paths (writes are staged in tx, not immediately visible
	// through underlying delegated read methods).
	nodes := []*storage.Node{
		{ID: "test:n1", Labels: []string{"Person"}, Properties: map[string]interface{}{}},
		{ID: "test:n2", Labels: []string{"Person"}, Properties: map[string]interface{}{}},
	}
	require.NoError(t, w.BulkCreateNodes(nodes))

	edges := []*storage.Edge{{ID: "test:e1", StartNode: "test:n1", EndNode: "test:n2", Type: "REL", Properties: map[string]interface{}{}}}
	require.NoError(t, w.BulkCreateEdges(edges))

	// BatchGetNodes and delegate methods
	gotMap, err := w.BatchGetNodes([]storage.NodeID{"test:r1", "test:r2"})
	require.NoError(t, err)
	assert.Len(t, gotMap, 2)

	allNodes, err := w.AllNodes()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(allNodes), 2)

	allEdges, err := w.AllEdges()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(allEdges), 1)

	firstByLabel, err := w.GetFirstNodeByLabel("Person")
	require.NoError(t, err)
	assert.NotNil(t, firstByLabel)

	edgesBetween, err := w.GetEdgesBetween("test:r1", "test:r2")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edgesBetween), 1)

	edgesByType, err := w.GetEdgesByType("REL")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edgesByType), 1)

	assert.GreaterOrEqual(t, len(w.GetAllNodes()), 2)
	assert.GreaterOrEqual(t, w.GetOutDegree("test:r1"), 0)
	assert.GreaterOrEqual(t, w.GetInDegree("test:r2"), 0)
	assert.NotNil(t, w.GetSchema())

	_, err = w.NodeCount()
	require.NoError(t, err)
	_, err = w.EdgeCount()
	require.NoError(t, err)

	require.NoError(t, w.BulkDeleteEdges([]storage.EdgeID{"test:re1"}))
	require.NoError(t, w.BulkDeleteNodes([]storage.NodeID{"test:r1", "test:r2"}))

	err = w.Close()
	require.NoError(t, err)

	_, _, err = w.DeleteByPrefix("test:")
	assert.Error(t, err)
}

func TestTransactionStorageWrapper_BulkOps_WithNamespace(t *testing.T) {
	eng := newTestMemoryEngine(t)
	defer eng.Close()

	tx, err := eng.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	w := &transactionStorageWrapper{tx: tx, underlying: eng, namespace: "tenant", separator: ":"}

	nodes := []*storage.Node{
		{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "a"}},
		{ID: "n2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "b"}},
	}
	require.NoError(t, w.BulkCreateNodes(nodes))

	edges := []*storage.Edge{
		{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS", Properties: map[string]interface{}{}},
	}
	require.NoError(t, w.BulkCreateEdges(edges))

	require.NoError(t, tx.Commit())

	// Ensure transactional create path wrote namespaced IDs.
	createdNode, err := eng.GetNode("tenant:n1")
	require.NoError(t, err)
	require.NotNil(t, createdNode)
	assert.Equal(t, storage.NodeID("tenant:n1"), createdNode.ID)

	createdEdge, err := eng.GetEdge("tenant:e1")
	require.NoError(t, err)
	require.NotNil(t, createdEdge)
	assert.Equal(t, storage.EdgeID("tenant:e1"), createdEdge.ID)
	assert.Equal(t, storage.NodeID("tenant:n1"), createdEdge.StartNode)
	assert.Equal(t, storage.NodeID("tenant:n2"), createdEdge.EndNode)
}

func TestTransactionStorageWrapper_BulkDeleteEdges_WithNamespaceInvalidatesPreloadedCaches(t *testing.T) {
	eng := newTestMemoryEngine(t)
	defer eng.Close()

	ns := storage.NewNamespacedEngine(eng, "tenant")
	_, err := ns.CreateNode(&storage.Node{ID: "a", Labels: []string{"Entity"}})
	require.NoError(t, err)
	_, err = ns.CreateNode(&storage.Node{ID: "b", Labels: []string{"Entity"}})
	require.NoError(t, err)
	_, err = ns.CreateNode(&storage.Node{ID: "ep", Labels: []string{"Episodic"}})
	require.NoError(t, err)
	require.NoError(t, ns.CreateEdge(&storage.Edge{ID: "r1", StartNode: "a", EndNode: "b", Type: "RELATES_TO", Properties: map[string]interface{}{"uuid": "r1"}}))
	require.NoError(t, ns.CreateEdge(&storage.Edge{ID: "m1", StartNode: "ep", EndNode: "a", Type: "MENTIONS", Properties: map[string]interface{}{"uuid": "m1"}}))

	byType, err := ns.GetEdgesByType("RELATES_TO")
	require.NoError(t, err)
	require.Len(t, byType, 1)
	outgoingA, err := ns.GetOutgoingEdges("a")
	require.NoError(t, err)
	require.Len(t, outgoingA, 1)
	outgoingEP, err := ns.GetOutgoingEdges("ep")
	require.NoError(t, err)
	require.Len(t, outgoingEP, 1)

	tx, err := eng.BeginTransaction()
	require.NoError(t, err)
	w := &transactionStorageWrapper{tx: tx, underlying: eng, namespace: "tenant", separator: ":"}
	require.NoError(t, w.BulkDeleteEdges([]storage.EdgeID{"r1", "m1"}))
	require.NoError(t, tx.Commit())

	byType, err = ns.GetEdgesByType("RELATES_TO")
	require.NoError(t, err)
	require.Empty(t, byType)
	outgoingA, err = ns.GetOutgoingEdges("a")
	require.NoError(t, err)
	require.Empty(t, outgoingA)
	outgoingEP, err = ns.GetOutgoingEdges("ep")
	require.NoError(t, err)
	require.Empty(t, outgoingEP)
}

func TestTransactionStorageWrapper_ToUserNode_NilSafe(t *testing.T) {
	w := &transactionStorageWrapper{namespace: "tenant", separator: ":"}
	assert.Nil(t, w.toUserNode(nil))

	n := &storage.Node{ID: "tenant:n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"x": 1}}
	out := w.toUserNode(n)
	require.NotNil(t, out)
	assert.Equal(t, storage.NodeID("n1"), out.ID)
	assert.EqualValues(t, 1, out.Properties["x"])
}

func TestTransactionStorageWrapper_BulkDelete_ErrorPaths(t *testing.T) {
	eng := newTestMemoryEngine(t)
	defer eng.Close()

	tx, err := eng.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	w := &transactionStorageWrapper{tx: tx, underlying: eng, namespace: "", separator: ":"}

	err = w.BulkDeleteNodes([]storage.NodeID{"missing-node"})
	require.Error(t, err)

	err = w.BulkDeleteEdges([]storage.EdgeID{"missing-edge"})
	require.Error(t, err)
}

func TestTransactionStorageWrapper_BulkCreate_ErrorPaths(t *testing.T) {
	eng := newTestMemoryEngine(t)
	defer eng.Close()

	tx, err := eng.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	// Non-namespaced bulk create: duplicate ID should fail on second create.
	w := &transactionStorageWrapper{tx: tx, underlying: eng, namespace: "", separator: ":"}
	err = w.BulkCreateNodes([]*storage.Node{
		{ID: "dup", Labels: []string{"X"}, Properties: map[string]interface{}{}},
		{ID: "dup", Labels: []string{"X"}, Properties: map[string]interface{}{}},
	})
	require.Error(t, err)

	// Namespaced bulk edge create: duplicate edge ID should fail.
	tx2, err := eng.BeginTransaction()
	require.NoError(t, err)
	defer tx2.Rollback()
	wNS := &transactionStorageWrapper{tx: tx2, underlying: eng, namespace: "tenant", separator: ":"}
	err = wNS.BulkCreateNodes([]*storage.Node{
		{ID: "n1", Labels: []string{"X"}, Properties: map[string]interface{}{}},
		{ID: "n2", Labels: []string{"X"}, Properties: map[string]interface{}{}},
	})
	require.NoError(t, err)
	err = wNS.BulkCreateEdges([]*storage.Edge{
		{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "REL", Properties: map[string]interface{}{}},
		{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "REL", Properties: map[string]interface{}{}},
	})
	require.Error(t, err)
}

func TestExecuteSetTrailingUnwind_ErrorAndProjectionBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)

	node := &storage.Node{
		ID:         "p1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "alice"},
	}

	matchResult := &ExecuteResult{
		Columns: []string{"n", "x"},
		Rows:    [][]interface{}{{node, int64(7)}},
	}

	_, err := exec.executeSetTrailingUnwind(context.Background(), "RETURN 1", matchResult, &ExecuteResult{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "UNWIND clause expected")

	_, err = exec.executeSetTrailingUnwind(context.Background(), "UNWIND [1,2,3] RETURN 1", matchResult, &ExecuteResult{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "UNWIND requires AS clause")

	_, err = exec.executeSetTrailingUnwind(context.Background(), "UNWIND [1,2,3] AS item", matchResult, &ExecuteResult{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires RETURN clause")

	ctxWithParams := context.WithValue(context.Background(), paramsKey, map[string]interface{}{
		"vals": []interface{}{int64(10), int64(20)},
	})
	ok, err := exec.executeSetTrailingUnwind(
		ctxWithParams,
		"UNWIND ($vals) AS item RETURN item, n.name, x, n, toUpper(n.name), ghost.prop",
		matchResult,
		&ExecuteResult{},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"item", "n.name", "x", "n", "toUpper(n.name)", "ghost.prop"}, ok.Columns)
	require.Len(t, ok.Rows, 2)

	assert.Equal(t, int64(10), ok.Rows[0][0])
	assert.Equal(t, "alice", ok.Rows[0][1])
	assert.Equal(t, int64(7), ok.Rows[0][2])
	assert.Equal(t, node, ok.Rows[0][3])
	assert.Equal(t, "ALICE", ok.Rows[0][4])
	assert.Equal(t, "ghost.prop", ok.Rows[0][5])

	assert.Equal(t, int64(20), ok.Rows[1][0])
	assert.Equal(t, "alice", ok.Rows[1][1])
	assert.Equal(t, int64(7), ok.Rows[1][2])
	assert.Equal(t, node, ok.Rows[1][3])
	assert.Equal(t, "ALICE", ok.Rows[1][4])
	assert.Equal(t, "ghost.prop", ok.Rows[1][5])
}

func TestTryAsyncCreateNodeBatch_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Non-CREATE queries are not handled.
	_, err, handled := exec.tryAsyncCreateNodeBatch(ctx, "MATCH (n) RETURN n")
	require.NoError(t, err)
	assert.False(t, handled)

	// Schema/system CREATE queries must be routed elsewhere.
	_, err, handled = exec.tryAsyncCreateNodeBatch(ctx, "CREATE CONSTRAINT c IF NOT EXISTS FOR (n:Person) REQUIRE n.id IS UNIQUE")
	require.NoError(t, err)
	assert.False(t, handled)

	// Relationship patterns are not handled by node batch fast-path.
	_, err, handled = exec.tryAsyncCreateNodeBatch(ctx, "CREATE (a)-[:R]->(b) RETURN a")
	require.NoError(t, err)
	assert.False(t, handled)

	// Valid simple node create is handled and returns deterministic projection.
	res, err, handled := exec.tryAsyncCreateNodeBatch(ctx, "CREATE (n:Person {name:'alice'}), (m:Person {name:'bob'}) RETURN n.name AS n, m.name AS m")
	require.NoError(t, err)
	require.True(t, handled)
	require.NotNil(t, res)
	require.Len(t, res.Rows, 1)
	require.Equal(t, []string{"n", "m"}, res.Columns)
	assert.Equal(t, "alice", res.Rows[0][0])
	assert.Equal(t, "bob", res.Rows[0][1])
	require.NotNil(t, res.Stats)
	assert.Equal(t, 2, res.Stats.NodesCreated)
	require.NotNil(t, res.Metadata)
	optimisticRaw, ok := res.Metadata["optimistic"]
	require.True(t, ok)
	optimistic, ok := optimisticRaw.(*optimisticMutationMeta)
	require.True(t, ok)
	require.Len(t, optimistic.CreatedNodeIDs, 2)

	// Invalid identifiers should return handled errors (strict parsing).
	_, err, handled = exec.tryAsyncCreateNodeBatch(ctx, "CREATE (n:123bad {name:'x'}) RETURN n")
	require.True(t, handled)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid label name")
}

func TestExecuteCreateRelSegment_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	makeNode := func(id string) *storage.Node {
		n := &storage.Node{ID: storage.NodeID(id), Labels: []string{"N"}, Properties: map[string]interface{}{}}
		_, err := store.CreateNode(n)
		require.NoError(t, err)
		return n
	}

	a := makeNode("a")
	b := makeNode("b")

	t.Run("parse error", func(t *testing.T) {
		err := exec.executeCreateRelSegment(ctx, "CREATE (a)-[:KNOWS]->", map[string]*storage.Node{"a": a, "b": b}, map[string]*storage.Edge{}, &ExecuteResult{Stats: &QueryStats{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse relationship pattern")
	})

	t.Run("unbound endpoint creates and binds node", func(t *testing.T) {
		nodeCtx := map[string]*storage.Node{"a": a}
		result := &ExecuteResult{Stats: &QueryStats{}}
		err := exec.executeCreateRelSegment(ctx, "CREATE (a)-[:KNOWS]->(missing)", nodeCtx, map[string]*storage.Edge{}, result)
		require.NoError(t, err)
		require.Contains(t, nodeCtx, "missing")
		require.Equal(t, 1, result.Stats.NodesCreated)
		require.Equal(t, 1, result.Stats.RelationshipsCreated)
	})

	t.Run("empty source id", func(t *testing.T) {
		err := exec.executeCreateRelSegment(ctx, "CREATE (a)-[:KNOWS]->(b)", map[string]*storage.Node{"a": {ID: storage.NodeID("")}, "b": b}, map[string]*storage.Edge{}, &ExecuteResult{Stats: &QueryStats{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "source node a has empty ID")
	})

	t.Run("empty target id", func(t *testing.T) {
		err := exec.executeCreateRelSegment(ctx, "CREATE (a)-[:KNOWS]->(b)", map[string]*storage.Node{"a": a, "b": {ID: storage.NodeID("")}}, map[string]*storage.Edge{}, &ExecuteResult{Stats: &QueryStats{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "target node b has empty ID")
	})

	t.Run("relationship type required", func(t *testing.T) {
		err := exec.executeCreateRelSegment(ctx, "CREATE (a)-[r]->(b)", map[string]*storage.Node{"a": a, "b": b}, map[string]*storage.Edge{}, &ExecuteResult{Stats: &QueryStats{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "relationship type is required")
	})

	t.Run("forward and reverse creation", func(t *testing.T) {
		edgeCtx := map[string]*storage.Edge{}
		result := &ExecuteResult{Stats: &QueryStats{}}

		err := exec.executeCreateRelSegment(ctx, "CREATE (a)-[r:KNOWS {since: 2020}]->(b)", map[string]*storage.Node{"a": a, "b": b}, edgeCtx, result)
		require.NoError(t, err)
		require.Equal(t, 1, result.Stats.RelationshipsCreated)
		require.Contains(t, edgeCtx, "r")
		assert.Equal(t, storage.NodeID("a"), edgeCtx["r"].StartNode)
		assert.Equal(t, storage.NodeID("b"), edgeCtx["r"].EndNode)
		assert.EqualValues(t, 2020, edgeCtx["r"].Properties["since"])

		result2 := &ExecuteResult{Stats: &QueryStats{}}
		err = exec.executeCreateRelSegment(ctx, "CREATE (a)<-[r2:KNOWS]-(b)", map[string]*storage.Node{"a": a, "b": b}, edgeCtx, result2)
		require.NoError(t, err)
		require.Contains(t, edgeCtx, "r2")
		assert.Equal(t, storage.NodeID("b"), edgeCtx["r2"].StartNode)
		assert.Equal(t, storage.NodeID("a"), edgeCtx["r2"].EndNode)
		assert.Equal(t, 1, result2.Stats.RelationshipsCreated)
	})
}

func TestExecuteCallInTransactions_AdditionalBatchingBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Person {name:'a'}), (:Person {name:'b'}), (:Person {name:'c'})", nil)
	require.NoError(t, err)

	// Known-row-count path: read-only conversion succeeds, but write batch fails.
	_, err = exec.executeCallInTransactions(ctx, "MATCH (n:Person) SET n += 1 RETURN n.name AS name", 2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "batch 1/")

	// Guard branch error path for non-batchable writes.
	_, err = exec.executeCallInTransactions(ctx, "CREATE (n:TmpBad RETURN n", 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "batch 1 failed")
}

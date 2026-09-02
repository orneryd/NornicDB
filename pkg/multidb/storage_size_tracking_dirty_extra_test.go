package multidb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type dirtyBranchEngine struct {
	storage.Engine
	failGetNode       bool
	failGetEdge       bool
	failUpdateNode    bool
	failUpdateEdge    bool
	failDeleteNode    bool
	failDeleteEdge    bool
	failOutgoingEdges bool
	failIncomingEdges bool
	badNodeOnGet      bool
	badEdgeOnGet      bool
	outgoingEdges     []*storage.Edge
	incomingEdges     []*storage.Edge
}

type storageSizingErrorEngine struct {
	storage.Engine
	nodes    []*storage.Node
	edges    []*storage.Edge
	nodesErr error
	edgesErr error
}

type lifecycleDelegateEngine struct {
	storage.Engine
	registered       bool
	deregistered     bool
	pruned           bool
	paused           bool
	resumed          bool
	scheduleInterval time.Duration
}

type flipAfterGetEngine struct {
	storage.Engine
	nodeCalls int
	edgeCalls int
}

func (e *flipAfterGetEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	e.nodeCalls++
	if e.nodeCalls > 1 {
		return nil, storage.ErrNotFound
	}
	return e.Engine.GetNode(id)
}

func (e *flipAfterGetEngine) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	e.edgeCalls++
	if e.edgeCalls > 1 {
		return nil, storage.ErrNotFound
	}
	return e.Engine.GetEdge(id)
}

func (e *lifecycleDelegateEngine) RegisterSnapshotReader(storage.SnapshotReaderInfo) func() {
	e.registered = true
	return func() { e.deregistered = true }
}

func (e *lifecycleDelegateEngine) LifecycleStatus() map[string]interface{} {
	return map[string]interface{}{"enabled": true}
}

func (e *lifecycleDelegateEngine) TriggerPruneNow(context.Context) error {
	e.pruned = true
	return nil
}

func (e *lifecycleDelegateEngine) PauseLifecycle()  { e.paused = true }
func (e *lifecycleDelegateEngine) ResumeLifecycle() { e.resumed = true }

func (e *lifecycleDelegateEngine) SetLifecycleSchedule(interval time.Duration) error {
	e.scheduleInterval = interval
	return nil
}

func (e *lifecycleDelegateEngine) TopLifecycleDebtKeys(limit int) []storage.MVCCLifecycleDebtKey {
	return []storage.MVCCLifecycleDebtKey{{Namespace: "db", LogicalKey: "n1", DebtBytes: int64(limit)}}
}

func (e *storageSizingErrorEngine) AllNodes() ([]*storage.Node, error) {
	if e.nodesErr != nil {
		return nil, e.nodesErr
	}
	return e.nodes, nil
}

func (e *storageSizingErrorEngine) AllEdges() ([]*storage.Edge, error) {
	if e.edgesErr != nil {
		return nil, e.edgesErr
	}
	return e.edges, nil
}

func (e *dirtyBranchEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	if e.failGetNode {
		return nil, storage.ErrNotFound
	}
	if e.badNodeOnGet {
		return &storage.Node{ID: id, Properties: map[string]interface{}{"bad": make(chan int)}}, nil
	}
	return e.Engine.GetNode(id)
}

func (e *dirtyBranchEngine) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	if e.failGetEdge {
		return nil, storage.ErrNotFound
	}
	if e.badEdgeOnGet {
		return &storage.Edge{ID: id, Type: "BAD", Properties: map[string]interface{}{"bad": make(chan int)}}, nil
	}
	return e.Engine.GetEdge(id)
}

func (e *dirtyBranchEngine) UpdateNode(node *storage.Node) error {
	if e.failUpdateNode {
		return errors.New("update node failed")
	}
	return e.Engine.UpdateNode(node)
}

func (e *dirtyBranchEngine) DeleteNode(id storage.NodeID) error {
	if e.failDeleteNode {
		return errors.New("delete node failed")
	}
	return e.Engine.DeleteNode(id)
}

func (e *dirtyBranchEngine) UpdateEdge(edge *storage.Edge) error {
	if e.failUpdateEdge {
		return errors.New("update edge failed")
	}
	return e.Engine.UpdateEdge(edge)
}

func (e *dirtyBranchEngine) DeleteEdge(id storage.EdgeID) error {
	if e.failDeleteEdge {
		return errors.New("delete edge failed")
	}
	return e.Engine.DeleteEdge(id)
}

func (e *dirtyBranchEngine) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	if e.failOutgoingEdges {
		return nil, errors.New("outgoing unavailable")
	}
	if e.outgoingEdges != nil {
		return e.outgoingEdges, nil
	}
	return e.Engine.GetOutgoingEdges(id)
}

func (e *dirtyBranchEngine) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	if e.failIncomingEdges {
		return nil, errors.New("incoming unavailable")
	}
	if e.incomingEdges != nil {
		return e.incomingEdges, nil
	}
	return e.Engine.GetIncomingEdges(id)
}

func TestSizeTrackingEngine_MarksDirtyWhenCreatedNodeCannotBeReadBack(t *testing.T) {
	manager, dbName := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	inner := &dirtyBranchEngine{Engine: base, failGetNode: true}
	wrapped := newSizeTrackingEngine(inner, manager, dbName)

	id, err := wrapped.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":dirty-create-node"), Labels: []string{"Person"}})
	require.NoError(t, err)
	require.Equal(t, storage.NodeID(dbName+":dirty-create-node"), id)
	require.False(t, storageSizeInitialized(t, manager, dbName))
}

func TestSizeTrackingEngine_MarksDirtyWhenUpdatedNodeCannotBeSized(t *testing.T) {
	manager, dbName := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	nodeID := storage.NodeID(dbName + ":dirty-update-node")
	_, err := base.CreateNode(&storage.Node{ID: nodeID, Labels: []string{"Person"}})
	require.NoError(t, err)
	inner := &dirtyBranchEngine{Engine: base, failGetNode: true}
	wrapped := newSizeTrackingEngine(inner, manager, dbName)

	require.NoError(t, wrapped.UpdateNode(&storage.Node{
		ID:         nodeID,
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "updated"},
	}))
	require.False(t, storageSizeInitialized(t, manager, dbName))
}

func TestSizeTrackingEngine_MarksDirtyWhenNodeReadBackCannotBeEncoded(t *testing.T) {
	manager, dbName := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	inner := &dirtyBranchEngine{Engine: base, badNodeOnGet: true}
	wrapped := newSizeTrackingEngine(inner, manager, dbName)

	id := storage.NodeID(dbName + ":bad-node-size")
	createdID, err := wrapped.CreateNode(&storage.Node{ID: id, Labels: []string{"Person"}})
	require.NoError(t, err)
	require.Equal(t, id, createdID)
	require.False(t, storageSizeInitialized(t, manager, dbName))

	inner.badNodeOnGet = false
	require.NoError(t, manager.ensureStorageSizeInitialized(dbName, base))
	require.True(t, storageSizeInitialized(t, manager, dbName))
	inner.badNodeOnGet = true
	require.NoError(t, wrapped.UpdateNode(&storage.Node{ID: id, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "new"}}))
	require.False(t, storageSizeInitialized(t, manager, dbName))
}

func TestSizeTrackingEngine_MarksDirtyWhenCreatedEdgeCannotBeReadBack(t *testing.T) {
	manager, dbName := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	idA, err := base.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":dirty-edge-a"), Labels: []string{"Person"}})
	require.NoError(t, err)
	idB, err := base.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":dirty-edge-b"), Labels: []string{"Person"}})
	require.NoError(t, err)
	inner := &dirtyBranchEngine{Engine: base, failGetEdge: true}
	wrapped := newSizeTrackingEngine(inner, manager, dbName)

	require.NoError(t, wrapped.CreateEdge(&storage.Edge{ID: storage.EdgeID(dbName + ":dirty-create-edge"), StartNode: idA, EndNode: idB, Type: "KNOWS"}))
	require.False(t, storageSizeInitialized(t, manager, dbName))
}

func TestSizeTrackingEngine_MarksDirtyWhenEdgeReadBackCannotBeEncoded(t *testing.T) {
	manager, dbName := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	idA, err := base.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":bad-edge-a"), Labels: []string{"Person"}})
	require.NoError(t, err)
	idB, err := base.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":bad-edge-b"), Labels: []string{"Person"}})
	require.NoError(t, err)
	inner := &dirtyBranchEngine{Engine: base, badEdgeOnGet: true}
	wrapped := newSizeTrackingEngine(inner, manager, dbName)

	edgeID := storage.EdgeID(dbName + ":bad-edge-size")
	require.NoError(t, wrapped.CreateEdge(&storage.Edge{ID: edgeID, StartNode: idA, EndNode: idB, Type: "KNOWS"}))
	require.False(t, storageSizeInitialized(t, manager, dbName))

	inner.badEdgeOnGet = false
	require.NoError(t, manager.ensureStorageSizeInitialized(dbName, base))
	require.True(t, storageSizeInitialized(t, manager, dbName))
	inner.badEdgeOnGet = true
	require.NoError(t, wrapped.UpdateEdge(&storage.Edge{ID: edgeID, StartNode: idA, EndNode: idB, Type: "LIKES"}))
	require.False(t, storageSizeInitialized(t, manager, dbName))

	inner.badEdgeOnGet = false
	require.NoError(t, manager.ensureStorageSizeInitialized(dbName, base))
	require.True(t, storageSizeInitialized(t, manager, dbName))
	inner.badEdgeOnGet = true
	require.NoError(t, wrapped.DeleteEdge(edgeID))
	require.False(t, storageSizeInitialized(t, manager, dbName))
}

func TestSizeTrackingEngine_DeleteNodeMarksDirtyWhenConnectedEdgesFail(t *testing.T) {
	manager, dbName := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	idA, err := base.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":dirty-delete-a"), Labels: []string{"Person"}})
	require.NoError(t, err)
	idB, err := base.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":dirty-delete-b"), Labels: []string{"Person"}})
	require.NoError(t, err)
	require.NoError(t, base.CreateEdge(&storage.Edge{ID: storage.EdgeID(dbName + ":dirty-delete-edge"), StartNode: idA, EndNode: idB, Type: "KNOWS"}))
	inner := &dirtyBranchEngine{Engine: base, failOutgoingEdges: true}
	wrapped := newSizeTrackingEngine(inner, manager, dbName)

	require.NoError(t, wrapped.DeleteNode(idA))
	require.False(t, storageSizeInitialized(t, manager, dbName))
}

func TestSizeTrackingEngine_ConnectedEdgeBytesReturnsIncomingError(t *testing.T) {
	manager, dbName := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	nodeID, err := base.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":incoming-error-node"), Labels: []string{"Person"}})
	require.NoError(t, err)
	inner := &dirtyBranchEngine{Engine: base, failIncomingEdges: true}
	wrapper := newSizeTrackingEngine(inner, manager, dbName).(*sizeTrackingEngine)

	_, err = wrapper.connectedEdgeBytes(nodeID)
	require.ErrorContains(t, err, "get incoming edges")
}

func TestSizeTrackingEngineExposesInjectedQueryLimitChecker(t *testing.T) {
	manager, dbName := setupTestManager(t)
	checker := newDatabaseLimitCheckerWithLimits(manager, dbName, &Limits{})
	wrapper := newSizeTrackingEngine(storage.NewMemoryEngine(), manager, dbName, checker).(*sizeTrackingEngine)

	require.Same(t, checker, wrapper.GetQueryLimitChecker())
}

func TestSizeTrackingEngineConnectedEdgeBytesDeduplicatesSelfLoop(t *testing.T) {
	manager, dbName := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	nodeID, err := base.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":loop-node"), Labels: []string{"Person"}})
	require.NoError(t, err)
	edge := &storage.Edge{ID: storage.EdgeID(dbName + ":self-loop"), StartNode: nodeID, EndNode: nodeID, Type: "LOOPS", Properties: map[string]interface{}{"weight": 1}}
	require.NoError(t, base.CreateEdge(edge))
	wrapper := newSizeTrackingEngine(base, manager, dbName).(*sizeTrackingEngine)

	got, err := wrapper.connectedEdgeBytes(nodeID)
	require.NoError(t, err)
	want, err := calculateEdgeSize(edge)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestSizeTrackingEngineBulkOperationsStopOnFirstError(t *testing.T) {
	manager, dbName := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	wrapper := newSizeTrackingEngine(base, manager, dbName).(*sizeTrackingEngine)

	dupNode := storage.NodeID(dbName + ":bulk-dup-node")
	err := wrapper.BulkCreateNodes([]*storage.Node{
		{ID: dupNode, Labels: []string{"Person"}},
		{ID: dupNode, Labels: []string{"Person"}},
	})
	require.Error(t, err)

	idA, err := base.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":bulk-a"), Labels: []string{"Person"}})
	require.NoError(t, err)
	idB, err := base.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":bulk-b"), Labels: []string{"Person"}})
	require.NoError(t, err)
	dupEdge := storage.EdgeID(dbName + ":bulk-dup-edge")
	err = wrapper.BulkCreateEdges([]*storage.Edge{
		{ID: dupEdge, StartNode: idA, EndNode: idB, Type: "KNOWS"},
		{ID: dupEdge, StartNode: idA, EndNode: idB, Type: "KNOWS"},
	})
	require.Error(t, err)

	require.Error(t, wrapper.BulkDeleteNodes([]storage.NodeID{storage.NodeID(dbName + ":missing-node")}))
	require.Error(t, wrapper.BulkDeleteEdges([]storage.EdgeID{storage.EdgeID(dbName + ":missing-edge")}))
}

func TestSizeTrackingEngineLifecycleFallbacksOnPlainEngine(t *testing.T) {
	manager, dbName := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	wrapper := newSizeTrackingEngine(base, manager, dbName).(*sizeTrackingEngine)

	deregister := wrapper.RegisterSnapshotReader(storage.SnapshotReaderInfo{Namespace: dbName})
	require.NotNil(t, deregister)
	deregister()
	require.Equal(t, map[string]interface{}{"enabled": false}, wrapper.LifecycleStatus())
	require.NoError(t, wrapper.TriggerPruneNow(context.Background()))
	require.NotPanics(t, wrapper.PauseLifecycle)
	require.NotPanics(t, wrapper.ResumeLifecycle)
	require.NoError(t, wrapper.SetLifecycleSchedule(time.Second))
	require.Nil(t, wrapper.TopLifecycleDebtKeys(10))
}

func TestSizeTrackingEngineLifecycleDelegatesWhenSupported(t *testing.T) {
	manager, dbName := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	inner := &lifecycleDelegateEngine{Engine: base}
	wrapper := newSizeTrackingEngine(inner, manager, dbName).(*sizeTrackingEngine)

	deregister := wrapper.RegisterSnapshotReader(storage.SnapshotReaderInfo{Namespace: dbName})
	require.True(t, inner.registered)
	deregister()
	require.True(t, inner.deregistered)
	require.Equal(t, map[string]interface{}{"enabled": true}, wrapper.LifecycleStatus())
	require.NoError(t, wrapper.TriggerPruneNow(context.Background()))
	require.True(t, inner.pruned)
	wrapper.PauseLifecycle()
	wrapper.ResumeLifecycle()
	require.True(t, inner.paused)
	require.True(t, inner.resumed)
	require.NoError(t, wrapper.SetLifecycleSchedule(2*time.Second))
	require.Equal(t, 2*time.Second, inner.scheduleInterval)
	require.Equal(t, []storage.MVCCLifecycleDebtKey{{Namespace: "db", LogicalKey: "n1", DebtBytes: 3}}, wrapper.TopLifecycleDebtKeys(3))
}

func TestSizeTrackingEngineStreamingFallbackBranches(t *testing.T) {
	manager, dbName := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	plain := &dirtyBranchEngine{Engine: base}
	wrapper := newSizeTrackingEngine(plain, manager, dbName).(*sizeTrackingEngine)

	n1, err := base.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":stream-a"), Labels: []string{"Person"}})
	require.NoError(t, err)
	n2, err := base.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":stream-b"), Labels: []string{"Person"}})
	require.NoError(t, err)
	require.NoError(t, base.CreateEdge(&storage.Edge{ID: storage.EdgeID(dbName + ":stream-edge"), StartNode: n1, EndNode: n2, Type: "KNOWS"}))

	var nodeIDs []storage.NodeID
	require.NoError(t, wrapper.StreamNodes(context.Background(), func(node *storage.Node) error {
		nodeIDs = append(nodeIDs, node.ID)
		return nil
	}))
	require.ElementsMatch(t, []storage.NodeID{n1, n2}, nodeIDs)

	var chunks [][]*storage.Node
	require.NoError(t, wrapper.StreamNodeChunks(context.Background(), 0, func(nodes []*storage.Node) error {
		copied := append([]*storage.Node(nil), nodes...)
		chunks = append(chunks, copied)
		return nil
	}))
	require.Len(t, chunks, 2)

	stopCount := 0
	require.NoError(t, wrapper.StreamEdges(context.Background(), func(edge *storage.Edge) error {
		stopCount++
		return storage.ErrIterationStopped
	}))
	require.Equal(t, 1, stopCount)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, wrapper.StreamEdges(ctx, func(edge *storage.Edge) error { return nil }), context.Canceled)

	plain.failOutgoingEdges = true
	_, err = wrapper.connectedEdgeBytes(n1)
	require.ErrorContains(t, err, "get outgoing edges")
}

func TestSizeTrackingEngineMutationErrorBranches(t *testing.T) {
	manager, dbName := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	nodeID, err := base.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":mut-node"), Labels: []string{"Person"}})
	require.NoError(t, err)
	otherID, err := base.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":mut-other"), Labels: []string{"Person"}})
	require.NoError(t, err)
	edgeID := storage.EdgeID(dbName + ":mut-edge")
	require.NoError(t, base.CreateEdge(&storage.Edge{ID: edgeID, StartNode: nodeID, EndNode: otherID, Type: "KNOWS"}))
	inner := &dirtyBranchEngine{Engine: base}
	wrapper := newSizeTrackingEngine(inner, manager, dbName).(*sizeTrackingEngine)

	inner.failUpdateNode = true
	err = wrapper.UpdateNode(&storage.Node{ID: nodeID, Labels: []string{"Person"}, Properties: map[string]interface{}{"x": 1}})
	require.ErrorContains(t, err, "update node failed")
	inner.failUpdateNode = false

	inner.failUpdateEdge = true
	err = wrapper.UpdateEdge(&storage.Edge{ID: edgeID, StartNode: nodeID, EndNode: otherID, Type: "LIKES"})
	require.ErrorContains(t, err, "update edge failed")
	inner.failUpdateEdge = false

	inner.failDeleteEdge = true
	err = wrapper.DeleteEdge(edgeID)
	require.ErrorContains(t, err, "delete edge failed")
	inner.failDeleteEdge = false

	inner.failDeleteNode = true
	err = wrapper.DeleteNode(nodeID)
	require.ErrorContains(t, err, "delete node failed")
	inner.failDeleteNode = false
}

func TestSizeTrackingEngineConnectedEdgeBytesMalformedEdge(t *testing.T) {
	manager, dbName := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	nodeID, err := base.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":bad-connected-node"), Labels: []string{"Person"}})
	require.NoError(t, err)
	otherID, err := base.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":bad-connected-other"), Labels: []string{"Person"}})
	require.NoError(t, err)
	badEdge := &storage.Edge{
		ID:         storage.EdgeID(dbName + ":bad-connected-edge"),
		StartNode:  nodeID,
		EndNode:    otherID,
		Type:       "BAD",
		Properties: map[string]interface{}{"bad": make(chan int)},
	}
	inner := &dirtyBranchEngine{Engine: base, outgoingEdges: []*storage.Edge{badEdge}, incomingEdges: []*storage.Edge{}}
	wrapper := newSizeTrackingEngine(inner, manager, dbName).(*sizeTrackingEngine)

	_, err = wrapper.connectedEdgeBytes(nodeID)
	require.Error(t, err)
}

func TestApplyStorageSizeDeltaClampsBelowZero(t *testing.T) {
	manager, dbName := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	require.NoError(t, manager.ensureStorageSizeInitialized(dbName, base))

	manager.applyStorageSizeDelta(dbName, -1_000_000, -1_000_000)

	nodeBytes, edgeBytes, totalBytes := manager.GetStorageSize(dbName)
	require.Zero(t, nodeBytes)
	require.Zero(t, edgeBytes)
	require.Zero(t, totalBytes)
}

func TestSizeTrackingEngineMutationsPropagateMissingDatabase(t *testing.T) {
	manager, _ := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	wrapper := newSizeTrackingEngine(base, manager, "ghost_db")

	_, err := wrapper.CreateNode(&storage.Node{ID: "ghost-node", Labels: []string{"Ghost"}})
	require.ErrorIs(t, err, ErrDatabaseNotFound)
	require.ErrorIs(t, wrapper.UpdateNode(&storage.Node{ID: "ghost-node"}), ErrDatabaseNotFound)
	require.ErrorIs(t, wrapper.DeleteNode("ghost-node"), ErrDatabaseNotFound)
	require.ErrorIs(t, wrapper.CreateEdge(&storage.Edge{ID: "ghost-edge", StartNode: "a", EndNode: "b", Type: "R"}), ErrDatabaseNotFound)
	require.ErrorIs(t, wrapper.UpdateEdge(&storage.Edge{ID: "ghost-edge"}), ErrDatabaseNotFound)
	require.ErrorIs(t, wrapper.DeleteEdge("ghost-edge"), ErrDatabaseNotFound)
}

func TestSizeTrackingEngineUpdateSecondReadMarksDirty(t *testing.T) {
	manager, dbName := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	nodeID, err := base.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":second-read-node"), Labels: []string{"Person"}})
	require.NoError(t, err)
	otherID, err := base.CreateNode(&storage.Node{ID: storage.NodeID(dbName + ":second-read-other"), Labels: []string{"Person"}})
	require.NoError(t, err)
	edgeID := storage.EdgeID(dbName + ":second-read-edge")
	require.NoError(t, base.CreateEdge(&storage.Edge{ID: edgeID, StartNode: nodeID, EndNode: otherID, Type: "KNOWS"}))
	require.NoError(t, manager.ensureStorageSizeInitialized(dbName, base))

	inner := &flipAfterGetEngine{Engine: base}
	wrapper := newSizeTrackingEngine(inner, manager, dbName).(*sizeTrackingEngine)
	require.NoError(t, wrapper.UpdateNode(&storage.Node{ID: nodeID, Labels: []string{"Person"}, Properties: map[string]interface{}{"v": 1}}))
	require.False(t, storageSizeInitialized(t, manager, dbName))

	require.NoError(t, manager.ensureStorageSizeInitialized(dbName, base))
	inner.edgeCalls = 0
	require.NoError(t, wrapper.UpdateEdge(&storage.Edge{ID: edgeID, StartNode: nodeID, EndNode: otherID, Type: "LIKES"}))
	require.False(t, storageSizeInitialized(t, manager, dbName))
}

func TestDatabaseManagerCalculateStorageSizeFromEngineErrors(t *testing.T) {
	manager, _ := setupTestManager(t)
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	_, _, err := manager.calculateStorageSizeFromEngine(&storageSizingErrorEngine{Engine: base, nodesErr: errors.New("nodes unavailable")})
	require.ErrorContains(t, err, "failed to get all nodes")

	badNode := &storage.Node{ID: "testdb:bad-node", Properties: map[string]interface{}{"bad": make(chan int)}}
	_, _, err = manager.calculateStorageSizeFromEngine(&storageSizingErrorEngine{Engine: base, nodes: []*storage.Node{badNode}})
	require.ErrorContains(t, err, "failed to calculate node size")

	_, _, err = manager.calculateStorageSizeFromEngine(&storageSizingErrorEngine{Engine: base, edgesErr: errors.New("edges unavailable")})
	require.ErrorContains(t, err, "failed to get all edges")

	badEdge := &storage.Edge{ID: "testdb:bad-edge", Type: "BAD", Properties: map[string]interface{}{"bad": make(chan int)}}
	_, _, err = manager.calculateStorageSizeFromEngine(&storageSizingErrorEngine{Engine: base, edges: []*storage.Edge{badEdge}})
	require.ErrorContains(t, err, "failed to calculate edge size")
}

func storageSizeInitialized(t *testing.T, manager *DatabaseManager, dbName string) bool {
	t.Helper()
	manager.mu.RLock()
	info := manager.databases[dbName]
	manager.mu.RUnlock()
	require.NotNil(t, info)
	info.sizeMu.RLock()
	defer info.sizeMu.RUnlock()
	return info.sizeInitialized
}

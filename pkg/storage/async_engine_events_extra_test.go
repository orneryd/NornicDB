package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type asyncBaseEngine struct {
	inner *MemoryEngine
}

func newAsyncBaseEngine() *asyncBaseEngine {
	return &asyncBaseEngine{inner: NewMemoryEngine()}
}

func (e *asyncBaseEngine) CreateNode(node *Node) (NodeID, error) {
	return e.inner.CreateNode(node)
}

func (e *asyncBaseEngine) GetNode(id NodeID) (*Node, error) {
	return e.inner.GetNode(id)
}

func (e *asyncBaseEngine) UpdateNode(node *Node) error {
	return e.inner.UpdateNode(node)
}

func (e *asyncBaseEngine) DeleteNode(id NodeID) error {
	return e.inner.DeleteNode(id)
}

func (e *asyncBaseEngine) CreateEdge(edge *Edge) error {
	return e.inner.CreateEdge(edge)
}

func (e *asyncBaseEngine) GetEdge(id EdgeID) (*Edge, error) {
	return e.inner.GetEdge(id)
}

func (e *asyncBaseEngine) UpdateEdge(edge *Edge) error {
	return e.inner.UpdateEdge(edge)
}

func (e *asyncBaseEngine) DeleteEdge(id EdgeID) error {
	return e.inner.DeleteEdge(id)
}

func (e *asyncBaseEngine) GetNodesByLabel(label string) ([]*Node, error) {
	return e.inner.GetNodesByLabel(label)
}

func (e *asyncBaseEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	return e.inner.GetFirstNodeByLabel(label)
}

func (e *asyncBaseEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	return e.inner.GetOutgoingEdges(nodeID)
}

func (e *asyncBaseEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	return e.inner.GetIncomingEdges(nodeID)
}

func (e *asyncBaseEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	return e.inner.GetEdgesBetween(startID, endID)
}

func (e *asyncBaseEngine) GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge {
	return e.inner.GetEdgeBetween(startID, endID, edgeType)
}

func (e *asyncBaseEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	return e.inner.GetEdgesByType(edgeType)
}

func (e *asyncBaseEngine) AllNodes() ([]*Node, error) {
	return e.inner.AllNodes()
}

func (e *asyncBaseEngine) AllEdges() ([]*Edge, error) {
	return e.inner.AllEdges()
}

func (e *asyncBaseEngine) GetAllNodes() []*Node {
	return e.inner.GetAllNodes()
}

func (e *asyncBaseEngine) GetInDegree(nodeID NodeID) int {
	return e.inner.GetInDegree(nodeID)
}

func (e *asyncBaseEngine) GetOutDegree(nodeID NodeID) int {
	return e.inner.GetOutDegree(nodeID)
}

func (e *asyncBaseEngine) GetSchema() *SchemaManager {
	return e.inner.GetSchema()
}

func (e *asyncBaseEngine) BulkCreateNodes(nodes []*Node) error {
	return e.inner.BulkCreateNodes(nodes)
}

func (e *asyncBaseEngine) BulkCreateEdges(edges []*Edge) error {
	return e.inner.BulkCreateEdges(edges)
}

func (e *asyncBaseEngine) BulkDeleteNodes(ids []NodeID) error {
	return e.inner.BulkDeleteNodes(ids)
}

func (e *asyncBaseEngine) BulkDeleteEdges(ids []EdgeID) error {
	return e.inner.BulkDeleteEdges(ids)
}

func (e *asyncBaseEngine) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	return e.inner.BatchGetNodes(ids)
}

func (e *asyncBaseEngine) Close() error {
	return e.inner.Close()
}

func (e *asyncBaseEngine) NodeCount() (int64, error) {
	return e.inner.NodeCount()
}

func (e *asyncBaseEngine) EdgeCount() (int64, error) {
	return e.inner.EdgeCount()
}

func (e *asyncBaseEngine) DeleteByPrefix(prefix string) (int64, int64, error) {
	return e.inner.DeleteByPrefix(prefix)
}

type asyncOptionalDelegationEngine struct {
	*MemoryEngine
	currentNode        bool
	currentErr         error
	currentEdge        bool
	currentEdgeErr     error
	rebuildTemporalErr error
	pruneTemporalCount int64
	pruneTemporalErr   error
	nodeVisible        *Node
	edgeVisible        *Edge
	nodesVisible       []*Node
	edgesVisible       []*Edge
	edgesBetween       []*Edge
	head               MVCCHead
	headErr            error
	rebuildMVCCErr     error
	pruneMVCCCount     int64
	pruneMVCCErr       error
	registered         bool
	status             map[string]interface{}
	triggerErr         error
	paused             bool
	resumed            bool
	scheduleErr        error
	debtKeys           []MVCCLifecycleDebtKey
}

func (e *asyncOptionalDelegationEngine) IsCurrentTemporalNode(_ *Node, _ time.Time) (bool, error) {
	return e.currentNode, e.currentErr
}

func (e *asyncOptionalDelegationEngine) RebuildTemporalIndexes(context.Context) error {
	return e.rebuildTemporalErr
}

func (e *asyncOptionalDelegationEngine) PruneTemporalHistory(context.Context, TemporalPruneOptions) (int64, error) {
	return e.pruneTemporalCount, e.pruneTemporalErr
}

func (e *asyncOptionalDelegationEngine) GetNodeLatestVisible(id NodeID) (*Node, error) {
	return e.nodeVisible, nil
}

func (e *asyncOptionalDelegationEngine) GetEdgeLatestVisible(id EdgeID) (*Edge, error) {
	return e.edgeVisible, nil
}

func (e *asyncOptionalDelegationEngine) GetNodeVisibleAt(id NodeID, version MVCCVersion) (*Node, error) {
	return e.nodeVisible, nil
}

func (e *asyncOptionalDelegationEngine) GetEdgeVisibleAt(id EdgeID, version MVCCVersion) (*Edge, error) {
	return e.edgeVisible, nil
}

func (e *asyncOptionalDelegationEngine) GetNodesByLabelVisibleAt(label string, version MVCCVersion) ([]*Node, error) {
	return e.nodesVisible, nil
}

func (e *asyncOptionalDelegationEngine) GetOutgoingEdgesVisibleAt(nodeID NodeID, version MVCCVersion) ([]*Edge, error) {
	return e.edgesVisible, nil
}

func (e *asyncOptionalDelegationEngine) GetIncomingEdgesVisibleAt(nodeID NodeID, version MVCCVersion) ([]*Edge, error) {
	return e.edgesVisible, nil
}

func (e *asyncOptionalDelegationEngine) GetEdgesByTypeVisibleAt(edgeType string, version MVCCVersion) ([]*Edge, error) {
	return e.edgesVisible, nil
}

func (e *asyncOptionalDelegationEngine) GetEdgesBetweenVisibleAt(startID, endID NodeID, version MVCCVersion) ([]*Edge, error) {
	return e.edgesBetween, nil
}

func (e *asyncOptionalDelegationEngine) GetNodeCurrentHead(id NodeID) (MVCCHead, error) {
	return e.head, e.headErr
}

func (e *asyncOptionalDelegationEngine) GetEdgeCurrentHead(id EdgeID) (MVCCHead, error) {
	return e.head, e.headErr
}

func (e *asyncOptionalDelegationEngine) RebuildMVCCHeads(context.Context) error {
	return e.rebuildMVCCErr
}

func (e *asyncOptionalDelegationEngine) PruneMVCCVersions(context.Context, MVCCPruneOptions) (int64, error) {
	return e.pruneMVCCCount, e.pruneMVCCErr
}

func (e *asyncOptionalDelegationEngine) RegisterSnapshotReader(info SnapshotReaderInfo) func() {
	e.registered = true
	return func() { e.registered = false }
}

func (e *asyncOptionalDelegationEngine) LifecycleStatus() map[string]interface{} {
	if e.status == nil {
		return map[string]interface{}{"enabled": true}
	}
	return e.status
}

func (e *asyncOptionalDelegationEngine) TriggerPruneNow(context.Context) error {
	return e.triggerErr
}

func (e *asyncOptionalDelegationEngine) PauseLifecycle() {
	e.paused = true
}

func (e *asyncOptionalDelegationEngine) ResumeLifecycle() {
	e.resumed = true
}

func (e *asyncOptionalDelegationEngine) SetLifecycleSchedule(interval time.Duration) error {
	return e.scheduleErr
}

func (e *asyncOptionalDelegationEngine) TopLifecycleDebtKeys(limit int) []MVCCLifecycleDebtKey {
	return e.debtKeys
}

func TestAsyncEngine_OptionalDelegates_DefaultFallbacks(t *testing.T) {
	inner := newAsyncBaseEngine()
	nodeID := NodeID(prefixTestID("n-1"))
	edgeID := EdgeID(prefixTestID("e-1"))
	_, err := inner.CreateNode(&Node{ID: nodeID, Properties: map[string]interface{}{"name": "node"}})
	require.NoError(t, err)
	err = inner.CreateEdge(&Edge{ID: edgeID, StartNode: nodeID, EndNode: nodeID, Type: "SELF"})
	require.NoError(t, err)
	ae := &AsyncEngine{engine: inner}

	assert.Same(t, inner, ae.GetInnerEngine())
	var nilAsync *AsyncEngine
	assert.Nil(t, nilAsync.GetInnerEngine())
	assert.Nil(t, (&AsyncEngine{}).GetInnerEngine())

	current, err := ae.IsCurrentTemporalNode(&Node{ID: nodeID}, time.Now())
	require.NoError(t, err)
	assert.True(t, current)

	require.NoError(t, ae.RebuildTemporalIndexes(context.Background()))
	count, err := ae.PruneTemporalHistory(context.Background(), TemporalPruneOptions{})
	require.NoError(t, err)
	assert.EqualValues(t, 0, count)

	_, err = ae.GetNodeVisibleAt(nodeID, MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = ae.GetEdgeVisibleAt(edgeID, MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = ae.GetNodesByLabelVisibleAt("Node", MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = ae.GetOutgoingEdgesVisibleAt(nodeID, MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = ae.GetIncomingEdgesVisibleAt(nodeID, MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = ae.GetEdgesByTypeVisibleAt("SELF", MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = ae.GetEdgesBetweenVisibleAt(nodeID, nodeID, MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)

	head, err := ae.GetNodeCurrentHead(nodeID)
	require.ErrorIs(t, err, ErrNotImplemented)
	assert.Equal(t, MVCCHead{}, head)
	head, err = ae.GetEdgeCurrentHead(edgeID)
	require.ErrorIs(t, err, ErrNotImplemented)
	assert.Equal(t, MVCCHead{}, head)

	require.ErrorIs(t, ae.RebuildMVCCHeads(context.Background()), ErrNotImplemented)
	pruned, err := ae.PruneMVCCVersions(context.Background(), MVCCPruneOptions{})
	require.ErrorIs(t, err, ErrNotImplemented)
	assert.EqualValues(t, 0, pruned)

	release := ae.RegisterSnapshotReader(SnapshotReaderInfo{ReaderID: "reader-1"})
	release()
	assert.Equal(t, map[string]interface{}{"enabled": false}, ae.LifecycleStatus())
	require.NoError(t, ae.TriggerPruneNow(context.Background()))
	ae.PauseLifecycle()
	ae.ResumeLifecycle()
	require.NoError(t, ae.SetLifecycleSchedule(time.Second))
	assert.Nil(t, ae.TopLifecycleDebtKeys(5))

	latest, err := ae.GetNodeLatestVisible(nodeID)
	require.NoError(t, err)
	require.NotNil(t, latest)
	assert.Equal(t, nodeID, latest.ID)
	edgeLatest, err := ae.GetEdgeLatestVisible(edgeID)
	require.NoError(t, err)
	require.NotNil(t, edgeLatest)
	assert.Equal(t, edgeID, edgeLatest.ID)
}

func TestAsyncEngine_AdjacentAndLabelCountBranches(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	for _, id := range []NodeID{"tenant:n1", "tenant:n2", "tenant:n3", "tenant:n4", "tenant:n5"} {
		_, err := base.CreateNode(&Node{ID: id, Labels: []string{"Endpoint"}})
		require.NoError(t, err)
	}
	require.NoError(t, base.CreateEdge(&Edge{ID: "tenant:base-out", StartNode: "tenant:n1", EndNode: "tenant:n2", Type: "R"}))
	require.NoError(t, base.CreateEdge(&Edge{ID: "tenant:base-in", StartNode: "tenant:n3", EndNode: "tenant:n1", Type: "R"}))
	_, err := base.CreateNode(&Node{ID: "tenant:base", Labels: []string{"Person"}})
	require.NoError(t, err)

	ae := NewAsyncEngine(base, &AsyncEngineConfig{FlushInterval: time.Hour, MinFlushInterval: time.Hour, MaxFlushInterval: time.Hour})
	t.Cleanup(func() { _ = ae.Close() })
	require.NoError(t, ae.CreateEdge(&Edge{ID: "tenant:cache-out", StartNode: "tenant:n1", EndNode: "tenant:n4", Type: "R"}))
	require.NoError(t, ae.CreateEdge(&Edge{ID: "tenant:cache-in", StartNode: "tenant:n5", EndNode: "tenant:n1", Type: "R"}))
	_, err = ae.CreateNode(&Node{ID: "tenant:cached", Labels: []string{"person"}})
	require.NoError(t, err)
	_, err = ae.CreateNode(&Node{ID: "other:cached", Labels: []string{"Person"}})
	require.NoError(t, err)

	out, in, err := ae.GetAdjacentEdges("tenant:n1")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"tenant:cache-out", "tenant:base-out"}, []EdgeID{out[0].ID, out[1].ID})
	require.ElementsMatch(t, []EdgeID{"tenant:cache-in", "tenant:base-in"}, []EdgeID{in[0].ID, in[1].ID})

	count, err := ae.NodeCountByLabel("Person")
	require.NoError(t, err)
	require.EqualValues(t, 3, count)
	count, err = ae.NodeCountByLabelInNamespace("tenant", "Person")
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	ae.RecordMaterializedAccess("tenant:cached")

	fallback := &adjacentFallbackEngine{Engine: NewMemoryEngine(), outErr: errors.New("out failed")}
	fallbackAE := NewAsyncEngine(fallback, &AsyncEngineConfig{FlushInterval: time.Hour, MinFlushInterval: time.Hour, MaxFlushInterval: time.Hour})
	t.Cleanup(func() { _ = fallbackAE.Close() })
	_, err = fallback.CreateNode(&Node{ID: "tenant:n1", Labels: []string{"Endpoint"}})
	require.NoError(t, err)
	_, err = fallback.CreateNode(&Node{ID: "tenant:n2", Labels: []string{"Endpoint"}})
	require.NoError(t, err)
	require.NoError(t, fallbackAE.CreateEdge(&Edge{ID: "tenant:cache-only", StartNode: "tenant:n1", EndNode: "tenant:n2", Type: "R"}))
	out, in, err = fallbackAE.GetAdjacentEdges("tenant:n1")
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Empty(t, in)

	recorder := &namespaceLifecycleRecorder{Engine: NewMemoryEngine()}
	recorderAE := NewAsyncEngine(recorder, &AsyncEngineConfig{FlushInterval: time.Hour, MinFlushInterval: time.Hour, MaxFlushInterval: time.Hour})
	t.Cleanup(func() { _ = recorderAE.Close() })
	recorderAE.RecordMaterializedAccess("tenant:recorded")
	require.Equal(t, []string{"tenant:recorded"}, recorder.accesses)
}

func TestAsyncEngine_OptionalDelegates_ForwardToWrappedEngine(t *testing.T) {
	inner := &asyncOptionalDelegationEngine{
		MemoryEngine:       NewMemoryEngine(),
		currentNode:        false,
		currentErr:         errors.New("temporal check"),
		pruneTemporalCount: 7,
		rebuildTemporalErr: errors.New("rebuild temporal"),
		pruneTemporalErr:   errors.New("prune temporal"),
		nodeVisible:        &Node{ID: NodeID("visible-node")},
		edgeVisible:        &Edge{ID: EdgeID("visible-edge")},
		nodesVisible:       []*Node{{ID: "n1"}, {ID: "n2"}},
		edgesVisible:       []*Edge{{ID: "e1"}},
		edgesBetween:       []*Edge{{ID: "eb1"}, {ID: "eb2"}},
		head:               MVCCHead{Tombstoned: true},
		headErr:            errors.New("head lookup"),
		rebuildMVCCErr:     errors.New("rebuild mvcc"),
		pruneMVCCCount:     9,
		pruneMVCCErr:       errors.New("prune mvcc"),
		status:             map[string]interface{}{"enabled": true, "state": "live"},
		triggerErr:         errors.New("trigger"),
		scheduleErr:        errors.New("schedule"),
		debtKeys:           []MVCCLifecycleDebtKey{{LogicalKey: "k1", DebtBytes: 42}},
	}
	ae := &AsyncEngine{engine: inner}

	current, err := ae.IsCurrentTemporalNode(&Node{ID: "n-2"}, time.Now())
	require.Error(t, err)
	assert.False(t, current)

	require.Error(t, ae.RebuildTemporalIndexes(context.Background()))
	count, err := ae.PruneTemporalHistory(context.Background(), TemporalPruneOptions{})
	require.Error(t, err)
	assert.EqualValues(t, 7, count)

	node, err := ae.GetNodeVisibleAt("n-2", MVCCVersion{})
	require.NoError(t, err)
	assert.Equal(t, NodeID("visible-node"), node.ID)
	edge, err := ae.GetEdgeVisibleAt("e-2", MVCCVersion{})
	require.NoError(t, err)
	assert.Equal(t, EdgeID("visible-edge"), edge.ID)
	latestNode, err := ae.GetNodeLatestVisible("n-2")
	require.NoError(t, err)
	assert.Equal(t, NodeID("visible-node"), latestNode.ID)
	latestEdge, err := ae.GetEdgeLatestVisible("e-2")
	require.NoError(t, err)
	assert.Equal(t, EdgeID("visible-edge"), latestEdge.ID)
	nodes, err := ae.GetNodesByLabelVisibleAt("Label", MVCCVersion{})
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	edges, err := ae.GetOutgoingEdgesVisibleAt("n1", MVCCVersion{})
	require.NoError(t, err)
	require.Len(t, edges, 1)
	edges, err = ae.GetIncomingEdgesVisibleAt("n2", MVCCVersion{})
	require.NoError(t, err)
	require.Len(t, edges, 1)
	edges, err = ae.GetEdgesByTypeVisibleAt("TYPE", MVCCVersion{})
	require.NoError(t, err)
	require.Len(t, edges, 1)
	edgesBetween, err := ae.GetEdgesBetweenVisibleAt("n1", "n2", MVCCVersion{})
	require.NoError(t, err)
	require.Len(t, edgesBetween, 2)

	head, err := ae.GetNodeCurrentHead("n-2")
	require.Error(t, err)
	assert.True(t, head.Tombstoned)
	head, err = ae.GetEdgeCurrentHead("e-2")
	require.Error(t, err)
	assert.True(t, head.Tombstoned)

	require.Error(t, ae.RebuildMVCCHeads(context.Background()))
	pruned, err := ae.PruneMVCCVersions(context.Background(), MVCCPruneOptions{})
	require.Error(t, err)
	assert.EqualValues(t, 9, pruned)

	release := ae.RegisterSnapshotReader(SnapshotReaderInfo{ReaderID: "reader-2"})
	assert.True(t, inner.registered)
	release()
	assert.False(t, inner.registered)
	assert.Equal(t, map[string]interface{}{"enabled": true, "state": "live"}, ae.LifecycleStatus())
	require.Error(t, ae.TriggerPruneNow(context.Background()))
	ae.PauseLifecycle()
	ae.ResumeLifecycle()
	assert.True(t, inner.paused)
	assert.True(t, inner.resumed)
	require.Error(t, ae.SetLifecycleSchedule(time.Second))
	assert.Equal(t, []MVCCLifecycleDebtKey{{LogicalKey: "k1", DebtBytes: 42}}, ae.TopLifecycleDebtKeys(1))
}

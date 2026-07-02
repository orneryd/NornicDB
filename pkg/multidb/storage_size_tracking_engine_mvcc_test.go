package multidb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type mvccDelegatingEngine struct {
	storage.Engine
}

func (m *mvccDelegatingEngine) GetNodeLatestVisible(id storage.NodeID) (*storage.Node, error) {
	return &storage.Node{ID: id}, nil
}

func (m *mvccDelegatingEngine) GetNodeVisibleAt(id storage.NodeID, version storage.MVCCVersion) (*storage.Node, error) {
	return &storage.Node{ID: id, Properties: map[string]interface{}{"seq": version.CommitSequence}}, nil
}

func (m *mvccDelegatingEngine) GetEdgeLatestVisible(id storage.EdgeID) (*storage.Edge, error) {
	return &storage.Edge{ID: id}, nil
}

func (m *mvccDelegatingEngine) GetEdgeVisibleAt(id storage.EdgeID, version storage.MVCCVersion) (*storage.Edge, error) {
	return &storage.Edge{ID: id, Properties: map[string]interface{}{"seq": version.CommitSequence}}, nil
}

func (m *mvccDelegatingEngine) GetNodesByLabelVisibleAt(label string, _ storage.MVCCVersion) ([]*storage.Node, error) {
	return []*storage.Node{{ID: storage.NodeID("n:" + label)}}, nil
}

func (m *mvccDelegatingEngine) GetOutgoingEdgesVisibleAt(nodeID storage.NodeID, _ storage.MVCCVersion) ([]*storage.Edge, error) {
	return []*storage.Edge{{ID: storage.EdgeID("out:" + string(nodeID)), StartNode: nodeID}}, nil
}

func (m *mvccDelegatingEngine) GetIncomingEdgesVisibleAt(nodeID storage.NodeID, _ storage.MVCCVersion) ([]*storage.Edge, error) {
	return []*storage.Edge{{ID: storage.EdgeID("in:" + string(nodeID)), EndNode: nodeID}}, nil
}

func (m *mvccDelegatingEngine) GetEdgesByTypeVisibleAt(edgeType string, _ storage.MVCCVersion) ([]*storage.Edge, error) {
	return []*storage.Edge{{ID: storage.EdgeID("e:" + edgeType)}}, nil
}

func (m *mvccDelegatingEngine) GetEdgesBetweenVisibleAt(startID, endID storage.NodeID, _ storage.MVCCVersion) ([]*storage.Edge, error) {
	return []*storage.Edge{{ID: storage.EdgeID(startID + "->" + endID), StartNode: startID, EndNode: endID}}, nil
}

func (m *mvccDelegatingEngine) GetNodeCurrentHead(id storage.NodeID) (storage.MVCCHead, error) {
	_ = id
	return storage.MVCCHead{Version: storage.MVCCVersion{CommitSequence: 11}}, nil
}

func (m *mvccDelegatingEngine) GetEdgeCurrentHead(id storage.EdgeID) (storage.MVCCHead, error) {
	_ = id
	return storage.MVCCHead{Version: storage.MVCCVersion{CommitSequence: 13}}, nil
}

func TestSizeTrackingEngine_MVCCDelegation_NotImplementedFallback(t *testing.T) {
	t.Parallel()

	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	unsupported := &sizeTrackingStreamingInner{Engine: base}
	engine := newSizeTrackingEngine(unsupported, &DatabaseManager{}, "tenant_a")
	tracker, ok := engine.(*sizeTrackingEngine)
	require.True(t, ok)

	version := storage.MVCCVersion{CommitSequence: 7}

	_, err := tracker.GetNodeLatestVisible("n1")
	require.ErrorIs(t, err, storage.ErrNotImplemented)

	_, err = tracker.GetNodeVisibleAt("n1", version)
	require.ErrorIs(t, err, storage.ErrNotImplemented)

	_, err = tracker.GetEdgeLatestVisible("e1")
	require.ErrorIs(t, err, storage.ErrNotImplemented)

	_, err = tracker.GetEdgeVisibleAt("e1", version)
	require.ErrorIs(t, err, storage.ErrNotImplemented)

	_, err = tracker.GetNodesByLabelVisibleAt("Person", version)
	require.ErrorIs(t, err, storage.ErrNotImplemented)

	_, err = tracker.GetOutgoingEdgesVisibleAt("n1", version)
	require.ErrorIs(t, err, storage.ErrNotImplemented)

	_, err = tracker.GetIncomingEdgesVisibleAt("n1", version)
	require.ErrorIs(t, err, storage.ErrNotImplemented)

	_, err = tracker.GetEdgesByTypeVisibleAt("KNOWS", version)
	require.ErrorIs(t, err, storage.ErrNotImplemented)

	_, err = tracker.GetEdgesBetweenVisibleAt("a", "b", version)
	require.ErrorIs(t, err, storage.ErrNotImplemented)

	_, err = tracker.GetNodeCurrentHead("n1")
	require.ErrorIs(t, err, storage.ErrNotImplemented)

	_, err = tracker.GetEdgeCurrentHead("e1")
	require.ErrorIs(t, err, storage.ErrNotImplemented)
}

func TestSizeTrackingEngine_MVCCDelegation_Supported(t *testing.T) {
	t.Parallel()

	delegate := &mvccDelegatingEngine{Engine: storage.NewMemoryEngine()}
	engine := newSizeTrackingEngine(delegate, &DatabaseManager{}, "tenant_a")
	tracker, ok := engine.(*sizeTrackingEngine)
	require.True(t, ok)

	version := storage.MVCCVersion{CommitSequence: 9}

	nodeLatest, err := tracker.GetNodeLatestVisible("n1")
	require.NoError(t, err)
	require.Equal(t, storage.NodeID("n1"), nodeLatest.ID)

	nodeAt, err := tracker.GetNodeVisibleAt("n1", version)
	require.NoError(t, err)
	require.Equal(t, uint64(9), nodeAt.Properties["seq"])

	edgeLatest, err := tracker.GetEdgeLatestVisible("e1")
	require.NoError(t, err)
	require.Equal(t, storage.EdgeID("e1"), edgeLatest.ID)

	edgeAt, err := tracker.GetEdgeVisibleAt("e1", version)
	require.NoError(t, err)
	require.Equal(t, uint64(9), edgeAt.Properties["seq"])

	nodesAt, err := tracker.GetNodesByLabelVisibleAt("Person", version)
	require.NoError(t, err)
	require.Len(t, nodesAt, 1)
	require.Equal(t, storage.NodeID("n:Person"), nodesAt[0].ID)

	outgoingAt, err := tracker.GetOutgoingEdgesVisibleAt("n1", version)
	require.NoError(t, err)
	require.Len(t, outgoingAt, 1)
	require.Equal(t, storage.EdgeID("out:n1"), outgoingAt[0].ID)
	require.Equal(t, storage.NodeID("n1"), outgoingAt[0].StartNode)

	incomingAt, err := tracker.GetIncomingEdgesVisibleAt("n1", version)
	require.NoError(t, err)
	require.Len(t, incomingAt, 1)
	require.Equal(t, storage.EdgeID("in:n1"), incomingAt[0].ID)
	require.Equal(t, storage.NodeID("n1"), incomingAt[0].EndNode)

	edgesByType, err := tracker.GetEdgesByTypeVisibleAt("KNOWS", version)
	require.NoError(t, err)
	require.Len(t, edgesByType, 1)
	require.Equal(t, storage.EdgeID("e:KNOWS"), edgesByType[0].ID)

	edgesBetween, err := tracker.GetEdgesBetweenVisibleAt("a", "b", version)
	require.NoError(t, err)
	require.Len(t, edgesBetween, 1)
	require.Equal(t, storage.EdgeID("a->b"), edgesBetween[0].ID)
	require.Equal(t, storage.NodeID("a"), edgesBetween[0].StartNode)
	require.Equal(t, storage.NodeID("b"), edgesBetween[0].EndNode)

	nodeHead, err := tracker.GetNodeCurrentHead("n1")
	require.NoError(t, err)
	require.Equal(t, uint64(11), nodeHead.Version.CommitSequence)

	edgeHead, err := tracker.GetEdgeCurrentHead("e1")
	require.NoError(t, err)
	require.Equal(t, uint64(13), edgeHead.Version.CommitSequence)
}

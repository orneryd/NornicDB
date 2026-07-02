package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type walOptionalEngine struct {
	*MemoryEngine
	node *Node
	edge *Edge
}

func (e *walOptionalEngine) IsCurrentTemporalNode(node *Node, asOf time.Time) (bool, error) {
	_ = asOf
	return node != nil, nil
}
func (e *walOptionalEngine) RebuildTemporalIndexes(context.Context) error { return nil }
func (e *walOptionalEngine) PruneTemporalHistory(context.Context, TemporalPruneOptions) (int64, error) {
	return 7, nil
}

func (e *walOptionalEngine) RebuildMVCCHeads(context.Context) error { return nil }
func (e *walOptionalEngine) PruneMVCCVersions(context.Context, MVCCPruneOptions) (int64, error) {
	return 8, nil
}

func (e *walOptionalEngine) GetNodeLatestEffective(id NodeID) (*Node, error) {
	if e.node != nil && e.node.ID == id {
		return copyNode(e.node), nil
	}
	return nil, ErrNotFound
}
func (e *walOptionalEngine) GetEdgeLatestEffective(id EdgeID) (*Edge, error) {
	if e.edge != nil && e.edge.ID == id {
		return copyEdge(e.edge), nil
	}
	return nil, ErrNotFound
}
func (e *walOptionalEngine) GetNodeLatestVisible(id NodeID) (*Node, error) {
	return e.GetNodeLatestEffective(id)
}
func (e *walOptionalEngine) GetEdgeLatestVisible(id EdgeID) (*Edge, error) {
	return e.GetEdgeLatestEffective(id)
}
func (e *walOptionalEngine) GetNodeVisibleAt(id NodeID, _ MVCCVersion) (*Node, error) {
	return e.GetNodeLatestEffective(id)
}
func (e *walOptionalEngine) GetEdgeVisibleAt(id EdgeID, _ MVCCVersion) (*Edge, error) {
	return e.GetEdgeLatestEffective(id)
}
func (e *walOptionalEngine) GetNodesByLabelVisibleAt(label string, _ MVCCVersion) ([]*Node, error) {
	if e.node != nil && hasLabel(e.node.Labels, label) {
		return []*Node{copyNode(e.node)}, nil
	}
	return nil, nil
}
func (e *walOptionalEngine) GetOutgoingEdgesVisibleAt(nodeID NodeID, _ MVCCVersion) ([]*Edge, error) {
	if e.edge != nil && e.edge.StartNode == nodeID {
		return []*Edge{copyEdge(e.edge)}, nil
	}
	return nil, nil
}
func (e *walOptionalEngine) GetIncomingEdgesVisibleAt(nodeID NodeID, _ MVCCVersion) ([]*Edge, error) {
	if e.edge != nil && e.edge.EndNode == nodeID {
		return []*Edge{copyEdge(e.edge)}, nil
	}
	return nil, nil
}
func (e *walOptionalEngine) GetEdgesByTypeVisibleAt(edgeType string, _ MVCCVersion) ([]*Edge, error) {
	if e.edge != nil && e.edge.Type == edgeType {
		return []*Edge{copyEdge(e.edge)}, nil
	}
	return nil, nil
}
func (e *walOptionalEngine) GetEdgesBetweenVisibleAt(startID, endID NodeID, _ MVCCVersion) ([]*Edge, error) {
	if e.edge != nil && e.edge.StartNode == startID && e.edge.EndNode == endID {
		return []*Edge{copyEdge(e.edge)}, nil
	}
	return nil, nil
}
func (e *walOptionalEngine) GetNodeCurrentHead(NodeID) (MVCCHead, error) {
	return MVCCHead{Version: MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 1}}, nil
}
func (e *walOptionalEngine) GetEdgeCurrentHead(EdgeID) (MVCCHead, error) {
	return MVCCHead{Version: MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 2}}, nil
}
func (e *walOptionalEngine) RegisterSnapshotReader(SnapshotReaderInfo) func() { return func() {} }
func (e *walOptionalEngine) LifecycleStatus() map[string]interface{} {
	return map[string]interface{}{"enabled": true}
}
func (e *walOptionalEngine) TriggerPruneNow(context.Context) error    { return nil }
func (e *walOptionalEngine) PauseLifecycle()                          {}
func (e *walOptionalEngine) ResumeLifecycle()                         {}
func (e *walOptionalEngine) SetLifecycleSchedule(time.Duration) error { return nil }
func (e *walOptionalEngine) TopLifecycleDebtKeys(int) []MVCCLifecycleDebtKey {
	return []MVCCLifecycleDebtKey{{Namespace: "test", LogicalKey: "k1"}}
}

func TestWALEngine_OptionalDelegationAndFallbackBranches(t *testing.T) {
	node := &Node{ID: "test:n1", Labels: []string{"L"}}
	edge := &Edge{ID: "test:e1", StartNode: "test:n1", EndNode: "test:n1", Type: "R"}
	opt := &walOptionalEngine{MemoryEngine: NewMemoryEngine(), node: node, edge: edge}
	t.Cleanup(func() { _ = opt.Close() })

	we := NewWALEngine(opt, nil)

	ok, err := we.IsCurrentTemporalNode(node, time.Now())
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, we.RebuildTemporalIndexes(context.Background()))
	prunedTemporal, err := we.PruneTemporalHistory(context.Background(), TemporalPruneOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(7), prunedTemporal)

	require.NoError(t, we.RebuildMVCCHeads(context.Background()))
	prunedMVCC, err := we.PruneMVCCVersions(context.Background(), MVCCPruneOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(8), prunedMVCC)

	gotNode, err := we.GetNodeLatestEffective("test:n1")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:n1"), gotNode.ID)
	gotEdge, err := we.GetEdgeLatestEffective("test:e1")
	require.NoError(t, err)
	require.Equal(t, EdgeID("test:e1"), gotEdge.ID)

	_, err = we.GetNodeLatestVisible("test:n1")
	require.NoError(t, err)
	_, err = we.GetEdgeLatestVisible("test:e1")
	require.NoError(t, err)

	_, err = we.GetNodeVisibleAt("test:n1", MVCCVersion{})
	require.NoError(t, err)
	_, err = we.GetEdgeVisibleAt("test:e1", MVCCVersion{})
	require.NoError(t, err)

	nodes, err := we.GetNodesByLabelVisibleAt("L", MVCCVersion{})
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	edges, err := we.GetEdgesByTypeVisibleAt("R", MVCCVersion{})
	require.NoError(t, err)
	require.Len(t, edges, 1)
	edges, err = we.GetEdgesBetweenVisibleAt("test:n1", "test:n1", MVCCVersion{})
	require.NoError(t, err)
	require.Len(t, edges, 1)

	head, err := we.GetNodeCurrentHead("test:n1")
	require.NoError(t, err)
	require.Equal(t, uint64(1), head.Version.CommitSequence)
	head, err = we.GetEdgeCurrentHead("test:e1")
	require.NoError(t, err)
	require.Equal(t, uint64(2), head.Version.CommitSequence)

	require.NotNil(t, we.RegisterSnapshotReader(SnapshotReaderInfo{ReaderID: "x"}))
	require.Equal(t, true, we.LifecycleStatus()["enabled"])
	require.NoError(t, we.TriggerPruneNow(context.Background()))
	we.PauseLifecycle()
	we.ResumeLifecycle()
	require.NoError(t, we.SetLifecycleSchedule(time.Second))
	require.Len(t, we.TopLifecycleDebtKeys(1), 1)
}

func TestWALEngine_OptionalFallbackBranches(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	we := NewWALEngine(&nonMVCCEngine{Engine: inner}, nil)

	ok, err := we.IsCurrentTemporalNode(&Node{}, time.Now())
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, we.RebuildTemporalIndexes(context.Background()))
	pruned, err := we.PruneTemporalHistory(context.Background(), TemporalPruneOptions{})
	require.NoError(t, err)
	require.Zero(t, pruned)

	require.ErrorIs(t, we.RebuildMVCCHeads(context.Background()), ErrNotImplemented)
	_, err = we.PruneMVCCVersions(context.Background(), MVCCPruneOptions{})
	require.ErrorIs(t, err, ErrNotImplemented)

	// latest-effective/visible fallback to regular engine lookups
	_, err = inner.CreateNode(&Node{ID: "test:n1", Labels: []string{"L"}})
	require.NoError(t, err)
	require.NoError(t, inner.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:n1", EndNode: "test:n1", Type: "R"}))

	_, err = we.GetNodeLatestEffective("test:n1")
	require.NoError(t, err)
	_, err = we.GetEdgeLatestEffective("test:e1")
	require.NoError(t, err)
	_, err = we.GetNodeLatestVisible("test:n1")
	require.NoError(t, err)
	_, err = we.GetEdgeLatestVisible("test:e1")
	require.NoError(t, err)

	_, err = we.GetNodeVisibleAt("test:n1", MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = we.GetEdgeVisibleAt("test:e1", MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = we.GetNodesByLabelVisibleAt("L", MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = we.GetEdgesByTypeVisibleAt("R", MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = we.GetEdgesBetweenVisibleAt("test:n1", "test:n1", MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)

	_, err = we.GetNodeCurrentHead("test:n1")
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = we.GetEdgeCurrentHead("test:e1")
	require.ErrorIs(t, err, ErrNotImplemented)

	require.NotNil(t, we.RegisterSnapshotReader(SnapshotReaderInfo{}))
	require.Equal(t, false, we.LifecycleStatus()["enabled"])
	require.NoError(t, we.TriggerPruneNow(context.Background()))
	we.PauseLifecycle()
	we.ResumeLifecycle()
	require.NoError(t, we.SetLifecycleSchedule(time.Second))
	require.Nil(t, we.TopLifecycleDebtKeys(1))
}

func TestWALEngine_Logger_NilBranches(t *testing.T) {
	var we *WALEngine
	require.NotNil(t, we.logger())
	we = &WALEngine{}
	require.NotNil(t, we.logger())

	we = &WALEngine{wal: &WAL{}}
	require.NotNil(t, we.logger())

	we = &WALEngine{wal: &WAL{walLog: discardWALSlog()}}
	require.NotNil(t, we.logger())
}

func TestWALEngine_FallbackLatestEffectivePropagatesErrors(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	we := NewWALEngine(inner, nil)

	_, err := we.GetNodeLatestEffective("test:missing")
	require.True(t, errors.Is(err, ErrNotFound))
	_, err = we.GetEdgeLatestEffective("test:missing")
	require.True(t, errors.Is(err, ErrNotFound))
}

package multidb

import (
	"context"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type mvccDelegatingEngine struct {
	storage.Engine
}

func (m *mvccDelegatingEngine) GetNodeLatestVisible(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	return &storage.Node{ID: id}, nil
}

func (m *mvccDelegatingEngine) GetNodeVisibleAt(ctx context.Context, id storage.NodeID, version storage.MVCCVersion) (*storage.Node, error) {
	return &storage.Node{ID: id, Properties: map[string]interface{}{"seq": version.CommitSequence}}, nil
}

func (m *mvccDelegatingEngine) GetEdgeLatestVisible(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	return &storage.Edge{ID: id}, nil
}

func (m *mvccDelegatingEngine) GetEdgeVisibleAt(ctx context.Context, id storage.EdgeID, version storage.MVCCVersion) (*storage.Edge, error) {
	return &storage.Edge{ID: id, Properties: map[string]interface{}{"seq": version.CommitSequence}}, nil
}

func (m *mvccDelegatingEngine) GetNodesByLabelVisibleAt(ctx context.Context, label string, _ storage.MVCCVersion) ([]*storage.Node, error) {
	return []*storage.Node{{ID: storage.NodeID("n:" + label)}}, nil
}

func (m *mvccDelegatingEngine) GetOutgoingEdgesVisibleAt(ctx context.Context, nodeID storage.NodeID, _ storage.MVCCVersion) ([]*storage.Edge, error) {
	return []*storage.Edge{{ID: storage.EdgeID("out:" + string(nodeID)), StartNode: nodeID}}, nil
}

func (m *mvccDelegatingEngine) GetIncomingEdgesVisibleAt(ctx context.Context, nodeID storage.NodeID, _ storage.MVCCVersion) ([]*storage.Edge, error) {
	return []*storage.Edge{{ID: storage.EdgeID("in:" + string(nodeID)), EndNode: nodeID}}, nil
}

func (m *mvccDelegatingEngine) GetEdgesByTypeVisibleAt(ctx context.Context, edgeType string, _ storage.MVCCVersion) ([]*storage.Edge, error) {
	return []*storage.Edge{{ID: storage.EdgeID("e:" + edgeType)}}, nil
}

func (m *mvccDelegatingEngine) GetEdgesBetweenVisibleAt(ctx context.Context, startID, endID storage.NodeID, _ storage.MVCCVersion) ([]*storage.Edge, error) {
	return []*storage.Edge{{ID: storage.EdgeID(startID + "->" + endID), StartNode: startID, EndNode: endID}}, nil
}

func (m *mvccDelegatingEngine) GetNodeCurrentHead(ctx context.Context, id storage.NodeID) (storage.MVCCHead, error) {
	_ = id
	return storage.MVCCHead{Version: storage.MVCCVersion{CommitSequence: 11}}, nil
}

func (m *mvccDelegatingEngine) GetEdgeCurrentHead(ctx context.Context, id storage.EdgeID) (storage.MVCCHead, error) {
	_ = id
	return storage.MVCCHead{Version: storage.MVCCVersion{CommitSequence: 13}}, nil
}

// Both capability outcomes use the same API population. Each operation's typed
// value assertions remain in its row; unsupported dispatch shares one error
// assertion owner instead of maintaining a second method-call inventory.
func TestSizeTrackingEngine_MVCCDelegation(t *testing.T) {
	t.Parallel()
	operations := []struct {
		name  string
		check func(*testing.T, *sizeTrackingEngine, storage.MVCCVersion, func(error) bool)
	}{
		{name: "GetNodeLatestVisible", check: func(t *testing.T, tracker *sizeTrackingEngine, version storage.MVCCVersion, accept func(error) bool) {
			nodeLatest, err := tracker.GetNodeLatestVisible(t.Context(), "n1")
			if accept(err) {
				require.Equal(t, storage.NodeID("n1"), nodeLatest.ID)
			}
		}},
		{name: "GetNodeVisibleAt", check: func(t *testing.T, tracker *sizeTrackingEngine, version storage.MVCCVersion, accept func(error) bool) {
			nodeAt, err := tracker.GetNodeVisibleAt(t.Context(), "n1", version)
			if accept(err) {
				require.Equal(t, uint64(9), nodeAt.Properties["seq"])
			}
		}},
		{name: "GetEdgeLatestVisible", check: func(t *testing.T, tracker *sizeTrackingEngine, version storage.MVCCVersion, accept func(error) bool) {
			edgeLatest, err := tracker.GetEdgeLatestVisible(t.Context(), "e1")
			if accept(err) {
				require.Equal(t, storage.EdgeID("e1"), edgeLatest.ID)
			}
		}},
		{name: "GetEdgeVisibleAt", check: func(t *testing.T, tracker *sizeTrackingEngine, version storage.MVCCVersion, accept func(error) bool) {
			edgeAt, err := tracker.GetEdgeVisibleAt(t.Context(), "e1", version)
			if accept(err) {
				require.Equal(t, uint64(9), edgeAt.Properties["seq"])
			}
		}},
		{name: "GetNodesByLabelVisibleAt", check: func(t *testing.T, tracker *sizeTrackingEngine, version storage.MVCCVersion, accept func(error) bool) {
			nodesAt, err := tracker.GetNodesByLabelVisibleAt(context.Background(), "Person", version)
			if accept(err) {
				require.Len(t, nodesAt, 1)
				require.Equal(t, storage.NodeID("n:Person"), nodesAt[0].ID)
			}
		}},
		{name: "GetOutgoingEdgesVisibleAt", check: func(t *testing.T, tracker *sizeTrackingEngine, version storage.MVCCVersion, accept func(error) bool) {
			outgoingAt, err := tracker.GetOutgoingEdgesVisibleAt(context.Background(), "n1", version)
			if accept(err) {
				require.Len(t, outgoingAt, 1)
				require.Equal(t, storage.EdgeID("out:n1"), outgoingAt[0].ID)
				require.Equal(t, storage.NodeID("n1"), outgoingAt[0].StartNode)
			}
		}},
		{name: "GetIncomingEdgesVisibleAt", check: func(t *testing.T, tracker *sizeTrackingEngine, version storage.MVCCVersion, accept func(error) bool) {
			incomingAt, err := tracker.GetIncomingEdgesVisibleAt(context.Background(), "n1", version)
			if accept(err) {
				require.Len(t, incomingAt, 1)
				require.Equal(t, storage.EdgeID("in:n1"), incomingAt[0].ID)
				require.Equal(t, storage.NodeID("n1"), incomingAt[0].EndNode)
			}
		}},
		{name: "GetEdgesByTypeVisibleAt", check: func(t *testing.T, tracker *sizeTrackingEngine, version storage.MVCCVersion, accept func(error) bool) {
			edgesByType, err := tracker.GetEdgesByTypeVisibleAt(context.Background(), "KNOWS", version)
			if accept(err) {
				require.Len(t, edgesByType, 1)
				require.Equal(t, storage.EdgeID("e:KNOWS"), edgesByType[0].ID)
			}
		}},
		{name: "GetEdgesBetweenVisibleAt", check: func(t *testing.T, tracker *sizeTrackingEngine, version storage.MVCCVersion, accept func(error) bool) {
			edgesBetween, err := tracker.GetEdgesBetweenVisibleAt(context.Background(), "a", "b", version)
			if accept(err) {
				require.Len(t, edgesBetween, 1)
				require.Equal(t, storage.EdgeID("a->b"), edgesBetween[0].ID)
				require.Equal(t, storage.NodeID("a"), edgesBetween[0].StartNode)
				require.Equal(t, storage.NodeID("b"), edgesBetween[0].EndNode)
			}
		}},
		{name: "GetNodeCurrentHead", check: func(t *testing.T, tracker *sizeTrackingEngine, version storage.MVCCVersion, accept func(error) bool) {
			nodeHead, err := tracker.GetNodeCurrentHead(t.Context(), "n1")
			if accept(err) {
				require.Equal(t, uint64(11), nodeHead.Version.CommitSequence)
			}
		}},
		{name: "GetEdgeCurrentHead", check: func(t *testing.T, tracker *sizeTrackingEngine, version storage.MVCCVersion, accept func(error) bool) {
			edgeHead, err := tracker.GetEdgeCurrentHead(t.Context(), "e1")
			if accept(err) {
				require.Equal(t, uint64(13), edgeHead.Version.CommitSequence)
			}
		}},
	}
	for _, tc := range []struct {
		name      string
		supported bool
		version   uint64
	}{
		{name: "TestSizeTrackingEngine_MVCCDelegation_NotImplementedFallback", version: 7},
		{name: "TestSizeTrackingEngine_MVCCDelegation_Supported", supported: true, version: 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			var inner storage.Engine = &sizeTrackingStreamingInner{Engine: base}
			if tc.supported {
				inner = &mvccDelegatingEngine{Engine: base}
			}
			engine := newSizeTrackingEngine(inner, &DatabaseManager{}, "tenant_a")
			tracker, ok := engine.(*sizeTrackingEngine)
			require.True(t, ok)
			version := storage.MVCCVersion{CommitSequence: tc.version}
			for _, operation := range operations {
				t.Run(operation.name, func(t *testing.T) {
					operation.check(t, tracker, version, func(err error) bool {
						if !tc.supported {
							require.ErrorIs(t, err, storage.ErrNotImplemented)
							return false
						}
						require.NoError(t, err)
						return true
					})
				})
			}
		})
	}
}

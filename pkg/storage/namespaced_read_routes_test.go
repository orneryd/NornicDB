package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

type pointRouteFixture struct {
	Engine
	node     *Node
	edge     *Edge
	err      error
	calls    []string
	ids      []string
	versions []MVCCVersion
}

func (f *pointRouteFixture) record(op, id string, v MVCCVersion) {
	f.calls = append(f.calls, op)
	f.ids = append(f.ids, id)
	f.versions = append(f.versions, v)
}
func (f *pointRouteFixture) nodeResult(op string, id NodeID, v MVCCVersion) (*Node, error) {
	f.record(op, string(id), v)
	return f.node, f.err
}
func (f *pointRouteFixture) edgeResult(op string, id EdgeID, v MVCCVersion) (*Edge, error) {
	f.record(op, string(id), v)
	return f.edge, f.err
}
func (f *pointRouteFixture) GetNode(ctx context.Context, id NodeID) (*Node, error) {
	return f.nodeResult("GetNode", id, MVCCVersion{})
}
func (f *pointRouteFixture) GetQueryNode(ctx context.Context, id NodeID) (*Node, error) {
	return f.nodeResult("GetQueryNode", id, MVCCVersion{})
}
func (f *pointRouteFixture) GetNodeLatestVisible(ctx context.Context, id NodeID) (*Node, error) {
	return f.nodeResult("GetNodeLatestVisible", id, MVCCVersion{})
}
func (f *pointRouteFixture) GetNodeLatestEffective(ctx context.Context, id NodeID) (*Node, error) {
	return f.nodeResult("GetNodeLatestEffective", id, MVCCVersion{})
}
func (f *pointRouteFixture) GetNodeVisibleAt(ctx context.Context, id NodeID, v MVCCVersion) (*Node, error) {
	return f.nodeResult("GetNodeVisibleAt", id, v)
}
func (f *pointRouteFixture) GetEdge(ctx context.Context, id EdgeID) (*Edge, error) {
	return f.edgeResult("GetEdge", id, MVCCVersion{})
}
func (f *pointRouteFixture) GetEdgeLatestVisible(ctx context.Context, id EdgeID) (*Edge, error) {
	return f.edgeResult("GetEdgeLatestVisible", id, MVCCVersion{})
}
func (f *pointRouteFixture) GetEdgeLatestEffective(ctx context.Context, id EdgeID) (*Edge, error) {
	return f.edgeResult("GetEdgeLatestEffective", id, MVCCVersion{})
}
func (f *pointRouteFixture) GetEdgeVisibleAt(ctx context.Context, id EdgeID, v MVCCVersion) (*Edge, error) {
	return f.edgeResult("GetEdgeVisibleAt", id, v)
}
func (f *pointRouteFixture) GetNodeCurrentHead(ctx context.Context, id NodeID) (MVCCHead, error) {
	f.record("GetNodeCurrentHead", string(id), MVCCVersion{})
	return MVCCHead{Version: MVCCVersion{CommitSequence: 23}}, f.err
}
func (f *pointRouteFixture) GetEdgeCurrentHead(ctx context.Context, id EdgeID) (MVCCHead, error) {
	f.record("GetEdgeCurrentHead", string(id), MVCCVersion{})
	return MVCCHead{Version: MVCCVersion{CommitSequence: 23}}, f.err
}

func TestNamespacedPointRoutingPreservesProviderContracts(t *testing.T) {
	version := MVCCVersion{CommitSequence: 17}
	injected := errors.New("decorated reader error")
	nodes := []struct {
		name string
		read func(*NamespacedEngine) (*Node, error)
		keep bool
	}{
		{"GetNode", func(n *NamespacedEngine) (*Node, error) { return n.GetNode(t.Context(), "n") }, false},
		{"GetQueryNode", func(n *NamespacedEngine) (*Node, error) { return n.GetQueryNode(t.Context(), "n") }, false},
		{"GetNodeLatestVisible", func(n *NamespacedEngine) (*Node, error) { return n.GetNodeLatestVisible(t.Context(), "n") }, true},
		{"GetNodeLatestEffective", func(n *NamespacedEngine) (*Node, error) { return n.GetNodeLatestEffective(context.Background(), "n") }, true},
		{"GetNodeVisibleAt", func(n *NamespacedEngine) (*Node, error) { return n.GetNodeVisibleAt(t.Context(), "n", version) }, true},
	}
	for _, tc := range nodes {
		for _, missing := range []bool{false, true} {
			for _, failed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/nil=%v/error=%v", tc.name, missing, failed), func(t *testing.T) {
					f := &pointRouteFixture{node: &Node{ID: "tenant:n", Properties: map[string]any{"payload": "source"}}}
					if missing {
						f.node = nil
					}
					if failed {
						f.err = injected
					}
					got, err := tc.read(NewNamespacedEngine(f, "tenant"))
					require.Equal(t, f.err, err)
					require.Equal(t, []string{tc.name}, f.calls)
					require.Equal(t, []string{"tenant:n"}, f.ids)
					wantVersion := MVCCVersion{}
					if tc.name == "GetNodeVisibleAt" {
						wantVersion = version
					}
					require.Equal(t, []MVCCVersion{wantVersion}, f.versions)
					if missing || (failed && !tc.keep) {
						require.Nil(t, got)
					} else if failed {
						require.Same(t, f.node, got)
						require.Equal(t, NodeID("tenant:n"), got.ID)
					} else {
						require.NotSame(t, f.node, got)
						require.Equal(t, NodeID("n"), got.ID)
						require.Equal(t, "tenant", got.RuntimeMetadata().Database)
						require.Equal(t, f.node.Properties, got.Properties)
					}
					if f.node != nil {
						require.Equal(t, NodeID("tenant:n"), f.node.ID)
					}
				})
			}
		}
	}
	edges := []struct {
		name string
		read func(*NamespacedEngine) (*Edge, error)
		keep bool
	}{
		{"GetEdge", func(n *NamespacedEngine) (*Edge, error) { return n.GetEdge(t.Context(), "e") }, false},
		{"GetEdgeLatestVisible", func(n *NamespacedEngine) (*Edge, error) { return n.GetEdgeLatestVisible(t.Context(), "e") }, true},
		{"GetEdgeLatestEffective", func(n *NamespacedEngine) (*Edge, error) { return n.GetEdgeLatestEffective(context.Background(), "e") }, true},
		{"GetEdgeVisibleAt", func(n *NamespacedEngine) (*Edge, error) { return n.GetEdgeVisibleAt(t.Context(), "e", version) }, true},
	}
	for _, tc := range edges {
		for _, missing := range []bool{false, true} {
			for _, failed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/nil=%v/error=%v", tc.name, missing, failed), func(t *testing.T) {
					f := &pointRouteFixture{edge: &Edge{ID: "tenant:e", StartNode: "tenant:a", EndNode: "tenant:b"}}
					if missing {
						f.edge = nil
					}
					if failed {
						f.err = injected
					}
					got, err := tc.read(NewNamespacedEngine(f, "tenant"))
					require.Equal(t, f.err, err)
					require.Equal(t, []string{tc.name}, f.calls)
					require.Equal(t, []string{"tenant:e"}, f.ids)
					wantVersion := MVCCVersion{}
					if tc.name == "GetEdgeVisibleAt" {
						wantVersion = version
					}
					require.Equal(t, []MVCCVersion{wantVersion}, f.versions)
					if missing || (failed && !tc.keep) {
						require.Nil(t, got)
					} else if failed {
						require.Same(t, f.edge, got)
						require.Equal(t, EdgeID("tenant:e"), got.ID)
					} else {
						require.NotSame(t, f.edge, got)
						require.Equal(t, EdgeID("e"), got.ID)
						require.Equal(t, NodeID("a"), got.StartNode)
						require.Equal(t, NodeID("b"), got.EndNode)
					}
					if f.edge != nil {
						require.Equal(t, EdgeID("tenant:e"), f.edge.ID)
					}
				})
			}
		}
	}
	for _, edgeCase := range []bool{false, true} {
		for _, failed := range []bool{false, true} {
			t.Run(fmt.Sprintf("head/edge=%v/error=%v", edgeCase, failed), func(t *testing.T) {
				f := &pointRouteFixture{}
				if failed {
					f.err = injected
				}
				n := NewNamespacedEngine(f, "tenant")
				var head MVCCHead
				var err error
				op, id := "GetNodeCurrentHead", "tenant:n"
				if edgeCase {
					head, err = n.GetEdgeCurrentHead(context.Background(), "e")
					op, id = "GetEdgeCurrentHead", "tenant:e"
				} else {
					head, err = n.GetNodeCurrentHead(context.Background(), "n")
				}
				require.Equal(t, f.err, err)
				require.EqualValues(t, 23, head.Version.CommitSequence)
				require.Equal(t, []string{op}, f.calls)
				require.Equal(t, []string{id}, f.ids)
			})
		}
	}
	// Hide optional capabilities, preserving the same decorated Engine live reads.
	for _, failed := range []bool{false, true} {
		f := &pointRouteFixture{node: &Node{ID: "tenant:n"}, edge: &Edge{ID: "tenant:e"}}
		if failed {
			f.err = injected
		}
		n := NewNamespacedEngine(struct{ Engine }{f}, "tenant")
		for _, read := range []func(context.Context, NodeID) (*Node, error){func(ctx context.Context, id NodeID) (*Node, error) { return n.GetQueryNode(ctx, id) }, func(ctx context.Context, id NodeID) (*Node, error) { return n.GetNodeLatestVisible(ctx, id) }, n.GetNodeLatestEffective} {
			got, err := read(context.Background(), "n")
			require.Equal(t, f.err, err)
			if failed {
				require.Nil(t, got)
			} else {
				require.Equal(t, NodeID("n"), got.ID)
			}
		}
		for _, read := range []func(context.Context, EdgeID) (*Edge, error){func(ctx context.Context, id EdgeID) (*Edge, error) { return n.GetEdgeLatestVisible(ctx, id) }, n.GetEdgeLatestEffective} {
			got, err := read(context.Background(), "e")
			require.Equal(t, f.err, err)
			if failed {
				require.Nil(t, got)
			} else {
				require.Equal(t, EdgeID("e"), got.ID)
			}
		}
		require.Equal(t, []string{"GetNode", "GetNode", "GetNode", "GetEdge", "GetEdge"}, f.calls)
		_, err := n.GetNodeVisibleAt(t.Context(), "n", version)
		require.ErrorIs(t, err, ErrNotImplemented)
		_, err = n.GetEdgeVisibleAt(t.Context(), "e", version)
		require.ErrorIs(t, err, ErrNotImplemented)
		_, err = n.GetNodeCurrentHead(context.Background(), "n")
		require.ErrorIs(t, err, ErrNotImplemented)
		_, err = n.GetEdgeCurrentHead(context.Background(), "e")
		require.ErrorIs(t, err, ErrNotImplemented)
	}
}

type adjacencyRouteFixture struct {
	pointRouteFixture
	edges []*Edge
}

func (f *adjacencyRouteFixture) list(op string, start, end NodeID, v MVCCVersion) ([]*Edge, error) {
	id := string(start)
	if end != "" {
		id += "->" + string(end)
	}
	f.record(op, id, v)
	return f.edges, f.err
}

// ReadAdjacentEdges answers the one adjacent-edge request the route fixture
// observes, recording the read point by direction.
func (f *adjacencyRouteFixture) ReadAdjacentEdges(ctx context.Context, request AdjacentEdgeRead) ([]*Edge, error) {
	switch request.Direction {
	case AdjacentBetween:
		return f.list("between", request.Node, request.Other, MVCCVersion{})
	case AdjacentIncoming:
		return f.list("in", request.Node, "", MVCCVersion{})
	case AdjacentBoth:
		return f.list("both", request.Node, "", MVCCVersion{})
	default:
		return f.list("out", request.Node, "", MVCCVersion{})
	}
}

// ReadAdjacentEdgesVisibleAt is the pinned read of the same request.
func (f *adjacencyRouteFixture) ReadAdjacentEdgesVisibleAt(ctx context.Context, request AdjacentEdgeRead, v MVCCVersion) ([]*Edge, error) {
	switch request.Direction {
	case AdjacentBetween:
		return f.list("between-at", request.Node, request.Other, v)
	case AdjacentIncoming:
		return f.list("in-at", request.Node, "", v)
	default:
		return f.list("out-at", request.Node, "", v)
	}
}
func (f *adjacencyRouteFixture) GetNodesByLabelVisibleAt(ctx context.Context, label string, v MVCCVersion) ([]*Node, error) {
	return nil, f.err
}
func (f *adjacencyRouteFixture) GetEdgesByTypeVisibleAt(ctx context.Context, edgeType string, v MVCCVersion) ([]*Edge, error) {
	return f.edges, f.err
}

func TestNamespacedAdjacencyRoutingPreservesReadPoint(t *testing.T) {
	version := MVCCVersion{CommitSequence: 19}
	injected := errors.New("adjacency provider failed")
	rows := []*Edge{{ID: "tenant:e", StartNode: "tenant:a", EndNode: "tenant:b"}, {ID: "other:e", StartNode: "other:a", EndNode: "other:b"}}
	tests := []struct {
		op, id string
		pinned bool
		read   func(*NamespacedEngine) ([]*Edge, error)
	}{
		{"out", "tenant:a", false, func(n *NamespacedEngine) ([]*Edge, error) {
			return n.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Node: "a"})
		}},
		{"in", "tenant:b", false, func(n *NamespacedEngine) ([]*Edge, error) {
			return n.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "tenant:b"})
		}},
		{"between", "tenant:a->tenant:b", false, func(n *NamespacedEngine) ([]*Edge, error) {
			return n.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: "tenant:a", Other: "b"})
		}},
		{"out-at", "tenant:a", true, func(n *NamespacedEngine) ([]*Edge, error) {
			return n.ReadAdjacentEdgesVisibleAt(context.Background(), AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: "a"}, version)
		}},
		{"in-at", "tenant:b", true, func(n *NamespacedEngine) ([]*Edge, error) {
			return n.ReadAdjacentEdgesVisibleAt(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "tenant:b"}, version)
		}},
		{"between-at", "tenant:a->tenant:b", true, func(n *NamespacedEngine) ([]*Edge, error) {
			return n.ReadAdjacentEdgesVisibleAt(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: "tenant:a", Other: "b"}, version)
		}},
	}
	for _, tc := range tests {
		for _, failed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/error=%v", tc.op, failed), func(t *testing.T) {
				f := &adjacencyRouteFixture{edges: rows}
				if failed {
					f.err = injected
				}
				n := NewNamespacedEngine(f, "tenant")
				edges, err := tc.read(n)
				require.Equal(t, f.err, err)
				require.Equal(t, []string{tc.op}, f.calls)
				require.Equal(t, []string{tc.id}, f.ids)
				wantVersion := MVCCVersion{}
				if tc.pinned {
					wantVersion = version
				}
				require.Equal(t, []MVCCVersion{wantVersion}, f.versions)
				if failed {
					require.Nil(t, edges)
				} else {
					require.Len(t, edges, 1)
					require.Equal(t, EdgeID("e"), edges[0].ID)
					require.Equal(t, NodeID("a"), edges[0].StartNode)
					require.Equal(t, NodeID("b"), edges[0].EndNode)
					require.NotSame(t, rows[0], edges[0])
				}
				require.Equal(t, EdgeID("tenant:e"), rows[0].ID)
			})
		}
	}
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprintf("both/error=%v", failed), func(t *testing.T) {
			f := &adjacencyRouteFixture{edges: rows}
			if failed {
				f.err = injected
			}
			// A both-direction request is one request: the fixture answers it in
			// one read point and the namespace qualifies it once.
			edges, err := NewNamespacedEngine(f, "tenant").ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBoth, Node: "a"})
			require.Equal(t, f.err, err)
			require.Equal(t, []string{"both"}, f.calls)
			for _, id := range f.ids {
				require.Equal(t, "tenant:a", id)
			}
			if failed {
				require.Nil(t, edges)
			} else {
				require.Len(t, edges, 1)
				require.Equal(t, EdgeID("e"), edges[0].ID)
			}
		})
	}
}

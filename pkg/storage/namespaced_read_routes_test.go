package storage

import (
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
func (f *pointRouteFixture) GetNode(id NodeID) (*Node, error) {
	return f.nodeResult("GetNode", id, MVCCVersion{})
}
func (f *pointRouteFixture) GetQueryNode(id NodeID) (*Node, error) {
	return f.nodeResult("GetQueryNode", id, MVCCVersion{})
}
func (f *pointRouteFixture) GetNodeLatestVisible(id NodeID) (*Node, error) {
	return f.nodeResult("GetNodeLatestVisible", id, MVCCVersion{})
}
func (f *pointRouteFixture) GetNodeLatestEffective(id NodeID) (*Node, error) {
	return f.nodeResult("GetNodeLatestEffective", id, MVCCVersion{})
}
func (f *pointRouteFixture) GetNodeVisibleAt(id NodeID, v MVCCVersion) (*Node, error) {
	return f.nodeResult("GetNodeVisibleAt", id, v)
}
func (f *pointRouteFixture) GetEdge(id EdgeID) (*Edge, error) {
	return f.edgeResult("GetEdge", id, MVCCVersion{})
}
func (f *pointRouteFixture) GetEdgeLatestVisible(id EdgeID) (*Edge, error) {
	return f.edgeResult("GetEdgeLatestVisible", id, MVCCVersion{})
}
func (f *pointRouteFixture) GetEdgeLatestEffective(id EdgeID) (*Edge, error) {
	return f.edgeResult("GetEdgeLatestEffective", id, MVCCVersion{})
}
func (f *pointRouteFixture) GetEdgeVisibleAt(id EdgeID, v MVCCVersion) (*Edge, error) {
	return f.edgeResult("GetEdgeVisibleAt", id, v)
}
func (f *pointRouteFixture) GetNodeCurrentHead(id NodeID) (MVCCHead, error) {
	f.record("GetNodeCurrentHead", string(id), MVCCVersion{})
	return MVCCHead{Version: MVCCVersion{CommitSequence: 23}}, f.err
}
func (f *pointRouteFixture) GetEdgeCurrentHead(id EdgeID) (MVCCHead, error) {
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
		{"GetNode", func(n *NamespacedEngine) (*Node, error) { return n.GetNode("n") }, false},
		{"GetQueryNode", func(n *NamespacedEngine) (*Node, error) { return n.GetQueryNode("n") }, false},
		{"GetNodeLatestVisible", func(n *NamespacedEngine) (*Node, error) { return n.GetNodeLatestVisible("n") }, true},
		{"GetNodeLatestEffective", func(n *NamespacedEngine) (*Node, error) { return n.GetNodeLatestEffective("n") }, true},
		{"GetNodeVisibleAt", func(n *NamespacedEngine) (*Node, error) { return n.GetNodeVisibleAt("n", version) }, true},
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
						require.Equal(t, "tenant", got.Database)
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
		{"GetEdge", func(n *NamespacedEngine) (*Edge, error) { return n.GetEdge("e") }, false},
		{"GetEdgeLatestVisible", func(n *NamespacedEngine) (*Edge, error) { return n.GetEdgeLatestVisible("e") }, true},
		{"GetEdgeLatestEffective", func(n *NamespacedEngine) (*Edge, error) { return n.GetEdgeLatestEffective("e") }, true},
		{"GetEdgeVisibleAt", func(n *NamespacedEngine) (*Edge, error) { return n.GetEdgeVisibleAt("e", version) }, true},
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
					head, err = n.GetEdgeCurrentHead("e")
					op, id = "GetEdgeCurrentHead", "tenant:e"
				} else {
					head, err = n.GetNodeCurrentHead("n")
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
		for _, read := range []func(NodeID) (*Node, error){n.GetQueryNode, n.GetNodeLatestVisible, n.GetNodeLatestEffective} {
			got, err := read("n")
			require.Equal(t, f.err, err)
			if failed {
				require.Nil(t, got)
			} else {
				require.Equal(t, NodeID("n"), got.ID)
			}
		}
		for _, read := range []func(EdgeID) (*Edge, error){n.GetEdgeLatestVisible, n.GetEdgeLatestEffective} {
			got, err := read("e")
			require.Equal(t, f.err, err)
			if failed {
				require.Nil(t, got)
			} else {
				require.Equal(t, EdgeID("e"), got.ID)
			}
		}
		require.Equal(t, []string{"GetNode", "GetNode", "GetNode", "GetEdge", "GetEdge"}, f.calls)
		_, err := n.GetNodeVisibleAt("n", version)
		require.ErrorIs(t, err, ErrNotImplemented)
		_, err = n.GetEdgeVisibleAt("e", version)
		require.ErrorIs(t, err, ErrNotImplemented)
		_, err = n.GetNodeCurrentHead("n")
		require.ErrorIs(t, err, ErrNotImplemented)
		_, err = n.GetEdgeCurrentHead("e")
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
func (f *adjacencyRouteFixture) GetOutgoingEdges(id NodeID) ([]*Edge, error) {
	return f.list("out", id, "", MVCCVersion{})
}
func (f *adjacencyRouteFixture) GetIncomingEdges(id NodeID) ([]*Edge, error) {
	return f.list("in", id, "", MVCCVersion{})
}
func (f *adjacencyRouteFixture) GetEdgesBetween(a, b NodeID) ([]*Edge, error) {
	return f.list("between", a, b, MVCCVersion{})
}
func (f *adjacencyRouteFixture) GetOutgoingEdgesVisibleAt(id NodeID, v MVCCVersion) ([]*Edge, error) {
	return f.list("out-at", id, "", v)
}
func (f *adjacencyRouteFixture) GetIncomingEdgesVisibleAt(id NodeID, v MVCCVersion) ([]*Edge, error) {
	return f.list("in-at", id, "", v)
}
func (f *adjacencyRouteFixture) GetEdgesBetweenVisibleAt(a, b NodeID, v MVCCVersion) ([]*Edge, error) {
	return f.list("between-at", a, b, v)
}
func (f *adjacencyRouteFixture) GetNodesByLabelVisibleAt(string, MVCCVersion) ([]*Node, error) {
	return nil, f.err
}
func (f *adjacencyRouteFixture) GetEdgesByTypeVisibleAt(string, MVCCVersion) ([]*Edge, error) {
	return f.edges, f.err
}
func (f *adjacencyRouteFixture) GetAdjacentEdges(id NodeID) ([]*Edge, []*Edge, error) {
	edges, err := f.list("both", id, "", MVCCVersion{})
	return edges, edges, err
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
		{"out", "tenant:a", false, func(n *NamespacedEngine) ([]*Edge, error) { return n.GetOutgoingEdges("a") }},
		{"in", "tenant:b", false, func(n *NamespacedEngine) ([]*Edge, error) { return n.GetIncomingEdges("tenant:b") }},
		{"between", "tenant:a->tenant:b", false, func(n *NamespacedEngine) ([]*Edge, error) { return n.GetEdgesBetween("tenant:a", "b") }},
		{"out-at", "tenant:a", true, func(n *NamespacedEngine) ([]*Edge, error) { return n.GetOutgoingEdgesVisibleAt("a", version) }},
		{"in-at", "tenant:b", true, func(n *NamespacedEngine) ([]*Edge, error) { return n.GetIncomingEdgesVisibleAt("tenant:b", version) }},
		{"between-at", "tenant:a->tenant:b", true, func(n *NamespacedEngine) ([]*Edge, error) {
			return n.GetEdgesBetweenVisibleAt("tenant:a", "b", version)
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
	for _, combined := range []bool{false, true} {
		for _, failed := range []bool{false, true} {
			t.Run(fmt.Sprintf("both/combined=%v/error=%v", combined, failed), func(t *testing.T) {
				f := &adjacencyRouteFixture{edges: rows}
				if failed {
					f.err = injected
				}
				var engine Engine = f
				if !combined {
					engine = struct{ Engine }{f}
				}
				out, in, err := NewNamespacedEngine(engine, "tenant").GetAdjacentEdges("a")
				require.Equal(t, f.err, err)
				wantCalls := []string{"both"}
				if !combined {
					wantCalls = []string{"out"}
					if !failed {
						wantCalls = append(wantCalls, "in")
					}
				}
				require.Equal(t, wantCalls, f.calls)
				for _, id := range f.ids {
					require.Equal(t, "tenant:a", id)
				}
				if failed {
					require.Nil(t, out)
					require.Nil(t, in)
				} else {
					require.Len(t, out, 1)
					require.Len(t, in, 1)
					require.Equal(t, EdgeID("e"), out[0].ID)
					require.Equal(t, EdgeID("e"), in[0].ID)
				}
			})
		}
	}
}

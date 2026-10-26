package storage

import (
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
)

type prefixCountRouteFixture struct {
	Engine
	nodes    []*Node
	edges    []*Edge
	err      error
	calls    []string
	prefixes []string
}

func (f *prefixCountRouteFixture) NodeCountByPrefix(prefix string) (int64, error) {
	f.calls = append(f.calls, "nodes")
	f.prefixes = append(f.prefixes, prefix)
	return 11, f.err
}
func (f *prefixCountRouteFixture) EdgeCountByPrefix(prefix string) (int64, error) {
	f.calls = append(f.calls, "edges")
	f.prefixes = append(f.prefixes, prefix)
	return 11, f.err
}
func (f *prefixCountRouteFixture) AllNodes() ([]*Node, error) {
	f.calls = append(f.calls, "all-nodes")
	return f.nodes, f.err
}
func (f *prefixCountRouteFixture) AllEdges() ([]*Edge, error) {
	f.calls = append(f.calls, "all-edges")
	return f.edges, f.err
}

func TestNamespacedPrefixCountRoutingPreservesFallbacks(t *testing.T) {
	injected := errors.New("prefix provider error")
	for _, provider := range []bool{false, true} {
		for _, failed := range []bool{false, true} {
			for _, prefix := range []string{"p", "", "tenant:p"} {
				f := &prefixCountRouteFixture{nodes: []*Node{{ID: "tenant:p1"}, {ID: "tenant:q1"}, {ID: "other:p1"}}, edges: []*Edge{{ID: "tenant:p1"}, {ID: "tenant:q1"}, {ID: "other:p1"}}}
				if failed {
					f.err = injected
				}
				var engine Engine = f
				if !provider {
					engine = struct{ Engine }{f}
				}
				n := NewNamespacedEngine(engine, "tenant")
				nodes, err := n.NodeCountByPrefix(prefix)
				require.Equal(t, f.err, err)
				edges, err := n.EdgeCountByPrefix(prefix)
				require.Equal(t, f.err, err)
				want := int64(1)
				if prefix == "" {
					want = 2
				} else if prefix == "tenant:p" {
					want = 0
				}
				if provider {
					want = 11
				} else if failed {
					want = 0
				}
				require.Equal(t, want, nodes)
				require.Equal(t, want, edges)
				if provider {
					qualified := prefix
					if prefix != "tenant:p" {
						qualified = "tenant:" + prefix
					}
					require.Equal(t, []string{"nodes", "edges"}, f.calls)
					require.Equal(t, []string{qualified, qualified}, f.prefixes)
				} else {
					require.Equal(t, []string{"all-nodes", "all-edges"}, f.calls)
					require.Empty(t, f.prefixes)
				}
				require.Equal(t, NodeID("tenant:p1"), f.nodes[0].ID)
				require.Equal(t, EdgeID("tenant:p1"), f.edges[0].ID)
			}
		}
	}
}

type pendingRouteFixture struct {
	Engine
	marked, queued, checked []NodeID
}

func (f *pendingRouteFixture) MarkNodeEmbedded(id NodeID)       { f.marked = append(f.marked, id) }
func (f *pendingRouteFixture) AddToPendingEmbeddings(id NodeID) { f.queued = append(f.queued, id) }
func (f *pendingRouteFixture) RelationshipEndpointVisible(id NodeID) (bool, bool) {
	f.checked = append(f.checked, id)
	return true, true
}
func (f *pendingRouteFixture) GetInnerEngine() Engine { return nil } // Discovery must use this decorator.
type markOnlyRouteFixture struct {
	Engine
	ids []NodeID
}

func (f *markOnlyRouteFixture) MarkNodeEmbedded(id NodeID) { f.ids = append(f.ids, id) }

type queueOnlyRouteFixture struct {
	Engine
	ids []NodeID
}

func (f *queueOnlyRouteFixture) AddToPendingEmbeddings(id NodeID) { f.ids = append(f.ids, id) }

func TestNamespacedCapabilityRoutesKeepDecoratorOwnership(t *testing.T) {
	f := &pendingRouteFixture{}
	n := NewNamespacedEngine(f, "tenant")
	n.MarkNodeEmbedded("tenant:n")
	n.AddToPendingEmbeddings("n")
	require.Equal(t, []NodeID{"tenant:n"}, f.marked)
	require.Equal(t, []NodeID{"tenant:n"}, f.queued)
	visible, answered := n.RelationshipEndpointVisible("other:n")
	require.True(t, visible)
	require.True(t, answered)
	require.Equal(t, []NodeID{"tenant:other:n"}, f.checked)
	w := &WALEngine{engine: f}
	visible, answered = w.RelationshipEndpointVisible("tenant:n")
	require.True(t, visible)
	require.True(t, answered)
	require.Equal(t, []NodeID{"tenant:other:n", "tenant:n"}, f.checked)
	plain := NewNamespacedEngine(struct{ Engine }{f}, "tenant")
	visible, answered = plain.RelationshipEndpointVisible("n")
	require.False(t, visible)
	require.False(t, answered)
	plain.MarkNodeEmbedded("n")
	plain.AddToPendingEmbeddings("n")
	require.Len(t, f.marked, 1)
	require.Len(t, f.queued, 1)
	require.Len(t, f.checked, 2)
	mark := &markOnlyRouteFixture{}
	m := NewNamespacedEngine(mark, "tenant")
	m.MarkNodeEmbedded("n")
	m.AddToPendingEmbeddings("n")
	require.Equal(t, []NodeID{"tenant:n"}, mark.ids)
	queue := &queueOnlyRouteFixture{}
	q := NewNamespacedEngine(queue, "tenant")
	q.MarkNodeEmbedded("n")
	q.AddToPendingEmbeddings("n")
	require.Equal(t, []NodeID{"tenant:n"}, queue.ids)
}

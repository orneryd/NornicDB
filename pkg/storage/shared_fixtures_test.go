package storage

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Shared test fixtures moved out of the deleted async engine test file so the
// remaining suites keep compiling. makeNode/makeEdge are used by temporal and
// storage-event tests.

func makeNode(id string) *Node {
	return &Node{
		ID:         NodeID(prefixTestID(id)),
		Labels:     []string{"TestLabel"},
		Properties: map[string]interface{}{"name": id},
	}
}

func makeEdge(id, from, to string) *Edge {
	return &Edge{
		ID:         EdgeID(prefixTestID(id)),
		StartNode:  NodeID(prefixTestID(from)),
		EndNode:    NodeID(prefixTestID(to)),
		Type:       "RELATED",
		Properties: map[string]interface{}{},
	}
}

// nonStreamingCountEngine hides optional streaming-count paths so wrappers
// fall back to AllNodes/AllEdges, and injects scan errors. Moved out of the
// deleted async engine test file because composite tests still use it.
type nonStreamingCountEngine struct {
	Engine
	allNodesErr error
	allEdgesErr error
}

func (e *nonStreamingCountEngine) AllNodes(ctx context.Context) ([]*Node, error) {
	return fixtureowner.ReadWithFailure(ctx, e.allNodesErr, func(ctx context.Context) ([]*Node, error) {
		return e.Engine.AllNodes(ctx)
	})
}

func (e *nonStreamingCountEngine) AllEdges(ctx context.Context) ([]*Edge, error) {
	return fixtureowner.ReadWithFailure(ctx, e.allEdgesErr, func(ctx context.Context) ([]*Edge, error) {
		return e.Engine.AllEdges(ctx)
	})
}

// firstLabelEngine returns a canned node or error from GetFirstNodeByLabel,
// for wrapper-delegation tests. Moved out of the deleted async engine test
// file because namespaced tests still use it.
type firstLabelEngine struct {
	Engine
	node *Node
	err  error
}

func (e *firstLabelEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	if e.err != nil {
		return nil, e.err
	}
	return CopyNode(e.node), nil
}

// streamFixtureItems is the one materialized stream loop the storage test
// doubles use. read produces the entity population for ctx, then every entity
// is examined in order: the cancellation checkpoint runs first (a cancelled ctx
// returns ctx.Err() instead of an invented read error), keep skips entities the
// caller's predicate rejects, and visit's error ends the stream as the caller
// returned it. read receives ctx itself, so a double that deliberately ignores
// the caller's context passes context.Background() and its checkpoint stays
// inert instead of consulting a caller it never honored.
func streamFixtureItems[T any](ctx context.Context, read func(context.Context) ([]T, error), keep func(T) bool, visit func(T) error) error {
	items, err := read(ctx)
	if err != nil {
		return err
	}
	for _, item := range items {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if keep != nil && !keep(item) {
			continue
		}
		if err := visit(item); err != nil {
			return err
		}
	}
	return nil
}

// streamFixtureNodes streams nodes through read, keeping only the nodes whose
// stored IDs carry prefix when prefix is set. An empty prefix keeps every node,
// matching both the plain and the options-driven streaming entry points.
func streamFixtureNodes(ctx context.Context, read func(context.Context) ([]*Node, error), prefix string, visit func(*Node) error) error {
	var keep func(*Node) bool
	if prefix != "" {
		keep = func(node *Node) bool { return strings.HasPrefix(string(node.ID), prefix) }
	}
	return streamFixtureItems(ctx, read, keep, visit)
}

// assertLabelVisitorStopsAfterTwo seeds five nodes built by node for label
// through create, then asserts forEach visits exactly two of them: the visitor
// returns false on its second call, so the subject's early-stop contract ends
// the scan there. Each caller keeps its own node IDs, prefix and label, and
// passes its own subject's ForEachNodeIDByLabel so the underlying Engine under
// test stays the one that runs the scan.
func assertLabelVisitorStopsAfterTwo(t *testing.T, label string, node func(int) *Node, create func(*Node) (NodeID, error), forEach func(context.Context, string, func(NodeID) bool) error) {
	t.Helper()
	for i := 0; i < 5; i++ {
		_, err := create(node(i))
		require.NoError(t, err)
	}

	var visited []NodeID
	err := forEach(context.Background(), label, func(id NodeID) bool {
		visited = append(visited, id)
		return len(visited) < 2
	})
	require.NoError(t, err)
	assert.Len(t, visited, 2)
}

// testEdgeIDs and testNodeIDs project typed IDs without changing input order.
func testEdgeIDs(edges []*Edge) []EdgeID {
	ids := make([]EdgeID, 0, len(edges))
	for _, edge := range edges {
		ids = append(ids, edge.ID)
	}
	return ids
}

func testNodeIDs(nodes []*Node) []NodeID {
	ids := make([]NodeID, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
	}
	return ids
}

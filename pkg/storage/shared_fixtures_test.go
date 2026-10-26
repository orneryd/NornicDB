package storage

import "context"

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

// nonStreamingCountEngine hides optional streams. Its mandatory node kernel
// and materialized APIs share the injected ordinary-scan failure; embedding
// Engine must not let the promoted native kernel bypass that fault owner.
type nonStreamingCountEngine struct {
	Engine
	allNodesErr error
	allEdgesErr error
}

func (e *nonStreamingCountEngine) StreamNodesWithOptions(ctx context.Context, options StreamNodesOptions, visit func(*Node) error) error {
	if e.allNodesErr != nil {
		return e.allNodesErr
	}
	return e.Engine.StreamNodesWithOptions(ctx, options, visit)
}

func (e *nonStreamingCountEngine) AllNodes() ([]*Node, error) {
	if e.allNodesErr != nil {
		return nil, e.allNodesErr
	}
	return e.Engine.AllNodes()
}

func (e *nonStreamingCountEngine) AllEdges() ([]*Edge, error) {
	if e.allEdgesErr != nil {
		return nil, e.allEdgesErr
	}
	return e.Engine.AllEdges()
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

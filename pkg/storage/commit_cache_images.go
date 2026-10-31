package storage

// commitCacheImages is the cache-payload projection of one logical writer's
// immutable operation stream. Index transitions and event multiplicity retain
// their original stream; cache bodies contain only the final image of each key.
type commitCacheImages struct {
	nodes map[NodeID]*Node
	edges map[EdgeID]*Edge
}

// acceptOperation consumes the writer's existing publication stream. It does
// not discover affected entities or depend on the optional notification queue.
func (images *commitCacheImages) acceptOperation(op Operation) {
	switch op.Type {
	case OpCreateNode, OpUpdateNode:
		if op.Node != nil {
			if images.nodes == nil {
				images.nodes = make(map[NodeID]*Node)
			}
			images.nodes[op.Node.ID] = op.Node
		}
	case OpDeleteNode:
		if images.nodes == nil {
			images.nodes = make(map[NodeID]*Node)
		}
		images.nodes[op.NodeID] = nil
		for _, id := range op.DeletedEdgeIDs {
			if images.edges == nil {
				images.edges = make(map[EdgeID]*Edge)
			}
			images.edges[id] = nil
		}
	case OpCreateEdge, OpUpdateEdge:
		if op.Edge != nil {
			if images.edges == nil {
				images.edges = make(map[EdgeID]*Edge)
			}
			images.edges[op.Edge.ID] = op.Edge
		}
	case OpDeleteEdge:
		if images.edges == nil {
			images.edges = make(map[EdgeID]*Edge)
		}
		images.edges[op.EdgeID] = nil
	}
}

func (cw *commitWriter) publishCacheImages() {
	b := cw.engine
	for _, image := range cw.cacheImages.nodes {
		if image != nil {
			b.cacheStoreWrittenNode(image, cw.durableVersion)
		}
	}
	for _, image := range cw.cacheImages.edges {
		if image != nil {
			b.cacheStoreEdge(image, cw.durableVersion)
		}
	}
}

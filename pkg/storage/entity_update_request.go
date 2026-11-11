package storage

// NodeUpdateRequest carries one node mutation and its image interpretation.
// Before, when supplied, is the immutable image the caller actually observed;
// keep it unchanged until the transaction terminates. A nil Before deliberately
// selects a blind image write. After is the complete image to stage or store.
// Policy retains the existing per-operation client/authoritative distinction.
// The request is runtime-only and changes no persistent encoding.
type NodeUpdateRequest struct {
	Before *Node
	After  *Node
	Policy EntityImagePolicy
	// retained names the vectors a decoded journal record omitted because
	// the update kept them, and retainedIDs identifies them; StoredNodeImages
	// staging restores them from the stored predecessor when it holds those
	// very images (restoreRetainedVectorsLocked). retainedIDs is nil when the
	// record identifies nothing. Client edits set neither.
	retained    vectorRetention
	retainedIDs *retainedVectorIDs
}

// EdgeUpdateRequest carries a relationship mutation. Before is the immutable
// observed image, or nil for a deliberate blind image write; After is the
// complete new image. Observations belong to writes, never to ordinary reads.
type EdgeUpdateRequest struct {
	Before *Edge
	After  *Edge
}

// Operation projects client mutation input for a managed coordinator. Native
// staging captures its own committed preimage before publication or encoding.
func (request NodeUpdateRequest) Operation() Operation {
	return Operation{Type: OpUpdateNode, Node: request.After, OldNode: request.Before, nodeImagePolicy: request.Policy, retainedVectors: request.retained}
}

// Operation projects relationship mutation input for a managed coordinator.
func (request EdgeUpdateRequest) Operation() Operation {
	return Operation{Type: OpUpdateEdge, Edge: request.After, OldEdge: request.Before}
}

// NodeUpdate reads client mutation input from an operation. Accepted semantic
// replay explicitly selects StoredNodeImages at its staging boundary instead.
func (operation Operation) NodeUpdate() NodeUpdateRequest {
	return NodeUpdateRequest{Before: operation.OldNode, After: operation.Node, Policy: operation.nodeImagePolicy, retained: operation.retainedVectors}
}

// EdgeUpdate reads relationship mutation input from an operation.
func (operation Operation) EdgeUpdate() EdgeUpdateRequest {
	return EdgeUpdateRequest{Before: operation.OldEdge, After: operation.Edge}
}

// InScope qualifies both observation and replacement IDs at the same
// namespace boundary. It copies only headers whose IDs need qualification;
// property maps remain immutable inputs owned by the caller.
// For example, request.InScope("tenant:") maps a->b to tenant:a->tenant:b
// in both Before and After, without changing request's original records.
func (request EdgeUpdateRequest) InScope(scope string) EdgeUpdateRequest {
	if scope == "" {
		return request
	}
	request.Before = edgeInScope(request.Before, scope)
	request.After = edgeInScope(request.After, scope)
	return request
}

func edgeInScope(edge *Edge, scope string) *Edge {
	if edge == nil {
		return nil
	}
	id := EdgeID(qualifyEntityID(scope, string(edge.ID)))
	start := NodeID(qualifyEntityID(scope, string(edge.StartNode)))
	end := NodeID(qualifyEntityID(scope, string(edge.EndNode)))
	if id == edge.ID && start == edge.StartNode && end == edge.EndNode {
		return edge
	}
	input := *edge
	input.ID, input.StartNode, input.EndNode = id, start, end
	return &input
}

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
	return Operation{Type: OpUpdateNode, Node: request.After, OldNode: request.Before, nodeImagePolicy: request.Policy}
}

// Operation projects relationship mutation input for a managed coordinator.
func (request EdgeUpdateRequest) Operation() Operation {
	return Operation{Type: OpUpdateEdge, Edge: request.After, OldEdge: request.Before}
}

// NodeUpdate reads client mutation input from an operation. Accepted semantic
// replay explicitly selects StoredNodeImages at its staging boundary instead.
func (operation Operation) NodeUpdate() NodeUpdateRequest {
	return NodeUpdateRequest{Before: operation.OldNode, After: operation.Node, Policy: operation.nodeImagePolicy}
}

// EdgeUpdate reads relationship mutation input from an operation.
func (operation Operation) EdgeUpdate() EdgeUpdateRequest {
	return EdgeUpdateRequest{Before: operation.OldEdge, After: operation.Edge}
}

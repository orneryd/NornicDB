package storage

import "context"

// GetStoredNode reads a node through the transaction's snapshot and pending
// writes, without applying suppression or decay to its committed body.
func (tx *BadgerTransaction) GetStoredNode(id NodeID) (*Node, error) {
	return tx.getNode(id, storedRead)
}

// GetStoredEdge is GetStoredNode for a relationship.
func (tx *BadgerTransaction) GetStoredEdge(id EdgeID) (*Edge, error) {
	return tx.getEdge(id, storedRead)
}

// GetStoredOutgoingEdges returns the transaction's physical outgoing edges.
func (tx *BadgerTransaction) GetStoredOutgoingEdges(id NodeID) ([]*Edge, error) {
	return tx.GetStoredOutgoingEdgesWithContext(context.Background(), id)
}

// GetStoredOutgoingEdgesWithContext retains the transaction snapshot and writes.
func (tx *BadgerTransaction) GetStoredOutgoingEdgesWithContext(ctx context.Context, id NodeID) ([]*Edge, error) {
	return tx.getStoredAdjacentEdgesWithContext(ctx, id, Outgoing)
}

// GetStoredIncomingEdges returns the transaction's physical incoming edges.
func (tx *BadgerTransaction) GetStoredIncomingEdges(id NodeID) ([]*Edge, error) {
	return tx.GetStoredIncomingEdgesWithContext(context.Background(), id)
}

// GetStoredIncomingEdgesWithContext retains the transaction snapshot and writes.
func (tx *BadgerTransaction) GetStoredIncomingEdgesWithContext(ctx context.Context, id NodeID) ([]*Edge, error) {
	return tx.getStoredAdjacentEdgesWithContext(ctx, id, Incoming)
}

func (tx *BadgerTransaction) getStoredAdjacentEdgesWithContext(ctx context.Context, id NodeID, direction EdgeDirection) ([]*Edge, error) {
	edges, err := tx.IncidentEdgesWithContext(ctx, id)
	if err != nil {
		return nil, err
	}
	result := edges[:0]
	for _, edge := range edges {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if (direction == Outgoing && edge.StartNode == id) || (direction == Incoming && edge.EndNode == id) {
			result = append(result, edge)
		}
	}
	return result, nil
}

// GetStoredNodesByLabel collects the transaction's unfiltered label stream.
func (tx *BadgerTransaction) GetStoredNodesByLabel(label string) ([]*Node, error) {
	return tx.GetStoredNodesByLabelWithContext(context.Background(), label)
}

// GetStoredNodesByLabelWithContext carries the caller through this read and its materialization.
func (tx *BadgerTransaction) GetStoredNodesByLabelWithContext(ctx context.Context, label string) ([]*Node, error) {
	var nodes []*Node
	err := StreamStoredNodesByLabelProjectedWithContext(ctx, tx, label, nil, func(node *Node) error {
		nodes = append(nodes, node)
		return nil
	})
	return nodes, err
}

// StreamStoredNodesByLabelProjected preserves label-index and transaction
// overlay reads while including suppressed and decayed committed nodes.
func (tx *BadgerTransaction) StreamStoredNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error {
	return tx.StreamStoredNodesByLabelProjectedWithContext(context.Background(), label, properties, visit)
}

// StreamStoredNodesByLabelProjectedWithContext carries the caller through this read and its materialization.
func (tx *BadgerTransaction) StreamStoredNodesByLabelProjectedWithContext(ctx context.Context, label string, properties []string, visit func(*Node) error) error {
	return tx.streamNodesByLabelProjectedWithContext(ctx, label, properties, storedRead, visit)
}

// StreamStoredEdgesByType preserves the transaction's type stream and overlay
// while including suppressed and decayed committed relationships.
func (tx *BadgerTransaction) StreamStoredEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	return tx.streamEdgesByType(ctx, edgeType, storedRead, visit)
}

var _ StoredEntityReader = (*BadgerTransaction)(nil)

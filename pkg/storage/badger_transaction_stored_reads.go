package storage

// Context-owner and envelope-metadata convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import "context"

// GetStoredNode reads a node through the transaction's snapshot and pending
// writes, without applying suppression or decay to its committed body.
func (tx *BadgerTransaction) GetStoredNode(id NodeID) (*Node, error) {
	return readTransactionEntity(tx, id, (*BadgerTransaction).nodeReadState, cloneTransactionNode, loadTransactionNode, nil, transactionNodeRead{mvccEntityRead: mvccEntityRead{vectors: true, visibility: storedRead}})
}

// GetStoredEdge is GetStoredNode for a relationship.
func (tx *BadgerTransaction) GetStoredEdge(id EdgeID) (*Edge, error) {
	return readTransactionEntity(tx, id, (*BadgerTransaction).edgeReadState, cloneTransactionEdge, (*BadgerTransaction).getCommittedEdgeReadLocked, nil, storedRead)
}

// GetStoredOutgoingEdges returns the transaction's physical outgoing edges.
func (tx *BadgerTransaction) GetStoredOutgoingEdges(id NodeID) ([]*Edge, error) {
	return tx.getStoredAdjacentEdges(id, Outgoing)
}

// GetStoredIncomingEdges returns the transaction's physical incoming edges.
func (tx *BadgerTransaction) GetStoredIncomingEdges(id NodeID) ([]*Edge, error) {
	return tx.getStoredAdjacentEdges(id, Incoming)
}

func (tx *BadgerTransaction) getStoredAdjacentEdges(id NodeID, direction EdgeDirection) ([]*Edge, error) {
	edges, err := tx.IncidentEdges(id)
	if err != nil {
		return nil, err
	}
	result := edges[:0]
	for _, edge := range edges {
		if (direction == Outgoing && edge.StartNode == id) || (direction == Incoming && edge.EndNode == id) {
			result = append(result, edge)
		}
	}
	return result, nil
}

// GetStoredNodesByLabel collects the transaction's unfiltered label stream.
func (tx *BadgerTransaction) GetStoredNodesByLabel(label string) ([]*Node, error) {
	var nodes []*Node
	err := tx.StreamStoredNodesByLabelProjected(label, nil, func(node *Node) error {
		nodes = append(nodes, node)
		return nil
	})
	return nodes, err
}

// StreamStoredNodesByLabelProjected preserves label-index and transaction
// overlay reads while including suppressed and decayed committed nodes.
func (tx *BadgerTransaction) StreamStoredNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error {
	return tx.streamNodesByLabelProjected(context.Background(), label, properties, storedRead, visit)
}

// StreamStoredEdgesByType preserves the transaction's type stream and overlay
// while including suppressed and decayed committed relationships.
func (tx *BadgerTransaction) StreamStoredEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	return tx.streamEdgesByType(ctx, edgeType, storedRead, visit)
}

var _ StoredEntityReader = (*BadgerTransaction)(nil)

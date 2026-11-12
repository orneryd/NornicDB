package storage

import (
	"context"
)

// GetStoredNode reads a node through the transaction's snapshot and pending
// writes, without applying suppression or decay to its committed body.
func (tx *BadgerTransaction) GetStoredNode(ctx context.Context, id NodeID) (*Node, error) {
	return readTransactionEntity(ctx, tx, id, (*BadgerTransaction).nodeReadState, cloneTransactionNode, loadTransactionNode, transactionNodeRead{mvccEntityRead: mvccEntityRead{vectors: true, visibility: storedRead}})
}

// GetStoredEdge is GetStoredNode for a relationship.
func (tx *BadgerTransaction) GetStoredEdge(ctx context.Context, id EdgeID) (*Edge, error) {
	return readTransactionEntity(ctx, tx, id, (*BadgerTransaction).edgeReadState, cloneTransactionEdge, loadTransactionEdge, storedRead)
}

// ReadStoredAdjacentEdges answers one adjacent-edge request over the
// transaction's physically stored incidence: hidden bodies included, the
// request's direction, endpoints and types selecting the relationships.
func (tx *BadgerTransaction) ReadStoredAdjacentEdges(ctx context.Context, request AdjacentEdgeRead) ([]*Edge, error) {
	if err := request.validate(); err != nil {
		return nil, err
	}
	edges, err := tx.IncidentEdges(ctx, request.Node)
	if err != nil {
		return nil, err
	}
	result := edges[:0]
	var cancel bulkReadCancellation
	for _, edge := range edges {
		if err := cancel.check(ctx); err != nil {
			return nil, err
		}
		if !request.admits(edge) {
			continue
		}
		result = append(result, request.header(edge))
	}
	return result, nil
}

// GetStoredNodesByLabel collects the transaction's unfiltered label stream.
func (tx *BadgerTransaction) GetStoredNodesByLabel(ctx context.Context, label string) ([]*Node, error) {
	var nodes []*Node
	err := tx.StreamStoredNodesByLabelProjected(ctx, label, nil, func(node *Node) error {
		nodes = append(nodes, node)
		return nil
	})
	return nodes, err
}

// StreamStoredNodesByLabelProjected preserves label-index and transaction
// overlay reads while including suppressed and decayed committed nodes.
func (tx *BadgerTransaction) StreamStoredNodesByLabelProjected(ctx context.Context, label string, properties []string, visit func(*Node) error) error {
	return tx.streamNodesByLabelProjected(ctx, label, properties, storedRead, visit)
}

// StreamStoredEdgesByType preserves the transaction's type stream and overlay
// while including suppressed and decayed committed relationships.
func (tx *BadgerTransaction) StreamStoredEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	return tx.streamEdgesByType(ctx, edgeType, storedRead, visit)
}

var _ StoredEntityReader = (*BadgerTransaction)(nil)

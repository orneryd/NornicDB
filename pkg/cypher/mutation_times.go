package cypher

import (
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// mutateNodeWithObservation advances a user mutation's version time and sends
// its immutable pre-mutation image through the canonical storage update API.
// Callers own property/label rollback; this helper restores only the timestamp.
func mutateNodeWithObservation(store storage.Engine, before, node *storage.Node) error {
	if node == nil {
		return store.UpdateNode(storage.NodeUpdateRequest{Before: before})
	}
	previous := node.UpdatedAt
	node.UpdatedAt = time.Now()
	updateRequest := storage.NodeUpdateRequest{Before: before, After: node}
	if err := store.UpdateNode(updateRequest); err != nil {
		node.UpdatedAt = previous
		return err
	}
	return nil
}

// mutateEdgeWithObservation stores edge as an update observed from before, as
// mutateNodeWithObservation does for a node.
func mutateEdgeWithObservation(store storage.Engine, before, edge *storage.Edge) error {
	if edge == nil {
		return store.UpdateEdge(storage.EdgeUpdateRequest{Before: before})
	}
	previous := edge.UpdatedAt
	edge.UpdatedAt = time.Now()
	updateRequest := storage.EdgeUpdateRequest{Before: before, After: edge}
	if err := store.UpdateEdge(updateRequest); err != nil {
		edge.UpdatedAt = previous
		return err
	}
	return nil
}

// observeNode copies the record header. Before editing shared maps or slices,
// the caller must detach the changed fields or bind existing rollback copies.
func observeNode(node *storage.Node) *storage.Node {
	if node == nil {
		return nil
	}
	observed := *node
	return &observed
}

// observeEdge returns the image of edge as it is now, as observeNode does.
func observeEdge(edge *storage.Edge) *storage.Edge {
	if edge == nil {
		return nil
	}
	observed := *edge
	return &observed
}

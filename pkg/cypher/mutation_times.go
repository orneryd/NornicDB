package cypher

import (
	"context"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Cypher owns the time of a user mutation. Storage preserves supplied times so
// imports and recovery can restore historical versions without rewriting them.
// Restore the in-memory time if persistence fails, as with the caller's values.
func updateMutatedNode(ctx context.Context, store storage.Engine, node *storage.Node) error {
	previous := node.UpdatedAt
	node.UpdatedAt = time.Now()
	if err := storage.UpdateNodeWithContext(ctx, store, node); err != nil {
		node.UpdatedAt = previous
		return err
	}
	return nil
}

func updateMutatedEdge(store storage.Engine, edge *storage.Edge) error {
	previous := edge.UpdatedAt
	edge.UpdatedAt = time.Now()
	if err := store.UpdateEdge(edge); err != nil {
		edge.UpdatedAt = previous
		return err
	}
	return nil
}

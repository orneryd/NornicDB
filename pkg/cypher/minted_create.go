package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// mintedEntityCreator creates entities whose IDs the executor minted for the
// create (generateID). Such an ID names no stored entity, visible or hidden,
// so a store that would otherwise read storage to rule one out need not.
type mintedEntityCreator interface {
	CreateMintedNode(ctx context.Context, node *storage.Node) (storage.NodeID, error)
	BulkCreateMintedNodes(ctx context.Context, nodes []*storage.Node) error
	CreateMintedEdge(ctx context.Context, edge *storage.Edge) error
	BulkCreateMintedEdges(ctx context.Context, edges []*storage.Edge) error
}

// createMintedNode creates node, whose ID the executor minted, through
// store's minted create when it has one and its ordinary create otherwise.
func createMintedNode(ctx context.Context, store storage.Engine, node *storage.Node) (storage.NodeID, error) {
	if creator, ok := store.(mintedEntityCreator); ok {
		return creator.CreateMintedNode(ctx, node)
	}
	return store.CreateNode(ctx, node)
}

// bulkCreateMintedNodes is createMintedNode for a batch.
func bulkCreateMintedNodes(ctx context.Context, store storage.Engine, nodes []*storage.Node) error {
	if creator, ok := store.(mintedEntityCreator); ok {
		return creator.BulkCreateMintedNodes(ctx, nodes)
	}
	return store.BulkCreateNodes(ctx, nodes)
}

// createMintedEdge is createMintedNode for a relationship.
func createMintedEdge(ctx context.Context, store storage.Engine, edge *storage.Edge) error {
	if creator, ok := store.(mintedEntityCreator); ok {
		return creator.CreateMintedEdge(ctx, edge)
	}
	return store.CreateEdge(ctx, edge)
}

// bulkCreateMintedEdges is createMintedEdge for a batch.
func bulkCreateMintedEdges(ctx context.Context, store storage.Engine, edges []*storage.Edge) error {
	if creator, ok := store.(mintedEntityCreator); ok {
		return creator.BulkCreateMintedEdges(ctx, edges)
	}
	return store.BulkCreateEdges(ctx, edges)
}

package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// mintedEntityCreator creates entities whose IDs the executor minted for the
// create (generateID). Such an ID names no stored entity, visible or hidden,
// so a store that would otherwise read storage to rule one out need not.
type mintedEntityCreator interface {
	CreateMintedNode(node *storage.Node) (storage.NodeID, error)
	BulkCreateMintedNodes(nodes []*storage.Node) error
	CreateMintedEdge(edge *storage.Edge) error
	BulkCreateMintedEdges(edges []*storage.Edge) error
}

// createMintedNode creates node, whose ID the executor minted, through
// store's minted create when it has one and its ordinary create otherwise.
func createMintedNode(ctx context.Context, store storage.Engine, node *storage.Node) (storage.NodeID, error) {
	if creator, ok := storage.OwnedCapability[mintedEntityCreator](store); ok {
		return storage.CreateMintedNodeWithContext(ctx, creator, node)
	}
	return store.CreateNode(node)
}

// bulkCreateMintedNodes is createMintedNode for a batch.
func bulkCreateMintedNodes(ctx context.Context, store storage.Engine, nodes []*storage.Node) error {
	if creator, ok := storage.OwnedCapability[mintedEntityCreator](store); ok {
		return storage.BulkCreateMintedNodesWithContext(ctx, creator, nodes)
	}
	return store.BulkCreateNodes(nodes)
}

// createMintedEdge is createMintedNode for a relationship.
func createMintedEdge(ctx context.Context, store storage.Engine, edge *storage.Edge) error {
	if creator, ok := storage.OwnedCapability[mintedEntityCreator](store); ok {
		return storage.CreateMintedEdgeWithContext(ctx, creator, edge)
	}
	return store.CreateEdge(edge)
}

// bulkCreateMintedEdges is createMintedEdge for a batch.
func bulkCreateMintedEdges(ctx context.Context, store storage.Engine, edges []*storage.Edge) error {
	if creator, ok := storage.OwnedCapability[mintedEntityCreator](store); ok {
		return storage.BulkCreateMintedEdgesWithContext(ctx, creator, edges)
	}
	return store.BulkCreateEdges(edges)
}

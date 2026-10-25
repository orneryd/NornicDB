package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

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
func createMintedNode(store storage.Engine, node *storage.Node) (storage.NodeID, error) {
	if creator, ok := store.(mintedEntityCreator); ok {
		return creator.CreateMintedNode(node)
	}
	return store.CreateNode(node)
}

// bulkCreateMintedNodes is createMintedNode for a batch.
func bulkCreateMintedNodes(store storage.Engine, nodes []*storage.Node) error {
	if creator, ok := store.(mintedEntityCreator); ok {
		return creator.BulkCreateMintedNodes(nodes)
	}
	return store.BulkCreateNodes(nodes)
}

// createMintedEdge is createMintedNode for a relationship.
func createMintedEdge(store storage.Engine, edge *storage.Edge) error {
	if creator, ok := store.(mintedEntityCreator); ok {
		return creator.CreateMintedEdge(edge)
	}
	return store.CreateEdge(edge)
}

// bulkCreateMintedEdges is createMintedEdge for a batch.
func bulkCreateMintedEdges(store storage.Engine, edges []*storage.Edge) error {
	if creator, ok := store.(mintedEntityCreator); ok {
		return creator.BulkCreateMintedEdges(edges)
	}
	return store.BulkCreateEdges(edges)
}

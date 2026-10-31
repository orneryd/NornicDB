// NOR-160 lane (Claude), 30 October 2026: entity caller copies are complete.
package storage

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/stretchr/testify/require"
)

func nor160OneCopyNode(id NodeID) *Node {
	return &Node{ID: id, Labels: []string{"Held"}, Properties: nor160OwnershipProperties(),
		ChunkEmbeddings: [][]float32{{1, 2, 3}}, NamedEmbeddings: map[string][]float32{"original": {4, 5, 6}},
		EmbedMeta: map[string]any{"nested": map[string]any{"value": "original"}}}
}

// Every storage Copier, and every shape the single node-shape owner copies,
// leaves no exported reference shared with the retained image.
func TestNOR160CopierCompleteStorageEntities(t *testing.T) {
	node := nor160OneCopyNode("test:a")
	edge := &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: nor160OwnershipProperties()}
	require.Empty(t, readownership.Unpopulated(node))
	require.Empty(t, readownership.Unpopulated(edge))
	require.Empty(t, readownership.SharedReferences(node, node.CallerCopy()), "Node")
	require.Empty(t, readownership.SharedReferences(edge, edge.CallerCopy()), "Edge")
	for _, shape := range []struct {
		name       string
		properties []string
		strip      bool
	}{{"full", nil, false}, {"stripped", nil, true}, {"projected", []string{"nested", "bytes"}, false}, {"projected-stripped", []string{"nested"}, true}} {
		require.Empty(t, readownership.SharedReferences(node, shapeNode(node, shape.properties, shape.strip, false)), shape.name)
	}
}

// Planted control: a node copier that keeps the retained property map must
// turn the completeness check red.
func TestNOR160CopierControlPartialNode(t *testing.T) {
	node := nor160OneCopyNode("test:a")
	partial := *node
	partial.Labels = append([]string(nil), node.Labels...)
	require.Contains(t, readownership.SharedReferences(node, &partial), ".Properties")
}

// deliveredNodes and deliveredEdges deliver a read's images as its public
// method does (NOR-160 read delivery owner), for tests of internal readers.
func deliveredNodes(images []nodeImage, err error) ([]*Node, error) {
	return deliverNodes(images, nil, false), err
}

func deliveredEdges(images []edgeImage, err error) ([]*Edge, error) {
	return deliverEdges(images), err
}

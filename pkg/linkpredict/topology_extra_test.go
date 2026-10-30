package linkpredict

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
)

func TestNodeSet_Size(t *testing.T) {
	ns := make(NodeSet)
	assert.Equal(t, 0, ns.Size())

	ns[storage.NodeID("n1")] = struct{}{}
	assert.Equal(t, 1, ns.Size())

	ns[storage.NodeID("n2")] = struct{}{}
	ns[storage.NodeID("n3")] = struct{}{}
	assert.Equal(t, 3, ns.Size())
}

func TestGraph_Neighbors(t *testing.T) {
	g := make(Graph)

	n1 := storage.NodeID("n1")
	n2 := storage.NodeID("n2")
	n3 := storage.NodeID("n3")

	// Node with no neighbors
	neighbors := g.Neighbors(n1)
	assert.NotNil(t, neighbors)
	assert.Equal(t, 0, len(neighbors))

	// Add neighbors
	g[n1] = NodeSet{
		n2: struct{}{},
		n3: struct{}{},
	}

	neighbors = g.Neighbors(n1)
	assert.Equal(t, 2, len(neighbors))
	assert.True(t, neighbors.Contains(n2))
	assert.True(t, neighbors.Contains(n3))
}

func TestNodeSet_Contains(t *testing.T) {
	ns := NodeSet{
		storage.NodeID("n1"): struct{}{},
	}

	assert.True(t, ns.Contains(storage.NodeID("n1")))
	assert.False(t, ns.Contains(storage.NodeID("n2")))
}

func TestGraph_Degree(t *testing.T) {
	g := make(Graph)
	n1 := storage.NodeID("n1")
	n2 := storage.NodeID("n2")

	// Node not in graph
	assert.Equal(t, 0, g.Degree(n1))

	// Node with edges
	g[n1] = NodeSet{n2: struct{}{}}
	assert.Equal(t, 1, g.Degree(n1))
}

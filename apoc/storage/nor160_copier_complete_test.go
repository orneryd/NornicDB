// NOR-160 lane (Claude), 30 October 2026: APOC entity caller copies are complete.
package storage

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/stretchr/testify/require"
)

func TestNOR160CopierCompleteAPOCEntities(t *testing.T) {
	node := &Node{ID: 1, Labels: []string{"Held"}, Properties: nor160Props()}
	rel := &Relationship{ID: 2, Type: "T", StartNode: 1, EndNode: 3, Properties: nor160Props()}
	path := &Path{Nodes: []*Node{node}, Relationships: []*Relationship{rel}, Length: 1}
	for name, sample := range map[string]any{"Node": node, "Relationship": rel, "Path": path} {
		require.Empty(t, readownership.Unpopulated(sample), name)
	}
	require.Empty(t, readownership.SharedReferences(node, node.CallerCopy()), "Node")
	require.Empty(t, readownership.SharedReferences(rel, rel.CallerCopy()), "Relationship")
	require.Empty(t, readownership.SharedReferences(path, path.callerOwned()), "Path")
}

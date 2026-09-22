package tck

import (
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

func TestPathFromBoltRestoresTraversalOrderWhenNodesRepeat(t *testing.T) {
	path := neo4j.Path{
		Nodes: []neo4j.Node{
			{Id: 1, Labels: []string{"A"}},
			{Id: 2, Labels: []string{"B"}},
		},
		Relationships: []neo4j.Relationship{
			{Id: 10, StartId: 1, EndId: 2, Type: "OUT"},
			{Id: 11, StartId: 2, EndId: 1, Type: "BACK"},
		},
	}

	converted := pathFromBolt(path)
	require.Len(t, converted.Nodes, 3)
	require.Len(t, converted.Segments, 2)
	require.Equal(t, converted.Nodes[0].Identity, converted.Nodes[2].Identity)
	require.True(t, converted.Segments[0].Forward)
	require.True(t, converted.Segments[1].Forward)
}

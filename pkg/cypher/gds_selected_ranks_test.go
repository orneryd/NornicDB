package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGDSSelectedConstituentRanks(t *testing.T) {
	projection := newGraphProjection("ranks", "selection")
	projection.NodeIDs = []string{"a", "b", "c"}
	projection.Types = []string{"R", "S"}
	projection.Relationships = []ProjectedRelationship{
		{Source: "a", Target: "c", Type: "R", Properties: map[string]float64{"w": 2}},
		{Source: "a", Target: "b", Type: "R", Properties: map[string]float64{"w": 1}},
		{Source: "a", Target: "b", Type: "R", Properties: map[string]float64{"w": 3}},
		{Source: "a", Target: "c", Type: "S", Properties: map[string]float64{"w": 4}},
		{Source: "a", Target: "b", Type: "S", Properties: map[string]float64{"w": 5}},
	}
	projection.RelationshipPropertyKeys = map[string][]string{"R": {"w"}, "S": {"w"}}
	projection.indexNodes(context.Background(), true)
	projection.finishCounts(context.Background())
	for _, tc := range []struct {
		name     string
		types    []string
		options  gdsSubgraphOptions
		ranks    bool
		outgoing []gdsEdge
	}{
		{"all", nil, gdsSubgraphOptions{includeTypes: true}, true, []gdsEdge{{1, 1}, {1, 3}, {2, 2}, {1, 5}, {2, 4}}},
		{"wildcard", []string{"*"}, gdsSubgraphOptions{includeTypes: true}, true, []gdsEdge{{1, 1}, {1, 3}, {2, 2}, {1, 5}, {2, 4}}},
		{"one of several", []string{"R"}, gdsSubgraphOptions{includeTypes: true}, false, []gdsEdge{{1, 1}, {1, 3}, {2, 2}}},
		{"another type", []string{"S"}, gdsSubgraphOptions{includeTypes: true}, false, []gdsEdge{{1, 5}, {2, 4}}},
		{"duplicate selection", []string{"R", "R"}, gdsSubgraphOptions{includeTypes: true}, false, []gdsEdge{{1, 1}, {1, 3}, {2, 2}}},
		{"distinct duplicate selection", []string{"S", "R", "S"}, gdsSubgraphOptions{includeTypes: true}, true, []gdsEdge{{1, 1}, {1, 3}, {2, 2}, {1, 5}, {2, 4}}},
		{"no types", []string{}, gdsSubgraphOptions{includeTypes: true, emptyTypesNone: true}, false, nil},
		{"nodes only", nil, gdsSubgraphOptions{includeTypes: true, omitAdjacency: true}, false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			weightProperty := "w"
			if tc.options.emptyTypesNone {
				weightProperty = ""
			}
			graph, err := selectGDSSubgraph(projection, nil, tc.types, weightProperty, tc.options)
			require.NoError(t, err)
			require.Equal(t, tc.ranks, graph.outgoingTypes != nil)
			graph.orderOutgoing(context.Background())
			if tc.options.omitAdjacency {
				require.Nil(t, graph.outgoing)
			} else {
				require.Equal(t, tc.outgoing, graph.outgoing[0])
			}
		})
	}
}

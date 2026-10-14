package cypher

import (
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/apoc"
	"github.com/orneryd/nornicdb/apoc/community"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestLouvainEntryPointsShareRingCommunities(t *testing.T) {
	gdsFollowUpCatalog(t)
	exec, run := newGDSEngineGraph(t, "louvainEntryPoints", gdsNode("0", "N", nil), gdsNode("1", "N", nil), gdsNode("2", "N", nil), gdsNode("3", "N", nil))
	nodes := make([]*community.Node, 4)
	relationships := make([]*community.Relationship, 4)
	for i := range nodes {
		nodes[i] = &community.Node{ID: int64(i)}
		relationships[i] = &community.Relationship{ID: int64(i), StartNode: int64(i), EndNode: int64((i + 1) % 4)}
		require.NoError(t, exec.storage.CreateEdge(&storage.Edge{ID: storage.EdgeID(strconv.Itoa(i)), StartNode: storage.NodeID(strconv.Itoa(i)), EndNode: storage.NodeID(strconv.Itoa((i + 1) % 4)), Type: "R"}))
	}
	for _, query := range []string{
		"CALL apoc.algo.louvain(['N'])",
		"CALL gds.graph.project('ring','N',{R:{orientation:'UNDIRECTED'}}) YIELD graphName CALL gds.louvain.stream(graphName,{concurrency:1,consecutiveIds:true}) YIELD nodeId,communityId RETURN nodeId,communityId",
	} {
		t.Run(query, func(t *testing.T) {
			result, err := run(query)
			require.NoError(t, err)
			require.Len(t, result.Rows, 4)
			for _, row := range result.Rows {
				require.EqualValues(t, 0, row[1])
			}
		})
	}
	t.Run("registered apoc.community.louvain", func(t *testing.T) {
		require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
		registered, found := apoc.GetInfo("apoc.community.louvain")
		require.True(t, found)
		handler, ok := registered.Handler.(func([]*community.Node, []*community.Relationship, community.LouvainConfig) []community.CommunityResult)
		require.True(t, ok)
		result := handler(nodes, relationships, community.DefaultLouvainConfig())
		require.Len(t, result, 4)
		for _, row := range result {
			require.Equal(t, int64(0), row.CommunityID)
		}
	})
}

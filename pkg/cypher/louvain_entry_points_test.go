package cypher

import (
	"context"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/apoc"
	"github.com/orneryd/nornicdb/apoc/community"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/louvain"
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

func TestLouvainEntryPointsSelfLoopsAndParallelEdges(t *testing.T) {
	for _, tc := range []struct {
		name  string
		edges [][3]int
		want  []int
	}{
		{"self loop", [][3]int{{0, 1, 1}, {0, 3, 1}, {1, 2, 1}, {2, 2, 1}}, []int{0, 1, 1, 0}},
		// Splitting an edge's weight across distinct physical relationships
		// must preserve the graph, including parallel self relationships.
		{"parallel", [][3]int{{0, 1, 1}, {0, 1, 1}, {0, 3, 2}, {1, 2, 2}, {2, 2, 1}, {2, 2, 1}}, []int{0, 1, 1, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec, run := newGDSEngineGraph(t, "louvainLoops", gdsNode("0", "N", nil), gdsNode("1", "N", nil), gdsNode("2", "N", nil), gdsNode("3", "N", nil))
			nodes := make([]*community.Node, 4)
			for i := range nodes {
				nodes[i] = &community.Node{ID: int64(i)}
			}
			var rels []*community.Relationship
			graph := louvain.NewGraph(4)
			for i, edge := range tc.edges {
				require.NoError(t, exec.storage.CreateEdge(&storage.Edge{ID: storage.EdgeID(strconv.Itoa(i)), StartNode: storage.NodeID(strconv.Itoa(edge[0])), EndNode: storage.NodeID(strconv.Itoa(edge[1])), Type: "R", Properties: map[string]interface{}{"w": int64(edge[2])}}))
				rels = append(rels, &community.Relationship{ID: int64(i), StartNode: int64(edge[0]), EndNode: int64(edge[1]), Properties: map[string]interface{}{"weight": float64(edge[2])}})
				graph.Link(edge[0], edge[1], float64(edge[2]))
			}
			require.Equal(t, tc.want, louvain.Final(louvain.Run(graph, louvain.DefaultOptions())), "engine reference")
			check := func(t *testing.T, rows []community.CommunityResult) {
				t.Helper()
				got := make([]int, len(rows))
				for i, row := range rows {
					got[i] = int(row.CommunityID)
				}
				require.Equal(t, tc.want, got)
			}
			check(t, community.Louvain(nodes, rels, community.DefaultLouvainConfig()))
			require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
			registered, ok := apoc.GetInfo("apoc.community.louvain")
			require.True(t, ok)
			handler := registered.Handler.(func([]*community.Node, []*community.Relationship, community.LouvainConfig) []community.CommunityResult)
			check(t, handler(nodes, rels, community.DefaultLouvainConfig()))
			// Exercise the bundled typed-dispatch path, rather than calling the
			// library handler only; this is also the plugin's exact handler.
			value, err := callPluginHandlerContext(context.Background(), handler, []interface{}{nodes, rels, community.DefaultLouvainConfig()})
			require.NoError(t, err)
			check(t, value.([]community.CommunityResult))
			result, err := run("CALL apoc.algo.louvain(['N'],{weightProperty:'w'})")
			require.NoError(t, err)
			got := make([]int, 4)
			for _, row := range result.Rows {
				node := row[0].(*storage.Node)
				i, err := strconv.Atoi(string(node.ID))
				require.NoError(t, err)
				got[i] = int(toInt64(row[1]))
			}
			require.Equal(t, tc.want, got, "native CALL must share the same input and engine")
		})
	}
}

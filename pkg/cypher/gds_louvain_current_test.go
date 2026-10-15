package cypher

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGDSLouvainSelectedSeedProperty(t *testing.T) {
	gdsFollowUpCatalog(t)
	e, run := newGDSEngineGraph(t, "louvainseedselection", gdsNode("0", "N", nil), gdsNode("1", "N", nil), gdsNode("2", "N", nil), gdsNode("3", "Other", map[string]interface{}{"seed": int64(100)}))
	require.NoError(t, e.storage.CreateEdge(&storage.Edge{ID: "r", StartNode: "0", EndNode: "1", Type: "R"}))
	_, err := run("CALL gds.graph.project('seedlabels',{N:{},Other:{properties:'seed'}},'R')")
	require.NoError(t, err)
	// GDS rejects availability on a selected label even though the full
	// projection's property list (also shown in the error) contains the key.
	for _, labels := range []string{"['N']", "['N','Other']", "['Other','N']", "[]", "['*']"} {
		_, err := run("CALL gds.louvain.stream('seedlabels',{concurrency:1,nodeLabels:" + labels + ",seedProperty:'seed'})")
		require.ErrorContains(t, err, "Seed property `seed` not found in graph with node properties: [seed]", labels)
	}
	result, err := run("CALL gds.louvain.stream('seedlabels',{concurrency:1,nodeLabels:['Other'],seedProperty:'seed'})")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"3", int64(100), nil}}, result.Rows)
	result, err = run("CALL gds.louvain.stream('seedlabels',{concurrency:1,nodeLabels:['N'],tolerance:9007199254740993})")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"0", int64(1), nil}, {"1", int64(1), nil}, {"2", int64(2), nil}}, result.Rows)
}

func TestGDSLouvainStoredStringIDSignature(t *testing.T) {
	for _, ids := range [][]string{{"0", "1", "2"}, {"node-a", "node-b", "node-c"}} {
		t.Run(ids[0], func(t *testing.T) {
			gdsFollowUpCatalog(t)
			e, run := newGDSEngineGraph(t, "louvainstringids", gdsNode(ids[0], "N", nil), gdsNode(ids[1], "N", nil), gdsNode(ids[2], "N", nil))
			require.NoError(t, e.storage.CreateEdge(&storage.Edge{ID: "r", StartNode: storage.NodeID(ids[0]), EndNode: storage.NodeID(ids[1]), Type: "R"}))
			_, err := run("CALL gds.graph.project('ids','N','R')")
			require.NoError(t, err)
			for _, prefix := range []string{"", "WITH 1 AS x "} {
				result, err := run(prefix + "CALL gds.louvain.stream('ids',{concurrency:1}) YIELD nodeId RETURN nodeId,size(nodeId) AS size,nodeId + 1 AS concatenated")
				require.NoError(t, err)
				require.Len(t, result.Rows, len(ids))
				for i, id := range ids {
					// NornicDB deliberately exposes stored string IDs; GDS
					// exposes INTEGER IDs. The declared type must match the
					// extension's values for direct and prefixed calls alike.
					require.Equal(t, []interface{}{id, int64(len(id)), fmt.Sprint(id, 1)}, result.Rows[i])
				}
			}
		})
	}
}

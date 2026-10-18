package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGDSStreamsRenderNodeIDs: every GDS algorithm stream over an aggregation
// projection of INTEGER ids returns those INTEGER ids, as GDS does, through
// the one renderer. A registered gds.*.stream that has no call here, or no
// projection-rendered entry in gdsNodeProcedures, fails the test, so a new algorithm can't
// skip the renderer unnoticed.
func TestGDSStreamsRenderNodeIDs(t *testing.T) {
	gdsFollowUpCatalog(t)
	_, run := newGDSEngineGraph(t, "streamnodeids")
	must := func(query string) [][]interface{} {
		t.Helper()
		result, err := run(query)
		require.NoError(t, err, query)
		return result.Rows
	}
	must("UNWIND [[1,2],[2,3],[3,4],[4,5],[5,1],[2,4]] AS e " +
		"RETURN gds.graph.project('ints', e[0], e[1], {relationshipProperties: {w: 1.0}}, {undirectedRelationshipTypes: ['*']})")
	linkPrediction := func(name string) string {
		return "CALL gds.linkPrediction." + name + ".stream('ints', {sourceNode: 1, topK: 5}) YIELD node1, node2 RETURN node1, node2"
	}
	calls := map[string]string{
		"gds.fastRP.stream":                  "CALL gds.fastRP.stream('ints', {embeddingDimension: 4, randomSeed: 1}) YIELD nodeId RETURN nodeId",
		"gds.louvain.stream":                 "CALL gds.louvain.stream('ints') YIELD nodeId RETURN nodeId",
		"gds.nodeSimilarity.stream":          "CALL gds.nodeSimilarity.stream('ints') YIELD node1, node2, shared RETURN node1, node2, shared",
		"gds.nodeSimilarity.filtered.stream": "CALL gds.nodeSimilarity.filtered.stream('ints', {sourceNodeFilter: [1]}) YIELD node1, node2, shared RETURN node1, node2, shared",
	}
	for _, name := range []string{"adamicAdar", "commonNeighbors", "resourceAllocation", "preferentialAttachment", "jaccard", "predict"} {
		calls["gds.linkPrediction."+name+".stream"] = linkPrediction(name)
	}

	ensureBuiltInProceduresRegistered()
	streams := 0
	for _, spec := range ListRegisteredProcedures() {
		if !strings.HasPrefix(spec.Name, "gds.") || !strings.HasSuffix(spec.Name, ".stream") {
			continue
		}
		streams++
		require.Contains(t, calls, spec.Name, "a GDS stream without a node-id case here")
		require.Equal(t, gdsRenderProjection, gdsNodeProcedures[spec.Name].rendering, "a GDS stream the renderer doesn't know: %s", spec.Name)
	}
	require.Equal(t, len(calls), streams)

	for name, query := range calls {
		rows := must(query)
		require.NotEmpty(t, rows, name)
		for _, row := range rows {
			for _, value := range row {
				if shared, isList := value.([]interface{}); isList {
					for _, entry := range shared {
						require.IsType(t, int64(0), entry.(map[string]interface{})["nodeId"], "%s shared %v", name, row)
					}
					continue
				}
				require.IsType(t, int64(0), value, "%s %v", name, row)
			}
		}
	}
}

// TestGDSStreamRendersWithTheProjectionItRead: a stream's ids are produced
// from the projection it computed from as its rows are built, so another
// request dropping the name afterwards, or dropping it and projecting another
// graph under it, changes nothing in rows already returned.
func TestGDSStreamRendersWithTheProjectionItRead(t *testing.T) {
	gdsFollowUpCatalog(t)
	exec, run := newGDSEngineGraph(t, "streamread")
	project := "UNWIND [[1,2],[2,3],[3,1]] AS e RETURN gds.graph.project('ints', e[0], e[1])"
	for _, replace := range []bool{false, true} {
		_, err := run(project)
		require.NoError(t, err)
		result, err := exec.callGdsLouvainStream([]interface{}{"ints"})
		require.NoError(t, err)
		_, err = run("CALL gds.graph.drop('ints')")
		require.NoError(t, err)
		if replace {
			_, err = run("UNWIND [[7,8]] AS e RETURN gds.graph.project('ints', e[0], e[1])")
			require.NoError(t, err)
		}
		require.Len(t, result.Rows, 3, "replace=%v", replace)
		for _, row := range result.Rows {
			require.IsType(t, int64(0), row[0], "replace=%v", replace)
		}
		if replace {
			_, err = run("CALL gds.graph.drop('ints')")
			require.NoError(t, err)
		}
	}
}

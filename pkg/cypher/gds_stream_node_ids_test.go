package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGDSStreamsRenderNodeIDs: every GDS algorithm stream over an aggregation
// projection of INTEGER ids returns those INTEGER ids, as GDS does, through
// the one renderer. A registered gds.*.stream that has no call here, or no
// entry in gdsStreamNodeIDColumns, fails the test, so a new algorithm can't
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
		require.Contains(t, gdsStreamNodeIDColumns, spec.Name, "a GDS stream the renderer doesn't know")
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

// TestGDSStreamRendersWithTheProjectionItRead: a stream renders its ids with
// the projection it computed from, even when another request drops the name
// meanwhile, or drops it and projects another graph under it.
func TestGDSStreamRendersWithTheProjectionItRead(t *testing.T) {
	gdsFollowUpCatalog(t)
	exec, run := newGDSEngineGraph(t, "streamread")
	project := "UNWIND [[1,2],[2,3],[3,1]] AS e RETURN gds.graph.project('ints', e[0], e[1])"
	for _, replace := range []bool{false, true} {
		_, err := run(project)
		require.NoError(t, err)
		handler := gdsStreamHandler("gds.louvain.stream", func(statement admittedRelationshipStatement, ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
			result, err := e.callGdsLouvainStream(args)
			_, dropErr := run("CALL gds.graph.drop('ints')")
			require.NoError(t, dropErr)
			if replace {
				_, projectErr := run("UNWIND [[7,8]] AS e RETURN gds.graph.project('ints', e[0], e[1])")
				require.NoError(t, projectErr)
			}
			return result, err
		})
		streamContext, statement := procedureTestContext(context.Background(), "gds.louvain.stream", []interface{}{"ints"})
		result, err := handler(statement, streamContext, exec, "CALL gds.louvain.stream($fixtureArgument0)", []interface{}{"ints"})
		require.NoError(t, err)
		require.Len(t, result.Rows, 3, "replace=%v", replace)
		for _, row := range result.Rows {
			require.IsType(t, int64(0), row[0], "replace=%v", replace)
		}
		require.Nil(t, result.gdsProjection)
		if replace {
			_, err = run("CALL gds.graph.drop('ints')")
			require.NoError(t, err)
		}
	}
}

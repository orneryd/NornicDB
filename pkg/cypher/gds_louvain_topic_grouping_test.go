package cypher

import (
	"context"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// topicGroupingGraph is three weighted topic clusters joined by weak bridges,
// over two relationship types, plus a topic x with three light links into the
// a-cluster and one heavy link into the c-cluster. One a-pair is linked in
// both directions, so the undirected projection keeps a parallel pair.
const topicGroupingGraph = `CREATE (a1:Topic {name:'a1'}), (a2:Topic {name:'a2'}), (a3:Topic {name:'a3'}), (a4:Topic {name:'a4'}),
	(b1:Topic {name:'b1'}), (b2:Topic {name:'b2'}), (b3:Topic {name:'b3'}), (b4:Topic {name:'b4'}),
	(c1:Topic {name:'c1'}), (c2:Topic {name:'c2'}), (c3:Topic {name:'c3'}), (c4:Topic {name:'c4'}),
	(x:Topic {name:'x'}),
	(a1)-[:RELATED {weight: 5.0}]->(a2), (a1)-[:RELATED {weight: 4.0}]->(a3), (a2)-[:RELATED {weight: 6.0}]->(a3),
	(a3)-[:RELATED {weight: 5.0}]->(a4), (a2)-[:RELATED {weight: 3.0}]->(a4), (a4)-[:RELATED {weight: 2.0}]->(a1),
	(a2)-[:RELATED {weight: 1.0}]->(a1),
	(b1)-[:RELATED {weight: 4.0}]->(b2), (b2)-[:RELATED {weight: 5.0}]->(b3), (b3)-[:RELATED {weight: 6.0}]->(b4),
	(b1)-[:RELATED {weight: 3.0}]->(b3), (b2)-[:RELATED {weight: 4.0}]->(b4), (b4)-[:RELATED {weight: 2.5}]->(b1),
	(c1)-[:CO_OCCURS {weight: 6.0}]->(c2), (c2)-[:CO_OCCURS {weight: 4.5}]->(c3), (c3)-[:CO_OCCURS {weight: 5.5}]->(c4),
	(c1)-[:CO_OCCURS {weight: 3.5}]->(c3), (c4)-[:CO_OCCURS {weight: 4.0}]->(c2), (c4)-[:CO_OCCURS {weight: 3.0}]->(c1),
	(a4)-[:RELATED {weight: 0.5}]->(b1), (b4)-[:CO_OCCURS {weight: 0.3}]->(c1), (c4)-[:RELATED {weight: 0.2}]->(a1),
	(x)-[:RELATED {weight: 0.1}]->(a1), (x)-[:RELATED {weight: 0.1}]->(a2), (x)-[:RELATED {weight: 0.1}]->(a3),
	(x)-[:CO_OCCURS {weight: 4.0}]->(c2)`

// TestGDSLouvainTopicGroupingEndToEnd pins the topic-grouping flow a
// downstream application runs: an undirected, weighted Cypher aggregation
// projection of stored relationships, gds.louvain.stream read back through
// gds.util.asNode, then gds.graph.drop. Counts, schema and communities are
// GDS 2.13.12's on the same graph; community IDs are compared as partitions,
// since GDS names a community by one of its internal node IDs.
func TestGDSLouvainTopicGroupingEndToEnd(t *testing.T) {
	gdsFollowUpCatalog(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "topicgrouping"))
	ctx := context.Background()
	rows := func(query string) [][]interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	rows(topicGroupingGraph)

	projected := rows("MATCH (a:Topic)-[r]->(b:Topic) " +
		"RETURN gds.graph.project('topics', a, b, {relationshipType: type(r), relationshipProperties: {weight: r.weight}}, {undirectedRelationshipTypes: ['*']}) AS g")
	require.Len(t, projected, 1)
	g := projected[0][0].(map[string]interface{})
	require.Equal(t, "topics", g["graphName"])
	require.Equal(t, int64(13), g["nodeCount"])
	require.Equal(t, int64(52), g["relationshipCount"], "26 stored relationships, each projected in both directions")

	listed := rows("CALL gds.graph.list('topics') YIELD schemaWithOrientation RETURN schemaWithOrientation")
	require.Len(t, listed, 1)
	relationships := listed[0][0].(map[string]interface{})["relationships"].(map[string]interface{})
	require.Len(t, relationships, 2)
	for _, kind := range []string{"RELATED", "CO_OCCURS"} {
		schema, ok := relationships[kind].(map[string]interface{})
		require.True(t, ok, kind)
		require.Equal(t, "UNDIRECTED", schema["direction"], kind)
		require.Contains(t, schema["properties"], "weight", kind)
	}

	partitions := func(query string) [][]string {
		t.Helper()
		result := rows(query)
		require.Len(t, result, 13)
		byCommunity := map[int64][]string{}
		for _, row := range result {
			topic, ok := row[0].(string)
			require.True(t, ok, "gds.util.asNode(nodeId).name is the topic's name: %#v", row[0])
			community, ok := row[1].(int64)
			require.True(t, ok, "communityId is an INTEGER: %#v", row[1])
			byCommunity[community] = append(byCommunity[community], topic)
		}
		groups := make([][]string, 0, len(byCommunity))
		for _, members := range byCommunity {
			sort.Strings(members)
			groups = append(groups, members)
		}
		sort.Slice(groups, func(i, j int) bool { return groups[i][0] < groups[j][0] })
		return groups
	}

	// The weights decide x: three 0.1 links to the a-cluster lose to one 4.0
	// link to the c-cluster. Unweighted, GDS puts x with the a-cluster.
	require.Equal(t, [][]string{{"a1", "a2", "a3", "a4"}, {"b1", "b2", "b3", "b4"}, {"c1", "c2", "c3", "c4", "x"}},
		partitions("CALL gds.louvain.stream('topics', {relationshipWeightProperty: 'weight'}) YIELD nodeId, communityId "+
			"RETURN gds.util.asNode(nodeId).name AS topic, communityId"))
	require.Equal(t, [][]string{{"a1", "a2", "a3", "a4", "x"}, {"b1", "b2", "b3", "b4"}, {"c1", "c2", "c3", "c4"}},
		partitions("CALL gds.louvain.stream('topics') YIELD nodeId, communityId "+
			"RETURN gds.util.asNode(nodeId).name AS topic, communityId"))

	dropped := rows("CALL gds.graph.drop('topics') YIELD graphName, nodeCount, relationshipCount RETURN graphName, nodeCount, relationshipCount")
	require.Equal(t, [][]interface{}{{"topics", int64(13), int64(52)}}, dropped)
	require.Equal(t, [][]interface{}{{false}}, rows("CALL gds.graph.exists('topics') YIELD exists RETURN exists"))
	require.Empty(t, graphProjections)
}

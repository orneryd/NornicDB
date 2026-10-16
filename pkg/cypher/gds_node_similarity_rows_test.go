package cypher

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// similarityRowsQuery is nornicdb.similarity.stream over every H relationship
// of newNodeSimilarityGraph as rows, for one source.
func similarityRowsQuery(weight, config string) string {
	return "MATCH (d:Doc)-[r:H]->(i) WITH collect({source: d, thing: i" + weight + "}) AS rows " +
		"CALL nornicdb.similarity.stream(rows, {" + config + "}) YIELD node1, node2, similarity, shared " +
		"RETURN node1, node2, similarity, shared"
}

// TestSimilarityRowsMatchesNodeSimilarity: over complete rows, the GDS
// metrics give gds.nodeSimilarity.stream's rows for the same source, values,
// order and shared column alike (B9; the existing procedure is the oracle).
func TestSimilarityRowsMatchesNodeSimilarity(t *testing.T) {
	run := newNodeSimilarityGraph(t)
	for _, metric := range []string{"JACCARD", "OVERLAP", "COSINE"} {
		for _, weighted := range []bool{false, true} {
			gdsConfig, weight := "similarityMetric: '"+metric+"'", ""
			if weighted {
				gdsConfig += ", relationshipWeightProperty: 'w'"
				weight = ", weight: r.w"
			}
			all := run("CALL gds.nodeSimilarity.stream('ns', {" + gdsConfig + "}) YIELD node1, node2, similarity, shared RETURN node1, node2, similarity, shared")
			for _, source := range []string{"d1", "d2", "d3", "d4"} {
				var want [][]interface{}
				for _, row := range all {
					if row[0] == source {
						want = append(want, row)
					}
				}
				got := run(similarityRowsQuery(weight, "sourceNode: '"+source+"', similarityMetric: '"+strings.ToLower(metric)+"'"))
				require.Equal(t, want, got, "%s weighted=%v source=%s", metric, weighted, source)
			}
		}
	}
}

// TestSimilarityRowsResourceAllocation: RESOURCE_ALLOCATION sums 1/holders
// over the shared things, equal to the same score written in Cypher, ties by
// node id; maxHolders drops the things held by more sources.
func TestSimilarityRowsResourceAllocation(t *testing.T) {
	run := newNodeSimilarityGraph(t)
	reference := func(source string, maxHolders int) [][]interface{} {
		rows := run(fmt.Sprintf("MATCH (s:Doc {name: '%s'})-[:H]->(t)<-[:H]-(c:Doc) WHERE c <> s "+
			"WITH c, t, COUNT { (t)<-[:H]-(:Doc) } AS holders WHERE holders <= %d "+
			"RETURN c.name AS node2, sum(1.0 / holders) AS similarity ORDER BY similarity DESC, node2", source, maxHolders))
		out := make([][]interface{}, len(rows))
		for i, row := range rows {
			out[i] = []interface{}{row[0], row[1]}
		}
		return out
	}
	for _, source := range []string{"d1", "d2", "d3", "d4"} {
		for _, maxHolders := range []int{0, 2, 3} {
			config := "sourceNode: '" + source + "', similarityMetric: 'RESOURCE_ALLOCATION'"
			limit := 1 << 30
			if maxHolders > 0 {
				config += fmt.Sprintf(", maxHolders: %d", maxHolders)
				limit = maxHolders
			}
			rows := run(similarityRowsQuery("", config))
			got := make([][]interface{}, len(rows))
			for i, row := range rows {
				require.Equal(t, source, row[0])
				got[i] = []interface{}{row[1], row[2]}
			}
			want := reference(source, limit)
			require.Len(t, got, len(want), "source=%s maxHolders=%d", source, maxHolders)
			for i := range want {
				require.Equal(t, want[i][0], got[i][0], "source=%s maxHolders=%d", source, maxHolders)
				require.InDelta(t, want[i][1], got[i][1], 1e-12, "source=%s maxHolders=%d", source, maxHolders)
			}
		}
	}
	// d1 holds i1 (2 holders) and bank (4 holders).
	require.Equal(t, [][]interface{}{
		{"d1", "d2", 0.75, []interface{}{map[string]interface{}{"nodeId": "i1", "weight": 0.5}, map[string]interface{}{"nodeId": "bank", "weight": 0.25}}},
		{"d1", "d3", 0.25, []interface{}{map[string]interface{}{"nodeId": "bank", "weight": 0.25}}},
	}, run(similarityRowsQuery("", "sourceNode: 'd1', similarityMetric: 'RESOURCE_ALLOCATION', topK: 2")))
	// Source-centred rows (the source's things and their holders, as the
	// user guide builds them) give the scores complete rows give.
	for _, source := range []string{"d1", "d2", "d3", "d4"} {
		config := "{sourceNode: seed, similarityMetric: 'RESOURCE_ALLOCATION', maxHolders: 3}"
		centred := run("MATCH (seed:Doc {name: '" + source + "'})-[:H]->(thing)<-[:H]-(other:Doc) " +
			"WITH seed, collect(DISTINCT {source: other, thing: thing}) + [(seed)-[:H]->(t) | {source: seed, thing: t}] AS rows " +
			"CALL nornicdb.similarity.stream(rows, " + config + ") YIELD node1, node2, similarity, shared " +
			"RETURN node1, node2, similarity, shared")
		require.Equal(t, run(similarityRowsQuery("", "sourceNode: '"+source+"', similarityMetric: 'RESOURCE_ALLOCATION', maxHolders: 3")), centred, source)
	}
}

// TestSimilarityRowsContract: the row contract of nornicdb.similarity.stream:
// repeated rows are one with the largest weight, a null thing is a source
// with no things, an absent source gives no rows, an explicit cutoff of 0
// keeps every other source, and ids work as well as nodes.
func TestSimilarityRowsContract(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "ns"))
	run := func(rows, config string) [][]interface{} {
		t.Helper()
		result, err := exec.Execute(context.Background(), "CALL nornicdb.similarity.stream("+rows+", {"+config+"}) YIELD node1, node2, similarity RETURN node1, node2, similarity", nil)
		require.NoError(t, err)
		return result.Rows
	}
	// Repeated rows: a (thing) once, its largest weight; a default and an
	// explicit weight: the larger once the default (1/holders) is known.
	require.Equal(t, [][]interface{}{{"a", "b", 0.75}}, run(
		"[{source: 'a', thing: 'x', weight: 0.25}, {source: 'a', thing: 'x', weight: 0.75}, {source: 'a', thing: 'x'}, {source: 'b', thing: 'x', weight: 1}, {source: 'b', thing: 'x'}]",
		"sourceNode: 'a', similarityMetric: 'RESOURCE_ALLOCATION'"))
	require.Equal(t, [][]interface{}{{"a", "b", 0.5}}, run(
		"[{source: 'a', thing: 'x', weight: 0.25}, {source: 'a', thing: 'x'}, {source: 'b', thing: 'x'}]",
		"sourceNode: 'a', similarityMetric: 'RESOURCE_ALLOCATION'"))
	// Integer ids and a null thing; c shares nothing, so only an explicit 0
	// cutoff lists it.
	rows := "[{source: 1, thing: 10}, {source: 2, thing: 10}, {source: 2, thing: 11}, {source: 3, thing: null}]"
	require.Equal(t, [][]interface{}{{"1", "2", 0.5}}, run(rows, "sourceNode: 1"))
	require.Equal(t, [][]interface{}{{"1", "2", 0.5}, {"1", "3", 0.0}}, run(rows, "sourceNode: 1, similarityCutoff: 0"))
	require.Equal(t, [][]interface{}{}, run(rows, "sourceNode: 99"))
	require.Equal(t, [][]interface{}{}, run("[]", "sourceNode: 1"))
	// topK and the tie order by node id.
	require.Equal(t, [][]interface{}{{"a", "b", 1.0}, {"a", "c", 1.0}}, run(
		"[{source: 'a', thing: 'x'}, {source: 'd', thing: 'x'}, {source: 'c', thing: 'x'}, {source: 'b', thing: 'x'}]",
		"sourceNode: 'a', topK: 2"))
	// maxHolders unset or 0: no limit, every thing counts.
	hub := "[{source: 'a', thing: 'x'}, {source: 'b', thing: 'x'}, {source: 'c', thing: 'x'}, {source: 'b', thing: 'y'}, {source: 'a', thing: 'y'}]"
	unlimited := [][]interface{}{{"a", "b", 0.5 + 1.0/3}, {"a", "c", 1.0 / 3}}
	require.Equal(t, unlimited, run(hub, "sourceNode: 'a', similarityMetric: 'RESOURCE_ALLOCATION'"))
	require.Equal(t, unlimited, run(hub, "sourceNode: 'a', similarityMetric: 'RESOURCE_ALLOCATION', maxHolders: 0"))
	require.Equal(t, [][]interface{}{{"a", "b", 0.5}}, run(hub, "sourceNode: 'a', similarityMetric: 'RESOURCE_ALLOCATION', maxHolders: 2"))
	// A list sourceNode is one source: its nodes' things together, none of
	// them a candidate, node1 their ids.
	require.Equal(t, [][]interface{}{{[]interface{}{"a1", "a2"}, "b", 1.0/3 + 1.0/3}, {[]interface{}{"a1", "a2"}, "c", 1.0 / 3}}, run(
		"[{source: 'a1', thing: 'x'}, {source: 'a2', thing: 'y'}, {source: 'b', thing: 'x'}, {source: 'b', thing: 'y'}, {source: 'c', thing: 'y'}, {source: 'a2', thing: 'x'}]",
		"sourceNode: ['a2', 'a1'], similarityMetric: 'RESOURCE_ALLOCATION'"))
	require.Equal(t, [][]interface{}{}, run("[{source: 'b', thing: 'x'}]", "sourceNode: ['a1', 'a2']"))
}

// TestSimilarityRowsDroppedWeights: a weight that is null, NaN or infinite
// (1.0 / having for a thing nobody holds) drops its row's thing, and its
// source stays a source; a weight given is used as is.
func TestSimilarityRowsDroppedWeights(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "ns"))
	rows := "[{source: 'a', thing: 'x', weight: 1.0}, {source: 'b', thing: 'x', weight: $inf}, {source: 'c', thing: 'x', weight: null}, " +
		"{source: 'd', thing: 'x', weight: $nan}, {source: 'e', thing: 'x', weight: 0.5}, {source: 'f', thing: 'x', weight: 1.0 / $zero}]"
	run := func(config string) [][]interface{} {
		t.Helper()
		result, err := exec.Execute(context.Background(), "CALL nornicdb.similarity.stream("+rows+", {"+config+"}) YIELD node1, node2, similarity RETURN node1, node2, similarity",
			map[string]interface{}{"inf": math.Inf(1), "nan": math.NaN(), "zero": 0.0})
		require.NoError(t, err)
		return result.Rows
	}
	require.Equal(t, [][]interface{}{{"a", "e", 0.5}}, run("sourceNode: 'a', similarityMetric: 'RESOURCE_ALLOCATION'"))
	require.Equal(t, [][]interface{}{{"a", "e", 0.5}, {"a", "b", 0.0}, {"a", "c", 0.0}, {"a", "d", 0.0}, {"a", "f", 0.0}},
		run("sourceNode: 'a', similarityMetric: 'RESOURCE_ALLOCATION', similarityCutoff: 0"))
}

// TestSimilarityRowsMatchesRelatedScoring reproduces a related-documents
// ranking written in Cypher (the oracle): a start with two seed documents
// (their things together, neither returned), library-wide holder counts
// passed as weight 1.0 / having, candidates kept to one collection,
// duplicate links, score = reduce over the shared things rarest first, ties by
// the documents' id property. The stored ids run opposite to the id
// properties, so the property, not the node id, must decide ties. Scores and
// shared things are compared exactly.
func TestSimilarityRowsMatchesRelatedScoring(t *testing.T) {
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "ns")
	exec := NewStorageExecutor(engine)
	propertyOf := map[string]string{}
	node := func(id, label, property string) {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{label}, Properties: map[string]interface{}{"id": property}})
		require.NoError(t, err)
		propertyOf[id] = property
	}
	for i, property := range []string{"doc-h", "doc-g", "doc-f", "doc-e", "doc-d", "doc-c", "doc-b", "doc-a"} {
		node(fmt.Sprintf("n%d", i+1), "D", property)
	}
	for i, property := range []string{"thing-f", "thing-e", "thing-d", "thing-c", "thing-b", "thing-a"} {
		node(fmt.Sprintf("t%d", i+1), "T", property)
	}
	node("c1", "C", "c1")
	node("c2", "C", "c2")
	edges := 0
	link := func(from, kind, to string) {
		edges++
		require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%d", edges)), StartNode: storage.NodeID(from), EndNode: storage.NodeID(to), Type: kind}))
	}
	// Seeds n1 and n2 share t2; n1 links t1 twice. n3 and n4 tie, as do n5
	// and n6; n7 is outside the collection; n8 shares only t4, which no seed
	// has; t5 and t1 have four holders each, so their shared weights tie and
	// the things' id properties order them; t6 is a hub every document has.
	for _, edge := range [][2]string{
		{"n1", "t1"}, {"n1", "t1"}, {"n1", "t2"}, {"n2", "t2"}, {"n2", "t3"},
		{"n3", "t1"}, {"n3", "t1"}, {"n3", "t3"}, {"n4", "t1"}, {"n4", "t3"},
		{"n5", "t2"}, {"n6", "t2"}, {"n7", "t1"}, {"n8", "t4"},
		{"n1", "t5"}, {"n3", "t5"}, {"n4", "t5"}, {"n8", "t5"},
	} {
		link(edge[0], "HAS", edge[1])
	}
	for i := 1; i <= 8; i++ {
		link(fmt.Sprintf("n%d", i), "HAS", "t6")
		collection := "c1"
		if i >= 7 {
			collection = "c2"
		}
		link(fmt.Sprintf("n%d", i), "MEMBER_OF", collection)
	}
	run := func(query string) [][]interface{} {
		t.Helper()
		result, err := exec.Execute(context.Background(), query, map[string]interface{}{"seeds": []interface{}{"doc-h", "doc-g"}, "scope": []interface{}{"c1"}})
		require.NoError(t, err, query)
		return result.Rows
	}
	const held = "MATCH (seed:D) WHERE seed.id IN $seeds " +
		"WITH collect(seed) AS seeds UNWIND seeds AS s MATCH (s)-[:HAS]->(thing:T) " +
		"WITH seeds, collect(DISTINCT thing) AS things UNWIND things AS thing " +
		"MATCH (holder:D)-[:HAS]->(thing) WITH seeds, thing, count(DISTINCT holder) AS having "
	const inScope = "EXISTS { MATCH (candidate)-[:MEMBER_OF]->(c:C) WHERE c.id IN $scope }"
	oracle := run(held +
		"MATCH (candidate:D)-[:HAS]->(thing) WHERE NOT candidate IN seeds AND having > 0 AND " + inScope + " " +
		"WITH DISTINCT candidate, thing, having ORDER BY having, thing.id " +
		"WITH candidate, collect({id: thing.id, weight: 1.0 / having}) AS shared " +
		"RETURN candidate.id AS id, reduce(total = 0.0, s IN shared | total + s.weight) AS score, [s IN shared | s.id] AS things " +
		"ORDER BY score DESC, id")
	procedure := func(tieBreak string) [][]interface{} {
		rows := run(held +
			"MATCH (candidate:D)-[:HAS]->(thing) WHERE candidate IN seeds OR " + inScope + " " +
			"WITH seeds, collect({source: candidate, thing: thing, weight: 1.0 / having}) AS rows " +
			"CALL nornicdb.similarity.stream(rows, {sourceNode: seeds, similarityMetric: 'RESOURCE_ALLOCATION', topK: 100" + tieBreak + "}) " +
			"YIELD node1, node2, similarity, shared RETURN node1, node2, similarity, [s IN shared | s.nodeId] AS things")
		out := make([][]interface{}, len(rows))
		for i, row := range rows {
			require.Equal(t, []interface{}{"n1", "n2"}, row[0])
			things := []interface{}{}
			for _, thing := range row[3].([]interface{}) {
				things = append(things, propertyOf[thing.(string)])
			}
			out[i] = []interface{}{propertyOf[row[1].(string)], row[2], things}
		}
		return out
	}
	require.Len(t, oracle, 4)
	require.Equal(t, oracle, procedure(", tieBreakProperty: 'id'"))
	// Without tieBreakProperty, ties go by the stored id instead: n3 before n4.
	require.Equal(t, []interface{}{"doc-e", "doc-f"}, []interface{}{oracle[0][0], oracle[1][0]})
	byNodeID := procedure("")
	require.Equal(t, []interface{}{"doc-f", "doc-e"}, []interface{}{byNodeID[0][0], byNodeID[1][0]})
}

// TestSimilarityRowsErrors: invalid rows and configuration are refused,
// and gds.nodeSimilarity still refuses RESOURCE_ALLOCATION as GDS does.
func TestSimilarityRowsErrors(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "ns"))
	for _, tc := range []struct{ call, want string }{
		{"nornicdb.similarity.stream(null, {sourceNode: 'a'})", "rows"},
		{"nornicdb.similarity.stream([{source: 'a', thing: 'x'}], {})", "sourceNode parameter required"},
		{"nornicdb.similarity.stream([{source: 'a', thing: 'x'}], {sourceNode: true})", "sourceNode"},
		{"nornicdb.similarity.stream([{source: 'a', thing: 'x'}], {sourceNode: 'a', similarityMetric: 'PEARSON'})", "PEARSON is not a valid metric. Available metrics are JACCARD, OVERLAP, COSINE and RESOURCE_ALLOCATION"},
		{"nornicdb.similarity.stream([{source: 'a', thing: 'x'}], {sourceNode: 'a', nodeLabels: ['A']})", "nodeLabels"},
		{"nornicdb.similarity.stream([{source: 'a', thing: 'x'}], {sourceNode: 'a', similarityCutoff: 1.5})", "similarityCutoff"},
		{"nornicdb.similarity.stream([{source: 'a', thing: 'x'}], {sourceNode: 'a', similarityMetric: 'RESOURCE_ALLOCATION', similarityCutoff: -1})", "similarityCutoff"},
		{"nornicdb.similarity.stream([{source: 'a', thing: 'x'}], {sourceNode: 'a', topK: 0})", "topK"},
		{"nornicdb.similarity.stream([{source: 'a', thing: 'x'}], {sourceNode: 'a', maxHolders: -1})", "maxHolders"},
		{"nornicdb.similarity.stream([{source: 'a', thing: 'x'}], {sourceNode: []})", "sourceNode parameter required"},
		{"nornicdb.similarity.stream([{source: 'a', thing: 'x'}], {sourceNode: ['a', true]})", "sourceNode"},
		{"nornicdb.similarity.stream([{source: 'a', thing: 'x'}], {sourceNode: 'a', tieBreakProperty: 1})", "tieBreakProperty"},
		{"nornicdb.similarity.stream(['a'], {sourceNode: 'a'})", "Row 0 of the similarity rows must be a map with source and thing, but found: String"},
		{"nornicdb.similarity.stream([{source: 'a', thing: 'x', kind: 1}], {sourceNode: 'a'})", "Row 0 of the similarity rows has the unexpected key `kind`"},
		{"nornicdb.similarity.stream([{source: 'a', thing: 'x'}, {thing: 'x'}], {sourceNode: 'a'})", "Row 1 of the similarity rows needs a node or node id for `source`, but found: null"},
		{"nornicdb.similarity.stream([{source: 'a', thing: true}], {sourceNode: 'a'})", "Row 0 of the similarity rows needs a node or node id for `thing`, but found: Boolean"},
		{"nornicdb.similarity.stream([{source: 'a', weight: 1}], {sourceNode: 'a'})", "Row 0 of the similarity rows needs a node or node id for `thing`, but found: null"},
		{"nornicdb.similarity.stream([{source: 'a', thing: 'x', weight: -1}], {sourceNode: 'a'})", "Row 0 of the similarity rows has the weight -1.0"},
		{"nornicdb.similarity.stream([{source: 'a', thing: 'x', weight: 'heavy'}], {sourceNode: 'a'})", "Row 0 of the similarity rows has the weight String"},
	} {
		_, err := exec.Execute(context.Background(), "CALL "+tc.call, nil)
		require.Error(t, err, tc.call)
		require.Contains(t, err.Error(), tc.want, tc.call)
	}
	_, err := readNodeSimilarityConfig(context.Background(), &GraphProjection{}, map[string]interface{}{"similarityMetric": "RESOURCE_ALLOCATION"}, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "RESOURCE_ALLOCATION is not a valid metric. Available metrics include Jaccard and Overlap")
}

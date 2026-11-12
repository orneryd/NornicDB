package cypher

// The graph readers: the identity graph reader the whole-graph algorithms and
// the apoc.path expanders read through, the point-read owner, and the path
// algorithms' one undirected adjacency read. These controls hold a PROFILE
// statement to the populations those readers actually returned, through the
// public entry point on both parsers: the node population a scan handed over,
// the entries each adjacency request returned before the caller's own type
// selection narrowed them, and each point read the statement performed. They
// also hold the other half of the contract - a read that fails is the
// statement's error, not an empty expansion - and the registration policy that
// keeps a custom replacement of one of these names opaque.
//
// The fixtures fix every population in the statement: each labelled entry point
// is unique, so a statement's own scan population is one node and a difference
// in the records is a difference in the reader, not in a planner choice. An
// adjacency read is reported as one record per direction the request selected,
// because two directional requests are two reads; the type-selection detail
// ("outgoing:X") is the pattern's, because the reader applies it.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newGraphReaderPathFixture is the graph the path readers below walk: a :Start
// node a with an :X relationship to b, b's :X relationship to c, c's :X
// relationship back to a, a :Side node d with a :Y relationship into a, and an
// unconnected :Side node e. d's :Y relationship is what makes a's incoming
// population differ from its outgoing one, and the cycle is what makes the
// path algorithms read b's adjacency after a's.
func newGraphReaderPathFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	// The result cache is off: these controls observe the same text twice (once
	// unobserved and once with a collector), and a cached answer would perform
	// no read for the second run.
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "graph_reader_paths"), QueryCachePolicy{MaxEntries: 0})
	_, err := exec.Execute(context.Background(), `CREATE
		(a:Start {name: 'a'}), (b:Step {name: 'b'}), (c:End {name: 'c'}),
		(d:Side {name: 'd'}), (e:Side {name: 'e'}),
		(a)-[:X]->(b), (b)-[:X]->(c), (c)-[:X]->(a),
		(d)-[:Y]->(a)`, nil)
	require.NoError(t, err)
	return exec
}

// TestProfilePathExpandersReportTheAdjacencyTheyRead: the apoc.path expanders
// read their start node's adjacency in both directions for every node they
// expand, and each reached node once, whatever relationship filter the call
// named: the filter selects among the entries the read returned, so it cannot
// hide the entries of another type. a's outgoing read returns its one :X
// relationship and its incoming read returns c's :X and d's :Y relationship.
func TestProfilePathExpandersReportTheAdjacencyTheyRead(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newGraphReaderPathFixture(t)
		start := gdsAccess{kind: profileKindNodeByLabelScan, detail: "Start", rows: 1, hits: 1}
		outgoing := func(population int64) gdsAccess {
			return gdsAccess{kind: profileKindExpand, detail: "outgoing", rows: population, hits: population}
		}
		incoming := func(population int64) gdsAccess {
			return gdsAccess{kind: profileKindExpand, detail: "incoming", rows: population, hits: population}
		}
		points := func(population int64) gdsAccess {
			return gdsAccess{kind: profileKindNodeByIdSeek, detail: "", rows: population, hits: population}
		}

		// One step from a: a's adjacency, and the one node the step reaches.
		expanded := []gdsAccess{start, outgoing(1), incoming(2), points(1)}
		expandedRows := requireAlgorithmReads(t, exec,
			"MATCH (n:Start) CALL apoc.path.expand(n, 'X>', '', 1, 1) YIELD path RETURN length(path) AS len",
			expanded...)
		require.Len(t, expandedRows.Rows, 1)

		// expandConfig is the same walk through a config: same reads.
		configured := requireAlgorithmReads(t, exec,
			"MATCH (n:Start) CALL apoc.path.expandConfig(n, {relationshipFilter: 'X>', minLevel: 1, maxLevel: 1}) YIELD path RETURN length(path) AS len",
			expanded...)
		require.Equal(t, expandedRows.Rows, configured.Rows, "expandConfig reads and returns what expand does")

		// Two steps: a's adjacency and b's, and the two nodes they reach.
		steps := []gdsAccess{start, outgoing(2), incoming(3), points(2)}
		subgraph := requireAlgorithmReads(t, exec,
			"MATCH (n:Start) CALL apoc.path.subgraphNodes(n, {relationshipFilter: 'X>', minLevel: 1, maxLevel: 2}) YIELD node RETURN node.name AS name",
			steps...)
		require.Len(t, subgraph.Rows, 2, "the walk reaches b and c")

		// subgraphAll answers with the relationships between the nodes it
		// returned, so it reads each of those nodes' outgoing adjacency once
		// more: b's :X and c's :X relationship.
		all := requireAlgorithmReads(t, exec,
			"MATCH (n:Start) CALL apoc.path.subgraphAll(n, {relationshipFilter: 'X>', minLevel: 1, maxLevel: 2}) YIELD nodes, relationships RETURN size(nodes) AS n",
			start, outgoing(4), incoming(3), points(2))
		require.Len(t, all.Rows, 1)

		// The spanning tree includes the start node itself, so it reads a's
		// adjacency and the one node it reaches, and nothing else.
		tree := requireAlgorithmReads(t, exec,
			"MATCH (n:Start) CALL apoc.path.spanningTree(n, {relationshipFilter: 'X>', minLevel: 0, maxLevel: 1}) YIELD path RETURN length(path) AS len",
			start, outgoing(1), incoming(2), points(1))
		require.Len(t, tree.Rows, 2, "the tree holds the start path and the one path that extends it")
	})
}

// TestProfilePathAlgorithmsReportTheAdjacencyTheyRead: the path algorithms
// follow adjacency through the one read owner the expander uses, so a call
// reports each request's entries in both directions even though its filter
// follows one of them: the entries of the other direction were read. A* reads
// the endpoint and every node it heuristically scores through the statement's
// point-read owner; dijkstra and allSimplePaths read no node at all.
func TestProfilePathAlgorithmsReportTheAdjacencyTheyRead(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newGraphReaderPathFixture(t)
		start := gdsAccess{kind: profileKindNodeByLabelScan, detail: "Start", rows: 1, hits: 1}
		end := gdsAccess{kind: profileKindNodeByLabelScan, detail: "End", rows: 1, hits: 1}
		outgoing := gdsAccess{kind: profileKindExpand, detail: "outgoing", rows: 2, hits: 2}
		incoming := gdsAccess{kind: profileKindExpand, detail: "incoming", rows: 3, hits: 3}

		// a and b are expanded; the search stops at c.
		requireAlgorithmReads(t, exec,
			"MATCH (s:Start), (t:End) CALL apoc.algo.dijkstra(s, t, 'X>', '', 1.0) YIELD path, weight RETURN weight",
			start, end, outgoing, incoming)

		requireAlgorithmReads(t, exec,
			"MATCH (s:Start), (t:End) CALL apoc.algo.allSimplePaths(s, t, 'X>', 10) YIELD path RETURN path",
			start, end, outgoing, incoming)

		// The endpoint, the start node the first heuristic scores, and the two
		// candidates the search scores after expanding a and b.
		requireAlgorithmReads(t, exec,
			"MATCH (s:Start), (t:End) CALL apoc.algo.aStar(s, t, 'X>', '', 'lat', 'lon') YIELD path, weight RETURN weight",
			start, end, outgoing, incoming,
			gdsAccess{kind: profileKindNodeByIdSeek, detail: "", rows: 4, hits: 4})
	})
}

// TestProfileNeighborListingsReportTheAdjacencyTheyRead: the hop listings walk
// the same undirected adjacency, so each hop of each node in the frontier is
// one request in each direction, and every node they return is one point read.
func TestProfileNeighborListingsReportTheAdjacencyTheyRead(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newGraphReaderPathFixture(t)
		want := []gdsAccess{
			{kind: profileKindNodeByLabelScan, detail: "Start", rows: 1, hits: 1},
			{kind: profileKindExpand, detail: "outgoing", rows: 2, hits: 2},
			{kind: profileKindExpand, detail: "incoming", rows: 3, hits: 3},
			{kind: profileKindNodeByIdSeek, detail: "", rows: 2, hits: 2},
		}
		requireAlgorithmReads(t, exec,
			"MATCH (n:Start) CALL apoc.neighbors.tohop(n, 'X>', 2) YIELD node RETURN node.name AS name", want...)
		requireAlgorithmReads(t, exec,
			"MATCH (n:Start) CALL apoc.neighbors.byhop(n, 'X>', 2) YIELD nodes RETURN size(nodes) AS n", want...)
	})
}

// TestProfileLinkPredictionStreamsReportTheGraphTheyRead: a link-prediction
// stream reads every stored node's outgoing adjacency to build the graph it
// scores, and the hybrid one also reads each node whose embeddings it compares -
// the source once and every candidate it scores - through the config's
// point-read owner. The fixture's a, b, c and disconnected iso are the four
// nodes, and a, b and c start the three :K relationships.
func TestProfileLinkPredictionStreamsReportTheGraphTheyRead(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newGDSGraph(t)
		source := graphReaderElementID(t, exec, "MATCH (n:P {name: 'a'}) RETURN elementId(n) AS id")
		nodes := gdsAccess{kind: profileKindAllNodesScan, detail: "", rows: 4, hits: 4}
		outgoing := gdsAccess{kind: profileKindExpand, detail: "outgoing", rows: 3, hits: 3}

		requireAlgorithmReads(t, exec,
			fmt.Sprintf("CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: %q}) YIELD node1, node2, score RETURN node1, node2, score", source),
			nodes, outgoing)

		// Hybrid reads the source's full image and the one candidate c it
		// scores; common_neighbors needs no degree, so nothing reads adjacency
		// twice.
		requireAlgorithmReads(t, exec,
			fmt.Sprintf("CALL gds.linkPrediction.predict.stream({sourceNode: %q, algorithm: 'common_neighbors'}) YIELD node1, node2, score, topology_score, semantic_score, reason RETURN node1, node2, score", source),
			nodes, outgoing,
			gdsAccess{kind: profileKindNodeByIdSeek, detail: "", rows: 2, hits: 2})
	})
}

// graphReaderElementID returns the element id of the one node query returns.
func graphReaderElementID(t *testing.T, exec *StorageExecutor, query string) string {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	id, ok := result.Rows[0][0].(string)
	require.True(t, ok, "an element id is a string")
	return id
}

// TestProfileExpandReportsAPointNodeInput: a start node given as an id is one
// point read of the statement's point-read owner - the same owner every other
// point read uses - and the node the step reaches is one more. The id names no
// scan population, so the statement's node reads are its point reads alone.
func TestProfileExpandReportsAPointNodeInput(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newGraphReaderPathFixture(t)
		start := graphReaderElementID(t, exec, "MATCH (n:Start) RETURN elementId(n) AS id")
		query := "CALL apoc.path.expand($start, 'X>', '', 1, 1) YIELD path RETURN length(path) AS len"
		params := map[string]interface{}{"start": start}

		plain, err := exec.Execute(context.Background(), query, params)
		require.NoError(t, err)
		collector := newProfileRuntimeCollector()
		observed, err := exec.Execute(withProfileRuntimeCollector(context.Background(), collector), query, params)
		require.NoError(t, err)
		require.Equal(t, plain.Rows, observed.Rows, "an observed statement is the same statement")
		require.ElementsMatch(t, []gdsAccess{
			{kind: profileKindNodeByIdSeek, detail: "", rows: 2, hits: 2},
			{kind: profileKindExpand, detail: "outgoing", rows: 1, hits: 1},
			{kind: profileKindExpand, detail: "incoming", rows: 2, hits: 2},
		}, profileGDSAccessesOfKinds(collector, profileKindAllNodesScan, profileKindNodeByLabelScan, profileKindExpand, profileKindNodeByIdSeek), "the id the statement read and the node it reached")

		result, err := exec.Execute(t.Context(), "PROFILE "+query, params)
		require.NoError(t, err)
		require.Equal(t, plain.Rows, result.Rows, "PROFILE returns the unobserved statement's rows")
		plan := executionPlanFromResult(t, result)
		require.True(t, plan.TotalDBHitsObserved)
		require.Empty(t, plan.Unobserved)
	})
}

// TestProfileTraversalStepKeepsItsTypedReadDetail: a traversal step reads with
// the pattern's types, and the one recorder keeps that selection in the detail
// ("outgoing:X", "outgoing:X|Y"), while the population it reports is the
// entries the read handed over.
func TestProfileTraversalStepKeepsItsTypedReadDetail(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newGraphReaderPathFixture(t)
		for _, tc := range []struct {
			pattern, detail string
			rows            [][]interface{}
		}{
			{"-[:X]->", "outgoing:X", [][]interface{}{{"b"}}},
			{"-[:X|Y]->", "outgoing:X|Y", [][]interface{}{{"b"}}},
			{"<-[:X]-", "incoming:X", [][]interface{}{{"c"}}},
			{"<-[:X|Y]-", "incoming:X|Y", [][]interface{}{{"c"}, {"d"}}},
			{"-[:X]-", "both:X", [][]interface{}{{"b"}, {"c"}}},
			{"-[:X|Y]-", "both:X|Y", [][]interface{}{{"b"}, {"c"}, {"d"}}},
		} {
			query := "MATCH (n:Start)" + tc.pattern + "(m) RETURN m.name AS name"
			rows, collector := profileReaderAccesses(t, exec, query)
			require.ElementsMatch(t, tc.rows, rows)
			population := int64(len(tc.rows))
			requireExpandAccesses(t, collector, map[string]gdsAccess{tc.detail: {rows: population, hits: population}})
			result, err := exec.Execute(t.Context(), "PROFILE "+query, nil)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.rows, result.Rows)
			plan := executionPlanFromResult(t, result)
			operators := profilePlanOperators(plan.Root, profileKindExpand, tc.detail)
			require.Len(t, operators, 1)
			require.Equal(t, population, operators[0].ActualRows)
			require.Equal(t, population, operators[0].DBHits)
		}
	})
}

// TestGraphAdjacencyReadFailureFailsThePathStatements: the path expander and
// the path algorithms follow adjacency through one read owner, and a read that
// fails is the statement's error. Before that owner reported the failure these
// statements returned their partial result - no path at all - and no error.
func TestGraphAdjacencyReadFailureFailsThePathStatements(t *testing.T) {
	failure := errors.New("live relationships unavailable")
	forEachParser(t, func(t *testing.T) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "path_read_error")
		seed := NewStorageExecutor(store)
		_, err := seed.Execute(context.Background(), `CREATE (a:Start {name: 'a'})-[:X]->(b:End {name: 'b'})`, nil)
		require.NoError(t, err)
		exec := NewStorageExecutor(&liveAlgorithmReadFailureEngine{Engine: store, failure: failure})
		for _, query := range []string{
			"MATCH (n:Start) CALL apoc.path.expand(n, 'X>', '', 1, 1) YIELD path RETURN length(path) AS len",
			"MATCH (s:Start), (t:End) CALL apoc.algo.dijkstra(s, t, 'X>', '', 1.0) YIELD path, weight RETURN weight",
		} {
			_, err := exec.Execute(context.Background(), query, nil)
			require.ErrorIs(t, err, failure, query)
		}
	})
}

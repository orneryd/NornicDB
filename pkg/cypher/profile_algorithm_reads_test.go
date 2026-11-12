package cypher

// The six whole-graph APOC algorithms read one node population and the adjacency
// of every node they rank. These controls hold a PROFILE statement to what those
// reads actually returned: the nodes the scan handed over, and every adjacency
// entry a request returned before the call's relationship-type selection
// narrowed it. Each case runs through the public entry point on both parsers,
// once with the collector PROFILE's owner installs in the context and once under
// PROFILE itself, so the statement's operators and the ledger behind them are
// the same reads.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// newAlgorithmReadGraph is five nodes and four relationships: a, b and c are :A
// nodes on a directed :X cycle, d is a :B node with one :Y relationship into
// that cycle, and e is a :B node with no relationship at all.
func newAlgorithmReadGraph(t *testing.T) *StorageExecutor {
	t.Helper()
	exec, _ := newProfileRuntimeTestExecutor(t, `CREATE
		(a:A {name: 'a'}), (b:A {name: 'b'}), (c:A {name: 'c'}),
		(d:B {name: 'd'}), (e:B {name: 'e'}),
		(a)-[:X]->(b), (b)-[:X]->(c), (c)-[:X]->(a),
		(d)-[:Y]->(a)`)
	return exec
}

// requireAlgorithmReads runs query (without the PROFILE keyword) with the
// collector PROFILE's owner installs in the context, holds it to the rows it
// returns unobserved, and then runs PROFILE query and holds it to the same
// reads: the ledger records want exactly, and one plan operator per record with
// the same population. The statement's db hits are the populations it read, so a
// read that is performed and not reported fails here.
func requireAlgorithmReads(t *testing.T, exec *StorageExecutor, query string, want ...gdsAccess) *ExecuteResult {
	t.Helper()
	rows, collector := profileReaderAccesses(t, exec, query)
	require.ElementsMatch(t, want, profileGDSAccessesOfKinds(collector, profileKindAllNodesScan, profileKindNodeByLabelScan, profileKindExpand, profileKindNodeByIdSeek), "the reads %q reported", query)

	result, err := exec.Execute(t.Context(), "PROFILE "+query, nil)
	require.NoError(t, err)
	require.Equal(t, rows, result.Rows, "PROFILE returns the unobserved statement's rows: %q", query)
	plan := executionPlanFromResult(t, result)
	require.True(t, plan.TotalDBHitsObserved, "every read of %q is reported", query)
	require.Empty(t, plan.Unobserved, query)
	var hits int64
	for _, read := range want {
		hits += read.hits
		operators := profilePlanOperators(plan.Root, read.kind, read.detail)
		require.Len(t, operators, 1, "%q reports one %s record for %q", query, read.kind, read.detail)
		operator := operators[0]
		require.True(t, operator.RowsObserved, query)
		require.True(t, operator.DBHitsObserved, query)
		require.Equal(t, read.rows, operator.ActualRows,
			"%s %q returned %d entities", read.kind, read.detail, read.rows)
		require.Equal(t, read.hits, operator.DBHits, query)
	}
	require.Equal(t, hits, plan.TotalDBHits, "the statement's hits are the populations it read")
	return result
}

// algorithmRowInteger reads one integer value of an algorithm's result column.
func algorithmRowInteger(t *testing.T, value interface{}) int64 {
	t.Helper()
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int64:
		return typed
	default:
		require.FailNowf(t, "unexpected integer value", "%T", value)
		return 0
	}
}

// algorithmComponentIDs returns the distinct values of a one-column partition.
func algorithmComponentIDs(t *testing.T, result *ExecuteResult) []int64 {
	t.Helper()
	seen := make(map[int64]struct{}, len(result.Rows))
	ids := make([]int64, 0, len(result.Rows))
	for _, row := range result.Rows {
		require.Len(t, row, 1)
		id := algorithmRowInteger(t, row[0])
		if _, listed := seen[id]; listed {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids
}

// TestProfileWholeGraphAlgorithmsReportTheNodesAndAdjacencyTheyRead is the
// per-algorithm control. PageRank, Louvain and label propagation read the
// adjacency of the nodes they rank; betweenness reads it once per source's
// breadth-first search, which reaches d through its :Y relationship; closeness
// and wcc read both directions of every node they reach.
func TestProfileWholeGraphAlgorithmsReportTheNodesAndAdjacencyTheyRead(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newAlgorithmReadGraph(t)
		nodes := gdsAccess{kind: profileKindAllNodesScan, detail: "", rows: 5, hits: 5}
		outgoing := func(population int64) gdsAccess {
			return gdsAccess{kind: profileKindExpand, detail: "outgoing", rows: population, hits: population}
		}
		incoming := func(population int64) gdsAccess {
			return gdsAccess{kind: profileKindExpand, detail: "incoming", rows: population, hits: population}
		}
		for _, tc := range []struct {
			procedure, column string
			reads             []gdsAccess
		}{
			// One outgoing request per ranked node: the four relationships.
			{procedure: "pageRank", column: "score", reads: []gdsAccess{nodes, outgoing(4)}},
			{procedure: "louvain", column: "community", reads: []gdsAccess{nodes, outgoing(4)}},
			// Five searches over {a, b, c, d}: a, b and c reach the cycle
			// through their own relationship, d reaches all four, and e reads
			// no relationship.
			{procedure: "betweenness", column: "score", reads: []gdsAccess{nodes, outgoing(13)}},
			// Five searches, each reading both directions of the node it
			// dequeues: four relationships in each direction per search over
			// the connected four, none from e.
			{procedure: "closeness", column: "score", reads: []gdsAccess{nodes, outgoing(16), incoming(16)}},
			// One pass, both directions of each of the five nodes.
			{procedure: "wcc", column: "componentId", reads: []gdsAccess{nodes, outgoing(4), incoming(4)}},
		} {
			t.Run(tc.procedure, func(t *testing.T) {
				requireAlgorithmReads(t, exec,
					"CALL apoc.algo."+tc.procedure+"() YIELD "+tc.column+" RETURN "+tc.column, tc.reads...)
			})
		}
	})
}

// TestProfileLabelPropagationReportsEveryRoundItReads: label propagation reads
// its nodes' adjacency in each propagation round, so the record holds the
// entries of every round the call ran - not the last round alone. Three nodes on
// a directed cycle settle in two rounds, each reading one entry per direction
// per node.
func TestProfileLabelPropagationReportsEveryRoundItReads(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t, `CREATE
			(a:A {name: 'a'}), (b:A {name: 'b'}), (c:A {name: 'c'}),
			(a)-[:X]->(b), (b)-[:X]->(c), (c)-[:X]->(a)`)
		result := requireAlgorithmReads(t, exec,
			"CALL apoc.algo.labelPropagation() YIELD community RETURN community",
			gdsAccess{kind: profileKindAllNodesScan, detail: "", rows: 3, hits: 3},
			gdsAccess{kind: profileKindExpand, detail: "outgoing", rows: 6, hits: 6},
			gdsAccess{kind: profileKindExpand, detail: "incoming", rows: 6, hits: 6},
		)
		require.Len(t, result.Rows, 3)
		require.Len(t, algorithmComponentIDs(t, result), 1, "the cycle is one community")
	})
}

// TestProfileWholeGraphAlgorithmNodePopulationFollowsItsInput: the call reports
// the population the input it was given actually read - a label's nodes, or the
// caller's own list, which the clause that produced it read - and never a second
// read of the nodes it was handed.
func TestProfileWholeGraphAlgorithmNodePopulationFollowsItsInput(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newAlgorithmReadGraph(t)
		// The three :A nodes are read by the label scan. Their outgoing adjacency
		// is the three :X relationships, and their incoming adjacency is those
		// three plus d's :Y relationship, which the read returns as a's incoming
		// one although d is outside the population.
		byLabel := []gdsAccess{
			{kind: profileKindNodeByLabelScan, detail: "A", rows: 3, hits: 3},
			{kind: profileKindExpand, detail: "outgoing", rows: 3, hits: 3},
			{kind: profileKindExpand, detail: "incoming", rows: 4, hits: 4},
		}
		result := requireAlgorithmReads(t, exec,
			"CALL apoc.algo.wcc('A') YIELD componentId RETURN componentId", byLabel...)
		require.Len(t, algorithmComponentIDs(t, result), 1, "the :X cycle is one component")

		// A LIST<NODE> is the caller's values. The MATCH read those three nodes
		// once; the call adds no node read of its own, so there is one node
		// population record and it is the scan's.
		byList := requireAlgorithmReads(t, exec,
			"MATCH (n:A) WITH n ORDER BY n.name WITH collect(n) AS nodes CALL apoc.algo.wcc(nodes) YIELD componentId RETURN componentId",
			byLabel...)
		require.Len(t, algorithmComponentIDs(t, byList), 1)
	})
}

// TestProfileWholeGraphAlgorithmReportsReadsBeforeItsTypeSelection: a type list
// narrows the adjacency after the read, so the record keeps every entry the read
// returned. "*" and an omitted list are the same every-type call, and an empty
// list reads the same population while keeping no relationship.
func TestProfileWholeGraphAlgorithmReportsReadsBeforeItsTypeSelection(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newAlgorithmReadGraph(t)
		population := []gdsAccess{
			{kind: profileKindAllNodesScan, detail: "", rows: 5, hits: 5},
			{kind: profileKindExpand, detail: "outgoing", rows: 4, hits: 4},
			{kind: profileKindExpand, detail: "incoming", rows: 4, hits: 4},
		}
		every := requireAlgorithmReads(t, exec,
			"CALL apoc.algo.wcc() YIELD componentId RETURN componentId", population...)
		require.Len(t, algorithmComponentIDs(t, every), 2, "the connected four and e")

		// "*" selects every type, so the call reads and keeps everything the
		// omitted list does.
		all := requireAlgorithmReads(t, exec,
			"CALL apoc.algo.wcc(null, ['*']) YIELD componentId RETURN componentId", population...)
		require.Equal(t, every.Rows, all.Rows, `"*" is the every-type call`)

		// One type keeps the :X cycle alone, yet d's :Y relationship was read
		// by the same request and is counted by it.
		typed := requireAlgorithmReads(t, exec,
			"CALL apoc.algo.wcc(null, ['X']) YIELD componentId RETURN componentId", population...)
		require.Len(t, algorithmComponentIDs(t, typed), 3, "the :X cycle, d and e")

		// An empty list keeps no relationship, so every node is its own
		// component; the reads the call performed are unchanged.
		empty := requireAlgorithmReads(t, exec,
			"CALL apoc.algo.wcc(null, []) YIELD componentId RETURN componentId", population...)
		require.Len(t, algorithmComponentIDs(t, empty), 5)
	})
}

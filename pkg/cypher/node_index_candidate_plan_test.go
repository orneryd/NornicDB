package cypher

// NOR-414 deferred node-index candidate planning: the end anchor's candidate
// set is prepared without reading a node, so the traversal seed choice can
// compare, count or drop it without materialising it.
//
// These controls use the existing public fixtures (newSeedFixture and its
// seedReadEngine, which counts every node the store hands a statement, and
// traversalSeedCostStatement) through the public Execute path on both parsers:
//
//   - unselected inline candidates (the pattern-property seek route) are
//     dropped unread while the same candidate reader still materialises them
//     for the row path,
//   - a selected end is read once,
//   - an empty index answer is reported by reading the plan, not inferred from
//     a count,
//   - a singleton end is still read while the start walks (boundEndNodeID).
//
// The remaining routes' controls are the existing ones the split moved behind
// the same owners: pending and staged writes through the plan
// (TestNodeSeedChoiceTransactionEstimate, which asserts the plan's IDs inside a
// transaction), counter errors and cancellation at the traversal seed choice
// (TestTraversalSeedCostPreservesCounterAndCancellationErrors), and each
// route's recognition and admission through the plan's resolver and reader
// (node_index_candidate_route_read_test.go).

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// The end anchor's candidates come from the pattern's own indexed property
// (nodeCandidateInline). The reported collection walk must not read them: the
// control proves the same reader still materialises all of them for the row
// path, so a small bound on the traversal read is evidence about the plan, not
// about a disabled index.
func TestTraversalEndPlanDropsUnselectedInlineCandidates(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const documents = 128
		fixture := newSeedFixture(t,
			"CREATE INDEX plan_candidate_status FOR (d:PDDocument) ON (d.status)",
			"CREATE (:PDCollection {archived: false})",
			"UNWIND range(0,127) AS slot CREATE (:PDDocument {id: 'doc-' + toString(slot), status: 'active'})",
			"MATCH (k:PDCollection) MATCH (d:PDDocument) WHERE d.id IN ['doc-0','doc-1'] CREATE (d)-[:MEMBER_OF]->(k)",
		)
		all, reads := fixture.query(t, "MATCH (d:PDDocument {status: 'active'}) RETURN d.id", nil)
		require.Len(t, all, documents)
		require.GreaterOrEqual(t, reads, int64(documents),
			"the candidate reader must materialise all %d inline candidates for the row path", documents)
		rows, reads := fixture.query(t,
			"MATCH (k:PDCollection)<-[:MEMBER_OF]-(d:PDDocument {status: 'active'}) RETURN d.id, k.archived", nil)
		require.Equal(t, seedRowKeys([][]interface{}{{"doc-0", false}, {"doc-1", false}}), rows)
		require.LessOrEqual(t, reads, int64(8),
			"the chosen collection walk must not read the %d unselected inline candidates: %d", documents, reads)
	})
}

// The opposite orientation: 200 unindexed collections against 12 indexed
// documents. The end plan is the chosen walk, so its 12 candidates are read
// once and seed it; reading them a second time would show up here.
func TestTraversalEndPlanReadsSelectedEndOnce(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const documents = 12
		fixture := newSeedFixture(t,
			"CREATE CONSTRAINT plan_document_id_unique FOR (d:PDDocument) REQUIRE d.id IS UNIQUE",
			"UNWIND range(0,199) AS slot CREATE (:PDCollection {id: 'col-' + toString(slot), archived: false})",
			"UNWIND range(0,11) AS slot CREATE (:PDDocument {id: 'doc-' + toString(slot)})",
			"MATCH (k:PDCollection {id: 'col-0'}), (d:PDDocument) CREATE (d)-[:MEMBER_OF]->(k)",
		)
		ids := make([]interface{}, documents)
		for index := range ids {
			ids[index] = fmt.Sprintf("doc-%d", index)
		}
		rows, reads := fixture.query(t, traversalSeedCostStatement, map[string]interface{}{"ids": ids})
		expected := make([][]interface{}, 0, documents)
		for index := 0; index < documents; index++ {
			expected = append(expected, []interface{}{fmt.Sprintf("doc-%d", index), false})
		}
		require.Equal(t, seedRowKeys(expected), rows)
		require.LessOrEqual(t, reads, int64(2*documents+8),
			"the selected end's %d candidates are read once for the walk: %d", documents, reads)
	})
}

// An index that lists no end candidate is the route's own answer. The plan
// holds no node, so reading it reports the empty result without reading any
// node at all: the walk never starts.
func TestTraversalEndPlanEmptyCandidatesAnswerNoRows(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := newSeedFixture(t,
			"CREATE CONSTRAINT plan_empty_document_id_unique FOR (d:PDDocument) REQUIRE d.id IS UNIQUE",
			"CREATE (:PDCollection {archived: false})",
			"CREATE (:PDDocument {id: 'doc-0'})",
			"MATCH (k:PDCollection) MATCH (d:PDDocument) CREATE (d)-[:MEMBER_OF]->(k)",
		)
		rows, reads := fixture.query(t, traversalSeedCostStatement,
			map[string]interface{}{"ids": []interface{}{"missing-0", "missing-1"}})
		require.Empty(t, rows, "an end index answer that lists nothing matches no path")
		require.Zero(t, reads, "an empty candidate plan is read without reading a node: %d", reads)
	})
}

// A singleton end candidate keeps its node bound while the start side walks
// (boundEndNodeID), so the plan is the one route that is still read here: the
// collection's own walk plus the one end node.
func TestTraversalEndPlanBindsSingletonEndWhileStartWalks(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := newSeedFixture(t,
			"CREATE CONSTRAINT plan_singleton_document_id_unique FOR (d:PDDocument) REQUIRE d.id IS UNIQUE",
			"CREATE (:PDCollection {archived: false})",
			"CREATE (:PDDocument {id: 'doc-0'})",
			"MATCH (k:PDCollection) MATCH (d:PDDocument) CREATE (d)-[:MEMBER_OF]->(k)",
		)
		rows, reads := fixture.query(t, traversalSeedCostStatement,
			map[string]interface{}{"ids": []interface{}{"doc-0"}})
		require.Equal(t, seedRowKeys([][]interface{}{{"doc-0", false}}), rows)
		require.LessOrEqual(t, reads, int64(6),
			"the singleton end is read once for its binding, and the collection walk reads its own path: %d", reads)
	})
}

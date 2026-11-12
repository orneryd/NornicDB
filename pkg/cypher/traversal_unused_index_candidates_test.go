package cypher

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTraversalDoesNotHydrateUnselectedIndexCandidates(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const candidates = 128
		fixture := newSeedFixture(t,
			"CREATE CONSTRAINT candidate_doc_id FOR (d:PDDocument) REQUIRE d.id IS UNIQUE",
			"CREATE (:PDCollection {archived: false})",
			"UNWIND range(0,127) AS slot CREATE (:PDDocument {id: 'doc-' + toString(slot)})",
			"MATCH (k:PDCollection) MATCH (d:PDDocument) WHERE d.id IN ['doc-0','doc-1'] CREATE (d)-[:MEMBER_OF]->(k)",
		)
		ids := make([]interface{}, candidates)
		for i := range ids {
			ids[i] = fmt.Sprintf("doc-%d", i)
		}
		params := map[string]interface{}{"ids": ids}
		// This control exercises the same candidate reader and proves that the
		// native-capability-preserving observer sees its actual materialization.
		all, reads := fixture.query(t, "MATCH (d:PDDocument) WHERE d.id IN $ids RETURN d.id", params)
		require.Len(t, all, candidates)
		require.GreaterOrEqual(t, reads, int64(candidates))
		rows, reads := fixture.query(t, traversalSeedCostStatement, params)
		require.Equal(t, seedRowKeys([][]interface{}{{"doc-0", false}, {"doc-1", false}}), rows)
		require.LessOrEqual(t, reads, int64(8),
			"the chosen collection walk must not hydrate all %d unselected index candidates", candidates)
	})
}

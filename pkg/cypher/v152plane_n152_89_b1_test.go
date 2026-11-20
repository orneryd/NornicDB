package cypher

// Single-EXISTS control for the NOR-403 reproduction.
//
// The recorded statement carries the complete multi-EXISTS eligibility
// predicate. This control proves the main test is not satisfied by a weaker
// predicate: reducing the filter to the document-membership EXISTS alone keeps
// every summary row whose document is visible, including rows the recorded
// decision table marks ineligible (not-current, archived collection owner,
// mismatched owner kind, source mismatch). If a future change collapsed the
// predicate to one EXISTS, the reproduction would disagree with its recorded
// table instead of silently passing.

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/memorypressure"
	"github.com/stretchr/testify/require"
)

const nor403DocumentMembershipOnlyStatement = `CALL db.index.fulltext.queryNodes('` + nor403FulltextIndex + `', $query) YIELD node, score
WHERE (NOT EXISTS { MATCH (:PDDocument {id: node.document_id})-[:MEMBER_OF]->(:PDCollection) }
        OR EXISTS { MATCH (:PDDocument {id: node.document_id})-[:MEMBER_OF]->(open_in:PDCollection) WHERE NOT coalesce(open_in.archived, false) })
RETURN node, score ORDER BY score DESC, node.id LIMIT $limit`

// TestNOR403FulltextExistsSingleExistsControl shows that the recorded statement
// is the full predicate: one EXISTS keeps rows the recorded decisions reject.
func TestNOR403FulltextExistsSingleExistsControl(t *testing.T) {
	memorypressure.WithoutRuntimePressure(t)
	forEachParser(t, func(t *testing.T) {
		fixture := newNOR403Fixture(t)
		exec := fixture.newExecutor(QueryCachePolicy{MaxEntries: 0, TTL: 0})
		population := int64(len(fixture.rows))

		raw := nor403RunStatement(t, exec, nor403RawStatement, nor403QueryTerm, population)
		require.Len(t, raw, len(fixture.rows))
		full := nor403RunStatement(t, exec, nor403EligibilityStatement, nor403QueryTerm, population)
		membershipOnly := nor403RunStatement(t, exec, nor403DocumentMembershipOnlyStatement, nor403QueryTerm, population)

		// The membership-only control keeps exactly the rows whose document is
		// visible under the same corpus.
		expectedControl := nor403RankedRows(raw, len(fixture.rows), func(id string) bool {
			return fixture.truth.documentVisible(fixture.byID[id].documentID)
		})
		require.Equal(t, expectedControl, membershipOnly,
			"the membership EXISTS alone keeps every row whose document is visible")

		require.Greater(t, len(membershipOnly), len(full),
			"the complete predicate removes summary rows a single EXISTS keeps")
		require.NotEqual(t, nor403ObservedIDs(full), nor403ObservedIDs(membershipOnly))

		controlIDs := nor403ObservedIDs(membershipOnly)
		fullIDs := nor403ObservedIDs(full)
		for _, id := range []string{"m03", "m08", "m09", "m12"} {
			require.Contains(t, controlIDs, id,
				"the single-EXISTS control keeps %s", id)
			require.NotContains(t, fullIDs, id,
				"the recorded predicate rejects %s (%s)", id, fixture.byID[id].reason)
		}
	})
}

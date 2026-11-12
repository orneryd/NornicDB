package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFulltextUnrankedPageStopsAtRequestedRows(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := newSeedFixture(t,
			"UNWIND range(0,63) AS i CREATE (:PageSource {id:i,title:'page source'})",
			"CREATE FULLTEXT INDEX page_source_title FOR (n:PageSource) ON EACH [n.title]",
		)
		const query = "CALL db.index.fulltext.queryNodes('page_source_title', $term, $options) YIELD node, score RETURN node.id AS id, score"
		forEachTraversalTransactionMode(t, fixture.exec, func(t *testing.T) {
			explicit := fixture.exec.txContext != nil && fixture.exec.txContext.active
			for _, term := range []string{"*", "*:*", "title:*"} {
				fixture.engine.reads.Store(0)
				all, err := fixture.exec.Execute(t.Context(), query, map[string]interface{}{"term": term, "options": map[string]interface{}{}})
				require.NoError(t, err)
				require.Len(t, all.Rows, 64)
				// Explicit reads use BadgerTransaction directly and bypass the live
				// engine meter. Exact rows/order cover both; acquisition counts
				// are asserted only for the automatic route this probe observes.
				if !explicit {
					require.GreaterOrEqual(t, fixture.engine.reads.Load(), int64(64), "positive control observes actual acquisition")
				}
				fixture.engine.reads.Store(0)
				page, err := fixture.exec.Execute(t.Context(), query, map[string]interface{}{"term": term, "options": map[string]interface{}{"skip": int64(2), "limit": int64(5)}})
				require.NoError(t, err)
				require.Equal(t, all.Rows[2:7], page.Rows, "preserve release order and exact row/score values")
				if !explicit {
					require.LessOrEqual(t, fixture.engine.reads.Load(), int64(8), "do not read the remaining corpus after the page is fulfilled")
				}
				for _, control := range []struct {
					skip, limit int64
					want        [][]interface{}
				}{
					{0, 0, nil}, {64, 5, nil}, {62, 5, all.Rows[62:]},
				} {
					got, err := fixture.exec.Execute(t.Context(), query, map[string]interface{}{"term": term, "options": map[string]interface{}{"skip": control.skip, "limit": control.limit}})
					require.NoError(t, err)
					if len(control.want) == 0 {
						require.Empty(t, got.Rows)
					} else {
						require.Equal(t, control.want, got.Rows)
					}
				}
			}
			missing, err := fixture.exec.Execute(t.Context(), query, map[string]interface{}{"term": "missing:*", "options": map[string]interface{}{"limit": int64(5)}})
			require.NoError(t, err)
			require.Empty(t, missing.Rows)
		})
	})
}

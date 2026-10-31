// Author: #1683 lane (Claude), 2026-10-30 (NOR-148).
package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A large write refreshes a maintained full-text index by the documents it can
// affect, through a namespaced view on the public Cypher path, without
// rereading the store:
//   - indexed_documents: one explicit transaction rewrites the 2,000 Source
//     documents of an 8,000-node store, each title becoming about 4 KB. The
//     in-memory engine keeps the bodies inline, so the commit needs several
//     Badger batches (TestReaderPublicationLargeInlineUpdateIsOneStep measures
//     that shape).
//   - unrelated_entities: one explicit transaction rewrites all 4,000 nodes of
//     a store whose index holds 1,000 documents. The 3,000 other nodes can
//     never be documents, so they must not count against the index's change
//     bound. A later label removal still retires its document.
func TestFulltextMaintainedPublicLargeTransactionRefresh(t *testing.T) {
	query := "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN node.id, score ORDER BY score DESC, node.id"
	for _, shape := range []struct {
		name    string
		nodes   int
		match   string
		revised int
	}{
		{"indexed_documents", 8000, "MATCH (n:Source)", 1999},
		{"unrelated_entities", 4000, "MATCH (n)", 999},
	} {
		t.Run(shape.name, func(t *testing.T) {
			exec, store := fulltextMaintainedFixtureOn(t, storage.NewMemoryEngine().BadgerEngine, shape.nodes)
			ctx := context.Background()
			warm, err := exec.Execute(ctx, query, map[string]any{"query": "hotel"})
			require.NoError(t, err)
			require.NotEmpty(t, warm.Rows)
			t.Logf("corpus scans after warming: %d", store.scans.Load())

			// Existing labels and property names only; every title grows to about
			// 4 KB, so the bodies, and the commit, are several times larger.
			for _, statement := range []string{
				"BEGIN",
				shape.match + " SET n.title = CASE n.id WHEN 'n000040' THEN 'newunique' ELSE n.text + ' ' + n.text + ' revised' END",
				"COMMIT",
			} {
				_, err := exec.Execute(ctx, statement, nil)
				require.NoError(t, err, statement)
			}
			// The statement itself may read the store; only the ranked queries
			// after the commit are measured.
			scans := store.scans.Load()
			t.Logf("corpus scans after the transaction: %d", scans)
			changed, err := exec.Execute(ctx, query, map[string]any{"query": "newunique"})
			require.NoError(t, err)
			require.Len(t, changed.Rows, 1)
			require.Equal(t, "n000040", changed.Rows[0][0])
			t.Logf("corpus scans after the first ranked query: %d", store.scans.Load())
			require.Equal(t, scans, store.scans.Load(), "one large transaction must refresh by ID without rereading the whole store")
			revised, err := exec.Execute(ctx, query, map[string]any{"query": "revised"})
			require.NoError(t, err)
			require.Len(t, revised.Rows, shape.revised, "every other Source document carries the revised title")
			require.Equal(t, scans, store.scans.Load(), "a repeated ranked query must reuse the refreshed documents")

			// A document whose entity leaves the index's label is retired by ID.
			hotel, err := exec.Execute(ctx, query, map[string]any{"query": "hotel"})
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "MATCH (n {id: 'n000080'}) REMOVE n:Source SET n:Unrelated", nil)
			require.NoError(t, err)
			retired, err := exec.Execute(ctx, query, map[string]any{"query": "hotel"})
			require.NoError(t, err)
			require.Len(t, retired.Rows, len(hotel.Rows)-1, "the relabelled node's document must be retired")
			for _, row := range retired.Rows {
				require.NotEqual(t, "n000080", row[0])
			}
			require.Equal(t, scans, store.scans.Load(), "retiring one document must not reread the whole store")
		})
	}
}

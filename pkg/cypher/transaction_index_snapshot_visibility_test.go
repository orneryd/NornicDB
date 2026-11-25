package cypher

// N152-279 (report C65): an older explicit transaction's node-index reads drop
// rows a peer commits away after the transaction began.
//
// The transactional wrapper answers a transaction's point and label reads from
// its pinned snapshot, but its node-index routes read the live committed index
// and merge only the transaction's own pending writes:
//
//   - propertyIndexLookup / compositeIndexLookup (transaction_storage_wrapper.go)
//     read SchemaManager postings of committed state, then merge the
//     transaction's own pending and deleted nodes
//     (BadgerTransaction.MergePendingPropertyMatches / MergePendingCompositeMatches).
//   - resolvePropertyIndexRangeCandidates (node_index_candidate_plan.go) reads
//     the committed ordered index through orderedPropertyIndexSchema and merges
//     nothing; it only declines the index when the transaction itself staged a
//     node write.
//
// None of them accounts for a commit another session landed after this
// transaction pinned its snapshot. A peer that deletes a node, rewrites an
// indexed value or moves the node off the pattern label retires its index
// entry, so the seek lists no candidate for a row the snapshot still holds -
// and a scalar value's empty posting is an authoritative miss, so the route
// answers "no rows" instead of falling back to the transaction's scan. The
// unindexed controls below read the same rows through routes no index can
// serve, which is what establishes that the snapshot, not the graph, is the
// answer the transaction owes.
//
// The rows asserted after the peer's commits are exactly the rows the same
// transaction read before them: one transaction's reads answer from its own
// snapshot. The label count control is the neighbouring owner that already
// behaves this way - NodeCountByLabel asks storage whether the committed
// counters still describe the rows the transaction reads
// (BadgerTransaction.committedCountersDescribeRowsLocked, via
// PendingNodeLabelCountDelta) and answers from the snapshot's rows once a peer
// commit moved past the transaction's read version.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOlderTransactionIndexReadsKeepPinnedSnapshot(t *testing.T) {
	// The peer session deletes k1, rewrites k2's indexed rank and moves k3 off
	// the pattern label after the transaction below has begun.
	const seed = "UNWIND range(0, 9) AS i CREATE (:Doc {key:'k'+toString(i), rank:i, category:'x', body:'b'+toString(i)})"
	reads := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{"equality on a node the peer deleted", "MATCH (n:Doc {key:'k1'}) RETURN n.body", [][]interface{}{{"b1"}}},
		{"IN list on a node the peer deleted", "MATCH (n:Doc) WHERE n.key IN ['k1'] RETURN n.body", [][]interface{}{{"b1"}}},
		{"equality on a property the peer rewrote", "MATCH (n:Doc {rank:2}) RETURN n.body", [][]interface{}{{"b2"}}},
		{"composite equality on a property the peer rewrote", "MATCH (n:Doc {category:'x', rank:2}) RETURN n.body", [][]interface{}{{"b2"}}},
		{"range on a property the peer rewrote", "MATCH (n:Doc) WHERE n.rank >= 2 AND n.rank < 3 RETURN n.body", [][]interface{}{{"b2"}}},
		{"equality on a node the peer relabelled", "MATCH (n:Doc {key:'k3'}) RETURN n.body", [][]interface{}{{"b3"}}},
		{"ordered window after peer writes", "MATCH (n:Doc) RETURN n.body ORDER BY n.rank LIMIT 3", [][]interface{}{{"b0"}, {"b1"}, {"b2"}}},
		// Controls: no index can answer these, so they read the pattern label
		// through the transaction's snapshot.
		{"control: unindexable equality", "MATCH (n:Doc) WHERE toString(n.key) = 'k1' RETURN n.body", [][]interface{}{{"b1"}}},
		{"control: unindexable arithmetic", "MATCH (n:Doc) WHERE n.rank + 0 = 2 RETURN n.body", [][]interface{}{{"b2"}}},
		{"control: label count", "MATCH (n:Doc) RETURN count(n)", [][]interface{}{{int64(10)}}},
	}
	for _, schema := range []struct {
		name string
		ddl  []string
	}{
		// The control case: with no index, every route above reads the
		// transaction's snapshot and the expected rows are the ones the
		// transaction reads, which keeps the expectations themselves honest.
		{"no index", nil},
		{
			name: "property and composite indexes",
			ddl: []string{
				"CREATE INDEX n152_key FOR (n:Doc) ON (n.key)",
				"CREATE INDEX n152_rank FOR (n:Doc) ON (n.rank)",
				"CREATE INDEX n152_category_rank FOR (n:Doc) ON (n.category, n.rank)",
			},
		},
		{"unique constraint", []string{"CREATE CONSTRAINT n152_key FOR (n:Doc) REQUIRE n.key IS UNIQUE"}},
	} {
		t.Run(schema.name, func(t *testing.T) {
			ctx := context.Background()
			base, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			store := storage.NewNamespacedEngine(base, "n152")
			peer := NewStorageExecutor(store)
			executeClauseQueries(t, peer, ctx, schema.ddl...)
			executeClauseQueries(t, peer, ctx, seed)

			older := NewStorageExecutor(store)
			_, err = older.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = older.Execute(ctx, "ROLLBACK", nil) })

			// The transaction's own answers before the peer writes: a route
			// that cannot read the row here is a fixture problem, not the
			// snapshot question this test asks.
			for _, read := range reads {
				result, err := older.Execute(ctx, read.query, nil)
				require.NoError(t, err, "before the peer's writes: %s", read.query)
				require.Equal(t, read.want, result.Rows, "before the peer's writes: %s", read.query)
			}

			executeClauseQueries(t, peer, ctx,
				"MATCH (n:Doc {key:'k1'}) DETACH DELETE n",
				"MATCH (n:Doc {key:'k2'}) SET n.rank = 77",
				"MATCH (n:Doc {key:'k3'}) REMOVE n:Doc SET n:Gone",
			)

			for _, read := range reads {
				result, err := older.Execute(ctx, read.query, nil)
				require.NoError(t, err, read.query)
				assert.Equal(t, read.want, result.Rows,
					"%s: the transaction's pinned snapshot still holds this row, so its reads must keep it: %s",
					read.name, read.query)
			}
			// MERGE's direct identity lookup must share MATCH's pinned read
			// authority. A missed posting would create a second row instead of
			// matching the row already visible in this transaction.
			for _, merge := range []struct {
				query string
				want  string
			}{
				{"MERGE (n:Doc {key:'k1'}) RETURN n.body", "b1"},
				{"MERGE (n:Doc {category:'x', rank:2}) RETURN n.body", "b2"},
			} {
				result, err := older.Execute(ctx, merge.query, nil)
				require.NoError(t, err, merge.query)
				assert.Equal(t, [][]interface{}{{merge.want}}, result.Rows, merge.query)
			}
		})
	}
}

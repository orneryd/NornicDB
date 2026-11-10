package cypher

import (
	"context"
	"errors"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestCompetingUniqueClaimsFailAtCommit: the transaction that loses a UNIQUE
// claim at commit fails with Neo4j's ConstraintValidationFailed, and nothing
// of it survives — no orphan node, no duplicated claim, no half-applied
// property.
//
// Both transactions of a case write entities no other transaction wrote: two
// distinct existing nodes whose key each one sets to the same claimed value,
// or the two fresh identities an unlabeled MERGE creates. No entity write
// lock orders them, so both statements succeed and only the constraint
// decides the commits. A writer of the entity another transaction writes
// instead waits at its statement and commits on top of that commit
// (TestNOR213WritersOfOneEntityWaitOnEveryWriteForm); a UNIQUE conflict is
// what remains of the constraint's own claim on one value.
func TestCompetingUniqueClaimsFailAtCommit(t *testing.T) {
	for _, tc := range []struct {
		name string
		// setup creates the nodes the case starts from.
		setup []string
		// query is the statement both transactions run; ids names the node
		// each of them matches, nil when the statement creates its own.
		query string
		ids   []string
		// want is every Doc node after the losing commit, ordered by id.
		want [][]interface{}
		// count is every node after the losing commit.
		count int64
		// retry is the id a fresh manual transaction MERGEs after the
		// failure, empty when the case has no retry.
		retry string
	}{
		{
			// The key a node sets to a value another node of the same label
			// already committed is a UNIQUE conflict, whatever the nodes
			// write besides it.
			name: "distinct_nodes_claim_one_key_value",
			setup: []string{
				"CREATE (:Doc {id: 'original', summary_input: 'before'})",
				"CREATE (:Doc {id: 'second', summary_input: 'before'})",
			},
			query: "MATCH (n:Doc {id: $id}) SET n.id = 'claimed', n.summary_input = $fingerprint",
			ids:   []string{"original", "second"},
			want:  [][]interface{}{{"claimed", "peer"}, {"original", "before"}},
			count: 2,
		},
		{
			// This MERGE has no constrained label at lookup. Adding Doc afterward
			// introduces an ordinary unique claim, not a retryable MERGE-key race.
			// Pinned Neo4j 5.26.30 blocks the second SET until the first commits,
			// then reports ConstraintValidationFailed at statement consumption.
			// Native deferred validation reports the same status at COMMIT; this
			// test does not claim equal error phases. A manual fresh transaction
			// can still MERGE the winner without creating another node.
			name:  "unlabeled_merge_creates_competing_identities",
			setup: []string{"CREATE (:Doc {id: 'original', summary_input: 'before'})"},
			query: "MERGE (n {id: 'claimed'}) SET n:Doc, n.summary_input = $fingerprint",
			want:  [][]interface{}{{"claimed", "peer"}, {"original", "before"}},
			count: 2,
			retry: "claimed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			store := storage.NewNamespacedEngine(base, "test")
			ctx := context.Background()
			setup := NewStorageExecutor(store)
			for _, query := range append([]string{"CREATE CONSTRAINT FOR (n:Doc) REQUIRE n.id IS UNIQUE"}, tc.setup...) {
				_, err = setup.Execute(ctx, query, nil)
				require.NoError(t, err)
			}
			loser, peer := NewStorageExecutor(store), NewStorageExecutor(store)
			for _, exec := range []*StorageExecutor{loser, peer} {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
			}
			for i, exec := range []*StorageExecutor{loser, peer} {
				params := map[string]interface{}{"fingerprint": []string{"loser", "peer"}[i]}
				if tc.ids != nil {
					params["id"] = tc.ids[i]
				}
				_, err = exec.Execute(ctx, tc.query, params)
				require.NoError(t, err)
			}
			_, err = peer.Execute(ctx, "COMMIT", nil)
			require.NoError(t, err)
			_, err = loser.Execute(ctx, "COMMIT", nil)
			require.Error(t, err)
			var violation *storage.ConstraintViolationError
			require.True(t, errors.As(err, &violation), "commit error: %v", err)
			require.Equal(t, storage.ConstraintUnique, violation.Type)
			code, _ := nornicerrors.Neo4jStatus(nornicerrors.TransactionControlFailure(nornicerrors.TerminalCommit, err))
			require.Equal(t, nornicerrors.ConstraintValidationFailed, code, "commit error: %v", err)

			// The failed commit must leave only the peer's image and no orphan
			// loser node, including the unlabeled MERGE's fresh identity.
			result, err := setup.Execute(ctx, "MATCH (n:Doc) RETURN n.id, n.summary_input ORDER BY n.id", nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, result.Rows)
			result, err = setup.Execute(ctx, "MATCH (n) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.count}}, result.Rows)
			if tc.retry == "" {
				return
			}

			// A fresh manual retry is valid even when a UNIQUE error is not
			// automatically driver-retryable. MERGE must find the winner.
			_, err = loser.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			_, err = loser.Execute(ctx, tc.query, map[string]interface{}{"fingerprint": "retry"})
			require.NoError(t, err)
			_, err = loser.Execute(ctx, "COMMIT", nil)
			require.NoError(t, err)
			result, err = setup.Execute(ctx, "MATCH (n:Doc {id: $id}) RETURN n.summary_input", map[string]interface{}{"id": tc.retry})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"retry"}}, result.Rows)
			result, err = setup.Execute(ctx, "MATCH (n) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.count}}, result.Rows)
		})
	}
}

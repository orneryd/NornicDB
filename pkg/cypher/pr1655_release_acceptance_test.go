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

func TestPR1655ReleaseUniqueClaimConflict(t *testing.T) {
	for _, tc := range []struct {
		name       string
		query      string
		wantUnique bool
	}{
		{"non_key_set", "UNWIND $items AS item MATCH (n:Doc {id: item.id}) SET n.summary_input = item.fingerprint", false},
		{"same_key_set", "MATCH (n:Doc {id: 'original'}) SET n.id = 'original', n.summary_input = $fingerprint", false},
		{"changed_key", "MATCH (n:Doc {id: 'original'}) SET n.id = 'claimed', n.summary_input = $fingerprint", true},
		// This MERGE has no constrained label at lookup. Adding Doc afterward
		// introduces an ordinary unique claim, not a retryable MERGE-key race.
		// Pinned Neo4j 5.26.30 blocks the second SET until the first commits,
		// then reports ConstraintValidationFailed at statement consumption.
		// Native deferred validation reports the same status at COMMIT; this
		// test does not claim equal error phases. A manual fresh transaction
		// can still MERGE the winner without creating another node.
		{"merge_create", "MERGE (n {id: 'claimed'}) SET n:Doc, n.summary_input = $fingerprint", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			store := storage.NewNamespacedEngine(base, "test")
			ctx := context.Background()
			setup := NewStorageExecutor(store)
			for _, query := range []string{
				"CREATE CONSTRAINT FOR (n:Doc) REQUIRE n.id IS UNIQUE",
				"CREATE (:Doc {id: 'original', summary_input: 'before'})",
			} {
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
				fingerprint := []string{"loser", "peer"}[i]
				_, err = exec.Execute(ctx, tc.query, map[string]interface{}{
					"fingerprint": fingerprint,
					"items":       []interface{}{map[string]interface{}{"id": "original", "fingerprint": fingerprint}},
				})
				require.NoError(t, err)
			}
			_, err = peer.Execute(ctx, "COMMIT", nil)
			require.NoError(t, err)
			_, err = loser.Execute(ctx, "COMMIT", nil)
			require.Error(t, err)
			var violation *storage.ConstraintViolationError
			require.Equal(t, tc.wantUnique, errors.As(err, &violation), "commit error: %v", err)
			if tc.wantUnique {
				require.Equal(t, storage.ConstraintUnique, violation.Type)
				code, _ := nornicerrors.Neo4jStatus(nornicerrors.TransactionControlFailure(nornicerrors.TerminalCommit, err))
				require.Equal(t, nornicerrors.ConstraintValidationFailed, code)
			} else {
				require.ErrorIs(t, err, storage.ErrConflict)
				require.Contains(t, err.Error(), "changed after transaction start")
				require.NotContains(t, err.Error(), "constraint violation")
				code, _ := nornicerrors.Neo4jStatus(nornicerrors.TransactionControlFailure(nornicerrors.TerminalCommit, err))
				require.Equal(t, nornicerrors.TransientOutdated, code)
			}

			// The failed commit must leave only the peer's image and no orphan
			// loser node, including the unlabeled MERGE's fresh identity.
			result, err := setup.Execute(ctx, "MATCH (n:Doc) RETURN n.id, n.summary_input ORDER BY n.id", nil)
			require.NoError(t, err)
			want := [][]interface{}{{"original", "peer"}}
			count := int64(1)
			if tc.name == "changed_key" {
				want = [][]interface{}{{"claimed", "peer"}}
			} else if tc.name == "merge_create" {
				want = [][]interface{}{{"claimed", "peer"}, {"original", "before"}}
				count = 2
			}
			require.Equal(t, want, result.Rows)
			result, err = setup.Execute(ctx, "MATCH (n) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{count}}, result.Rows)
			if tc.name == "changed_key" {
				return
			}

			// A fresh manual retry is valid even when a UNIQUE error is not
			// automatically driver-retryable. MERGE must find the winner.
			_, err = loser.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			_, err = loser.Execute(ctx, tc.query, map[string]interface{}{
				"fingerprint": "retry",
				"items":       []interface{}{map[string]interface{}{"id": "original", "fingerprint": "retry"}},
			})
			require.NoError(t, err)
			_, err = loser.Execute(ctx, "COMMIT", nil)
			require.NoError(t, err)
			id := "original"
			if tc.name == "merge_create" {
				id = "claimed"
			}
			result, err = setup.Execute(ctx, "MATCH (n:Doc {id: $id}) RETURN n.summary_input", map[string]interface{}{"id": id})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"retry"}}, result.Rows)
			result, err = setup.Execute(ctx, "MATCH (n) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{count}}, result.Rows)
		})
	}
}

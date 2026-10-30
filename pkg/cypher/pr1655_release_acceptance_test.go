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
		// Applying the constrained label after MERGE lets both explicit
		// transactions stage a new key before either commits. Labelled MERGE
		// admission can otherwise serialize this race before commit.
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
				code, _ := nornicerrors.Neo4jCommitStatus(err)
				require.Equal(t, nornicerrors.ConstraintValidationFailed, code)
			} else {
				require.ErrorIs(t, err, storage.ErrConflict)
				require.Contains(t, err.Error(), "changed after transaction start")
				require.NotContains(t, err.Error(), "constraint violation")
				code, _ := nornicerrors.Neo4jCommitStatus(err)
				require.Equal(t, nornicerrors.TransientOutdated, code)
				_, err = loser.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				_, err = loser.Execute(ctx, tc.query, map[string]interface{}{
					"fingerprint": "retry",
					"items":       []interface{}{map[string]interface{}{"id": "original", "fingerprint": "retry"}},
				})
				require.NoError(t, err)
				_, err = loser.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}

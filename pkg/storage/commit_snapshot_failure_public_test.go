package storage_test

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const snapshotFailureQuery = "MATCH (n) WHERE id(n) = 'doc' SET n.summary_input = 'changed' RETURN n.id"
const snapshotFailureStatus = "Neo.DatabaseError.Transaction.TransactionCommitFailed"

func TestSnapshotCommitFailureExecute(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		name := "implicit"
		if explicit {
			name = "explicit"
		}
		t.Run(name, func(t *testing.T) {
			base, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			store := storage.NewNamespacedEngine(base, "test")
			exec := cypher.NewStorageExecutor(store)
			ctx := context.Background()
			_, err = exec.Execute(ctx, "CREATE CONSTRAINT FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil)
			require.NoError(t, err)
			_, err = store.CreateNode(&storage.Node{ID: "doc", Labels: []string{"Doc"}, Properties: map[string]any{"id": "original"}})
			require.NoError(t, err)
			cause := base.CommitSnapshotFailureFixture(t, "test:doc")
			if explicit {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			result, err := exec.Execute(ctx, snapshotFailureQuery, nil)
			if explicit {
				require.NoError(t, err, "the statement must reach commit with a readable historical node")
				require.Equal(t, [][]interface{}{{"original"}}, result.Rows)
				_, err = exec.Execute(ctx, "COMMIT", nil)
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), "failed to read transaction snapshot")
			require.Contains(t, err.Error(), cause)
			var commitFailure *storage.CommitFailureError
			require.ErrorAs(t, err, &commitFailure)
			leaf := error(commitFailure)
			for errors.Unwrap(leaf) != nil {
				leaf = errors.Unwrap(leaf)
			}
			require.Equal(t, cause, leaf.Error(), "the original decoder error must remain in the chain")
			require.NotErrorIs(t, err, storage.ErrConflict)
			var violation *storage.ConstraintViolationError
			require.False(t, errors.As(err, &violation))
			_, transient := nornicerrors.MapTransientTransactionError(err)
			require.False(t, transient)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, snapshotFailureStatus, code)
			commitCode, _ := nornicerrors.Neo4jCommitStatus(err)
			require.Equal(t, snapshotFailureStatus, commitCode)
		})
	}
}

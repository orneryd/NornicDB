package storage

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConstraintKeyPreparationCancellationDoesNotAdmitPartialKeys(t *testing.T) {
	schema := NewSchemaManager()
	c := Constraint{Name: "tuple", Type: ConstraintUnique, Label: "P", Properties: []string{"a", "b"}}
	require.NoError(t, schema.AddConstraint(c))
	node := &Node{ID: "test:p", Labels: []string{"P"}, Properties: map[string]interface{}{"a": int64(1), "b": int64(2)}}
	keys, err := schema.appendConstraintValueKeys(newIndexPollCancelContext(t, 4), nil, []*Node{node}, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, keys)
	require.Empty(t, schema.uniqueConstraintCommitLocks)
	healthy, err := schema.appendConstraintValueKeys(t.Context(), nil, []*Node{node}, nil)
	require.NoError(t, err)
	require.Len(t, healthy, 1)
	merge, err := schema.uniqueMergeKeys(t.Context(), node.Labels, node.Properties)
	require.NoError(t, err)
	require.Len(t, merge, 1)
	require.Equal(t, healthy[0], merge[0].lock)
}

func TestConstraintCreationKeyCancellationDoesNotPublishBucket(t *testing.T) {
	seen := make(constraintCreationValues)
	// A temporal value uses add's canonical NewCompositeKey fallback bucket.
	values := []interface{}{time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	_, duplicate, err := seen.add(newIndexPollCancelContext(t, 2), "first", values)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, duplicate)
	require.Empty(t, seen)
	_, duplicate, err = seen.add(t.Context(), "first", values)
	require.NoError(t, err)
	require.False(t, duplicate)
	prior, duplicate, err := seen.add(t.Context(), "second", values)
	require.NoError(t, err)
	require.True(t, duplicate)
	require.Equal(t, "first", prior)
}

// The public preparation API retains its original native authority while an
// additional durable-owner retention attempt remains abortable. A canceled
// value-key projection must release the extra entity-key holds it just took.
func TestPreparedConstraintKeyCancellationReleasesAdditionalAuthority(t *testing.T) {
	engine, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{Name: "prepared_tuple", Type: ConstraintUnique, Label: "PreparedTuple", Properties: []string{"a", "b"}}))
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(t.Context(), &Node{ID: "test:prepared_tuple", Labels: []string{"PreparedTuple"}, Properties: map[string]interface{}{"a": int64(1), "b": int64(2)}})
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: "constraint-key-cancellation"})
	require.NoError(t, err)
	t.Cleanup(prepared.Abort)
	snapshot := func() map[uniqueConstraintLockKey][2]int {
		schema.uniqueConstraintCommitLocksMu.Lock()
		defer schema.uniqueConstraintCommitLocksMu.Unlock()
		out := make(map[uniqueConstraintLockKey][2]int, len(schema.uniqueConstraintCommitLocks))
		for key, lock := range schema.uniqueConstraintCommitLocks {
			out[key] = [2]int{lock.refs, lock.holds}
		}
		return out
	}
	before := snapshot()
	require.NotEmpty(t, before)
	// Existing keys are reentrant under this prepared owner, so the first
	// cancellation poll is the subsequent value-key preparation admission.
	canceled := newIndexPollCancelContext(t, 1)
	release, err := prepared.RetainAcceptedAuthority(canceled)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, release)
	require.Equal(t, before, snapshot(), "failed key preparation leaked additional entity authority")
	_, err = engine.GetStoredNode(t.Context(), "test:prepared_tuple")
	require.ErrorIs(t, err, ErrNotFound, "failed retention published the prepared node")
	release, err = prepared.RetainAcceptedAuthority(context.Background())
	require.NoError(t, err)
	release()
	require.Equal(t, before, snapshot(), "healthy retry did not release its additional authority")
	require.NoError(t, prepared.Commit(nil))
	require.Empty(t, snapshot())
	stored, err := engine.GetStoredNode(t.Context(), "test:prepared_tuple")
	require.NoError(t, err)
	require.Equal(t, int64(1), stored.Properties["a"])
	require.Equal(t, int64(2), stored.Properties["b"])
}

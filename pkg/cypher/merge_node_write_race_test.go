package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// mergeKeyWaits records each time a transaction starts waiting for a
// constraint key lock, so a test orders two statements by the wait itself.
func mergeKeyWaits(t *testing.T) chan struct{} {
	t.Helper()
	waits := make(chan struct{}, 64)
	restore := storage.SetUniqueKeyLockWaitHookForTesting(func() {
		select {
		case waits <- struct{}{}:
		default:
		}
	})
	t.Cleanup(restore)
	return waits
}

func drainMergeKeyWaits(waits chan struct{}) {
	for {
		select {
		case <-waits:
		default:
			return
		}
	}
}

func newMergeRaceStore(t *testing.T) (storage.Engine, *StorageExecutor) {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	setup := NewStorageExecutor(store)
	_, err := setup.Execute(context.Background(), "CREATE CONSTRAINT doc_id FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil)
	require.NoError(t, err)
	return store, setup
}

// runMergeStatement runs query in its own session, as an auto-commit
// statement or in an explicit transaction.
func runMergeStatement(ctx context.Context, store storage.Engine, explicit bool, query string, params map[string]interface{}) error {
	session := NewStorageExecutor(store)
	if !explicit {
		_, err := session.Execute(ctx, query, params)
		return err
	}
	if _, err := session.Execute(ctx, "BEGIN", nil); err != nil {
		return err
	}
	if _, err := session.Execute(ctx, query, params); err != nil {
		_, _ = session.Execute(ctx, "ROLLBACK", nil)
		return err
	}
	_, err := session.Execute(ctx, "COMMIT", nil)
	return err
}

// TestMergeWritersOfOneNodeSerializeLikeNeo4j (NOR-117): two MERGE statements
// that write the node they match, and race. In Neo4j 5.26 both succeed: the
// second waits for the first, then writes on top of its commit. Before, both
// wrote the node from the same snapshot and the later commit failed with a
// UNIQUE violation, though no duplicate was being created. The second
// statement starts only once the first holds the node and ends only after
// the first commits; the order is set by the key wait, not by timing.
func TestMergeWritersOfOneNodeSerializeLikeNeo4j(t *testing.T) {
	store, setup := newMergeRaceStore(t)
	waits := mergeKeyWaits(t)
	ctx := context.Background()
	for _, query := range []string{
		"MERGE (n:Doc {id: $k}) ON MATCH SET n.w = $w",
		"MERGE (n:Doc {id: $k}) ON CREATE SET n.c = 1 ON MATCH SET n.w = $w",
		"MERGE (n:Doc {id: $k}) SET n.w = $w",
		"MERGE (n:Doc {id: $k}) SET n += {w: $w}",
		"MERGE (n:Doc {id: $k}) SET n.w = $w REMOVE n.gone",
		"MERGE (n:Doc {id: $k}) WITH n SET n.w = $w",
		"UNWIND [$k] AS k MERGE (n:Doc {id: k}) ON MATCH SET n.w = $w",
		"MERGE (n:Doc {id: $k})-[:HAS]->(:Part {id: $k}) ON MATCH SET n.w = $w",
	} {
		for _, explicit := range []bool{false, true} {
			key := query + map[bool]string{false: " auto", true: " explicit"}[explicit]
			_, err := setup.Execute(ctx, "CREATE (:Doc {id: $k, gone: 1})-[:HAS]->(:Part {id: $k})", map[string]interface{}{"k": key})
			require.NoError(t, err)
			drainMergeKeyWaits(waits)

			first := NewStorageExecutor(store)
			_, err = first.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			_, err = first.Execute(ctx, query, map[string]interface{}{"k": key, "w": int64(1)})
			require.NoError(t, err, key)
			done := make(chan error, 1)
			go func() {
				done <- runMergeStatement(ctx, store, explicit, query, map[string]interface{}{"k": key, "w": int64(2)})
			}()
			var secondErr error
			waited := false
			select {
			case <-waits:
				waited = true
			case secondErr = <-done:
			}
			_, err = first.Execute(ctx, "COMMIT", nil)
			require.NoError(t, err, key)
			if waited {
				secondErr = <-done
			}
			require.NoError(t, secondErr, key)
			require.True(t, waited, "%s: the second MERGE writer waits for the first", key)

			result, err := setup.Execute(ctx, "MATCH (n:Doc {id: $k}) RETURN count(n), collect(n.w)", map[string]interface{}{"k": key})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1), []interface{}{int64(2)}}}, result.Rows, key)
		}
	}
}

// TestMergeWritersWaitForTheCreatingMerge: three MERGEs of a new key that
// write its node. The first creates it; the others wait for it, then for
// each other, and match it: one node, every statement succeeds.
func TestMergeWritersWaitForTheCreatingMerge(t *testing.T) {
	store, setup := newMergeRaceStore(t)
	waits := mergeKeyWaits(t)
	ctx := context.Background()
	const query = "MERGE (n:Doc {id: 'new'}) ON CREATE SET n.created = $w ON MATCH SET n.matched = coalesce(n.matched, 0) + 1"
	first := NewStorageExecutor(store)
	_, err := first.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = first.Execute(ctx, query, map[string]interface{}{"w": int64(1)})
	require.NoError(t, err)
	done := make(chan error, 2)
	for _, explicit := range []bool{false, true} {
		go func() { done <- runMergeStatement(ctx, store, explicit, query, map[string]interface{}{"w": int64(2)}) }()
		<-waits
	}
	_, err = first.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	require.NoError(t, <-done)
	require.NoError(t, <-done)
	result, err := setup.Execute(ctx, "MATCH (n:Doc {id: 'new'}) RETURN count(n), collect(n.created), collect(n.matched)", nil)
	require.NoError(t, err)
	// The matchers' increments serialize: neither is lost.
	require.Equal(t, [][]interface{}{{int64(1), []interface{}{int64(1)}, []interface{}{int64(2)}}}, result.Rows)
}

// TestMergeReadersDoNotWaitForMergeWriters: a MERGE whose statement doesn't
// write the node it matches (it only reads it, or sets it only when it
// creates it) matches without waiting for a transaction that writes it.
func TestMergeReadersDoNotWaitForMergeWriters(t *testing.T) {
	store, setup := newMergeRaceStore(t)
	waits := mergeKeyWaits(t)
	ctx := context.Background()
	_, err := setup.Execute(ctx, "CREATE (:Doc {id: 'r', w: 0})", nil)
	require.NoError(t, err)
	writer := NewStorageExecutor(store)
	_, err = writer.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = writer.Execute(ctx, "MERGE (n:Doc {id: 'r'}) ON MATCH SET n.w = 1", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"MERGE (n:Doc {id: 'r'}) RETURN n.w",
		"MERGE (n:Doc {id: 'r'}) ON CREATE SET n.w = 5 RETURN n.w",
		"MERGE (n:Doc {id: 'r'}) MERGE (n)-[:SEEN]->(:Part {id: 'r'}) RETURN n.w",
	} {
		drainMergeKeyWaits(waits)
		done := make(chan *ExecuteResult, 1)
		failed := make(chan error, 1)
		go func() {
			result, err := NewStorageExecutor(store).Execute(ctx, query, nil)
			if err != nil {
				failed <- err
				return
			}
			done <- result
		}()
		select {
		case result := <-done:
			require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, query)
		case err := <-failed:
			require.NoError(t, err, query)
		case <-waits:
			_, _ = writer.Execute(ctx, "ROLLBACK", nil)
			t.Fatalf("%s waited for the writer", query)
		}
	}
	_, err = writer.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
}

// TestMergeWriterAfterWritesStaysRetryable: a transaction that wrote before
// its MERGE can't move its snapshot past the writer it waited for, so its
// MERGE fails with the retryable conflict of snapshot isolation, not a
// UNIQUE violation, and nothing is duplicated.
func TestMergeWriterAfterWritesStaysRetryable(t *testing.T) {
	store, setup := newMergeRaceStore(t)
	waits := mergeKeyWaits(t)
	ctx := context.Background()
	_, err := setup.Execute(ctx, "CREATE (:Doc {id: 'seen'})", nil)
	require.NoError(t, err)
	first := NewStorageExecutor(store)
	_, err = first.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = first.Execute(ctx, "MERGE (n:Doc {id: 'seen'}) ON MATCH SET n.w = 1", nil)
	require.NoError(t, err)
	second := NewStorageExecutor(store)
	_, err = second.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = second.Execute(ctx, "CREATE (:Other)", nil)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		_, err := second.Execute(ctx, "MERGE (n:Doc {id: 'seen'}) ON MATCH SET n.w = 2", nil)
		done <- err
	}()
	waited := false
	select {
	case <-waits:
		waited = true
	case err = <-done:
	}
	_, commitErr := first.Execute(ctx, "COMMIT", nil)
	require.NoError(t, commitErr)
	if waited {
		err = <-done
	}
	require.True(t, waited, "the MERGE writer waits for the first")
	require.Error(t, err)
	require.True(t, errors.Is(err, storage.ErrConflict), "%v", err)
	var violation *storage.ConstraintViolationError
	require.False(t, errors.As(err, &violation), "%v", err)
	_, _ = second.Execute(ctx, "ROLLBACK", nil)
	result, err := setup.Execute(ctx, "MATCH (n:Doc {id: 'seen'}) RETURN count(n), collect(n.w)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), []interface{}{int64(1)}}}, result.Rows)
}

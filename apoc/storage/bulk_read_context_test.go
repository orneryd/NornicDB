package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// bulkReadFixtureSize seeds far more entries than a bulk read copies between
// two checks of its caller's context. Healthy reads exercise the interval;
// pre-canceled reads prove admission rejection, not mid-loop interruption.
const bulkReadFixtureSize = 1024

// stoppedBulkReadContexts returns the two real ways a caller's lifetime ends
// before a read starts: a context that was canceled, and one whose deadline has
// already expired. Both are live contexts built from real CancelFuncs.
func stoppedBulkReadContexts(t *testing.T) map[string]context.Context {
	t.Helper()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, expire := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	t.Cleanup(expire)
	require.Error(t, expired.Err())
	return map[string]context.Context{
		"canceled": canceled,
		"expired":  expired,
	}
}

// bulkReadFixture seeds a store whose node and relationship counts both cross
// the bulk read's in-loop context interval.
func bulkReadFixture(t *testing.T) *InMemoryStorage {
	t.Helper()
	require.Less(t, 2*bulkReadInterval, bulkReadFixtureSize,
		"the fixture must cross the in-loop context check")
	store := NewInMemoryStorage()
	for index := 0; index < bulkReadFixtureSize; index++ {
		_, err := store.CreateNode(t.Context(), []string{"Fixture"}, map[string]interface{}{"index": int64(index)})
		require.NoError(t, err)
		if index == 0 {
			continue
		}
		_, err = store.CreateRelationship(t.Context(), int64(index), int64(index+1), "NEXT", map[string]interface{}{"index": int64(index)})
		require.NoError(t, err)
	}
	return store
}

// TestInMemoryStorageAllNodesStopsWithoutCallerContext requires the whole-graph
// node read to stop with its caller's own context error and to hand back no
// partial collection.
func TestInMemoryStorageAllNodesStopsWithoutCallerContext(t *testing.T) {
	store := bulkReadFixture(t)
	nodes, err := store.AllNodes(context.Background())
	require.NoError(t, err)
	require.Len(t, nodes, bulkReadFixtureSize)

	for name, ctx := range stoppedBulkReadContexts(t) {
		t.Run(name, func(t *testing.T) {
			nodes, err := store.AllNodes(ctx)
			require.ErrorIs(t, err, ctx.Err())
			require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded), err)
			require.Nil(t, nodes, "a stopped whole-graph read must not publish a partial collection")
		})
	}
}

// TestInMemoryStorageAllRelationshipsStopsWithoutCallerContext is the same
// requirement for the relationship read, whose context is now its lifetime.
func TestInMemoryStorageAllRelationshipsStopsWithoutCallerContext(t *testing.T) {
	store := bulkReadFixture(t)
	rels, err := store.AllRelationships(context.Background())
	require.NoError(t, err)
	require.Len(t, rels, bulkReadFixtureSize-1)

	for name, ctx := range stoppedBulkReadContexts(t) {
		t.Run(name, func(t *testing.T) {
			rels, err := store.AllRelationships(ctx)
			require.ErrorIs(t, err, ctx.Err())
			require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded), err)
			require.Nil(t, rels, "a stopped whole-graph read must not publish a partial collection")
		})
	}
}

// TestInMemoryStorageAllNodesKeepsItsValueContract pins the result a live
// caller already received: every node of the store, and a non-nil empty slice
// for an empty store.
func TestInMemoryStorageAllNodesKeepsItsValueContract(t *testing.T) {
	store := bulkReadFixture(t)
	nodes, err := store.AllNodes(context.Background())
	require.NoError(t, err)
	require.Len(t, nodes, bulkReadFixtureSize)
	ids := map[int64]bool{}
	for _, node := range nodes {
		require.False(t, ids[node.ID], "every node appears once")
		ids[node.ID] = true
	}

	empty, err := NewInMemoryStorage().AllNodes(context.Background())
	require.NoError(t, err)
	require.NotNil(t, empty, "an empty whole-graph read stays a non-nil empty collection")
	require.Empty(t, empty)
}

// TestInMemoryStorageAllRelationshipsKeepsItsValueContract pins the same
// contract for the relationship read.
func TestInMemoryStorageAllRelationshipsKeepsItsValueContract(t *testing.T) {
	store := bulkReadFixture(t)
	rels, err := store.AllRelationships(context.Background())
	require.NoError(t, err)
	require.Len(t, rels, bulkReadFixtureSize-1)
	ids := map[int64]bool{}
	for _, rel := range rels {
		require.False(t, ids[rel.ID], "every relationship appears once")
		ids[rel.ID] = true
	}

	empty, err := NewInMemoryStorage().AllRelationships(context.Background())
	require.NoError(t, err)
	require.NotNil(t, empty, "an empty whole-graph read stays a non-nil empty collection")
	require.Empty(t, empty)
}

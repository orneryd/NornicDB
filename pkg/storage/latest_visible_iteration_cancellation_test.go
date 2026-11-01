package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// The optional MVCCEnumerationEngine contract loads the latest visible
// population and then hands every record to the caller's visitor. These tests
// pin the caller-lifetime half of that contract on the real Badger engine the
// MVCC public-API fixture builds: once the visitor cancels the caller's
// context — during the population or on its final record — the enumeration
// must retire with the context's own error instead of reporting a successful
// read. The stride and the end of the population are the two places a
// cancellation is observed, so the population below is deliberately larger
// than one cancellation interval.

const latestVisibleBulkPopulation = 300

// latestVisibleFixtureEngine builds the shared MVCC public-API fixture and
// grows it past one bulkReadCancellation interval through the standard
// committed write path, so the assertions run against a native Badger
// population rather than a synthetic carrier.
func latestVisibleFixtureEngine(t *testing.T) (*MemoryEngine, NodeID, NodeID) {
	t.Helper()
	engine, aliceID, bobID, _ := mvccPublicAPIFixture(t)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	for i := 0; i < latestVisibleBulkPopulation; i++ {
		_, err := tx.CreateNode(&Node{
			ID:         NodeID(fmt.Sprintf("test:bulk-node-%03d", i)),
			Labels:     []string{"Bulk"},
			Properties: map[string]any{"i": int64(i)},
		})
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
	return engine, aliceID, bobID
}

func collectLatestVisibleNodeIDs(t *testing.T, engine *MemoryEngine, ctx context.Context) []NodeID {
	t.Helper()
	var ids []NodeID
	require.NoError(t, engine.IterateLatestVisibleNodes(ctx, func(n *Node) error {
		require.NotNil(t, n)
		ids = append(ids, n.ID)
		return nil
	}))
	return ids
}

func TestMVCCLatestVisibleNodesIterationCancellation(t *testing.T) {
	engine, _, _ := latestVisibleFixtureEngine(t)

	population := collectLatestVisibleNodeIDs(t, engine, context.Background())
	require.Len(t, population, 2+latestVisibleBulkPopulation)
	require.Greater(t, len(population), bulkReadCancellationInterval,
		"the fixture population must exceed one cancellation interval")
	seen := make(map[NodeID]bool, len(population))
	for _, id := range population {
		require.False(t, seen[id], "node %q yielded twice", id)
		seen[id] = true
	}

	t.Run("visitor cancels on the first record", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		yielded := 0
		err := engine.IterateLatestVisibleNodes(ctx, func(*Node) error {
			yielded++
			if yielded == 1 {
				cancel()
			}
			return nil
		})
		require.ErrorIs(t, err, context.Canceled,
			"a visitor that canceled the caller's context must not be reported as a successful enumeration")
		require.Equal(t, bulkReadCancellationInterval, yielded,
			"the bulk read stride must observe the cancellation one interval after the first check")
		require.Less(t, yielded, len(population))
	})

	t.Run("visitor cancels on the last record", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		yielded := 0
		err := engine.IterateLatestVisibleNodes(ctx, func(*Node) error {
			yielded++
			if yielded == len(population) {
				cancel()
			}
			return nil
		})
		require.ErrorIs(t, err, context.Canceled,
			"the final check must report a cancellation the last yield caused")
		require.Equal(t, len(population), yielded)
	})

	t.Run("pre-canceled admission", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		yielded := 0
		err := engine.IterateLatestVisibleNodes(ctx, func(*Node) error {
			yielded++
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, yielded)
	})

	t.Run("visitor error identity", func(t *testing.T) {
		visitorErr := errors.New("visitor stopped the enumeration")

		yielded := 0
		err := engine.IterateLatestVisibleNodes(context.Background(), func(*Node) error {
			yielded++
			return visitorErr
		})
		require.Equal(t, visitorErr, err,
			"an ordinary visitor error must surface unchanged")
		require.Equal(t, 1, yielded)
	})

	t.Run("visitor error outranks the cancellation it caused", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		visitorErr := errors.New("visitor stopped and canceled")

		err := engine.IterateLatestVisibleNodes(ctx, func(*Node) error {
			cancel()
			return visitorErr
		})
		require.Equal(t, visitorErr, err,
			"the visitor's own error must win over the cancellation it produced")
	})
}

func TestMVCCLatestVisibleEdgesIterationLifetime(t *testing.T) {
	engine, aliceID, bobID := latestVisibleFixtureEngine(t)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	for i := 0; i < latestVisibleBulkPopulation; i++ {
		require.NoError(t, tx.CreateEdge(&Edge{
			ID:         EdgeID(fmt.Sprintf("test:bulk-edge-%03d", i)),
			StartNode:  aliceID,
			EndNode:    bobID,
			Type:       "BULK",
			Properties: map[string]any{"i": int64(i)},
		}))
	}
	require.NoError(t, tx.Commit())

	var population []EdgeID
	require.NoError(t, engine.IterateLatestVisibleEdges(func(e *Edge) error {
		require.NotNil(t, e)
		population = append(population, e.ID)
		return nil
	}))
	require.Len(t, population, 1+latestVisibleBulkPopulation,
		"edge enumeration keeps its context-free lifetime and delivers the whole population")

	t.Run("visitor error identity", func(t *testing.T) {
		visitorErr := errors.New("visitor stopped the edge enumeration")

		yielded := 0
		err := engine.IterateLatestVisibleEdges(func(*Edge) error {
			yielded++
			return visitorErr
		})
		require.Equal(t, visitorErr, err)
		require.Equal(t, 1, yielded)
	})
}

func TestMVCCLatestVisibleEnumerationNamespaceConflictPrecedence(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "victim:n"})
	require.NoError(t, err)

	cleanupErr := errors.New("cleanup unavailable")
	engine.dropPrefixTailHook = func(prefix string) error {
		if prefix == "victim:" {
			return cleanupErr
		}
		return nil
	}
	_, _, err = engine.DeleteByPrefix("victim:")
	require.ErrorIs(t, err, cleanupErr)
	require.EqualValues(t, 1, engine.prefixDrops.pendingCount.Load())
	t.Cleanup(func() {
		engine.dropPrefixTailHook = nil
		_, _, err := engine.DeleteByPrefix("victim:")
		require.NoError(t, err)
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	conflictErr := engine.pendingNamespaceConflict("")
	require.ErrorIs(t, conflictErr, ErrConflict)

	yielded := 0
	err = engine.IterateLatestVisibleNodes(ctx, func(*Node) error {
		yielded++
		return nil
	})
	require.ErrorIs(t, err, ErrConflict,
		"the pending namespace drop must keep precedence over a canceled caller context")
	require.Zero(t, yielded)

	err = engine.IterateLatestVisibleEdges(func(*Edge) error {
		yielded++
		return nil
	})
	require.ErrorIs(t, err, ErrConflict)
	require.Zero(t, yielded)
}

func TestMVCCLatestVisibleNodesIterationReadErrorIdentity(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:n"})
	require.NoError(t, err)
	require.NoError(t, engine.Close())

	err = engine.IterateLatestVisibleNodes(context.Background(), func(*Node) error { return nil })
	require.ErrorIs(t, err, ErrStorageClosed,
		"a read failure must surface as itself, not as a cancellation")
}

func TestLatestVisibleIterationOwnerAllocations(t *testing.T) {
	records := make([]*Node, 4096)
	for i := range records {
		records[i] = &Node{ID: NodeID(fmt.Sprintf("alloc-%04d", i))}
	}
	load := func(context.Context) ([]*Node, error) { return records, nil }

	visited := 0
	yield := func(*Node) error {
		visited++
		return nil
	}

	var iterErr error
	allocs := testing.AllocsPerRun(16, func() {
		visited = 0
		iterErr = yieldLatestVisible(context.Background(), load, yield)
	})
	require.NoError(t, iterErr)
	require.Equal(t, len(records), visited)
	require.Zero(t, allocs,
		"the shared enumeration owner must not allocate per record")
}

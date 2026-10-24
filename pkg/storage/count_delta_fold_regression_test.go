package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// TestCountDeltaFoldLargeBacklogChunked pins the chunked fold (monster retest
// part 2): a single pair with far more deltas than one Badger batch can hold
// must fold without ErrTxnTooBig, and the deltas must be gone afterwards.
// Folding is off the read path (PR #1043 issue B): a count read sums the
// deltas in place and the background fold worker collapses them later.
func TestCountDeltaFoldLargeBacklogChunked(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	const deltas = countDeltaFoldChunk*2 + 137 // spans three fold chunks
	require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		for i := 0; i < deltas; i++ {
			if err := engine.adjustLabelCountInTxn(txn, "ns1", "BigFold", 1); err != nil {
				return err
			}
		}
		return nil
	}))

	// The read sums the deltas in place and returns the correct count
	// without paying a synchronous fold.
	count, err := engine.NodeCountByLabelInNamespace("ns1", "BigFold")
	require.NoError(t, err)
	require.Equal(t, int64(deltas), count)

	// The background fold path must collapse every delta into the base
	// without ErrTxnTooBig, exactly as the read-triggered fold used to.
	engine.foldCountDeltasInBackground()

	var remaining int
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		prefix := countDeltaPrefixFor(countDeltaKindLabel, "ns1", "BigFold")
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			remaining++
		}
		return nil
	}))
	require.Zero(t, remaining, "background fold must consume the whole backlog")

	// The folded base answers the same count.
	count, err = engine.NodeCountByLabelInNamespace("ns1", "BigFold")
	require.NoError(t, err)
	require.Equal(t, int64(deltas), count)
}

// TestCountDeltaLargeBacklogReopen pins the open/close fold (monster retest
// part 2): a single-statement load leaves one delta per commit; past the old
// one-batch fold limit the database could not be reopened. A large backlog of
// real nodes must fold at close and reopen cleanly with the correct count.
func TestCountDeltaLargeBacklogReopen(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewBadgerEngine(dir)
	require.NoError(t, err)

	const nodes = countDeltaFoldChunk + 1000 // > one chunk of deltas
	for i := 0; i < nodes; i++ {
		_, err := engine.CreateNode(&Node{ID: NodeID(fmt.Sprintf("ns1:rb%05d", i)), Labels: []string{"ReopenBig"}})
		require.NoError(t, err)
	}

	// Close without ever reading the count, so every delta is folded at close.
	require.NoError(t, engine.Close())

	reopened, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	count, err := reopened.NodeCountByLabelInNamespace("ns1", "ReopenBig")
	require.NoError(t, err)
	require.Equal(t, int64(nodes), count)

	// No deltas may survive the close-time fold: a further reopen must see a
	// clean base and remain correct.
	require.NoError(t, reopened.Close())
	reopened2, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened2.Close() })
	count, err = reopened2.NodeCountByLabelInNamespace("ns1", "ReopenBig")
	require.NoError(t, err)
	require.Equal(t, int64(nodes), count)
}

// TestConcurrentRelationshipCreateCountsExact pins the positional-count lock
// ordering (monster retest part 2): concurrent commits creating relationships
// of one (label, type) pair must not conflict on the read-modify-write
// positional counter and double-count.
func TestConcurrentRelationshipCreateCountsExact(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	ns := "ns1"
	const nodeCount = 100
	for i := 0; i < nodeCount; i++ {
		_, err := engine.CreateNode(&Node{ID: NodeID(fmt.Sprintf("%s:p%03d", ns, i)), Labels: []string{"P"}})
		require.NoError(t, err)
	}

	const total = 2000
	const workers = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := worker; i < total; i += workers {
				a := i % nodeCount
				b := (i + 1) % nodeCount
				tx, err := engine.BeginTransaction()
				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					return
				}
				if err := tx.SetNamespace(ns); err != nil {
					_ = tx.Rollback()
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					return
				}
				edge := &Edge{
					ID:        EdgeID(fmt.Sprintf("%s:k%05d", ns, i)),
					Type:      "K",
					StartNode: NodeID(fmt.Sprintf("%s:p%03d", ns, a)),
					EndNode:   NodeID(fmt.Sprintf("%s:p%03d", ns, b)),
				}
				if err := tx.CreateEdge(edge); err != nil {
					_ = tx.Rollback()
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					return
				}
				if err := tx.Commit(); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					return
				}
			}
		}(w)
	}
	wg.Wait()
	require.Empty(t, errs)

	count, err := engine.EdgeCountByTypeInNamespace(ns, "K")
	require.NoError(t, err)
	require.Equal(t, int64(total), count, "per-type count must be exact")

	startCount, err := engine.EdgeCountByStartLabelInNamespace(ns, "P", "K")
	require.NoError(t, err)
	require.Equal(t, int64(total), startCount, "start-label positional count must be exact")

	endCount, err := engine.EdgeCountByEndLabelInNamespace(ns, "P", "K")
	require.NoError(t, err)
	require.Equal(t, int64(total), endCount, "end-label positional count must be exact")
}

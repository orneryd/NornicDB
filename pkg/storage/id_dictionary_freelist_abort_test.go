package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// An aborted transaction must not leave its pending name aliased with a later
// transaction that sees the same still-durable freelist entry.
func TestNumericFreelistAbortDoesNotAliasLaterAllocation(t *testing.T) {
	for _, kind := range []byte{freelistKindNode, freelistKindEdge} {
		t.Run(string(kind), func(t *testing.T) {
			b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			b.idDict.setFreelistTTL(time.Nanosecond)
			require.NoError(t, b.db.Update(func(tx *badger.Txn) error {
				if err := tx.Set(freelistKey(kind, 1, 100), nil); err != nil {
					return err
				}
				return tx.Set(freelistKey(kind, 2, 101), nil)
			}))
			if kind == freelistKindNode {
				b.idDict.nodeFreelistPending.Store(2)
			} else {
				b.idDict.edgeFreelistPending.Store(2)
			}
			allocate := func(name string) (*badger.Txn, uint64) {
				t.Helper()
				txn := b.db.DB.NewTransactionAt(b.db.oracle.published.Load(), true)
				staged := &BadgerTransaction{badgerTx: txn, pendingWrites: map[string][]byte{}, pendingDeletes: map[string]bool{}}
				var id uint64
				if kind == freelistKindNode {
					id, err = resolveOrAllocateNumericIDInTxn(b.idDict, staged.staged(), NodeID(NodeID(name)))
				} else {
					id, err = resolveOrAllocateNumericIDInTxn(b.idDict, staged.staged(), EdgeID(EdgeID(name)))
				}
				require.NoError(t, err)
				return txn, id
			}
			firstTxn, first := allocate("test:first")
			require.Equal(t, uint64(100), first)
			b.idDict.discardTxnCounters(firstTxn)
			firstTxn.Discard()
			secondTxn, second := allocate("test:second")
			t.Cleanup(func() { b.idDict.discardTxnCounters(secondTxn); secondTxn.Discard() })
			var still uint64
			var exists bool
			if kind == freelistKindNode {
				still, exists = b.idDict.lookupNodeNumID("test:first")
			} else {
				still, exists = b.idDict.lookupEdgeNumID("test:first")
			}
			if exists {
				require.NotEqual(t, still, second, "an abandoned pending name cannot share a numeric ID")
			}
		})
	}
}

func TestNumericFreelistAbortPreservesPublicLabelReads(t *testing.T) {
	b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.idDict.setFreelistTTL(time.Nanosecond)
	b.idDict.nextNode.Store(101)
	require.NoError(t, b.db.Update(func(tx *badger.Txn) error {
		if err := tx.Set(freelistKey(freelistKindNode, 1, 100), nil); err != nil {
			return err
		}
		return tx.Set(freelistKey(freelistKindNode, 2, 101), nil)
	}))
	b.idDict.nodeFreelistPending.Store(2)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:first", Labels: []string{"First"}})
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	_, err = b.CreateNode(&Node{ID: "test:second", Labels: []string{"Second"}})
	require.NoError(t, err)
	_, err = b.CreateNode(&Node{ID: "test:first", Labels: []string{"First"}})
	require.NoError(t, err)
	first, ok := b.idDict.lookupNodeNumID("test:first")
	require.True(t, ok)
	second, ok := b.idDict.lookupNodeNumID("test:second")
	require.True(t, ok)
	require.NotEqual(t, first, second)
	nodes, err := b.GetNodesByLabelInScope("test", "Second")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, NodeID("test:second"), nodes[0].ID)
}

func TestNumericFreelistPendingNamePersistsItsClaimAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	b.idDict.setFreelistTTL(time.Nanosecond)
	b.idDict.nextNode.Store(101)
	firstKey := freelistKey(freelistKindNode, 1, 100)
	require.NoError(t, b.db.Update(func(tx *badger.Txn) error {
		if err := tx.Set(firstKey, nil); err != nil {
			return err
		}
		return tx.Set(freelistKey(freelistKindNode, 2, 101), nil)
	}))
	b.idDict.nodeFreelistPending.Store(2)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:first", Labels: []string{"First"}})
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	_, err = b.CreateNode(&Node{ID: "test:first", Labels: []string{"First"}})
	require.NoError(t, err)
	require.NoError(t, b.db.View(func(tx *badger.Txn) error {
		_, err := tx.Get(firstKey)
		require.ErrorIs(t, err, badger.ErrKeyNotFound, "adopting the pending ID must remove its free key")
		return nil
	}))
	require.NoError(t, b.Close())
	b, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	b.idDict.setFreelistTTL(time.Nanosecond)
	_, err = b.CreateNode(&Node{ID: "test:second", Labels: []string{"Second"}})
	require.NoError(t, err)
	first, ok := b.idDict.lookupNodeNumID("test:first")
	require.True(t, ok)
	second, ok := b.idDict.lookupNodeNumID("test:second")
	require.True(t, ok)
	require.NotEqual(t, first, second)
	for label, id := range map[string]NodeID{"First": "test:first", "Second": "test:second"} {
		nodes, err := b.GetNodesByLabelInScope("test", label)
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		require.Equal(t, id, nodes[0].ID)
	}
}

func TestNumericFreelistConcurrentStagedClaimsUseDifferentIDs(t *testing.T) {
	for _, kind := range []byte{freelistKindNode, freelistKindEdge} {
		t.Run(string(kind), func(t *testing.T) {
			b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			b.idDict.setFreelistTTL(time.Nanosecond)
			require.NoError(t, b.db.Update(func(tx *badger.Txn) error {
				if err := tx.Set(freelistKey(kind, 1, 100), nil); err != nil {
					return err
				}
				return tx.Set(freelistKey(kind, 2, 101), nil)
			}))
			if kind == freelistKindNode {
				b.idDict.nodeFreelistPending.Store(2)
			} else {
				b.idDict.edgeFreelistPending.Store(2)
			}
			allocate := func(name string) uint64 {
				txn := b.db.DB.NewTransactionAt(b.db.oracle.published.Load(), true)
				t.Cleanup(func() { b.idDict.discardTxnCounters(txn); txn.Discard() })
				staged := &BadgerTransaction{badgerTx: txn, pendingWrites: map[string][]byte{}, pendingDeletes: map[string]bool{}}
				var n uint64
				if kind == freelistKindNode {
					n, err = resolveOrAllocateNumericIDInTxn(b.idDict, staged.staged(), NodeID(NodeID(name)))
				} else {
					n, err = resolveOrAllocateNumericIDInTxn(b.idDict, staged.staged(), EdgeID(EdgeID(name)))
				}
				require.NoError(t, err)
				return n
			}
			first, second := allocate("test:first"), allocate("test:second")
			require.Equal(t, uint64(100), first)
			require.Equal(t, uint64(101), second)
			require.NotEqual(t, first, second)
		})
	}
}

func TestNumericFreelistParallelReservationsAreUnique(t *testing.T) {
	const count = 24
	for _, kind := range []byte{freelistKindNode, freelistKindEdge} {
		t.Run(string(kind), func(t *testing.T) {
			b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			b.idDict.setFreelistTTL(time.Nanosecond)
			require.NoError(t, b.db.Update(func(tx *badger.Txn) error {
				for i := 0; i < count; i++ {
					if err := tx.Set(freelistKey(kind, uint64(i+1), uint64(i+100)), nil); err != nil {
						return err
					}
				}
				return nil
			}))
			if kind == freelistKindNode {
				b.idDict.nodeFreelistPending.Store(count)
			} else {
				b.idDict.edgeFreelistPending.Store(count)
			}
			type result struct {
				id  uint64
				txn *badger.Txn
				err error
			}
			results := make(chan result, count)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := 0; i < count; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					txn := b.db.DB.NewTransactionAt(b.db.oracle.published.Load(), true)
					staged := &BadgerTransaction{badgerTx: txn, pendingWrites: map[string][]byte{}, pendingDeletes: map[string]bool{}}
					<-start
					name := fmt.Sprintf("test:parallel-%d", i)
					var id uint64
					var err error
					if kind == freelistKindNode {
						id, err = resolveOrAllocateNumericIDInTxn(b.idDict, staged.staged(), NodeID(NodeID(name)))
					} else {
						id, err = resolveOrAllocateNumericIDInTxn(b.idDict, staged.staged(), EdgeID(EdgeID(name)))
					}
					results <- result{id: id, txn: txn, err: err}
				}(i)
			}
			close(start)
			wg.Wait()
			close(results)
			seen := make(map[uint64]bool, count)
			for result := range results {
				b.idDict.discardTxnCounters(result.txn)
				result.txn.Discard()
				require.NoError(t, result.err)
				require.GreaterOrEqual(t, result.id, uint64(100))
				require.Less(t, result.id, uint64(100+count))
				require.False(t, seen[result.id], "each recycled ID must have one in-flight owner")
				seen[result.id] = true
			}
			require.Len(t, seen, count)
		})
	}
}

func TestRejectedBufferedCommitDropsUnpersistedIDBeforeRetry(t *testing.T) {
	dir := t.TempDir()
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, manualDrain: true})
	require.NoError(t, b.bufferedCommitWAL.Close())
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:rejected", Labels: []string{"Retry"}})
	require.NoError(t, err)
	require.ErrorIs(t, tx.Commit(), ErrWALClosed)
	_, pending := b.idDict.nodePendingWriter("test:rejected")
	require.False(t, pending, "a rejected commit must release detached bookkeeping")
	_, found := b.idDict.lookupNodeNumID("test:rejected")
	require.False(t, found, "an unpersisted entry must not look durable")
	b.SetBufferedCommitWAL(nil)
	_, err = b.CreateNode(&Node{ID: "test:rejected", Labels: []string{"Retry"}})
	require.NoError(t, err)
	_, pending = b.idDict.nodePendingWriter("test:rejected")
	require.False(t, pending)
	require.NoError(t, b.Close())
	reopened, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	nodes, err := reopened.GetNodesByLabelInScope("test", "Retry")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, NodeID("test:rejected"), nodes[0].ID)
}

// numericIDKinds are the dictionary freelists both rejected-adopter tests cover.
var numericIDKinds = []byte{freelistKindNode, freelistKindEdge}

// resolveNumericIDForKind resolves or allocates id's number in kind's dictionary
// inside tx, returning the number an adopter must agree on.
func resolveNumericIDForKind(t *testing.T, b *BadgerEngine, tx *badger.Txn, kind byte, id string) uint64 {
	t.Helper()
	var (
		num uint64
		err error
	)
	if kind == freelistKindNode {
		num, err = resolveOrAllocateNumericIDInTxn(b.idDict, tx, NodeID(id))
	} else {
		num, err = resolveOrAllocateNumericIDInTxn(b.idDict, tx, EdgeID(id))
	}
	require.NoError(t, err)
	return num
}

// A later adopter can be rejected while the original writer is still able to
// commit. Its rejection must not remove the original writer's in-memory ID.
func TestNumericIDRejectedAdopterPreservesLiveWinner(t *testing.T) {
	for _, kind := range numericIDKinds {
		t.Run(string(kind), func(t *testing.T) {
			b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
			require.NoError(t, err)
			newTxn := writeTxnFor(t, b)
			winner, adopter := newTxn(), newTxn()
			winningID := resolveNumericIDForKind(t, b, winner, kind, "test:adopted")
			require.Equal(t, winningID, resolveNumericIDForKind(t, b, adopter, kind, "test:adopted"))
			b.idDict.flushTxnCounters(adopter)
			b.idDict.abandonPending(b.idDict.takeTxnCommitting(adopter))
			adopter.Discard()

			b.idDict.flushTxnCounters(winner)
			require.NoError(t, b.db.commit(context.Background(), winner, nil))
			b.idDict.markTxnCommitted(winner)
			if kind == freelistKindNode {
				num, ok := b.idDict.lookupNodeNumID("test:adopted")
				require.True(t, ok, "rejection must preserve an in-flight winner")
				require.Equal(t, winningID, num)
				id, ok := b.idDict.lookupNodeIDByNum(num)
				require.True(t, ok)
				require.Equal(t, NodeID("test:adopted"), id)
			} else {
				num, ok := b.idDict.lookupEdgeNumID("test:adopted")
				require.True(t, ok, "rejection must preserve an in-flight winner")
				require.Equal(t, winningID, num)
				id, ok := b.idDict.lookupEdgeIDByNum(num)
				require.True(t, ok)
				require.Equal(t, EdgeID("test:adopted"), id)
			}
		})
	}
}

func TestNumericIDCommittedWinnerSurvivesRejectedAdopter(t *testing.T) {
	for _, kind := range numericIDKinds {
		t.Run(string(kind), func(t *testing.T) {
			b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
			require.NoError(t, err)
			newTxn := writeTxnFor(t, b)
			winner, adopter := newTxn(), newTxn()
			first := resolveNumericIDForKind(t, b, winner, kind, "test:committed")
			second := resolveNumericIDForKind(t, b, adopter, kind, "test:committed")
			require.Equal(t, first, second)
			b.idDict.flushTxnCounters(adopter)
			adopterState := b.idDict.takeTxnCommitting(adopter)
			b.idDict.flushTxnCounters(winner)
			require.NoError(t, b.db.commit(context.Background(), winner, nil))
			b.idDict.markTxnCommitted(winner)
			b.idDict.abandonPending(adopterState)
			var got uint64
			var found bool
			if kind == freelistKindNode {
				got, found = b.idDict.lookupNodeNumID("test:committed")
			} else {
				got, found = b.idDict.lookupEdgeNumID("test:committed")
			}
			require.True(t, found)
			require.Equal(t, first, got)
		})
	}
}

func TestNumericIDRejectedAdopterReleasesAbortedWinnerClaim(t *testing.T) {
	for _, kind := range []byte{freelistKindNode, freelistKindEdge} {
		t.Run(string(kind), func(t *testing.T) {
			b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			b.idDict.setFreelistTTL(time.Nanosecond)
			require.NoError(t, b.db.Update(func(tx *badger.Txn) error { return tx.Set(freelistKey(kind, 1, 100), nil) }))
			if kind == freelistKindNode {
				b.idDict.nextNode.Store(100)
				b.idDict.nodeFreelistPending.Store(1)
			} else {
				b.idDict.nextEdge.Store(100)
				b.idDict.edgeFreelistPending.Store(1)
			}
			newTxn := writeTxnFor(t, b)
			winner, adopter := newTxn(), newTxn()
			resolve := func(tx *badger.Txn, name string) uint64 {
				t.Helper()
				var n uint64
				if kind == freelistKindNode {
					n, err = resolveOrAllocateNumericIDInTxn(b.idDict, tx, NodeID(NodeID(name)))
				} else {
					n, err = resolveOrAllocateNumericIDInTxn(b.idDict, tx, EdgeID(EdgeID(name)))
				}
				require.NoError(t, err)
				return n
			}
			require.EqualValues(t, 100, resolve(winner, "test:aborted"))
			require.EqualValues(t, 100, resolve(adopter, "test:aborted"))
			b.idDict.discardTxnCounters(winner)
			winner.Discard()
			b.idDict.flushTxnCounters(adopter)
			b.idDict.abandonPending(b.idDict.takeTxnCommitting(adopter))
			adopter.Discard()
			require.Zero(t, b.idDict.freelistClaimCount.Load())
			if kind == freelistKindNode {
				_, found := b.idDict.lookupNodeNumID("test:aborted")
				require.False(t, found)
			} else {
				_, found := b.idDict.lookupEdgeNumID("test:aborted")
				require.False(t, found)
			}
			require.EqualValues(t, 100, resolve(newTxn(), "test:reused"))
		})
	}
}

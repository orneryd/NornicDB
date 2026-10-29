package storage

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// TestIDDictionary_ShardedConcurrentPairs exercises the sharded node maps
// under concurrency: distinct IDs allocate in parallel, the same ID
// allocated concurrently from multiple goroutines resolves to ONE numID,
// and every (id, num) pair stays forward/reverse consistent throughout.
// Run with -race.
func TestIDDictionary_ShardedConcurrentPairs(t *testing.T) {
	e := idDictTestEngine(t)
	d := e.BadgerEngine.idDict

	const goroutines = 8
	const perGoroutine = 500
	ids := make([]NodeID, goroutines*perGoroutine)
	for i := range ids {
		ids[i] = NodeID(fmt.Sprintf("n-%06d", i))
	}

	// Phase 1: every goroutine allocates the SAME ids concurrently. Each
	// id must resolve to exactly one numID shared by all goroutines.
	seen := make([]atomic.Uint64, len(ids))
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i, id := range ids {
				var num uint64
				err := e.db.Update(func(txn *badger.Txn) error {
					var err error
					num, err = resolveOrAllocateNumericIDInTxn(d, txn, NodeID(id))
					return err
				})
				require.NoError(t, err)
				if !seen[i].CompareAndSwap(0, num) {
					require.Equal(t, seen[i].Load(), num, "concurrent allocs of %s must agree", id)
				}
			}
		}(g)
	}
	wg.Wait()

	// Every pair round-trips both directions.
	for i, id := range ids {
		num, ok := d.lookupNodeNumID(id)
		require.True(t, ok)
		require.Equal(t, seen[i].Load(), num)
		back, ok := d.lookupNodeIDByNum(num)
		require.True(t, ok)
		require.Equal(t, id, back)
	}

	// Phase 2: concurrent delete-all, verifying reverse entries vanish too.
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := g; i < len(ids); i += goroutines {
				err := e.db.Update(func(txn *badger.Txn) error {
					return deleteIDEntryInTxn(txn, &d.nodes, nodeForwardShard, nodeReverseShard, nodeIDForwardKey, prefixIDDictNodeReverse, ids[i], 0, idPairByString)
				})
				require.NoError(t, err)
			}
		}(g)
	}
	wg.Wait()
	for i := range ids {
		_, ok := d.lookupNodeNumID(ids[i])
		require.False(t, ok, "forward entry for %s must be gone", ids[i])
		_, ok = d.lookupNodeIDByNum(seen[i].Load())
		require.False(t, ok, "reverse entry for %d must be gone", seen[i].Load())
	}

	// Phase 3: re-allocate after delete (fresh pairs, same ids) and verify.
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := g; i < len(ids); i += goroutines {
				err := e.db.Update(func(txn *badger.Txn) error {
					_, err := resolveOrAllocateNumericIDInTxn(d, txn, NodeID(ids[i]))
					return err
				})
				require.NoError(t, err)
			}
		}(g)
	}
	wg.Wait()
	for _, id := range ids {
		num, ok := d.lookupNodeNumID(id)
		require.True(t, ok)
		back, ok := d.lookupNodeIDByNum(num)
		require.True(t, ok)
		require.Equal(t, id, back)
	}
}

// TestIDDictionary_ShardDistribution pins the shard functions: all 64
// shards are reachable and pair helpers route forward and reverse entries
// to their respective shards.
func TestIDDictionary_ShardDistribution(t *testing.T) {
	d := newIDDictionary()
	require.Equal(t, len(d.nodes), nodeDictShards)

	// A spread of IDs must cover many shards, not collapse onto one.
	var touched [nodeDictShards]bool
	for i := 0; i < 4096; i++ {
		touched[nodeForwardShard(NodeID(fmt.Sprintf("id-%d", i)))] = true
	}
	covered := 0
	for _, ok := range touched {
		if ok {
			covered++
		}
	}
	require.Greater(t, covered, nodeDictShards/2, "forward sharding must spread")

	// Reverse sharding is num % shards: consecutive nums hit consecutive shards.
	require.Equal(t, 0, nodeReverseShard(0))
	require.Equal(t, 1, nodeReverseShard(1))
	require.Equal(t, nodeDictShards-1, nodeReverseShard(uint64(nodeDictShards-1)))
	require.Equal(t, 0, nodeReverseShard(uint64(nodeDictShards)))
}

// TestIDDictionary_FreedStampsMonotonic pins the freelist stamp discipline:
// concurrent pushes must mint strictly increasing freed-at stamps, so the
// on-disk sort (kind, stamp, numID) keeps "head entry is oldest" true even
// under NTP steps or per-thread cached wall-clock reads.
func TestIDDictionary_FreedStampsMonotonic(t *testing.T) {
	e := idDictTestEngine(t)
	d := e.BadgerEngine.idDict

	const goroutines = 8
	const perGoroutine = 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				num := uint64(g*perGoroutine + i + 1)
				err := e.db.Update(func(txn *badger.Txn) error {
					return d.pushFreeNodeInTxn(txn, num)
				})
				require.NoError(t, err)
			}
		}(g)
	}
	wg.Wait()

	// Keys under the freelist prefix sort by stamp: scan and verify the
	// stamps are strictly increasing.
	var stamps []uint64
	err := e.db.View(func(txn *badger.Txn) error {
		opts := badgerIteratorOptions()
		opts.Prefix = []byte{prefixIDFreelist, freelistKindNode}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != 1+1+8+8 {
				continue
			}
			stamps = append(stamps, binary.BigEndian.Uint64(key[2:10]))
		}
		return nil
	})
	require.NoError(t, err)
	require.Len(t, stamps, goroutines*perGoroutine)
	for i := 1; i < len(stamps); i++ {
		require.Greater(t, stamps[i], stamps[i-1],
			"stamps must be strictly increasing in key order; got %d after %d", stamps[i], stamps[i-1])
	}

	// The floor keeps new stamps above the highest persisted one (restart
	// invariant): reloading a fresh dictionary from the same store must not
	// let stamps fall behind.
	fresh := newIDDictionary()
	require.NoError(t, fresh.loadFromBadger(e.db))
	next := fresh.nextFreedStamp()
	require.Greater(t, next, stamps[len(stamps)-1])
}

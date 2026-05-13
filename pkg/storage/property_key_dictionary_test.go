package storage

import (
	"encoding/binary"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// TestPropertyKeyDict_ResolveAndLookup covers the basic happy path: a
// fresh dictionary allocates monotonic IDs, persists forward + reverse
// + counter entries, and round-trips through lookup.
func TestPropertyKeyDict_ResolveAndLookup(t *testing.T) {
	eng := newTestEngine(t)
	dict := eng.propKeyDict

	var idA, idB uint64
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		var err error
		idA, err = dict.resolveOrAllocateInTxn(txn, "ns", "alpha")
		if err != nil {
			return err
		}
		idB, err = dict.resolveOrAllocateInTxn(txn, "ns", "beta")
		if err != nil {
			return err
		}
		return dict.flushTxnCounters(txn)
	}))

	require.Equal(t, uint64(1), idA, "first allocation should be id 1")
	require.Equal(t, uint64(2), idB, "second allocation should be id 2")

	name, ok := dict.lookup("ns", idA)
	require.True(t, ok)
	require.Equal(t, "alpha", name)

	name, ok = dict.lookup("ns", idB)
	require.True(t, ok)
	require.Equal(t, "beta", name)

	// Resolve of an already-allocated name returns the same ID without
	// allocating a new one.
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		got, err := dict.resolveOrAllocateInTxn(txn, "ns", "alpha")
		if err != nil {
			return err
		}
		require.Equal(t, idA, got)
		return nil
	}))
}

// TestPropertyKeyDict_NamespaceIsolation ensures the same key name
// allocated under two namespaces gets independent IDs.
func TestPropertyKeyDict_NamespaceIsolation(t *testing.T) {
	eng := newTestEngine(t)
	dict := eng.propKeyDict

	var idNs1, idNs2 uint64
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		var err error
		idNs1, err = dict.resolveOrAllocateInTxn(txn, "tenant1", "name")
		if err != nil {
			return err
		}
		idNs2, err = dict.resolveOrAllocateInTxn(txn, "tenant2", "name")
		if err != nil {
			return err
		}
		return dict.flushTxnCounters(txn)
	}))

	// Both start fresh per namespace, so both see ID 1.
	require.Equal(t, uint64(1), idNs1)
	require.Equal(t, uint64(1), idNs2)

	// Lookups respect namespace boundaries.
	name1, ok1 := dict.lookup("tenant1", idNs1)
	require.True(t, ok1)
	require.Equal(t, "name", name1)

	// tenant1's id 2 was never allocated.
	_, ok := dict.lookup("tenant1", 2)
	require.False(t, ok)

	// tenant2 also has id 1 but it points to a different (logical) entry.
	name2, ok2 := dict.lookup("tenant2", idNs2)
	require.True(t, ok2)
	require.Equal(t, "name", name2)
}

// TestPropertyKeyDict_Lookup_UnknownNamespaceReturnsFalse covers the
// branch where the namespace has never been seen by the dictionary.
func TestPropertyKeyDict_Lookup_UnknownNamespaceReturnsFalse(t *testing.T) {
	eng := newTestEngine(t)
	_, ok := eng.propKeyDict.lookup("never-seen", 1)
	require.False(t, ok)
}

// TestPropertyKeyDict_HydrateFromBadger persists allocations, reopens
// the engine against the same data dir, and asserts the dictionary
// rebuilds in-memory state from disk.
func TestPropertyKeyDict_HydrateFromBadger(t *testing.T) {
	dir := t.TempDir()

	first, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
	require.NoError(t, err)

	var allocated []uint64
	require.NoError(t, first.withUpdate(func(txn *badger.Txn) error {
		for _, name := range []string{"productID", "productName", "sku"} {
			id, err := first.propKeyDict.resolveOrAllocateInTxn(txn, "shop", name)
			if err != nil {
				return err
			}
			allocated = append(allocated, id)
		}
		return first.propKeyDict.flushTxnCounters(txn)
	}))
	require.NoError(t, first.Close())

	// Reopen — V0 store with no bodies, so the upgrade gate doesn't
	// trigger and the runner advances straight to V2 silently.
	second, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { second.Close() })

	for i, name := range []string{"productID", "productName", "sku"} {
		got, ok := second.propKeyDict.lookup("shop", allocated[i])
		require.True(t, ok, "id %d should resolve after hydrate", allocated[i])
		require.Equal(t, name, got)
	}

	// The next allocation continues the counter rather than restarting.
	require.NoError(t, second.withUpdate(func(txn *badger.Txn) error {
		id, err := second.propKeyDict.resolveOrAllocateInTxn(txn, "shop", "newField")
		if err != nil {
			return err
		}
		require.Equal(t, allocated[len(allocated)-1]+1, id)
		return second.propKeyDict.flushTxnCounters(txn)
	}))
}

// TestPropertyKeyDict_HydrateRecoversFromMissingCounter exercises the
// fallback path in loadFromBadger: even if the counter key is missing
// (corruption / partial write), nextID is re-derived from the maximum
// id in the forward map.
func TestPropertyKeyDict_HydrateRecoversFromMissingCounter(t *testing.T) {
	dir := t.TempDir()
	first, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
	require.NoError(t, err)

	require.NoError(t, first.withUpdate(func(txn *badger.Txn) error {
		_, err := first.propKeyDict.resolveOrAllocateInTxn(txn, "ns", "a")
		require.NoError(t, err)
		_, err = first.propKeyDict.resolveOrAllocateInTxn(txn, "ns", "b")
		require.NoError(t, err)
		return first.propKeyDict.flushTxnCounters(txn)
	}))

	// Manually delete the counter key to simulate a partial write.
	require.NoError(t, first.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(propKeyCounterKey("ns"))
	}))
	require.NoError(t, first.Close())

	second, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { second.Close() })

	// nextID must be at least 2 — derived from the forward map's max id.
	counters := second.PropKeyDictCounters()
	require.GreaterOrEqual(t, counters["ns"], uint64(2))

	// The next allocation still produces a fresh ID (not one that
	// collides with "a" or "b").
	require.NoError(t, second.withUpdate(func(txn *badger.Txn) error {
		id, err := second.propKeyDict.resolveOrAllocateInTxn(txn, "ns", "c")
		if err != nil {
			return err
		}
		require.GreaterOrEqual(t, id, uint64(3))
		return second.propKeyDict.flushTxnCounters(txn)
	}))
}

// TestPropertyKeyDict_CounterFlushesOncePerNamespace exercises the
// txn-counter batching contract: N allocations in a single txn produce
// exactly one counter Set per dirty namespace at commit, regardless of
// allocation count. Verified by counting counter keys present after.
func TestPropertyKeyDict_CounterFlushesOncePerNamespace(t *testing.T) {
	eng := newTestEngine(t)
	dict := eng.propKeyDict

	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		for i := 0; i < 50; i++ {
			_, err := dict.resolveOrAllocateInTxn(txn, "nsA", "k"+string(rune('a'+i%26)))
			require.NoError(t, err)
		}
		for i := 0; i < 50; i++ {
			_, err := dict.resolveOrAllocateInTxn(txn, "nsB", "k"+string(rune('a'+i%26)))
			require.NoError(t, err)
		}
		return dict.flushTxnCounters(txn)
	}))

	// Exactly two counter keys should exist.
	require.NoError(t, eng.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{prefixPropKeyCounter}
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		count := 0
		for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
			count++
		}
		require.Equal(t, 2, count)
		return nil
	}))
}

// TestPropertyKeyDict_FlushOnEmptyTxnIsNoop covers the early-return
// branch of flushTxnCounters when no allocations were staged.
func TestPropertyKeyDict_FlushOnEmptyTxnIsNoop(t *testing.T) {
	eng := newTestEngine(t)
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		return eng.propKeyDict.flushTxnCounters(txn)
	}))
}

// TestPropertyKeyDict_DiscardTxnCounters covers the rollback branch:
// staged counters dropped on discard do not leak into a subsequent
// flush against a different txn.
func TestPropertyKeyDict_DiscardTxnCounters(t *testing.T) {
	eng := newTestEngine(t)
	dict := eng.propKeyDict

	// Open a write txn, stage allocations, then discard.
	txn := eng.db.NewTransaction(true)
	_, err := dict.resolveOrAllocateInTxn(txn, "ns", "x")
	require.NoError(t, err)
	dict.discardTxnCounters(txn)
	txn.Discard()

	// A fresh txn that flushes immediately should not write a counter
	// for "ns" — the staged state from the discarded txn is gone.
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		return dict.flushTxnCounters(txn)
	}))

	// Counter key for "ns" must not have been written.
	require.NoError(t, eng.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(propKeyCounterKey("ns"))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}

// TestPropertyKeyDict_ConcurrentAllocate races multiple goroutines
// allocating the same name and asserts they all converge on the same
// ID. The losing goroutine's allocation is discarded; the dictionary
// never returns two different IDs for the same name.
func TestPropertyKeyDict_ConcurrentAllocate(t *testing.T) {
	eng := newTestEngine(t)
	dict := eng.propKeyDict

	const goroutines = 20
	ids := make([]uint64, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := eng.withUpdate(func(txn *badger.Txn) error {
				id, err := dict.resolveOrAllocateInTxn(txn, "race", "key")
				if err != nil {
					return err
				}
				ids[i] = id
				return dict.flushTxnCounters(txn)
			})
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()

	for i := 1; i < goroutines; i++ {
		require.Equal(t, ids[0], ids[i], "all goroutines must converge on the same id")
	}
}

// TestPropertyKeyDict_ForwardKeyEncoding asserts the on-disk key
// encoding for the forward map round-trips through the decode helper.
// Length-prefixed namespace prevents collisions like ("ab", "c") vs.
// ("a", "bc").
func TestPropertyKeyDict_ForwardKeyEncoding(t *testing.T) {
	cases := []struct {
		namespace string
		name      string
	}{
		{"ab", "c"},
		{"a", "bc"},
		{"", "name"}, // empty namespace
		{"ns", ""},   // empty name
		{"long namespace string", "long property key name"},
	}

	for _, tc := range cases {
		key := propKeyForwardKey(tc.namespace, tc.name)
		ns, name, ok := decodePropKeyForwardKey(key)
		require.True(t, ok, "decodePropKeyForwardKey should accept a key it just encoded")
		require.Equal(t, tc.namespace, ns)
		require.Equal(t, tc.name, name)
	}

	// Distinct logical inputs produce distinct keys.
	key1 := propKeyForwardKey("ab", "c")
	key2 := propKeyForwardKey("a", "bc")
	require.NotEqual(t, key1, key2)
}

// TestPropertyKeyDict_DecodeForwardKey_Malformed exercises every
// rejection branch of decodePropKeyForwardKey.
func TestPropertyKeyDict_DecodeForwardKey_Malformed(t *testing.T) {
	t.Run("empty key", func(t *testing.T) {
		_, _, ok := decodePropKeyForwardKey(nil)
		require.False(t, ok)
	})

	t.Run("wrong prefix byte", func(t *testing.T) {
		_, _, ok := decodePropKeyForwardKey([]byte{0x00, 0x01, 'a'})
		require.False(t, ok)
	})

	t.Run("truncated namespace varint", func(t *testing.T) {
		// Only the prefix byte; nothing after it parses as a valid varint.
		_, _, ok := decodePropKeyForwardKey([]byte{prefixPropKeyForward})
		require.False(t, ok)
	})

	t.Run("namespace length exceeds remainder", func(t *testing.T) {
		key := []byte{prefixPropKeyForward}
		key = binary.AppendUvarint(key, 100) // claims 100-byte namespace
		key = append(key, "ns"...)
		_, _, ok := decodePropKeyForwardKey(key)
		require.False(t, ok)
	})

	t.Run("truncated name varint", func(t *testing.T) {
		key := []byte{prefixPropKeyForward}
		key = binary.AppendUvarint(key, 2)
		key = append(key, "ns"...)
		_, _, ok := decodePropKeyForwardKey(key)
		require.False(t, ok)
	})

	t.Run("name length exceeds remainder", func(t *testing.T) {
		key := []byte{prefixPropKeyForward}
		key = binary.AppendUvarint(key, 2)
		key = append(key, "ns"...)
		key = binary.AppendUvarint(key, 100) // claims 100-byte name
		key = append(key, "x"...)
		_, _, ok := decodePropKeyForwardKey(key)
		require.False(t, ok)
	})

	t.Run("trailing garbage past name", func(t *testing.T) {
		key := []byte{prefixPropKeyForward}
		key = binary.AppendUvarint(key, 2)
		key = append(key, "ns"...)
		key = binary.AppendUvarint(key, 1)
		key = append(key, "k"...)
		key = append(key, "extra"...)
		_, _, ok := decodePropKeyForwardKey(key)
		require.False(t, ok)
	})
}

// TestPropertyKeyDict_DecodeCounterKey_Malformed covers
// decodePropKeyCounterKey rejection branches.
func TestPropertyKeyDict_DecodeCounterKey_Malformed(t *testing.T) {
	t.Run("empty key", func(t *testing.T) {
		_, ok := decodePropKeyCounterKey(nil)
		require.False(t, ok)
	})

	t.Run("wrong prefix", func(t *testing.T) {
		_, ok := decodePropKeyCounterKey([]byte{0x00, 0x02, 'n', 's'})
		require.False(t, ok)
	})

	t.Run("truncated varint", func(t *testing.T) {
		_, ok := decodePropKeyCounterKey([]byte{prefixPropKeyCounter})
		require.False(t, ok)
	})

	t.Run("namespace length mismatches remainder", func(t *testing.T) {
		key := []byte{prefixPropKeyCounter}
		key = binary.AppendUvarint(key, 5)
		key = append(key, "ab"...) // claims 5 bytes, only 2 present
		_, ok := decodePropKeyCounterKey(key)
		require.False(t, ok)
	})

	t.Run("trailing garbage past namespace", func(t *testing.T) {
		key := []byte{prefixPropKeyCounter}
		key = binary.AppendUvarint(key, 2)
		key = append(key, "ns"...)
		key = append(key, "extra"...)
		_, ok := decodePropKeyCounterKey(key)
		require.False(t, ok)
	})

	t.Run("round-trip valid key", func(t *testing.T) {
		key := propKeyCounterKey("ns-name")
		ns, ok := decodePropKeyCounterKey(key)
		require.True(t, ok)
		require.Equal(t, "ns-name", ns)
	})
}

// TestPropKeyDictCounters_NilEngine covers the nil-safe branch on the
// observability accessor.
func TestPropKeyDictCounters_NilEngine(t *testing.T) {
	var b *BadgerEngine
	require.Nil(t, b.PropKeyDictCounters())

	emptyEngine := &BadgerEngine{}
	require.Nil(t, emptyEngine.PropKeyDictCounters())
}

// TestPropertyKeyDict_HydrateSkipsMalformedKeys covers the
// loadFromBadger "skip malformed forward key" branch and the
// "malformed counter key" branch. We write garbage keys directly under
// the dict prefixes, then reopen the engine and assert it survives
// rather than refusing to start.
func TestPropertyKeyDict_HydrateSkipsMalformedKeys(t *testing.T) {
	dir := t.TempDir()
	first, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
	require.NoError(t, err)

	// One real entry so reopen exercises the loop body.
	require.NoError(t, first.withUpdate(func(txn *badger.Txn) error {
		_, err := first.propKeyDict.resolveOrAllocateInTxn(txn, "ns", "real")
		require.NoError(t, err)
		return first.propKeyDict.flushTxnCounters(txn)
	}))

	// Inject malformed keys under both prefixes.
	require.NoError(t, first.db.Update(func(txn *badger.Txn) error {
		// Forward prefix + just one byte that doesn't decode as a
		// length-prefixed namespace.
		if err := txn.Set([]byte{prefixPropKeyForward, 0x80}, []byte{0x01}); err != nil {
			return err
		}
		// Counter prefix + garbage tail (varint claims 100-byte ns).
		bad := []byte{prefixPropKeyCounter, 100, 'a'}
		return txn.Set(bad, []byte{0x01})
	}))
	require.NoError(t, first.Close())

	second, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
	require.NoError(t, err, "engine must skip malformed dict entries rather than refuse to open")
	t.Cleanup(func() { second.Close() })

	// The real entry survived hydration.
	got, ok := second.propKeyDict.lookup("ns", 1)
	require.True(t, ok)
	require.Equal(t, "real", got)
}

// TestPropertyKeyDict_HydrateRejectsMalformedForwardValue covers the
// inner "value malformed" error branch in loadFromBadger.
func TestPropertyKeyDict_HydrateRejectsMalformedForwardValue(t *testing.T) {
	dir := t.TempDir()
	first, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
	require.NoError(t, err)

	// Inject a valid forward key with a zero-byte value (varint of 0
	// length is a parse error per binary.Uvarint).
	key := propKeyForwardKey("ns", "k")
	require.NoError(t, first.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, []byte{})
	}))
	require.NoError(t, first.Close())

	_, err = NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
	require.Error(t, err)
	require.Contains(t, err.Error(), "property key forward value malformed")
}

// TestPropertyKeyDict_HydrateRejectsMalformedCounterValue covers the
// counter-value parse error branch.
func TestPropertyKeyDict_HydrateRejectsMalformedCounterValue(t *testing.T) {
	dir := t.TempDir()
	first, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
	require.NoError(t, err)

	require.NoError(t, first.db.Update(func(txn *badger.Txn) error {
		return txn.Set(propKeyCounterKey("ns"), []byte{})
	}))
	require.NoError(t, first.Close())

	_, err = NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
	require.Error(t, err)
	require.Contains(t, err.Error(), "property key counter malformed")
}

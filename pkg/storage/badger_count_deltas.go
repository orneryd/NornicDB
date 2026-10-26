package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/util"
)

// errMalformedDerivedCount marks a persisted base count that cannot be
// decoded. Readers repair the pair from authoritative state and retry, as the
// pre-delta read-modify-write path did (PR #1043 item #2).
var errMalformedDerivedCount = errors.New("derived count value malformed")

// Derived-count delta keys (PR #1043 write-path item #2).
//
// Label and edge-type counts are derived data: instead of one
// read-modify-write count key per label under a global mutex (which was 99%
// of durable-path lock wait), every committed delta is appended as its own
// unique key in the same transaction as the write that justifies it. Blind
// appends never conflict, so concurrent commits to any labels and types
// proceed without a count mutex. Reads sum the base count plus the deltas
// visible in their snapshot; folds collapse deltas back into the base.
//
// Delta keys are unique via an engine-local monotonic suffix. Folding at
// engine open removes every persisted delta before new writes, so suffix
// reuse across restarts cannot collide.

const countDeltaFoldThreshold = 64

// countDeltaKey builds the derived-count delta key for one namespace and
// name (a label or an edge type).
func countDeltaKey(kind byte, namespace, name string, suffix uint64) []byte {
	key := make([]byte, 0, util.SafePreallocSum(5, len(namespace), len(name)))
	key = append(key, prefixMVCCMeta, kind)
	key = append(key, namespace...)
	key = append(key, 0)
	key = append(key, name...)
	key = append(key, 0)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], suffix)
	return append(key, buf[:]...)
}

// countDeltaPrefixFor returns the iteration prefix of every delta of one
// namespace/name pair.
func countDeltaPrefixFor(kind byte, namespace, name string) []byte {
	key := make([]byte, 0, util.SafePreallocSum(3, len(namespace), len(name)))
	key = append(key, prefixMVCCMeta, kind)
	key = append(key, namespace...)
	key = append(key, 0)
	key = append(key, name...)
	return append(key, 0)
}

// countDeltaAllPrefix returns the iteration prefix of every delta of one
// kind across all namespaces and names.
func countDeltaAllPrefix(kind byte) []byte {
	return []byte{prefixMVCCMeta, kind}
}

// countDeltaNamespacePrefix returns the iteration prefix of every delta of
// one kind in one namespace (the trailing separator excludes namespaces that
// are string prefixes of this one).
func countDeltaNamespacePrefix(kind byte, namespace string) []byte {
	key := make([]byte, 0, util.SafePreallocSum(3, len(namespace)))
	key = append(key, prefixMVCCMeta, kind)
	key = append(key, namespace...)
	return append(key, 0)
}

// countDeltaKindLabel / countDeltaKindEdgeType are the second bytes of the
// delta keys, kept distinct from the base count keys.
const (
	countDeltaKindLabel    = prefixMVCCMetaLabelCountDelta
	countDeltaKindEdgeType = prefixMVCCMetaEdgeTypeCountDelta
)

func encodeCountDelta(delta int64) []byte {
	return encodeDerivedCount(delta)
}

func decodeCountDelta(val []byte) (int64, error) {
	return decodeDerivedCount(val)
}

// sumCountDeltasInTxn sums the persisted deltas of one namespace/name pair
// visible in txn.
func sumCountDeltasInTxn(txn *badger.Txn, kind byte, namespace, name string) (int64, int, error) {
	prefix := countDeltaPrefixFor(kind, namespace, name)
	var total int64
	var seen int
	it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
	defer it.Close()
	for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
		seen++
		if err := it.Item().Value(func(val []byte) error {
			delta, decodeErr := decodeCountDelta(val)
			if decodeErr != nil {
				return decodeErr
			}
			total += delta
			return nil
		}); err != nil {
			return 0, seen, err
		}
	}
	return total, seen, nil
}

// sumCountDeltasByNameInTxn sums the persisted deltas of one kind for one
// name (a label or an edge type) across all namespaces visible in txn.
func sumCountDeltasByNameInTxn(txn *badger.Txn, kind byte, name string) (int64, error) {
	var total int64
	prefix := countDeltaAllPrefix(kind)
	needle := []byte(name)
	it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
	defer it.Close()
	for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().KeyCopy(nil)
		sep := bytesIndexByte(key[2:], 0)
		if sep < 0 {
			continue
		}
		nameSep := bytesIndexByte(key[2+sep+1:], 0)
		if nameSep < 0 {
			continue
		}
		if !bytes.Equal(key[2+sep+1:2+sep+1+nameSep], needle) {
			continue
		}
		if err := it.Item().Value(func(val []byte) error {
			delta, decodeErr := decodeCountDelta(val)
			if decodeErr != nil {
				return decodeErr
			}
			total += delta
			return nil
		}); err != nil {
			return 0, err
		}
	}
	return total, nil
}

// hasAnyCountDeltasInDB reports whether any persisted delta of one kind
// exists. It lets open- and close-time folding skip opening an update
// transaction (and the tail hook it fires) when there is nothing to fold.
func (b *BadgerEngine) hasAnyCountDeltasInDB(kind byte) (bool, error) {
	var found bool
	err := b.db.View(func(txn *badger.Txn) error {
		prefix := countDeltaAllPrefix(kind)
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		it.Rewind()
		if it.ValidForPrefix(prefix) {
			found = true
		}
		return nil
	})
	return found, err
}

// countDeltaFoldChunk bounds how many delta keys one fold transaction may
// touch, so a large backlog (a single-statement corpus replay leaves one
// delta key per commit, hundreds of thousands for one label or type) never
// overflows one Badger batch (ErrTxnTooBig).
const countDeltaFoldChunk = 10000

// countDeltaKeyParts splits a delta key of one kind into its namespace and
// name. ok is false for a malformed key.
func countDeltaKeyParts(key []byte) (namespace, name string, ok bool) {
	if len(key) < 3 {
		return "", "", false
	}
	sep := bytesIndexByte(key[2:], 0)
	if sep < 0 {
		return "", "", false
	}
	nameSep := bytesIndexByte(key[2+sep+1:], 0)
	if nameSep < 0 {
		return "", "", false
	}
	return string(key[2 : 2+sep]), string(key[2+sep+1 : 2+sep+1+nameSep]), true
}

// foldCountDeltaKeys folds one bounded set of delta keys into their base keys
// in a single transaction run through run. Each chunk is atomic: it sums and
// deletes exactly the keys it was handed and adds the chunk sums to the base
// keys, so a crash between chunks leaves the remaining deltas to fold at the
// next open (or the next read past the fold threshold). Keys are collected
// up front, so a delta appended by a concurrent commit after collection is
// never deleted without being summed.
func (b *BadgerEngine) foldCountDeltaKeys(run func(func(*badger.Txn) error) error, baseKey func(string, string) []byte, keys [][]byte) error {
	sums := make(map[namespaceLabel]int64)
	return run(func(txn *badger.Txn) error {
		for _, key := range keys {
			ns, name, ok := countDeltaKeyParts(key)
			if !ok {
				continue
			}
			item, err := txn.Get(key)
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return err
			}
			var delta int64
			if err := item.Value(func(val []byte) error {
				var decodeErr error
				delta, decodeErr = decodeCountDelta(val)
				return decodeErr
			}); err != nil {
				return err
			}
			sums[namespaceLabel{namespace: ns, label: name}] += delta
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		for p, sum := range sums {
			if sum == 0 {
				continue
			}
			base, err := readDerivedCountInTxn(txn, baseKey(p.namespace, p.label))
			if err != nil {
				return err
			}
			next := base + sum
			if next < 0 {
				return fmt.Errorf("derived count underflow for %s:%s", p.namespace, p.label)
			}
			if next == 0 {
				if err := txn.Delete(baseKey(p.namespace, p.label)); err != nil {
					return err
				}
			} else if err := txn.Set(baseKey(p.namespace, p.label), encodeDerivedCount(next)); err != nil {
				return err
			}
		}
		return nil
	})
}

// foldCountDeltaKeysChunked folds keys in bounded transactions.
func (b *BadgerEngine) foldCountDeltaKeysChunked(run func(func(*badger.Txn) error) error, baseKey func(string, string) []byte, keys [][]byte) error {
	for start := 0; start < len(keys); start += countDeltaFoldChunk {
		end := start + countDeltaFoldChunk
		if end > len(keys) {
			end = len(keys)
		}
		if err := b.foldCountDeltaKeys(run, baseKey, keys[start:end]); err != nil {
			return err
		}
	}
	return nil
}

// foldCountDeltasOn collapses the persisted deltas of one namespace/name pair
// (or every delta of one kind when namespace is empty) in bounded
// transactions. view and run supply the read and write transaction runners so
// the same kernel serves engine paths (b.withView / b.withUpdate) and the
// post-close raw-handle path.
func (b *BadgerEngine) foldCountDeltasOn(view func(func(*badger.Txn) error) error, run func(func(*badger.Txn) error) error, kind byte, baseKey func(string, string) []byte, namespace, name string) error {
	var keys [][]byte
	if err := view(func(txn *badger.Txn) error {
		var prefix []byte
		if namespace == "" {
			prefix = countDeltaAllPrefix(kind)
		} else {
			prefix = countDeltaPrefixFor(kind, namespace, name)
		}
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			keys = append(keys, it.Item().KeyCopy(nil))
		}
		return nil
	}); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	return b.foldCountDeltaKeysChunked(run, baseKey, keys)
}

// foldCountDeltasChunked collapses the persisted deltas of one
// namespace/name pair in bounded transactions. It replaces the single-
// transaction fold for reads past the fold threshold, where a single pair
// may hold hundreds of thousands of deltas.
func (b *BadgerEngine) foldCountDeltasChunked(kind byte, baseKey func(string, string) []byte, namespace, name string) error {
	return b.foldCountDeltasOn(b.withView, func(fn func(*badger.Txn) error) error { return b.withMetadataUpdate(fn) }, kind, baseKey, namespace, name)
}

// foldRestoredCountDeltas folds every derived-count delta a restore loaded,
// past the restore's read hold.
func (b *BadgerEngine) foldRestoredCountDeltas(db *managedBadgerDB) error {
	b.labelCountWriteMu.Lock()
	err := b.foldCountDeltasOn(db.viewHeld, db.updateHeld, countDeltaKindLabel, labelCountKey, "", "")
	b.labelCountWriteMu.Unlock()
	if err != nil {
		return err
	}
	b.edgeTypeCountWriteMu.Lock()
	defer b.edgeTypeCountWriteMu.Unlock()
	return b.foldCountDeltasOn(db.viewHeld, db.updateHeld, countDeltaKindEdgeType, edgeTypeCountKey, "", "")
}

// readDerivedCountInTxn reads one persisted base count (0 when absent).
func readDerivedCountInTxn(txn *badger.Txn, key []byte) (int64, error) {
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var count int64
	if err := item.Value(func(val []byte) error {
		decoded, decodeErr := decodeDerivedCount(val)
		if decodeErr != nil {
			return fmt.Errorf("%w: %v", errMalformedDerivedCount, decodeErr)
		}
		count = decoded
		return nil
	}); err != nil {
		return 0, err
	}
	return count, nil
}

func bytesIndexByte(data []byte, needle byte) int {
	for i, c := range data {
		if c == needle {
			return i
		}
	}
	return -1
}

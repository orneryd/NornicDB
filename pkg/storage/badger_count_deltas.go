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

// foldCountDeltasInTxn collapses the persisted deltas of one namespace/name
// pair into the base key. The caller must hold the owning count mutex
// (labelCountWriteMu or edgeTypeCountWriteMu) so folds and rebuilds never
// rewrite the base key concurrently. An underflowing sum is an error, as the
// read-modify-write adjuster used to report it.
func (b *BadgerEngine) foldCountDeltasInTxn(txn *badger.Txn, kind byte, baseKey func(string, string) []byte, namespace, name string) error {
	sum, _, err := sumCountDeltasInTxn(txn, kind, namespace, name)
	if err != nil {
		return err
	}
	if sum == 0 {
		return nil
	}
	base, err := readDerivedCountInTxn(txn, baseKey(namespace, name))
	if err != nil {
		return err
	}
	next := base + sum
	if next < 0 {
		return fmt.Errorf("derived count underflow for %s:%s", namespace, name)
	}
	prefix := countDeltaPrefixFor(kind, namespace, name)
	it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
	for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().KeyCopy(nil)
		if err := txn.Delete(key); err != nil {
			it.Close()
			return err
		}
	}
	it.Close()
	if next == 0 {
		return txn.Delete(baseKey(namespace, name))
	}
	return txn.Set(baseKey(namespace, name), encodeDerivedCount(next))
}

// foldAllCountDeltasInTxn collapses every persisted delta of one kind. It is
// used at engine open so no delta survives a restart.
func (b *BadgerEngine) foldAllCountDeltasInTxn(txn *badger.Txn, kind byte, baseKey func(string, string) []byte) error {
	type pair struct {
		namespace string
		name      string
	}
	var pairs []pair
	prefix := countDeltaAllPrefix(kind)
	it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
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
		pairs = append(pairs, pair{
			namespace: string(key[2 : 2+sep]),
			name:      string(key[2+sep+1 : 2+sep+1+nameSep]),
		})
	}
	it.Close()
	seen := make(map[pair]struct{}, len(pairs))
	for _, p := range pairs {
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		if err := b.foldCountDeltasInTxn(txn, kind, baseKey, p.namespace, p.name); err != nil {
			return err
		}
	}
	return nil
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

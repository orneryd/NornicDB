package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/util"
)

var labelCountReadyKey = []byte{prefixMVCCMeta, prefixMVCCMetaLabelCountReady}

func labelCountKey(namespace, label string) []byte {
	key := make([]byte, 0, util.SafePreallocSum(3, len(namespace), len(label)))
	key = append(key, prefixMVCCMeta, prefixMVCCMetaLabelCount)
	key = append(key, namespace...)
	key = append(key, 0)
	key = append(key, label...)
	return key
}

func labelCountPrefix() []byte {
	return []byte{prefixMVCCMeta, prefixMVCCMetaLabelCount}
}

func labelCountNamespacePrefix(namespace string) []byte {
	key := make([]byte, 0, util.SafePreallocSum(3, len(namespace)))
	key = append(key, prefixMVCCMeta, prefixMVCCMetaLabelCount)
	key = append(key, namespace...)
	return append(key, 0)
}

type namespaceLabel struct {
	namespace string
	label     string
}

type storedNodeLabels struct {
	Labels []string
}

func decodeStoredNodeLabels(data []byte) ([]string, error) {
	envelope, failure := splitNodeEnvelope(data)
	// Preserve the label reader's release diagnostics and metadata decoder.
	// Only the shared owner interprets format/length framing.
	switch failure {
	case nodeEnvelopeEmpty, nodeEnvelopeWrongFormat:
		return nil, fmt.Errorf("node body has unexpected format")
	case nodeEnvelopeShortVarint, nodeEnvelopeOverflowVarint:
		return nil, fmt.Errorf("node body: malformed properties length varint")
	case nodeEnvelopeShortProperties:
		return nil, fmt.Errorf("node body: properties payload truncated")
	}

	var node storedNodeLabels
	if err := decodeValue(envelope.metadata, &node); err != nil {
		return nil, err
	}
	return node.Labels, nil
}

func (b *BadgerEngine) collectNodeLabelCountsByPrefix(keyPrefix []byte) (int64, map[namespaceLabel]int64, error) {
	counts := make(map[namespaceLabel]int64)
	var nodes int64
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions(keyPrefix))
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(keyPrefix); it.Next() {
			nodes++
			key := it.Item().Key()
			namespace, _, ok := ParseDatabasePrefix(string(key[1:]))
			if !ok {
				continue
			}
			if err := it.Item().Value(func(value []byte) error {
				labels, decodeErr := decodeStoredNodeLabels(value)
				if decodeErr != nil {
					return decodeErr
				}
				for index, label := range labels {
					if label == "" {
						continue
					}
					duplicate := false
					for previous := 0; previous < index; previous++ {
						if labels[previous] == label {
							duplicate = true
							break
						}
					}
					if !duplicate {
						counts[namespaceLabel{namespace: namespace, label: label}]++
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return nodes, counts, err
}

func (b *BadgerEngine) decrementLabelCounts(ctx context.Context, counts map[namespaceLabel]int64) error {
	return applyCountDeltas(ctx, b, counts, func(txn *badger.Txn, key namespaceLabel, count int64) error {
		return b.adjustLabelCountInTxn(txn, key.namespace, key.label, -count)
	})
}

// encodeDerivedCount / decodeDerivedCount are the shared 8-byte big-endian
// codec for persisted derived counts (label counts and edge-type counts).
func encodeDerivedCount(count int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(count))
	return buf
}

func decodeDerivedCount(val []byte) (int64, error) {
	if len(val) != 8 {
		return 0, fmt.Errorf("decode derived count: expected 8 bytes, got %d", len(val))
	}
	return int64(binary.BigEndian.Uint64(val)), nil
}

// applyCountDeltas applies one batch of derived count deltas in a single
// metadata update, so the batch's count changes commit together. ctx is the
// batch's owning phase: a lifecycle drop passes its lease, an accepted replay
// or publication its already-detached completion lifetime.
func applyCountDeltas[K comparable](ctx context.Context, b *BadgerEngine, deltas map[K]int64, adjust func(txn *badger.Txn, key K, delta int64) error) error {
	return b.withUpdate(ctx, updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
		for key, delta := range deltas {
			if err := adjust(txn, key, delta); err != nil {
				return err
			}
		}
		return nil
	})
}

func uniqueLabels(labels []string) []string {
	if len(labels) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(labels))
	unique := make([]string, 0, len(labels))
	for _, label := range labels {
		if label == "" {
			continue
		}
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		unique = append(unique, label)
	}
	return unique
}

func labelSet(labels []string) map[string]struct{} {
	set := make(map[string]struct{}, len(labels))
	for _, label := range uniqueLabels(labels) {
		set[label] = struct{}{}
	}
	return set
}

func (b *BadgerEngine) readLabelCountInTxn(ctx context.Context, txn *badger.Txn, namespace, label string) (int64, int, error) {
	base, err := readDerivedCountInTxn(txn, labelCountKey(namespace, label))
	if err != nil {
		return 0, 0, err
	}
	deltaSum, seen, err := sumCountDeltasInTxn(ctx, txn, countDeltaKindLabel, namespace, label)
	if err != nil {
		return 0, seen, err
	}
	if base+deltaSum < 0 {
		return 0, seen, fmt.Errorf("label count underflow for %s:%s", namespace, label)
	}
	return base + deltaSum, seen, nil
}

// adjustLabelCountInTxn appends a signed label-count delta as its own unique
// key inside the caller's transaction, atomic with the node write that
// justifies it. Blind appends never conflict, so no count mutex is needed on
// the commit path.
func (b *BadgerEngine) adjustLabelCountInTxn(txn *badger.Txn, namespace, label string, delta int64) error {
	if delta == 0 || namespace == "" || label == "" {
		return nil
	}
	suffix := b.countDeltaSeq.Add(1)
	key := countDeltaKey(countDeltaKindLabel, namespace, label, suffix)
	if err := txn.Set(key, encodeCountDelta(delta)); err != nil {
		return err
	}
	b.recordCountDeltaAppend()
	return nil
}

func (b *BadgerEngine) adjustNodeLabelCountsInTxn(txn *badger.Txn, namespace string, oldLabels, newLabels []string) error {
	for label, delta := range nodeLabelCountDeltas(oldLabels, newLabels) {
		if err := b.adjustLabelCountInTxn(txn, namespace, label, delta); err != nil {
			return err
		}
	}
	return nil
}

func nodeLabelCountDeltas(oldLabels, newLabels []string) map[string]int64 {
	oldSet := labelSet(oldLabels)
	newSet := labelSet(newLabels)
	deltas := make(map[string]int64, len(oldSet)+len(newSet))
	for label := range newSet {
		if _, ok := oldSet[label]; ok {
			continue
		}
		deltas[label] = 1
	}
	for label := range oldSet {
		if _, ok := newSet[label]; ok {
			continue
		}
		deltas[label] = -1
	}
	return deltas
}

// applyLabelCountDeltasLocked persists transaction-local derived count deltas.
// The scalar tier appends unique delta keys (blind writes), so no count mutex
// is required: concurrent commits to the same label cannot lose updates, and
// folds only rewrite keys present in their own snapshot.
func (b *BadgerEngine) applyLabelCountDeltasLocked(ctx context.Context, deltas map[namespaceLabel]int64) error {
	if len(deltas) == 0 {
		return nil
	}
	return applyCountDeltas(ctx, b, deltas, func(txn *badger.Txn, key namespaceLabel, delta int64) error {
		return b.adjustLabelCountInTxn(txn, key.namespace, key.label, delta)
	})
}

func (b *BadgerEngine) rebuildAuthoritativeLabelCountsLocked(ctx context.Context) error {
	counts, err := b.collectAuthoritativeLabelCounts()
	if err != nil {
		return err
	}
	return b.rebuildLabelCounts(ctx, counts)
}

// NodeCountByLabelInNamespace reads the stored namespace count and pending
// deltas under ctx, including any required derived-count repair.
func (b *BadgerEngine) NodeCountByLabelInNamespace(ctx context.Context, namespace, label string) (int64, error) {
	return b.namedCount(ctx, countDeltaKindLabel, namespace, label, true)
}

// repairNamedCountLocked rebuilds a label or relationship-type base count and
// deletes its unfolded deltas atomically. The caller holds namedCountMutex(kind).
// Author: storage-format-owners lane (Codex), 1 November 2026.
func (b *BadgerEngine) repairNamedCountLocked(ctx context.Context, kind byte, namespace, name string, baseKey func(string, string) []byte) error {
	return b.withUpdate(ctx, updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
		prefix := countDeltaPrefixFor(kind, namespace, name)
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		var cancel bulkReadCancellation
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			if err := cancel.check(ctx); err != nil {
				it.Close()
				return err
			}
			if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
				it.Close()
				return err
			}
		}
		it.Close()
		count, err := b.countNamedIndexEntriesInNamespace(ctx, kind, namespace, name)
		if err != nil {
			return err
		}
		key := baseKey(namespace, name)
		if count == 0 {
			return txn.Delete(key)
		}
		return txn.Set(key, encodeDerivedCount(count))
	})
}

// countNamedIndexEntriesInNamespace counts the selected index family using
// its numeric-ID dictionary. Entries without a valid identity or namespace
// retain the existing repair behavior and do not contribute to the count.
func (b *BadgerEngine) countNamedIndexEntriesInNamespace(ctx context.Context, kind byte, namespace, name string) (int64, error) {
	var count int64
	err := b.db.View(func(txn *badger.Txn) error {
		var prefix []byte
		if kind == countDeltaKindEdgeType {
			prefix = edgeTypeIndexPrefix(name)
		} else {
			prefix = labelIndexPrefix(name)
		}
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		var cancel bulkReadCancellation
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			key := it.Item().KeyCopy(nil)
			var id string
			if kind == countDeltaKindEdgeType {
				num, ok := extractEdgeNumIDFromEdgeTypeKey(key)
				if !ok {
					continue
				}
				edgeID, found := b.idDict.lookupEdgeIDByNum(num)
				if !found {
					continue
				}
				id = string(edgeID)
			} else {
				num, ok := extractNodeNumIDFromLabelIndex(key, len(name))
				if !ok {
					continue
				}
				nodeID, found := b.idDict.lookupNodeIDByNum(num)
				if !found {
					continue
				}
				id = string(nodeID)
			}
			ns, _, ok := ParseDatabasePrefix(id)
			if ok && ns == namespace {
				count++
			}
		}
		return nil
	})
	return count, err
}

// foldLabelCountDeltas collapses the persisted deltas of one label into its
// base count. It is triggered by reads past the fold threshold and by engine
// open; the returned error is discarded on the read-triggered path (the read
// already returned a correct sum).
func (b *BadgerEngine) foldLabelCountDeltas(ctx context.Context, namespace, label string) error {
	return b.foldNamedCountDeltas(ctx, countDeltaKindLabel, namespace, label, false)
}

// foldAllLabelCountDeltas collapses every persisted label-count delta at
// engine open, before any new write can reuse a suffix. When nothing is
// pending it does not open an update transaction, so a clean reopen does no
// open-time write.
func (b *BadgerEngine) foldAllLabelCountDeltas(ctx context.Context) error {
	return b.foldNamedCountDeltas(ctx, countDeltaKindLabel, "", "", true)
}

// NodeCountByLabel reads the label cardinality across namespaces under ctx.
func (b *BadgerEngine) NodeCountByLabel(ctx context.Context, label string) (int64, error) {
	return b.namedCount(ctx, countDeltaKindLabel, "", label, false)
}

func (b *BadgerEngine) labelCountReady() (bool, error) {
	var ready bool
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(labelCountReadyKey)
		if err == nil {
			ready = true
			return nil
		}
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
	return ready, err
}

func (b *BadgerEngine) loadPersistedLabelCounts() (map[string]int64, error) {
	persisted := make(map[string]int64)
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions(labelCountPrefix()))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			candidate, ok := namedCountKeyParts(it.Item().Key())
			if !ok {
				continue
			}
			namespace, label := candidate.namespace, candidate.label
			if err := it.Item().Value(func(val []byte) error {
				count, decodeErr := decodeDerivedCount(val)
				if decodeErr != nil {
					return decodeErr
				}
				persisted[namespace+"\x00"+label] = count
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return persisted, err
}

func (b *BadgerEngine) collectAuthoritativeLabelCounts() (map[string]int64, error) {
	counts := make(map[string]int64)
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions([]byte{prefixNode}))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().KeyCopy(nil)
			if len(key) <= 1 {
				continue
			}
			nodeID := NodeID(key[1:])
			namespace, _, ok := ParseDatabasePrefix(string(nodeID))
			if !ok {
				continue
			}
			if err := it.Item().Value(func(val []byte) error {
				node, decodeErr := b.decodeNodeProjected(txn, namespace, val, nil)
				if decodeErr != nil {
					return fmt.Errorf("decode node %q for label counts: %w", nodeID, decodeErr)
				}
				for _, label := range uniqueLabels(node.Labels) {
					counts[namespace+"\x00"+label]++
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return counts, err
}

func sameLabelCounts(actual, persisted map[string]int64) bool {
	if len(actual) != len(persisted) {
		return false
	}
	for key, actualCount := range actual {
		if persisted[key] != actualCount {
			return false
		}
	}
	return true
}

func (b *BadgerEngine) rebuildLabelCounts(ctx context.Context, counts map[string]int64) error {
	return b.withUpdate(ctx, updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
		prefix := labelCountPrefix()
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			if err := txn.Delete(key); err != nil {
				it.Close()
				return fmt.Errorf("clear label count %q before rebuild: %w", key, err)
			}
		}
		it.Close()
		for composite, count := range counts {
			parts := strings.SplitN(composite, "\x00", 2)
			if len(parts) != 2 {
				continue
			}
			if err := txn.Set(labelCountKey(parts[0], parts[1]), encodeDerivedCount(count)); err != nil {
				return err
			}
		}
		return txn.Set(labelCountReadyKey, []byte{1})
	})
}

func (b *BadgerEngine) ensureLabelCounts(ctx context.Context) error {
	if err := b.ensureOpen(false); err != nil {
		return err
	}
	// Fold any deltas a crash left behind first, so the persisted form is
	// base keys again before verification compares them.
	if err := b.foldAllLabelCountDeltas(ctx); err != nil {
		return err
	}
	// Verification and rebuild rewrite the base keys: they hold the count
	// lock, as every rewrite of them does (rebuildAuthoritativeLabelCountsLocked).
	b.labelCountWriteMu.Lock()
	defer b.labelCountWriteMu.Unlock()
	ready, err := b.labelCountReady()
	if err != nil {
		return err
	}
	actual, err := b.collectAuthoritativeLabelCounts()
	if err != nil {
		return err
	}
	persisted, err := b.loadPersistedLabelCounts()
	if err != nil {
		return err
	}
	if ready && sameLabelCounts(actual, persisted) {
		return nil
	}
	return b.rebuildLabelCounts(ctx, actual)
}

func (tx *BadgerTransaction) bufferAdjustLabelCount(namespace, label string, delta int64) {
	if delta == 0 || namespace == "" || label == "" {
		return
	}
	key := namespaceLabel{namespace: namespace, label: label}
	tx.pendingLabelCountDeltas[key] += delta
	if tx.pendingLabelCountDeltas[key] == 0 {
		delete(tx.pendingLabelCountDeltas, key)
	}
}

func (tx *BadgerTransaction) bufferAdjustNodeLabelCounts(namespace string, oldLabels, newLabels []string) {
	for label, delta := range nodeLabelCountDeltas(oldLabels, newLabels) {
		tx.bufferAdjustLabelCount(namespace, label, delta)
	}
}

// namedCountMutex binds derived-count reads, repairs and folds to the same
// family lock used by writers. The selected policy is local to each operation.
func (b *BadgerEngine) namedCountMutex(kind byte) *trackedRWMutex {
	if kind == countDeltaKindEdgeType {
		return &b.edgeTypeCountWriteMu
	}
	return &b.labelCountWriteMu
}

// readNamedCountInTxn reads one persisted base count of one namespace and its
// unfolded deltas: the canonical per-(namespace, name) derived-count read.
func (b *BadgerEngine) readNamedCountInTxn(ctx context.Context, txn *badger.Txn, kind byte, namespace, name string) (int64, error) {
	if kind == countDeltaKindEdgeType {
		count, _, err := b.readEdgeTypeCountInTxn(ctx, txn, namespace, name)
		return count, err
	}
	count, _, err := b.readLabelCountInTxn(ctx, txn, namespace, name)
	return count, err
}

// readNamedCountSnapshot reads the persisted base and its unfolded deltas
// while its caller holds the count-family lock.
func (b *BadgerEngine) readNamedCountSnapshot(ctx context.Context, kind byte, namespace, name string, scoped bool) (int64, error) {
	var count int64
	err := b.withView(func(txn *badger.Txn) error {
		var err error
		if scoped {
			count, err = b.readNamedCountInTxn(ctx, txn, kind, namespace, name)
		} else {
			count, err = b.sumNamedCountsInTxn(ctx, txn, kind, []string{name})
		}
		return err
	})
	return count, err
}

// namedCountPrefix returns the base-count keyspace of one derived-count family.
func namedCountPrefix(kind byte) []byte {
	if kind == countDeltaKindEdgeType {
		return edgeTypeCountPrefix()
	}
	return labelCountPrefix()
}

// namedCountKeyParts splits a persisted base-count key into its
// (namespace, name) pair.
func namedCountKeyParts(key []byte) (namespaceLabel, bool) {
	rest := key[len(labelCountPrefix()):]
	separator := bytes.IndexByte(rest, 0)
	if separator < 0 {
		return namespaceLabel{}, false
	}
	return namespaceLabel{namespace: string(rest[:separator]), label: string(rest[separator+1:])}, true
}

// namedCountNameFilter accepts every name when names is empty, so an untyped
// read sums the whole family instead of one name.
func namedCountNameFilter(names []string) func(namespaceLabel) bool {
	if len(names) == 0 {
		return func(namespaceLabel) bool { return true }
	}
	return func(candidate namespaceLabel) bool {
		for _, name := range names {
			if candidate.label == name {
				return true
			}
		}
		return false
	}
}

// visitCountKeysInTxn is the one iterator over a derived-count keyspace: it
// visits the keys under prefix whose (namespace, name) pair the filter accepts,
// with the key's value. No entity body is read.
func visitCountKeysInTxn(ctx context.Context, txn *badger.Txn, prefix []byte, parts func([]byte) (namespaceLabel, bool), wanted func(namespaceLabel) bool, visit func(namespaceLabel, []byte) error) error {
	var cancel bulkReadCancellation
	it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
	defer it.Close()
	for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
		if err := cancel.check(ctx); err != nil {
			return err
		}
		candidate, ok := parts(it.Item().Key())
		if !ok || !wanted(candidate) {
			continue
		}
		if err := it.Item().Value(func(val []byte) error { return visit(candidate, val) }); err != nil {
			return err
		}
	}
	return nil
}

// sumNamedCountsInTxn sums the base counts and unfolded deltas of the requested
// names across every namespace visible in txn, or of every name the count
// keyspace holds when names is empty. Both families share it, so an untyped
// count costs O(#names) point reads and never a scan of the population.
func (b *BadgerEngine) sumNamedCountsInTxn(ctx context.Context, txn *badger.Txn, kind byte, names []string) (int64, error) {
	wanted := namedCountNameFilter(names)
	var total int64
	if err := visitCountKeysInTxn(ctx, txn, namedCountPrefix(kind), namedCountKeyParts, wanted, func(_ namespaceLabel, value []byte) error {
		count, decodeErr := decodeDerivedCount(value)
		if decodeErr != nil {
			return fmt.Errorf("%w: %v", errMalformedDerivedCount, decodeErr)
		}
		total += count
		return nil
	}); err != nil {
		return 0, err
	}
	delta, err := sumCountDeltasByNamesInTxn(ctx, txn, kind, wanted)
	if err != nil {
		return 0, err
	}
	return total + delta, nil
}

// sumEveryNamedCountInTxn sums the live count of every name the scope holds (or
// of the requested names), read pair by pair through the canonical
// per-(namespace, name) reader so each name's unfolded deltas are its own.
func (b *BadgerEngine) sumEveryNamedCountInTxn(ctx context.Context, txn *badger.Txn, kind byte, namespace string, names []string) (int64, error) {
	if namespace != "" && len(names) > 0 {
		var total int64
		for i, name := range names {
			duplicate := false
			for _, previous := range names[:i] {
				if previous == name {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			count, err := b.readNamedCountInTxn(ctx, txn, kind, namespace, name)
			if err != nil {
				return 0, err
			}
			total += count
		}
		return total, nil
	}

	pairs, err := b.namedCountPairsInTxn(ctx, txn, kind, namespace, names)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, pair := range pairs {
		count, err := b.readNamedCountInTxn(ctx, txn, kind, pair.namespace, pair.label)
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

// namedCountPairsInTxn collects the (namespace, name) pairs the count keyspace
// and its unfolded deltas name in the scope.
func (b *BadgerEngine) namedCountPairsInTxn(ctx context.Context, txn *badger.Txn, kind byte, namespace string, names []string) ([]namespaceLabel, error) {
	wanted := inScopeNamedCountFilter(namespace, names)
	pairs := make(map[namespaceLabel]struct{})
	if err := visitCountKeysInTxn(ctx, txn, namedCountPrefix(kind), namedCountKeyParts, wanted, func(candidate namespaceLabel, _ []byte) error {
		pairs[candidate] = struct{}{}
		return nil
	}); err != nil {
		return nil, err
	}
	if err := visitCountKeysInTxn(ctx, txn, countDeltaAllPrefix(kind), countDeltaLabelKeyParts, wanted, func(candidate namespaceLabel, _ []byte) error {
		pairs[candidate] = struct{}{}
		return nil
	}); err != nil {
		return nil, err
	}
	collected := make([]namespaceLabel, 0, len(pairs))
	for pair := range pairs {
		collected = append(collected, pair)
	}
	return collected, nil
}

// inScopeNamedCountFilter accepts the requested names in the requested
// namespace; an empty namespace accepts every namespace and an empty name list
// every name.
func inScopeNamedCountFilter(namespace string, names []string) func(namespaceLabel) bool {
	byName := namedCountNameFilter(names)
	return func(candidate namespaceLabel) bool {
		return (namespace == "" || candidate.namespace == namespace) && byName(candidate)
	}
}

func (b *BadgerEngine) namedCount(ctx context.Context, kind byte, namespace, name string, scoped bool) (int64, error) {
	if err := b.pendingNamespaceConflict(namespace); err != nil {
		return 0, err
	}

	if err := b.ensureOpen(false); err != nil {
		return 0, err
	}
	mu := b.namedCountMutex(kind)
	if err := mu.RLockContext(ctx); err != nil {
		return 0, err
	}
	held := true
	defer func() {
		if held {
			mu.RUnlock()
		}
	}()
	count, err := b.readNamedCountSnapshot(ctx, kind, namespace, name, scoped)
	if scoped && errors.Is(err, errMalformedDerivedCount) {
		// Every hold belongs to the same request, including upgrade and retry.
		mu.RUnlock()
		held = false
		if err := mu.LockContext(ctx); err != nil {
			return 0, err
		}
		var repairErr error
		if kind == countDeltaKindEdgeType {
			repairErr = b.repairNamedCountLocked(ctx, countDeltaKindEdgeType, namespace, name, edgeTypeCountKey)
		} else {
			repairErr = b.repairNamedCountLocked(ctx, countDeltaKindLabel, namespace, name, labelCountKey)
		}
		mu.Unlock()
		if repairErr != nil {
			return 0, repairErr
		}
		if err := mu.RLockContext(ctx); err != nil {
			return 0, err
		}
		held = true
		count, err = b.readNamedCountSnapshot(ctx, kind, namespace, name, scoped)
	}
	if err != nil {
		return 0, err
	}
	if b.writeBehind != nil {
		delta, err := b.writeBehind.namedCountDelta(ctx, kind, namespace, name, scoped)
		if err != nil {
			return 0, err
		}
		count += delta
	}
	return count, nil
}

// foldNamedCountDeltas preserves the open-time all-family precheck outside the
// write hold, while scoped folds always enter the existing bounded kernel.
func (b *BadgerEngine) foldNamedCountDeltas(ctx context.Context, kind byte, namespace, name string, all bool) error {
	if all {
		any, err := b.hasAnyCountDeltasInDB(kind)
		if err != nil || !any {
			return err
		}
	}
	mu := b.namedCountMutex(kind)
	mu.Lock()
	defer mu.Unlock()
	baseKey := labelCountKey
	if kind == countDeltaKindEdgeType {
		baseKey = edgeTypeCountKey
	}
	return b.foldCountDeltasChunked(ctx, kind, baseKey, namespace, name)
}

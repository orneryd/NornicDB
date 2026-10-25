package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"

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

// The count/maintenance adapter consumes the foundation label decoder,
// including its retained release diagnostics and legacy metadata support.
type storedNodeLabels struct{ Labels []string }

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
	err := b.withView(func(txn *badger.Txn) error {
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

func (b *BadgerEngine) decrementLabelCounts(counts map[namespaceLabel]int64) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		for key, count := range counts {
			if err := b.adjustLabelCountInTxn(txn, key.namespace, key.label, -count); err != nil {
				return err
			}
		}
		return nil
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

func uniqueLabels(labels []string) []string {
	unique, _ := uniqueLabelsWithContext(context.Background(), labels)
	return unique
}

func uniqueLabelsWithContext(ctx context.Context, labels []string) ([]string, error) {
	if len(labels) == 0 {
		return nil, ctx.Err()
	}
	seen := make(map[string]struct{}, len(labels))
	unique := make([]string, 0, len(labels))
	for _, label := range labels {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if label == "" {
			continue
		}
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		unique = append(unique, label)
	}
	return unique, nil
}

func labelSet(labels []string) map[string]struct{} {
	set := make(map[string]struct{}, len(labels))
	for _, label := range uniqueLabels(labels) {
		set[label] = struct{}{}
	}
	return set
}

func (b *BadgerEngine) readLabelCountInTxn(txn *badger.Txn, namespace, label string) (int64, int, error) {
	return b.readLabelCountInTxnWithContext(context.Background(), txn, namespace, label)
}

func (b *BadgerEngine) readLabelCountInTxnWithContext(ctx context.Context, txn *badger.Txn, namespace, label string) (int64, int, error) {
	base, err := readDerivedCountInTxn(txn, labelCountKey(namespace, label))
	if err != nil {
		return 0, 0, err
	}
	deltaSum, seen, err := sumCountDeltasInTxnWithContext(ctx, txn, countDeltaKindLabel, namespace, label)
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
func (b *BadgerEngine) applyLabelCountDeltasLocked(deltas map[namespaceLabel]int64) error {
	if len(deltas) == 0 {
		return nil
	}
	return b.withUpdate(func(txn *badger.Txn) error {
		for key, delta := range deltas {
			if err := b.adjustLabelCountInTxn(txn, key.namespace, key.label, delta); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BadgerEngine) rebuildAuthoritativeLabelCountsLocked() error {
	counts, err := b.collectAuthoritativeLabelCounts()
	if err != nil {
		return err
	}
	return b.rebuildLabelCounts(counts)
}

func (b *BadgerEngine) NodeCountByLabelInNamespace(namespace, label string) (int64, error) {
	return b.NodeCountByLabelInNamespaceWithContext(context.Background(), namespace, label)
}

// NodeCountByLabelInNamespaceWithContext carries the caller through this read and its materialization.
func (b *BadgerEngine) NodeCountByLabelInNamespaceWithContext(ctx context.Context, namespace, label string) (int64, error) {
	// Until the label index is ready, count the stored bodies: a partial
	// generation must not repair or answer a count.
	if err := b.ensureOpen(false); err != nil {
		return 0, err
	}
	ready, err := b.labelIndexReady()
	if err != nil {
		return 0, err
	}
	if !ready {
		return b.countStoredLabelNodes(ctx, nodeScopeForNamespace(namespace), label)
	}
	return b.namedCountInNamespaceWithContext(ctx, countDeltaKindLabel, namespace, label)
}

// repairLabelCountLocked recomputes one namespace/label count from the label
// index after dropping the pair's persisted base and deltas. The caller must
// hold labelCountWriteMu.
func (b *BadgerEngine) repairLabelCountLocked(namespace, label string) error {
	return b.repairLabelCountLockedWithContext(context.Background(), namespace, label)
}

func (b *BadgerEngine) repairLabelCountLockedWithContext(ctx context.Context, namespace, label string) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		prefix := countDeltaPrefixFor(countDeltaKindLabel, namespace, label)
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
				it.Close()
				return err
			}
		}
		it.Close()
		count, err := b.countLabelIndexEntriesInNamespaceWithContext(ctx, namespace, label)
		if err != nil {
			return err
		}
		key := labelCountKey(namespace, label)
		if count == 0 {
			return txn.Delete(key)
		}
		return txn.Set(key, encodeDerivedCount(count))
	})
}

// countLabelIndexEntriesInNamespace counts one label through the shared
// readiness-aware reader, including the stored-node fallback during rebuild.
func (b *BadgerEngine) countLabelIndexEntriesInNamespace(namespace, label string) (int64, error) {
	return b.countLabelIndexEntriesInNamespaceWithContext(context.Background(), namespace, label)
}

func (b *BadgerEngine) countLabelIndexEntriesInNamespaceWithContext(ctx context.Context, namespace, label string) (int64, error) {
	var count int64
	err := b.withView(func(txn *badger.Txn) error {
		reader := b.newLabelIndexReader(ctx, txn, nodeScopeForNamespace(namespace), label, "", storedRead, false)
		defer reader.Close()
		for reader.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			count++
		}
		return reader.Err()
	})
	return count, err
}

// foldLabelCountDeltas collapses the persisted deltas of one label into its
// base count. It is triggered by reads past the fold threshold and by engine
// open; the returned error is discarded on the read-triggered path (the read
// already returned a correct sum).
func (b *BadgerEngine) foldLabelCountDeltas(namespace, label string) error {
	return b.foldNamedCountDeltas(countDeltaKindLabel, namespace, label, false)
}

// foldAllLabelCountDeltas collapses every persisted label-count delta at
// engine open, before any new write can reuse a suffix. When nothing is
// pending it does not open an update transaction, so a clean reopen does no
// open-time write.
func (b *BadgerEngine) foldAllLabelCountDeltas() error {
	return b.foldNamedCountDeltas(countDeltaKindLabel, "", "", true)
}

func (b *BadgerEngine) NodeCountByLabel(label string) (int64, error) {
	return b.NodeCountByLabelWithContext(context.Background(), label)
}

// NodeCountByLabelWithContext carries the caller through this read and its materialization.
func (b *BadgerEngine) NodeCountByLabelWithContext(ctx context.Context, label string) (int64, error) {
	if err := b.ensureOpen(false); err != nil {
		return 0, err
	}
	ready, err := b.labelIndexReady()
	if err != nil {
		return 0, err
	}
	if !ready {
		return b.countStoredLabelNodes(ctx, "", label)
	}
	return b.namedCountWithContext(ctx, countDeltaKindLabel, label)
}

func (b *BadgerEngine) labelCountReady() (bool, error) {
	var ready bool
	err := b.withView(func(txn *badger.Txn) error {
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
	err := b.withView(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions(labelCountPrefix()))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().KeyCopy(nil)
			sep := bytes.IndexByte(key[2:], 0)
			if sep < 0 {
				continue
			}
			namespace := string(key[2 : 2+sep])
			label := string(key[2+sep+1:])
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
	err := b.withView(func(txn *badger.Txn) error {
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
				node, decodeErr := decodeNodeMetadata(val)
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

func (b *BadgerEngine) rebuildLabelCounts(counts map[string]int64) error {
	return b.withUpdate(func(txn *badger.Txn) error {
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

func (b *BadgerEngine) ensureLabelCounts() error {
	if err := b.ensureOpen(false); err != nil {
		return err
	}
	// Fold any deltas a crash left behind first, so the persisted form is
	// base keys again before verification compares them.
	if err := b.foldAllLabelCountDeltas(); err != nil {
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
	return b.rebuildLabelCounts(actual)
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
func (b *BadgerEngine) namedCountMutex(kind byte) *sync.RWMutex {
	if kind == countDeltaKindEdgeType {
		return &b.edgeTypeCountWriteMu
	}
	return &b.labelCountWriteMu
}

// bufferedNamedCountDelta reads only committed buffered generations; persisted
// counters and their deltas remain protected by the caller's family read hold.
func (b *BadgerEngine) bufferedNamedCountDelta(kind byte, namespace, name string, scoped bool) int64 {
	if kind == countDeltaKindEdgeType {
		if scoped {
			return b.writeBehind.EdgeTypeCountDeltaInNamespace(namespace, name)
		}
		return b.writeBehind.EdgeTypeCountDelta(name)
	}
	if scoped {
		return b.writeBehind.LabelCountDeltaInNamespace(namespace, name)
	}
	return b.writeBehind.LabelCountDelta(name)
}

func (b *BadgerEngine) readNamedCountInNamespace(kind byte, namespace, name string) (int64, error) {
	return b.readNamedCountInNamespaceWithContext(context.Background(), kind, namespace, name)
}

func (b *BadgerEngine) readNamedCountInNamespaceWithContext(ctx context.Context, kind byte, namespace, name string) (int64, error) {
	var count int64
	err := b.withView(func(txn *badger.Txn) error {
		var err error
		if kind == countDeltaKindEdgeType {
			count, _, err = b.readEdgeTypeCountInTxn(txn, namespace, name)
		} else {
			count, _, err = b.readLabelCountInTxnWithContext(ctx, txn, namespace, name)
		}
		return err
	})
	return count, err
}

func (b *BadgerEngine) namedCountInNamespace(kind byte, namespace, name string) (int64, error) {
	return b.namedCountInNamespaceWithContext(context.Background(), kind, namespace, name)
}

func (b *BadgerEngine) namedCountInNamespaceWithContext(ctx context.Context, kind byte, namespace, name string) (int64, error) {
	if err := b.pendingNamespaceConflict(namespace); err != nil {
		return 0, err
	}
	if err := b.ensureOpen(false); err != nil {
		return 0, err
	}
	mu := b.namedCountMutex(kind)
	mu.RLock()
	defer mu.RUnlock()
	count, err := b.readNamedCountInNamespaceWithContext(ctx, kind, namespace, name)
	if errors.Is(err, errMalformedDerivedCount) {
		// Repair once under the exclusive family lock, then restore the read hold
		// before retrying or returning so the deferred release remains balanced.
		mu.RUnlock()
		mu.Lock()
		var repairErr error
		if kind == countDeltaKindEdgeType {
			repairErr = b.repairEdgeTypeCountLocked(namespace, name)
		} else {
			repairErr = b.repairLabelCountLockedWithContext(ctx, namespace, name)
		}
		mu.Unlock()
		mu.RLock()
		if repairErr != nil {
			return 0, repairErr
		}
		count, err = b.readNamedCountInNamespaceWithContext(ctx, kind, namespace, name)
	}
	if err != nil {
		return 0, err
	}
	if b.writeBehind != nil {
		delta, err := b.bufferedNamedCountDeltaWithContext(ctx, kind, namespace, name, true)
		if err != nil {
			return 0, err
		}
		count += delta
	}
	return count, nil
}

// namedCount sums persisted base counts, their unfolded deltas and committed
// write-behind deltas across namespaces under one family read hold.
func (b *BadgerEngine) namedCount(kind byte, name string) (int64, error) {
	return b.namedCountWithContext(context.Background(), kind, name)
}

func (b *BadgerEngine) namedCountWithContext(ctx context.Context, kind byte, name string) (int64, error) {
	if err := b.pendingNamespaceConflict(""); err != nil {
		return 0, err
	}
	if err := b.ensureOpen(false); err != nil {
		return 0, err
	}
	mu := b.namedCountMutex(kind)
	mu.RLock()
	defer mu.RUnlock()
	var prefix []byte
	if kind == countDeltaKindEdgeType {
		prefix = edgeTypeCountPrefix()
	} else {
		prefix = labelCountPrefix()
	}
	needle := []byte(name)
	var total int64
	err := b.withView(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := it.Item().KeyCopy(nil)
			sep := bytes.IndexByte(key[2:], 0)
			if sep < 0 {
				continue
			}
			if !bytes.Equal(key[2+sep+1:], needle) {
				continue
			}
			if err := it.Item().Value(func(val []byte) error {
				count, decodeErr := decodeDerivedCount(val)
				if decodeErr != nil {
					return decodeErr
				}
				total += count
				return nil
			}); err != nil {
				return err
			}
		}
		// Unfolded per-type deltas are part of the count too.
		delta, deltaErr := sumCountDeltasByNameInTxnWithContext(ctx, txn, kind, name)
		if deltaErr != nil {
			return deltaErr
		}
		total += delta
		return nil
	})
	if err != nil {
		return 0, err
	}
	if b.writeBehind != nil {
		delta, err := b.bufferedNamedCountDeltaWithContext(ctx, kind, "", name, false)
		if err != nil {
			return 0, err
		}
		total += delta
	}
	return total, nil
}

// foldNamedCountDeltas preserves the open-time all-family precheck outside the
// write hold, while scoped folds always enter the existing bounded kernel.
func (b *BadgerEngine) foldNamedCountDeltas(kind byte, namespace, name string, all bool) error {
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
	return b.foldCountDeltasChunked(kind, baseKey, namespace, name)
}

func (b *BadgerEngine) bufferedNamedCountDeltaWithContext(ctx context.Context, kind byte, namespace, name string, scoped bool) (int64, error) {
	if kind == countDeltaKindEdgeType {
		return b.bufferedNamedCountDelta(kind, namespace, name, scoped), ctx.Err()
	}
	if scoped {
		return b.writeBehind.LabelCountDeltaInNamespaceWithContext(ctx, namespace, name)
	}
	return b.writeBehind.LabelCountDeltaWithContext(ctx, name)
}

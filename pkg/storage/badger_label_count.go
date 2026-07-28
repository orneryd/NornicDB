package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/util"
)

var labelCountReadyKey = []byte{prefixMVCCMeta, prefixMVCCMetaLabelCountReady}

func normalizeCountLabel(label string) string {
	return strings.ToLower(label)
}

func labelCountKey(namespace, label string) []byte {
	key := make([]byte, 0, util.SafePreallocSum(3, len(namespace), len(label)))
	key = append(key, prefixMVCCMeta, prefixMVCCMetaLabelCount)
	key = append(key, namespace...)
	key = append(key, 0)
	key = append(key, normalizeCountLabel(label)...)
	return key
}

func labelCountPrefix() []byte {
	return []byte{prefixMVCCMeta, prefixMVCCMetaLabelCount}
}

func encodeLabelCount(count int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(count))
	return buf
}

func decodeLabelCount(val []byte) (int64, error) {
	if len(val) != 8 {
		return 0, fmt.Errorf("decode label count: expected 8 bytes, got %d", len(val))
	}
	return int64(binary.BigEndian.Uint64(val)), nil
}

func uniqueNormalizedLabels(labels []string) []string {
	if len(labels) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(labels))
	unique := make([]string, 0, len(labels))
	for _, label := range labels {
		normalized := normalizeCountLabel(label)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		unique = append(unique, normalized)
	}
	return unique
}

func labelSet(labels []string) map[string]struct{} {
	set := make(map[string]struct{}, len(labels))
	for _, label := range uniqueNormalizedLabels(labels) {
		set[label] = struct{}{}
	}
	return set
}

func (b *BadgerEngine) readLabelCountInTxn(txn *badger.Txn, namespace, label string) (int64, error) {
	item, err := txn.Get(labelCountKey(namespace, label))
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var count int64
	if err := item.Value(func(val []byte) error {
		decoded, decodeErr := decodeLabelCount(val)
		if decodeErr != nil {
			return decodeErr
		}
		count = decoded
		return nil
	}); err != nil {
		return 0, err
	}
	return count, nil
}

func (b *BadgerEngine) adjustLabelCountInTxn(txn *badger.Txn, namespace, label string, delta int64) error {
	if delta == 0 || namespace == "" || label == "" {
		return nil
	}
	count, err := b.readLabelCountInTxn(txn, namespace, label)
	if err != nil {
		return err
	}
	next := count + delta
	if next < 0 {
		return fmt.Errorf("label count underflow for %s:%s", namespace, label)
	}
	key := labelCountKey(namespace, label)
	if next == 0 {
		if err := txn.Delete(key); err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		return nil
	}
	return txn.Set(key, encodeLabelCount(next))
}

func (b *BadgerEngine) adjustNodeLabelCountsInTxn(txn *badger.Txn, namespace string, oldLabels, newLabels []string) error {
	oldSet := labelSet(oldLabels)
	newSet := labelSet(newLabels)
	for label := range newSet {
		if _, ok := oldSet[label]; ok {
			continue
		}
		if err := b.adjustLabelCountInTxn(txn, namespace, label, 1); err != nil {
			return err
		}
	}
	for label := range oldSet {
		if _, ok := newSet[label]; ok {
			continue
		}
		if err := b.adjustLabelCountInTxn(txn, namespace, label, -1); err != nil {
			return err
		}
	}
	return nil
}

func (b *BadgerEngine) NodeCountByLabelInNamespace(namespace, label string) (int64, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}
	var count int64
	err := b.withView(func(txn *badger.Txn) error {
		var err error
		count, err = b.readLabelCountInTxn(txn, namespace, label)
		return err
	})
	return count, err
}

func (b *BadgerEngine) NodeCountByLabel(label string) (int64, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}
	needle := []byte(normalizeCountLabel(label))
	var total int64
	err := b.withView(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerIterOptsPrefetchValues(labelCountPrefix(), 64))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().KeyCopy(nil)
			sep := bytes.IndexByte(key[2:], 0)
			if sep < 0 {
				continue
			}
			if !bytes.Equal(key[2+sep+1:], needle) {
				continue
			}
			if err := it.Item().Value(func(val []byte) error {
				count, decodeErr := decodeLabelCount(val)
				if decodeErr != nil {
					return decodeErr
				}
				total += count
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return total, err
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
		it := txn.NewIterator(badgerIterOptsPrefetchValues(labelCountPrefix(), 64))
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
				count, decodeErr := decodeLabelCount(val)
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
		it := txn.NewIterator(badgerIterOptsPrefetchValues([]byte{prefixNode}, 128))
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
				node, decodeErr := b.decodeNode(namespace, val)
				if decodeErr != nil {
					return fmt.Errorf("decode node %q for label counts: %w", nodeID, decodeErr)
				}
				for _, label := range uniqueNormalizedLabels(node.Labels) {
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
		it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
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
			if err := txn.Set(labelCountKey(parts[0], parts[1]), encodeLabelCount(count)); err != nil {
				return err
			}
		}
		return txn.Set(labelCountReadyKey, []byte{1})
	})
}

func (b *BadgerEngine) ensureLabelCounts() error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
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

func (tx *BadgerTransaction) readBufferedLabelCount(namespace, label string) (int64, error) {
	key := labelCountKey(namespace, label)
	keyStr := string(key)
	if tx.pendingDeletes[keyStr] {
		return 0, nil
	}
	if val, ok := tx.pendingWrites[keyStr]; ok {
		return decodeLabelCount(val)
	}
	item, err := tx.badgerTx.Get(key)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var count int64
	if err := item.Value(func(val []byte) error {
		decoded, decodeErr := decodeLabelCount(val)
		if decodeErr != nil {
			return decodeErr
		}
		count = decoded
		return nil
	}); err != nil {
		return 0, err
	}
	return count, nil
}

func (tx *BadgerTransaction) bufferAdjustLabelCount(namespace, label string, delta int64) error {
	if delta == 0 || namespace == "" || label == "" {
		return nil
	}
	count, err := tx.readBufferedLabelCount(namespace, label)
	if err != nil {
		return err
	}
	next := count + delta
	if next < 0 {
		return fmt.Errorf("label count underflow for %s:%s", namespace, label)
	}
	key := labelCountKey(namespace, label)
	if next == 0 {
		tx.bufferDelete(key)
		return nil
	}
	tx.bufferSet(key, encodeLabelCount(next))
	return nil
}

func (tx *BadgerTransaction) bufferAdjustNodeLabelCounts(namespace string, oldLabels, newLabels []string) error {
	oldSet := labelSet(oldLabels)
	newSet := labelSet(newLabels)
	for label := range newSet {
		if _, ok := oldSet[label]; ok {
			continue
		}
		if err := tx.bufferAdjustLabelCount(namespace, label, 1); err != nil {
			return err
		}
	}
	for label := range oldSet {
		if _, ok := newSet[label]; ok {
			continue
		}
		if err := tx.bufferAdjustLabelCount(namespace, label, -1); err != nil {
			return err
		}
	}
	return nil
}

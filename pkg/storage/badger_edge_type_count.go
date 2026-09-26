// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/util"
)

// Per-relationship-type derived counts (issue #638).
//
// This file mirrors the label-count machinery (badger_label_count.go): counts
// are persisted under the mvcc_meta keyspace, namespaced per database, updated
// transactionally on every edge write path, and verified/rebuilt once at
// engine open behind a ready marker. It shares the encodeDerivedCount /
// decodeDerivedCount codec with the label counters.
//
// Key shape:
//
//	[prefixMVCCMeta, prefixMVCCMetaEdgeTypeCount, namespace..., 0x00, lower(type)] -> uint64 count
//
// A typed relationship count is then a single Badger point read (summed across
// namespaces by EdgeCountByType), never a function of edge cardinality or
// store size.

var edgeTypeCountReadyKey = []byte{prefixMVCCMeta, prefixMVCCMetaEdgeTypeCountReady}

func normalizeCountEdgeType(edgeType string) string {
	return strings.ToLower(edgeType)
}

func edgeTypeCountKey(namespace, edgeType string) []byte {
	key := make([]byte, 0, util.SafePreallocSum(3, len(namespace), len(edgeType)))
	key = append(key, prefixMVCCMeta, prefixMVCCMetaEdgeTypeCount)
	key = append(key, namespace...)
	key = append(key, 0)
	key = append(key, normalizeCountEdgeType(edgeType)...)
	return key
}

func edgeTypeCountPrefix() []byte {
	return []byte{prefixMVCCMeta, prefixMVCCMetaEdgeTypeCount}
}

func edgeTypeCountNamespacePrefix(namespace string) []byte {
	key := make([]byte, 0, util.SafePreallocSum(3, len(namespace)))
	key = append(key, prefixMVCCMeta, prefixMVCCMetaEdgeTypeCount)
	key = append(key, namespace...)
	return append(key, 0)
}

// namespaceEdgeType is the composite key for edge-type count deltas.
type namespaceEdgeType struct {
	namespace string
	edgeType  string
}

// readEdgeTypeCountInTxn reads one persisted edge-type count (0 when absent).
func (b *BadgerEngine) readEdgeTypeCountInTxn(txn *badger.Txn, namespace, edgeType string) (int64, error) {
	item, err := txn.Get(edgeTypeCountKey(namespace, edgeType))
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
			return decodeErr
		}
		count = decoded
		return nil
	}); err != nil {
		return 0, err
	}
	return count, nil
}

// adjustEdgeTypeCountInTxn applies a signed delta to one persisted edge-type
// count inside the caller's transaction, so the counter commit is atomic with
// the edge write that justifies it.
func (b *BadgerEngine) adjustEdgeTypeCountInTxn(txn *badger.Txn, namespace, edgeType string, delta int64) error {
	if delta == 0 || namespace == "" || edgeType == "" {
		return nil
	}
	count, err := b.readEdgeTypeCountInTxn(txn, namespace, edgeType)
	if err != nil {
		return err
	}
	next := count + delta
	if next < 0 {
		return fmt.Errorf("edge-type count underflow for %s:%s", namespace, edgeType)
	}
	key := edgeTypeCountKey(namespace, edgeType)
	if next == 0 {
		if err := txn.Delete(key); err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		return nil
	}
	return txn.Set(key, encodeDerivedCount(next))
}

// EdgeCountByTypeInNamespace returns the persisted count of edges of the given
// type in one namespace (0 when absent).
func (b *BadgerEngine) EdgeCountByTypeInNamespace(namespace, edgeType string) (int64, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}
	b.edgeTypeCountWriteMu.RLock()
	defer b.edgeTypeCountWriteMu.RUnlock()
	var count int64
	err := b.withView(func(txn *badger.Txn) error {
		var err error
		count, err = b.readEdgeTypeCountInTxn(txn, namespace, edgeType)
		return err
	})
	return count, err
}

// EdgeCountByType returns the total count of edges of the given type across
// all namespaces. This is the O(#namespaces) point-read fast path behind
// MATCH ()-[r:T]->() RETURN count(r).
func (b *BadgerEngine) EdgeCountByType(edgeType string) (int64, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}
	b.edgeTypeCountWriteMu.RLock()
	defer b.edgeTypeCountWriteMu.RUnlock()
	needle := []byte(normalizeCountEdgeType(edgeType))
	var total int64
	err := b.withView(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerIterOptsPrefetchValues(edgeTypeCountPrefix(), 64))
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
		return nil
	})
	return total, err
}

func (b *BadgerEngine) edgeTypeCountReady() (bool, error) {
	var ready bool
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(edgeTypeCountReadyKey)
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

// edgeTypeCountSnapshot holds the authoritative derived counts for all three
// tiers: per-type, per-(startLabel, type), and per-(endLabel, type).
type edgeTypeCountSnapshot struct {
	types       map[namespaceEdgeType]int64
	startLabels map[namespaceEdgeTypeLabel]int64
	endLabels   map[namespaceEdgeTypeLabel]int64
}

func (s *edgeTypeCountSnapshot) empty() bool {
	return len(s.types) == 0 && len(s.startLabels) == 0 && len(s.endLabels) == 0
}

func (s *edgeTypeCountSnapshot) same(other *edgeTypeCountSnapshot) bool {
	if len(s.types) != len(other.types) || len(s.startLabels) != len(other.startLabels) || len(s.endLabels) != len(other.endLabels) {
		return false
	}
	for key, count := range s.types {
		if other.types[key] != count {
			return false
		}
	}
	for key, count := range s.startLabels {
		if other.startLabels[key] != count {
			return false
		}
	}
	for key, count := range s.endLabels {
		if other.endLabels[key] != count {
			return false
		}
	}
	return true
}

func loadPersistedEdgeTypeCountsSnapshot(txn *badger.Txn) (*edgeTypeCountSnapshot, error) {
	snap := &edgeTypeCountSnapshot{
		types:       make(map[namespaceEdgeType]int64),
		startLabels: make(map[namespaceEdgeTypeLabel]int64),
		endLabels:   make(map[namespaceEdgeTypeLabel]int64),
	}
	loadPrefix := func(prefix []byte, visit func(namespace, label, edgeType string, count int64)) error {
		it := txn.NewIterator(badgerIterOptsPrefetchValues(prefix, 64))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().KeyCopy(nil)
			sep := bytes.IndexByte(key[2:], 0)
			if sep < 0 {
				continue
			}
			namespace := string(key[2 : 2+sep])
			rest := key[2+sep+1:]
			if err := it.Item().Value(func(val []byte) error {
				count, decodeErr := decodeDerivedCount(val)
				if decodeErr != nil {
					return decodeErr
				}
				visit(namespace, "", string(rest), count)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}
	loadPair := func(prefix []byte, target map[namespaceEdgeTypeLabel]int64) error {
		it := txn.NewIterator(badgerIterOptsPrefetchValues(prefix, 64))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().KeyCopy(nil)
			sep1 := bytes.IndexByte(key[2:], 0)
			if sep1 < 0 {
				continue
			}
			namespace := string(key[2 : 2+sep1])
			rest := key[2+sep1+1:]
			sep2 := bytes.IndexByte(rest, 0)
			if sep2 < 0 {
				continue
			}
			label := string(rest[:sep2])
			edgeType := string(rest[sep2+1:])
			if err := it.Item().Value(func(val []byte) error {
				count, decodeErr := decodeDerivedCount(val)
				if decodeErr != nil {
					return decodeErr
				}
				target[namespaceEdgeTypeLabel{namespace: namespace, label: label, edgeType: edgeType}] = count
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}
	if err := loadPrefix(edgeTypeCountPrefix(), func(namespace, label, edgeType string, count int64) {
		snap.types[namespaceEdgeType{namespace: namespace, edgeType: edgeType}] = count
	}); err != nil {
		return nil, err
	}
	if err := loadPair(edgeTypeStartLabelCountPrefix(), snap.startLabels); err != nil {
		return nil, err
	}
	if err := loadPair(edgeTypeEndLabelCountPrefix(), snap.endLabels); err != nil {
		return nil, err
	}
	return snap, nil
}

// readNodeLabelsIfPresentInTxn reads only the labels of a node body inside the
// caller's transaction (no property decode). Absent bodies contribute no
// labels; legacy-format bodies fall back to the compat decoder, and
// undecodable bodies contribute no labels (the startup rebuild repairs any
// resulting drift).
func (b *BadgerEngine) readNodeLabelsIfPresentInTxn(txn *badger.Txn, id NodeID) ([]string, error) {
	item, err := txn.Get(nodeKey(id))
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var labels []string
	if err := item.Value(func(val []byte) error {
		var decodeErr error
		labels, decodeErr = decodeStoredNodeLabels(val)
		if decodeErr != nil {
			// Pre-tokenization (V0) bodies: use the full compat decoder.
			node, nodeErr := b.decodeNode(namespaceForNodeID(id), val)
			if nodeErr != nil {
				return nil // undecodable: skip positional bookkeeping
			}
			labels = node.Labels
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return labels, nil
}

// collectAuthoritativeEdgeTypeCountsSnapshot derives the true counts for all
// three tiers in one pass over edge primary keys: each edge contributes its
// type, its start-endpoint labels, and its end-endpoint labels.
func (b *BadgerEngine) collectAuthoritativeEdgeTypeCountsSnapshot() (*edgeTypeCountSnapshot, error) {
	snap := &edgeTypeCountSnapshot{
		types:       make(map[namespaceEdgeType]int64),
		startLabels: make(map[namespaceEdgeTypeLabel]int64),
		endLabels:   make(map[namespaceEdgeTypeLabel]int64),
	}
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerIterOptsPrefetchValues([]byte{prefixEdge}, 64))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().KeyCopy(nil)
			if len(key) <= 1 {
				continue
			}
			edgeID := EdgeID(key[1:])
			namespace, _, ok := ParseDatabasePrefix(string(edgeID))
			if !ok {
				continue
			}
			if err := it.Item().Value(func(value []byte) error {
				edge, decodeErr := b.decodeEdgeBodyByID(value, edgeID)
				if decodeErr != nil {
					return nil // skip undecodable bodies (same as EdgeCount semantics)
				}
				edgeType := normalizeCountEdgeType(edge.Type)
				if edgeType == "" {
					return nil
				}
				snap.types[namespaceEdgeType{namespace: namespace, edgeType: edgeType}]++
				startLabels, startErr := b.readNodeLabelsIfPresentInTxn(txn, edge.StartNode)
				if startErr != nil {
					return startErr
				}
				endLabels, endErr := b.readNodeLabelsIfPresentInTxn(txn, edge.EndNode)
				if endErr != nil {
					return endErr
				}
				for _, label := range uniqueNormalizedLabels(startLabels) {
					snap.startLabels[namespaceEdgeTypeLabel{namespace: namespace, label: label, edgeType: edgeType}]++
				}
				for _, label := range uniqueNormalizedLabels(endLabels) {
					snap.endLabels[namespaceEdgeTypeLabel{namespace: namespace, label: label, edgeType: edgeType}]++
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return snap, nil
}

// collectAuthoritativeEdgeTypeCounts is the type-tier projection of the
// snapshot collector, retained for DeleteByPrefix accounting.
func (b *BadgerEngine) collectAuthoritativeEdgeTypeCounts() (map[string]int64, error) {
	snap, err := b.collectAuthoritativeEdgeTypeCountsSnapshot()
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int64, len(snap.types))
	for key, count := range snap.types {
		counts[key.namespace+"\x00"+key.edgeType] = count
	}
	return counts, nil
}

func (b *BadgerEngine) rebuildEdgeTypeCountsSnapshot(snap *edgeTypeCountSnapshot) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		prefixes := [][]byte{edgeTypeCountPrefix(), edgeTypeStartLabelCountPrefix(), edgeTypeEndLabelCountPrefix()}
		for _, prefix := range prefixes {
			it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
			for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
				key := it.Item().KeyCopy(nil)
				if err := txn.Delete(key); err != nil {
					it.Close()
					return fmt.Errorf("clear edge-type count %q before rebuild: %w", key, err)
				}
			}
			it.Close()
		}
		for key, count := range snap.types {
			if err := txn.Set(edgeTypeCountKey(key.namespace, key.edgeType), encodeDerivedCount(count)); err != nil {
				return err
			}
		}
		for key, count := range snap.startLabels {
			if err := txn.Set(edgeTypeLabelCountKey(prefixMVCCMetaEdgeTypeStartLabelCount, key.namespace, key.label, key.edgeType), encodeDerivedCount(count)); err != nil {
				return err
			}
		}
		for key, count := range snap.endLabels {
			if err := txn.Set(edgeTypeLabelCountKey(prefixMVCCMetaEdgeTypeEndLabelCount, key.namespace, key.label, key.edgeType), encodeDerivedCount(count)); err != nil {
				return err
			}
		}
		return txn.Set(edgeTypeCountReadyKey, []byte{1})
	})
}

// ensureEdgeTypeCounts verifies persisted edge-type counts (all three tiers)
// against the edge primary keys and rebuilds them on drift. It runs once at
// engine open, behind the ready marker (same shape as ensureLabelCounts).
func (b *BadgerEngine) ensureEdgeTypeCounts() error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	ready, err := b.edgeTypeCountReady()
	if err != nil {
		return err
	}
	actual, err := b.collectAuthoritativeEdgeTypeCountsSnapshot()
	if err != nil {
		return err
	}
	var persisted *edgeTypeCountSnapshot
	err = b.db.View(func(txn *badger.Txn) error {
		var loadErr error
		persisted, loadErr = loadPersistedEdgeTypeCountsSnapshot(txn)
		return loadErr
	})
	if err != nil {
		return err
	}
	if ready && actual.same(persisted) {
		return nil
	}
	return b.rebuildEdgeTypeCountsSnapshot(actual)
}

// decrementEdgeTypeCounts applies a batch of negative deltas (DeleteByPrefix).
func (b *BadgerEngine) decrementEdgeTypeCounts(counts map[namespaceEdgeType]int64) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		for key, count := range counts {
			if err := b.adjustEdgeTypeCountInTxn(txn, key.namespace, key.edgeType, -count); err != nil {
				return err
			}
		}
		return nil
	})
}

// decrementEdgeTypeLabelCounts applies a batch of negative positional deltas
// (DeleteByPrefix).
func (b *BadgerEngine) decrementEdgeTypeLabelCounts(sub byte, counts map[namespaceEdgeTypeLabel]int64) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		for key, count := range counts {
			if err := b.adjustEdgeTypeLabelCountInTxn(txn, sub, key.namespace, key.label, key.edgeType, -count); err != nil {
				return err
			}
		}
		return nil
	})
}

// bufferAdjustEdgeTypeCount stages a per-type counter delta on the
// transaction. The deltas are applied after the user-data commit, so
// independent edge writes that share a type never conflict on the counter key.
func (tx *BadgerTransaction) bufferAdjustEdgeTypeCount(namespace, edgeType string, delta int64) {
	if delta == 0 || namespace == "" || edgeType == "" {
		return
	}
	key := namespaceEdgeType{namespace: namespace, edgeType: normalizeCountEdgeType(edgeType)}
	tx.pendingEdgeTypeCountDeltas[key] += delta
	if tx.pendingEdgeTypeCountDeltas[key] == 0 {
		delete(tx.pendingEdgeTypeCountDeltas, key)
	}
}

// applyEdgeTypeCountDeltasLocked persists transaction-local derived count
// deltas. The caller must hold edgeTypeCountWriteMu across both the
// user-data commit and this metadata update so count changes retain the same
// order as edge changes.
func (b *BadgerEngine) applyEdgeTypeCountDeltasLocked(deltas map[namespaceEdgeType]int64) error {
	if len(deltas) == 0 {
		return nil
	}
	return b.withUpdate(func(txn *badger.Txn) error {
		for key, delta := range deltas {
			if err := b.adjustEdgeTypeCountInTxn(txn, key.namespace, key.edgeType, delta); err != nil {
				return err
			}
		}
		return nil
	})
}

// rebuildAuthoritativeEdgeTypeCountsLocked repairs the persisted edge-type
// counters (all three tiers) from the edge primary keys. The caller must hold
// edgeTypeCountWriteMu.
func (b *BadgerEngine) rebuildAuthoritativeEdgeTypeCountsLocked() error {
	snap, err := b.collectAuthoritativeEdgeTypeCountsSnapshot()
	if err != nil {
		return err
	}
	return b.rebuildEdgeTypeCountsSnapshot(snap)
}

// collectEdgeTypeCountsByPrefix derives per-(namespace, type) counts for edges
// whose IDs carry the given prefix, by decoding only the type field of each
// edge body.
func (b *BadgerEngine) collectEdgeTypeCountsByPrefix(keyPrefix []byte) (map[namespaceEdgeType]int64, error) {
	counts, _, _, err := b.collectEdgeTypeCountsByPrefixSnapshot(keyPrefix)
	return counts, err
}

// collectEdgeTypeCountsByPrefixSnapshot derives the type tier plus the
// positional (label, type) tiers for edges whose IDs carry the given prefix.
func (b *BadgerEngine) collectEdgeTypeCountsByPrefixSnapshot(keyPrefix []byte) (map[namespaceEdgeType]int64, map[namespaceEdgeTypeLabel]int64, map[namespaceEdgeTypeLabel]int64, error) {
	counts := make(map[namespaceEdgeType]int64)
	startLabels := make(map[namespaceEdgeTypeLabel]int64)
	endLabels := make(map[namespaceEdgeTypeLabel]int64)
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerIterOptsPrefetchValues(keyPrefix, 64))
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(keyPrefix); it.Next() {
			key := it.Item().Key()
			if len(key) <= 1 {
				continue
			}
			edgeID := EdgeID(key[1:])
			namespace, _, ok := ParseDatabasePrefix(string(edgeID))
			if !ok {
				continue
			}
			if err := it.Item().Value(func(value []byte) error {
				edge, decodeErr := b.decodeEdgeBodyByID(value, edgeID)
				if decodeErr != nil {
					return decodeErr
				}
				if edge.Type == "" {
					return nil
				}
				counts[namespaceEdgeType{namespace: namespace, edgeType: normalizeCountEdgeType(edge.Type)}]++
				startNodeLabels, labelErr := b.readNodeLabelsIfPresentInTxn(txn, edge.StartNode)
				if labelErr != nil {
					return labelErr
				}
				endNodeLabels, labelErr := b.readNodeLabelsIfPresentInTxn(txn, edge.EndNode)
				if labelErr != nil {
					return labelErr
				}
				for _, label := range uniqueNormalizedLabels(startNodeLabels) {
					startLabels[namespaceEdgeTypeLabel{namespace: namespace, label: label, edgeType: normalizeCountEdgeType(edge.Type)}]++
				}
				for _, label := range uniqueNormalizedLabels(endNodeLabels) {
					endLabels[namespaceEdgeTypeLabel{namespace: namespace, label: label, edgeType: normalizeCountEdgeType(edge.Type)}]++
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return counts, startLabels, endLabels, err
}

// ============================================================================
// Positional (label, relationship-type) counts (issue #638, one-labeled shapes)
// ============================================================================

func edgeTypeStartLabelCountPrefix() []byte {
	return []byte{prefixMVCCMeta, prefixMVCCMetaEdgeTypeStartLabelCount}
}

func edgeTypeEndLabelCountPrefix() []byte {
	return []byte{prefixMVCCMeta, prefixMVCCMetaEdgeTypeEndLabelCount}
}

func edgeTypeLabelCountNamespacePrefix(sub byte, namespace string) []byte {
	key := make([]byte, 0, util.SafePreallocSum(3, len(namespace)))
	key = append(key, prefixMVCCMeta, sub)
	key = append(key, namespace...)
	return append(key, 0)
}

func edgeTypeLabelCountKey(sub byte, namespace, label, edgeType string) []byte {
	key := make([]byte, 0, util.SafePreallocSum(4, len(namespace), len(label), len(edgeType)))
	key = append(key, prefixMVCCMeta, sub)
	key = append(key, namespace...)
	key = append(key, 0)
	key = append(key, normalizeCountLabel(label)...)
	key = append(key, 0)
	key = append(key, normalizeCountEdgeType(edgeType)...)
	return key
}

// namespaceEdgeTypeLabel is the composite key for positional count deltas.
type namespaceEdgeTypeLabel struct {
	namespace string
	label     string
	edgeType  string
}

func (b *BadgerEngine) readEdgeTypeLabelCountInTxn(txn *badger.Txn, sub byte, namespace, label, edgeType string) (int64, error) {
	item, err := txn.Get(edgeTypeLabelCountKey(sub, namespace, label, edgeType))
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
			return decodeErr
		}
		count = decoded
		return nil
	}); err != nil {
		return 0, err
	}
	return count, nil
}

func (b *BadgerEngine) adjustEdgeTypeLabelCountInTxn(txn *badger.Txn, sub byte, namespace, label, edgeType string, delta int64) error {
	if delta == 0 || namespace == "" || label == "" || edgeType == "" {
		return nil
	}
	count, err := b.readEdgeTypeLabelCountInTxn(txn, sub, namespace, label, edgeType)
	if err != nil {
		return err
	}
	next := count + delta
	if next < 0 {
		return fmt.Errorf("edge-type label count underflow for %s:%s:%s", namespace, label, edgeType)
	}
	key := edgeTypeLabelCountKey(sub, namespace, label, edgeType)
	if next == 0 {
		if err := txn.Delete(key); err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		return nil
	}
	return txn.Set(key, encodeDerivedCount(next))
}

// EdgeCountByStartLabelInNamespace returns the count of edges of the given
// type whose physical START endpoint carries the label, in one namespace.
func (b *BadgerEngine) EdgeCountByStartLabelInNamespace(namespace, label, edgeType string) (int64, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}
	b.edgeTypeCountWriteMu.RLock()
	defer b.edgeTypeCountWriteMu.RUnlock()
	var count int64
	err := b.withView(func(txn *badger.Txn) error {
		var err error
		count, err = b.readEdgeTypeLabelCountInTxn(txn, prefixMVCCMetaEdgeTypeStartLabelCount, namespace, label, edgeType)
		return err
	})
	return count, err
}

// EdgeCountByEndLabelInNamespace returns the count of edges of the given type
// whose physical END endpoint carries the label, in one namespace.
func (b *BadgerEngine) EdgeCountByEndLabelInNamespace(namespace, label, edgeType string) (int64, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}
	b.edgeTypeCountWriteMu.RLock()
	defer b.edgeTypeCountWriteMu.RUnlock()
	var count int64
	err := b.withView(func(txn *badger.Txn) error {
		var err error
		count, err = b.readEdgeTypeLabelCountInTxn(txn, prefixMVCCMetaEdgeTypeEndLabelCount, namespace, label, edgeType)
		return err
	})
	return count, err
}

// sumEdgeTypeLabelCountAcrossNamespaces sums one positional tier across every
// namespace (O(#namespaces) point reads).
func (b *BadgerEngine) sumEdgeTypeLabelCountAcrossNamespaces(sub byte, label, edgeType string) (int64, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}
	b.edgeTypeCountWriteMu.RLock()
	defer b.edgeTypeCountWriteMu.RUnlock()
	needleLabel := []byte(normalizeCountLabel(label))
	needleType := []byte(normalizeCountEdgeType(edgeType))
	var prefix []byte
	if sub == prefixMVCCMetaEdgeTypeStartLabelCount {
		prefix = edgeTypeStartLabelCountPrefix()
	} else {
		prefix = edgeTypeEndLabelCountPrefix()
	}
	var total int64
	err := b.withView(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerIterOptsPrefetchValues(prefix, 64))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().KeyCopy(nil)
			sep1 := bytes.IndexByte(key[2:], 0)
			if sep1 < 0 {
				continue
			}
			rest := key[2+sep1+1:]
			sep2 := bytes.IndexByte(rest, 0)
			if sep2 < 0 {
				continue
			}
			if !bytes.Equal(rest[:sep2], needleLabel) || !bytes.Equal(rest[sep2+1:], needleType) {
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
		return nil
	})
	return total, err
}

// EdgeCountByStartLabel returns the count of edges of the given type whose
// physical START endpoint carries the label, across all namespaces.
func (b *BadgerEngine) EdgeCountByStartLabel(label, edgeType string) (int64, error) {
	return b.sumEdgeTypeLabelCountAcrossNamespaces(prefixMVCCMetaEdgeTypeStartLabelCount, label, edgeType)
}

// EdgeCountByEndLabel returns the count of edges of the given type whose
// physical END endpoint carries the label, across all namespaces.
func (b *BadgerEngine) EdgeCountByEndLabel(label, edgeType string) (int64, error) {
	return b.sumEdgeTypeLabelCountAcrossNamespaces(prefixMVCCMetaEdgeTypeEndLabelCount, label, edgeType)
}

// edgePositionalLabelDeltaKeys returns the (namespace, label, type) delta keys
// an endpoint's labels contribute at one position (start or end).
func edgePositionalLabelDeltaKeys(namespace, edgeType string, labels []string) []namespaceEdgeTypeLabel {
	unique := uniqueNormalizedLabels(labels)
	if len(unique) == 0 {
		return nil
	}
	keys := make([]namespaceEdgeTypeLabel, 0, len(unique))
	for _, label := range unique {
		keys = append(keys, namespaceEdgeTypeLabel{namespace: namespace, label: label, edgeType: normalizeCountEdgeType(edgeType)})
	}
	return keys
}

// adjustEdgeTypeLabelCountsForEdgeInTxn applies one edge's positional
// (label, type) contributions — its start-endpoint labels to the start tier
// and its end-endpoint labels to the end tier — with the given sign.
func (b *BadgerEngine) adjustEdgeTypeLabelCountsForEdgeInTxn(txn *badger.Txn, namespace, edgeType string, startLabels, endLabels []string, sign int64) error {
	if sign == 0 || edgeType == "" {
		return nil
	}
	for _, key := range edgePositionalLabelDeltaKeys(namespace, edgeType, startLabels) {
		if err := b.adjustEdgeTypeLabelCountInTxn(txn, prefixMVCCMetaEdgeTypeStartLabelCount, key.namespace, key.label, key.edgeType, sign); err != nil {
			return err
		}
	}
	for _, key := range edgePositionalLabelDeltaKeys(namespace, edgeType, endLabels) {
		if err := b.adjustEdgeTypeLabelCountInTxn(txn, prefixMVCCMetaEdgeTypeEndLabelCount, key.namespace, key.label, key.edgeType, sign); err != nil {
			return err
		}
	}
	return nil
}

// nodeLabelChangeDeltas returns the normalized labels added and removed by a
// transition from oldLabels to newLabels.
func nodeLabelChangeDeltas(oldLabels, newLabels []string) (added, removed []string) {
	oldSet := labelSet(oldLabels)
	newSet := labelSet(newLabels)
	for label := range newSet {
		if _, ok := oldSet[label]; !ok {
			added = append(added, label)
		}
	}
	for label := range oldSet {
		if _, ok := newSet[label]; !ok {
			removed = append(removed, label)
		}
	}
	return added, removed
}

// adjustEdgeTypeLabelCountsForNodeLabelChangeInTxn moves a node's incident
// edges between positional label buckets when the node gains or loses labels.
// Outgoing edges contribute at the start tier, incoming edges at the end tier.
func (b *BadgerEngine) adjustEdgeTypeLabelCountsForNodeLabelChangeInTxn(txn *badger.Txn, namespace, nodeID string, added, removed []string) error {
	addedUnique := uniqueNormalizedLabels(added)
	removedUnique := uniqueNormalizedLabels(removed)
	if len(addedUnique) == 0 && len(removedUnique) == 0 {
		return nil
	}
	adjustTier := func(sub byte, edgeTypes map[string]int64) error {
		for edgeType, multiplicity := range edgeTypes {
			for _, label := range addedUnique {
				if err := b.adjustEdgeTypeLabelCountInTxn(txn, sub, namespace, label, edgeType, multiplicity); err != nil {
					return err
				}
			}
			for _, label := range removedUnique {
				if err := b.adjustEdgeTypeLabelCountInTxn(txn, sub, namespace, label, edgeType, -multiplicity); err != nil {
					return err
				}
			}
		}
		return nil
	}
	outTypes, err := b.incidentEdgeTypesInTxn(txn, b.outgoingIndexPrefixString(NodeID(nodeID)))
	if err != nil {
		return err
	}
	if err := adjustTier(prefixMVCCMetaEdgeTypeStartLabelCount, outTypes); err != nil {
		return err
	}
	inTypes, err := b.incidentEdgeTypesInTxn(txn, b.incomingIndexPrefixString(NodeID(nodeID)))
	if err != nil {
		return err
	}
	return adjustTier(prefixMVCCMetaEdgeTypeEndLabelCount, inTypes)
}

// incidentEdgeTypesInTxn counts the edge types reachable through one adjacency
// prefix (outgoing or incoming) of a node, inside the caller's transaction.
func (b *BadgerEngine) incidentEdgeTypesInTxn(txn *badger.Txn, prefix []byte) (map[string]int64, error) {
	if prefix == nil {
		return nil, nil
	}
	types := make(map[string]int64)
	it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		edgeNum, ok := extractEdgeNumIDFromOutgoingKey(it.Item().KeyCopy(nil))
		if !ok {
			continue
		}
		edgeID, ok := b.idDict.lookupEdgeIDByNum(edgeNum)
		if !ok {
			continue
		}
		item, err := txn.Get(edgeKey(edgeID))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				continue
			}
			return nil, err
		}
		if err := item.Value(func(val []byte) error {
			edge, decodeErr := b.decodeEdgeBodyByID(val, edgeID)
			if decodeErr != nil {
				return nil // skip undecodable bodies
			}
			edgeType := normalizeCountEdgeType(edge.Type)
			if edgeType == "" {
				return nil
			}
			types[edgeType]++
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return types, nil
}

// edgeTypeLabelDelta is one buffered positional (label, type) counter delta on
// a transaction.
type edgeTypeLabelDelta struct {
	sub       byte
	namespace string
	label     string
	edgeType  string
}

// bufferAdjustEdgeTypeLabelCount stages a positional (label, type) counter
// delta on the transaction. Applied after the user-data commit, so
// independent edge writes never conflict on the counter keys.
func (tx *BadgerTransaction) bufferAdjustEdgeTypeLabelCount(sub byte, namespace, label, edgeType string, delta int64) {
	if delta == 0 || namespace == "" || label == "" || edgeType == "" {
		return
	}
	key := edgeTypeLabelDelta{
		sub:       sub,
		namespace: namespace,
		label:     normalizeCountLabel(label),
		edgeType:  normalizeCountEdgeType(edgeType),
	}
	tx.pendingEdgeTypeLabelCountDeltas[key] += delta
	if tx.pendingEdgeTypeLabelCountDeltas[key] == 0 {
		delete(tx.pendingEdgeTypeLabelCountDeltas, key)
	}
}

// bufferEdgePositionalLabelDeltas buffers one edge's positional label
// contributions with the given sign.
func (tx *BadgerTransaction) bufferEdgePositionalLabelDeltas(namespace, edgeType string, startLabels, endLabels []string, sign int64) {
	if sign == 0 || edgeType == "" {
		return
	}
	for _, key := range edgePositionalLabelDeltaKeys(namespace, edgeType, startLabels) {
		tx.bufferAdjustEdgeTypeLabelCount(prefixMVCCMetaEdgeTypeStartLabelCount, key.namespace, key.label, key.edgeType, sign)
	}
	for _, key := range edgePositionalLabelDeltaKeys(namespace, edgeType, endLabels) {
		tx.bufferAdjustEdgeTypeLabelCount(prefixMVCCMetaEdgeTypeEndLabelCount, key.namespace, key.label, key.edgeType, sign)
	}
}

// nodeLabelsTxVisibleLocked returns a node's labels as visible to this
// transaction: the pending overlay wins, then committed state. The caller
// must hold tx.mu.
func (tx *BadgerTransaction) nodeLabelsTxVisibleLocked(id NodeID) ([]string, error) {
	if node, ok := tx.pendingNodes[id]; ok {
		return node.Labels, nil
	}
	node, err := tx.getCommittedNodeLocked(id)
	if err == ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return node.Labels, nil
}

// incidentEdgeTypesTxLocked counts the edge types reachable through one
// adjacency direction of a node as visible to this transaction: committed
// adjacency with the pending overlay applied. The caller must hold tx.mu.
func (tx *BadgerTransaction) incidentEdgeTypesTxLocked(nodeID NodeID, outgoing bool) (map[string]int64, error) {
	types := make(map[string]int64)
	var prefix []byte
	if outgoing {
		prefix = tx.engine.outgoingIndexPrefixString(nodeID)
	} else {
		prefix = tx.engine.incomingIndexPrefixString(nodeID)
	}
	if prefix != nil {
		it := tx.badgerTx.NewIterator(badgerIterOptsKeyOnly(prefix))
		for it.Rewind(); it.Valid(); it.Next() {
			edgeNum, ok := extractEdgeNumIDFromOutgoingKey(it.Item().KeyCopy(nil))
			if !ok {
				continue
			}
			edgeID, ok := tx.engine.idDict.lookupEdgeIDByNum(edgeNum)
			if !ok {
				continue
			}
			if _, deleted := tx.deletedEdges[edgeID]; deleted {
				continue
			}
			if _, pending := tx.pendingEdges[edgeID]; pending {
				continue // count the pending version below instead
			}
			edge, err := tx.getCommittedEdgeLocked(edgeID)
			if err != nil {
				if err == ErrNotFound {
					continue
				}
				it.Close()
				return nil, err
			}
			if t := normalizeCountEdgeType(edge.Type); t != "" {
				types[t]++
			}
		}
		it.Close()
	}
	for _, edge := range tx.pendingEdges {
		if edge == nil {
			continue
		}
		if outgoing && edge.StartNode != nodeID {
			continue
		}
		if !outgoing && edge.EndNode != nodeID {
			continue
		}
		if t := normalizeCountEdgeType(edge.Type); t != "" {
			types[t]++
		}
	}
	return types, nil
}

// bufferNodeLabelChangeEdgeTypeDeltasLocked buffers the positional
// (label, type) deltas a node's relabel implies across its tx-visible
// incident edges. The caller must hold tx.mu.
func (tx *BadgerTransaction) bufferNodeLabelChangeEdgeTypeDeltasLocked(nodeID NodeID, added, removed []string) error {
	addedUnique := uniqueNormalizedLabels(added)
	removedUnique := uniqueNormalizedLabels(removed)
	if len(addedUnique) == 0 && len(removedUnique) == 0 {
		return nil
	}
	outTypes, err := tx.incidentEdgeTypesTxLocked(nodeID, true)
	if err != nil {
		return err
	}
	inTypes, err := tx.incidentEdgeTypesTxLocked(nodeID, false)
	if err != nil {
		return err
	}
	for edgeType, multiplicity := range outTypes {
		for _, label := range addedUnique {
			tx.bufferAdjustEdgeTypeLabelCount(prefixMVCCMetaEdgeTypeStartLabelCount, tx.namespace, label, edgeType, multiplicity)
		}
		for _, label := range removedUnique {
			tx.bufferAdjustEdgeTypeLabelCount(prefixMVCCMetaEdgeTypeStartLabelCount, tx.namespace, label, edgeType, -multiplicity)
		}
	}
	for edgeType, multiplicity := range inTypes {
		for _, label := range addedUnique {
			tx.bufferAdjustEdgeTypeLabelCount(prefixMVCCMetaEdgeTypeEndLabelCount, tx.namespace, label, edgeType, multiplicity)
		}
		for _, label := range removedUnique {
			tx.bufferAdjustEdgeTypeLabelCount(prefixMVCCMetaEdgeTypeEndLabelCount, tx.namespace, label, edgeType, -multiplicity)
		}
	}
	return nil
}

// applyEdgeTypeLabelCountDeltasLocked persists transaction-local positional
// counter deltas. The caller must hold edgeTypeCountWriteMu across both the
// user-data commit and this metadata update.
func (b *BadgerEngine) applyEdgeTypeLabelCountDeltasLocked(deltas map[edgeTypeLabelDelta]int64) error {
	if len(deltas) == 0 {
		return nil
	}
	return b.withUpdate(func(txn *badger.Txn) error {
		for key, delta := range deltas {
			if err := b.adjustEdgeTypeLabelCountInTxn(txn, key.sub, key.namespace, key.label, key.edgeType, delta); err != nil {
				return err
			}
		}
		return nil
	})
}

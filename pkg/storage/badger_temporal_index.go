package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/util"
)

type temporalRefreshTarget struct {
	constraint installedConstraintDefinition
	desc       temporalIndexDescriptor
	keyValue   interface{}
}

type temporalIndexDescriptor struct {
	namespace string
	label     string
	keyProp   string
	startProp string
	endProp   string
	keyHash   string
}

// temporalConstraintForLookup finds the single-key temporal constraint on label
// whose properties are exactly (keyProp, startProp, endProp). Composite-key
// constraints are not addressable by one key property.
func temporalConstraintForLookup(schema *SchemaManager, label, keyProp, startProp, endProp string) (installedConstraintDefinition, bool) {
	if schema == nil {
		return installedConstraintDefinition{}, false
	}
	constraints := schema.constraintsForLabels([]string{label})
	for _, c := range constraints {
		if c.Type != ConstraintTemporal || c.Properties.Len() != 3 {
			continue
		}
		if c.Label == label && c.Properties.At(0) == keyProp && c.Properties.At(1) == startProp && c.Properties.At(2) == endProp {
			return c, true
		}
	}
	return installedConstraintDefinition{}, false
}

func temporalConstraintsForLabels(schema *SchemaManager, labels []string) []installedConstraintDefinition {
	if schema == nil {
		return nil
	}
	constraints := schema.constraintsForLabels(labels)
	out := make([]installedConstraintDefinition, 0, len(constraints))
	for _, c := range constraints {
		if c.Type != ConstraintTemporal || c.EffectiveEntityType() != ConstraintEntityNode {
			continue
		}
		if _, ok := splitTemporalKeySpec(c.Properties); ok {
			out = append(out, c)
		}
	}
	return out
}

// makeTemporalDescriptor builds the index descriptor of one grouping key of a
// temporal constraint. Single-key constraints produce the same descriptor (and
// so the same persisted index keys) as before composite keys were supported;
// composite keys join their property names with temporalKeyPropSeparator and
// hash the []interface{} key value as a composite key.
func makeTemporalDescriptor(namespace string, source constraintDefinitionSource, keyValue interface{}) temporalIndexDescriptor {
	c := source.installedConstraint()

	spec, _ := splitTemporalKeySpec(c.Properties)
	return temporalIndexDescriptor{
		namespace: namespace,
		label:     c.Label,
		keyProp:   spec.indexKeyProp(),
		startProp: spec.startProp,
		endProp:   spec.endProp,
		keyHash:   spec.keyHash(keyValue),
	}
}

func encodeTemporalSortTime(t time.Time) []byte {
	buf := make([]byte, 8)
	value := uint64(t.UTC().UnixNano()) ^ (1 << 63)
	binary.BigEndian.PutUint64(buf, value)
	return buf
}

func temporalHistoryPrefix(desc temporalIndexDescriptor) []byte {
	key := make([]byte, 0, util.SafePreallocSum(2, len(desc.namespace)+len(desc.label)+len(desc.keyProp)+len(desc.startProp)+len(desc.endProp)+len(desc.keyHash), 6))
	key = append(key, prefixTemporalIndex)
	key = append(key, []byte(desc.namespace)...)
	key = append(key, 0x00)
	key = append(key, []byte(desc.label)...)
	key = append(key, 0x00)
	key = append(key, []byte(desc.keyProp)...)
	key = append(key, 0x00)
	key = append(key, []byte(desc.startProp)...)
	key = append(key, 0x00)
	key = append(key, []byte(desc.endProp)...)
	key = append(key, 0x00)
	key = append(key, []byte(desc.keyHash)...)
	key = append(key, 0x00)
	return key
}

func temporalHistoryKey(desc temporalIndexDescriptor, start time.Time, nodeID NodeID) []byte {
	key := temporalHistoryPrefix(desc)
	key = append(key, encodeTemporalSortTime(start)...)
	key = append(key, 0x00)
	key = append(key, []byte(nodeID)...)
	return key
}

func temporalCurrentKey(desc temporalIndexDescriptor) []byte {
	key := make([]byte, 0, util.SafePreallocSum(2, len(desc.namespace)+len(desc.label)+len(desc.keyProp)+len(desc.startProp)+len(desc.endProp)+len(desc.keyHash), 6))
	key = append(key, prefixTemporalHead)
	key = append(key, []byte(desc.namespace)...)
	key = append(key, 0x00)
	key = append(key, []byte(desc.label)...)
	key = append(key, 0x00)
	key = append(key, []byte(desc.keyProp)...)
	key = append(key, 0x00)
	key = append(key, []byte(desc.startProp)...)
	key = append(key, 0x00)
	key = append(key, []byte(desc.endProp)...)
	key = append(key, 0x00)
	key = append(key, []byte(desc.keyHash)...)
	return key
}

func extractNodeIDFromTemporalHistoryKey(key []byte, prefixLen int) NodeID {
	offset := prefixLen + 8 + 1
	if offset >= len(key) {
		return ""
	}
	return NodeID(key[offset:])
}

// temporalNodeState returns the grouping key value (see temporalKeySpec) and
// validity window of node under temporal constraint c; ok is false when the
// key is incomplete or the start is not a temporal value.
func temporalNodeState(node *Node, source constraintDefinitionSource) (interface{}, time.Time, time.Time, bool, bool) {
	c := source.installedConstraint()

	spec, ok := splitTemporalKeySpec(c.Properties)
	if node == nil || !ok {
		return nil, time.Time{}, time.Time{}, false, false
	}
	keyValue, missing := spec.keyValue(node.Properties)
	if missing != "" {
		return nil, time.Time{}, time.Time{}, false, false
	}
	interval, ok := spec.interval(node.Properties)
	if !ok {
		return nil, time.Time{}, time.Time{}, false, false
	}
	return keyValue, interval.start, interval.end, interval.hasEnd, true
}

// temporalKeyValuesEqual compares two grouping key values of constraint c.
func temporalKeyValuesEqual(source constraintDefinitionSource, a, b interface{}) bool {
	c := source.installedConstraint()

	spec, ok := splitTemporalKeySpec(c.Properties)
	return ok && spec.keyEqual(a, b)
}

func temporalTargetForNode(namespace string, node *Node, source constraintDefinitionSource) (temporalRefreshTarget, time.Time, bool) {
	c := source.installedConstraint()

	keyValue, start, _, _, ok := temporalNodeState(node, c)
	if !ok {
		return temporalRefreshTarget{}, time.Time{}, false
	}
	return temporalRefreshTarget{
		constraint: c,
		desc:       makeTemporalDescriptor(namespace, c, keyValue),
		keyValue:   keyValue,
	}, start, true
}

func temporalTargetMapKey(target temporalRefreshTarget) string {
	return string(temporalCurrentKey(target.desc))
}

func mergeTemporalTargets(targets map[string]temporalRefreshTarget, target temporalRefreshTarget) {
	if targets == nil {
		return
	}
	targets[temporalTargetMapKey(target)] = target
}

func (b *BadgerEngine) loadNodeForTemporalTxn(txn *badger.Txn, nodeID NodeID, withEmbeddings bool) (*Node, error) {
	item, err := txn.Get(nodeKey(nodeID))
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var node *Node
	err = item.Value(func(val []byte) error {
		var decodeErr error
		if withEmbeddings {
			node, decodeErr = b.decodeNodeWithEmbeddings(txn, val, nodeID)
		} else {
			node, decodeErr = b.decodeNode(namespaceForNodeID(nodeID), val)
		}
		return decodeErr
	})
	if err != nil {
		return nil, err
	}
	return node, nil
}

func nodeMatchesTemporalLookup(node *Node, source constraintDefinitionSource, keyValue interface{}, asOf time.Time) bool {
	c := source.installedConstraint()

	if node == nil {
		return false
	}
	value, start, end, hasEnd, ok := temporalNodeState(node, c)
	if !ok || !temporalKeyValuesEqual(c, value, keyValue) {
		return false
	}
	if asOf.Before(start) {
		return false
	}
	if hasEnd && !asOf.Before(end) {
		return false
	}
	return true
}

// temporalHistorySelection describes either an as-of body lookup or the two
// neighbours needed by overlap validation, within the caller's transaction.
type temporalHistorySelection struct {
	at             time.Time
	exclude        map[NodeID]struct{}
	excludeNodeID  NodeID
	withEmbeddings bool
	adjacent       bool
	latest         func(*Node) (bool, error)
	touched        map[NodeID]bool
	ctx            context.Context
}

// visitTemporalHistoryNodeInTxn owns history traversal and retained-body
// materialization. It skips missing/excluded bodies before validating periods;
// an adjacent search shares the backward scan and adds a bounded forward scan.
func (b *BadgerEngine) visitTemporalHistoryNodeInTxn(txn *badger.Txn, target temporalRefreshTarget, selection temporalHistorySelection) (*Node, *Node, error) {
	prefix := temporalHistoryPrefix(target.desc)
	var encoded, seek []byte
	if selection.latest == nil {
		encoded = encodeTemporalSortTime(selection.at)
		seek = append(append([]byte{}, prefix...), encoded...)
		seek = append(seek, 0xFF)
	}
	var previous, next *Node
	forward := false
	accept := func(item *badger.Item) (bool, error) {
		id := extractNodeIDFromTemporalHistoryKey(item.Key(), len(prefix))
		_, excluded := selection.exclude[id]
		if id == "" || excluded || selection.touched[id] || (selection.excludeNodeID != "" && id == selection.excludeNodeID) {
			return true, nil
		}
		node, err := b.loadNodeForTemporalTxn(txn, id, selection.withEmbeddings)
		if err != nil {
			return false, err
		}
		if node == nil {
			return true, nil
		}
		if selection.latest != nil {
			return selection.latest(node)
		}
		if selection.adjacent {
			key, start, _, _, ok := temporalNodeState(node, target.constraint)
			if !ok || !temporalKeyValuesEqual(target.constraint, key, target.keyValue) || (forward && start.Before(selection.at)) {
				return true, nil
			}
		} else if !nodeMatchesTemporalLookup(node, target.constraint, target.keyValue, selection.at) {
			return true, nil
		}
		if forward {
			next = node
		} else {
			previous = node
		}
		return false, nil
	}
	if selection.latest == nil {
		if err := descendingPrefixKeys(txn, prefix, seek, accept); err != nil {
			return nil, nil, err
		}
		if !selection.adjacent {
			return previous, nil, nil
		}
	}
	forward = true
	opts := badgerPrefixIteratorOptions(prefix)
	it := txn.NewIterator(opts)
	defer it.Close()
	if selection.latest != nil {
		seek = prefix
	} else {
		seek = append(append([]byte{}, prefix...), encoded...)
	}
	for it.Seek(seek); it.ValidForPrefix(prefix); it.Next() {
		if selection.ctx != nil {
			if err := selection.ctx.Err(); err != nil {
				return nil, nil, err
			}
		}
		more, err := accept(it.Item())
		if err != nil {
			return nil, nil, err
		}
		if !more {
			break
		}
	}
	return previous, next, nil
}

func (b *BadgerEngine) writeTemporalIndexForNodeInTxn(txn *badger.Txn, namespace string, node *Node, source constraintDefinitionSource) (temporalRefreshTarget, error) {
	c := source.installedConstraint()

	target, start, ok := temporalTargetForNode(namespace, node, c)
	if !ok {
		return temporalRefreshTarget{}, nil
	}
	if err := txn.Set(temporalHistoryKey(target.desc, start, node.ID), []byte{}); err != nil {
		return temporalRefreshTarget{}, err
	}
	return target, nil
}

func (b *BadgerEngine) deleteTemporalIndexForNodeInTxn(txn *badger.Txn, namespace string, node *Node, source constraintDefinitionSource) (temporalRefreshTarget, error) {
	c := source.installedConstraint()

	target, start, ok := temporalTargetForNode(namespace, node, c)
	if !ok {
		return temporalRefreshTarget{}, nil
	}
	if err := txn.Delete(temporalHistoryKey(target.desc, start, node.ID)); err != nil && err != badger.ErrKeyNotFound {
		return temporalRefreshTarget{}, err
	}
	return target, nil
}

func (b *BadgerEngine) applyTemporalIndexesForNodeChangeInTxn(txn *badger.Txn, namespace string, schema *SchemaManager, oldNode, newNode *Node) error {
	targets := make(map[string]temporalRefreshTarget)
	if oldNode != nil {
		for _, c := range temporalConstraintsForLabels(schema, oldNode.Labels) {
			target, err := b.deleteTemporalIndexForNodeInTxn(txn, namespace, oldNode, c)
			if err != nil {
				return err
			}
			if target.constraint.Name != "" {
				mergeTemporalTargets(targets, target)
			}
		}
	}
	if newNode != nil {
		for _, c := range temporalConstraintsForLabels(schema, newNode.Labels) {
			target, err := b.writeTemporalIndexForNodeInTxn(txn, namespace, newNode, c)
			if err != nil {
				return err
			}
			if target.constraint.Name != "" {
				mergeTemporalTargets(targets, target)
			}
		}
	}
	return b.refreshTemporalCurrentPointers(&batchWriter{txn: txn}, targets, temporalPointerState{readTxn: txn, at: time.Now().UTC()})
}

func (b *BadgerEngine) GetTemporalNodeAsOfInNamespace(namespace, label, keyProp string, keyValue interface{}, validFromProp, validToProp string, asOf time.Time) (*Node, error) {
	if err := b.pendingNamespaceConflict(namespace); err != nil {
		return nil, err
	}

	if err := b.ensureOpen(false); err != nil {
		return nil, err
	}
	schema := b.GetSchemaForNamespace(namespace)
	constraint, ok := temporalConstraintForLookup(schema, label, keyProp, validFromProp, validToProp)
	if !ok {
		return nil, nil
	}
	spec, _ := splitTemporalKeySpec(constraint.Properties)
	if spec.listBearing(keyValue) {
		return b.temporalListNodeAsOfInNamespace(namespace, constraint, keyValue, asOf, true)
	}
	target := temporalRefreshTarget{
		constraint: constraint,
		desc:       makeTemporalDescriptor(namespace, constraint, keyValue),
		keyValue:   keyValue,
	}
	var node *Node
	err := b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(temporalCurrentKey(target.desc))
		if err == nil {
			var currentID NodeID
			if err := item.Value(func(val []byte) error {
				currentID = NodeID(append([]byte(nil), val...))
				return nil
			}); err != nil {
				return err
			}
			currentNode, err := b.loadNodeForTemporalTxn(txn, currentID, true)
			if err != nil {
				return err
			}
			if nodeMatchesTemporalLookup(currentNode, target.constraint, keyValue, asOf) {
				node = currentNode
				return nil
			}
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		node, _, err = b.visitTemporalHistoryNodeInTxn(txn, target, temporalHistorySelection{at: asOf, withEmbeddings: true})
		return err
	})
	if err != nil {
		return nil, err
	}
	return node, nil
}

func qualifyTemporalNodeID(namespace string, nodeID NodeID) NodeID {
	if nodeID == "" || namespace == "" {
		return nodeID
	}
	if dbName, _, ok := ParseDatabasePrefix(string(nodeID)); ok && dbName == namespace {
		return nodeID
	}
	return NodeID(namespace + ":" + string(nodeID))
}

func (b *BadgerEngine) currentTemporalNodeByScanInNamespace(namespace string, source constraintDefinitionSource, keyValue interface{}, asOf time.Time) (*Node, error) {
	constraint := source.installedConstraint()

	nodes, err := b.GetNodesByLabel(constraint.Label)
	if err != nil {
		return nil, err
	}
	var best *Node
	var bestStart time.Time
	for _, candidate := range nodes {
		if candidate == nil {
			continue
		}
		dbName, _, ok := ParseDatabasePrefix(string(candidate.ID))
		if !ok || dbName != namespace {
			continue
		}
		if !nodeMatchesTemporalLookup(candidate, constraint, keyValue, asOf) {
			continue
		}
		_, start, _, _, ok := temporalNodeState(candidate, constraint)
		if !ok {
			continue
		}
		if best == nil || start.After(bestStart) {
			best = candidate
			bestStart = start
		}
	}
	return best, nil
}

// temporalListNodeAsOfInNamespace shares the existing descending history reader
// across list as-of/current lookups. A current hint can hide a later historical
// holder when an upgrade merges overlapping old-format list histories.
func (b *BadgerEngine) temporalListNodeAsOfInNamespace(namespace string, source constraintDefinitionSource, keyValue interface{}, asOf time.Time, withEmbeddings bool) (*Node, error) {
	constraint := source.installedConstraint()

	if b.writeBehindEnabled() || !b.temporalIndexAuthoritative(b.GetSchemaForNamespace(namespace), constraint) {
		node, err := b.currentTemporalNodeByScanInNamespace(namespace, constraint, keyValue, asOf)
		if err != nil || node == nil || !withEmbeddings {
			return node, err
		}
		return b.GetNode(node.ID)
	}
	target := temporalRefreshTarget{constraint: constraint, desc: makeTemporalDescriptor(namespace, constraint, keyValue), keyValue: keyValue}
	var node *Node
	err := b.withView(func(txn *badger.Txn) error {
		var err error
		node, _, err = b.visitTemporalHistoryNodeInTxn(txn, target, temporalHistorySelection{at: asOf, withEmbeddings: withEmbeddings})
		return err
	})
	return node, err
}

// IsCurrentTemporalNodeInNamespace reports whether the given temporal node is the live/current version.
func (b *BadgerEngine) IsCurrentTemporalNodeInNamespace(namespace string, node *Node, asOf time.Time) (bool, error) {
	if err := b.pendingNamespaceConflict(namespace); err != nil {
		return false, err
	}

	if err := b.ensureOpen(false); err != nil {
		return false, err
	}
	if node == nil {
		return false, nil
	}
	schema := b.GetSchemaForNamespace(namespace)
	constraints := temporalConstraintsForLabels(schema, node.Labels)
	if len(constraints) == 0 {
		return true, nil
	}
	qualifiedID := qualifyTemporalNodeID(namespace, node.ID)
	matched := false
	for _, constraint := range constraints {
		keyValue, _, _, _, ok := temporalNodeState(node, constraint)
		if !ok {
			continue
		}
		matched = true
		target := temporalRefreshTarget{
			constraint: constraint,
			desc:       makeTemporalDescriptor(namespace, constraint, keyValue),
			keyValue:   keyValue,
		}
		var current *Node
		var err error
		spec, _ := splitTemporalKeySpec(constraint.Properties)
		if spec.listBearing(keyValue) {
			current, err = b.temporalListNodeAsOfInNamespace(namespace, constraint, keyValue, asOf, false)
		} else {
			err = b.withView(func(txn *badger.Txn) error {
				item, err := txn.Get(temporalCurrentKey(target.desc))
				if err == nil {
					var currentID NodeID
					if err := item.Value(func(val []byte) error {
						currentID = NodeID(append([]byte(nil), val...))
						return nil
					}); err != nil {
						return err
					}
					currentNode, err := b.loadNodeForTemporalTxn(txn, currentID, false)
					if err != nil {
						return err
					}
					if nodeMatchesTemporalLookup(currentNode, target.constraint, target.keyValue, asOf) {
						current = currentNode
						return nil
					}
				} else if err != badger.ErrKeyNotFound {
					return err
				}
				current, _, err = b.visitTemporalHistoryNodeInTxn(txn, target, temporalHistorySelection{at: asOf})
				return err
			})
		}
		if err != nil {
			return false, err
		}
		if current == nil && nodeMatchesTemporalLookup(node, constraint, keyValue, asOf) {
			current, err = b.currentTemporalNodeByScanInNamespace(namespace, constraint, keyValue, asOf)
			if err != nil {
				return false, err
			}
		}
		if current == nil || current.ID != qualifiedID {
			return false, nil
		}
	}
	if !matched {
		return true, nil
	}
	return true, nil
}

// IsCurrentTemporalNode reports whether the given temporal node is the live/current version.
func (b *BadgerEngine) IsCurrentTemporalNode(node *Node, asOf time.Time) (bool, error) {
	if node == nil {
		return false, nil
	}
	namespace, _, ok := ParseDatabasePrefix(string(node.ID))
	if !ok {
		return true, nil
	}
	return b.IsCurrentTemporalNodeInNamespace(namespace, node, asOf)
}

func (b *BadgerEngine) clearBadgerPrefix(ctx context.Context, prefix byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := b.ensureOpen(false); err != nil {
		return err
	}
	wb := b.db.NewWriteBatch()
	defer wb.Cancel()
	prefixBytes := []byte{prefix}
	err := b.withView(func(txn *badger.Txn) error {
		opts := badgerIteratorOptions()
		opts.Prefix = prefixBytes
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := append([]byte(nil), it.Item().Key()...)
			if err := wb.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return wb.Flush()
}

func (b *BadgerEngine) rebuildTemporalNodeInTxn(txn *badger.Txn, node *Node, asOf time.Time) error {
	if node == nil {
		return nil
	}
	namespace, _, ok := ParseDatabasePrefix(string(node.ID))
	if !ok {
		return nil
	}
	schema := b.GetSchemaForNamespace(namespace)
	constraints := temporalConstraintsForLabels(schema, node.Labels)
	if len(constraints) == 0 {
		return nil
	}
	for _, constraint := range constraints {
		target, start, ok := temporalTargetForNode(namespace, node, constraint)
		if !ok {
			continue
		}
		if err := txn.Set(temporalHistoryKey(target.desc, start, node.ID), []byte{}); err != nil {
			return err
		}
		if !nodeMatchesTemporalLookup(node, constraint, target.keyValue, asOf) {
			continue
		}
		currentKey := temporalCurrentKey(target.desc)
		item, err := txn.Get(currentKey)
		if err == nil {
			var existingID NodeID
			if err := item.Value(func(val []byte) error {
				existingID = NodeID(append([]byte(nil), val...))
				return nil
			}); err != nil {
				return err
			}
			existingNode, err := b.loadNodeForTemporalTxn(txn, existingID, false)
			if err != nil {
				return err
			}
			if existingNode != nil {
				_, existingStart, _, _, ok := temporalNodeState(existingNode, constraint)
				if ok && !start.After(existingStart) {
					continue
				}
			}
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		if err := txn.Set(currentKey, []byte(node.ID)); err != nil {
			return err
		}
	}
	return nil
}

// RebuildTemporalIndexes recreates the temporal history and current-pointer indexes from stored nodes.
func (b *BadgerEngine) RebuildTemporalIndexes(ctx context.Context) error {
	if err := b.ensureOpen(false); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// The index is replaced wholesale from the stored nodes: a maintenance
	// write (beginMaintenanceWrite), so every write-behind commit
	// acknowledged before it is applied first and no write runs across it.
	lifecycle, err := b.beginMaintenanceWrite(nil)
	if err != nil {
		return err
	}
	defer lifecycle.finish()
	// The index is cleared and refilled below: until that finishes no
	// constraint's index may be trusted to be complete.
	b.temporalIndexReady.Store(false)
	b.resetTemporalIndexCompleteness()
	// A failed rebuild must not leave a current-format marker that a later
	// clean shutdown could certify over partially cleared/refilled indexes.
	if err := b.withUpdate(func(txn *badger.Txn) error {
		return txn.Delete(temporalIndexFormatKey)
	}); err != nil {
		return err
	}
	if err := b.clearBadgerPrefix(ctx, prefixTemporalIndex); err != nil {
		return err
	}
	if err := b.clearBadgerPrefix(ctx, prefixTemporalHead); err != nil {
		return err
	}
	asOf := time.Now().UTC()
	// The store's own nodes, every one (no decay filter): StreamStoredNodes on b.
	if err := b.StreamNodesWithOptions(ctx, StreamNodesOptions{WithEmbeddings: true}, func(node *Node) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		maintenancePause(pauseTemporalRebuildNode)
		return b.withUpdate(func(txn *badger.Txn) error {
			return b.rebuildTemporalNodeInTxn(txn, node, asOf)
		})
	}); err != nil {
		return err
	}
	// Every node of every temporal constraint is indexed now, in the current
	// key format; the per-schema runtime marks are superseded.
	if err := b.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(temporalIndexFormatKey, temporalIndexFormatCurrent)
	}); err != nil {
		return err
	}
	b.resetTemporalIndexCompleteness()
	b.temporalIndexReady.Store(true)
	return nil
}

// temporalIndexFormatKey records the key format of the stored temporal
// history index. Format 4 adds canonical list tokens to format 2's stable scalar
// digests and makes equal composite/list-bearing keys share one whole-key hash.
// Older list histories/current pointers must be rebuilt before trust, including
// after a clean shutdown. Pure-scalar hash bytes are unchanged.
var (
	temporalIndexFormatKey     = []byte{prefixMVCCMeta, prefixMVCCMetaTemporalIndexFormat}
	temporalIndexFormatCurrent = []byte{4}
)

// temporalIndexFormatIsCurrent reports whether the stored temporal index was
// built in the current key format.
func (b *BadgerEngine) temporalIndexFormatIsCurrent() (bool, error) {
	current := false
	err := b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(temporalIndexFormatKey)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			current = bytes.Equal(v, temporalIndexFormatCurrent)
			return nil
		})
	})
	return current, err
}

// resetTemporalIndexCompleteness drops the runtime completeness marks of
// every loaded schema.
func (b *BadgerEngine) resetTemporalIndexCompleteness() {
	b.schemasMu.RLock()
	schemas := make([]*SchemaManager, 0, len(b.schemas))
	for _, sm := range b.schemas {
		schemas = append(schemas, sm)
	}
	b.schemasMu.RUnlock()
	for _, sm := range schemas {
		sm.mu.Lock()
		sm.temporalIndexCompleteKeys = nil
		sm.temporalIndexIncompleteKeys = nil
		sm.mu.Unlock()
	}
}

type temporalPruneCandidate struct {
	id    NodeID
	start time.Time
	end   time.Time
}

type temporalPruneGroup struct {
	key        interface{}
	candidates []temporalPruneCandidate
}

// PruneTemporalHistory removes older closed temporal versions according to opts.
func (b *BadgerEngine) PruneTemporalHistory(ctx context.Context, opts TemporalPruneOptions) (int64, error) {
	if err := b.ensureOpen(false); err != nil {
		return 0, err
	}
	if opts.MaxVersionsPerKey <= 0 && opts.MinRetentionAge <= 0 {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now := time.Now().UTC()
	groups := make(map[string][]*temporalPruneGroup)
	err := StreamStoredNodes(ctx, b, StreamNodesOptions{WithEmbeddings: true}, func(node *Node) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if node == nil {
			return nil
		}
		namespace, _, ok := ParseDatabasePrefix(string(node.ID))
		if !ok {
			return nil
		}
		schema := b.GetSchemaForNamespace(namespace)
		for _, constraint := range temporalConstraintsForLabels(schema, node.Labels) {
			keyValue, start, end, hasEnd, ok := temporalNodeState(node, constraint)
			if !ok || !hasEnd {
				continue
			}
			target := temporalRefreshTarget{
				constraint: constraint,
				desc:       makeTemporalDescriptor(namespace, constraint, keyValue),
				keyValue:   keyValue,
			}
			bucket := temporalTargetMapKey(target)
			var group *temporalPruneGroup
			for _, existing := range groups[bucket] {
				if temporalKeyValuesEqual(constraint, existing.key, keyValue) {
					group = existing
					break
				}
			}
			if group == nil {
				group = &temporalPruneGroup{key: keyValue}
				groups[bucket] = append(groups[bucket], group)
			}
			group.candidates = append(group.candidates, temporalPruneCandidate{
				id:    node.ID,
				start: start,
				end:   end,
			})
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	cutoff := time.Time{}
	if opts.MinRetentionAge > 0 {
		cutoff = now.Add(-opts.MinRetentionAge)
	}
	deleteIDs := make(map[NodeID]struct{})
	for _, bucket := range groups {
		for _, group := range bucket {
			candidates := group.candidates
			sort.Slice(candidates, func(i, j int) bool {
				return candidates[i].start.After(candidates[j].start)
			})
			keptClosed := 0
			for _, candidate := range candidates {
				if !cutoff.IsZero() && candidate.end.After(cutoff) {
					continue
				}
				if opts.MaxVersionsPerKey > 0 && keptClosed < opts.MaxVersionsPerKey {
					keptClosed++
					continue
				}
				deleteIDs[candidate.id] = struct{}{}
			}
		}
	}
	var deleted int64
	for nodeID := range deleteIDs {
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		if err := b.DeleteNode(nodeID); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func (tx *BadgerTransaction) bufferTemporalIndexWrites() (map[string]temporalRefreshTarget, error) {
	targets := make(map[string]temporalRefreshTarget)
	for _, op := range tx.operations {
		switch op.Type {
		case OpCreateNode:
			if op.Node == nil {
				continue
			}
			dbName, _, ok := ParseDatabasePrefix(string(op.Node.ID))
			if !ok {
				continue
			}
			schema := tx.engine.GetSchemaForNamespace(dbName)
			for _, c := range temporalConstraintsForLabels(schema, op.Node.Labels) {
				target, start, ok := temporalTargetForNode(dbName, op.Node, c)
				if !ok {
					continue
				}
				tx.bufferSet(temporalHistoryKey(target.desc, start, op.Node.ID), []byte{})
				mergeTemporalTargets(targets, target)
			}
		case OpUpdateNode:
			if op.OldNode != nil {
				dbName, _, ok := ParseDatabasePrefix(string(op.OldNode.ID))
				if ok {
					schema := tx.engine.GetSchemaForNamespace(dbName)
					for _, c := range temporalConstraintsForLabels(schema, op.OldNode.Labels) {
						target, start, ok := temporalTargetForNode(dbName, op.OldNode, c)
						if !ok {
							continue
						}
						tx.bufferDelete(temporalHistoryKey(target.desc, start, op.OldNode.ID))
						mergeTemporalTargets(targets, target)
					}
				}
			}
			if op.Node != nil {
				dbName, _, ok := ParseDatabasePrefix(string(op.Node.ID))
				if ok {
					schema := tx.engine.GetSchemaForNamespace(dbName)
					for _, c := range temporalConstraintsForLabels(schema, op.Node.Labels) {
						target, start, ok := temporalTargetForNode(dbName, op.Node, c)
						if !ok {
							continue
						}
						tx.bufferSet(temporalHistoryKey(target.desc, start, op.Node.ID), []byte{})
						mergeTemporalTargets(targets, target)
					}
				}
			}
		case OpDeleteNode:
			if op.OldNode == nil {
				continue
			}
			dbName, _, ok := ParseDatabasePrefix(string(op.OldNode.ID))
			if !ok {
				continue
			}
			schema := tx.engine.GetSchemaForNamespace(dbName)
			for _, c := range temporalConstraintsForLabels(schema, op.OldNode.Labels) {
				target, start, ok := temporalTargetForNode(dbName, op.OldNode, c)
				if !ok {
					continue
				}
				tx.bufferDelete(temporalHistoryKey(target.desc, start, op.OldNode.ID))
				mergeTemporalTargets(targets, target)
			}
		}
	}
	return targets, nil
}

// temporalPointerState keeps direct transaction reads and commit-overlay reads
// explicit while both publish through the same target and batch-writer owner.
type temporalPointerState struct {
	readTxn *badger.Txn
	at      time.Time
	pending map[NodeID]*Node
	deleted map[NodeID]struct{}
}

// refreshTemporalCurrentPointers recomputes the current-version pointer of
// every temporal key the commit touched, writing through the commit writer w
// (see Commit); each pointer is its own unit.
func (b *BadgerEngine) refreshTemporalCurrentPointers(w *batchWriter, targets map[string]temporalRefreshTarget, state temporalPointerState) error {
	if len(targets) == 0 {
		return nil
	}
	now := state.at
	if now.IsZero() {
		now = time.Now().UTC()
	}
	publish := func(target temporalRefreshTarget, id NodeID) error {
		currentKey := temporalCurrentKey(target.desc)
		return w.write(func(txn *badger.Txn) error {
			if id == "" {
				err := txn.Delete(currentKey)
				if err == badger.ErrKeyNotFound {
					return nil
				}
				return err
			}
			return txn.Set(currentKey, []byte(id))
		})
	}
	// The direct operation already holds its transaction and target map. Keep
	// each result on the stack instead of allocating commit-overlay maps.
	if state.readTxn != nil {
		for _, target := range targets {
			node, _, err := b.visitTemporalHistoryNodeInTxn(state.readTxn, target, temporalHistorySelection{at: now})
			if err != nil {
				return err
			}
			var id NodeID
			if node != nil {
				id = node.ID
			}
			if err := publish(target, id); err != nil {
				return err
			}
		}
		return nil
	}
	exclude := make(map[NodeID]struct{}, util.SafePreallocSum(len(state.deleted), len(state.pending)))
	for nodeID := range state.deleted {
		exclude[nodeID] = struct{}{}
	}
	for nodeID := range state.pending {
		exclude[nodeID] = struct{}{}
	}

	// The current pending version of each target: one pass over the pending
	// nodes, keyed like the targets, instead of a pass per target (which made
	// a commit of n temporal nodes O(n^2)).
	type pendingCurrent struct {
		id    NodeID
		start time.Time
	}
	bestPending := make(map[string]pendingCurrent)
	for _, node := range state.pending {
		if node == nil {
			continue
		}
		namespace, _, ok := ParseDatabasePrefix(string(node.ID))
		if !ok {
			continue
		}
		for _, c := range temporalConstraintsForLabels(b.GetSchemaForNamespace(namespace), node.Labels) {
			target, _, ok := temporalTargetForNode(namespace, node, c)
			if !ok {
				continue
			}
			key := temporalTargetMapKey(target)
			if _, wanted := targets[key]; !wanted {
				continue
			}
			_, start, end, hasEnd, ok := temporalNodeState(node, c)
			if !ok || now.Before(start) || (hasEnd && !now.Before(end)) {
				continue
			}
			if best, seen := bestPending[key]; !seen || start.After(best.start) {
				bestPending[key] = pendingCurrent{id: node.ID, start: start}
			}
		}
	}

	// The current committed version of each target the transaction has no
	// current pending version for. The transaction's own nodes are excluded,
	// so this reads committed history only, through one read-only view: an
	// iterator on the commit's write batch would sort all of its pending
	// writes for every target.
	current := make(map[string]NodeID, len(targets))
	for key, p := range bestPending {
		current[key] = p.id
	}
	if len(current) < len(targets) {
		read := func(txn *badger.Txn) error {
			for key, target := range targets {
				if _, done := current[key]; done {
					continue
				}
				node, _, err := b.visitTemporalHistoryNodeInTxn(txn, target, temporalHistorySelection{at: now, exclude: exclude})
				if err != nil {
					return err
				}
				if node != nil {
					current[key] = node.ID
				}
			}
			return nil
		}
		if err := b.withView(read); err != nil {
			return err
		}
	}

	for _, target := range targets {
		if err := publish(target, current[temporalTargetMapKey(target)]); err != nil {
			return err
		}
	}
	return nil
}

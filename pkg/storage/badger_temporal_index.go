package storage

import (
	"context"
	"encoding/binary"
	"sort"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type temporalRefreshTarget struct {
	constraint Constraint
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

func temporalConstraintForLookup(schema *SchemaManager, label, keyProp, startProp, endProp string) (Constraint, bool) {
	if schema == nil {
		return Constraint{}, false
	}
	constraints := schema.GetConstraintsForLabels([]string{label})
	for _, c := range constraints {
		if c.Type != ConstraintTemporal || len(c.Properties) != 3 {
			continue
		}
		if c.Label == label && c.Properties[0] == keyProp && c.Properties[1] == startProp && c.Properties[2] == endProp {
			return c, true
		}
	}
	return Constraint{}, false
}

func temporalConstraintsForLabels(schema *SchemaManager, labels []string) []Constraint {
	if schema == nil {
		return nil
	}
	constraints := schema.GetConstraintsForLabels(labels)
	out := make([]Constraint, 0, len(constraints))
	for _, c := range constraints {
		if c.Type == ConstraintTemporal && len(c.Properties) == 3 {
			out = append(out, c)
		}
	}
	return out
}

func makeTemporalDescriptor(namespace string, c Constraint, keyValue interface{}) temporalIndexDescriptor {
	return temporalIndexDescriptor{
		namespace: namespace,
		label:     strings.ToLower(c.Label),
		keyProp:   c.Properties[0],
		startProp: c.Properties[1],
		endProp:   c.Properties[2],
		keyHash:   constraintValueKey(keyValue),
	}
}

func encodeTemporalSortTime(t time.Time) []byte {
	buf := make([]byte, 8)
	value := uint64(t.UTC().UnixNano()) ^ (1 << 63)
	binary.BigEndian.PutUint64(buf, value)
	return buf
}

func temporalHistoryPrefix(desc temporalIndexDescriptor) []byte {
	key := make([]byte, 0, 2+len(desc.namespace)+len(desc.label)+len(desc.keyProp)+len(desc.startProp)+len(desc.endProp)+len(desc.keyHash)+6)
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
	key := make([]byte, 0, 2+len(desc.namespace)+len(desc.label)+len(desc.keyProp)+len(desc.startProp)+len(desc.endProp)+len(desc.keyHash)+6)
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

func temporalNodeState(node *Node, c Constraint) (interface{}, time.Time, time.Time, bool, bool) {
	if node == nil || len(c.Properties) != 3 {
		return nil, time.Time{}, time.Time{}, false, false
	}
	keyValue, ok := node.Properties[c.Properties[0]]
	if !ok || keyValue == nil {
		return nil, time.Time{}, time.Time{}, false, false
	}
	start, ok := coerceTemporalTime(node.Properties[c.Properties[1]])
	if !ok {
		return nil, time.Time{}, time.Time{}, false, false
	}
	end, hasEnd := coerceTemporalTime(node.Properties[c.Properties[2]])
	return keyValue, start, end, hasEnd, true
}

func temporalTargetForNode(namespace string, node *Node, c Constraint) (temporalRefreshTarget, time.Time, bool) {
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

func nodeMatchesTemporalLookup(node *Node, c Constraint, keyValue interface{}, asOf time.Time) bool {
	if node == nil {
		return false
	}
	value, start, end, hasEnd, ok := temporalNodeState(node, c)
	if !ok || !compareValues(value, keyValue) {
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

func (b *BadgerEngine) temporalHistoryNodeAsOfInTxn(txn *badger.Txn, target temporalRefreshTarget, asOf time.Time, exclude map[NodeID]struct{}, withEmbeddings bool) (*Node, error) {
	prefix := temporalHistoryPrefix(target.desc)
	seek := append(append([]byte{}, prefix...), encodeTemporalSortTime(asOf)...)
	seek = append(seek, 0xFF)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Prefix = prefix
	opts.Reverse = true
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Seek(seek); it.ValidForPrefix(prefix); it.Next() {
		nodeID := extractNodeIDFromTemporalHistoryKey(it.Item().Key(), len(prefix))
		if nodeID == "" {
			continue
		}
		if exclude != nil {
			if _, skip := exclude[nodeID]; skip {
				continue
			}
		}
		node, err := b.loadNodeForTemporalTxn(txn, nodeID, withEmbeddings)
		if err != nil {
			return nil, err
		}
		if nodeMatchesTemporalLookup(node, target.constraint, target.keyValue, asOf) {
			return node, nil
		}
	}
	return nil, nil
}

func (b *BadgerEngine) temporalAdjacentNodesInTxn(txn *badger.Txn, target temporalRefreshTarget, start time.Time, excludeNodeID NodeID) (*Node, *Node, error) {
	prefix := temporalHistoryPrefix(target.desc)
	encodedStart := encodeTemporalSortTime(start)
	seekPrev := append(append([]byte{}, prefix...), encodedStart...)
	seekPrev = append(seekPrev, 0xFF)
	prevOpts := badger.DefaultIteratorOptions
	prevOpts.PrefetchValues = false
	prevOpts.Prefix = prefix
	prevOpts.Reverse = true
	prevIt := txn.NewIterator(prevOpts)
	defer prevIt.Close()
	var prevNode *Node
	for prevIt.Seek(seekPrev); prevIt.ValidForPrefix(prefix); prevIt.Next() {
		nodeID := extractNodeIDFromTemporalHistoryKey(prevIt.Item().Key(), len(prefix))
		if nodeID == "" || nodeID == excludeNodeID {
			continue
		}
		node, err := b.loadNodeForTemporalTxn(txn, nodeID, false)
		if err != nil {
			return nil, nil, err
		}
		if node != nil {
			prevNode = node
			break
		}
	}
	seekNext := append(append([]byte{}, prefix...), encodedStart...)
	forwardOpts := badger.DefaultIteratorOptions
	forwardOpts.PrefetchValues = false
	forwardOpts.Prefix = prefix
	forwardIt := txn.NewIterator(forwardOpts)
	defer forwardIt.Close()
	var nextNode *Node
	for forwardIt.Seek(seekNext); forwardIt.ValidForPrefix(prefix); forwardIt.Next() {
		nodeID := extractNodeIDFromTemporalHistoryKey(forwardIt.Item().Key(), len(prefix))
		if nodeID == "" || nodeID == excludeNodeID {
			continue
		}
		node, err := b.loadNodeForTemporalTxn(txn, nodeID, false)
		if err != nil {
			return nil, nil, err
		}
		if node == nil {
			continue
		}
		_, candidateStart, _, _, ok := temporalNodeState(node, target.constraint)
		if !ok {
			continue
		}
		if candidateStart.Before(start) {
			continue
		}
		nextNode = node
		break
	}
	return prevNode, nextNode, nil
}

func (b *BadgerEngine) refreshTemporalCurrentPointerInTxn(txn *badger.Txn, target temporalRefreshTarget, asOf time.Time, exclude map[NodeID]struct{}) error {
	currentKey := temporalCurrentKey(target.desc)
	node, err := b.temporalHistoryNodeAsOfInTxn(txn, target, asOf, exclude, false)
	if err != nil {
		return err
	}
	if node == nil {
		if err := txn.Delete(currentKey); err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		return nil
	}
	return txn.Set(currentKey, []byte(node.ID))
}

func (b *BadgerEngine) writeTemporalIndexForNodeInTxn(txn *badger.Txn, namespace string, node *Node, c Constraint) (temporalRefreshTarget, error) {
	target, start, ok := temporalTargetForNode(namespace, node, c)
	if !ok {
		return temporalRefreshTarget{}, nil
	}
	if err := txn.Set(temporalHistoryKey(target.desc, start, node.ID), []byte{}); err != nil {
		return temporalRefreshTarget{}, err
	}
	return target, nil
}

func (b *BadgerEngine) deleteTemporalIndexForNodeInTxn(txn *badger.Txn, namespace string, node *Node, c Constraint) (temporalRefreshTarget, error) {
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
	now := time.Now().UTC()
	for _, target := range targets {
		if err := b.refreshTemporalCurrentPointerInTxn(txn, target, now, nil); err != nil {
			return err
		}
	}
	return nil
}

func (b *BadgerEngine) GetTemporalNodeAsOfInNamespace(namespace, label, keyProp string, keyValue interface{}, validFromProp, validToProp string, asOf time.Time) (*Node, error) {
	if err := b.ensureOpen(); err != nil {
		return nil, err
	}
	schema := b.GetSchemaForNamespace(namespace)
	constraint, ok := temporalConstraintForLookup(schema, label, keyProp, validFromProp, validToProp)
	if !ok {
		return nil, nil
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
		node, err = b.temporalHistoryNodeAsOfInTxn(txn, target, asOf, nil, true)
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

func (b *BadgerEngine) currentTemporalNodeByScanInNamespace(namespace string, constraint Constraint, keyValue interface{}, asOf time.Time) (*Node, error) {
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

// IsCurrentTemporalNodeInNamespace reports whether the given temporal node is the live/current version.
func (b *BadgerEngine) IsCurrentTemporalNodeInNamespace(namespace string, node *Node, asOf time.Time) (bool, error) {
	if err := b.ensureOpen(); err != nil {
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
			current, err = b.temporalHistoryNodeAsOfInTxn(txn, target, asOf, nil, false)
			return err
		})
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
	wb := b.db.NewWriteBatch()
	defer wb.Cancel()
	prefixBytes := []byte{prefix}
	err := b.withView(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
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
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := b.clearBadgerPrefix(ctx, prefixTemporalIndex); err != nil {
		return err
	}
	if err := b.clearBadgerPrefix(ctx, prefixTemporalHead); err != nil {
		return err
	}
	asOf := time.Now().UTC()
	return StreamNodesWithFallback(ctx, b, 1000, func(node *Node) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return b.withUpdate(func(txn *badger.Txn) error {
			return b.rebuildTemporalNodeInTxn(txn, node, asOf)
		})
	})
}

type temporalPruneCandidate struct {
	id    NodeID
	start time.Time
	end   time.Time
}

// PruneTemporalHistory removes older closed temporal versions according to opts.
func (b *BadgerEngine) PruneTemporalHistory(ctx context.Context, opts TemporalPruneOptions) (int64, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}
	if opts.MaxVersionsPerKey <= 0 && opts.MinRetentionAge <= 0 {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now := time.Now().UTC()
	groups := make(map[string][]temporalPruneCandidate)
	err := StreamNodesWithFallback(ctx, b, 1000, func(node *Node) error {
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
			groups[temporalTargetMapKey(target)] = append(groups[temporalTargetMapKey(target)], temporalPruneCandidate{
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
	for _, candidates := range groups {
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

func (tx *BadgerTransaction) refreshTemporalCurrentPointers(targets map[string]temporalRefreshTarget) error {
	now := time.Now().UTC()
	exclude := make(map[NodeID]struct{}, len(tx.deletedNodes)+len(tx.pendingNodes))
	for nodeID := range tx.deletedNodes {
		exclude[nodeID] = struct{}{}
	}
	for nodeID := range tx.pendingNodes {
		exclude[nodeID] = struct{}{}
	}
	for _, target := range targets {
		if err := tx.engine.refreshTemporalCurrentPointerInTxn(tx.badgerTx, target, now, exclude); err != nil {
			return err
		}
		var bestPendingID NodeID
		var bestPendingStart time.Time
		for _, node := range tx.pendingNodes {
			if node == nil {
				continue
			}
			if !hasLabel(node.Labels, target.constraint.Label) {
				continue
			}
			keyValue, start, end, hasEnd, ok := temporalNodeState(node, target.constraint)
			if !ok || !compareValues(keyValue, target.keyValue) {
				continue
			}
			if now.Before(start) || (hasEnd && !now.Before(end)) {
				continue
			}
			if bestPendingID == "" || start.After(bestPendingStart) {
				bestPendingID = node.ID
				bestPendingStart = start
			}
		}
		if bestPendingID != "" {
			currentKey := temporalCurrentKey(target.desc)
			if err := tx.badgerTx.Set(currentKey, []byte(bestPendingID)); err != nil {
				return err
			}
		}
	}
	return nil
}

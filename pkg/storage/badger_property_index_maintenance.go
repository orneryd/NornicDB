package storage

// Property-index maintenance: deterministic, DDL-driven.
//
// The SchemaManager keeps a map of (label, property) → PropertyIndex
// populated by the user's `CREATE INDEX … FOR (n:Label) ON (n.prop)` DDL.
// Every node mutation — CreateNode, UpdateNode (with a known old node),
// DeleteNode — must be reflected in the indexes synchronously. Without
// that, reads that consult the index silently return empty — NOT because
// the value is absent, but because the maintenance path never ran.
//
// Rules:
//   - Walk each label on the node and each property present on the node.
//   - Consult `MaintainsPropertyIndex(label, property)` to decide whether
//     the (label, property) pair is indexed (filled or not, #875).
//   - Call the schema's Insert / Delete helpers only when an index exists
//     — the helpers are no-ops for unindexed pairs but an explicit gate
//     avoids the unnecessary map lookup on every CREATE.
//   - Namespace is derived from the node ID prefix (see
//     extractNamespaceFromID) so the correct SchemaManager is used.
//   - Errors are logged (via the engine logger) and NOT returned; a
//     single bad write must not block an entire bulk CREATE. The caller
//     can still detect failure via end-to-end correctness checks.
//
// This file deliberately stays storage-local — it references only
// *BadgerEngine / *SchemaManager / *Node, no cypher / knowledgepolicy
// imports.

import (
	"log/slog"
	"slices"

	"github.com/dgraph-io/badger/v4"
)

// maintainPropertyIndexesOnNodeCreated files the created node in every
// equality index that covers one of its labels, from its latest committed
// state (refileFromCommitted). Arity-1 indexes file the node under its
// single-property key; arity-N indexes under full/prefix.
func (b *BadgerEngine) maintainPropertyIndexesOnNodeCreated(node *Node) {
	if node == nil {
		return
	}
	sm := b.schemaForNodeID(node.ID)
	if sm == nil {
		return
	}
	b.maintainCompositeIndexesOnNodeCreated(node, sm)
	b.maintainContainedIndexes(sm, node.ID, nil, node)
}

// maintainCompositeIndexesOnNodeCreated files the node in every equality
// index that covers one of its labels. A later write of the node may have
// published first, so the committed state, not the created payload, is filed.
func (b *BadgerEngine) maintainCompositeIndexesOnNodeCreated(node *Node, sm *SchemaManager) {
	for _, label := range node.Labels {
		for _, idx := range sm.GetCompositeIndexesForLabel(label) {
			if idx == nil {
				continue
			}
			b.refileFromCommitted(idx, node.ID, nil, node.Properties)
		}
	}
}

// maintainPropertyIndexesOnNodeUpdated removes old index entries (when the
// oldNode snapshot is available) and inserts the current entries. Without
// the oldNode snapshot we can only do the insert half, which keeps new
// values findable but leaves any stale entries in place — correctness
// fallout from a missing diff source is the caller's problem.
func (b *BadgerEngine) maintainPropertyIndexesOnNodeUpdated(node, oldNode *Node) {
	if node == nil {
		return
	}
	sm := b.schemaForNodeID(node.ID)
	if sm == nil {
		return
	}
	b.maintainCompositeIndexesOnNodeUpdated(node, oldNode, sm)
	b.maintainContainedIndexes(sm, node.ID, oldNode, node)
}

// maintainCompositeIndexesOnNodeUpdated moves the node's entries from
// oldNode to its current state in every index of an old or new label whose
// entries change (refileFromCommitted). Without oldNode it only inserts the
// written node's entries.
func (b *BadgerEngine) maintainCompositeIndexesOnNodeUpdated(node, oldNode *Node, sm *SchemaManager) {
	if oldNode != nil {
		seen := make(map[*CompositeIndex]bool)
		for _, labels := range [][]string{oldNode.Labels, node.Labels} {
			for _, label := range labels {
				for _, idx := range sm.GetCompositeIndexesForLabel(label) {
					if idx == nil || seen[idx] {
						continue
					}
					seen[idx] = true
					wasIn, isIn := hasLabel(oldNode.Labels, idx.Label), hasLabel(node.Labels, idx.Label)
					if wasIn && isIn && idx.filesSameEntries(oldNode.Properties, node.Properties) {
						continue
					}
					var old, written map[string]interface{}
					if wasIn {
						old = oldNode.Properties
					}
					if isIn {
						written = node.Properties
					}
					b.refileFromCommitted(idx, node.ID, old, written)
				}
			}
		}
		return
	}
	b.maintainCompositeIndexesOnNodeCreated(node, sm)
}

// maintainPropertyIndexesOnNodeDeletedWithNode removes index entries for a
// deleted node using the caller's pre-delete snapshot. It is the one delete
// maintenance (cacheOnNodeDeletedWithNode): every delete path holds the
// deleted node, while the node cache is cleared before this runs and never
// held buffered write-behind nodes.
func (b *BadgerEngine) maintainPropertyIndexesOnNodeDeletedWithNode(nodeID NodeID, old *Node) {
	if old == nil || len(old.Labels) == 0 {
		return
	}
	sm := b.schemaForNodeID(nodeID)
	if sm == nil {
		return
	}
	b.maintainCompositeIndexesOnNodeDeleted(nodeID, old, sm)
	b.maintainContainedIndexes(sm, nodeID, old, nil)
}

// maintainCompositeIndexesOnNodeDeleted removes the node from every equality
// index its pre-delete snapshot could have been filed under, keeping what a
// later recreation of the ID holds (refileFromCommitted).
func (b *BadgerEngine) maintainCompositeIndexesOnNodeDeleted(id NodeID, old *Node, sm *SchemaManager) {
	for _, label := range old.Labels {
		for _, idx := range sm.GetCompositeIndexesForLabel(label) {
			if idx == nil {
				continue
			}
			b.refileFromCommitted(idx, id, old.Properties, nil)
		}
	}
}

// refileFromCommitted replaces the entries old filed for id in idx with
// those of the node's latest committed state. Index publication follows the
// commit, so a later write of the node (an update, or a create of a deleted
// ID) can publish before an earlier one; filing from the committed state,
// read under the index lock that publication also takes, converges on it
// in any order instead of removing the newer state's entries. If the read
// fails, the entries of both old and written (the operation's own result,
// nil when it leaves the index) are kept: a stale entry is re-checked by
// every read, a missing one hides the node.
func (b *BadgerEngine) refileFromCommitted(idx *CompositeIndex, id NodeID, old, written map[string]interface{}) {
	idx.RefileNode(id, old, func() []map[string]interface{} {
		current, err := b.committedNodeProperties(id, idx.Properties)
		if err != nil {
			b.log.Warn("composite index refile read failed",
				slog.String("component", "storage"),
				slog.String("index", idx.Name),
				slog.String("error", err.Error()))
			var keep []map[string]interface{}
			for _, properties := range []map[string]interface{}{old, written} {
				if properties != nil {
					keep = append(keep, properties)
				}
			}
			return keep
		}
		if current == nil || !hasLabel(current.Labels, idx.Label) {
			return nil
		}
		return []map[string]interface{}{current.Properties}
	})
}

// maintainContainedIndexes refiles id in every contained-key index of a
// label old or written carries (old is the pre-write node, nil for a create;
// written the result, nil for a delete) from the node's latest committed
// state, read under the index's lock (F-PUBLISH). An index whose keys the
// write leaves alone is skipped.
func (b *BadgerEngine) maintainContainedIndexes(sm *SchemaManager, id NodeID, old, written *Node) {
	seen := make(map[*ContainedIndex]bool)
	for _, node := range []*Node{old, written} {
		if node == nil {
			continue
		}
		for _, label := range node.Labels {
			for _, idx := range sm.GetContainedIndexesForLabel(label) {
				if seen[idx] {
					continue
				}
				seen[idx] = true
				wasIn := old != nil && hasLabel(old.Labels, idx.Label)
				isIn := written != nil && hasLabel(written.Labels, idx.Label)
				if wasIn && isIn && sameContainedKeys(idx.Properties, old.Properties, written.Properties) {
					continue
				}
				var fallback map[string]interface{}
				if isIn {
					fallback = written.Properties
				}
				idx.RefileNode(id, func() (map[string]interface{}, error) {
					current, err := b.committedNodeProperties(id, idx.Properties)
					if err != nil {
						b.log.Warn("contained index refile read failed",
							slog.String("component", "storage"),
							slog.String("index", idx.Name),
							slog.String("error", err.Error()))
						return nil, err
					}
					if current == nil || !hasLabel(current.Labels, idx.Label) {
						return nil, nil
					}
					return current.Properties, nil
				}, fallback)
			}
		}
	}
}

// sameContainedKeys reports whether two property maps give a node the same
// keys in a contained-key index over indexed.
func sameContainedKeys(indexed []string, first, second map[string]interface{}) bool {
	firstKeys, firstEmpty := containedKeys(indexed, first)
	secondKeys, secondEmpty := containedKeys(indexed, second)
	if firstEmpty != secondEmpty || len(firstKeys) != len(secondKeys) {
		return false
	}
	for _, key := range firstKeys {
		if !slices.Contains(secondKeys, key) {
			return false
		}
	}
	return true
}

// publishUniqueValues is the one post-commit maintenance of the unique-value
// registry, for direct writes, transaction commits and write-behind replay:
// it refiles id under every unique constraint of a label old or written
// carries (old is the pre-write node, nil for a create; written the result,
// nil for a delete) from the node's latest committed state.
func (b *BadgerEngine) publishUniqueValues(schema *SchemaManager, id NodeID, old, written *Node) {
	if schema == nil || !schema.HasUniqueValueTracking() {
		return
	}
	seen := make(map[[2]string]bool)
	for _, node := range []*Node{old, written} {
		if node == nil {
			continue
		}
		for _, label := range node.Labels {
			for property := range node.Properties {
				pair := [2]string{label, property}
				if seen[pair] {
					continue
				}
				seen[pair] = true
				var oldValue interface{}
				hadOld := false
				if old != nil && hasLabel(old.Labels, label) {
					oldValue, hadOld = old.Properties[property]
				}
				schema.RefileUniqueValue(label, property, id, oldValue, hadOld, func() (interface{}, bool) {
					current, err := b.committedNodeProperties(id, []string{property})
					if err != nil {
						b.log.Warn("unique value refile read failed",
							slog.String("component", "storage"),
							slog.String("label", label),
							slog.String("property", property),
							slog.String("error", err.Error()))
						if written != nil && hasLabel(written.Labels, label) {
							value, ok := written.Properties[property]
							return value, ok
						}
						return oldValue, hadOld
					}
					if current == nil || !hasLabel(current.Labels, label) {
						return nil, false
					}
					value, ok := current.Properties[property]
					return value, ok
				})
			}
		}
	}
}

// publishUniqueValuesForOps applies publishUniqueValues to a commit's node
// operations, in order.
func (b *BadgerEngine) publishUniqueValuesForOps(schema *SchemaManager, ops []Operation) {
	if schema == nil || !schema.HasUniqueValueTracking() {
		return
	}
	for _, op := range ops {
		switch op.Type {
		case OpCreateNode:
			if op.Node != nil {
				b.publishUniqueValues(schema, op.Node.ID, nil, op.Node)
			}
		case OpUpdateNode:
			if op.Node != nil {
				b.publishUniqueValues(schema, op.Node.ID, op.OldNode, op.Node)
			}
		case OpDeleteNode:
			if op.OldNode != nil {
				b.publishUniqueValues(schema, op.NodeID, op.OldNode, nil)
			}
		}
	}
}

// committedNodeProperties reads the node's latest committed body, decoding
// only the given properties; nil when the node does not exist. It bypasses
// the node caches, which publication updates after the commit.
func (b *BadgerEngine) committedNodeProperties(id NodeID, properties []string) (*Node, error) {
	include := make(map[string]struct{}, len(properties))
	for _, property := range properties {
		include[property] = struct{}{}
	}
	namespace, _, _ := ParseDatabasePrefix(string(id))
	var node *Node
	err := b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(id))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var decodeErr error
			node, decodeErr = b.decodeNodeProjected(namespace, val, include)
			return decodeErr
		})
	})
	return node, err
}

// schemaForNodeID returns the SchemaManager for the namespace extracted
// from a node ID (e.g. "nornic:abc" → "nornic"). Unnamespaced IDs fall
// back to the default namespace.
func (b *BadgerEngine) schemaForNodeID(id NodeID) *SchemaManager {
	ns := extractNamespaceFromID(string(id))
	if ns == "" {
		return nil
	}
	b.schemasMu.RLock()
	sm := b.schemas[ns]
	b.schemasMu.RUnlock()
	return sm
}

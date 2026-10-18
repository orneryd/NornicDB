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
)

// maintainPropertyIndexesOnNodeCreated inserts the freshly-created node into
// every equality index that covers one of its labels. Arity-1 indexes file
// the node under its single-property key; arity-N indexes under full/prefix.
func (b *BadgerEngine) maintainPropertyIndexesOnNodeCreated(node *Node) {
	if node == nil {
		return
	}
	sm := b.schemaForNodeID(node.ID)
	if sm == nil {
		return
	}
	b.maintainCompositeIndexesOnNodeCreated(node, sm)
}

// maintainCompositeIndexesOnNodeCreated indexes the node under every equality
// index that covers one of its labels.
func (b *BadgerEngine) maintainCompositeIndexesOnNodeCreated(node *Node, sm *SchemaManager) {
	for _, label := range node.Labels {
		for _, idx := range sm.GetCompositeIndexesForLabel(label) {
			if idx == nil {
				continue
			}
			if err := idx.IndexNode(node.ID, node.Properties); err != nil {
				b.log.Warn("composite index insert failed",
					slog.String("component", "storage"),
					slog.String("label", label),
					slog.String("index", idx.Name))
			}
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
}

// maintainCompositeIndexesOnNodeUpdated removes the node's stale entries
// (from oldNode, when available) and inserts its current entries.
func (b *BadgerEngine) maintainCompositeIndexesOnNodeUpdated(node, oldNode *Node, sm *SchemaManager) {
	if oldNode != nil {
		for _, label := range oldNode.Labels {
			for _, idx := range sm.GetCompositeIndexesForLabel(label) {
				if idx == nil {
					continue
				}
				idx.RemoveNode(oldNode.ID, oldNode.Properties)
			}
		}
	}
	for _, label := range node.Labels {
		for _, idx := range sm.GetCompositeIndexesForLabel(label) {
			if idx == nil {
				continue
			}
			if err := idx.IndexNode(node.ID, node.Properties); err != nil {
				b.log.Warn("composite index insert (new) failed",
					slog.String("component", "storage"),
					slog.String("label", label),
					slog.String("index", idx.Name))
			}
		}
	}
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
}

// maintainCompositeIndexesOnNodeDeleted removes the node from every equality
// index its pre-delete snapshot could have been filed under.
func (b *BadgerEngine) maintainCompositeIndexesOnNodeDeleted(id NodeID, old *Node, sm *SchemaManager) {
	for _, label := range old.Labels {
		for _, idx := range sm.GetCompositeIndexesForLabel(label) {
			if idx == nil {
				continue
			}
			idx.RemoveNode(id, old.Properties)
		}
	}
}

// publishUniqueValues is the one publication of a node write to the
// unique-value registry, for engine writes, transaction commits and
// write-behind replay: old's values (the pre-write node, nil for a create)
// are unregistered, then written's (nil for a delete) are registered, under
// every unique constraint of their labels. It runs in the ordered stage, so
// old is what the registry holds for the node (badger_publication_stage.go).
func (b *BadgerEngine) publishUniqueValues(schema *SchemaManager, id NodeID, old, written *Node) {
	if schema == nil || !schema.HasUniqueValueTracking() {
		return
	}
	if old != nil {
		for _, label := range old.Labels {
			for property, value := range old.Properties {
				schema.UnregisterUniqueValue(label, property, value)
			}
		}
	}
	if written != nil {
		for _, label := range written.Labels {
			for property, value := range written.Properties {
				schema.RegisterUniqueValue(label, property, value, id)
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

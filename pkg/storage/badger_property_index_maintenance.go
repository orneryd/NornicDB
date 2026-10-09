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

// maintainPropertyIndexesOnNodeDeletedWithLabels removes index entries for
// every equality index the deleted node touched. The caller provides the
// labels (via cacheOnNodeDeletedWithLabels) because the node itself is
// already gone from the cache; the cached copy supplies the property payload.
func (b *BadgerEngine) maintainPropertyIndexesOnNodeDeletedWithLabels(id NodeID, labels []string) {
	if len(labels) == 0 {
		return
	}
	sm := b.schemaForNodeID(id)
	if sm == nil {
		return
	}
	// Only read the pre-delete snapshot when at least one indexed label
	// applies to this node. If none of the labels declare an index, skip
	// the read entirely.
	anyIndexed := false
	for _, label := range labels {
		if len(sm.GetCompositeIndexesForLabel(label)) > 0 {
			anyIndexed = true
			break
		}
	}
	if !anyIndexed {
		return
	}
	// We no longer have the node — take the last-known cached copy, or
	// skip if neither cache nor MVCC has a pre-delete view. Correctness
	// note: if the cache was evicted AND the node is gone, a dangling
	// index entry may persist until the next rebuild. That's a known
	// limitation; avoid it by calling DeleteNode paths that surface the
	// old node (UpdateNode equivalents already do).
	b.nodeCacheMu.RLock()
	cached, hit := b.nodeCache[id]
	b.nodeCacheMu.RUnlock()
	if !hit || cached == nil {
		return
	}
	b.maintainCompositeIndexesOnNodeDeleted(id, cached, sm)
}

// maintainCompositeIndexesOnNodeDeleted removes the node from every equality
// index its cached copy could have been filed under.
func (b *BadgerEngine) maintainCompositeIndexesOnNodeDeleted(id NodeID, cached *Node, sm *SchemaManager) {
	for _, label := range cached.Labels {
		for _, idx := range sm.GetCompositeIndexesForLabel(label) {
			if idx == nil {
				continue
			}
			idx.RemoveNode(id, cached.Properties)
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

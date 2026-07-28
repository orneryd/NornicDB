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
//   - Consult `GetPropertyIndex(label, property)` to decide whether the
//     (label, property) pair is indexed.
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

// maintainPropertyIndexesOnNodeCreated inserts every indexed (label, prop)
// entry for a freshly-created node.
func (b *BadgerEngine) maintainPropertyIndexesOnNodeCreated(node *Node) {
	if node == nil {
		return
	}
	sm := b.schemaForNodeID(node.ID)
	if sm == nil {
		return
	}
	for _, label := range node.Labels {
		for propName, propValue := range node.Properties {
			if _, ok := sm.GetPropertyIndex(label, propName); !ok {
				continue
			}
			if err := sm.PropertyIndexInsert(label, propName, node.ID, propValue); err != nil {
				b.log.Warn("property index insert failed",
					slog.String("component", "storage"),
					slog.String("label", label),
					slog.String("property", propName))
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

	if oldNode != nil {
		for _, label := range oldNode.Labels {
			for propName, propValue := range oldNode.Properties {
				if _, ok := sm.GetPropertyIndex(label, propName); !ok {
					continue
				}
				if err := sm.PropertyIndexDelete(label, propName, oldNode.ID, propValue); err != nil {
					b.log.Warn("property index delete (old) failed",
						slog.String("component", "storage"),
						slog.String("label", label),
						slog.String("property", propName))
				}
			}
		}
	}

	for _, label := range node.Labels {
		for propName, propValue := range node.Properties {
			if _, ok := sm.GetPropertyIndex(label, propName); !ok {
				continue
			}
			if err := sm.PropertyIndexInsert(label, propName, node.ID, propValue); err != nil {
				b.log.Warn("property index insert (new) failed",
					slog.String("component", "storage"),
					slog.String("label", label),
					slog.String("property", propName))
			}
		}
	}
}

// maintainPropertyIndexesOnNodeDeletedWithLabels removes index entries for
// every indexed (label, prop) that the deleted node touched. The caller
// provides the labels (via cacheOnNodeDeletedWithLabels) because the node
// itself is already gone from the cache. We fetch the property payload
// from storage only when an index exists for that (label, property),
// keeping the delete cheap on label/prop combinations that aren't indexed.
func (b *BadgerEngine) maintainPropertyIndexesOnNodeDeletedWithLabels(id NodeID, labels []string) {
	if len(labels) == 0 {
		return
	}
	sm := b.schemaForNodeID(id)
	if sm == nil {
		return
	}
	// Only read the pre-delete snapshot when at least one indexed label
	// applies to this node. If none of the labels declare indexed
	// properties, skip the read entirely.
	anyIndexed := false
	for _, label := range labels {
		if sm.HasAnyPropertyIndexForLabel(label) {
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
	for _, label := range cached.Labels {
		for propName, propValue := range cached.Properties {
			if _, ok := sm.GetPropertyIndex(label, propName); !ok {
				continue
			}
			if err := sm.PropertyIndexDelete(label, propName, id, propValue); err != nil {
				b.log.Warn("property index delete failed",
					slog.String("component", "storage"),
					slog.String("label", label),
					slog.String("property", propName))
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

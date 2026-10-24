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
//   - Every write moves the node from its old entries to its new ones with
//     one CompositeIndex.ReplaceNode per index (replaceNodeIndexEntries), so
//     a concurrent seek never misses a node in between, and an unchanged key
//     is never removed.
//   - Namespace is derived from the node ID prefix (see
//     extractNamespaceFromID) so the correct SchemaManager is used.
//   - Maintenance cannot fail: a value with no index key (null, byte
//     arrays, ...) is simply not filed, so a bulk CREATE is never blocked.
//
// This file deliberately stays storage-local — it references only
// *BadgerEngine / *SchemaManager / *Node, no cypher / knowledgepolicy
// imports.

import (
	"slices"
	"sync/atomic"
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
	b.replaceNodeIndexEntries(node.ID, nil, node, sm)
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

// maintainCompositeIndexesOnNodeUpdated moves the node from its stale entries
// (from oldNode, when available) to its current entries.
func (b *BadgerEngine) maintainCompositeIndexesOnNodeUpdated(node, oldNode *Node, sm *SchemaManager) {
	b.replaceNodeIndexEntries(node.ID, oldNode, node, sm)
}

// maintainPropertyIndexesOnNodeDeletedWithNode removes index entries for a
// deleted node using the caller's pre-delete snapshot. The replay path uses
// it because a buffered node never entered the engine node cache, so the
// cache-based variant would leave its committed index entries dangling.
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
	b.replaceNodeIndexEntries(id, cached, nil, sm)
}

// replaceNodeIndexEntries is the one index-maintenance step of every node
// write: for each equality index covering a label of the old or the new body,
// it replaces the node's old entries with its new ones in one
// CompositeIndex.ReplaceNode. A nil body is no entries (create: no old body;
// delete: no new body); an index whose label only one body carries gets only
// that side.
func (b *BadgerEngine) replaceNodeIndexEntries(id NodeID, oldNode, newNode *Node, sm *SchemaManager) {
	var oldLabels, newLabels []string
	var oldProperties, newProperties map[string]interface{}
	if oldNode != nil {
		oldLabels, oldProperties = oldNode.Labels, oldNode.Properties
	}
	if newNode != nil {
		newLabels, newProperties = newNode.Labels, newNode.Properties
	}
	replace := func(label string) {
		before := oldProperties
		if !slices.Contains(oldLabels, label) {
			before = nil
		}
		after := newProperties
		if !slices.Contains(newLabels, label) {
			after = nil
		}
		for _, idx := range sm.GetCompositeIndexesForLabel(label) {
			if idx != nil {
				idx.ReplaceNode(id, before, after)
			}
		}
	}
	for _, label := range newLabels {
		replace(label)
	}
	for _, label := range oldLabels {
		if !slices.Contains(newLabels, label) {
			replace(label)
		}
	}
	if hook := nodeIndexEntriesReplacedHook.Load(); hook != nil {
		(*hook)(id)
	}
}

// nodeIndexEntriesReplacedHook lets storage tests pause a node write after its
// equality-index entries are replaced, while the writer is still publishing.
var nodeIndexEntriesReplacedHook atomic.Pointer[func(NodeID)]

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

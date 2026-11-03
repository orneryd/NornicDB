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
//   - A value with no index key (null, byte arrays, ...) is simply not
//     filed, so a bulk CREATE is never blocked. A failed replacement is
//     logged (via the engine logger) and NOT returned; the caller can still
//     detect failure via end-to-end correctness checks.
//   - ReplaceNode's keys belong to the write the engine already accepted, so
//     maintenance passes context.Background() rather than a request's
//     context.
//
// This file deliberately stays storage-local — it references only
// *BadgerEngine / *SchemaManager / *Node, no cypher / knowledgepolicy
// imports.

import (
	"context"
	"log/slog"
	"slices"
)

// publishIndexMembership consumes the canonical before/final frame once per
// entity. The operation stream is reserved for counts and notifications.
func (b *BadgerEngine) publishIndexMembership(states commitStates) {
	for id, pair := range states.nodes {
		schema := b.schemaForNodeID(id)
		if schema == nil {
			continue
		}
		b.replaceNodeIndexEntries(id, pair.before, pair.after, schema)
	}
	b.publishRelationshipIndexMembership(states)
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
			if idx == nil {
				continue
			}
			// The keys belong to a write the engine already accepted, so the
			// replacement has that write's lifetime: it cannot cancel.
			if err := idx.ReplaceNode(context.Background(), id, before, after); err != nil {
				b.log.Warn("composite index replace failed",
					slog.String("component", "storage"),
					slog.String("label", label),
					slog.String("index", idx.Name),
					slog.String("error", err.Error()))
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

// publishUniqueValues projects one atomic frame into the canonical claims.
// Changed predecessor claims retire before any final claim is acquired, so key
// transfers do not depend on mutation order. Recovery folds a bounded physical
// batch; its historical claims cannot overwrite another current holder.
// The accepted frame is folded on the caller's completion lifetime and
// completes with Background; ApplyUniqueValue can only return a preparation
// cancellation error, which this completion lifetime cannot have.
func (b *BadgerEngine) publishUniqueValues(schema *SchemaManager, states commitStates, action UniqueValueAction) {
	if schema == nil || !schema.HasUniqueValueTracking() {
		return
	}
	accepted := context.Background()
	for id, pair := range states.nodes {
		old := pair.before
		if old == nil {
			continue
		}
		for _, label := range old.Labels {
			for property, value := range old.Properties {
				// Keeping the claim is essential even when another constraint's
				// publication blocks: trusted readers must never see it absent.
				if retainsNodeUniqueClaim(accepted, schema, label, property, value, pair.after) {
					continue
				}
				_ = schema.ApplyUniqueValue(accepted, label, property, value, id, RetiredUniqueClaim)
			}
		}
	}
	for id, pair := range states.nodes {
		written := pair.after
		if written == nil {
			continue
		}
		for _, label := range written.Labels {
			for property, value := range written.Properties {
				_ = schema.ApplyUniqueValue(accepted, label, property, value, id, action)
			}
		}
	}
}

// retainsNodeUniqueClaim compares the canonical claim identity, not Go value
// identity: numeric widths, lists and temporal values use the same keys
// as validation and publication. Unconstrained properties have no claim to retire.
func retainsNodeUniqueClaim(ctx context.Context, schema *SchemaManager, label, property string, before any, after *Node) bool {
	if err := schema.mu.RLockContext(ctx); err != nil {
		return false
	}
	_, constrained := schema.uniqueConstraints[nodePropertyKey{label: label, property: property}]
	schema.mu.RUnlock()
	if !constrained {
		return true
	}
	if after == nil || !slices.Contains(after.Labels, label) {
		return false
	}
	next, exists := after.Properties[property]
	if !exists {
		return false
	}
	beforeKey, beforeOK, err := indexValueKey(ctx, before)
	if err != nil || !beforeOK {
		return false
	}
	afterKey, afterOK, err := indexValueKey(ctx, next)
	return err == nil && afterOK && beforeKey == afterKey
}

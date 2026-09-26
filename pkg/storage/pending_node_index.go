package storage

import (
	"sort"
	"strings"
)

// pendingNodeIndex indexes the nodes an AsyncEngine holds in its write
// cache: created or updated, committed to the caller, not yet flushed. The
// underlying engine maintains its label index, property indexes and unique
// constraint values only when a write reaches it, at flush time, so every
// read that consults those indexes merges this view in. It is the one
// pending-write view for label scans (#448), property-index seeks and
// ordered index scans, unique-value lookups, and the async engine's own
// uniqueness checks (#719).
//
// It is guarded by AsyncEngine.mu, and holds exactly the nodes in
// AsyncEngine.nodeCache: every cache write goes through add / replace, and
// every cache removal through remove.
type pendingNodeIndex struct {
	// byLabel: lower-cased label → pending node IDs (label scans).
	byLabel map[string]map[NodeID]bool
	// byValue: (label, property, value key) → pending node IDs.
	byValue map[pendingValueKey]map[NodeID]struct{}
	// byProperty: (label, property) → pending node ID → value key, for the
	// ordered and not-null scans of a property index.
	byProperty map[pendingPropertyKey]map[NodeID]interface{}
}

// pendingPropertyKey is a (label, property) pair; the label is as stored on
// the node, as in the schema's property indexes.
type pendingPropertyKey struct {
	label    string
	property string
}

// pendingValueKey is a (label, property, value) triple; value is the
// indexValueKey form, so it matches as the property indexes do.
type pendingValueKey struct {
	label    string
	property string
	value    interface{}
}

func newPendingNodeIndex() *pendingNodeIndex {
	return &pendingNodeIndex{
		byLabel:    make(map[string]map[NodeID]bool),
		byValue:    make(map[pendingValueKey]map[NodeID]struct{}),
		byProperty: make(map[pendingPropertyKey]map[NodeID]interface{}),
	}
}

// add lists node under its labels and each (label, property, value) with a
// comparable non-null value (the values the property indexes hold).
func (p *pendingNodeIndex) add(node *Node) {
	if node == nil {
		return
	}
	for _, label := range node.Labels {
		normalLabel := strings.ToLower(label)
		ids := p.byLabel[normalLabel]
		if ids == nil {
			ids = make(map[NodeID]bool)
			p.byLabel[normalLabel] = ids
		}
		ids[node.ID] = true
		for property, value := range node.Properties {
			valueKey, ok := indexValueKey(value)
			if !ok {
				continue
			}
			key := pendingValueKey{label: label, property: property, value: valueKey}
			valueIDs := p.byValue[key]
			if valueIDs == nil {
				valueIDs = make(map[NodeID]struct{})
				p.byValue[key] = valueIDs
			}
			valueIDs[node.ID] = struct{}{}
			pair := pendingPropertyKey{label: label, property: property}
			propertyIDs := p.byProperty[pair]
			if propertyIDs == nil {
				propertyIDs = make(map[NodeID]interface{})
				p.byProperty[pair] = propertyIDs
			}
			propertyIDs[node.ID] = valueKey
		}
	}
}

// remove drops the entries add made for node (the cached object being
// replaced or retired).
func (p *pendingNodeIndex) remove(node *Node) {
	if node == nil {
		return
	}
	for _, label := range node.Labels {
		normalLabel := strings.ToLower(label)
		if ids := p.byLabel[normalLabel]; ids != nil {
			delete(ids, node.ID)
			if len(ids) == 0 {
				delete(p.byLabel, normalLabel)
			}
		}
		for property, value := range node.Properties {
			valueKey, ok := indexValueKey(value)
			if !ok {
				continue
			}
			key := pendingValueKey{label: label, property: property, value: valueKey}
			if valueIDs := p.byValue[key]; valueIDs != nil {
				delete(valueIDs, node.ID)
				if len(valueIDs) == 0 {
					delete(p.byValue, key)
				}
			}
			pair := pendingPropertyKey{label: label, property: property}
			if propertyIDs := p.byProperty[pair]; propertyIDs != nil {
				delete(propertyIDs, node.ID)
				if len(propertyIDs) == 0 {
					delete(p.byProperty, pair)
				}
			}
		}
	}
}

// replace moves node's entries from the cached object prev to node.
func (p *pendingNodeIndex) replace(prev, node *Node) {
	p.remove(prev)
	p.add(node)
}

// pendingWriteSource is the write cache of a layer above the engine that
// maintains a schema's indexes (AsyncEngine). A schema it is attached to
// merges its pending writes into every index lookup (#719).
type pendingWriteSource interface {
	// readPendingWrites calls read with the pending writes held still: a
	// flush can't retire a pending node until read returns, so a lookup that
	// combines the index with the view sees each committed node once. read
	// gets a zero view when nothing is pending.
	readPendingWrites(read func(view pendingWriteView))
}

// pendingWriteView is one namespace's view of an AsyncEngine's pending
// writes, valid inside readPendingWrites.
type pendingWriteView struct {
	index *pendingNodeIndex
	// superseded reports a node with a pending create, update or delete: the
	// engine's index entries for it are stale.
	superseded func(NodeID) bool
	// namespace limits pending nodes to the schema's namespace: a node whose
	// ID carries another database's prefix is not listed.
	namespace string
}

// active reports whether there are pending writes to merge.
func (v pendingWriteView) active() bool {
	return v.index != nil
}

// keep reports whether an ID from the engine's index is current.
func (v pendingWriteView) keep(id NodeID) bool {
	return v.index == nil || !v.superseded(id)
}

// inNamespace reports whether a pending node belongs to the view's
// namespace. Unprefixed IDs (an engine scoped to one namespace) do.
func (v pendingWriteView) inNamespace(id NodeID) bool {
	namespace, _, prefixed := ParseDatabasePrefix(string(id))
	return !prefixed || namespace == v.namespace
}

// valueMatches appends the namespace's pending nodes with label and
// property = valueKey to out.
func (v pendingWriteView) valueMatches(out []NodeID, label, property string, valueKey interface{}) []NodeID {
	if v.index == nil {
		return out
	}
	for id := range v.index.byValue[pendingValueKey{label: label, property: property, value: valueKey}] {
		if v.inNamespace(id) {
			out = append(out, id)
		}
	}
	return out
}

// propertyEntries returns the namespace's pending nodes with label and a
// non-null property, grouped by value key.
func (v pendingWriteView) propertyEntries(label, property string) map[interface{}][]NodeID {
	if v.index == nil {
		return nil
	}
	var entries map[interface{}][]NodeID
	for id, valueKey := range v.index.byProperty[pendingPropertyKey{label: label, property: property}] {
		if !v.inNamespace(id) {
			continue
		}
		if entries == nil {
			entries = make(map[interface{}][]NodeID)
		}
		entries[valueKey] = append(entries[valueKey], id)
	}
	return entries
}

// pendingWriteAttachment is a schema's attached pending-write source and
// the schema's namespace.
type pendingWriteAttachment struct {
	source    pendingWriteSource
	namespace string
}

// attachPendingWrites makes sm's index lookups merge source's pending writes
// for namespace (#719). Attaching the same source again is a no-op.
func (sm *SchemaManager) attachPendingWrites(source pendingWriteSource, namespace string) {
	if sm == nil || source == nil {
		return
	}
	if current := sm.pendingWrites.Load(); current != nil && current.source == source {
		return
	}
	sm.pendingWrites.Store(&pendingWriteAttachment{source: source, namespace: namespace})
}

// withPendingWrites runs read with the attached pending-write view, or with
// a zero view when no source is attached or nothing is pending.
func (sm *SchemaManager) withPendingWrites(read func(view pendingWriteView)) {
	attachment := sm.pendingWrites.Load()
	if attachment == nil {
		read(pendingWriteView{})
		return
	}
	attachment.source.readPendingWrites(func(view pendingWriteView) {
		view.namespace = attachment.namespace
		read(view)
	})
}

// orderedIndexIDs lists an index's node IDs in value order (descending when
// asked), skipping null values, merged with the pending view: stale engine
// entries are dropped and pending nodes placed at their values. IDs with the
// same value are in ID order. limit < 0 lists all. Caller holds idx.mu.
func (idx *PropertyIndex) orderedIDsLocked(view pendingWriteView, property string, descending bool, limit int) []NodeID {
	keys := idx.sortedKeysLocked()
	pending := view.propertyEntries(idx.Label, property)
	if len(pending) > 0 {
		for valueKey := range pending {
			if valueKey == nil {
				continue
			}
			if ids := idx.values[valueKey]; len(ids) == 0 {
				keys = append(keys, valueKey)
			}
		}
		sort.SliceStable(keys, func(i, j int) bool { return compareSchemaIndexValues(keys[i], keys[j]) < 0 })
	}
	capacity := len(keys)
	if limit >= 0 && limit < capacity {
		capacity = limit
	}
	out := make([]NodeID, 0, capacity)
	appendKey := func(valueKey interface{}) bool {
		ids := make([]NodeID, 0, len(idx.values[valueKey])+len(pending[valueKey]))
		for _, id := range idx.values[valueKey] {
			if view.keep(id) {
				ids = append(ids, id)
			}
		}
		ids = append(ids, pending[valueKey]...)
		sort.Slice(ids, func(i, j int) bool { return string(ids[i]) < string(ids[j]) })
		for _, id := range ids {
			out = append(out, id)
			if limit >= 0 && len(out) >= limit {
				return false
			}
		}
		return true
	}
	if descending {
		for i := len(keys) - 1; i >= 0; i-- {
			if !appendKey(keys[i]) {
				break
			}
		}
	} else {
		for _, valueKey := range keys {
			if !appendKey(valueKey) {
				break
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

package storage

import (
	"slices"
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
// Every pending node is listed by label. Only the (label, property) pairs
// that have a property index or a UNIQUE / NODE KEY constraint in some
// attached schema are indexed by value (tracked), so a write pays for the
// lookups that can happen.
//
// It is guarded by AsyncEngine.mu, and holds exactly the nodes in
// AsyncEngine.nodeCache: every cache write goes through replace, and every
// cache removal through remove.
type pendingNodeIndex struct {
	// byLabel: lower-cased label → pending node IDs (label scans).
	byLabel map[string]map[NodeID]bool
	// tracked: label (as stored) → properties indexed by value.
	tracked map[string]map[string]struct{}
	// byValue: (label, property, value key) → pending node IDs, without
	// duplicates. A value usually has one pending holder, so a slice is
	// cheaper to build and read than a set.
	byValue map[pendingValueKey][]NodeID
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
		tracked:    make(map[string]map[string]struct{}),
		byValue:    make(map[pendingValueKey][]NodeID),
		byProperty: make(map[pendingPropertyKey]map[NodeID]interface{}),
	}
}

// add lists node under its labels and each tracked (label, property) with a
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
		for property := range p.tracked[label] {
			p.addValue(node, label, property)
		}
	}
}

// addValue indexes node's value of property under label.
func (p *pendingNodeIndex) addValue(node *Node, label, property string) {
	valueKey, ok := indexValueKey(node.Properties[property])
	if !ok {
		return
	}
	key := pendingValueKey{label: label, property: property, value: valueKey}
	if valueIDs := p.byValue[key]; !slices.Contains(valueIDs, node.ID) {
		p.byValue[key] = append(valueIDs, node.ID)
	}
	pair := pendingPropertyKey{label: label, property: property}
	propertyIDs := p.byProperty[pair]
	if propertyIDs == nil {
		propertyIDs = make(map[NodeID]interface{})
		p.byProperty[pair] = propertyIDs
	}
	propertyIDs[node.ID] = valueKey
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
		for property := range p.tracked[label] {
			valueKey, ok := indexValueKey(node.Properties[property])
			if !ok {
				continue
			}
			key := pendingValueKey{label: label, property: property, value: valueKey}
			valueIDs := p.byValue[key]
			for index, id := range valueIDs {
				if id != node.ID {
					continue
				}
				last := len(valueIDs) - 1
				valueIDs[index] = valueIDs[last]
				if last == 0 {
					delete(p.byValue, key)
				} else {
					p.byValue[key] = valueIDs[:last]
				}
				break
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

// replace moves node's entries from the cached object prev to node. A
// write-back that keeps the labels and tracked values (an embedding update)
// leaves the entries as they are.
func (p *pendingNodeIndex) replace(prev, node *Node) {
	if prev != nil && node != nil && prev.ID == node.ID && p.sameEntries(prev, node) {
		return
	}
	p.remove(prev)
	p.add(node)
}

// sameEntries reports whether two versions of a node have the same labels
// and tracked values.
func (p *pendingNodeIndex) sameEntries(prev, node *Node) bool {
	if !labelsEqual(prev.Labels, node.Labels) {
		return false
	}
	for _, label := range node.Labels {
		for property := range p.tracked[label] {
			before, beforeOK := indexValueKey(prev.Properties[property])
			after, afterOK := indexValueKey(node.Properties[property])
			if beforeOK != afterOK || before != after {
				return false
			}
		}
	}
	return true
}

// track indexes property by value for label's pending nodes from now on,
// including the nodes already pending. cached looks a pending node up.
func (p *pendingNodeIndex) track(label, property string, cached func(NodeID) *Node) {
	properties := p.tracked[label]
	if properties == nil {
		properties = make(map[string]struct{})
		p.tracked[label] = properties
	}
	if _, already := properties[property]; already {
		return
	}
	properties[property] = struct{}{}
	for id := range p.byLabel[strings.ToLower(label)] {
		if node := cached(id); node != nil && hasLabel(node.Labels, label) {
			p.addValue(node, label, property)
		}
	}
}

// pendingWriteSource is the write cache of a layer above the engine that
// maintains a schema's indexes (AsyncEngine). A schema it is attached to
// merges its pending writes into every index lookup (#719).
type pendingWriteSource interface {
	// lockPendingWrites holds the pending writes still until
	// unlockPendingWrites: a flush can't retire a pending node meanwhile, so
	// a lookup that combines the index with the view sees each committed
	// node once. The view is zero when nothing is pending.
	lockPendingWrites() pendingWriteView
	unlockPendingWrites()
	// trackPendingValues indexes these (label, property) pairs of pending
	// nodes by value.
	trackPendingValues(pairs []pendingPropertyKey)
}

// pendingWriteOwner reports a node with a pending create, update or delete,
// whose engine index entries are stale. Called with the pending writes
// locked.
type pendingWriteOwner interface {
	nodeSupersededLocked(id NodeID) bool
}

// pendingWriteView is one namespace's view of an AsyncEngine's pending
// writes, valid while they are locked.
type pendingWriteView struct {
	index *pendingNodeIndex
	owner pendingWriteOwner
	// namespace limits pending nodes to the schema's namespace: a node whose
	// ID carries another database's prefix is not listed.
	namespace string
}

// keep reports whether an ID from the engine's index is current.
func (v pendingWriteView) keep(id NodeID) bool {
	return v.index == nil || !v.owner.nodeSupersededLocked(id)
}

// inNamespace reports whether a pending node belongs to the view's
// namespace. Unprefixed IDs (an engine scoped to one namespace) do.
func (v pendingWriteView) inNamespace(id NodeID) bool {
	namespace, _, prefixed := ParseDatabasePrefix(string(id))
	return !prefixed || namespace == v.namespace
}

// valueMatchSet is the pending nodes with label and property = valueKey,
// before the namespace filter. The slice is the index's: callers read it
// under the pending-write lock and don't keep or change it.
func (v pendingWriteView) valueMatchSet(label, property string, valueKey interface{}) []NodeID {
	if v.index == nil {
		return nil
	}
	return v.index.byValue[pendingValueKey{label: label, property: property, value: valueKey}]
}

// valueMatches appends the namespace's pending nodes with label and
// property = valueKey to out.
func (v pendingWriteView) valueMatches(out []NodeID, label, property string, valueKey interface{}) []NodeID {
	return v.appendInNamespace(out, v.valueMatchSet(label, property, valueKey))
}

// appendInNamespace appends the ids that belong to the view's namespace to
// out.
func (v pendingWriteView) appendInNamespace(out, ids []NodeID) []NodeID {
	for _, id := range ids {
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
// for namespace (#719), and has source track sm's indexed and constrained
// pairs. Attaching the same source again is a no-op.
func (sm *SchemaManager) attachPendingWrites(source pendingWriteSource, namespace string) {
	if sm == nil || source == nil {
		return
	}
	if current := sm.pendingWrites.Load(); current != nil && current.source == source {
		return
	}
	sm.pendingWrites.Store(&pendingWriteAttachment{source: source, namespace: namespace})
	sm.trackPendingPairs()
}

// trackPendingPairs has the attached source index sm's property-indexed and
// UNIQUE / NODE KEY-constrained (label, property) pairs by value. Schema
// changes call it after releasing sm.mu.
func (sm *SchemaManager) trackPendingPairs() {
	attachment := sm.pendingWrites.Load()
	if attachment == nil {
		return
	}
	sm.mu.RLock()
	pairs := make([]pendingPropertyKey, 0, len(sm.propertyIndexes)+len(sm.uniqueConstraints))
	for _, idx := range sm.propertyIndexes {
		if len(idx.Properties) > 0 {
			pairs = append(pairs, pendingPropertyKey{label: idx.Label, property: idx.Properties[0]})
		}
	}
	for _, constraint := range sm.uniqueConstraints {
		pairs = append(pairs, pendingPropertyKey{label: constraint.Label, property: constraint.Property})
	}
	for _, constraint := range sm.constraints {
		if (constraint.Type == ConstraintUnique || constraint.Type == ConstraintNodeKey) && len(constraint.Properties) > 0 {
			pairs = append(pairs, pendingPropertyKey{label: constraint.Label, property: constraint.Properties[0]})
		}
	}
	sm.mu.RUnlock()
	attachment.source.trackPendingValues(pairs)
}

// beginPendingRead locks the attached source's pending writes and returns
// their view for sm's namespace; endPendingRead unlocks them. Without an
// attached source the view is zero and nothing is locked.
func (sm *SchemaManager) beginPendingRead() (pendingWriteView, pendingWriteSource) {
	attachment := sm.pendingWrites.Load()
	if attachment == nil {
		return pendingWriteView{}, nil
	}
	view := attachment.source.lockPendingWrites()
	view.namespace = attachment.namespace
	return view, attachment.source
}

func endPendingRead(source pendingWriteSource) {
	if source != nil {
		source.unlockPendingWrites()
	}
}

// orderedIDsLocked lists an index's node IDs in value order (descending when
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

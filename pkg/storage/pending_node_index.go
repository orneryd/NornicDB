package storage

import (
	"context"
	"github.com/orneryd/nornicdb/internal/ctxsort"
	"slices"
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
	// byLabel: label (exact case) → pending node IDs (label scans).
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
		ids := p.byLabel[label]
		if ids == nil {
			ids = make(map[NodeID]bool)
			p.byLabel[label] = ids
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
		if ids := p.byLabel[label]; ids != nil {
			delete(ids, node.ID)
			if len(ids) == 0 {
				delete(p.byLabel, label)
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
				valueIDs[last] = ""
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

// replace moves node's entries from prev to node. A write-back that keeps
// the labels and tracked values (an embedding update) leaves the entries as
// they are, but only when prev's entries are actually listed: the
// write-behind buffer hands in the committed version as prev for a node it
// doesn't hold yet, and that version was never added here. Skipping the add
// for it would leave the node superseded in the committed index and absent
// from this one, so index lookups miss it until the flush (#1043).
func (p *pendingNodeIndex) replace(prev, node *Node) {
	if prev != nil && node != nil && prev.ID == node.ID && p.listed(prev) && p.sameEntries(prev, node) {
		return
	}
	p.remove(prev)
	p.add(node)
}

// listed reports whether add's label entries for node are present. add lists
// a node under every label together with its tracked values, so the label
// entries stand for all of them.
func (p *pendingNodeIndex) listed(node *Node) bool {
	for _, label := range node.Labels {
		if !p.byLabel[label][node.ID] {
			return false
		}
	}
	return true
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

// empty reports whether the index holds no node entries. The byLabel map is
// the one listing every indexed node, so emptiness there means the value and
// property maps are empty too (remove cleans them together).
func (p *pendingNodeIndex) empty() bool {
	return p == nil || len(p.byLabel) == 0
}

// track indexes property by value for label's pending nodes from now on,
// including the nodes already pending. cached looks a pending node up.
func (p *pendingNodeIndex) track(ctx context.Context, label, property string, cached func(NodeID) *Node) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, already := p.tracked[label][property]; already {
		return nil
	}
	// Build the new value population privately, including unchanged pairs. This
	// keeps both tracking metadata and all buckets unpublished on cancellation.
	values := make(map[pendingValueKey][]NodeID, len(p.byValue))
	for key, ids := range p.byValue {
		if err := ctx.Err(); err != nil {
			return err
		}
		values[key] = ids
	}
	propertyIDs := make(map[NodeID]interface{})
	for id := range p.byLabel[label] {
		if err := ctx.Err(); err != nil {
			return err
		}
		node := cached(id)
		if node == nil || !hasLabel(node.Labels, label) {
			continue
		}
		value, ok := indexValueKey(node.Properties[property])
		if !ok {
			continue
		}
		key := pendingValueKey{label: label, property: property, value: value}
		values[key] = append(values[key], id)
		propertyIDs[id] = value
	}
	properties := make(map[string]struct{}, len(p.tracked[label])+1)
	for name := range p.tracked[label] {
		if err := ctx.Err(); err != nil {
			return err
		}
		properties[name] = struct{}{}
	}
	properties[property] = struct{}{}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Constant-size publication under the pending owner's write lock.
	p.byValue = values
	if len(propertyIDs) > 0 {
		p.byProperty[pendingPropertyKey{label: label, property: property}] = propertyIDs
	}
	p.tracked[label] = properties
	return nil
}

// pendingWriteSource is the write cache of a layer above the engine that
// maintains a schema's indexes (AsyncEngine). A schema it is attached to
// merges its pending writes into every index lookup (#719).
type pendingWriteSource interface {
	// lockPendingWrites holds the pending writes still until
	// unlockPendingWrites: a flush can't retire a pending node meanwhile, so
	// a lookup that combines the index with the view sees each committed
	// node once. The view is zero when nothing is pending.
	lockPendingWrites(context.Context) (pendingWriteView, error)
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
func (v pendingWriteView) valueMatches(ctx context.Context, out []NodeID, label, property string, valueKey interface{}) ([]NodeID, error) {
	return v.appendInNamespace(ctx, out, v.valueMatchSet(label, property, valueKey))
}

// appendInNamespace appends the ids that belong to the view's namespace to
// out.
func (v pendingWriteView) appendInNamespace(ctx context.Context, out, ids []NodeID) ([]NodeID, error) {
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if v.inNamespace(id) {
			out = append(out, id)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// propertyEntries returns the namespace's pending nodes with label and a
// non-null property, grouped by value key.
func (v pendingWriteView) propertyEntries(ctx context.Context, label, property string) (map[interface{}][]NodeID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if v.index == nil {
		return nil, nil
	}
	var entries map[interface{}][]NodeID
	for id, valueKey := range v.index.byProperty[pendingPropertyKey{label: label, property: property}] {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !v.inNamespace(id) {
			continue
		}
		if entries == nil {
			entries = make(map[interface{}][]NodeID)
		}
		entries[valueKey] = append(entries[valueKey], id)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return entries, nil
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
	pairs := make([]pendingPropertyKey, 0, len(sm.compositeIndexes)+len(sm.uniqueConstraints))
	for _, idx := range sm.compositeIndexes {
		if len(idx.Properties) == 1 {
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
func (sm *SchemaManager) beginPendingRead(ctx context.Context) (pendingWriteView, pendingWriteSource, error) {
	if err := ctx.Err(); err != nil {
		return pendingWriteView{}, nil, err
	}
	attachment := sm.pendingWrites.Load()
	if attachment == nil {
		return pendingWriteView{}, nil, nil
	}
	view, err := attachment.source.lockPendingWrites(ctx)
	if err != nil {
		return pendingWriteView{}, nil, err
	}
	view.namespace = attachment.namespace
	return view, attachment.source, nil
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
// keep, when not nil, selects the index keys to list (PropertyIndexRange).
func (idx *PropertyIndex) orderedIDsLocked(ctx context.Context, view pendingWriteView, property string, descending bool, limit int, bounds PropertyIndexBounds) ([]NodeID, error) {
	inRange, _, err := idx.sortedKeyRangeLocked(ctx, bounds)
	if err != nil {
		return nil, err
	}
	keys := make([]interface{}, 0, len(inRange))
	for _, key := range inRange {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !bounds.bounded() || bounds.contains(key) {
			keys = append(keys, key)
		}
	}
	pending, err := view.propertyEntries(ctx, idx.Label, property)
	if err != nil {
		return nil, err
	}
	if len(pending) > 0 {
		for valueKey := range pending {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if valueKey == nil || (bounds.bounded() && !bounds.contains(valueKey)) {
				continue
			}
			if len(idx.values[valueKey]) == 0 {
				keys = append(keys, valueKey)
			}
		}
		if err := ctxsort.Sort(ctx, keys, compareSchemaIndexValues, true); err != nil {
			return nil, err
		}
	}
	capacity := len(keys)
	if limit >= 0 && limit < capacity {
		capacity = limit
	}
	out := make([]NodeID, 0, capacity)
	appendKey := func(valueKey interface{}) (bool, error) {
		ids := make([]NodeID, 0, len(idx.values[valueKey])+len(pending[valueKey]))
		for _, id := range idx.values[valueKey] {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if view.keep(id) {
				ids = append(ids, id)
			}
		}
		for _, id := range pending[valueKey] {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			ids = append(ids, id)
		}
		if err := ctxsort.Sort(ctx, ids, func(a, b NodeID) int { return strings.Compare(string(a), string(b)) }, false); err != nil {
			return false, err
		}
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			out = append(out, id)
			if limit >= 0 && len(out) >= limit {
				return false, nil
			}
		}
		return true, nil
	}
	for n := 0; n < len(keys); n++ {
		i := n
		if descending {
			i = len(keys) - 1 - n
		}
		more, err := appendKey(keys[i])
		if err != nil {
			return nil, err
		}
		if !more {
			break
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

package storage

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
)

// Relationship constraint indexes are derived equality indexes, maintained
// by the same committed-edge hooks as the query caches. They reuse the node
// equality index's canonical values; NodeID below is an internal index slot,
// not an entity conversion. Only matching edge IDs are read back and checked.
// The bounded entry count prevents historical schema names retaining indexes.
const relationshipUniqueIndexLimit = 16

type relationshipUniqueIndexKey struct{ namespace, relType, properties string }
type relationshipUniqueIndex struct {
	key        relationshipUniqueIndexKey
	properties []string
	index      *CompositeIndex
	keys       map[EdgeID][]interface{}
}
type relationshipUniqueIndexSet struct {
	active  atomic.Bool
	mu      sync.Mutex
	entries map[relationshipUniqueIndexKey]*relationshipUniqueIndex
}

func relationshipConstraintValues(properties []string, values map[string]interface{}) ([]interface{}, bool) {
	return relationshipConstraintValueKeys(properties, values, false)
}

// Composite/list keys serialize equal-value commits, but remain scan validated.
func relationshipConstraintValueKeys(properties []string, values map[string]interface{}, includeComposite bool) ([]interface{}, bool) {
	out := make([]interface{}, len(properties))
	for i, p := range properties {
		key, ok := indexValueKey(values[p])
		switch v := key.(type) {
		case float64:
			if v != v {
				ok = false
			} // NaN is not an equality/map key.
			if v == 0 {
				key = float64(0)
			}
		case string, bool:
		case compositeIndexKey:
			ok = includeComposite
		default:
			ok = false
		}
		if !ok {
			return nil, false
		}
		out[i] = key
	}
	return out, true
}
func (idx *relationshipUniqueIndex) remove(id EdgeID) {
	previous, ok := idx.keys[id]
	if !ok {
		return
	}
	props := make(map[string]interface{}, len(previous))
	for i, p := range idx.properties {
		props[p] = previous[i]
	}
	idx.index.RemoveNode(NodeID(id), props)
	delete(idx.keys, id)
}
func (idx *relationshipUniqueIndex) put(edge *Edge) {
	idx.remove(edge.ID)
	if edge.Type != idx.key.relType || !strings.HasPrefix(string(edge.ID), idx.key.namespace+":") {
		return
	}
	values, ok := relationshipConstraintValues(idx.properties, edge.Properties)
	if !ok {
		return
	}
	props := make(map[string]interface{}, len(values))
	for i, prop := range idx.properties {
		props[prop] = values[i]
	}
	_ = idx.index.IndexNode(NodeID(edge.ID), props)
	idx.keys[edge.ID] = values
}

// relationshipUniqueCandidates returns the exact-value equality candidates.
// Unsupported keys, decay-filtered query views and write-behind use the old
// complete scan, rather than trusting an incomplete index miss.
func (b *BadgerEngine) relationshipUniqueCandidates(edge *Edge, c Constraint, namespace string) ([]NodeID, bool, error) {
	if b.writeBehind != nil || b.decayEnabled || namespace == "" || len(c.Properties) == 0 {
		return nil, false, nil
	}
	if _, ok := relationshipConstraintValues(c.Properties, edge.Properties); !ok {
		return nil, false, nil
	}
	key := relationshipUniqueIndexKey{namespace, edge.Type, relationshipPropertyKey(c.Properties)}
	set := &b.relationshipUniqueIndexes
	set.mu.Lock()
	defer set.mu.Unlock()
	idx := set.entries[key]
	if idx == nil {
		set.active.Store(true) // Mutations must join publication before the build reads storage.
		if hook := getUniqueConstraintScanHook(); hook != nil {
			hook()
		}
		edges, err := b.GetEdgesByType(edge.Type)
		if err != nil {
			set.active.Store(len(set.entries) > 0)
			return nil, false, err
		}
		idx = &relationshipUniqueIndex{key: key, properties: append([]string(nil), c.Properties...), keys: make(map[EdgeID][]interface{}), index: &CompositeIndex{Properties: append([]string(nil), c.Properties...), values: make(map[interface{}][]NodeID), fullIndex: make(map[string][]NodeID), prefixIndex: make(map[string][]NodeID)}}
		for _, existing := range edges {
			idx.put(existing)
		}
		if set.entries == nil {
			set.entries = make(map[relationshipUniqueIndexKey]*relationshipUniqueIndex)
		}
		if len(set.entries) >= relationshipUniqueIndexLimit {
			for old := range set.entries {
				delete(set.entries, old)
				break
			}
		}
		set.entries[key] = idx
	}
	values, _ := relationshipConstraintValues(c.Properties, edge.Properties)
	return idx.index.LookupFull(values...), true, nil
}
func (b *BadgerEngine) updateRelationshipUniqueIndexes(edge *Edge) {
	set := &b.relationshipUniqueIndexes
	if edge == nil || !set.active.Load() {
		return
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	for _, idx := range set.entries {
		idx.put(edge)
	}
}
func (b *BadgerEngine) deleteRelationshipUniqueIndexes(ids ...EdgeID) {
	set := &b.relationshipUniqueIndexes
	if len(ids) == 0 || !set.active.Load() {
		return
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	for _, idx := range set.entries {
		for _, id := range ids {
			idx.remove(id)
		}
	}
}
func (b *BadgerEngine) resetRelationshipUniqueIndexes() {
	set := &b.relationshipUniqueIndexes
	set.mu.Lock()
	set.entries = nil
	set.active.Store(false)
	set.mu.Unlock()
}

// lockRelationshipConstraintKeysOf shares the existing value-lock registry.
// Its relationship prefix separates entity domains; locks cover validation,
// primary-key commit and publication to the derived equality indexes.
func (sm *SchemaManager) lockRelationshipConstraintKeysOf(owner string, edges ...*Edge) (func(), error) {
	if sm == nil {
		return func() {}, nil
	}
	var keys []uniqueConstraintLockKey
	for _, edge := range edges {
		if edge == nil || len(edge.Properties) == 0 {
			continue
		}
		for _, c := range sm.GetConstraintsForLabels([]string{edge.Type}) {
			if c.EffectiveEntityType() != ConstraintEntityRelationship || (c.Type != ConstraintUnique && c.Type != ConstraintRelationshipKey) {
				continue
			}
			values, ok := relationshipConstraintValueKeys(c.Properties, edge.Properties, true)
			if !ok || len(values) == 0 {
				continue
			}
			var key interface{} = values[0]
			if len(values) > 1 {
				key = NewCompositeKey(values...).Hash
			}
			keys = append(keys, uniqueConstraintLockKey{label: "\x00relationship:" + edge.Type, property: relationshipPropertyKey(c.Properties), value: key})
		}
	}
	if len(keys) == 0 {
		return func() {}, nil
	}
	if owner == "" {
		owner = newEngineWriteLockOwner()
	}
	return sm.acquireUniqueConstraintCommitLocks(context.Background(), owner, keys)
}
func (b *BadgerEngine) lockRelationshipConstraintEdge(edge *Edge) (func(), error) {
	namespace, _, _ := ParseDatabasePrefix(string(edge.ID))
	return b.GetSchemaForNamespace(namespace).lockRelationshipConstraintKeysOf("", edge)
}

func relationshipPropertyKey(properties []string) string {
	data, _ := json.Marshal(properties)
	return string(data)
}

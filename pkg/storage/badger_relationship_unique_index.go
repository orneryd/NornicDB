package storage

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
)

// Relationship property and constraint indexes share this derived equality owner,
// maintained by the same committed-edge hooks as the query caches. They reuse the node
// equality index's canonical values; NodeID below is an internal index slot,
// not an entity conversion. Only matching edge IDs are read back and checked.
// The bounded entry count prevents historical schema names retaining indexes.
const relationshipUniqueIndexLimit = 16

type relationshipUniqueIndexKey struct {
	namespace, relType, properties string
}
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
	dirty   map[EdgeID]struct{}
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
	values, ok := relationshipConstraintValueKeys(idx.properties, edge.Properties, true)
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
	key := relationshipUniqueIndexKey{namespace: namespace, relType: edge.Type, properties: relationshipPropertyKey(c.Properties)}
	values, _ := relationshipConstraintValues(c.Properties, edge.Properties)
	ids, err := b.relationshipEqualityCandidates(context.Background(), key, c.Properties, values, func(visit func(*Edge) error) error {
		if hook := getUniqueConstraintScanHook(); hook != nil {
			hook()
		}
		return b.StreamEdgesByTypeInScope(context.Background(), namespace+":", edge.Type, visit)
	})
	return ids, true, err
}

// Both constraint validation and query seeks use this owner. Snapshot entries
// are immutable; live entries are reconciled from committed records after a
// write notification. Reading the record, rather than a notification's payload,
// keeps overlapping writers' publication order from restoring an older value.
func (b *BadgerEngine) relationshipEqualityCandidates(ctx context.Context, key relationshipUniqueIndexKey, properties []string, values []interface{}, populate func(func(*Edge) error) error) ([]NodeID, error) {
	return b.relationshipEqualityCandidatesInSet(ctx, &b.relationshipUniqueIndexes, key, properties, values, populate)
}

// The read-view owner supplies its index set: maintained engine state for live
// access, or immutable state owned and released by one pinned transaction.
func (b *BadgerEngine) relationshipEqualityCandidatesInSet(ctx context.Context, set *relationshipUniqueIndexSet, key relationshipUniqueIndexKey, properties []string, values []interface{}, populate func(func(*Edge) error) error) ([]NodeID, error) {
	set.mu.Lock()
	defer set.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(set.dirty) > 0 {
		if err := b.withView(func(txn *badger.Txn) error {
			for id := range set.dirty {
				if err := ctx.Err(); err != nil {
					return err
				}
				edge, err := b.readEdgeInTxn(txn, id)
				if err != nil && !errors.Is(err, ErrNotFound) {
					return err
				}
				for _, idx := range set.entries {
					idx.remove(id)
					if edge != nil {
						idx.put(edge)
					}
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
		set.dirty = nil
	}
	idx := set.entries[key]
	if idx == nil {
		set.active.Store(true) // Mutations must join publication before the build reads storage.
		idx = &relationshipUniqueIndex{key: key, properties: append([]string(nil), properties...), keys: make(map[EdgeID][]interface{}), index: &CompositeIndex{Properties: append([]string(nil), properties...), values: make(map[interface{}][]NodeID), fullIndex: make(map[string][]NodeID), prefixIndex: make(map[string][]NodeID)}}
		if err := populate(func(edge *Edge) error { idx.put(edge); return nil }); err != nil {
			set.active.Store(len(set.entries) > 0)
			return nil, err
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
	return idx.index.LookupFull(values...), nil
}
func (b *BadgerEngine) updateRelationshipUniqueIndexes(edge *Edge) {
	set := &b.relationshipUniqueIndexes
	if edge == nil || !set.active.Load() {
		return
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	for _, idx := range set.entries {
		_, previous := idx.keys[edge.ID]
		if !previous && (edge.Type != idx.key.relType || !strings.HasPrefix(string(edge.ID), idx.key.namespace+":")) {
			continue
		}
		if set.dirty == nil {
			set.dirty = make(map[EdgeID]struct{})
		}
		set.dirty[edge.ID] = struct{}{}
		break
	}
}
func (b *BadgerEngine) deleteRelationshipUniqueIndexes(ids ...EdgeID) {
	set := &b.relationshipUniqueIndexes
	if len(ids) == 0 || !set.active.Load() {
		return
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	for _, id := range ids {
		for _, idx := range set.entries {
			if _, present := idx.keys[id]; present {
				if set.dirty == nil {
					set.dirty = make(map[EdgeID]struct{})
				}
				set.dirty[id] = struct{}{}
				break
			}
		}
	}
}
func (b *BadgerEngine) resetRelationshipUniqueIndexes() {
	set := &b.relationshipUniqueIndexes
	set.mu.Lock()
	set.entries = nil
	set.dirty = nil
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

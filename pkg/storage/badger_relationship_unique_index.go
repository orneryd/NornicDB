package storage

import (
	"context"
	"strings"
	"sync/atomic"

	"github.com/orneryd/nornicdb/internal/jsontext"
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
	mu      contextRWMutex
	entries map[relationshipUniqueIndexKey]*relationshipUniqueIndex
}

// Composite/list keys serialize equal-value commits, but remain scan validated.
// ctx is the lifetime the keys are prepared for: preparing a list or map value
// observes that lifetime's cancellation and yields no keys at all rather than a
// partially prepared set.
func relationshipConstraintValueKeys(ctx context.Context, properties []string, values map[string]interface{}, includeComposite bool) ([]interface{}, bool, error) {
	out := make([]interface{}, len(properties))
	for i, p := range properties {
		key, ok, err := indexValueKey(ctx, values[p])
		if err != nil {
			return nil, false, err
		}
		switch v := key.(type) {
		case float64:
			if v != v {
				ok = false
			} // NaN is not an equality/map key.
		case int64, uint64: // an exact integer beyond float64 precision (numericIndexKey)
		case string, bool:
		case compositeIndexKey:
			ok = includeComposite
		default:
			ok = false
		}
		if !ok {
			return nil, false, nil
		}
		out[i] = key
	}
	return out, true, nil
}
func (idx *relationshipUniqueIndex) keyProperties(values []interface{}) map[string]interface{} {
	props := make(map[string]interface{}, len(values))
	for i, p := range idx.properties {
		props[p] = values[i]
	}
	return props
}

func (idx *relationshipUniqueIndex) remove(id EdgeID) {
	previous, ok := idx.keys[id]
	if !ok {
		return
	}
	idx.index.RemoveNode(NodeID(id), idx.keyProperties(previous))
	delete(idx.keys, id)
}

// put replaces one cache entry only after every new posting key is prepared.
// Request-built caches retain the request lifetime; accepted write maintenance
// supplies its own non-cancelable lifetime. No rejected attempt removes an old
// posting or changes the retained key map.
func (idx *relationshipUniqueIndex) put(ctx context.Context, edge *Edge) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var values []interface{}
	var props map[string]interface{}
	if edge.Type == idx.key.relType && strings.HasPrefix(string(edge.ID), idx.key.namespace+":") {
		var ok bool
		var err error
		values, ok, err = relationshipConstraintValueKeys(ctx, idx.properties, edge.Properties, false)
		if err != nil {
			return err
		}
		if ok {
			props = idx.keyProperties(values)
		}
	}
	if err := idx.index.mu.LockContext(ctx); err != nil {
		return err
	}
	defer idx.index.mu.Unlock()
	var keys preparedCompositeNodeKeys
	if props != nil {
		var err error
		err = idx.index.prepareNodeKeys(ctx, props, &keys)
		if err != nil {
			return err
		}
	} else if err := ctx.Err(); err != nil {
		return err
	}
	// Both removal and insertion now form one accepted, locked mutation.
	if previous, exists := idx.keys[edge.ID]; exists {
		idx.index.removeNodeLocked(NodeID(edge.ID), idx.keyProperties(previous))
		delete(idx.keys, edge.ID)
	}
	if props != nil {
		idx.index.applyNodeKeysLocked(NodeID(edge.ID), &keys)
		idx.keys[edge.ID] = values
	}
	return nil
}

// relationshipUniqueIndexLocked returns the exact-value index of one
// (namespace, relationship type, property set), building it from every stored
// relationship of the type when it is not retained yet, and reports whether it
// built a new one. The caller holds set.mu, so one build serves every waiter and
// a canceled build publishes nothing. The index is built from every stored
// relationship of the type: one hidden from reads still holds its value.
func (b *BadgerEngine) relationshipUniqueIndexLocked(ctx context.Context, key relationshipUniqueIndexKey, properties []string) (*relationshipUniqueIndex, bool, error) {
	set := &b.relationshipUniqueIndexes
	if idx := set.entries[key]; idx != nil {
		return idx, false, nil
	}
	set.active.Store(true) // Mutations must join publication before the build reads storage.
	if hook := getUniqueConstraintScanHook(); hook != nil {
		hook()
	}
	// This scan prepares a constraint decision before the write is accepted.
	// Its caller supplies the preparation lifetime, not a detached write lifetime.
	edges, err := b.GetStoredEdgesByType(ctx, key.relType)
	if err != nil {
		return nil, false, err
	}
	owned := append([]string(nil), properties...)
	idx := &relationshipUniqueIndex{key: key, properties: owned, keys: make(map[EdgeID][]interface{}), index: &CompositeIndex{Properties: append([]string(nil), owned...), values: make(map[interface{}][]NodeID), fullIndex: make(map[string][]NodeID), prefixIndex: make(map[string][]NodeID)}}
	for _, existing := range edges {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		// The insertion prepares this local cache under the caller's
		// preparation lifetime, so cancellation stops it before the cache is
		// queried or published.
		if err := idx.put(ctx, existing); err != nil {
			return nil, false, err
		}
	}
	return idx, true, nil
}

// publishRelationshipUniqueIndexLocked registers a completely built index under
// its own key, dropping an entry when the bound is reached. The caller
// holds set.mu.
func (b *BadgerEngine) publishRelationshipUniqueIndexLocked(idx *relationshipUniqueIndex) {
	set := &b.relationshipUniqueIndexes
	if set.entries == nil {
		set.entries = make(map[relationshipUniqueIndexKey]*relationshipUniqueIndex)
	}
	if len(set.entries) >= relationshipUniqueIndexLimit {
		for old := range set.entries {
			delete(set.entries, old)
			break
		}
	}
	set.entries[idx.key] = idx
}

// relationshipUniqueCandidates returns the exact-value equality candidates.
// Unsupported keys, decay-filtered query views and write-behind use the old
// complete scan, rather than trusting an incomplete index miss. The index is
// built from every stored relationship of the type: one hidden from reads
// still holds its value.
func (b *BadgerEngine) relationshipUniqueCandidates(ctx context.Context, edge *Edge, c Constraint, namespace string) ([]NodeID, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if b.writeBehind != nil || b.decayEnabled || namespace == "" || len(c.Properties) == 0 {
		return nil, false, nil
	}
	values, supported, err := relationshipConstraintValueKeys(ctx, c.Properties, edge.Properties, false)
	if err != nil {
		return nil, false, err
	}
	if !supported {
		return nil, false, ctx.Err()
	}
	key := relationshipUniqueIndexKey{namespace, edge.Type, relationshipPropertyKey(c.Properties)}
	set := &b.relationshipUniqueIndexes
	if err := set.mu.LockContext(ctx); err != nil {
		return nil, false, err
	}
	defer set.mu.Unlock()
	idx, building, err := b.relationshipUniqueIndexLocked(ctx, key, c.Properties)
	if err != nil {
		set.active.Store(len(set.entries) > 0)
		return nil, false, err
	}
	if building {
		defer func() { set.active.Store(len(set.entries) > 0) }()
	}
	ids, err := idx.index.LookupFull(ctx, values...)
	if err != nil {
		return nil, false, err
	}
	if building {
		// Publish only the completely built, successfully queried cache. Cancellation
		// above discards the local index without replacing any reusable entry.
		b.publishRelationshipUniqueIndexLocked(idx)
	}
	return ids, true, nil
}

// publishRelationshipIndexMembership shares one committed frame with the
// node indexes. Candidate readers cannot meet staged relationship images.
func (b *BadgerEngine) publishRelationshipIndexMembership(states commitStates) {
	set := &b.relationshipUniqueIndexes
	if len(states.edges) == 0 || !set.active.Load() {
		return
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	for _, idx := range set.entries {
		for id, pair := range states.edges {
			edge := pair.after
			if edge == nil {
				idx.remove(id)
			} else {
				// An accepted write carries that write's lifetime, not a request's.
				_ = idx.put(context.Background(), edge)
			}
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

func relationshipPropertyKey(properties []string) string {
	data, _ := jsontext.Marshal(properties)
	return string(data)
}

package cypher

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// mergeIdentityCacheDisabled makes every lookup read storage; the
// differential test runs the same statements both ways.
var mergeIdentityCacheDisabled = false

// relationshipMergeIdentityCache answers the relationship lookups of one
// MERGE clause over its input rows (UNWIND … MERGE …): the relationships of a
// type between a pair are read once and indexed by their identity values, so
// each row is a map lookup instead of a read of every relationship between
// the pair. The answer is findRelationshipsForMerge's: the index key of a set
// of identity values is equal exactly when relationshipMergeValuesEqual holds
// for each (relationshipMergeIdentityValuesKey).
//
// Only the MERGE itself writes while its rows run. A relationship it creates
// without ON CREATE SET is added to its pair's entry; any ON CREATE SET or
// ON MATCH SET, or a create whose relationship isn't known, drops every
// entry, since it can change identity values.
type relationshipMergeIdentityCache struct {
	entries map[string]*relationshipMergeIdentityEntry
}

type relationshipMergeIdentityEntry struct {
	keys  []string
	byKey map[string][]*storage.Edge
}

func newRelationshipMergeIdentityCache() *relationshipMergeIdentityCache {
	return &relationshipMergeIdentityCache{entries: map[string]*relationshipMergeIdentityEntry{}}
}

// relationships is findRelationshipsForMerge through the cache.
func (c *relationshipMergeIdentityCache) relationships(ctx context.Context, store storage.Engine, startID, endID storage.NodeID, relType string, matchProps map[string]interface{}) ([]*storage.Edge, error) {
	if c == nil || mergeIdentityCacheDisabled {
		return findRelationshipsForMerge(ctx, store, startID, endID, relType, matchProps)
	}
	if relationshipMergeIdentityContainsNaN(valueWalk(ctx), matchProps) {
		return nil, nil
	}
	keys := sortedIdentityKeys(matchProps)
	entryKey := relationshipMergeIdentityEntryKey(startID, endID, relType, keys)
	entry, ok := c.entries[entryKey]
	if !ok {
		scan := newStorageScan(ctx)
		defer scan.close()
		edges, err := matchEdgesBetween(scan, store, startID, endID, relType, keys, func(*storage.Edge) bool {
			if scan.canceled() {
				return false
			}
			return true
		})
		if err != nil {
			return nil, err
		}
		entry = &relationshipMergeIdentityEntry{keys: keys, byKey: make(map[string][]*storage.Edge, len(edges))}
		for batch, batchErr := range materializedBatches(ctx, len(edges)) {
			if batchErr != nil {
				return nil, batchErr
			}
			for _, edge := range edges[batch.start:batch.end] {
				entry.add(edge)
			}
		}
		c.entries[entryKey] = entry
	}
	key, ok := relationshipMergeIdentityValuesKey(matchProps, keys)
	if !ok {
		return nil, nil
	}
	return clonePopulation(ctx, entry.byKey[key])
}

// created records a relationship the MERGE created (and nothing else
// changed) in its pair's entries.
func (c *relationshipMergeIdentityCache) created(edge *storage.Edge) {
	if c == nil || edge == nil {
		return
	}
	for entryKey, entry := range c.entries {
		if entryKey == relationshipMergeIdentityEntryKey(edge.StartNode, edge.EndNode, edge.Type, entry.keys) {
			entry.add(edge)
		}
	}
}

// reset drops every entry.
func (c *relationshipMergeIdentityCache) reset() {
	if c != nil && len(c.entries) > 0 {
		c.entries = map[string]*relationshipMergeIdentityEntry{}
	}
}

func (entry *relationshipMergeIdentityEntry) add(edge *storage.Edge) {
	if key, ok := relationshipMergeIdentityValuesKey(edge.Properties, entry.keys); ok {
		entry.byKey[key] = append(entry.byKey[key], edge)
	}
}

func sortedIdentityKeys(props map[string]interface{}) []string {
	keys := make([]string, 0, len(props))
	for key := range props {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func relationshipMergeIdentityEntryKey(startID, endID storage.NodeID, relType string, keys []string) string {
	parts := append([]string{string(startID), string(endID), relType}, keys...)
	encoded, _ := json.Marshal(parts)
	return string(encoded)
}

// relationshipMergeIdentityValuesKey encodes the values of keys in props: two sets
// of values have the same key exactly when relationshipMergeValuesEqual
// holds for every key. The key is the type-tagged value key the general
// comparison falls back to (mergeValueKey of the normalized value), which is
// injective; normalization makes values that compare equal directly
// (-0.0 and 0.0, 1.0 and 1) canonically equal, and positiveZero does the same
// for a FLOAT kept as float32. ok is false when a key is
// missing or a value contains NaN: such properties match nothing.
func relationshipMergeIdentityValuesKey(props map[string]interface{}, keys []string) (string, bool) {
	var key strings.Builder
	for _, name := range keys {
		value, ok := props[name]
		if !ok {
			return "", false
		}
		normalized := normalizeRelationshipMergeIdentityValue(value)
		if relationshipMergeIdentityContainsNaN(nil, normalized) {
			return "", false
		}
		key.WriteString(mergeValueKey(nil, positiveZero(nil, normalized)))
		key.WriteByte(0)
	}
	return key.String(), true
}

// positiveZero replaces a negative float32 zero in value with a positive one:
// the two compare equal, but their canonical forms differ.
func positiveZero(walk *materializedRowCheckpoint, value interface{}) interface{} {
	normalized, _ := cypherfn.TransformValue(walk, value, positiveZeroStep)
	return normalized
}

// positiveZeroStep is a float32 negative zero made positive; the walker
// descends into lists and maps.
func positiveZeroStep(value interface{}) (interface{}, bool, bool) {
	switch typed := value.(type) {
	case float32:
		if typed == 0 && 1/typed < 0 {
			return float32(0), true, false
		}
	case []interface{}, map[string]interface{}:
		return value, false, true
	}
	return value, false, false
}

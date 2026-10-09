package cypher

import (
	"encoding/json"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"sort"
	"strings"

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
func (c *relationshipMergeIdentityCache) relationships(store storage.Engine, startID, endID storage.NodeID, relType string, matchProps map[string]interface{}) ([]*storage.Edge, error) {
	if c == nil || mergeIdentityCacheDisabled {
		return findRelationshipsForMerge(store, startID, endID, relType, matchProps)
	}
	if relationshipMergeIdentityContainsNaN(matchProps) {
		return nil, nil
	}
	keys := sortedIdentityKeys(matchProps)
	entryKey := relationshipMergeIdentityEntryKey(startID, endID, relType, keys)
	entry, ok := c.entries[entryKey]
	if !ok {
		edges, err := storage.MatchEdgesBetween(store, startID, endID, relType, keys, func(*storage.Edge) bool { return true })
		if err != nil {
			return nil, err
		}
		entry = &relationshipMergeIdentityEntry{keys: keys, byKey: make(map[string][]*storage.Edge, len(edges))}
		for _, edge := range edges {
			entry.add(edge)
		}
		c.entries[entryKey] = entry
	}
	key, ok := relationshipMergeIdentityValuesKey(matchProps, keys)
	if !ok {
		return nil, nil
	}
	return append([]*storage.Edge(nil), entry.byKey[key]...), nil
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
// holds for every key. The key is the canonical form the general comparison
// falls back to (canonicalUnwindMergeValue of the normalized value), whose
// JSON is injective; normalization makes values that compare equal directly
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
		if relationshipMergeIdentityContainsNaN(normalized) {
			return "", false
		}
		encoded, err := json.Marshal(canonicalUnwindMergeValue(positiveZero(normalized)))
		if err != nil {
			return "", false
		}
		key.Write(encoded)
		key.WriteByte(0)
	}
	return key.String(), true
}

// positiveZero replaces a negative float32 zero in value with a positive one:
// the two compare equal, but their canonical forms differ.
func positiveZero(value interface{}) interface{} {
	switch typed := value.(type) {
	case float32:
		if typed == 0 {
			return float32(0)
		}
	case []interface{}:
		var out []interface{}
		for i, item := range typed {
			if fixed := positiveZero(item); !isSameZero(fixed, item) {
				if out == nil {
					out = append([]interface{}(nil), typed...)
				}
				out[i] = fixed
			}
		}
		if out != nil {
			return out
		}
	case map[string]interface{}:
		var out map[string]interface{}
		for key, item := range typed {
			if fixed := positiveZero(item); !isSameZero(fixed, item) {
				if out == nil {
					out = make(map[string]interface{}, len(typed))
					for k, v := range typed {
						out[k] = v
					}
				}
				out[key] = fixed
			}
		}
		if out != nil {
			return out
		}
	}
	return value
}

// isSameZero reports whether positiveZero left item as it was.
func isSameZero(fixed, item interface{}) bool {
	f, fOK := fixed.(float32)
	i, iOK := item.(float32)
	if fOK && iOK {
		return math.Signbit(float64(f)) == math.Signbit(float64(i))
	}
	return true
}

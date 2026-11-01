package storage

import (
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/security"
)

// CompositeKey represents a key composed of multiple property values.
// The key is a hash of all property values in order for efficient lookup.
type CompositeKey struct {
	Hash   string        // Process-local HMAC-SHA256 of framed values (for map lookup)
	Values []interface{} // Original values (for debugging/display)
}

// NewCompositeKey creates a composite key from multiple property values.
//
// Composite keys enable uniqueness constraints and indexes on multiple properties
// together (e.g., unique combination of firstName + lastName). The key is hashed
// using process-local HMAC-SHA256 for map lookups while preserving the values.
// Each value is length-framed by the shared digest owner. Hash is an ephemeral
// cache/lock identity; it must not be persisted. Temporal storage keys use their
// separate process-independent owner and are unchanged.
//
// Parameters:
//   - values: Variable number of property values to combine
//
// Returns:
//   - CompositeKey with hash for lookup and original values
//
// Example 1 - Unique Person Name:
//
//	// Ensure no two people have the same first AND last name combination
//	key := storage.NewCompositeKey("Alice", "Johnson")
//	// key.Hash = "a1b2c3..." (SHA-256)
//	// key.Values = ["Alice", "Johnson"]
//
//	// Can store in map for O(1) lookup
//	uniqueKeys := make(map[string]bool)
//	uniqueKeys[key.Hash] = true
//
// Example 2 - Multi-Column Unique Constraint:
//
//	// Email + domain must be unique together
//	key1 := storage.NewCompositeKey("user", "example.com")
//	key2 := storage.NewCompositeKey("user", "different.com")
//	// key1.Hash != key2.Hash (different combinations)
//
//	key3 := storage.NewCompositeKey("user", "example.com")
//	// key3.Hash == key1.Hash (same combination)
//
// Example 3 - Geographic Uniqueness:
//
//	// Store locations - no duplicate (lat, lon) pairs
//	locations := make(map[string]storage.NodeID)
//
//	loc1 := storage.NewCompositeKey(40.7128, -74.0060) // NYC
//	locations[loc1.Hash] = storage.NodeID("loc-nyc")
//
//	loc2 := storage.NewCompositeKey(40.7128, -74.0060) // Same coords
//	if _, exists := locations[loc2.Hash]; exists {
//		// the configured logger should emit "Location already exists!"
//		_ = exists
//	}
//
// ELI12:
//
// Imagine you're making sure no two people in your class have the SAME
// full name (first + last together):
//
//   - Alice Smith → Create a "fingerprint" (hash) from "Alice" + "Smith"
//   - Bob Johnson → Different fingerprint
//   - Alice Smith → SAME fingerprint as the first Alice Smith!
//
// The hash is like a unique barcode for the combination. If two combinations
// have the same barcode, they're duplicates!
//
// Why hash instead of just combining strings?
//   - Fast lookups (constant time)
//   - Handles any data types (numbers, strings, booleans)
//   - Consistent length (SHA-256 always 64 chars)
//
// Use Cases:
//   - Composite unique constraints (email + database_id)
//   - Multi-column indexes
//   - Deduplication of complex records
func NewCompositeKey(values ...interface{}) CompositeKey {
	// Each value is a separate length-framed digest part. Joining type-tagged
	// text with "|" makes ["a|string:b", "c"] alias ["a", "b|string:c"].
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = fmt.Sprintf("%T:%v", v, v)
	}

	return CompositeKey{
		Hash:   security.KeyedDigestHex("storage.composite_key", parts...),
		Values: values,
	}
}

// String returns a human-readable representation of the composite key.
func (ck CompositeKey) String() string {
	var parts []string
	for _, v := range ck.Values {
		parts = append(parts, fmt.Sprintf("%v", v))
	}
	return strings.Join(parts, ", ")
}

// compositeIndexValues returns the canonical index-key forms of the leading
// values of idx.Properties present in properties. It stops at the first
// property that is missing or whose value a composite index cannot key
// (nil, byte arrays, ...). This is exactly what IndexNode stores: the longest
// leading prefix whose every value can be keyed, so full and prefix lookups
// key the same way as inserts and removes.
func compositeIndexValues(idx *CompositeIndex, properties map[string]interface{}) []interface{} {
	if idx == nil {
		return nil
	}
	values := make([]interface{}, 0, len(idx.Properties))
	for _, propName := range idx.Properties {
		val, exists := properties[propName]
		if !exists {
			break
		}
		key, ok := indexValueKey(val)
		if !ok {
			break
		}
		values = append(values, key)
	}
	return values
}

// compositeLookupKeyValues canonicalizes lookup values the way inserts do.
// It reports false when a value has no index key (so no stored entry can
// equal it).
func compositeLookupKeyValues(values []interface{}) ([]interface{}, bool) {
	canonical := make([]interface{}, len(values))
	for i, value := range values {
		key, ok := indexValueKey(value)
		if !ok {
			return nil, false
		}
		canonical[i] = key
	}
	return canonical, true
}

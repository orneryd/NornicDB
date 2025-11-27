// Package storage schema management for constraints and indexes.
//
// This file implements Neo4j-compatible schema management including:
//   - Unique constraints
//   - Property indexes (single and composite)
//   - Range indexes (for efficient range queries)
//   - Full-text indexes
//   - Vector indexes
//
// Schema definitions are stored in memory and enforced during node operations.
package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// ConstraintType represents the type of constraint.
type ConstraintType string

const (
	ConstraintUnique  ConstraintType = "UNIQUE"
	ConstraintNodeKey ConstraintType = "NODE_KEY"
	ConstraintExists  ConstraintType = "EXISTS"
)

// Constraint represents a Neo4j-compatible schema constraint.
type Constraint struct {
	Name       string
	Type       ConstraintType
	Label      string
	Properties []string
}

// SchemaManager manages database schema including constraints and indexes.
type SchemaManager struct {
	mu sync.RWMutex

	// Constraints
	uniqueConstraints map[string]*UniqueConstraint // key: "Label:property"
	constraints       map[string]Constraint        // key: constraint name, stores all constraint types

	// Indexes
	propertyIndexes  map[string]*PropertyIndex  // key: "Label:property" (single property)
	compositeIndexes map[string]*CompositeIndex // key: index name
	fulltextIndexes  map[string]*FulltextIndex  // key: index_name
	vectorIndexes    map[string]*VectorIndex    // key: index_name
	rangeIndexes     map[string]*RangeIndex     // key: index_name
}

// NewSchemaManager creates a new schema manager with empty constraint and index collections.
//
// The schema manager provides thread-safe management of database schema including:
//   - Unique constraints (enforce uniqueness on properties)
//   - Node key constraints (composite unique keys)
//   - Existence constraints (require properties to exist)
//   - Property indexes (speed up lookups)
//   - Vector indexes (semantic similarity search)
//   - Full-text indexes (text search with scoring)
//
// Returns:
//   - *SchemaManager ready for use
//
// Example 1 - Basic Usage:
//
//	schema := storage.NewSchemaManager()
//
//	// Add unique constraint
//	constraint := &storage.UniqueConstraint{
//		Name:     "unique_user_email",
//		Label:    "User",
//		Property: "email",
//	}
//	schema.AddUniqueConstraint(constraint)
//
//	// Validate before creating node
//	err := schema.ValidateUnique("User", "email", "alice@example.com", "")
//	if err != nil {
//		log.Fatal("Email already exists!")
//	}
//
// Example 2 - Multiple Constraints:
//
//	schema := storage.NewSchemaManager()
//
//	// Email must be unique
//	schema.AddUniqueConstraint(&storage.UniqueConstraint{
//		Name: "unique_email", Label: "User", Property: "email",
//	})
//
//	// Username must be unique
//	schema.AddUniqueConstraint(&storage.UniqueConstraint{
//		Name: "unique_username", Label: "User", Property: "username",
//	})
//
//	// All users must have email property
//	schema.AddConstraint(storage.Constraint{
//		Name: "user_must_have_email",
//		Type: storage.ConstraintExists,
//		Label: "User",
//		Properties: []string{"email"},
//	})
//
// Example 3 - With Indexes for Performance:
//
//	schema := storage.NewSchemaManager()
//
//	// Index for fast lookups
//	schema.AddPropertyIndex(&storage.PropertyIndex{
//		Name:       "idx_user_email",
//		Label:      "User",
//		Properties: []string{"email"},
//	})
//
//	// Vector index for semantic search
//	schema.AddVectorIndex(&storage.VectorIndex{
//		Name:       "doc_embeddings",
//		Label:      "Document",
//		Property:   "embedding",
//		Dimensions: 1024,
//	})
//
// ELI12:
//
// Think of a SchemaManager like a rule book for your database:
//   - "Every person must have a unique name" (unique constraint)
//   - "You can't create a person without an age" (existence constraint)
//   - "Make a quick-lookup list for emails" (index)
//
// Before you add data, the SchemaManager checks: "Does this follow the rules?"
// If yes, data goes in. If no, you get an error. It keeps your database clean!
//
// Thread Safety:
//
//	All methods are thread-safe for concurrent access.
func NewSchemaManager() *SchemaManager {
	return &SchemaManager{
		uniqueConstraints: make(map[string]*UniqueConstraint),
		constraints:       make(map[string]Constraint),
		propertyIndexes:   make(map[string]*PropertyIndex),
		compositeIndexes:  make(map[string]*CompositeIndex),
		fulltextIndexes:   make(map[string]*FulltextIndex),
		vectorIndexes:     make(map[string]*VectorIndex),
		rangeIndexes:      make(map[string]*RangeIndex),
	}
}

// UniqueConstraint represents a unique constraint on a label and property.
type UniqueConstraint struct {
	Name     string
	Label    string
	Property string
	values   map[interface{}]NodeID // Track unique values
	mu       sync.RWMutex
}

// PropertyIndex represents a property index for faster lookups.
type PropertyIndex struct {
	Name       string
	Label      string
	Properties []string
	values     map[interface{}][]NodeID // Property value -> node IDs
	mu         sync.RWMutex
}

// CompositeKey represents a key composed of multiple property values.
// The key is a hash of all property values in order for efficient lookup.
type CompositeKey struct {
	Hash   string        // SHA256 hash of encoded values (for map lookup)
	Values []interface{} // Original values (for debugging/display)
}

// NewCompositeKey creates a composite key from multiple property values.
//
// Composite keys enable uniqueness constraints and indexes on multiple properties
// together (e.g., unique combination of firstName + lastName). The key is hashed
// using SHA-256 for efficient map lookups while preserving the original values.
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
//		fmt.Println("Location already exists!")
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
//   - Composite unique constraints (email + tenant_id)
//   - Multi-column indexes
//   - Deduplication of complex records
func NewCompositeKey(values ...interface{}) CompositeKey {
	// Create deterministic string representation
	var parts []string
	for _, v := range values {
		parts = append(parts, fmt.Sprintf("%T:%v", v, v))
	}
	encoded := strings.Join(parts, "|")

	// Hash for efficient map lookup
	hash := sha256.Sum256([]byte(encoded))

	return CompositeKey{
		Hash:   hex.EncodeToString(hash[:]),
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

// CompositeIndex represents an index on multiple properties for efficient
// multi-property queries. This is Neo4j's composite index equivalent.
//
// Composite indexes support:
//   - Full key lookups (all properties specified)
//   - Prefix lookups (leading properties specified, for ordered access)
//   - Range queries on the last property in a prefix
type CompositeIndex struct {
	Name       string
	Label      string
	Properties []string // Ordered list of property names

	// Primary index: full composite key -> node IDs
	fullIndex map[string][]NodeID

	// Prefix indexes for partial key lookups
	// Key format: "prop1Value|prop2Value|..." -> node IDs
	prefixIndex map[string][]NodeID

	// Individual property value tracking for range queries
	// propertyValues[propIndex][value] = sorted list of (otherValues, nodeID)
	// This enables efficient range queries on any property

	mu sync.RWMutex
}

// FulltextIndex represents a full-text search index.
type FulltextIndex struct {
	Name       string
	Labels     []string
	Properties []string
}

// VectorIndex represents a vector similarity index.
type VectorIndex struct {
	Name           string
	Label          string
	Property       string
	Dimensions     int
	SimilarityFunc string // "cosine", "euclidean", "dot"
}

// RangeIndex represents an index for range queries on a single property.
// It maintains a sorted list of entries for efficient O(log n) range queries.
type RangeIndex struct {
	Name     string
	Label    string
	Property string
	entries  []rangeEntry   // Sorted by value for binary search
	nodeMap  map[NodeID]int // NodeID -> index in entries (for O(1) delete)
	mu       sync.RWMutex
}

// AddUniqueConstraint adds a unique constraint.
func (sm *SchemaManager) AddUniqueConstraint(name, label, property string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	key := fmt.Sprintf("%s:%s", label, property)
	if _, exists := sm.uniqueConstraints[key]; exists {
		// Constraint already exists - this is fine with IF NOT EXISTS
		return nil
	}

	sm.uniqueConstraints[key] = &UniqueConstraint{
		Name:     name,
		Label:    label,
		Property: property,
		values:   make(map[interface{}]NodeID),
	}

	return nil
}

// CheckUniqueConstraint checks if a value violates a unique constraint.
// Returns error if constraint is violated.
func (sm *SchemaManager) CheckUniqueConstraint(label, property string, value interface{}, excludeNode NodeID) error {
	sm.mu.RLock()
	key := fmt.Sprintf("%s:%s", label, property)
	constraint, exists := sm.uniqueConstraints[key]
	sm.mu.RUnlock()

	if !exists {
		return nil // No constraint
	}

	constraint.mu.RLock()
	defer constraint.mu.RUnlock()

	if existingNode, found := constraint.values[value]; found {
		if existingNode != excludeNode {
			return fmt.Errorf("Node(%s) already exists with %s = %v", label, property, value)
		}
	}

	return nil
}

// RegisterUniqueValue registers a value for a unique constraint.
func (sm *SchemaManager) RegisterUniqueValue(label, property string, value interface{}, nodeID NodeID) {
	sm.mu.RLock()
	key := fmt.Sprintf("%s:%s", label, property)
	constraint, exists := sm.uniqueConstraints[key]
	sm.mu.RUnlock()

	if !exists {
		return
	}

	constraint.mu.Lock()
	constraint.values[value] = nodeID
	constraint.mu.Unlock()
}

// UnregisterUniqueValue removes a value from a unique constraint.
func (sm *SchemaManager) UnregisterUniqueValue(label, property string, value interface{}) {
	sm.mu.RLock()
	key := fmt.Sprintf("%s:%s", label, property)
	constraint, exists := sm.uniqueConstraints[key]
	sm.mu.RUnlock()

	if !exists {
		return
	}

	constraint.mu.Lock()
	delete(constraint.values, value)
	constraint.mu.Unlock()
}

// AddPropertyIndex adds a property index.
func (sm *SchemaManager) AddPropertyIndex(name, label string, properties []string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	key := fmt.Sprintf("%s:%s", label, properties[0]) // Use first property as key
	if _, exists := sm.propertyIndexes[key]; exists {
		return nil // Already exists
	}

	sm.propertyIndexes[key] = &PropertyIndex{
		Name:       name,
		Label:      label,
		Properties: properties,
		values:     make(map[interface{}][]NodeID),
	}

	return nil
}

// AddCompositeIndex creates a composite index on multiple properties.
// Composite indexes enable efficient queries that filter on multiple properties.
//
// Example usage:
//
//	sm.AddCompositeIndex("user_location_idx", "User", []string{"country", "city", "zipcode"})
//
// This enables efficient queries like:
//   - WHERE country = 'US' AND city = 'NYC' AND zipcode = '10001' (full match)
//   - WHERE country = 'US' AND city = 'NYC' (prefix match)
//   - WHERE country = 'US' (prefix match, uses first property only)
func (sm *SchemaManager) AddCompositeIndex(name, label string, properties []string) error {
	if len(properties) < 2 {
		return fmt.Errorf("composite index requires at least 2 properties, got %d", len(properties))
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.compositeIndexes[name]; exists {
		return nil // Already exists (idempotent)
	}

	sm.compositeIndexes[name] = &CompositeIndex{
		Name:        name,
		Label:       label,
		Properties:  properties,
		fullIndex:   make(map[string][]NodeID),
		prefixIndex: make(map[string][]NodeID),
	}

	return nil
}

// GetCompositeIndex returns a composite index by name.
func (sm *SchemaManager) GetCompositeIndex(name string) (*CompositeIndex, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	idx, exists := sm.compositeIndexes[name]
	return idx, exists
}

// GetCompositeIndexForLabel returns all composite indexes for a label.
func (sm *SchemaManager) GetCompositeIndexesForLabel(label string) []*CompositeIndex {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var indexes []*CompositeIndex
	for _, idx := range sm.compositeIndexes {
		if idx.Label == label {
			indexes = append(indexes, idx)
		}
	}
	return indexes
}

// IndexNodeComposite indexes a node in a composite index.
// Call this when creating or updating a node with the indexed properties.
func (idx *CompositeIndex) IndexNode(nodeID NodeID, properties map[string]interface{}) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Extract values in property order
	values := make([]interface{}, len(idx.Properties))
	for i, propName := range idx.Properties {
		val, exists := properties[propName]
		if !exists {
			// Node doesn't have all properties - can't be fully indexed
			// But we can still index prefixes
			values = values[:i]
			break
		}
		values[i] = val
	}

	// Index full key if all properties present
	if len(values) == len(idx.Properties) {
		key := NewCompositeKey(values...)
		idx.fullIndex[key.Hash] = appendUnique(idx.fullIndex[key.Hash], nodeID)
	}

	// Index all prefixes for partial lookups
	for i := 1; i <= len(values); i++ {
		prefixKey := NewCompositeKey(values[:i]...)
		idx.prefixIndex[prefixKey.Hash] = appendUnique(idx.prefixIndex[prefixKey.Hash], nodeID)
	}

	return nil
}

// RemoveNode removes a node from the composite index.
// Call this when deleting a node or updating its indexed properties.
func (idx *CompositeIndex) RemoveNode(nodeID NodeID, properties map[string]interface{}) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Extract values in property order
	values := make([]interface{}, 0, len(idx.Properties))
	for _, propName := range idx.Properties {
		val, exists := properties[propName]
		if !exists {
			break
		}
		values = append(values, val)
	}

	// Remove from full index
	if len(values) == len(idx.Properties) {
		key := NewCompositeKey(values...)
		idx.fullIndex[key.Hash] = removeNodeID(idx.fullIndex[key.Hash], nodeID)
		if len(idx.fullIndex[key.Hash]) == 0 {
			delete(idx.fullIndex, key.Hash)
		}
	}

	// Remove from all prefix indexes
	for i := 1; i <= len(values); i++ {
		prefixKey := NewCompositeKey(values[:i]...)
		idx.prefixIndex[prefixKey.Hash] = removeNodeID(idx.prefixIndex[prefixKey.Hash], nodeID)
		if len(idx.prefixIndex[prefixKey.Hash]) == 0 {
			delete(idx.prefixIndex, prefixKey.Hash)
		}
	}
}

// LookupFull finds nodes matching all property values exactly.
// All properties in the composite index must be specified.
func (idx *CompositeIndex) LookupFull(values ...interface{}) []NodeID {
	if len(values) != len(idx.Properties) {
		return nil // Must specify all properties for full lookup
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	key := NewCompositeKey(values...)
	if nodes, exists := idx.fullIndex[key.Hash]; exists {
		// Return a copy to avoid race conditions
		result := make([]NodeID, len(nodes))
		copy(result, nodes)
		return result
	}
	return nil
}

// LookupPrefix finds nodes matching a prefix of property values.
// Specify 1 to N-1 property values (where N is total properties in index).
// Returns all nodes that match the prefix.
//
// Example: For index on (country, city, zipcode)
//   - LookupPrefix("US") returns all nodes in the US
//   - LookupPrefix("US", "NYC") returns all nodes in NYC, US
func (idx *CompositeIndex) LookupPrefix(values ...interface{}) []NodeID {
	if len(values) == 0 || len(values) > len(idx.Properties) {
		return nil
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Check if this is a full match (not a prefix)
	if len(values) == len(idx.Properties) {
		key := NewCompositeKey(values...)
		if nodes, exists := idx.fullIndex[key.Hash]; exists {
			result := make([]NodeID, len(nodes))
			copy(result, nodes)
			return result
		}
		return nil
	}

	// Prefix lookup
	key := NewCompositeKey(values...)
	if nodes, exists := idx.prefixIndex[key.Hash]; exists {
		result := make([]NodeID, len(nodes))
		copy(result, nodes)
		return result
	}
	return nil
}

// LookupWithFilter finds nodes using a prefix and applies a filter function.
// This enables more complex queries like range queries on the last property.
//
// Example: Find all users in "US", "NYC" with zipcode > "10000"
//
//	idx.LookupWithFilter(func(n NodeID, props map[string]interface{}) bool {
//	    zip := props["zipcode"].(string)
//	    return zip > "10000"
//	}, "US", "NYC")
func (idx *CompositeIndex) LookupWithFilter(filter func(NodeID) bool, values ...interface{}) []NodeID {
	candidates := idx.LookupPrefix(values...)
	if candidates == nil {
		return nil
	}

	var result []NodeID
	for _, nodeID := range candidates {
		if filter(nodeID) {
			result = append(result, nodeID)
		}
	}
	return result
}

// Stats returns statistics about the composite index.
func (idx *CompositeIndex) Stats() map[string]interface{} {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return map[string]interface{}{
		"name":             idx.Name,
		"label":            idx.Label,
		"properties":       idx.Properties,
		"fullIndexEntries": len(idx.fullIndex),
		"prefixEntries":    len(idx.prefixIndex),
	}
}

// appendUnique appends a nodeID to a slice if not already present.
func appendUnique(slice []NodeID, nodeID NodeID) []NodeID {
	for _, existing := range slice {
		if existing == nodeID {
			return slice
		}
	}
	return append(slice, nodeID)
}

// removeNodeID removes a nodeID from a slice.
func removeNodeID(slice []NodeID, nodeID NodeID) []NodeID {
	for i, existing := range slice {
		if existing == nodeID {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

// AddFulltextIndex adds a full-text index.
func (sm *SchemaManager) AddFulltextIndex(name string, labels, properties []string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.fulltextIndexes[name]; exists {
		return nil // Already exists
	}

	sm.fulltextIndexes[name] = &FulltextIndex{
		Name:       name,
		Labels:     labels,
		Properties: properties,
	}

	return nil
}

// AddVectorIndex adds a vector index.
func (sm *SchemaManager) AddVectorIndex(name, label, property string, dimensions int, similarityFunc string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.vectorIndexes[name]; exists {
		return nil // Already exists
	}

	sm.vectorIndexes[name] = &VectorIndex{
		Name:           name,
		Label:          label,
		Property:       property,
		Dimensions:     dimensions,
		SimilarityFunc: similarityFunc,
	}

	return nil
}

// AddRangeIndex adds a range index for a single property.
func (sm *SchemaManager) AddRangeIndex(name, label, property string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.rangeIndexes[name]; exists {
		return nil // Already exists
	}

	sm.rangeIndexes[name] = &RangeIndex{
		Name:     name,
		Label:    label,
		Property: property,
		entries:  make([]rangeEntry, 0),
		nodeMap:  make(map[NodeID]int), // NodeID -> index in entries
	}

	return nil
}

// rangeEntry represents a single entry in the range index.
type rangeEntry struct {
	value  float64 // Normalized numeric value for comparison
	nodeID NodeID
}

// RangeIndexInsert adds a value to a range index.
func (sm *SchemaManager) RangeIndexInsert(name string, nodeID NodeID, value interface{}) error {
	sm.mu.Lock()
	idx, exists := sm.rangeIndexes[name]
	sm.mu.Unlock()

	if !exists {
		return fmt.Errorf("range index %s not found", name)
	}

	// Convert value to float64 for comparison
	numVal, ok := toFloat64ForIndex(value)
	if !ok {
		return fmt.Errorf("range index only supports numeric values, got %T", value)
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Binary search for insert position
	pos := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].value >= numVal
	})

	// Insert at position
	entry := rangeEntry{value: numVal, nodeID: nodeID}
	idx.entries = append(idx.entries, rangeEntry{})
	copy(idx.entries[pos+1:], idx.entries[pos:])
	idx.entries[pos] = entry
	idx.nodeMap[nodeID] = pos

	// Update positions of entries after insert
	for i := pos + 1; i < len(idx.entries); i++ {
		idx.nodeMap[idx.entries[i].nodeID] = i
	}

	return nil
}

// RangeIndexDelete removes a value from a range index.
func (sm *SchemaManager) RangeIndexDelete(name string, nodeID NodeID) error {
	sm.mu.Lock()
	idx, exists := sm.rangeIndexes[name]
	sm.mu.Unlock()

	if !exists {
		return fmt.Errorf("range index %s not found", name)
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	pos, exists := idx.nodeMap[nodeID]
	if !exists {
		return nil // Not in index
	}

	// Remove from entries
	idx.entries = append(idx.entries[:pos], idx.entries[pos+1:]...)
	delete(idx.nodeMap, nodeID)

	// Update positions of entries after delete
	for i := pos; i < len(idx.entries); i++ {
		idx.nodeMap[idx.entries[i].nodeID] = i
	}

	return nil
}

// RangeQuery performs a range query on a range index.
// Returns node IDs where value is in range [minVal, maxVal].
// Pass nil for minVal or maxVal to indicate unbounded.
func (sm *SchemaManager) RangeQuery(name string, minVal, maxVal interface{}, includeMin, includeMax bool) ([]NodeID, error) {
	sm.mu.RLock()
	idx, exists := sm.rangeIndexes[name]
	sm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("range index %s not found", name)
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.entries) == 0 {
		return nil, nil
	}

	// Determine bounds
	var minF, maxF float64 = idx.entries[0].value - 1, idx.entries[len(idx.entries)-1].value + 1

	if minVal != nil {
		if f, ok := toFloat64ForIndex(minVal); ok {
			minF = f
		}
	}
	if maxVal != nil {
		if f, ok := toFloat64ForIndex(maxVal); ok {
			maxF = f
		}
	}

	// Binary search for start position
	start := sort.Search(len(idx.entries), func(i int) bool {
		if includeMin {
			return idx.entries[i].value >= minF
		}
		return idx.entries[i].value > minF
	})

	// Collect results
	var results []NodeID
	for i := start; i < len(idx.entries); i++ {
		v := idx.entries[i].value
		if includeMax {
			if v > maxF {
				break
			}
		} else {
			if v >= maxF {
				break
			}
		}
		results = append(results, idx.entries[i].nodeID)
	}

	return results, nil
}

// toFloat64ForIndex converts a value to float64 for range index operations.
func toFloat64ForIndex(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	case string:
		// Try to parse as float
		var f float64
		_, err := fmt.Sscanf(val, "%f", &f)
		return f, err == nil
	default:
		return 0, false
	}
}

// GetConstraints returns all unique constraints.
func (sm *SchemaManager) GetConstraints() []UniqueConstraint {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	constraints := make([]UniqueConstraint, 0, len(sm.uniqueConstraints))
	for _, c := range sm.uniqueConstraints {
		constraints = append(constraints, UniqueConstraint{
			Name:     c.Name,
			Label:    c.Label,
			Property: c.Property,
		})
	}

	return constraints
}

// GetConstraintsForLabels returns all constraints for given labels.
// Returns constraints from the constraints map, preserving their original types.
func (sm *SchemaManager) GetConstraintsForLabels(labels []string) []Constraint {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make([]Constraint, 0)

	// Get constraints from the constraints map (preserves original type)
	for _, c := range sm.constraints {
		for _, label := range labels {
			if c.Label == label {
				result = append(result, c)
				break
			}
		}
	}

	return result
}

// AddConstraint adds a constraint to the schema.
// Stores constraint in both the constraints map and uniqueConstraints (for backward compatibility).
func (sm *SchemaManager) AddConstraint(c Constraint) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Store in the constraints map (preserves type)
	if _, exists := sm.constraints[c.Name]; !exists {
		sm.constraints[c.Name] = c
	}

	// For UNIQUE constraints, also add to legacy uniqueConstraints map
	if c.Type == ConstraintUnique && len(c.Properties) == 1 {
		key := fmt.Sprintf("%s:%s", c.Label, c.Properties[0])
		if _, exists := sm.uniqueConstraints[key]; !exists {
			sm.uniqueConstraints[key] = &UniqueConstraint{
				Name:     c.Name,
				Label:    c.Label,
				Property: c.Properties[0],
				values:   make(map[interface{}]NodeID),
			}
		}
	}

	return nil
}

// GetIndexes returns all indexes.
func (sm *SchemaManager) GetIndexes() []interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	indexes := make([]interface{}, 0)

	for _, idx := range sm.propertyIndexes {
		indexes = append(indexes, map[string]interface{}{
			"name":       idx.Name,
			"type":       "PROPERTY",
			"label":      idx.Label,
			"properties": idx.Properties,
		})
	}

	for _, idx := range sm.compositeIndexes {
		indexes = append(indexes, map[string]interface{}{
			"name":       idx.Name,
			"type":       "COMPOSITE",
			"label":      idx.Label,
			"properties": idx.Properties,
		})
	}

	for _, idx := range sm.fulltextIndexes {
		indexes = append(indexes, map[string]interface{}{
			"name":       idx.Name,
			"type":       "FULLTEXT",
			"labels":     idx.Labels,
			"properties": idx.Properties,
		})
	}

	for _, idx := range sm.vectorIndexes {
		indexes = append(indexes, map[string]interface{}{
			"name":           idx.Name,
			"type":           "VECTOR",
			"label":          idx.Label,
			"property":       idx.Property,
			"dimensions":     idx.Dimensions,
			"similarityFunc": idx.SimilarityFunc,
		})
	}

	for _, idx := range sm.rangeIndexes {
		indexes = append(indexes, map[string]interface{}{
			"name":     idx.Name,
			"type":     "RANGE",
			"label":    idx.Label,
			"property": idx.Property,
		})
	}

	return indexes
}

// GetVectorIndex returns a vector index by name.
func (sm *SchemaManager) GetVectorIndex(name string) (*VectorIndex, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	idx, exists := sm.vectorIndexes[name]
	return idx, exists
}

// GetFulltextIndex returns a fulltext index by name.
func (sm *SchemaManager) GetFulltextIndex(name string) (*FulltextIndex, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	idx, exists := sm.fulltextIndexes[name]
	return idx, exists
}

// GetRangeIndex returns a range index by name.
func (sm *SchemaManager) GetRangeIndex(name string) (*RangeIndex, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	idx, exists := sm.rangeIndexes[name]
	return idx, exists
}

// GetPropertyIndex returns a property index by label and property.
func (sm *SchemaManager) GetPropertyIndex(label, property string) (*PropertyIndex, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", label, property)
	idx, exists := sm.propertyIndexes[key]
	return idx, exists
}

// PropertyIndexInsert adds a node to a property index.
func (sm *SchemaManager) PropertyIndexInsert(label, property string, nodeID NodeID, value interface{}) error {
	sm.mu.Lock()
	idx, exists := sm.propertyIndexes[fmt.Sprintf("%s:%s", label, property)]
	sm.mu.Unlock()

	if !exists {
		return fmt.Errorf("property index %s:%s not found", label, property)
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.values == nil {
		idx.values = make(map[interface{}][]NodeID)
	}

	idx.values[value] = append(idx.values[value], nodeID)
	return nil
}

// PropertyIndexDelete removes a node from a property index.
func (sm *SchemaManager) PropertyIndexDelete(label, property string, nodeID NodeID, value interface{}) error {
	sm.mu.Lock()
	idx, exists := sm.propertyIndexes[fmt.Sprintf("%s:%s", label, property)]
	sm.mu.Unlock()

	if !exists {
		return nil // Not indexed
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if ids, ok := idx.values[value]; ok {
		newIDs := make([]NodeID, 0, len(ids)-1)
		for _, id := range ids {
			if id != nodeID {
				newIDs = append(newIDs, id)
			}
		}
		if len(newIDs) > 0 {
			idx.values[value] = newIDs
		} else {
			delete(idx.values, value)
		}
	}
	return nil
}

// PropertyIndexLookup looks up node IDs by property value using an index.
// Returns nil if no index exists for the label/property.
func (sm *SchemaManager) PropertyIndexLookup(label, property string, value interface{}) []NodeID {
	sm.mu.RLock()
	idx, exists := sm.propertyIndexes[fmt.Sprintf("%s:%s", label, property)]
	sm.mu.RUnlock()

	if !exists {
		return nil
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if ids, ok := idx.values[value]; ok {
		// Return a copy to avoid mutation
		result := make([]NodeID, len(ids))
		copy(result, ids)
		return result
	}
	return nil
}

// IndexStats represents statistics about an index.
type IndexStats struct {
	Name         string   `json:"name"`
	Type         string   `json:"type"`
	Label        string   `json:"label"`
	Property     string   `json:"property,omitempty"`
	Properties   []string `json:"properties,omitempty"`
	TotalEntries int64    `json:"totalEntries"`
	UniqueValues int64    `json:"uniqueValues"`
	Selectivity  float64  `json:"selectivity"` // uniqueValues / totalEntries
}

// GetIndexStats returns statistics for all indexes.
func (sm *SchemaManager) GetIndexStats() []IndexStats {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var stats []IndexStats

	// Property indexes
	for _, idx := range sm.propertyIndexes {
		idx.mu.RLock()
		totalEntries := int64(0)
		for _, ids := range idx.values {
			totalEntries += int64(len(ids))
		}
		uniqueValues := int64(len(idx.values))
		selectivity := float64(0)
		if totalEntries > 0 {
			selectivity = float64(uniqueValues) / float64(totalEntries)
		}
		idx.mu.RUnlock()

		prop := ""
		if len(idx.Properties) > 0 {
			prop = idx.Properties[0]
		}

		stats = append(stats, IndexStats{
			Name:         idx.Name,
			Type:         "PROPERTY",
			Label:        idx.Label,
			Property:     prop,
			Properties:   idx.Properties,
			TotalEntries: totalEntries,
			UniqueValues: uniqueValues,
			Selectivity:  selectivity,
		})
	}

	// Range indexes
	for _, idx := range sm.rangeIndexes {
		idx.mu.RLock()
		totalEntries := int64(len(idx.entries))
		// For range indexes, each entry is unique
		uniqueValues := totalEntries
		selectivity := float64(1.0)
		if totalEntries > 0 {
			selectivity = float64(uniqueValues) / float64(totalEntries)
		}
		idx.mu.RUnlock()

		stats = append(stats, IndexStats{
			Name:         idx.Name,
			Type:         "RANGE",
			Label:        idx.Label,
			Property:     idx.Property,
			TotalEntries: totalEntries,
			UniqueValues: uniqueValues,
			Selectivity:  selectivity,
		})
	}

	// Composite indexes
	for _, idx := range sm.compositeIndexes {
		totalEntries := int64(0)
		for _, ids := range idx.fullIndex {
			totalEntries += int64(len(ids))
		}
		uniqueValues := int64(len(idx.fullIndex))
		selectivity := float64(0)
		if totalEntries > 0 {
			selectivity = float64(uniqueValues) / float64(totalEntries)
		}

		stats = append(stats, IndexStats{
			Name:         idx.Name,
			Type:         "COMPOSITE",
			Label:        idx.Label,
			Properties:   idx.Properties,
			TotalEntries: totalEntries,
			UniqueValues: uniqueValues,
			Selectivity:  selectivity,
		})
	}

	// Fulltext indexes
	for _, idx := range sm.fulltextIndexes {
		stats = append(stats, IndexStats{
			Name:         idx.Name,
			Type:         "FULLTEXT",
			Properties:   idx.Properties,
			TotalEntries: 0, // Would require integration with fulltext engine
			UniqueValues: 0,
			Selectivity:  0,
		})
	}

	// Vector indexes
	for _, idx := range sm.vectorIndexes {
		stats = append(stats, IndexStats{
			Name:         idx.Name,
			Type:         "VECTOR",
			Label:        idx.Label,
			Property:     idx.Property,
			TotalEntries: 0, // Would require integration with vector index
			UniqueValues: 0,
			Selectivity:  0,
		})
	}

	return stats
}

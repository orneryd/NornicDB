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
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
)

// ConstraintType represents the type of constraint.
type ConstraintType string

const (
	ConstraintUnique          ConstraintType = "UNIQUE"
	ConstraintNodeKey         ConstraintType = "NODE_KEY"
	ConstraintExists          ConstraintType = "EXISTS"
	ConstraintPropertyType    ConstraintType = "PROPERTY_TYPE"
	ConstraintTemporal        ConstraintType = "TEMPORAL_NO_OVERLAP"
	ConstraintRelationshipKey ConstraintType = "RELATIONSHIP_KEY"
	ConstraintDomain          ConstraintType = "DOMAIN"
	ConstraintCardinality     ConstraintType = "CARDINALITY"
	ConstraintPolicy          ConstraintType = "RELATIONSHIP_POLICY"
)

// ConstraintEntityType distinguishes node constraints from relationship constraints.
type ConstraintEntityType string

const (
	ConstraintEntityNode         ConstraintEntityType = "NODE"
	ConstraintEntityRelationship ConstraintEntityType = "RELATIONSHIP"
)

// Constraint represents a Neo4j-compatible schema constraint.
type Constraint struct {
	Name          string               `json:"name"`
	Type          ConstraintType       `json:"type"`
	EntityType    ConstraintEntityType `json:"entity_type,omitempty"` // defaults to NODE when empty
	Label         string               `json:"label"`                 // label for nodes, relationship type for relationships
	Properties    []string             `json:"properties,omitempty"`
	OwnedIndex    string               `json:"owned_index,omitempty"`    // name of the owned backing index (for uniqueness/key)
	AllowedValues []interface{}        `json:"allowed_values,omitempty"` // for DOMAIN constraints: list of allowed values
	MaxCount      int                  `json:"max_count,omitempty"`      // for CARDINALITY constraints: maximum edge count per node
	Direction     string               `json:"direction,omitempty"`      // for CARDINALITY constraints: "OUTGOING" or "INCOMING"
	SourceLabel   string               `json:"source_label,omitempty"`   // for RELATIONSHIP_POLICY constraints: required source node label
	TargetLabel   string               `json:"target_label,omitempty"`   // for RELATIONSHIP_POLICY constraints: required target node label
	PolicyMode    string               `json:"policy_mode,omitempty"`    // for RELATIONSHIP_POLICY constraints: "ALLOWED" or "DISALLOWED"
}

// EffectiveEntityType returns the entity type, defaulting to NODE for backward compatibility.
func (c Constraint) EffectiveEntityType() ConstraintEntityType {
	if c.EntityType == "" {
		return ConstraintEntityNode
	}
	return c.EntityType
}

// SchemaManager manages database schema including constraints and indexes.
type SchemaManager struct {
	mu sync.RWMutex

	// Constraints
	uniqueConstraints       map[string]*UniqueConstraint      // key: "Label:property"
	constraints             map[string]Constraint             // key: constraint name, stores all constraint types
	constraintContracts     map[string]ConstraintContract     // key: contract name
	propertyTypeConstraints map[string]PropertyTypeConstraint // key: constraint name

	// Indexes
	propertyIndexes  map[string]*PropertyIndex  // key: "Label:property" (single property)
	compositeIndexes map[string]*CompositeIndex // key: index name
	fulltextIndexes  map[string]*FulltextIndex  // key: index_name
	vectorIndexes    map[string]*VectorIndex    // key: index_name
	rangeIndexes     map[string]*RangeIndex     // key: index_name

	// Persistence hook (optional).
	// When set (by BadgerEngine), schema changes are persisted transactionally.
	persist func(def *SchemaDefinition) error

	// Knowledge-layer scoring subsystem
	decayProfileBundles  map[string]*knowledgepolicy.DecayProfileBundle
	decayProfileBindings map[string]*knowledgepolicy.DecayProfileBinding
	promotionProfiles    map[string]*knowledgepolicy.PromotionProfileDef
	promotionPolicies    map[string]*knowledgepolicy.PromotionPolicyDef
	bindingTable         *knowledgepolicy.BindingTable
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
		uniqueConstraints:       make(map[string]*UniqueConstraint),
		constraints:             make(map[string]Constraint),
		constraintContracts:     make(map[string]ConstraintContract),
		propertyTypeConstraints: make(map[string]PropertyTypeConstraint),
		propertyIndexes:         make(map[string]*PropertyIndex),
		compositeIndexes:        make(map[string]*CompositeIndex),
		fulltextIndexes:         make(map[string]*FulltextIndex),
		vectorIndexes:           make(map[string]*VectorIndex),
		rangeIndexes:            make(map[string]*RangeIndex),
	}
}

func (sm *SchemaManager) addConstraintLocked(c Constraint, silentOnDuplicate bool) error {
	if _, exists := sm.constraintContracts[c.Name]; exists {
		return fmt.Errorf("constraint %q already exists", c.Name)
	}
	if existing, exists := sm.constraints[c.Name]; exists {
		if constraintSchemaKey(existing) == constraintSchemaKey(c) && existing.Type == c.Type {
			if c.Type == ConstraintDomain && !allowedValuesEqual(existing.AllowedValues, c.AllowedValues) {
				return fmt.Errorf("constraint %q already exists with different allowed values", c.Name)
			}
			if c.Type == ConstraintCardinality && existing.MaxCount != c.MaxCount {
				return fmt.Errorf("constraint %q already exists with different max count (%d vs %d)", c.Name, existing.MaxCount, c.MaxCount)
			}
			if silentOnDuplicate {
				return nil
			}
			return fmt.Errorf("constraint %q already exists", c.Name)
		}
		return fmt.Errorf("constraint %q already exists with different schema or type", c.Name)
	}

	newKey := constraintSchemaKey(c)
	for _, existing := range sm.constraints {
		existKey := constraintSchemaKey(existing)
		if existKey != newKey {
			if c.Type == ConstraintPolicy && existing.Type == ConstraintPolicy &&
				c.Label == existing.Label &&
				c.SourceLabel == existing.SourceLabel && c.TargetLabel == existing.TargetLabel &&
				c.PolicyMode != existing.PolicyMode {
				return fmt.Errorf("conflicting policy: cannot have both ALLOWED and DISALLOWED for %s-[:%s]->%s (constraint %q)", c.SourceLabel, c.Label, c.TargetLabel, existing.Name)
			}
			continue
		}
		if existing.Type == c.Type {
			if c.Type == ConstraintDomain && !allowedValuesEqual(existing.AllowedValues, c.AllowedValues) {
				return fmt.Errorf("conflicting domain constraint %q already exists on same schema with different allowed values", existing.Name)
			}
			if c.Type == ConstraintCardinality && existing.MaxCount != c.MaxCount {
				return fmt.Errorf("conflicting cardinality constraint %q already exists on %s %s with max count %d (new: %d)", existing.Name, existing.Direction, existing.Label, existing.MaxCount, c.MaxCount)
			}
			if silentOnDuplicate {
				return nil
			}
			return fmt.Errorf("equivalent constraint %q already exists on same schema", existing.Name)
		}
		if (c.Type == ConstraintUnique && existing.Type == ConstraintRelationshipKey) ||
			(c.Type == ConstraintRelationshipKey && existing.Type == ConstraintUnique) ||
			(c.Type == ConstraintUnique && existing.Type == ConstraintNodeKey) ||
			(c.Type == ConstraintNodeKey && existing.Type == ConstraintUnique) {
			return fmt.Errorf("conflicting constraint %q already exists on same schema", existing.Name)
		}
	}

	if c.EffectiveEntityType() == ConstraintEntityRelationship &&
		(c.Type == ConstraintUnique || c.Type == ConstraintRelationshipKey) &&
		c.OwnedIndex == "" {
		c.OwnedIndex = c.Name + "_index"
	}

	sm.constraints[c.Name] = c

	if c.OwnedIndex != "" {
		if _, exists := sm.rangeIndexes[c.OwnedIndex]; !exists {
			prop := ""
			if len(c.Properties) > 0 {
				prop = c.Properties[0]
			}
			sm.rangeIndexes[c.OwnedIndex] = &RangeIndex{
				Name:             c.OwnedIndex,
				Label:            c.Label,
				Property:         prop,
				Properties:       c.Properties,
				EntityType:       c.EffectiveEntityType(),
				OwningConstraint: c.Name,
				entries:          make([]rangeEntry, 0),
				nodeValue:        make(map[NodeID]float64),
			}
		}
	}

	if c.Type == ConstraintUnique && len(c.Properties) == 1 {
		uniqueKey := fmt.Sprintf("%s:%s", c.Label, c.Properties[0])
		if _, exists := sm.uniqueConstraints[uniqueKey]; !exists {
			sm.uniqueConstraints[uniqueKey] = &UniqueConstraint{
				Name:     c.Name,
				Label:    c.Label,
				Property: c.Properties[0],
				values:   make(map[interface{}]NodeID),
			}
		}
	}

	return nil
}

// SetPersister sets an optional persistence hook for schema changes.
// When set, schema mutations will attempt to persist the updated schema definition
// and will roll back the in-memory change if persistence fails.
func (sm *SchemaManager) SetPersister(persist func(def *SchemaDefinition) error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.persist = persist
}

// UniqueConstraint represents a unique constraint on a label and property.
type UniqueConstraint struct {
	Name                string
	Label               string
	Property            string
	values              map[interface{}]NodeID // Track unique values
	valuesAuthoritative bool
	mu                  sync.RWMutex
}

// PropertyIndex represents a property index for faster lookups.
type PropertyIndex struct {
	Name       string
	Label      string
	Properties []string
	values     map[interface{}][]NodeID // Property value -> node IDs
	// sortedNonNilKeys caches non-nil keys in ascending order.
	// It is rebuilt lazily when values are mutated.
	sortedNonNilKeys []interface{}
	keysDirty        bool
	mu               sync.RWMutex
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
//   - Composite unique constraints (email + database_id)
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
	Name             string
	Label            string
	Property         string
	Properties       []string             // composite properties (for multi-property constraint indexes)
	EntityType       ConstraintEntityType // NODE or RELATIONSHIP
	OwningConstraint string               // name of the constraint that owns this index (empty if standalone)
	entries          []rangeEntry         // Sorted by value for binary search
	nodeValue        map[NodeID]float64   // NodeID -> current numeric value (for delete/update)
	mu               sync.RWMutex
}

// AddUniqueConstraint adds a unique constraint.
// Stores in both uniqueConstraints (for value tracking) and constraints (for lookup by label).
// Pass ifNotExists=true for IF NOT EXISTS semantics (duplicate is no-op).
func (sm *SchemaManager) AddUniqueConstraint(name, label, property string, ifNotExists ...bool) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	silent := len(ifNotExists) > 0 && ifNotExists[0]
	key := fmt.Sprintf("%s:%s", label, property)
	if _, exists := sm.uniqueConstraints[key]; exists {
		if silent {
			return nil
		}
		return fmt.Errorf("constraint %q already exists", name)
	}

	// Add to uniqueConstraints (for value tracking during CheckUniqueConstraint)
	sm.uniqueConstraints[key] = &UniqueConstraint{
		Name:     name,
		Label:    label,
		Property: property,
		values:   make(map[interface{}]NodeID),
	}

	// Also add to constraints map (for lookup by GetConstraintsForLabels in transactions)
	constraint := Constraint{
		Name:       name,
		Label:      label,
		Properties: []string{property},
		Type:       ConstraintUnique,
	}
	sm.constraints[name] = constraint

	// Persist schema if configured. If persistence fails, roll back the in-memory change.
	if sm.persist != nil {
		def := sm.exportDefinitionLocked()
		if err := sm.persist(def); err != nil {
			delete(sm.uniqueConstraints, key)
			delete(sm.constraints, name)
			return err
		}
	}

	return nil
}

// AddPropertyTypeConstraint adds a property type constraint to the schema.
// This enforces a specific type for a property on a label (NULL values allowed).
// An optional entityType can be passed to specify RELATIONSHIP constraints.
// PropertyTypeConstraintOptions holds optional parameters for AddPropertyTypeConstraint.
type PropertyTypeConstraintOptions struct {
	EntityType  ConstraintEntityType
	IfNotExists bool
}

// AddPropertyTypeConstraint adds a property type constraint to the schema.
// The entityType parameter controls NODE vs RELATIONSHIP scoping.
func (sm *SchemaManager) AddPropertyTypeConstraint(name, label, property string, expectedType PropertyType, entityType ...ConstraintEntityType) error {
	var et ConstraintEntityType
	if len(entityType) > 0 {
		et = entityType[0]
	}
	return sm.addPropertyTypeConstraint(name, label, property, expectedType, et, false)
}

// AddPropertyTypeConstraintWithOptions adds a property type constraint with full options.
func (sm *SchemaManager) AddPropertyTypeConstraintWithOptions(name, label, property string, expectedType PropertyType, opts PropertyTypeConstraintOptions) error {
	return sm.addPropertyTypeConstraint(name, label, property, expectedType, opts.EntityType, opts.IfNotExists)
}

func (sm *SchemaManager) addPropertyTypeConstraint(name, label, property string, expectedType PropertyType, entityType ConstraintEntityType, ifNotExists bool) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	snapshot := sm.exportDefinitionLocked()
	ptc := PropertyTypeConstraint{
		Name:         name,
		EntityType:   entityType,
		Label:        label,
		Property:     property,
		ExpectedType: expectedType,
	}
	if err := sm.addPropertyTypeConstraintValueLocked(ptc, ifNotExists); err != nil {
		return err
	}

	if sm.persist != nil {
		def := sm.exportDefinitionLocked()
		if err := sm.persist(def); err != nil {
			sm.replaceFromDefinitionLocked(snapshot)
			return err
		}
	}

	return nil
}

func (sm *SchemaManager) addPropertyTypeConstraintValueLocked(ptc PropertyTypeConstraint, ifNotExists bool) error {
	if _, exists := sm.propertyTypeConstraints[ptc.Name]; exists {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("constraint %q already exists", ptc.Name)
	}
	if _, exists := sm.constraintContracts[ptc.Name]; exists {
		return fmt.Errorf("constraint %q already exists", ptc.Name)
	}
	sm.propertyTypeConstraints[ptc.Name] = ptc
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

	valueKey, ok := uniqueConstraintValueKey(value)
	if !ok {
		return nil
	}

	if existingNode, found := constraint.values[valueKey]; found {
		if existingNode != excludeNode {
			return fmt.Errorf("Node(%s) already exists with %s = %v", label, property, value)
		}
	}

	return nil
}

// LookupUniqueConstraintValue returns the node currently registered for a
// single-property uniqueness constraint value. The second return value reports
// whether the value is present, and the third reports whether the unique
// constraint exists.
func (sm *SchemaManager) LookupUniqueConstraintValue(label, property string, value interface{}) (NodeID, bool, bool) {
	sm.mu.RLock()
	key := fmt.Sprintf("%s:%s", label, property)
	constraint, exists := sm.uniqueConstraints[key]
	sm.mu.RUnlock()
	if !exists || value == nil {
		return "", false, exists
	}
	valueKey, ok := uniqueConstraintValueKey(value)
	if !ok {
		return "", false, true
	}
	if !isComparableConstraintValue(value) {
		return "", true, false
	}

	constraint.mu.RLock()
	defer constraint.mu.RUnlock()
	nodeID, found := constraint.values[valueKey]
	return nodeID, found, true
}

func (sm *SchemaManager) lookupUniqueConstraintValueForValidation(label, property string, value interface{}) (NodeID, bool, bool, bool) {
	sm.mu.RLock()
	key := fmt.Sprintf("%s:%s", label, property)
	constraint, exists := sm.uniqueConstraints[key]
	sm.mu.RUnlock()
	if !exists || value == nil {
		return "", false, false, exists
	}
	valueKey, ok := uniqueConstraintValueKey(value)
	if !ok {
		return "", false, false, true
	}

	constraint.mu.RLock()
	defer constraint.mu.RUnlock()
	nodeID, found := constraint.values[valueKey]
	return nodeID, found, constraint.valuesAuthoritative, true
}

func uniqueConstraintValueKey(value interface{}) (interface{}, bool) {
	if numeric, ok := numericConstraintValue(value); ok {
		return numeric, true
	}
	if value == nil {
		return nil, false
	}
	if !reflect.TypeOf(value).Comparable() {
		return nil, false
	}
	return value, true
}

func isComparableConstraintValue(value interface{}) bool {
	if value == nil {
		return true
	}
	return reflect.TypeOf(value).Comparable()
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
	valueKey, ok := uniqueConstraintValueKey(value)
	if !ok {
		return
	}

	constraint.mu.Lock()
	constraint.values[valueKey] = nodeID
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
	valueKey, ok := uniqueConstraintValueKey(value)
	if !ok {
		return
	}

	constraint.mu.Lock()
	delete(constraint.values, valueKey)
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
		keysDirty:  true,
	}

	if sm.persist != nil {
		def := sm.exportDefinitionLocked()
		if err := sm.persist(def); err != nil {
			delete(sm.propertyIndexes, key)
			return err
		}
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

	if sm.persist != nil {
		def := sm.exportDefinitionLocked()
		if err := sm.persist(def); err != nil {
			delete(sm.compositeIndexes, name)
			return err
		}
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

	if sm.persist != nil {
		def := sm.exportDefinitionLocked()
		if err := sm.persist(def); err != nil {
			delete(sm.fulltextIndexes, name)
			return err
		}
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

	if sm.persist != nil {
		def := sm.exportDefinitionLocked()
		if err := sm.persist(def); err != nil {
			delete(sm.vectorIndexes, name)
			return err
		}
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
		Name:      name,
		Label:     label,
		Property:  property,
		entries:   make([]rangeEntry, 0),
		nodeValue: make(map[NodeID]float64), // NodeID -> value
	}

	if sm.persist != nil {
		def := sm.exportDefinitionLocked()
		if err := sm.persist(def); err != nil {
			delete(sm.rangeIndexes, name)
			return err
		}
	}

	return nil
}

// rangeEntry represents a single entry in the range index.
type rangeEntry struct {
	value  float64 // Normalized numeric value for comparison
	nodeID NodeID
}

func (idx *RangeIndex) deleteEntryLocked(nodeID NodeID, value float64) bool {
	// Find first entry with value >= target.
	start := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].value >= value
	})

	// Scan until value differs; remove the matching nodeID.
	for i := start; i < len(idx.entries); i++ {
		entry := idx.entries[i]
		if entry.value != value {
			break
		}
		if entry.nodeID != nodeID {
			continue
		}
		idx.entries = append(idx.entries[:i], idx.entries[i+1:]...)
		return true
	}

	return false
}

// RangeIndexInsert adds a value to a range index.
func (sm *SchemaManager) RangeIndexInsert(name string, nodeID NodeID, value interface{}) error {
	sm.mu.RLock()
	idx, exists := sm.rangeIndexes[name]
	sm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("range index %s not found", name)
	}

	// Convert value to float64 for comparison
	numVal, ok := convert.ToFloat64(value)
	if !ok {
		return fmt.Errorf("range index only supports numeric values, got %T", value)
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// If this node already exists in the index, remove its prior entry so we don't
	// accumulate duplicate NodeID rows (which would both be incorrect and cause
	// O(n^2) behavior over time).
	if prev, ok := idx.nodeValue[nodeID]; ok {
		_ = idx.deleteEntryLocked(nodeID, prev)
	}

	// Binary search for insert position
	pos := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].value >= numVal
	})

	// Insert at position
	entry := rangeEntry{value: numVal, nodeID: nodeID}
	idx.entries = append(idx.entries, rangeEntry{})
	copy(idx.entries[pos+1:], idx.entries[pos:])
	idx.entries[pos] = entry
	idx.nodeValue[nodeID] = numVal

	return nil
}

// RangeIndexDelete removes a value from a range index.
func (sm *SchemaManager) RangeIndexDelete(name string, nodeID NodeID) error {
	sm.mu.RLock()
	idx, exists := sm.rangeIndexes[name]
	sm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("range index %s not found", name)
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	value, exists := idx.nodeValue[nodeID]
	if !exists {
		return nil // Not in index
	}

	_ = idx.deleteEntryLocked(nodeID, value)
	delete(idx.nodeValue, nodeID)

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
		if f, ok := convert.ToFloat64(minVal); ok {
			minF = f
		}
	}
	if maxVal != nil {
		if f, ok := convert.ToFloat64(maxVal); ok {
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

// GetAllConstraints returns all constraints in the schema, regardless of label.
// This is used by db.constraints() procedure to list all constraints.
func (sm *SchemaManager) GetAllConstraints() []Constraint {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make([]Constraint, 0, len(sm.constraints))
	for _, c := range sm.constraints {
		result = append(result, c)
	}

	return result
}

// constraintSchemaKey returns a key that identifies the "schema" of a constraint
// (entity type + label + sorted properties). Two constraints with the same schema key
// target the same storage schema.
func constraintSchemaKey(c Constraint) string {
	props := make([]string, len(c.Properties))
	copy(props, c.Properties)
	sort.Strings(props)
	base := fmt.Sprintf("%s:%s:%s", c.EffectiveEntityType(), c.Label, strings.Join(props, ","))
	// Policy constraints are further scoped by source/target labels and direction
	if c.Type == ConstraintPolicy {
		return fmt.Sprintf("%s:%s->%s:%s", base, c.SourceLabel, c.TargetLabel, c.PolicyMode)
	}
	if c.Type == ConstraintCardinality {
		return fmt.Sprintf("%s:%s", base, c.Direction)
	}
	return base
}

// allowedValuesEqual checks whether two AllowedValues lists contain the same values (order-insensitive).
func allowedValuesEqual(a, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	// Build frequency map using string representation
	counts := make(map[string]int, len(a))
	for _, v := range a {
		counts[fmt.Sprint(v)]++
	}
	for _, v := range b {
		key := fmt.Sprint(v)
		counts[key]--
		if counts[key] < 0 {
			return false
		}
	}
	return true
}

// AddConstraint adds a constraint to the schema.
// Stores constraint in both the constraints map and uniqueConstraints (for backward compatibility).
//
// Conflict rules (matching Neo4j behavior):
//   - Same name, already exists with identical schema+type: error, unless ifNotExists (then no-op)
//   - Same name, different schema or type: error
//   - Different name, same schema + same type: error (duplicate schema), unless ifNotExists
//   - Uniqueness vs relationship key on same schema: error (conflicting)
//
// Pass ifNotExists=true when the DDL includes IF NOT EXISTS; duplicate-schema is then a no-op.
func (sm *SchemaManager) AddConstraint(c Constraint, ifNotExists ...bool) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	silentOnDuplicate := len(ifNotExists) > 0 && ifNotExists[0]
	snapshot := sm.exportDefinitionLocked()
	if err := sm.addConstraintLocked(c, silentOnDuplicate); err != nil {
		return err
	}

	if sm.persist != nil {
		def := sm.exportDefinitionLocked()
		if err := sm.persist(def); err != nil {
			sm.replaceFromDefinitionLocked(snapshot)
			return err
		}
	}

	return nil
}

// DropIndex removes an index (by name) from the schema.
// It searches across all index types: property, composite, fulltext, vector, and range.
func (sm *SchemaManager) DropIndex(name string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Track what we dropped for rollback on persist failure.
	type dropped struct {
		kind string
		key  string
	}
	var d dropped

	// compositeIndexes, fulltextIndexes, vectorIndexes, rangeIndexes are keyed by name.
	if _, ok := sm.compositeIndexes[name]; ok {
		d = dropped{kind: "composite", key: name}
	} else if _, ok := sm.fulltextIndexes[name]; ok {
		d = dropped{kind: "fulltext", key: name}
	} else if _, ok := sm.vectorIndexes[name]; ok {
		d = dropped{kind: "vector", key: name}
	} else if _, ok := sm.rangeIndexes[name]; ok {
		d = dropped{kind: "range", key: name}
	} else {
		// propertyIndexes are keyed by "label:property[0]", so search by name.
		for key, idx := range sm.propertyIndexes {
			if idx.Name == name {
				d = dropped{kind: "property", key: key}
				break
			}
		}
	}

	if d.kind == "" {
		return fmt.Errorf("index %q does not exist", name)
	}

	// Stash the old value for rollback, then delete.
	var oldProperty *PropertyIndex
	var oldComposite *CompositeIndex
	var oldFulltext *FulltextIndex
	var oldVector *VectorIndex
	var oldRange *RangeIndex

	switch d.kind {
	case "property":
		oldProperty = sm.propertyIndexes[d.key]
		delete(sm.propertyIndexes, d.key)
	case "composite":
		oldComposite = sm.compositeIndexes[d.key]
		delete(sm.compositeIndexes, d.key)
	case "fulltext":
		oldFulltext = sm.fulltextIndexes[d.key]
		delete(sm.fulltextIndexes, d.key)
	case "vector":
		oldVector = sm.vectorIndexes[d.key]
		delete(sm.vectorIndexes, d.key)
	case "range":
		oldRange = sm.rangeIndexes[d.key]
		delete(sm.rangeIndexes, d.key)
	}

	if sm.persist != nil {
		def := sm.exportDefinitionLocked()
		if err := sm.persist(def); err != nil {
			// Rollback in-memory delete.
			switch d.kind {
			case "property":
				sm.propertyIndexes[d.key] = oldProperty
			case "composite":
				sm.compositeIndexes[d.key] = oldComposite
			case "fulltext":
				sm.fulltextIndexes[d.key] = oldFulltext
			case "vector":
				sm.vectorIndexes[d.key] = oldVector
			case "range":
				sm.rangeIndexes[d.key] = oldRange
			}
			return err
		}
	}

	return nil
}

// DropConstraint removes a constraint (by name) from the schema.
// This supports both standard constraints and property type constraints.
func (sm *SchemaManager) DropConstraint(name string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var droppedConstraint *Constraint
	var droppedUnique *UniqueConstraint
	var droppedTypeConstraint *PropertyTypeConstraint
	var droppedUniqueKey string

	var droppedOwnedIndex *RangeIndex
	var droppedOwnedIndexName string

	if c, ok := sm.constraints[name]; ok {
		droppedConstraint = &c
		delete(sm.constraints, name)

		if c.Type == ConstraintUnique && len(c.Properties) == 1 {
			droppedUniqueKey = fmt.Sprintf("%s:%s", c.Label, c.Properties[0])
			if existing, ok := sm.uniqueConstraints[droppedUniqueKey]; ok {
				droppedUnique = existing
				delete(sm.uniqueConstraints, droppedUniqueKey)
			}
		}

		// Drop owned backing index
		if c.OwnedIndex != "" {
			if ri, ok := sm.rangeIndexes[c.OwnedIndex]; ok {
				droppedOwnedIndex = ri
				droppedOwnedIndexName = c.OwnedIndex
				delete(sm.rangeIndexes, c.OwnedIndex)
			}
		}
	} else if ptc, ok := sm.propertyTypeConstraints[name]; ok {
		droppedTypeConstraint = &ptc
		delete(sm.propertyTypeConstraints, name)
	} else {
		return fmt.Errorf("constraint %q does not exist", name)
	}

	if sm.persist != nil {
		def := sm.exportDefinitionLocked()
		if err := sm.persist(def); err != nil {
			if droppedConstraint != nil {
				sm.constraints[name] = *droppedConstraint
				if droppedUnique != nil {
					sm.uniqueConstraints[droppedUniqueKey] = droppedUnique
				}
				if droppedOwnedIndex != nil {
					sm.rangeIndexes[droppedOwnedIndexName] = droppedOwnedIndex
				}
			}
			if droppedTypeConstraint != nil {
				sm.propertyTypeConstraints[name] = *droppedTypeConstraint
			}
			return err
		}
	}

	return nil
}

// GetPropertyTypeConstraintsForLabels returns type constraints for the given labels.
func (sm *SchemaManager) GetPropertyTypeConstraintsForLabels(labels []string) []PropertyTypeConstraint {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make([]PropertyTypeConstraint, 0)
	for _, c := range sm.propertyTypeConstraints {
		for _, label := range labels {
			if c.Label == label {
				result = append(result, c)
				break
			}
		}
	}

	return result
}

// GetAllPropertyTypeConstraints returns all property type constraints.
func (sm *SchemaManager) GetAllPropertyTypeConstraints() []PropertyTypeConstraint {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make([]PropertyTypeConstraint, 0, len(sm.propertyTypeConstraints))
	for _, c := range sm.propertyTypeConstraints {
		result = append(result, c)
	}
	return result
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
		m := map[string]interface{}{
			"name":  idx.Name,
			"type":  "RANGE",
			"label": idx.Label,
		}
		// Export entity type (default NODE for backward compat)
		if idx.EntityType != "" {
			m["entityType"] = string(idx.EntityType)
		}
		// Export owning constraint if present
		if idx.OwningConstraint != "" {
			m["owningConstraint"] = idx.OwningConstraint
		}
		// Export properties: prefer composite list, fall back to single property
		if len(idx.Properties) > 0 {
			m["properties"] = idx.Properties
		} else if idx.Property != "" {
			m["property"] = idx.Property
		}
		indexes = append(indexes, m)
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

	if _, exists := idx.values[value]; !exists {
		idx.keysDirty = true
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
			idx.keysDirty = true
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

// PropertyIndexTopK returns up to limit node IDs from a property index ordered by
// indexed property value. Nil keys are skipped.
func (sm *SchemaManager) PropertyIndexTopK(label, property string, limit int, descending bool) []NodeID {
	if limit <= 0 {
		return nil
	}

	sm.mu.RLock()
	idx, exists := sm.propertyIndexes[fmt.Sprintf("%s:%s", label, property)]
	sm.mu.RUnlock()
	if !exists || idx == nil {
		return nil
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	keys := idx.sortedKeysLocked()
	if len(keys) == 0 {
		return nil
	}

	out := make([]NodeID, 0, limit)
	if descending {
		for i := len(keys) - 1; i >= 0; i-- {
			k := keys[i]
			ids := idx.values[k]
			if len(ids) == 0 {
				continue
			}
			copied := make([]NodeID, len(ids))
			copy(copied, ids)
			sort.Slice(copied, func(i, j int) bool { return string(copied[i]) < string(copied[j]) })
			for _, id := range copied {
				out = append(out, id)
				if len(out) >= limit {
					return out
				}
			}
		}
		return out
	}
	for _, k := range keys {
		ids := idx.values[k]
		if len(ids) == 0 {
			continue
		}
		copied := make([]NodeID, len(ids))
		copy(copied, ids)
		sort.Slice(copied, func(i, j int) bool { return string(copied[i]) < string(copied[j]) })
		for _, id := range copied {
			out = append(out, id)
			if len(out) >= limit {
				return out
			}
		}
	}
	return out
}

// PropertyIndexAllNonNil returns all node IDs from the property index in key order,
// excluding nil keys.
func (sm *SchemaManager) PropertyIndexAllNonNil(label, property string, descending bool) []NodeID {
	sm.mu.RLock()
	idx, exists := sm.propertyIndexes[fmt.Sprintf("%s:%s", label, property)]
	sm.mu.RUnlock()
	if !exists || idx == nil {
		return nil
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	keys := idx.sortedKeysLocked()
	if len(keys) == 0 {
		return nil
	}

	out := make([]NodeID, 0, len(keys))
	if descending {
		for i := len(keys) - 1; i >= 0; i-- {
			k := keys[i]
			ids := idx.values[k]
			if len(ids) == 0 {
				continue
			}
			copied := make([]NodeID, len(ids))
			copy(copied, ids)
			sort.Slice(copied, func(i, j int) bool { return string(copied[i]) < string(copied[j]) })
			out = append(out, copied...)
		}
		return out
	}
	for _, k := range keys {
		ids := idx.values[k]
		if len(ids) == 0 {
			continue
		}
		copied := make([]NodeID, len(ids))
		copy(copied, ids)
		sort.Slice(copied, func(i, j int) bool { return string(copied[i]) < string(copied[j]) })
		out = append(out, copied...)
	}
	return out
}

// sortedKeysLocked returns non-nil index keys in ascending order.
// Caller must hold idx.mu (read or write lock).
func (idx *PropertyIndex) sortedKeysLocked() []interface{} {
	if idx.keysDirty || idx.sortedNonNilKeys == nil {
		keys := make([]interface{}, 0, len(idx.values))
		for k, ids := range idx.values {
			if k == nil || len(ids) == 0 {
				continue
			}
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			return compareSchemaIndexValues(keys[i], keys[j]) < 0
		})
		idx.sortedNonNilKeys = keys
		idx.keysDirty = false
	}
	out := make([]interface{}, len(idx.sortedNonNilKeys))
	copy(out, idx.sortedNonNilKeys)
	return out
}

func compareSchemaIndexValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	if af, ok := convert.ToFloat64(a); ok {
		if bf, ok2 := convert.ToFloat64(b); ok2 {
			if af < bf {
				return -1
			}
			if af > bf {
				return 1
			}
			return 0
		}
	}

	if as, ok := a.(string); ok {
		if bs, ok2 := b.(string); ok2 {
			if as < bs {
				return -1
			}
			if as > bs {
				return 1
			}
			return 0
		}
	}

	astr := fmt.Sprintf("%v", a)
	bstr := fmt.Sprintf("%v", b)
	if astr < bstr {
		return -1
	}
	if astr > bstr {
		return 1
	}
	return 0
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

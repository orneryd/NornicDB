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
	"cmp"
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/security"
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
	mu         sync.RWMutex
	tokenOrder schemaTokenOrder

	// Active commit-time locks for exact UNIQUE constraint values. Entries are
	// reference-counted across holders and waiters, then removed when unused, so
	// same-value transactions serialize without false contention between
	// disjoint production-sized batches or unbounded historical-key growth.
	uniqueConstraintCommitLocksMu sync.Mutex
	uniqueConstraintCommitLocks   map[uniqueConstraintLockKey]*uniqueConstraintCommitLock
	uniqueConstraintCommitOrder   uint64
	// uniqueConstraintLockWaits maps each owner waiting for a constraint key
	// lock to that lock, for deadlock detection (#961).
	uniqueConstraintLockWaits map[string]*uniqueConstraintCommitLock
	schemaMutationMu          sync.Mutex

	// Constraints
	uniqueConstraints       map[string]*UniqueConstraint      // key: "Label:property"
	constraints             map[string]Constraint             // key: constraint name, stores all constraint types
	constraintContracts     map[string]ConstraintContract     // key: contract name
	propertyTypeConstraints map[string]PropertyTypeConstraint // key: constraint name

	// pendingWrites is the write cache of an AsyncEngine above the engine that
	// maintains these indexes; index lookups merge its pending writes (#719).
	// Nil when writes reach the engine directly.
	pendingWrites atomic.Pointer[pendingWriteAttachment]

	// Indexes. compositeIndexes is the single home of every property index:
	// a single-property index is an arity-1 CompositeIndex (see the
	// PropertyIndex type alias). fulltext/vector/range/lookup keep their own
	// structures because they are ordered, token or vector indexes, not
	// equality indexes.
	compositeIndexes map[string]*CompositeIndex      // key: index name
	fulltextIndexes  map[string]*FulltextIndex       // key: index_name
	vectorIndexes    map[string]*VectorIndex         // key: index_name
	rangeIndexes     map[string]*RangeIndex          // key: index_name
	lookupIndexes    map[ConstraintEntityType]string // token lookup index name by entity type (schema_lookup_index.go)

	// Persistence hook (optional).
	// When set (by BadgerEngine), schema changes are persisted transactionally.
	persist func(def *SchemaDefinition) error
	// Optional callback invoked after knowledge-policy mutations have been
	// persisted and the schema lock has been released.
	knowledgePolicyChanged func()

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
//	schema.AddPropertyIndex("idx_user_email", "User", []string{"email"})
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
		compositeIndexes:        make(map[string]*CompositeIndex),
		fulltextIndexes:         make(map[string]*FulltextIndex),
		vectorIndexes:           make(map[string]*VectorIndex),
		rangeIndexes:            make(map[string]*RangeIndex),
		lookupIndexes:           defaultLookupIndexes(),
	}
}

// addConstraintLocked admits c (admitConstraintLocked) and applies it when
// admitted; added is false for an IF NOT EXISTS that finds it present.
func (sm *SchemaManager) addConstraintLocked(c Constraint, silentOnDuplicate bool) (added bool, err error) {
	added, err = sm.admitConstraintLocked(c, silentOnDuplicate)
	if added {
		sm.applyConstraintLocked(c)
	}
	return added, err
}

// admitConstraintLocked checks c against the schema without changing it:
// an error for a conflict, false for an equivalent constraint under IF NOT
// EXISTS, true when c is to be added.
func (sm *SchemaManager) admitConstraintLocked(c Constraint, silentOnDuplicate bool) (bool, error) {
	if _, exists := sm.constraintContracts[c.Name]; exists {
		return false, newSchemaAdmissionError("ConstraintWithNameAlreadyExists", localization.StorageSchemaConstraintAlreadyExists(c.Name))
	}
	if existing, exists := sm.constraints[c.Name]; exists {
		if sameConstraintSchema(existing, c) && existing.Type == c.Type {
			if c.Type == ConstraintDomain && !allowedValuesEqual(existing.AllowedValues, c.AllowedValues) {
				return false, localizedError(localization.StorageSchemaConstraintDifferentAllowedValues(c.Name), nil)
			}
			if c.Type == ConstraintCardinality && existing.MaxCount != c.MaxCount {
				return false, localizedError(localization.StorageSchemaConstraintDifferentMaxCount(c.Name, existing.MaxCount, c.MaxCount), nil)
			}
			if silentOnDuplicate {
				return false, nil
			}
			return false, newSchemaAdmissionError("EquivalentSchemaRuleAlreadyExists", localization.StorageSchemaConstraintAlreadyExists(c.Name))
		}
		return false, newSchemaAdmissionError("ConstraintWithNameAlreadyExists", localization.StorageSchemaConstraintDifferentSchemaOrType(c.Name))
	}

	if sm.indexNameTakenLocked(c.Name) {
		return false, newSchemaAdmissionError("IndexWithNameAlreadyExists", localization.StorageSchemaIndexNameAlreadyExists(c.Name))
	}

	for _, existing := range sm.constraints {
		if !sameConstraintSchema(existing, c) {
			if c.Type == ConstraintPolicy && existing.Type == ConstraintPolicy &&
				c.Label == existing.Label &&
				c.SourceLabel == existing.SourceLabel && c.TargetLabel == existing.TargetLabel &&
				c.PolicyMode != existing.PolicyMode {
				return false, localizedError(localization.StorageSchemaConflictingPolicy(c.SourceLabel, c.Label, c.TargetLabel, existing.Name), nil)
			}
			continue
		}
		if existing.Type == c.Type {
			if c.Type == ConstraintDomain && !allowedValuesEqual(existing.AllowedValues, c.AllowedValues) {
				return false, localizedError(localization.StorageSchemaConflictingDomainConstraint(existing.Name), nil)
			}
			if c.Type == ConstraintCardinality && existing.MaxCount != c.MaxCount {
				return false, localizedError(localization.StorageSchemaConflictingCardinalityConstraint(existing.Name, existing.Direction, existing.Label, existing.MaxCount, c.MaxCount), nil)
			}
			if silentOnDuplicate {
				return false, nil
			}
			return false, newSchemaAdmissionError("ConstraintAlreadyExists", localization.StorageSchemaEquivalentConstraintAlreadyExists(existing.Name))
		}
		if (c.Type == ConstraintUnique && existing.Type == ConstraintRelationshipKey) ||
			(c.Type == ConstraintRelationshipKey && existing.Type == ConstraintUnique) ||
			(c.Type == ConstraintUnique && existing.Type == ConstraintNodeKey) ||
			(c.Type == ConstraintNodeKey && existing.Type == ConstraintUnique) {
			return false, localizedError(localization.StorageSchemaConflictingConstraintAlreadyExists(existing.Name), nil)
		}
	}

	// The constraint's index can't duplicate an index of its own, even under
	// IF NOT EXISTS: they are different schema rules (#884).
	if constraintPropertyIndexKey(c) != "" {
		if idx, exists := sm.arity1PropertyIndexLocked(c.Label, c.Properties[0]); exists && idx.OwningConstraint == "" {
			return false, newSchemaAdmissionError("IndexAlreadyExists", localization.StorageSchemaConstraintOverIndex(c.Label, c.Properties[0]))
		}
	}

	return true, nil
}

// applyConstraintLocked adds c, which admitConstraintLocked admitted, with
// its owned index and unique-value tracking.
func (sm *SchemaManager) applyConstraintLocked(c Constraint) {
	// An index-backed constraint owns an index of its own name, for nodes and
	// relationships alike, as in Neo4j.
	if c.OwnedIndex == "" && (c.Type == ConstraintUnique || c.Type == ConstraintNodeKey || c.Type == ConstraintRelationshipKey) {
		c.OwnedIndex = c.Name
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

	sm.addConstraintPropertyIndexLocked(c)

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
}

// constraintPropertyIndexKey reports whether a constraint owns a
// single-property node equality index (uniqueness or node key): it is the
// index's canonical name ("Label:property") when it does, "" otherwise. It
// is still used by callers that need to know whether c owns such an index.
func constraintPropertyIndexKey(c Constraint) string {
	if (c.Type != ConstraintUnique && c.Type != ConstraintNodeKey) || len(c.Properties) != 1 || c.EffectiveEntityType() != ConstraintEntityNode {
		return ""
	}
	return c.Label + ":" + c.Properties[0]
}

// arity1PropertyIndexLocked returns the arity-1 composite index on label's
// property, or nil. The caller holds sm.mu.
func (sm *SchemaManager) arity1PropertyIndexLocked(label, property string) (*CompositeIndex, bool) {
	for _, idx := range sm.compositeIndexes {
		if idx != nil && idx.Label == label && len(idx.Properties) == 1 && idx.Properties[0] == property {
			return idx, true
		}
	}
	return nil, false
}

// addConstraintPropertyIndexLocked registers the equality index a
// single-property uniqueness or node key constraint owns (#875), so property
// seeks use it as they use CREATE INDEX's. It starts empty and unfilled, so
// seeks ignore it until it is filled: by the caller that creates the
// constraint (cypher's addSchemaConstraint), by the startup rebuild with the
// other indexes, or by an import's rebuild. A store from before this index
// existed may already have an index of its own on the property, which then
// serves the seeks.
func (sm *SchemaManager) addConstraintPropertyIndexLocked(c Constraint) {
	if constraintPropertyIndexKey(c) == "" {
		return
	}
	if _, exists := sm.arity1PropertyIndexLocked(c.Label, c.Properties[0]); exists {
		return
	}
	name := c.OwnedIndex
	if name == "" {
		name = c.Name
	}
	sm.compositeIndexes[name] = &CompositeIndex{
		Name:             name,
		Label:            c.Label,
		Properties:       []string{c.Properties[0]},
		OwningConstraint: c.Name,
		values:           make(map[interface{}][]NodeID),
		keysDirty:        true,
	}
	sm.compositeIndexes[name].unfilled.Store(true)
}

// seekablePropertyIndexLocked returns the arity-1 index on label's property
// that seeks may use: one that exists and is filled (#875). The caller holds
// sm.mu.
func (sm *SchemaManager) seekablePropertyIndexLocked(label, property string) (*PropertyIndex, bool) {
	idx, exists := sm.arity1PropertyIndexLocked(label, property)
	if !exists || idx == nil || idx.unfilled.Load() {
		return nil, false
	}
	return idx, true
}

// MaintainsPropertyIndex reports whether writes must maintain an arity-1
// index on label's property, filled or not (#875): node writes and index
// rebuilds use it, seeks use GetPropertyIndex.
func (sm *SchemaManager) MaintainsPropertyIndex(label, property string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	_, exists := sm.arity1PropertyIndexLocked(label, property)
	return exists
}

// MarkPropertyIndexesFilled lets seeks use every equality index, once a
// rebuild has filled them all from the stored nodes (#875).
func (sm *SchemaManager) MarkPropertyIndexesFilled() {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for _, idx := range sm.compositeIndexes {
		idx.unfilled.Store(false)
	}
}

// ConstraintPropertyIndex returns the equality index constraint name owns
// (#875), if it has one.
func (sm *SchemaManager) ConstraintPropertyIndex(name string) (*PropertyIndex, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if _, exists := sm.constraints[name]; !exists {
		return nil, false
	}
	for _, idx := range sm.compositeIndexes {
		if idx.OwningConstraint == name {
			return idx, true
		}
	}
	return nil, false
}

// SetPersister sets an optional persistence hook for schema changes.
// When set, schema mutations will attempt to persist the updated schema definition
// and will roll back the in-memory change if persistence fails.
func (sm *SchemaManager) SetPersister(persist func(def *SchemaDefinition) error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.persist = persist
}

// SetKnowledgePolicyChangedHook registers a callback that runs after a
// knowledge-policy mutation has been persisted and the schema lock released.
func (sm *SchemaManager) SetKnowledgePolicyChangedHook(hook func()) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.knowledgePolicyChanged = hook
}

// UniqueConstraint represents a unique constraint on a label and property.
type UniqueConstraint struct {
	Name     string
	Label    string
	Property string
	values   map[interface{}]NodeID // Track unique values
	// valuesCacheComplete is true once the values cache has been rebuilt from storage.
	valuesCacheComplete bool
	mu                  sync.RWMutex
}

// PropertyIndex is the arity-1 form of a CompositeIndex. It is a type alias
// so every existing single-property index call site keeps working while the
// underlying value is the same composite index type used for multi-property
// indexes. The single-property ordered-scan fields (values, sortedNonNilKeys,
// sortedKeyKinds, keysDirty) and OwningConstraint live on CompositeIndex.
type PropertyIndex = CompositeIndex

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
	// Create deterministic string representation
	var parts []string
	for _, v := range values {
		parts = append(parts, fmt.Sprintf("%T:%v", v, v))
	}
	encoded := strings.Join(parts, "|")

	return CompositeKey{
		Hash:   security.KeyedDigestHex("storage.composite_key", encoded),
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

	// OwningConstraint names the uniqueness or node key constraint whose
	// index this is (#875): its equality and IN seeks use it like any
	// property index. It is listed, persisted and dropped as the
	// constraint's RANGE index, not as an index of its own. Empty for an
	// index created on its own.
	OwningConstraint string

	// unfilled is true while the index doesn't hold the stored nodes yet
	// (#875). Writes and rebuilds maintain it, but seeks don't use it until a
	// fill (BackfillPropertyIndex / BackfillCompositeIndex, the startup
	// rebuild or MarkPropertyIndexesFilled) clears it; until then they scan,
	// as they did before the index existed.
	unfilled atomic.Bool

	// Single-property ordered-scan state, used by the arity-1 indexes that
	// answer ORDER BY, range and not-null seeks. values maps a canonical
	// property value to node IDs; the sorted views are rebuilt lazily.
	values           map[interface{}][]NodeID
	sortedNonNilKeys []interface{}
	sortedKeyKinds   propertyIndexKeyKinds
	keysDirty        bool

	// Primary index: full composite key -> node IDs.
	fullIndex map[string][]NodeID

	// Prefix indexes for partial key lookups
	// Key format: "prop1Value|prop2Value|..." -> node IDs
	prefixIndex map[string][]NodeID

	mu sync.RWMutex
}

// FulltextIndex represents a full-text search index.
//
// An index is scoped by EITHER Labels (a node-scoped index, declared
// via CREATE FULLTEXT INDEX <name> FOR (n:Label) ON EACH [n.prop])
// OR RelationshipTypes (a relationship-scoped index, declared via
// CREATE FULLTEXT INDEX <name> FOR ()-[r:Type]-() ON EACH [r.prop]).
// Exactly one of those slices is populated for any well-formed index;
// the runtime uses the populated slice to decide which storage scan
// to drive.
//
// Both Labels and RelationshipTypes use omitempty so an index that
// only carries one kind serializes without a stray empty array for
// the other. RelationshipTypes was added after the initial release;
// older databases serialize without it and load cleanly because
// JSON unmarshal treats missing fields as the zero value.
type FulltextIndex struct {
	Name              string   `json:"name"`
	Labels            []string `json:"labels,omitempty"`
	RelationshipTypes []string `json:"relationship_types,omitempty"`
	Properties        []string `json:"properties"`
}

// VectorIndex represents a vector similarity index.
type VectorIndex struct {
	Name           string
	Label          string
	Property       string
	Dimensions     int
	SimilarityFunc string // "cosine", "euclidean", "dot"
	EntityType     ConstraintEntityType
}

// RangeIndex represents an index for range queries on a single property.
// It maintains a sorted list of entries for efficient O(log n) range queries.
type RangeIndex struct {
	Name             string
	Kind             IndexKind
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
	defer sm.trackPendingPairs()
	sm.mu.Lock()
	defer sm.mu.Unlock()

	silent := len(ifNotExists) > 0 && ifNotExists[0]
	constraint := Constraint{
		Name:       name,
		Label:      label,
		Properties: []string{property},
		Type:       ConstraintUnique,
	}
	return sm.addSchemaRuleLocked(
		func() (bool, error) { return sm.admitConstraintLocked(constraint, silent) },
		func() { sm.applyConstraintLocked(constraint) },
	)
}

// addSchemaRuleLocked adds a constraint: admit checks it against the
// schema without changing it, and only when admit accepts it does the
// adder take the rollback snapshot, apply the change and persist the
// schema; if persisting fails, the in-memory schema returns to the
// snapshot. An IF NOT EXISTS that finds the constraint already present
// costs the check alone, as an existing index does for the index adders
// (#823: every repeated CREATE CONSTRAINT ... IF NOT EXISTS exported and
// rewrote the whole schema).
func (sm *SchemaManager) addSchemaRuleLocked(admit func() (bool, error), apply func()) error {
	add, err := admit()
	if err != nil || !add {
		return err
	}
	if sm.persist == nil {
		apply()
		return nil
	}
	snapshot := sm.exportDefinitionLocked()
	apply()
	if err := sm.persist(sm.exportDefinitionLocked()); err != nil {
		sm.replaceFromDefinitionLocked(snapshot)
		return err
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
	ptc := PropertyTypeConstraint{
		Name:         name,
		EntityType:   entityType,
		Label:        label,
		Property:     property,
		ExpectedType: expectedType,
	}
	return sm.addSchemaRuleLocked(
		func() (bool, error) { return sm.admitPropertyTypeConstraintLocked(ptc, ifNotExists) },
		func() { sm.propertyTypeConstraints[ptc.Name] = ptc },
	)
}

// addPropertyTypeConstraintValueLocked admits ptc and adds it when admitted.
func (sm *SchemaManager) addPropertyTypeConstraintValueLocked(ptc PropertyTypeConstraint, ifNotExists bool) (added bool, err error) {
	added, err = sm.admitPropertyTypeConstraintLocked(ptc, ifNotExists)
	if added {
		sm.propertyTypeConstraints[ptc.Name] = ptc
	}
	return added, err
}

// admitPropertyTypeConstraintLocked checks ptc against the schema without
// changing it (see admitConstraintLocked).
func (sm *SchemaManager) admitPropertyTypeConstraintLocked(ptc PropertyTypeConstraint, ifNotExists bool) (bool, error) {
	if _, exists := sm.propertyTypeConstraints[ptc.Name]; exists {
		if ifNotExists {
			return false, nil
		}
		return false, localizedError(localization.StorageSchemaConstraintAlreadyExists(ptc.Name), nil)
	}
	if _, exists := sm.constraintContracts[ptc.Name]; exists {
		return false, localizedError(localization.StorageSchemaConstraintAlreadyExists(ptc.Name), nil)
	}
	return true, nil
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

	valueKey, ok := indexValueKey(value)
	if !ok {
		return nil
	}

	if existingNode, found := constraint.values[valueKey]; found {
		if existingNode != excludeNode {
			return localizedError(localization.StorageSchemaUniqueConstraintViolation(label, property, value), nil)
		}
	}

	return nil
}

// LookupUniqueConstraintValue returns the node currently registered for a
// single-property uniqueness constraint value. The second return value reports
// whether the value is present, and the third reports whether the unique
// constraint exists.
func (sm *SchemaManager) LookupUniqueConstraintValue(label, property string, value interface{}) (NodeID, bool, bool) {
	nodeID, found, exists, _ := sm.LookupUniqueConstraintValueForPlanning(label, property, value)
	return nodeID, found, exists
}

// LookupUniqueConstraintValueForPlanning returns the node currently registered
// for a single-property uniqueness constraint value, plus whether the values
// cache has been rebuilt from storage and can be trusted for misses. Planners
// may trust absence only when cacheComplete is true; otherwise they must retain
// a scan fallback because the cache may not have been rebuilt from storage yet.
func (sm *SchemaManager) LookupUniqueConstraintValueForPlanning(label, property string, value interface{}) (nodeID NodeID, valueFound bool, constraintExists bool, cacheComplete bool) {
	sm.mu.RLock()
	key := fmt.Sprintf("%s:%s", label, property)
	constraint, exists := sm.uniqueConstraints[key]
	sm.mu.RUnlock()
	if !exists || value == nil {
		return "", false, exists, false
	}
	valueKey, ok := indexValueKey(value)
	if !ok {
		return "", false, true, false
	}
	nodeID, valueFound, cacheComplete = sm.lookupUniqueValue(constraint, label, property, valueKey)
	return nodeID, valueFound, true, cacheComplete
}

func (sm *SchemaManager) lookupUniqueConstraintValueForValidation(label, property string, value interface{}) (nodeID NodeID, valueFound bool, cacheComplete bool, constraintExists bool) {
	sm.mu.RLock()
	key := fmt.Sprintf("%s:%s", label, property)
	constraint, exists := sm.uniqueConstraints[key]
	sm.mu.RUnlock()
	if !exists || value == nil {
		return "", false, false, exists
	}
	valueKey, ok := indexValueKey(value)
	if !ok {
		return "", false, false, true
	}

	nodeID, valueFound, cacheComplete = sm.lookupUniqueValue(constraint, label, property, valueKey)
	return nodeID, valueFound, cacheComplete, true
}

// uniqueValueHolders returns the nodes holding value under label's unique
// constraint on property, merged with the pending writes: the registered
// holder unless a pending write supersedes it, and every pending node with
// the value. cacheComplete reports whether the registered values cover all
// stored nodes, so an empty result can be trusted.
func (sm *SchemaManager) uniqueValueHolders(label, property string, value interface{}) (holders []NodeID, cacheComplete bool, constraintExists bool) {
	sm.mu.RLock()
	constraint, exists := sm.uniqueConstraints[fmt.Sprintf("%s:%s", label, property)]
	sm.mu.RUnlock()
	if !exists {
		return nil, false, false
	}
	valueKey, ok := indexValueKey(value)
	if !ok {
		return nil, false, true
	}
	view, source := sm.beginPendingRead()
	defer endPendingRead(source)
	constraint.mu.RLock()
	holder, found := constraint.values[valueKey]
	cacheComplete = constraint.valuesCacheComplete
	constraint.mu.RUnlock()
	if found && view.keep(holder) {
		holders = append(holders, holder)
	}
	holders = view.valueMatches(holders, label, property, valueKey)
	return holders, cacheComplete, true
}

// lookupUniqueValue returns the node holding valueKey under constraint,
// merged with the pending writes: a registered holder with a pending update
// or delete no longer counts, and a pending node with the value does.
func (sm *SchemaManager) lookupUniqueValue(constraint *UniqueConstraint, label, property string, valueKey interface{}) (nodeID NodeID, found bool, cacheComplete bool) {
	view, source := sm.beginPendingRead()
	defer endPendingRead(source)
	constraint.mu.RLock()
	nodeID, found = constraint.values[valueKey]
	cacheComplete = constraint.valuesCacheComplete
	constraint.mu.RUnlock()
	if found && !view.keep(nodeID) {
		nodeID, found = "", false
	}
	if pending := view.valueMatches(nil, label, property, valueKey); len(pending) > 0 {
		sort.Slice(pending, func(i, j int) bool { return string(pending[i]) < string(pending[j]) })
		nodeID, found = pending[0], true
	}
	return nodeID, found, cacheComplete
}

// indexValueKey is the key a property index, a unique constraint's values
// and an AsyncEngine's pending view (pendingNodeIndex) file a value under:
// numbers in one numeric form, so 1 and 1.0 match, lists and maps under their
// compositeIndexKey, and other comparable values as they are. It reports false
// for null and for values it can't key (byte arrays, lists holding values
// without a key), which no index holds. An index lookup is complete only
// because every keyed value is filed: a list that was not filed made an
// indexed equality on it return no rows (#844).
func indexValueKey(value interface{}) (interface{}, bool) {
	if numeric, ok := numericConstraintValue(value); ok {
		return numeric, true
	}
	if value == nil {
		return nil, false
	}
	if reflect.TypeOf(value).Comparable() {
		return value, true
	}
	if key, ok := compositeIndexKeyOf(value); ok {
		return key, true
	}
	return nil, false
}

// compositeIndexKey is the index key of a list or map value: a canonical text
// of its elements, numbers in their numeric form, so [1, 2] and [1.0, 2.0]
// share a key, and map entries in key order. Its own type keeps it apart from
// string keys. Ordered index scans skip it (sortedKeysViewLocked).
type compositeIndexKey string

func compositeIndexKeyOf(value interface{}) (compositeIndexKey, bool) {
	var b strings.Builder
	if !writeCompositeIndexKey(&b, value) {
		return "", false
	}
	return compositeIndexKey(b.String()), true
}

func writeCompositeIndexKey(b *strings.Builder, value interface{}) bool {
	if numeric, ok := numericConstraintValue(value); ok {
		b.WriteByte('n')
		b.WriteString(strconv.FormatFloat(numeric, 'g', -1, 64))
		return true
	}
	switch v := value.(type) {
	case nil:
		b.WriteByte('z')
		return true
	case string:
		b.WriteByte('s')
		b.WriteString(strconv.Quote(v))
		return true
	case bool:
		if v {
			b.WriteString("bt")
		} else {
			b.WriteString("bf")
		}
		return true
	case map[string]interface{}:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		b.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(strconv.Quote(key))
			b.WriteByte(':')
			if !writeCompositeIndexKey(b, v[key]) {
				return false
			}
		}
		b.WriteByte('}')
		return true
	}
	list := reflect.ValueOf(value)
	if (list.Kind() != reflect.Slice && list.Kind() != reflect.Array) || list.Type().Elem().Kind() == reflect.Uint8 {
		return false
	}
	b.WriteByte('[')
	for i := 0; i < list.Len(); i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		if !writeCompositeIndexKey(b, list.Index(i).Interface()) {
			return false
		}
	}
	b.WriteByte(']')
	return true
}

// uniqueConstraintLockKey identifies one (label, property, value) triple for
// the purpose of acquiring a commit-time mutex. Two transactions whose
// pending nodes touch the same (label, property, value) serialize at commit;
// transactions touching disjoint values commit in parallel. The granularity
// is per constrained value, not per constraint.
//
// The value is stored in its canonical comparable form returned by
// indexValueKey so semantically equal but type-distinct values
// (e.g. int and int64) use the same lock and serialize correctly. Values
// that are not comparable cannot acquire a lock; their constraint is still
// validated at commit but without commit-window serialization. (In practice
// every UNIQUE-constrained property in Eshu and Neo4j-compatible workloads
// uses comparable scalar types — strings, ints, floats, bools.)
//
// Lock granularity history: an earlier per-(label, property) design
// effectively serialized every writer touching any value of a constrained
// property. Under bootstrap-index Pass 2 fan-out (8 projector workers + the
// collector + the ingester all writing TerraformResource nodes with
// disjoint uids), this collapsed throughput to single-writer levels — a
// "serialization workaround" in disguise. Per-value locking lets disjoint
// writers commit concurrently while still preventing the silent-overwrite
// race that motivated the lock.
type uniqueConstraintLockKey struct {
	label    string
	property string
	value    interface{}
}

// uniqueConstraintCommitLock is one constraint key's lock. It belongs to one
// owner at a time (a transaction ID, or a direct engine write's own ID); the
// owner may take it again (holds counts its acquisitions), and released is
// closed whenever it becomes free, waking its waiters (#961).
type uniqueConstraintCommitLock struct {
	owner    string
	holds    int
	refs     int
	order    uint64
	released chan struct{}
}

type uniqueConstraintLockRequest struct {
	key  uniqueConstraintLockKey
	lock *uniqueConstraintCommitLock
}

// lockConstraintKeysOf acquires the commit locks of the constraint keys the
// nodes' values fall under, and returns their release function: the exact
// value of each single-property UNIQUE constraint, the composite value of each
// NODE KEY, and the key value of each TEMPORAL (no-overlap) constraint. Every
// writer of a constrained node holds them across its constraint check, its
// write and the publication of the value to the constraint cache: a
// transaction at commit, and the engine's own CreateNode / UpdateNode /
// BulkCreateNodes. So two writers of the same key (two transactions, or a
// transaction and a direct engine write such as the AsyncEngine's
// write-through of a constrained node) serialize, and the second one's check
// sees the first one's write (#700).
//
// A key that can't be a map key (a list or map value) takes no lock; the
// constraint check still runs, and serialization is best-effort for such
// values, which constrained workloads don't use in practice. A NODE KEY with a
// missing property takes no lock: the check rejects the node anyway.
//
// owner holds the locks (acquireUniqueConstraintCommitLocks); keys it already
// holds, such as a MERGE key a transaction locked, are taken again without
// waiting.
func (sm *SchemaManager) lockConstraintKeysOf(ctx context.Context, owner string, nodes ...*Node) (func(), error) {
	if sm == nil || len(nodes) == 0 {
		return func() {}, nil
	}
	// Allocated on the first key: an unconstrained write allocates nothing.
	var keys []uniqueConstraintLockKey
	for _, node := range nodes {
		if node == nil || len(node.Properties) == 0 {
			continue
		}
		for _, c := range sm.GetConstraintsForLabels(node.Labels) {
			if c.EffectiveEntityType() != ConstraintEntityNode {
				continue
			}
			var properties []string
			switch {
			case c.Type == ConstraintUnique && len(c.Properties) > 0:
				properties = c.Properties
			case c.Type == ConstraintNodeKey:
				properties = c.Properties
			case c.Type == ConstraintTemporal && len(c.Properties) >= 3:
				// The grouping key: every property before (valid_from, valid_to).
				properties = c.Properties[:len(c.Properties)-2]
			default:
				continue
			}
			if len(properties) == 1 {
				// One key property (UNIQUE, a one-property NODE KEY, the
				// temporal key): no intermediate slice.
				value, ok := constraintLockValue(node, properties[0])
				if ok {
					if keys == nil {
						keys = make([]uniqueConstraintLockKey, 0, len(nodes))
					}
					keys = append(keys, uniqueConstraintLockKey{label: c.Label, property: properties[0], value: value})
				}
				continue
			}
			values := make([]interface{}, len(properties))
			complete := true
			for i, prop := range properties {
				value, ok := constraintLockValue(node, prop)
				if !ok {
					complete = false
					break
				}
				values[i] = value
			}
			if complete {
				keys = append(keys, uniqueConstraintLockKey{
					label:    c.Label,
					property: string(c.Type) + ":" + strings.Join(properties, ","),
					value:    constraintCompositeKey(values),
				})
			}
		}
	}
	return sm.acquireUniqueConstraintCommitLocks(ctx, owner, keys)
}

// constraintLockValue is node's canonical value of prop for a constraint key
// lock; ok is false when the property is missing, null or not comparable (no
// key to lock).
func constraintLockValue(node *Node, prop string) (interface{}, bool) {
	rawValue, has := node.Properties[prop]
	if !has || rawValue == nil {
		return nil, false
	}
	return indexValueKey(rawValue)
}

// acquireUniqueConstraintCommitLocks acquires exact UNIQUE value locks for
// owner in a deterministic order and returns a release function.
// Deterministic ordering eliminates the AB-BA deadlock risk when two
// batches both touch overlapping sets of constrained values. A lock owner
// already holds (a MERGE key its transaction locked, #961) is taken again
// without waiting, and released with this release.
//
// Duplicate keys in the input are deduplicated. An empty input returns a
// no-op release function so callers can safely defer the result regardless
// of whether locks were acquired.
//
// Each lock guards the entire commit window for its specific value —
// validateAllConstraints, badgerTx.Commit, and the RegisterUniqueValue call
// that publishes the committed value to the constraint cache — so a
// subsequent transaction touching the same value always observes a coherent
// cache. Transactions touching disjoint values always acquire disjoint locks.
// Registry entries count both holders and waiters and are evicted when that
// count reaches zero, bounding memory by active commit demand rather than the
// historical graph cardinality.
//
// A wait that would close a cycle of owners waiting for each other fails with
// ErrDeadlock, and a wait ends with ctx's error when ctx ends. The locks
// taken so far are released either way.
func (sm *SchemaManager) acquireUniqueConstraintCommitLocks(ctx context.Context, owner string, keys []uniqueConstraintLockKey) (func(), error) {
	if len(keys) == 0 {
		return func() {}, nil
	}
	requests := make([]uniqueConstraintLockRequest, 0, len(keys))
	seen := make(map[uniqueConstraintLockKey]struct{}, len(keys))
	for _, k := range keys {
		valueType := reflect.TypeOf(k.value)
		if valueType != nil && !valueType.Comparable() {
			continue
		}
		if k.value != nil && k.value != k.value {
			// Non-reflexive comparable values such as NaN never conflict
			// under equality and cannot be safely used as registry map keys:
			// a lookup or delete with the same value would never find them.
			continue
		}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		requests = append(requests, uniqueConstraintLockRequest{key: k})
	}
	if len(requests) == 0 {
		return func() {}, nil
	}

	sm.uniqueConstraintCommitLocksMu.Lock()
	if sm.uniqueConstraintCommitLocks == nil {
		sm.uniqueConstraintCommitLocks = make(map[uniqueConstraintLockKey]*uniqueConstraintCommitLock, len(requests))
	}
	if len(sm.uniqueConstraintCommitLocks) == 0 {
		sm.uniqueConstraintCommitOrder = 0
	}
	for i := range requests {
		lock := sm.uniqueConstraintCommitLocks[requests[i].key]
		if lock == nil {
			sm.uniqueConstraintCommitOrder++
			if sm.uniqueConstraintCommitOrder == 0 {
				panic("UNIQUE commit lock order overflow")
			}
			lock = &uniqueConstraintCommitLock{order: sm.uniqueConstraintCommitOrder, released: make(chan struct{})}
			sm.uniqueConstraintCommitLocks[requests[i].key] = lock
		}
		lock.refs++
		requests[i].lock = lock
	}
	sm.uniqueConstraintCommitLocksMu.Unlock()
	sort.Slice(requests, func(i, j int) bool {
		return requests[i].lock.order < requests[j].lock.order
	})

	for i := range requests {
		if err := sm.waitForUniqueConstraintLock(ctx, owner, requests[i]); err != nil {
			sm.releaseUniqueConstraintLocks(owner, requests[:i], requests[i:])
			return nil, err
		}
	}
	return func() {
		sm.releaseUniqueConstraintLocks(owner, requests, nil)
	}, nil
}

// waitForUniqueConstraintLock takes request's lock for owner, waiting while
// another owner holds it.
func (sm *SchemaManager) waitForUniqueConstraintLock(ctx context.Context, owner string, request uniqueConstraintLockRequest) error {
	lock := request.lock
	sm.uniqueConstraintCommitLocksMu.Lock()
	for {
		if lock.owner == "" || lock.owner == owner {
			lock.owner = owner
			lock.holds++
			delete(sm.uniqueConstraintLockWaits, owner)
			sm.uniqueConstraintCommitLocksMu.Unlock()
			return nil
		}
		if sm.uniqueConstraintLockWaitClosesCycleLocked(owner, lock) {
			delete(sm.uniqueConstraintLockWaits, owner)
			sm.uniqueConstraintCommitLocksMu.Unlock()
			return localizedError(localization.StorageTransactionDeadlockDetected(request.key.label, request.key.property), ErrDeadlock)
		}
		if sm.uniqueConstraintLockWaits == nil {
			sm.uniqueConstraintLockWaits = make(map[string]*uniqueConstraintCommitLock)
		}
		sm.uniqueConstraintLockWaits[owner] = lock
		released := lock.released
		sm.uniqueConstraintCommitLocksMu.Unlock()
		select {
		case <-released:
		case <-ctx.Done():
			sm.uniqueConstraintCommitLocksMu.Lock()
			delete(sm.uniqueConstraintLockWaits, owner)
			sm.uniqueConstraintCommitLocksMu.Unlock()
			return ctx.Err()
		}
		sm.uniqueConstraintCommitLocksMu.Lock()
	}
}

// uniqueConstraintLockWaitClosesCycleLocked reports whether owner waiting for
// lock would close a cycle: lock's owner waits, directly or through other
// owners, for a lock owner holds.
func (sm *SchemaManager) uniqueConstraintLockWaitClosesCycleLocked(owner string, lock *uniqueConstraintCommitLock) bool {
	holder := lock.owner
	for steps := 0; holder != "" && steps <= len(sm.uniqueConstraintLockWaits); steps++ {
		if holder == owner {
			return true
		}
		next, waiting := sm.uniqueConstraintLockWaits[holder]
		if !waiting {
			return false
		}
		holder = next.owner
	}
	return false
}

// releaseUniqueConstraintLocks releases owner's hold on each of held and drops
// the registry references of held and unacquired.
func (sm *SchemaManager) releaseUniqueConstraintLocks(owner string, held, unacquired []uniqueConstraintLockRequest) {
	sm.uniqueConstraintCommitLocksMu.Lock()
	defer sm.uniqueConstraintCommitLocksMu.Unlock()
	for i := len(held) - 1; i >= 0; i-- {
		lock := held[i].lock
		if lock.owner != owner || lock.holds == 0 {
			continue
		}
		lock.holds--
		if lock.holds == 0 {
			lock.owner = ""
			close(lock.released)
			lock.released = make(chan struct{})
		}
	}
	for _, requests := range [2][]uniqueConstraintLockRequest{held, unacquired} {
		for _, request := range requests {
			request.lock.refs--
			if request.lock.refs == 0 && sm.uniqueConstraintCommitLocks[request.key] == request.lock {
				delete(sm.uniqueConstraintCommitLocks, request.key)
			}
		}
	}
}

// engineWriteLockOwners numbers the lock owners of direct engine writes,
// which aren't transactions.
var engineWriteLockOwners atomic.Uint64

// newEngineWriteLockOwner returns a lock owner for one direct engine write.
func newEngineWriteLockOwner() string {
	return "engine-write-" + strconv.FormatUint(engineWriteLockOwners.Add(1), 10)
}

// uniqueMergeKey returns the lock key of value under label's single-property
// UNIQUE constraint (or one-property NODE KEY) on property, as
// lockConstraintKeysOf builds it; ok is false when there's no such
// constraint or value is null.
func (sm *SchemaManager) uniqueMergeKey(label, property string, value interface{}) (uniqueConstraintLockKey, bool) {
	if sm == nil || value == nil {
		return uniqueConstraintLockKey{}, false
	}
	for _, c := range sm.GetConstraintsForLabels([]string{label}) {
		if c.EffectiveEntityType() != ConstraintEntityNode || c.Label != label || len(c.Properties) != 1 || c.Properties[0] != property {
			continue
		}
		if c.Type != ConstraintUnique && c.Type != ConstraintNodeKey {
			continue
		}
		key, ok := indexValueKey(value)
		if !ok {
			return uniqueConstraintLockKey{}, false
		}
		return uniqueConstraintLockKey{label: label, property: property, value: key}, true
	}
	return uniqueConstraintLockKey{}, false
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
	valueKey, ok := indexValueKey(value)
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
	valueKey, ok := indexValueKey(value)
	if !ok {
		return
	}

	constraint.mu.Lock()
	delete(constraint.values, valueKey)
	constraint.mu.Unlock()
}

// AddPropertyIndex adds an arity-1 equality index. It is the single-property
// spelling of AddCompositeIndex: the index lives in compositeIndexes and the
// arity-1 encoding (values) drives equality, ORDER BY, range and not-null
// seeks.
func (sm *SchemaManager) AddPropertyIndex(name, label string, properties []string) error {
	if len(properties) == 0 {
		return localizedError(localization.StorageSchemaRangeIndexPropertiesRequired(), nil)
	}
	return sm.AddCompositeIndex(name, label, properties)
}

// AddCompositeIndex creates a composite (equality) index on one or more
// properties. An arity-1 index is the single-property index; arity >= 2 adds
// prefix lookups.
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
	if len(properties) == 0 {
		return localizedError(localization.StorageSchemaCompositeIndexMinProperties(len(properties)), nil)
	}

	// A new arity-1 index changes which (label, property) pairs an attached
	// AsyncEngine indexes by value (#719); refresh its pending-pair tracking
	// once the lock is released.
	defer sm.trackPendingPairs()

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.compositeIndexes[name]; exists {
		return nil // Already exists (idempotent)
	}

	sm.compositeIndexes[name] = &CompositeIndex{
		Name:        name,
		Label:       label,
		Properties:  properties,
		values:      make(map[interface{}][]NodeID),
		keysDirty:   true,
		fullIndex:   make(map[string][]NodeID),
		prefixIndex: make(map[string][]NodeID),
	}
	// Like the historical single-property index, a standalone equality index
	// is seekable as soon as it is declared: the DDL path (cypher's
	// addPropertyIndex) backfills it from the stored nodes before the
	// statement returns. A constraint-owned index starts unfilled instead
	// (#875), because its fill is deferred to a rebuild.

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

// SeekableCompositeIndexesForLabel returns label's composite indexes that
// seek paths may use: filled only. Writes and rebuilds maintain every index
// through GetCompositeIndexesForLabel, so a DDL-created index mid-backfill is
// populated but never trusted for reads (#875).
func (sm *SchemaManager) SeekableCompositeIndexesForLabel(label string) []*CompositeIndex {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var indexes []*CompositeIndex
	for _, idx := range sm.compositeIndexes {
		if idx != nil && idx.Label == label && !idx.unfilled.Load() {
			indexes = append(indexes, idx)
		}
	}
	return indexes
}

// BackfillCompositeIndex fills a newly created composite index from the
// already-stored nodes, mirroring BackfillPropertyIndex. Nodes with pending
// writes in an attached AsyncEngine are skipped: the engine indexes them when
// they are flushed, and lookups see them through the pending view until then
// (#719). On success the index is marked filled so seeks can use it.
func (sm *SchemaManager) BackfillCompositeIndex(name string, entries map[NodeID]map[string]interface{}) error {
	view, source := sm.beginPendingRead()
	defer endPendingRead(source)

	sm.mu.RLock()
	idx := sm.compositeIndexes[name]
	sm.mu.RUnlock()
	if idx == nil {
		return localizedError(localization.StorageSchemaIndexNotFound(name), nil)
	}
	for nodeID, props := range entries {
		if !view.keep(nodeID) {
			continue
		}
		if err := idx.IndexNode(nodeID, props); err != nil {
			return err
		}
	}
	idx.unfilled.Store(false)
	return nil
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

// IndexNodeComposite indexes a node in a composite index. An arity-1 index
// files the node under its canonical single-property key (values, the
// encoding ORDER BY / range / equality seeks read); an arity >= 2 index files
// it under its full and every leading-prefix composite key.
// Call this when creating or updating a node with the indexed properties.
func (idx *CompositeIndex) IndexNode(nodeID NodeID, properties map[string]interface{}) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(idx.Properties) == 1 {
		value, exists := properties[idx.Properties[0]]
		if !exists {
			return nil
		}
		valueKey, ok := indexValueKey(value)
		if !ok {
			return nil // null / byte array / non-comparable: no key to file
		}
		if idx.values == nil {
			idx.values = make(map[interface{}][]NodeID)
		}
		if _, exists := idx.values[valueKey]; !exists {
			idx.keysDirty = true
		}
		idx.values[valueKey] = appendUnique(idx.values[valueKey], nodeID)
		return nil
	}

	values := compositeIndexValues(idx, properties)

	// Index full key if all properties present and keyable.
	if len(values) == len(idx.Properties) {
		key := NewCompositeKey(values...)
		idx.fullIndex[key.Hash] = appendUnique(idx.fullIndex[key.Hash], nodeID)
	}

	// Index all prefixes for partial lookups.
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

	if len(idx.Properties) == 1 {
		value, exists := properties[idx.Properties[0]]
		if !exists {
			return
		}
		valueKey, ok := indexValueKey(value)
		if !ok {
			return
		}
		ids := removeNodeID(idx.values[valueKey], nodeID)
		if len(ids) > 0 {
			idx.values[valueKey] = ids
		} else {
			delete(idx.values, valueKey)
			idx.keysDirty = true
		}
		return
	}

	values := compositeIndexValues(idx, properties)

	// Remove from full index.
	if len(values) == len(idx.Properties) {
		key := NewCompositeKey(values...)
		idx.fullIndex[key.Hash] = removeNodeID(idx.fullIndex[key.Hash], nodeID)
		if len(idx.fullIndex[key.Hash]) == 0 {
			delete(idx.fullIndex, key.Hash)
		}
	}

	// Remove from all prefix indexes.
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
	if len(idx.Properties) == 1 {
		valueKey, ok := indexValueKey(values[0])
		if !ok {
			return nil
		}
		idx.mu.RLock()
		defer idx.mu.RUnlock()
		nodes := idx.values[valueKey]
		result := make([]NodeID, len(nodes))
		copy(result, nodes)
		return result
	}
	canonical, ok := compositeLookupKeyValues(values)
	if !ok {
		return nil // a value no index entry can hold: no match
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	key := NewCompositeKey(canonical...)
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
// Returns all nodes that match the prefix. For an arity-1 index a one-value
// lookup is a full match.
//
// Example: For index on (country, city, zipcode)
//   - LookupPrefix("US") returns all nodes in the US
//   - LookupPrefix("US", "NYC") returns all nodes in NYC, US
func (idx *CompositeIndex) LookupPrefix(values ...interface{}) []NodeID {
	if len(values) == 0 || len(values) > len(idx.Properties) {
		return nil
	}
	if len(idx.Properties) == 1 {
		return idx.LookupFull(values...)
	}
	canonical, ok := compositeLookupKeyValues(values)
	if !ok {
		return nil // a value no index entry can hold: no match
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Check if this is a full match (not a prefix)
	if len(canonical) == len(idx.Properties) {
		key := NewCompositeKey(canonical...)
		if nodes, exists := idx.fullIndex[key.Hash]; exists {
			result := make([]NodeID, len(nodes))
			copy(result, nodes)
			return result
		}
		return nil
	}

	// Prefix lookup
	key := NewCompositeKey(canonical...)
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
		"valueEntries":     len(idx.values),
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

// AddFulltextIndex adds a node-scoped full-text index.
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

// AddFulltextRelationshipIndex adds a relationship-scoped full-text
// index. Mirrors AddFulltextIndex but populates RelationshipTypes
// instead of Labels. The two share the same `fulltextIndexes` map so
// every existing get/list/remove path works for both kinds; consumers
// that need to distinguish check which scope slice is non-empty.
func (sm *SchemaManager) AddFulltextRelationshipIndex(name string, relTypes, properties []string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.fulltextIndexes[name]; exists {
		return nil // Already exists
	}

	sm.fulltextIndexes[name] = &FulltextIndex{
		Name:              name,
		RelationshipTypes: relTypes,
		Properties:        properties,
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
	return sm.AddVectorIndexForEntity(name, label, property, dimensions, similarityFunc, ConstraintEntityNode)
}

// AddVectorIndexForEntity adds a vector index scoped to a node label or relationship type.
func (sm *SchemaManager) AddVectorIndexForEntity(name, label, property string, dimensions int, similarityFunc string, entityType ConstraintEntityType) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.vectorIndexes[name]; exists {
		return nil // Already exists
	}
	if entityType == "" {
		entityType = ConstraintEntityNode
	}

	sm.vectorIndexes[name] = &VectorIndex{
		Name:           name,
		Label:          label,
		Property:       property,
		Dimensions:     dimensions,
		SimilarityFunc: similarityFunc,
		EntityType:     entityType,
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
	return sm.AddRangeIndexForEntity(name, label, []string{property}, ConstraintEntityNode)
}

// AddRangeIndexForEntity adds a range index for NODE or RELATIONSHIP entities.
// For standalone CREATE INDEX forms, properties may contain one or more fields.
func (sm *SchemaManager) AddRangeIndexForEntity(name, label string, properties []string, entityType ConstraintEntityType) error {
	return sm.addIndexForEntity(IndexKindRange, name, label, properties, entityType)
}

func (sm *SchemaManager) addIndexForEntity(kind IndexKind, name, label string, properties []string, entityType ConstraintEntityType) error {
	if len(properties) == 0 {
		return localizedError(localization.StorageSchemaRangeIndexPropertiesRequired(), nil)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.rangeIndexes[name]; exists {
		return nil // Already exists
	}

	propCopy := append([]string(nil), properties...)
	sm.rangeIndexes[name] = &RangeIndex{
		Name:       name,
		Kind:       kind,
		Label:      label,
		Property:   properties[0],
		Properties: propCopy,
		EntityType: entityType,
		entries:    make([]rangeEntry, 0),
		nodeValue:  make(map[NodeID]float64), // NodeID -> value
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
		return localizedError(localization.StorageSchemaRangeIndexNotFound(name), nil)
	}

	// Convert value to float64 for comparison
	numVal, ok := convert.ToFloat64(value)
	if !ok {
		return localizedError(localization.StorageSchemaRangeIndexNumericValueRequired(value), nil)
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
		return localizedError(localization.StorageSchemaRangeIndexNotFound(name), nil)
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
		return nil, localizedError(localization.StorageSchemaRangeIndexNotFound(name), nil)
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

// GetConstraintsForLabels returns all constraints for given labels, ordered
// by constraint name so validation reports the same violation every time an
// entity breaks several constraints. Returns constraints from the constraints
// map, preserving their original types.
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
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })

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

// sameConstraintSchema reports whether a and b constrain the same schema:
// entity type, label (or relationship type) and set of properties, plus a
// relationship policy's endpoints and mode or a cardinality constraint's
// direction. It allocates nothing; admitting a constraint compares it with
// every existing one (#823).
func sameConstraintSchema(a, b Constraint) bool {
	if a.EffectiveEntityType() != b.EffectiveEntityType() || a.Label != b.Label || !samePropertySet(a.Properties, b.Properties) {
		return false
	}
	switch {
	case a.Type == ConstraintPolicy || b.Type == ConstraintPolicy:
		return a.Type == b.Type && a.SourceLabel == b.SourceLabel && a.TargetLabel == b.TargetLabel && a.PolicyMode == b.PolicyMode
	case a.Type == ConstraintCardinality || b.Type == ConstraintCardinality:
		return a.Type == b.Type && a.Direction == b.Direction
	}
	return true
}

// samePropertySet reports whether a and b hold the same property names, in
// any order.
func samePropertySet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, name := range a {
		if countName(a, name) != countName(b, name) {
			return false
		}
	}
	return true
}

func countName(names []string, name string) int {
	count := 0
	for _, candidate := range names {
		if candidate == name {
			count++
		}
	}
	return count
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
	defer sm.trackPendingPairs()
	sm.mu.Lock()
	defer sm.mu.Unlock()

	silentOnDuplicate := len(ifNotExists) > 0 && ifNotExists[0]
	return sm.addSchemaRuleLocked(
		func() (bool, error) { return sm.admitConstraintLocked(c, silentOnDuplicate) },
		func() { sm.applyConstraintLocked(c) },
	)
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
	if idx, ok := sm.compositeIndexes[name]; ok {
		if idx.OwningConstraint != "" {
			return &schemaAdmissionError{code: "Neo.DatabaseError.Schema.IndexDropFailed",
				cause: localizedError(localization.StorageSchemaIndexBelongsToConstraint(idx.OwningConstraint), nil)}
		}
		d = dropped{kind: "composite", key: name}
	} else if _, ok := sm.fulltextIndexes[name]; ok {
		d = dropped{kind: "fulltext", key: name}
	} else if _, ok := sm.vectorIndexes[name]; ok {
		d = dropped{kind: "vector", key: name}
	} else if ri, ok := sm.rangeIndexes[name]; ok {
		if ri.OwningConstraint != "" {
			return &schemaAdmissionError{code: "Neo.DatabaseError.Schema.IndexDropFailed",
				cause: localizedError(localization.StorageSchemaIndexBelongsToConstraint(ri.OwningConstraint), nil)}
		}
		d = dropped{kind: "range", key: name}
	} else if entityType, ok := sm.dropLookupIndexLocked(name); ok {
		if sm.persist != nil {
			if err := sm.persist(sm.exportDefinitionLocked()); err != nil {
				sm.lookupIndexes[entityType] = name
				return err
			}
		}
		return nil
	}

	if d.kind == "" {
		return localizedError(localization.StorageSchemaIndexNotFound(name), nil)
	}

	// Stash the old value for rollback, then delete.
	var oldComposite *CompositeIndex
	var oldFulltext *FulltextIndex
	var oldVector *VectorIndex
	var oldRange *RangeIndex

	switch d.kind {
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

// SchemaObjectCounts returns how many indexes and constraints the schema
// holds, as Neo4j's schema counters count them: the index a constraint owns
// belongs to the constraint and is not an index of its own. Constraints are
// the ones SHOW CONSTRAINTS lists (constraints, property type constraints and
// constraint contracts).
func (sm *SchemaManager) SchemaObjectCounts() (indexes, constraints int) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	indexes = len(sm.fulltextIndexes) + len(sm.vectorIndexes)
	for _, idx := range sm.compositeIndexes {
		if idx.OwningConstraint == "" {
			indexes++
		}
	}
	for _, idx := range sm.rangeIndexes {
		if idx.OwningConstraint == "" {
			indexes++
		}
	}
	constraints = len(sm.constraints) + len(sm.propertyTypeConstraints) + len(sm.constraintContracts)
	return indexes, constraints
}

// GetIndexes returns all indexes.
func (sm *SchemaManager) GetIndexes() []interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	indexes := make([]interface{}, 0)

	for _, idx := range sm.compositeIndexes {
		if idx.OwningConstraint != "" {
			continue // listed as its constraint's RANGE index
		}
		indexType := "COMPOSITE"
		if len(idx.Properties) == 1 {
			indexType = "PROPERTY"
		}
		indexes = append(indexes, map[string]interface{}{
			"name":       idx.Name,
			"type":       indexType,
			"label":      idx.Label,
			"properties": idx.Properties,
		})
	}

	for _, idx := range sm.fulltextIndexes {
		indexes = append(indexes, map[string]interface{}{
			"name":              idx.Name,
			"type":              "FULLTEXT",
			"labels":            idx.Labels,
			"relationshipTypes": idx.RelationshipTypes,
			"properties":        idx.Properties,
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
			"entityType":     string(defaultConstraintEntityType(idx.EntityType)),
		})
	}

	for entityType, name := range sm.lookupIndexes {
		indexes = append(indexes, map[string]interface{}{
			"name":       name,
			"type":       "LOOKUP",
			"entityType": string(entityType),
		})
	}

	for _, idx := range sm.rangeIndexes {
		m := map[string]interface{}{
			"name":  idx.Name,
			"type":  string(idx.effectiveKind()),
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

func defaultConstraintEntityType(entityType ConstraintEntityType) ConstraintEntityType {
	if entityType == "" {
		return ConstraintEntityNode
	}
	return entityType
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
	return sm.seekablePropertyIndexLocked(label, property)
}

// PropertyIndexInsert adds a node to an arity-1 index. It is the
// single-property spelling of IndexNode for callers that carry the value
// directly (imports, backfills); node-write maintenance calls IndexNode.
func (sm *SchemaManager) PropertyIndexInsert(label, property string, nodeID NodeID, value interface{}) error {
	sm.mu.RLock()
	idx, exists := sm.arity1PropertyIndexLocked(label, property)
	sm.mu.RUnlock()

	if !exists {
		return localizedError(localization.StorageSchemaPropertyIndexNotFound(label, property), nil)
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.values == nil {
		idx.values = make(map[interface{}][]NodeID)
	}

	valueKey, ok := indexValueKey(value)
	if !ok {
		// Ignore non-comparable values (maps/slices). These cannot be stored in
		// Go map keys and must not crash index maintenance paths.
		return nil
	}

	if _, exists := idx.values[valueKey]; !exists {
		idx.keysDirty = true
	}
	idx.values[valueKey] = appendUnique(idx.values[valueKey], nodeID)
	return nil
}

// BackfillPropertyIndex fills a newly created arity-1 index with values
// (node ID → property value). Nodes with pending writes in an attached
// AsyncEngine are skipped: the engine indexes them when they are flushed,
// and lookups see them through the pending view until then (#719).
func (sm *SchemaManager) BackfillPropertyIndex(label, property string, values map[NodeID]interface{}) error {
	sm.mu.RLock()
	idx, exists := sm.arity1PropertyIndexLocked(label, property)
	sm.mu.RUnlock()
	if !exists {
		return localizedError(localization.StorageSchemaPropertyIndexNotFound(label, property), nil)
	}

	view, source := sm.beginPendingRead()
	defer endPendingRead(source)
	for nodeID, value := range values {
		if !view.keep(nodeID) {
			continue
		}
		if err := sm.PropertyIndexInsert(label, property, nodeID, value); err != nil {
			return err
		}
	}
	idx.unfilled.Store(false)
	return nil
}

// PropertyIndexDelete removes a node from an arity-1 index.
func (sm *SchemaManager) PropertyIndexDelete(label, property string, nodeID NodeID, value interface{}) error {
	sm.mu.RLock()
	idx, exists := sm.arity1PropertyIndexLocked(label, property)
	sm.mu.RUnlock()

	if !exists {
		return nil // Not indexed
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	valueKey, ok := indexValueKey(value)
	if !ok {
		return nil
	}

	if ids, ok := idx.values[valueKey]; ok {
		newIDs := make([]NodeID, 0, len(ids)-1)
		for _, id := range ids {
			if id != nodeID {
				newIDs = append(newIDs, id)
			}
		}
		if len(newIDs) > 0 {
			idx.values[valueKey] = newIDs
		} else {
			delete(idx.values, valueKey)
			idx.keysDirty = true
		}
	}
	return nil
}

// HasPropertyIndex reports whether a filled arity-1 index exists for the
// given label+property combination. Callers can use this to choose between
// index-backed per-row lookups and batch preloads.
func (sm *SchemaManager) HasPropertyIndex(label, property string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	_, exists := sm.seekablePropertyIndexLocked(label, property)
	return exists
}

// HasAnyPropertyIndexForLabel reports whether ANY equality index (arity-1 or
// composite) is declared against the given label. Used by storage-side index
// maintenance to short-circuit per-node work when no index touches the label.
func (sm *SchemaManager) HasAnyPropertyIndexForLabel(label string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.hasAnyCompositeIndexForLabelLocked(label)
}

// hasAnyCompositeIndexForLabelLocked reports whether any composite index is
// declared against label. The caller holds sm.mu.
func (sm *SchemaManager) hasAnyCompositeIndexForLabelLocked(label string) bool {
	for _, idx := range sm.compositeIndexes {
		if idx != nil && idx.Label == label {
			return true
		}
	}
	return false
}

// PropertyIndexLookupAnyLabel looks up node IDs by property value across ALL
// indexes whose property name matches, regardless of the label they were
// declared against. Returns nil when no index covers `property`, and the
// union of hits across every matching index otherwise.
//
// This is the labelless-pattern counterpart to `PropertyIndexLookup`: a query
// like `MATCH (n {id:$x})` carries an inline equality on `id` but no label,
// so any index declared as `(Label1:id) … (LabelN:id)` is a legal candidate
// source. Without this method the executor has to choose between (a) probing
// every label-prop index name it can guess at, or (b) falling back to a full
// node scan; (b) is what graphify's edge MERGE hits today. The union here
// loses no information: it never returns false positives (a node either has
// the value indexed or it doesn't) and the executor still applies the rest
// of the pattern's filter on the returned candidates.
func (sm *SchemaManager) PropertyIndexLookupAnyLabel(property string, value interface{}) []NodeID {
	valueKey, ok := indexValueKey(value)
	if !ok {
		return nil
	}
	sm.mu.RLock()
	indexes := make([]*PropertyIndex, 0, 2)
	for _, idx := range sm.compositeIndexes {
		if idx == nil || len(idx.Properties) != 1 || idx.Properties[0] != property {
			continue
		}
		if idx.unfilled.Load() {
			// A label whose index isn't filled can't be answered from the
			// indexes; the caller's scan finds its nodes (#875).
			sm.mu.RUnlock()
			return nil
		}
		indexes = append(indexes, idx)
	}
	sm.mu.RUnlock()

	view, source := sm.beginPendingRead()
	defer endPendingRead(source)
	var ids []NodeID
	for _, idx := range indexes {
		ids = idx.lookupLocked(view, ids, property, valueKey)
	}
	return ids
}

// lookupLocked appends the index's node IDs for valueKey to out, merged with
// the pending view. It takes idx.mu.
func (idx *PropertyIndex) lookupLocked(view pendingWriteView, out []NodeID, property string, valueKey interface{}) []NodeID {
	pending := view.valueMatchSet(idx.Label, property, valueKey)
	idx.mu.RLock()
	stored := idx.values[valueKey]
	if len(stored)+len(pending) == 0 {
		idx.mu.RUnlock()
		return out
	}
	if out == nil {
		out = make([]NodeID, 0, len(stored)+len(pending))
	}
	for _, id := range stored {
		if view.keep(id) {
			out = append(out, id)
		}
	}
	idx.mu.RUnlock()
	return view.appendInNamespace(out, pending)
}

// PropertyIndexLookup looks up node IDs by property value using an index,
// merged with an attached AsyncEngine's pending writes (#719).
// Returns nil if no index exists for the label/property.
func (sm *SchemaManager) PropertyIndexLookup(label, property string, value interface{}) []NodeID {
	sm.mu.RLock()
	idx, exists := sm.seekablePropertyIndexLocked(label, property)
	sm.mu.RUnlock()

	if !exists {
		return nil
	}
	valueKey, ok := indexValueKey(value)
	if !ok {
		return nil
	}
	view, source := sm.beginPendingRead()
	defer endPendingRead(source)
	return idx.lookupLocked(view, nil, property, valueKey)
}

// PropertyIndexTopK returns up to limit node IDs from a property index ordered by
// indexed property value, merged with the pending writes. Nil keys are skipped.
func (sm *SchemaManager) PropertyIndexTopK(label, property string, limit int, descending bool) []NodeID {
	if limit <= 0 {
		return nil
	}
	ids, _ := sm.orderedPropertyIndexIDs(label, property, descending, limit, PropertyIndexBounds{})
	return ids
}

// PropertyIndexAllNonNil returns all node IDs from the property index in key order,
// excluding nil keys, merged with the pending writes.
func (sm *SchemaManager) PropertyIndexAllNonNil(label, property string, descending bool) []NodeID {
	ids, _ := sm.orderedPropertyIndexIDs(label, property, descending, -1, PropertyIndexBounds{})
	return ids
}

// orderedPropertyIndexIDs is the one ordered scan of a property index
// (orderedIDsLocked), merged with the pending writes; limit < 0 lists all,
// and keep, when set, selects the index values listed. exists is false when
// the label and property have no index.
func (sm *SchemaManager) orderedPropertyIndexIDs(label, property string, descending bool, limit int, bounds PropertyIndexBounds) (ids []NodeID, exists bool) {
	sm.mu.RLock()
	idx, exists := sm.seekablePropertyIndexLocked(label, property)
	sm.mu.RUnlock()
	if !exists {
		return nil, false
	}
	view, source := sm.beginPendingRead()
	defer endPendingRead(source)
	idx.rLockWithFreshKeys()
	defer idx.mu.RUnlock()
	return idx.orderedIDsLocked(view, property, descending, limit, bounds), true
}

// rLockWithFreshKeys takes idx.mu for reading with the sorted-key cache
// current, so a reader never rebuilds it: several readers hold the read lock
// at once, and two rebuilding together raced on the cache (#942). A stale
// cache is rebuilt under the write lock first; writers need that lock too,
// so the cache stays current while the read lock is held.
func (idx *PropertyIndex) rLockWithFreshKeys() {
	for {
		idx.mu.RLock()
		if !idx.keysDirty && idx.sortedNonNilKeys != nil {
			return
		}
		idx.mu.RUnlock()
		idx.mu.Lock()
		idx.sortedKeysViewLocked()
		idx.mu.Unlock()
	}
}

// sortedKeysViewLocked returns the cached non-nil index keys in ascending
// order and their kinds, rebuilding them after a write. The slice is shared
// and must not be modified; a rebuild replaces it rather than changing it.
// Caller must hold idx.mu for writing, or for reading through
// rLockWithFreshKeys, which leaves nothing to rebuild.
func (idx *PropertyIndex) sortedKeysViewLocked() ([]interface{}, propertyIndexKeyKinds) {
	if idx.keysDirty || idx.sortedNonNilKeys == nil {
		keys := make([]interface{}, 0, len(idx.values))
		for k, ids := range idx.values {
			if k == nil || len(ids) == 0 {
				continue
			}
			// Lists and maps are filed for equality only; ordered scans
			// read scalar keys (#844).
			if _, composite := k.(compositeIndexKey); composite {
				continue
			}
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			return compareSchemaIndexValues(keys[i], keys[j]) < 0
		})
		idx.sortedNonNilKeys = keys
		idx.sortedKeyKinds = newPropertyIndexKeyKinds(keys)
		idx.keysDirty = false
	}
	return idx.sortedNonNilKeys, idx.sortedKeyKinds
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

	left, right := reflect.ValueOf(a), reflect.ValueOf(b)
	switch {
	case left.CanInt() && right.CanInt():
		return cmp.Compare(left.Int(), right.Int())
	case left.CanUint() && right.CanUint():
		return cmp.Compare(left.Uint(), right.Uint())
	case left.CanInt() && right.CanUint():
		if left.Int() < 0 {
			return -1
		}
		return cmp.Compare(uint64(left.Int()), right.Uint())
	case left.CanUint() && right.CanInt():
		if right.Int() < 0 {
			return 1
		}
		return cmp.Compare(left.Uint(), uint64(right.Int()))
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

	// Equality indexes (arity-1 "property" and arity-N "composite" alike).
	for _, idx := range sm.compositeIndexes {
		if idx.OwningConstraint != "" {
			continue // counted as its constraint's RANGE index
		}
		idx.mu.RLock()
		totalEntries := int64(0)
		uniqueValues := int64(0)
		if len(idx.Properties) == 1 {
			for _, ids := range idx.values {
				totalEntries += int64(len(ids))
			}
			uniqueValues = int64(len(idx.values))
		} else {
			for _, ids := range idx.fullIndex {
				totalEntries += int64(len(ids))
			}
			uniqueValues = int64(len(idx.fullIndex))
		}
		selectivity := float64(0)
		if totalEntries > 0 {
			selectivity = float64(uniqueValues) / float64(totalEntries)
		}
		idx.mu.RUnlock()

		indexType := "COMPOSITE"
		prop := ""
		if len(idx.Properties) == 1 {
			indexType = "PROPERTY"
			prop = idx.Properties[0]
		}

		stats = append(stats, IndexStats{
			Name:         idx.Name,
			Type:         indexType,
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
			Type:         string(idx.effectiveKind()),
			Label:        idx.Label,
			Property:     idx.Property,
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

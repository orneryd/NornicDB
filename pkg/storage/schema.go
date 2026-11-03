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
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/orneryd/nornicdb/internal/ctxsort"
	"github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/localization"
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
	mu         contextRWMutex
	tokenOrder schemaTokenOrder
	// Names are added to the snapshot only after successful registration; a
	// schema replacement publishes a fresh snapshot.
	tokenSnapshot atomic.Pointer[schemaTokenSnapshot]

	// Temporal constraints (by temporalIndexCompletenessKey) created at
	// runtime: on an empty label their history index is complete from the
	// start; over existing nodes it is incomplete until the next rebuild. See
	// temporal_index_completeness.go.
	temporalIndexCompleteKeys   map[string]struct{}
	temporalIndexIncompleteKeys map[string]struct{}

	// Active commit-time locks for exact UNIQUE constraint values. Entries are
	// reference-counted across holders and waiters, then removed when unused, so
	// same-value transactions serialize without false contention between
	// disjoint production-sized batches or unbounded historical-key growth.
	uniqueConstraintCommitLocksMu sync.Mutex
	uniqueConstraintCommitLocks   map[uniqueConstraintLockKey]*uniqueConstraintCommitLock
	uniqueConstraintCommitOrder   uint64
	// uniqueConstraintLockWaits maps each owner waiting for a constraint key
	// lock to that lock, for deadlock detection (#961).
	uniqueConstraintLockWaits      map[string]*uniqueConstraintCommitLock
	uniqueConstraintLockWaitShared map[string]bool
	schemaMutationMu               contextRWMutex

	// Constraints
	uniqueConstraints       map[nodePropertyKey]*UniqueConstraint // exact node label/property identity
	constraints             map[string]Constraint                 // key: constraint name, stores all constraint types
	constraintContracts     map[string]ConstraintContract         // key: contract name
	propertyTypeConstraints map[string]PropertyTypeConstraint     // key: constraint name

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
	// publicationGate serializes a persisting schema mutation's definition
	// read, durable write and live publication with a native commit that
	// publishes a schema definition. A direct mutation holds it exclusively
	// from before it reads the published manager until after it persists, so it
	// cannot read between a native commit's physical write and that commit's
	// publication and persist a definition that omits the native change. A
	// native schema-dirty commit holds the same manager's gate from before its
	// first physical batch until its publication completes. Ordinary
	// entity-only commits never take it, and an isolated transaction schema
	// view is its own manager with its own gate.
	publicationGate trackedRWMutex
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
//	schema.AddVectorIndex("doc_embeddings", "Document", "embedding", 1024,
//		"cosine", storage.ConstraintEntityNode)
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
		uniqueConstraints:       make(map[nodePropertyKey]*UniqueConstraint),
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
		// Admission already holds the schema write lock: the index scan belongs
		// to that mutation's lifetime, not to a request that can walk away.
		idx, exists, err := sm.arity1PropertyIndexLocked(context.Background(), c.Label, c.Properties[0])
		if err != nil {
			return false, err
		}
		if exists && idx.OwningConstraint == "" {
			return false, newSchemaAdmissionError("IndexAlreadyExists", localization.StorageSchemaConstraintOverIndex(c.Label, c.Properties[0]))
		}
	}

	return true, nil
}

// applyConstraintLocked adds c, which admitConstraintLocked admitted, with
// its owned index and unique-value tracking.
func (sm *SchemaManager) applyConstraintLocked(c Constraint) {
	c = cloneConstraint(c)
	// An index-backed constraint owns an index of its own name, for nodes and
	// relationships alike, as in Neo4j.
	if c.OwnedIndex == "" && (c.Type == ConstraintUnique || c.Type == ConstraintNodeKey || c.Type == ConstraintRelationshipKey) {
		c.OwnedIndex = c.Name
	}

	sm.constraints[c.Name] = c
	sm.forgetTemporalIndexCompleteLocked(c)

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

	// A native DDL mutation holds the schema write lock and has no caller
	// context, so its own equality-index discovery runs on that mutation's
	// lifetime and cannot cancel mid-way.
	if err := sm.addConstraintPropertyIndexLocked(context.Background(), c); err != nil {
		panic(fmt.Sprintf("storage: non-cancellable DDL constraint-index discovery failed: %v", err))
	}

	if c.Type == ConstraintUnique && c.EffectiveEntityType() == ConstraintEntityNode && len(c.Properties) == 1 {
		uniqueKey := nodePropertyKey{label: c.Label, property: c.Properties[0]}
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

// nodeKeyScanHook lets storage tests assert that NODE KEY validation with a
// filled key index does not fall back to scanning the label.
var nodeKeyScanHook atomic.Pointer[func()]

// constraintCompositeKeyIndexWanted reports whether c is a multi-property
// node key (or multi-property uniqueness) constraint: it owns an arity-N
// equality index over its properties, so validating a write looks its key up
// instead of scanning the label.
func constraintCompositeKeyIndexWanted(c Constraint) bool {
	return (c.Type == ConstraintNodeKey || c.Type == ConstraintUnique) && len(c.Properties) > 1 && c.EffectiveEntityType() == ConstraintEntityNode
}

// addConstraintCompositeKeyIndexLocked registers the arity-N equality index a
// multi-property node key or uniqueness constraint owns. Like the arity-1
// owned index it starts unfilled: writes maintain it, and a fill (the DDL
// backfill or the startup rebuild) makes it usable for validation and seeks.
// An existing standalone index on exactly these properties serves instead.
//
// ctx is the lifetime of the mutation that owns the registration, as for the
// arity-1 owned index: a schema replacement discovers on its preparation
// context, a native DDL mutation on its write-held lifetime.
func (sm *SchemaManager) addConstraintCompositeKeyIndexLocked(ctx context.Context, c Constraint) error {
	existing, err := sm.compositeKeyIndexLocked(ctx, c.Label, c.Properties)
	if err != nil {
		return err
	}
	if existing != nil {
		return nil
	}
	name := c.OwnedIndex
	if name == "" {
		name = c.Name
	}
	idx := &CompositeIndex{
		Name:             name,
		Label:            c.Label,
		Properties:       append([]string(nil), c.Properties...),
		OwningConstraint: c.Name,
		values:           make(map[interface{}][]NodeID),
		keysDirty:        true,
		fullIndex:        make(map[string][]NodeID),
		prefixIndex:      make(map[string][]NodeID),
	}
	idx.unfilled.Store(true)
	sm.compositeIndexes[name] = idx
	return nil
}

// addConstraintPropertyIndexLocked registers the equality index a
// single-property uniqueness or node key constraint owns (#875), so property
// seeks use it as they use CREATE INDEX's. It starts empty and unfilled, so
// seeks ignore it until it is filled: by the caller that creates the
// constraint (cypher's addSchemaConstraint), by the startup rebuild with the
// other indexes, or by an import's rebuild. A store from before this index
// existed may already have an index of its own on the property, which then
// serves the seeks.
//
// ctx is the lifetime of the mutation that owns the registration: a schema
// replacement discovers on its own preparation context and reports a canceled
// scan, while a native DDL mutation holds the schema write lock and passes its
// own non-cancellable lifetime.
func (sm *SchemaManager) addConstraintPropertyIndexLocked(ctx context.Context, c Constraint) error {
	if constraintCompositeKeyIndexWanted(c) {
		return sm.addConstraintCompositeKeyIndexLocked(ctx, c)
	}
	if constraintPropertyIndexKey(c) == "" {
		return nil
	}
	if _, exists, err := sm.arity1PropertyIndexLocked(ctx, c.Label, c.Properties[0]); err != nil {
		return err
	} else if exists {
		return nil
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
	return nil
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

// schemaMutationLease is one admitted persisting schema mutation: the
// publication gate and the schema lock, both held from before the mutated
// state is read until after it is persisted. It is taken by value on the
// caller's stack and released once.
type schemaMutationLease struct {
	schema *SchemaManager
}

// beginSchemaMutation admits one persisting schema mutation. The publication
// gate is taken before the schema lock, the reverse of release, so a direct
// mutation cannot read the manager between a native commit's physical write
// and its publication, and a native commit waiting for the gate holds no
// schema lock.
func (sm *SchemaManager) beginSchemaMutation(ctx context.Context) (schemaMutationLease, error) {
	if err := sm.publicationGate.LockContext(ctx); err != nil {
		return schemaMutationLease{}, err
	}
	if err := sm.mu.LockContext(ctx); err != nil {
		sm.publicationGate.Unlock()
		return schemaMutationLease{}, err
	}
	lease := schemaMutationLease{schema: sm}
	if err := ctx.Err(); err != nil {
		lease.release()
		return schemaMutationLease{}, err
	}
	return lease, nil
}

// release abandons a mutation that published nothing: the schema lock and then
// the publication gate, the reverse of the order they were taken.
func (lease schemaMutationLease) release() {
	lease.schema.mu.Unlock()
	lease.schema.publicationGate.Unlock()
}

// commitPolicy persists the knowledge-policy change staged under the lease and
// publishes its effects only when the persister accepts it. On rejection it
// restores the published family and leaves every other family, the compiled
// binding table and all runtime index caches untouched. The changed hook runs
// after both the schema lock and the publication gate are released, because it
// re-enters the engine and admits itself again.
func (lease schemaMutationLease) commitPolicy(restore func()) error {
	sm := lease.schema
	// The staged families are compiled before the persister runs: a definition
	// whose decay bindings or policy references do not compile is rejected
	// instead of being persisted, and the previously published table keeps
	// serving until an accepted candidate replaces it.
	compiled, err := sm.compileBindingTableLocked()
	if err != nil {
		restore()
		lease.release()
		return err
	}
	if persist := sm.persist; persist != nil {
		// An accepted schema mutation owns its completion and has no caller
		// context to cancel, so its definition snapshot runs under its own
		// explicit unbounded lifetime.
		def, err := sm.exportDefinitionLocked(context.Background())
		if err == nil {
			err = persist(def)
		}
		if err != nil {
			restore()
			lease.release()
			return err
		}
	}
	sm.bindingTable = compiled
	onChanged := sm.knowledgePolicyChanged
	lease.release()
	if onChanged != nil {
		onChanged()
	}
	return nil
}

// schemaPublicationLease is one admitted native schema publication: the
// publication gate held from before a schema-dirty commit's first physical
// batch until its definition is published.
type schemaPublicationLease struct {
	schema *SchemaManager
}

// beginSchemaPublication admits a native commit that publishes a schema
// definition. A direct persisting mutation admitted afterwards reads the
// definition this commit published, not one that omits it. The wait honors the
// commit's context; a cancelled wait leaves the gate unheld.
func (sm *SchemaManager) beginSchemaPublication(ctx context.Context) (schemaPublicationLease, error) {
	if err := sm.publicationGate.LockContext(ctx); err != nil {
		return schemaPublicationLease{}, err
	}
	return schemaPublicationLease{schema: sm}, nil
}

// release ends the native commit's schema publication interval. The zero lease
// releases nothing, so a commit that never took the gate can release it
// unconditionally.
func (lease schemaPublicationLease) release() {
	if lease.schema != nil {
		lease.schema.publicationGate.Unlock()
	}
}

// UniqueConstraint represents a unique constraint on a label and property.
type UniqueConstraint struct {
	Name     string
	Label    string
	Property string
	values   map[interface{}]NodeID // Track unique values
	// valuesCacheComplete is true once the values cache has been rebuilt from storage.
	valuesCacheComplete bool
	mu                  contextRWMutex
}

// PropertyIndex is the arity-1 form of a CompositeIndex. It is a type alias
// so every existing single-property index call site keeps working while the
// underlying value is the same composite index type used for multi-property
// indexes. The single-property ordered-scan fields (values, sortedNonNilKeys,
// sortedKeyKinds, keysDirty) and OwningConstraint live on CompositeIndex.
type PropertyIndex = CompositeIndex

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

	mu contextRWMutex
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
	lease, admitErr := sm.beginSchemaMutation(context.Background())
	if admitErr != nil {
		return admitErr
	}
	defer lease.release()

	silent := len(ifNotExists) > 0 && ifNotExists[0]
	constraint := Constraint{
		Name:       name,
		Label:      label,
		Properties: []string{property},
		Type:       ConstraintUnique,
	}
	return sm.addSchemaRuleLocked(
		func() (bool, error) { return sm.admitConstraintLocked(constraint, silent) },
		sm.stageConstraintFamilies,
		func() { sm.applyConstraintLocked(constraint) },
	)
}

// addSchemaRuleLocked adds a constraint: admit checks it against the
// schema without changing it, and only when admit accepts it does the
// adder copy the schema families apply will change, apply the change and
// persist the schema; if persisting fails, those families return to the
// published entries they held, so every other constraint, index and runtime
// cache keeps its live allocation and no other family is rebuilt. An IF NOT
// EXISTS that finds the constraint already present costs the check alone, as an
// existing index does for the index adders (#823: every repeated CREATE
// CONSTRAINT ... IF NOT EXISTS exported and rewrote the whole schema).
func (sm *SchemaManager) addSchemaRuleLocked(admit func() (bool, error), stage func() schemaRuleRollback, apply func()) error {
	add, err := admit()
	if err != nil || !add {
		return err
	}
	if sm.persist == nil {
		apply()
		return nil
	}
	rollback := stage()
	apply()
	// The definition snapshot of an accepted mutation runs under its own
	// explicit unbounded lifetime; it has no caller context to cancel.
	def, err := sm.exportDefinitionLocked(context.Background())
	if err == nil {
		err = sm.persist(def)
	}
	if err != nil {
		rollback()
		return err
	}
	return nil
}

// schemaRuleRollback restores the entries one admitted schema rule add changed.
type schemaRuleRollback func()

// stageSchemaFamily copies one schema family so a rejected rule add restores
// its published entries, including the runtime value caches and compiled index
// pointers those entries own, without rebuilding any other family.
func stageSchemaFamily[K comparable, M any](field *map[K]M) schemaRuleRollback {
	published := maps.Clone(*field)
	return func() { *field = published }
}

// stageConstraintFamilies stages the families applyConstraintLocked changes.
func (sm *SchemaManager) stageConstraintFamilies() schemaRuleRollback {
	return combineSchemaRuleRollbacks(
		stageSchemaFamily(&sm.constraints),
		stageSchemaFamily(&sm.uniqueConstraints),
		stageSchemaFamily(&sm.compositeIndexes),
		stageSchemaFamily(&sm.rangeIndexes),
		stageSchemaFamily(&sm.temporalIndexCompleteKeys),
		stageSchemaFamily(&sm.temporalIndexIncompleteKeys),
	)
}

// stagePropertyTypeConstraintFamily stages the family a property type
// constraint add changes.
func (sm *SchemaManager) stagePropertyTypeConstraintFamily() schemaRuleRollback {
	return stageSchemaFamily(&sm.propertyTypeConstraints)
}

// combineSchemaRuleRollbacks restores several staged families as one.
func combineSchemaRuleRollbacks(rollbacks ...schemaRuleRollback) schemaRuleRollback {
	return func() {
		for _, rollback := range rollbacks {
			rollback()
		}
	}
}

// PropertyTypeConstraintOptions selects entity scope and duplicate admission.
// A zero EntityType retains node scoping; IfNotExists makes an equal rule a no-op.
type PropertyTypeConstraintOptions struct {
	EntityType  ConstraintEntityType
	IfNotExists bool
}

// AddPropertyTypeConstraint enforces a property type (NULL values allowed),
// with entity scope and duplicate admission selected in opts.
//
// Example: sm.AddPropertyTypeConstraint("age_type", "Person", "age",
// PropertyTypeInteger, PropertyTypeConstraintOptions{EntityType: ConstraintEntityNode}).
func (sm *SchemaManager) AddPropertyTypeConstraint(name, label, property string, expectedType PropertyType, opts PropertyTypeConstraintOptions) error {
	lease, admitErr := sm.beginSchemaMutation(context.Background())
	if admitErr != nil {
		return admitErr
	}
	defer lease.release()
	ptc := PropertyTypeConstraint{
		Name:         name,
		EntityType:   opts.EntityType,
		Label:        label,
		Property:     property,
		ExpectedType: expectedType,
	}
	return sm.addSchemaRuleLocked(
		func() (bool, error) { return sm.admitPropertyTypeConstraintLocked(ptc, opts.IfNotExists) },
		sm.stagePropertyTypeConstraintFamily,
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

	lease, admitErr := sm.beginSchemaMutation(context.Background())
	if admitErr != nil {
		return admitErr
	}
	defer lease.release()

	if _, exists := sm.compositeIndexes[name]; exists {
		return nil // Already exists (idempotent)
	}

	sm.compositeIndexes[name] = &CompositeIndex{
		Name:        name,
		Label:       label,
		Properties:  slices.Clone(properties),
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
		def, err := sm.exportDefinitionLocked(context.Background()) // accepted mutation owns its completion
		if err == nil {
			err = sm.persist(def)
		}
		if err != nil {
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
//
// ctx is the request the discovery serves: it bounds the wait for the schema
// lock and the scan of every declared index, and a discovery that observes its
// cancellation reports that error and no candidates rather than the indexes it
// had already collected.
func (sm *SchemaManager) SeekableCompositeIndexesForLabel(ctx context.Context, label string) ([]*CompositeIndex, error) {
	if err := sm.mu.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer sm.mu.RUnlock()

	var indexes []*CompositeIndex
	var cancellation bulkReadCancellation
	for _, idx := range sm.compositeIndexes {
		if err := cancellation.check(ctx); err != nil {
			return nil, err
		}
		if idx != nil && idx.Label == label && !idx.unfilled.Load() {
			indexes = append(indexes, idx)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return indexes, nil
}

// BackfillCompositeIndex fills a newly created composite index from the
// already-stored nodes, mirroring BackfillPropertyIndex. Nodes with pending
// writes in an attached AsyncEngine are skipped: the engine indexes them when
// they are flushed, and lookups see them through the pending view until then
// (#719). On success the index is marked filled so seeks can use it. The caller
// retains ctx through pending admission and every node preparation; a canceled
// fill never marks an unfilled index ready; the DDL owner discards a canceled
// staged declaration before it becomes visible.
func (sm *SchemaManager) BackfillCompositeIndex(ctx context.Context, name string, entries map[NodeID]map[string]interface{}) error {
	if err := sm.mu.RLockContext(ctx); err != nil {
		return err
	}
	idx := sm.compositeIndexes[name]
	sm.mu.RUnlock()
	if idx == nil {
		return localizedError(localization.StorageSchemaIndexNotFound(name), nil)
	}
	// The fill's one test hook runs at its entry: the index this call fills is
	// known (it names the hook), and no value is written yet. It runs without
	// sm.mu and before the pending-read lock, so a parked observer blocks
	// neither schema mutation nor the pending writes the fill is about to read.
	if hook := schemaIndexFillHook.Load(); hook != nil {
		if err := (*hook)(name, idx.Label); err != nil {
			return err
		}
	}

	// A DDL request owns this preparation until the completed index is admitted.
	view, source, err := sm.beginPendingRead(ctx)
	if err != nil {
		return err
	}
	defer endPendingRead(source)
	for nodeID, props := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !view.keep(nodeID) {
			continue
		}
		if err := idx.IndexNode(ctx, nodeID, props); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	idx.unfilled.Store(false)
	return nil
}

// LookupFull finds nodes matching all property values exactly.
// All properties in the composite index must be specified.
func (idx *CompositeIndex) LookupFull(ctx context.Context, values ...interface{}) ([]NodeID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(values) != len(idx.Properties) {
		return nil, nil
	}
	if len(idx.Properties) == 1 {
		valueKey, ok, err := indexValueKey(ctx, values[0])
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
		if err := idx.mu.RLockContext(ctx); err != nil {
			return nil, err
		}
		defer idx.mu.RUnlock()
		return appendIndexNodeIDs(ctx, nil, idx.values[valueKey])
	}
	canonical, ok, err := compositeLookupKeyValues(ctx, values)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ctx.Err()
	}
	key, err := NewCompositeKey(ctx, canonical...)
	if err != nil {
		return nil, err
	}
	return idx.lookupFullHash(ctx, key.Hash)
}

// appendIndexNodeIDs appends a borrowed index population into owned output in
// bounded blocks. No partial output escapes on cancellation. A nil output
// requests a fresh owned copy, including the existing nonnil empty-slice case.
func appendIndexNodeIDs(ctx context.Context, out, ids []NodeID) ([]NodeID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = make([]NodeID, 0, len(ids))
	}
	for start := 0; start < len(ids); start += bulkReadCancellationInterval {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		out = append(out, ids[start:min(start+bulkReadCancellationInterval, len(ids))]...)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// lookupFullHash returns a copy of the nodes an arity >= 2 index files under
// a full composite key hash, for callers that already hold the hash.
func (idx *CompositeIndex) lookupFullHash(ctx context.Context, hash string) ([]NodeID, error) {
	if err := idx.mu.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer idx.mu.RUnlock()
	if nodes, exists := idx.fullIndex[hash]; exists {
		return appendIndexNodeIDs(ctx, nil, nodes)
	}
	return nil, ctx.Err()
}

// LookupPrefix finds nodes matching a prefix of property values.
// Specify 1 to N-1 property values (where N is total properties in index).
// Returns all nodes that match the prefix. For an arity-1 index a one-value
// lookup is a full match.
//
// Example: For index on (country, city, zipcode)
//   - LookupPrefix(ctx, "US") returns all nodes in the US
//   - LookupPrefix(ctx, "US", "NYC") returns all nodes in NYC, US
func (idx *CompositeIndex) LookupPrefix(ctx context.Context, values ...interface{}) ([]NodeID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(values) == 0 || len(values) > len(idx.Properties) {
		return nil, nil
	}
	if len(idx.Properties) == 1 {
		return idx.LookupFull(ctx, values...)
	}
	canonical, ok, err := compositeLookupKeyValues(ctx, values)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ctx.Err()
	}
	// Prepare the key before taking the read lock: a cancelled preparation
	// files nothing and waits on no holder.
	key, err := NewCompositeKey(ctx, canonical...)
	if err != nil {
		return nil, err
	}
	if err := idx.mu.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer idx.mu.RUnlock()
	source := idx.prefixIndex
	if len(canonical) == len(idx.Properties) {
		source = idx.fullIndex
	}
	if nodes, exists := source[key.Hash]; exists {
		return appendIndexNodeIDs(ctx, nil, nodes)
	}
	return nil, ctx.Err()
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
func (idx *CompositeIndex) LookupWithFilter(ctx context.Context, filter func(NodeID) bool, values ...interface{}) ([]NodeID, error) {
	candidates, err := idx.LookupPrefix(ctx, values...)
	if err != nil || candidates == nil {
		return nil, err
	}
	var result []NodeID
	for _, id := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if filter(id) {
			result = append(result, id)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// Stats returns statistics about the composite index. valueEntries counts
// equality-key buckets; membership-only unkeyable values are excluded.
func (idx *CompositeIndex) Stats() map[string]interface{} {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	valueEntries := len(idx.values)
	if _, exists := idx.values[nonNilUnorderedIndexKey{}]; exists {
		valueEntries--
	}
	return map[string]interface{}{
		"name":             idx.Name,
		"label":            idx.Label,
		"properties":       idx.Properties,
		"fullIndexEntries": len(idx.fullIndex),
		"prefixEntries":    len(idx.prefixIndex),
		"valueEntries":     valueEntries,
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
			return slices.Delete(slice, i, i+1)
		}
	}
	return slice
}

// AddFulltextIndex adds a full-text index over node labels or relationship
// types. The entity kind selects which target list is stored; all supplied
// slices are owned by the schema. Existing names remain an idempotent no-op.
// For example, AddFulltextIndex("people", []string{"Person"}, []string{"name"},
// ConstraintEntityNode) indexes node names without changing stored formats.
func (sm *SchemaManager) AddFulltextIndex(name string, targets, properties []string, entityType ConstraintEntityType) error {
	if entityType != ConstraintEntityNode && entityType != ConstraintEntityRelationship {
		return fmt.Errorf("%w: full-text entity kind %q", ErrInvalidData, entityType)
	}
	lease, admitErr := sm.beginSchemaMutation(context.Background())
	if admitErr != nil {
		return admitErr
	}
	defer lease.release()
	if _, exists := sm.fulltextIndexes[name]; exists {
		return nil
	}
	definition := &FulltextIndex{Name: name, Properties: slices.Clone(properties)}
	if entityType == ConstraintEntityNode {
		definition.Labels = slices.Clone(targets)
	} else {
		definition.RelationshipTypes = slices.Clone(targets)
	}
	sm.fulltextIndexes[name] = definition
	if sm.persist != nil {
		def, err := sm.exportDefinitionLocked(context.Background()) // accepted mutation owns its completion
		if err == nil {
			err = sm.persist(def)
		}
		if err != nil {
			delete(sm.fulltextIndexes, name)
			return err
		}
	}
	return nil
}

// AddVectorIndex adds a vector index scoped to a node label or relationship type.
//
// Example: sm.AddVectorIndex("doc_vector", "Doc", "embedding", 384,
// "cosine", ConstraintEntityNode).
func (sm *SchemaManager) AddVectorIndex(name, label, property string, dimensions int, similarityFunc string, entityType ConstraintEntityType) error {
	lease, admitErr := sm.beginSchemaMutation(context.Background())
	if admitErr != nil {
		return admitErr
	}
	defer lease.release()

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
		def, err := sm.exportDefinitionLocked(context.Background()) // accepted mutation owns its completion
		if err == nil {
			err = sm.persist(def)
		}
		if err != nil {
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

	lease, admitErr := sm.beginSchemaMutation(context.Background())
	if admitErr != nil {
		return admitErr
	}
	defer lease.release()

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
		def, err := sm.exportDefinitionLocked(context.Background()) // accepted mutation owns its completion
		if err == nil {
			err = sm.persist(def)
		}
		if err != nil {
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
		idx.entries = slices.Delete(idx.entries, i, i+1)
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

// GetConstraints returns single-property node UNIQUE constraints.
// ctx bounds metadata admission and enumeration. The only errors are context
// cancellation/deadline errors; an error returns no partial constraints.
func (sm *SchemaManager) GetConstraints(ctx context.Context) ([]UniqueConstraint, error) {
	if err := sm.mu.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer sm.mu.RUnlock()

	constraints := make([]UniqueConstraint, 0, len(sm.uniqueConstraints))
	for _, c := range sm.uniqueConstraints {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		constraints = append(constraints, UniqueConstraint{
			Name:     c.Name,
			Label:    c.Label,
			Property: c.Property,
		})
	}

	return constraints, nil
}

// GetConstraintsForLabels returns all constraints for given labels, ordered
// by constraint name so validation reports the same violation every time an
// entity breaks several constraints. Returns constraints from the constraints
// map, preserving their original types. Optional types restrict the result
// before copying, so specialized consumers do not allocate unrelated rules.
//
// ctx is the request the discovery serves: it bounds the wait for the schema
// lock and the scan of every declared constraint, and a discovery that observes
// its cancellation reports that error and no constraints rather than the
// partially filtered list it had already collected.
func (sm *SchemaManager) GetConstraintsForLabels(ctx context.Context, labels []string, types ...ConstraintType) ([]Constraint, error) {
	if err := sm.mu.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer sm.mu.RUnlock()

	result := make([]Constraint, 0)
	var cancellation bulkReadCancellation

	// Get constraints from the constraints map (preserves original type)
	for _, c := range sm.constraints {
		if err := cancellation.check(ctx); err != nil {
			return nil, err
		}
		if len(types) > 0 && !slices.Contains(types, c.Type) {
			continue
		}
		for _, label := range labels {
			if c.Label == label {
				result = append(result, c)
				break
			}
		}
	}
	slices.SortFunc(result, func(a, b Constraint) int { return strings.Compare(a.Name, b.Name) })
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return result, nil
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
	lease, admitErr := sm.beginSchemaMutation(context.Background())
	if admitErr != nil {
		return admitErr
	}
	defer lease.release()

	silentOnDuplicate := len(ifNotExists) > 0 && ifNotExists[0]
	return sm.addSchemaRuleLocked(
		func() (bool, error) { return sm.admitConstraintLocked(c, silentOnDuplicate) },
		sm.stageConstraintFamilies,
		func() { sm.applyConstraintLocked(c) },
	)
}

// DropIndex removes an index (by name) from the schema.
// It searches across all index types: property, composite, fulltext, vector, and range.
func (sm *SchemaManager) DropIndex(name string) error {
	lease, admitErr := sm.beginSchemaMutation(context.Background())
	if admitErr != nil {
		return admitErr
	}
	defer lease.release()

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
			def, err := sm.exportDefinitionLocked(context.Background()) // accepted mutation owns its completion
			if err == nil {
				err = sm.persist(def)
			}
			if err != nil {
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
		def, err := sm.exportDefinitionLocked(context.Background()) // accepted mutation owns its completion
		if err == nil {
			err = sm.persist(def)
		}
		if err != nil {
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

// PropertyIndexInsert adds a node to an arity-1 index. It is the
// single-property spelling of IndexNode for callers that carry the value
// directly (imports, backfills); node-write maintenance calls IndexNode.
func (sm *SchemaManager) PropertyIndexInsert(label, property string, nodeID NodeID, value interface{}) error {
	sm.mu.RLock()
	// An import or backfill files into the index without a request of its own:
	// the presence scan has this call's lifetime and cannot cancel.
	idx, exists, err := sm.arity1PropertyIndexLocked(context.Background(), label, property)
	sm.mu.RUnlock()
	if err != nil {
		return err
	}

	if !exists {
		return localizedError(localization.StorageSchemaPropertyIndexNotFound(label, property), nil)
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.values == nil {
		idx.values = make(map[interface{}][]NodeID)
	}

	// An insert files the value of a write the engine already accepted, so the
	// key has that write's lifetime: it cannot cancel.
	valueKey, ok, err := indexNonNilMembershipKey(context.Background(), value)
	if err != nil {
		return err
	}
	if !ok {
		// Null has no nonnull membership.
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
	// Backfill is a context-free Go maintenance boundary: its presence scan
	// has that maintenance lifetime and cannot cancel.
	idx, exists, err := sm.arity1PropertyIndexLocked(context.Background(), label, property)
	sm.mu.RUnlock()
	if err != nil {
		return err
	}
	if !exists {
		return localizedError(localization.StorageSchemaPropertyIndexNotFound(label, property), nil)
	}

	// Backfill is a context-free Go maintenance boundary.
	view, source, err := sm.beginPendingRead(context.Background())
	if err != nil {
		return err
	}
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
	// Deletion maintenance follows the write that removed the node: the
	// presence scan has that write's lifetime and cannot cancel.
	idx, exists, err := sm.arity1PropertyIndexLocked(context.Background(), label, property)
	sm.mu.RUnlock()
	if err != nil {
		return err
	}

	if !exists {
		return nil // Not indexed
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// A delete follows the write that removed the node, so the key has that
	// write's lifetime: it cannot cancel.
	valueKey, ok, err := indexNonNilMembershipKey(context.Background(), value)
	if err != nil {
		return err
	}
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
func (sm *SchemaManager) PropertyIndexLookupAnyLabel(ctx context.Context, property string, value interface{}) ([]NodeID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	valueKey, ok, err := indexValueKey(ctx, value)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ctx.Err()
	}
	if err := sm.mu.RLockContext(ctx); err != nil {
		return nil, err
	}
	indexes := make([]*PropertyIndex, 0, 2)
	for _, idx := range sm.compositeIndexes {
		if err := ctx.Err(); err != nil {
			sm.mu.RUnlock()
			return nil, err
		}
		if idx == nil || len(idx.Properties) != 1 || idx.Properties[0] != property {
			continue
		}
		if idx.unfilled.Load() {
			sm.mu.RUnlock()
			return nil, nil
		}
		indexes = append(indexes, idx)
	}
	sm.mu.RUnlock()
	view, source, err := sm.beginPendingRead(ctx)
	if err != nil {
		return nil, err
	}
	defer endPendingRead(source)
	var ids []NodeID
	for _, idx := range indexes {
		ids, err = idx.lookupLocked(ctx, view, ids, property, valueKey)
		if err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

// lookupLocked appends the index's node IDs for valueKey to out, merged with
// the pending view. It takes idx.mu.
func (idx *PropertyIndex) lookupLocked(ctx context.Context, view pendingWriteView, out []NodeID, property string, valueKey interface{}) ([]NodeID, error) {
	pending := view.valueMatchSet(idx.Label, property, valueKey)
	if err := idx.mu.RLockContext(ctx); err != nil {
		return nil, err
	}
	stored := idx.values[valueKey]
	if len(stored)+len(pending) == 0 {
		idx.mu.RUnlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return out, nil
	}
	if out == nil {
		out = make([]NodeID, 0, len(stored)+len(pending))
	}
	if view.index == nil {
		out, err := appendIndexNodeIDs(ctx, out, stored)
		idx.mu.RUnlock()
		return out, err
	}
	var cancellation bulkReadCancellation
	for _, id := range stored {
		if err := cancellation.check(ctx); err != nil {
			idx.mu.RUnlock()
			return nil, err
		}
		if view.keep(id) {
			out = append(out, id)
		}
	}
	idx.mu.RUnlock()
	return view.appendInNamespace(ctx, out, pending)
}

// PropertyIndexLookup looks up node IDs by property value using an index,
// merged with an attached AsyncEngine's pending writes (#719).
// Returns nil if no index exists for the label/property.
func (sm *SchemaManager) PropertyIndexLookup(ctx context.Context, label, property string, value interface{}) ([]NodeID, error) {
	if err := sm.mu.RLockContext(ctx); err != nil {
		return nil, err
	}
	idx, exists, lookupErr := sm.seekablePropertyIndexLocked(ctx, label, property)
	sm.mu.RUnlock()
	if lookupErr != nil {
		return nil, lookupErr
	}
	if !exists {
		return nil, nil
	}
	valueKey, ok, err := indexValueKey(ctx, value)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ctx.Err()
	}
	view, source, err := sm.beginPendingRead(ctx)
	if err != nil {
		return nil, err
	}
	defer endPendingRead(source)
	return idx.lookupLocked(ctx, view, nil, property, valueKey)
}

// PropertyIndexTopK returns up to limit node IDs from a property index ordered by
// indexed property value, merged with the pending writes. Nil keys are skipped.
// ErrPropertyIndexValueOrderUnavailable reports nonnull memberships the index
// cannot value-order; callers must order the actual property values instead.
func (sm *SchemaManager) PropertyIndexTopK(ctx context.Context, label, property string, limit int, descending bool) ([]NodeID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	snapshot, _, err := sm.readPropertyIndexSnapshot(ctx, label, property, propertyIndexRead{descending: descending, limit: limit, exactBounds: true})
	return snapshot.ids, err
}

// PropertyIndexAllNonNil returns all node IDs from the property index in key order,
// excluding nil keys, merged with the pending writes. Unorderable memberships
// return ErrPropertyIndexValueOrderUnavailable, never a partial population.
// IS NOT NULL planners use PropertyIndexNonNilCandidates instead.
func (sm *SchemaManager) PropertyIndexAllNonNil(ctx context.Context, label, property string, descending bool) ([]NodeID, error) {
	snapshot, _, err := sm.readPropertyIndexSnapshot(ctx, label, property, propertyIndexRead{descending: descending, limit: -1, exactBounds: true})
	return snapshot.ids, err
}

// rLockWithFreshKeys takes idx.mu for reading with the sorted-key cache
// current, so a reader never rebuilds it: several readers hold the read lock
// at once, and two rebuilding together raced on the cache (#942). A stale
// cache is rebuilt under the write lock first, and that write hold is
// downgraded to the read hold this call returns: a writer queued behind the
// rebuild cannot dirty the cache between the rebuild and the caller's read,
// and it cannot starve the rebuild out (#1693). A canceled or failed rebuild
// releases the write lock and returns its error.
func (idx *PropertyIndex) rLockWithFreshKeys(ctx context.Context) error {
	if err := idx.mu.RLockContext(ctx); err != nil {
		return err
	}
	if !idx.keysDirty && idx.sortedNonNilKeys != nil {
		return nil
	}
	idx.mu.RUnlock()
	if err := idx.mu.LockContext(ctx); err != nil {
		return err
	}
	if _, _, err := idx.sortedKeysViewLocked(ctx); err != nil {
		idx.mu.Unlock()
		return err
	}
	idx.mu.downgrade()
	return nil
}

// sortedKeysViewLocked returns the cached non-nil index keys in ascending
// order and their kinds, rebuilding them after a write. The slice is shared
// and must not be modified; a rebuild replaces it rather than changing it.
// Caller must hold idx.mu for writing, or for reading through
// rLockWithFreshKeys, which leaves nothing to rebuild.
func (idx *PropertyIndex) sortedKeysViewLocked(ctx context.Context) ([]interface{}, propertyIndexKeyKinds, error) {
	if err := ctx.Err(); err != nil {
		return nil, propertyIndexKeyKinds{}, err
	}
	if idx.keysDirty || idx.sortedNonNilKeys == nil {
		keys := make([]interface{}, 0, len(idx.values))
		kinds := propertyIndexKeyKinds{strings: true, numbers: true}
		for key, ids := range idx.values {
			if err := ctx.Err(); err != nil {
				return nil, propertyIndexKeyKinds{}, err
			}
			if key == nil || len(ids) == 0 {
				continue
			}
			if unorderedIndexMembership(key) {
				kinds.unordered = true
				continue
			}
			keys = append(keys, key)
			isString, isNumber := indexValueKinds(key)
			kinds.strings = kinds.strings && isString
			kinds.numbers = kinds.numbers && isNumber
		}
		if err := ctxsort.Sort(ctx, keys, compareSchemaIndexValues, false); err != nil {
			return nil, propertyIndexKeyKinds{}, err
		}
		kinds.hasKeys = len(keys) > 0
		// Publish the complete cache only after all cancellable preparation succeeds.
		idx.sortedNonNilKeys, idx.sortedKeyKinds, idx.keysDirty = keys, kinds, false
	}
	return idx.sortedNonNilKeys, idx.sortedKeyKinds, nil
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

	// Numbers, including the exact integer keys numericIndexKey files beyond
	// float64's precision, order exactly; NaN compares equal, as before.
	if order, numeric, ordered := compareConstraintNumbers(reflect.ValueOf(a), reflect.ValueOf(b)); numeric {
		if !ordered {
			return 0
		}
		return order
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

// IndexStats represents statistics about an index. For an arity-one equality
// index, TotalEntries and UniqueValues describe equality-keyed entries only;
// they exclude membership-only unkeyable values, preserving their established
// meaning. They are not a count of complete nonnull candidates.
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
			for key, ids := range idx.values {
				// Membership-only markers are not equality values.
				if _, marker := key.(nonNilUnorderedIndexKey); marker {
					continue
				}
				totalEntries += int64(len(ids))
				uniqueValues++
			}
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

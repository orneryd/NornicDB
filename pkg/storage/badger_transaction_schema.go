package storage

import (
	"context"
	"fmt"
	"reflect"
)

// SchemaPurpose identifies the edits made through an isolated transaction schema.
type SchemaPurpose uint8

const (
	// KnowledgePolicyChanges reads an isolated schema without claiming a writer;
	// persisting a policy change performs admission before staging it.
	KnowledgePolicyChanges SchemaPurpose = iota
	// OrdinarySchemaChanges admits a schema writer and excludes entity writes.
	OrdinarySchemaChanges
)

func mixedSchemaAndDataError(schemaAfterData bool) error {
	message := "Tried to execute Write query after executing Schema modification"
	if schemaAfterData {
		message = "Tried to execute Schema modification after executing Write query"
	}
	return fmt.Errorf("Neo.ClientError.Transaction.ForbiddenDueToTransactionType: %s", message)
}

// StageSchemaChanges captures ordinary DDL and its runtime backfills for commit.
// Obtain the isolated view with Schema(OrdinarySchemaChanges), apply and backfill the
// schema change, then call StageSchemaChanges before Commit. Rollback discards
// the captured state; unchanged committed indexes keep their live caches.
func (tx *BadgerTransaction) StageSchemaChanges() error {
	tx.mu.Lock()
	view := tx.knowledgeSchema
	tx.mu.Unlock()
	if view == nil {
		return fmt.Errorf("schema changes require a transaction schema view")
	}
	view.mu.RLock()
	defer view.mu.RUnlock()
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionSchemaWrite); err != nil {
		return err
	}
	if !tx.knowledgeSchemaDirty {
		return nil
	}
	snapshot := NewSchemaManager()
	if err := snapshot.ReplaceFromDefinition(context.Background(), tx.knowledgeSchemaDefinition); err != nil {
		return err
	}
	for key, constraint := range snapshot.uniqueConstraints {
		if staged := view.uniqueConstraints[key]; staged != nil && staged.Name == constraint.Name {
			staged.mu.RLock()
			constraint.values = make(map[interface{}]NodeID, len(staged.values))
			for value, nodeID := range staged.values {
				constraint.values[value] = nodeID
			}
			constraint.valuesCacheComplete = staged.valuesCacheComplete
			staged.mu.RUnlock()
		}
	}
	for name, index := range snapshot.compositeIndexes {
		if staged := view.compositeIndexes[name]; staged != nil && reflect.DeepEqual(index.Properties, staged.Properties) {
			staged.mu.RLock()
			index.values = cloneSchemaIndexValues(staged.values)
			index.keysDirty = true
			index.unfilled.Store(staged.unfilled.Load())
			index.fullIndex = cloneSchemaIndexValues(staged.fullIndex)
			index.prefixIndex = cloneSchemaIndexValues(staged.prefixIndex)
			staged.mu.RUnlock()
		}
	}
	for name, index := range snapshot.rangeIndexes {
		if staged := view.rangeIndexes[name]; staged != nil && sameRangeIndexDefinition(index, staged) {
			staged.mu.RLock()
			index.entries = append([]rangeEntry(nil), staged.entries...)
			index.nodeValue = make(map[NodeID]float64, len(staged.nodeValue))
			for nodeID, value := range staged.nodeValue {
				index.nodeValue[nodeID] = value
			}
			staged.mu.RUnlock()
		}
	}
	// A new temporal constraint's history entries are staged in this transaction
	// (MarkNewTemporalConstraintsForEngine), and the statement's view records the
	// index complete once they are. The snapshot that publishes the runtime
	// values carries those marks as the statement's own: an index whose entries
	// are complete must stay trusted after publication, or every later write to
	// the label validates by scanning it instead (#1126). The view holds no mark
	// of a constraint the statement did not touch, so publication keeps the live
	// one (installSchemaLocked).
	snapshot.temporalIndexCompleteKeys = cloneTemporalIndexMarkSet(view.temporalIndexCompleteKeys)
	snapshot.temporalIndexIncompleteKeys = cloneTemporalIndexMarkSet(view.temporalIndexIncompleteKeys)
	tx.schemaRuntime = snapshot
	return nil
}

// cloneTemporalIndexMarkSet copies one set of runtime temporal-index
// completeness marks, so the published snapshot owns its own set.
func cloneTemporalIndexMarkSet(marks map[string]struct{}) map[string]struct{} {
	if len(marks) == 0 {
		return nil
	}
	cloned := make(map[string]struct{}, len(marks))
	for key := range marks {
		cloned[key] = struct{}{}
	}
	return cloned
}

func cloneSchemaIndexValues[Key comparable](values map[Key][]NodeID) map[Key][]NodeID {
	cloned := make(map[Key][]NodeID, len(values))
	for key, nodeIDs := range values {
		cloned[key] = append([]NodeID(nil), nodeIDs...)
	}
	return cloned
}

// schemaInstallation selects preservation of unchanged live caches for a
// transaction, or complete replacement by a validated detached definition.
type schemaInstallation uint8

const (
	transactionSchemaInstallation schemaInstallation = iota
	acceptedDefinitionInstallation
)

// installSchemaLocked is the canonical schema publication owner.
// REQUIRES: caller holds sm.mu or exclusively owns sm.
func (sm *SchemaManager) installSchemaLocked(snapshot *SchemaManager, installation schemaInstallation) {
	// An ordinary schema transaction can replace the durable token order.
	// Readers must stop using the previous accepted-token snapshot before
	// installing it, just as they do for a definition replacement.
	sm.tokenSnapshot.Store(nil)
	defer sm.publishTokenSnapshotLocked(nil, nil)
	if installation == transactionSchemaInstallation {
		for key, constraint := range snapshot.uniqueConstraints {
			if existing := sm.uniqueConstraints[key]; existing != nil && existing.Name == constraint.Name {
				snapshot.uniqueConstraints[key] = existing
			}
		}
		for name, index := range snapshot.compositeIndexes {
			if existing := sm.compositeIndexes[name]; existing != nil && existing.Label == index.Label && reflect.DeepEqual(existing.Properties, index.Properties) {
				snapshot.compositeIndexes[name] = existing
			}
		}
		for name, index := range snapshot.rangeIndexes {
			if existing := sm.rangeIndexes[name]; existing != nil && sameRangeIndexDefinition(index, existing) {
				snapshot.rangeIndexes[name] = existing
			}
		}
	}
	sm.tokenOrder = snapshot.tokenOrder
	sm.uniqueConstraints = snapshot.uniqueConstraints
	sm.constraints = snapshot.constraints
	sm.constraintContracts = snapshot.constraintContracts
	sm.propertyTypeConstraints = snapshot.propertyTypeConstraints
	sm.compositeIndexes = snapshot.compositeIndexes
	sm.fulltextIndexes = snapshot.fulltextIndexes
	sm.vectorIndexes = snapshot.vectorIndexes
	sm.rangeIndexes = snapshot.rangeIndexes
	sm.lookupIndexes = snapshot.lookupIndexes
	if installation == transactionSchemaInstallation {
		// A temporal history index's runtime completeness marks are runtime
		// state of the same schema, not part of the definition a statement
		// stages: its isolated view is built from that definition and records
		// only the marks the statement itself established. Publication
		// therefore leaves each mark with its own constraint: a constraint the
		// statement created or re-created owns the mark it staged, an unchanged
		// one keeps its live mark, and a dropped one keeps none (#1126). A full
		// definition replacement carries no runtime index state, so it leaves
		// the live marks as they are.
		sm.temporalIndexCompleteKeys, sm.temporalIndexIncompleteKeys = sm.transferTemporalIndexMarksLocked(snapshot)
	}
	if installation == acceptedDefinitionInstallation {
		// A replacement carries the knowledge layer too. Its bindings were
		// compiled while the accepted definition loaded and are reused as that
		// validated pointer instead of being compiled again after the durable
		// write.
		sm.decayProfileBundles = snapshot.decayProfileBundles
		sm.decayProfileBindings = snapshot.decayProfileBindings
		sm.promotionProfiles = snapshot.promotionProfiles
		sm.promotionPolicies = snapshot.promotionPolicies
		sm.bindingTable = snapshot.bindingTable
	}
}

// transferTemporalIndexMarksLocked returns the runtime temporal-index
// completeness marks one schema transaction's publication leaves, merging the
// snapshot's staged marks with the live ones the statement did not touch. The
// staged marks are the statement's own (the constraints its view created); the
// live marks belong to the constraints it left alone. A mark therefore stays
// with its own constraint identity: a staged mark wins over an older mark of
// the same label and properties, an unchanged identity keeps its live mark, and
// an identity the statement dropped is absent from the snapshot and keeps none.
// REQUIRES: caller holds sm.mu.
func (sm *SchemaManager) transferTemporalIndexMarksLocked(staged *SchemaManager) (map[string]struct{}, map[string]struct{}) {
	complete := cloneTemporalIndexMarkSet(staged.temporalIndexCompleteKeys)
	incomplete := cloneTemporalIndexMarkSet(staged.temporalIndexIncompleteKeys)
	stagedKeys := make(map[string]struct{}, len(complete)+len(incomplete))
	for key := range complete {
		stagedKeys[key] = struct{}{}
	}
	for key := range incomplete {
		stagedKeys[key] = struct{}{}
	}
	// The identities the publication keeps: a temporal constraint the statement
	// still registers. Its own mark decides for the constraint it created or
	// re-created; otherwise the live mark of that identity carries over.
	published := make(map[string]struct{}, len(staged.constraints))
	for _, constraint := range staged.constraints {
		if constraint.Type == ConstraintTemporal {
			published[temporalIndexCompletenessKey(constraint)] = struct{}{}
		}
	}
	carry := func(live map[string]struct{}, into map[string]struct{}) map[string]struct{} {
		for key := range live {
			if _, own := stagedKeys[key]; own {
				continue
			}
			if _, kept := published[key]; !kept {
				continue
			}
			if into == nil {
				into = make(map[string]struct{}, len(live))
			}
			into[key] = struct{}{}
		}
		return into
	}
	return carry(sm.temporalIndexCompleteKeys, complete), carry(sm.temporalIndexIncompleteKeys, incomplete)
}

func sameRangeIndexDefinition(first, second *RangeIndex) bool {
	return first.Name == second.Name && first.Kind == second.Kind && first.Label == second.Label &&
		first.Property == second.Property && first.EntityType == second.EntityType &&
		first.OwningConstraint == second.OwningConstraint && reflect.DeepEqual(first.Properties, second.Properties)
}

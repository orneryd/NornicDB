package storage

import (
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
	if err := snapshot.ReplaceFromDefinition(tx.knowledgeSchemaDefinition); err != nil {
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
	// Carry retirements even when DROP and CREATE leave identical definitions.
	snapshot.vectorPropertyRetired = make(map[vectorDescriptor]uint64, len(view.vectorPropertyRetired))
	for property, generation := range view.vectorPropertyRetired {
		snapshot.vectorPropertyRetired[property] = generation
	}
	tx.schemaRuntime = snapshot
	return nil
}

func cloneSchemaIndexValues[Key comparable](values map[Key][]NodeID) map[Key][]NodeID {
	cloned := make(map[Key][]NodeID, len(values))
	for key, nodeIDs := range values {
		cloned[key] = append([]NodeID(nil), nodeIDs...)
	}
	return cloned
}

func (sm *SchemaManager) installTransactionSchemaLocked(snapshot *SchemaManager) {
	// An ordinary schema transaction can replace the durable token order.
	// Readers must stop using the previous accepted-token snapshot before
	// installing it, just as they do for a definition replacement.
	sm.tokenSnapshot.Store(nil)
	defer sm.publishTokenSnapshotLocked(nil, nil)
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
	sm.tokenOrder = snapshot.tokenOrder
	sm.uniqueConstraints = snapshot.uniqueConstraints
	sm.constraints = snapshot.constraints
	sm.constraintContracts = snapshot.constraintContracts
	sm.propertyTypeConstraints = snapshot.propertyTypeConstraints
	sm.compositeIndexes = snapshot.compositeIndexes
	sm.fulltextIndexes = snapshot.fulltextIndexes
	sm.retireChangedVectorIndexesLocked(sm.vectorIndexes, snapshot.vectorIndexes, snapshot.vectorPropertyRetired)
	sm.vectorIndexes = snapshot.vectorIndexes
	sm.rangeIndexes = snapshot.rangeIndexes
	sm.lookupIndexes = snapshot.lookupIndexes
}

func sameRangeIndexDefinition(first, second *RangeIndex) bool {
	return first.Name == second.Name && first.Kind == second.Kind && first.Label == second.Label &&
		first.Property == second.Property && first.EntityType == second.EntityType &&
		first.OwningConstraint == second.OwningConstraint && reflect.DeepEqual(first.Properties, second.Properties)
}

// Caller owns tx.mu (or exclusively owns the newly created transaction).
func (tx *BadgerTransaction) bindVectorWorkOriginLocked(origin uint64) {
	if tx.vectorOriginBound {
		return
	}
	tx.vectorWorkOrigin, tx.vectorOriginBound = origin, true
}

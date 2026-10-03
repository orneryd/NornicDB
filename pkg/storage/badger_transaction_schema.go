package storage

import (
	"fmt"
	"reflect"
)

// Schema returns an isolated view for an ordinary schema-write transaction.
// Ordinary schema changes cannot be mixed with entity writes. For example,
// obtain view with tx.Schema(), call view.AddPropertyIndex and backfill it,
// then tx.StageSchemaChanges() and tx.Commit(); rollback discards the changes.
// Native knowledge-policy-only updates can instead use KnowledgePolicySchema.
func (tx *BadgerTransaction) Schema() (*SchemaManager, error) {
	tx.mu.Lock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		tx.mu.Unlock()
		return nil, err
	}
	if len(tx.operations) > 0 {
		tx.mu.Unlock()
		return nil, mixedSchemaAndDataError(true)
	}
	tx.schemaTransaction = true
	tx.mu.Unlock()
	return tx.KnowledgePolicySchema()
}

func mixedSchemaAndDataError(schemaAfterData bool) error {
	message := "Tried to execute Write query after executing Schema modification"
	if schemaAfterData {
		message = "Tried to execute Schema modification after executing Write query"
	}
	return fmt.Errorf("Neo.ClientError.Transaction.ForbiddenDueToTransactionType: %s", message)
}

func (tx *BadgerTransaction) ensureDataWriteAllowedLocked() error {
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return err
	}
	if tx.schemaTransaction {
		return mixedSchemaAndDataError(false)
	}
	return nil
}

// StageSchemaChanges captures ordinary DDL and its runtime backfills for commit.
// Obtain the isolated view with Schema, apply and backfill the
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
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return err
	}
	if len(tx.operations) > 0 {
		return mixedSchemaAndDataError(true)
	}
	tx.schemaTransaction = true
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
	for key, index := range snapshot.propertyIndexes {
		if staged := view.propertyIndexes[key]; samePropertyIndex(index, staged) {
			staged.mu.RLock()
			index.values = cloneSchemaIndexValues(staged.values)
			index.keysDirty = true
			staged.mu.RUnlock()
		}
	}
	for name, index := range snapshot.compositeIndexes {
		if staged := view.compositeIndexes[name]; staged != nil && reflect.DeepEqual(index.Properties, staged.Properties) {
			staged.mu.RLock()
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

func samePropertyIndex(first, second *PropertyIndex) bool {
	return first != nil && second != nil && first.Name == second.Name && first.Label == second.Label && reflect.DeepEqual(first.Properties, second.Properties)
}

func (sm *SchemaManager) installTransactionSchemaLocked(snapshot *SchemaManager) {
	for key, constraint := range snapshot.uniqueConstraints {
		if existing := sm.uniqueConstraints[key]; existing != nil && existing.Name == constraint.Name {
			snapshot.uniqueConstraints[key] = existing
		}
	}
	for key, index := range snapshot.propertyIndexes {
		if existing := sm.propertyIndexes[key]; samePropertyIndex(index, existing) {
			snapshot.propertyIndexes[key] = existing
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
	sm.propertyIndexes = snapshot.propertyIndexes
	sm.compositeIndexes = snapshot.compositeIndexes
	sm.fulltextIndexes = snapshot.fulltextIndexes
	sm.vectorIndexes = snapshot.vectorIndexes
	sm.rangeIndexes = snapshot.rangeIndexes
	sm.lookupIndexes = snapshot.lookupIndexes
}

func sameRangeIndexDefinition(first, second *RangeIndex) bool {
	return first.Name == second.Name && first.Kind == second.Kind && first.Label == second.Label &&
		first.Property == second.Property && first.EntityType == second.EntityType &&
		first.OwningConstraint == second.OwningConstraint && reflect.DeepEqual(first.Properties, second.Properties)
}

package storage

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// Schema returns the transaction's isolated schema for purpose. Ordinary DDL
// admits its writer before creating the view and cannot mix with entity writes.
// Knowledge-policy reads claim no writer; persisting a policy change admits it.
// For example, view, err := tx.Schema(KnowledgePolicyChanges) obtains a view
// for view.CreatePromotionPolicy; rollback discards its persisted changes.
// DDL uses tx.Schema(OrdinarySchemaChanges), then StageSchemaChanges and Commit.
func (tx *BadgerTransaction) Schema(purpose SchemaPurpose) (*SchemaManager, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	operation := transactionRead
	switch purpose {
	case OrdinarySchemaChanges:
		operation = transactionSchemaWrite
	case KnowledgePolicyChanges:
	default:
		return nil, fmt.Errorf("invalid transaction schema purpose: %d", purpose)
	}
	if err := tx.ensureOperationAllowedLocked(operation); err != nil {
		return nil, err
	}
	if tx.knowledgeSchema != nil {
		return tx.knowledgeSchema, nil
	}
	if tx.namespace == "" {
		return nil, fmt.Errorf("knowledge policy schema requires a transaction namespace")
	}
	definition := &SchemaDefinition{Version: schemaDefinitionVersion}
	item, err := tx.badgerTx.Get(schemaKey(tx.namespace))
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return nil, err
	}
	if err == nil {
		if err := item.Value(func(value []byte) error { return DecodeSchemaDefinition(value, definition) }); err != nil {
			return nil, err
		}
	}
	view := NewSchemaManager()
	if err := view.ReplaceFromDefinition(definition); err != nil {
		return nil, err
	}
	view.SetPersister(func(updated *SchemaDefinition) error {
		tx.mu.Lock()
		defer tx.mu.Unlock()
		if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
			return err
		}
		// The change is admitted before it is marshalled or written, so a
		// refused admission leaves the view and the transaction unchanged.
		if err := tx.admitWriteLocked(); err != nil {
			return err
		}
		blob, err := EncodeSchemaDefinition(updated)
		if err != nil {
			return err
		}
		persisted := &SchemaDefinition{}
		if err := DecodeSchemaDefinition(blob, persisted); err != nil {
			return err
		}
		if err := tx.badgerTx.Set(schemaKey(tx.namespace), blob); err != nil {
			return err
		}
		tx.knowledgeSchemaDefinition = persisted
		tx.knowledgeSchemaDirty = true
		return nil
	})
	tx.knowledgeSchema = view
	return view, nil
}

// HasKnowledgePolicyChanges reports native schema mutations pending commit.
// Callers can combine it with OperationCount when deciding whether an otherwise
// entity-empty transaction can be discarded without committing.
func (tx *BadgerTransaction) HasKnowledgePolicyChanges() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.knowledgeSchemaDirty
}

func (tx *BadgerTransaction) publishKnowledgePolicySchemaLocked() func() {
	if !tx.knowledgeSchemaDirty {
		return nil
	}
	definition := tx.knowledgeSchemaDefinition
	snapshot := NewSchemaManager()
	if err := snapshot.ReplaceFromDefinition(definition); err != nil {
		return nil
	}
	committed := tx.engine.GetSchemaForNamespace(tx.namespace)
	committed.mu.Lock()
	if tx.schemaRuntime != nil {
		committed.installTransactionSchemaLocked(tx.schemaRuntime)
	}
	committed.decayProfileBundles = snapshot.decayProfileBundles
	committed.decayProfileBindings = snapshot.decayProfileBindings
	committed.promotionProfiles = snapshot.promotionProfiles
	committed.promotionPolicies = snapshot.promotionPolicies
	committed.rebuildBindingTableLocked()
	onChanged := committed.knowledgePolicyChanged
	committed.mu.Unlock()
	if tx.schemaRuntime != nil {
		return func() {
			committed.trackPendingPairs()
			if onChanged != nil {
				onChanged()
			}
		}
	}
	return onChanged
}

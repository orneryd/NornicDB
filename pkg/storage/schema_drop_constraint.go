package storage

import (
	"fmt"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// DropConstraint removes a constraint (by name) from the schema.
//
// The name may refer to a standard constraint, a property type constraint, or
// a constraint contract. Dropping a contract removes its metadata together with
// every primitive constraint compiled out of it (named <contract>__entry_NN),
// so SHOW CONSTRAINTS and SHOW CONSTRAINT CONTRACTS stay consistent. The drop
// is all-or-nothing: if persisting the new schema fails, everything removed is
// restored.
//
// Example:
//
//	// CREATE CONSTRAINT person_contract FOR (n:Person) REQUIRE { n.id IS UNIQUE ... }
//	err := schema.DropConstraint("person_contract") // removes the contract and person_contract__entry_01
func (sm *SchemaManager) DropConstraint(name string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var restore func()
	if contract, ok := sm.constraintContracts[name]; ok {
		restore = sm.dropConstraintContractLocked(contract)
	} else {
		r, err := sm.dropConstraintLocked(name)
		if err != nil {
			return err
		}
		restore = r
	}

	if sm.persist != nil {
		def := sm.exportDefinitionLocked()
		if err := sm.persist(def); err != nil {
			restore()
			return err
		}
	}

	return nil
}

// dropConstraintContractLocked removes a contract and the primitives compiled
// from it, returning a function that restores all of them. Compiled entries a
// user already dropped individually are skipped.
func (sm *SchemaManager) dropConstraintContractLocked(contract ConstraintContract) func() {
	restores := make([]func(), 0, len(contract.Entries))
	for idx, entry := range contract.Entries {
		if entry.Kind != ConstraintContractKindPrimitiveNode && entry.Kind != ConstraintContractKindPrimitiveRelationship {
			continue
		}
		if restore, err := sm.dropConstraintLocked(ConstraintContractEntryName(contract.Name, idx)); err == nil {
			restores = append(restores, restore)
		}
	}
	delete(sm.constraintContracts, contract.Name)
	return func() {
		for i := len(restores) - 1; i >= 0; i-- {
			restores[i]()
		}
		sm.constraintContracts[contract.Name] = contract
	}
}

// dropConstraintLocked removes one standard or property type constraint and
// the indexes it owns, returning a function that puts them back.
func (sm *SchemaManager) dropConstraintLocked(name string) (func(), error) {
	if c, ok := sm.constraints[name]; ok {
		var droppedUnique *UniqueConstraint
		var droppedUniqueKey string
		var droppedOwnedIndex *RangeIndex
		var droppedPropertyIndex *PropertyIndex

		delete(sm.constraints, name)

		if constraintPropertyIndexKey(c) != "" {
			if idx, ok := sm.arity1PropertyIndexLocked(c.Label, c.Properties[0]); ok && idx.OwningConstraint == name {
				droppedPropertyIndex = idx
				delete(sm.compositeIndexes, idx.Name)
			}
		}

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
				delete(sm.rangeIndexes, c.OwnedIndex)
			}
		}

		return func() {
			sm.constraints[name] = c
			if droppedUnique != nil {
				sm.uniqueConstraints[droppedUniqueKey] = droppedUnique
			}
			if droppedOwnedIndex != nil {
				sm.rangeIndexes[c.OwnedIndex] = droppedOwnedIndex
			}
			if droppedPropertyIndex != nil {
				sm.compositeIndexes[droppedPropertyIndex.Name] = droppedPropertyIndex
			}
		}, nil
	}
	if ptc, ok := sm.propertyTypeConstraints[name]; ok {
		delete(sm.propertyTypeConstraints, name)
		return func() { sm.propertyTypeConstraints[name] = ptc }, nil
	}
	return nil, localizedError(localization.StorageSchemaConstraintNotFound(name), nil)
}

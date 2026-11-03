package storage

import (
	"context"
	"fmt"
)

// ReplaceDefinitionDurable replaces every schema family with def as one
// accepted definition replacement and persists that replacement through the
// manager's configured persister.
//
// The replacement is accepted in three steps, in this order, so a definition
// that cannot be accepted is neither written nor published:
//
//  1. A detached prospective manager is built from def
//     (ReplaceFromDefinition), its derived runtime state (single-property
//     UNIQUE values and equality index contents) is rebuilt from engine's
//     stored entities, and every constraint family is validated against them.
//     The detached manager's knowledge-policy bindings are compiled while it
//     loads, so a definition referencing a missing profile is rejected here.
//  2. The accepted prospective definition is persisted through the same
//     namespace-bound persister every other schema mutation uses. A rejected
//     write returns its error and leaves the live manager, its runtime index
//     and UNIQUE caches, and the durable definition exactly as they were.
//  3. Only then is the prospective manager published into sm: every family,
//     the token order, the token snapshot and the compiled knowledge-policy
//     binding table are installed through installSchemaLocked, after which the
//     knowledge-policy changed callback runs with no schema lock held. The
//     compiled table is the one the prospective manager validated, not a
//     second compilation after the write.
//
// Persisting and publishing are admitted as one schema mutation: the
// publication gate is held from before the accepted definition is read until
// publication completes, so a native commit that publishes a schema definition
// cannot be overtaken in that interval. The admission honors ctx, so a caller
// whose context is canceled before it is admitted persists and publishes
// nothing.
//
// Cancellation: ctx owns detached preparation, including stored enumeration,
// constraint validation, and UNIQUE cache rebuilding. A canceled preparation
// returns before any durable write or live publication. Accepted installation
// completes under its schema mutation lease.
//
// Ownership: the prospective state is derived from engine's stored entities
// before that admission, so this is an offline administrative replacement. It
// does not serialize against live entity writes, and the caller must own the
// destination for the duration; adminimport applies a schema document with it
// after importing into a namespace it has just required to be empty. A manager
// with no configured persister (a memory engine, or a schema with no namespace
// binding) publishes the replacement in memory only, as every other schema
// mutation does.
// Both ctx and engine are required; preparation is never detached from a missing caller context.
//
// Example, after exclusive import into a namespace:
//
//	definition := &SchemaDefinition{PropertyIndexes: []SchemaPropertyIndexDef{{
//		Name: "doc_id", Label: "Document", Properties: []string{"id"},
//	}}}
//	err := target.GetSchema().ReplaceDefinitionDurable(ctx, target, definition)
func (sm *SchemaManager) ReplaceDefinitionDurable(ctx context.Context, engine Engine, def *SchemaDefinition) error {
	if sm == nil {
		return fmt.Errorf("schema: manager is required")
	}
	if def == nil {
		return fmt.Errorf("schema: definition is required")
	}
	if engine == nil {
		return fmt.Errorf("schema: engine is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	prospective := NewSchemaManager()
	if err := prospective.ReplaceFromDefinition(ctx, def); err != nil {
		return err
	}
	if err := rebuildSchemaDerivedStateForEngine(ctx, engine, prospective); err != nil {
		return err
	}
	if err := validateSchemaDefinitionForEngine(ctx, engine, prospective); err != nil {
		return err
	}

	definition, err := prospective.ExportDefinition(ctx)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	lease, admitErr := sm.beginSchemaMutation(ctx)
	if admitErr != nil {
		return admitErr
	}
	if persist := sm.persist; persist != nil {
		if err := persist(definition); err != nil {
			lease.release()
			return err
		}
	}
	sm.installSchemaLocked(prospective, acceptedDefinitionInstallation)
	onChanged := sm.knowledgePolicyChanged
	lease.release()

	// The published index and constraint pairs changed; the attached pending
	// write source is re-tracked after the schema lock is released.
	sm.trackPendingPairs()
	if onChanged != nil {
		onChanged()
	}
	return nil
}

// rebuildSchemaDerivedStateForEngine fills schema's derived runtime state
// (single-property UNIQUE values and equality index contents) from engine's
// stored entities, then marks the equality indexes filled.
//
// Every stored node is read, the ones the visibility layer hides included: the
// indexes are trusted to hold every matching node. The node stream honors ctx;
// the UNIQUE refresh and subsequent validation share that lifetime. The
// schema must be a manager this caller owns, such as an unaccepted
// replacement's detached manager.
func rebuildSchemaDerivedStateForEngine(ctx context.Context, engine Engine, schema *SchemaManager) error {
	if engine == nil || schema == nil {
		return nil
	}
	if err := RefreshUniqueConstraintValuesForEngine(ctx, engine, schema); err != nil {
		return err
	}
	if err := StreamStoredNodes(ctx, engine, StreamNodesOptions{}, func(node *Node) error {
		for _, label := range node.Labels {
			for _, idx := range schema.GetCompositeIndexesForLabel(label) {
				if idx == nil {
					continue
				}
				if err := idx.IndexNode(ctx, node.ID, node.Properties); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	schema.MarkPropertyIndexesFilled()
	return nil
}

// validateSchemaDefinitionForEngine checks every constraint family of an
// accepted definition against engine's stored entities, so a replacement the
// data cannot satisfy is rejected before it is persisted or published.
//
// The validators share the preparing caller's context.
func validateSchemaDefinitionForEngine(ctx context.Context, engine Engine, schema *SchemaManager) error {
	for _, constraint := range schema.GetAllConstraints() {
		if err := ValidateConstraintOnCreationForEngine(ctx, engine, constraint); err != nil {
			return err
		}
	}
	for _, typeConstraint := range schema.GetAllPropertyTypeConstraints() {
		if err := ValidatePropertyTypeConstraintOnCreationForEngine(ctx, engine, typeConstraint); err != nil {
			return err
		}
	}
	contracts, err := schema.GetAllConstraintContracts(ctx)
	if err != nil {
		return err
	}
	for _, contract := range contracts {
		if err := ValidateConstraintContractOnCreationForEngine(ctx, engine, contract); err != nil {
			return err
		}
	}
	return nil
}

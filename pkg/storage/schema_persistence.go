package storage

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
)

// SchemaDefinition is the persisted representation of NornicDB schema rules.
//
// It stores schema definitions and durable token positions, not
// derived/indexed data structures (like unique value maps), which are rebuilt
// from stored nodes/edges on startup.
//
// This mirrors Neo4j’s model:
//   - schema rules are durable metadata
//   - index contents can be rebuilt if needed
type SchemaDefinition struct {
	Version                int      `json:"version"`
	LabelTokens            []string `json:"label_tokens,omitempty"`
	RelationshipTypeTokens []string `json:"relationship_type_tokens,omitempty"`

	Constraints         []Constraint         `json:"constraints,omitempty"`
	ConstraintContracts []ConstraintContract `json:"constraint_contracts,omitempty"`

	PropertyTypeConstraints []PropertyTypeConstraint `json:"property_type_constraints,omitempty"`

	PropertyIndexes  []SchemaPropertyIndexDef  `json:"property_indexes,omitempty"`
	CompositeIndexes []SchemaCompositeIndexDef `json:"composite_indexes,omitempty"`
	FulltextIndexes  []FulltextIndex           `json:"fulltext_indexes,omitempty"`
	VectorIndexes    []VectorIndex             `json:"vector_indexes,omitempty"`
	RangeIndexes     []SchemaRangeIndexDef     `json:"range_indexes,omitempty"`
	// LookupIndexes are the token lookup indexes (schema_lookup_index.go).
	// Nil in a definition written before they were persisted, which reads
	// as the default ones.
	LookupIndexes *[]SchemaLookupIndexDef `json:"lookup_indexes,omitempty"`

	DecayProfileBundles  []knowledgepolicy.DecayProfileBundle  `json:"decay_profile_bundles,omitempty"`
	DecayProfileBindings []knowledgepolicy.DecayProfileBinding `json:"decay_profile_bindings,omitempty"`
	PromotionProfiles    []knowledgepolicy.PromotionProfileDef `json:"promotion_profiles,omitempty"`
	PromotionPolicies    []knowledgepolicy.PromotionPolicyDef  `json:"promotion_policies,omitempty"`
}

type SchemaPropertyIndexDef struct {
	Name       string   `json:"name"`
	Label      string   `json:"label"`
	Properties []string `json:"properties"`
}

type SchemaCompositeIndexDef struct {
	Name       string   `json:"name"`
	Label      string   `json:"label"`
	Properties []string `json:"properties"`
}

type SchemaRangeIndexDef struct {
	Name             string               `json:"name"`
	Kind             IndexKind            `json:"kind,omitempty"`
	Label            string               `json:"label"`
	Property         string               `json:"property"`
	Properties       []string             `json:"properties,omitempty"`
	EntityType       ConstraintEntityType `json:"entity_type,omitempty"`
	OwningConstraint string               `json:"owning_constraint,omitempty"`
}

const schemaDefinitionVersion = 1

// ExportDefinition returns a stable, persisted representation of the schema.
// The returned object is safe to serialize and does not include runtime caches.
func (sm *SchemaManager) ExportDefinition() *SchemaDefinition {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.exportDefinitionLocked()
}

// exportDefinitionLocked builds a persisted schema definition from the current state.
// REQUIRES: caller holds either sm.mu.RLock() or sm.mu.Lock().
func (sm *SchemaManager) exportDefinitionLocked() *SchemaDefinition {
	def, _ := sm.exportDefinitionLockedWithContext(context.Background())
	return def
}

func (sm *SchemaManager) exportDefinitionLockedWithContext(ctx context.Context) (*SchemaDefinition, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var err error
	def := &SchemaDefinition{Version: schemaDefinitionVersion}
	if len(sm.tokenOrder.labels) > 0 {
		def.LabelTokens, err = copySliceWithContext(ctx, sm.tokenOrder.labels)
		if err != nil {
			return nil, err
		}
	}
	if len(sm.tokenOrder.relationships) > 0 {
		def.RelationshipTypeTokens, err = copySliceWithContext(ctx, sm.tokenOrder.relationships)
		if err != nil {
			return nil, err
		}
	}
	def.LookupIndexes, err = sm.exportLookupIndexesLockedWithContext(ctx)
	if err != nil {
		return nil, err
	}

	// Constraints (store as a sorted slice for stable persistence).
	if len(sm.constraints) > 0 {
		def.Constraints = make([]Constraint, 0, len(sm.constraints))
		for _, c := range sm.constraints {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			// Copy properties slice to avoid aliasing.
			props, err := copySliceWithContext(ctx, c.Properties)
			if err != nil {
				return nil, err
			}
			if props == nil {
				props = make([]string, 0)
			}
			var allowedVals []interface{}
			if len(c.AllowedValues) > 0 {
				allowedVals, err = copySliceWithContext(ctx, c.AllowedValues)
				if err != nil {
					return nil, err
				}
			}
			def.Constraints = append(def.Constraints, Constraint{
				Name:          c.Name,
				Type:          c.Type,
				EntityType:    c.EntityType,
				Label:         c.Label,
				Properties:    props,
				OwnedIndex:    c.OwnedIndex,
				AllowedValues: allowedVals,
				MaxCount:      c.MaxCount,
				Direction:     c.Direction,
				SourceLabel:   c.SourceLabel,
				TargetLabel:   c.TargetLabel,
				PolicyMode:    c.PolicyMode,
			})
		}
		if err := sortIndexCandidatesWithContext(ctx, def.Constraints, func(left, right Constraint) bool {
			if left.Label != right.Label {
				return left.Label < right.Label
			}
			return left.Name < right.Name
		}); err != nil {
			return nil, err
		}
	}

	if len(sm.constraintContracts) > 0 {
		def.ConstraintContracts = make([]ConstraintContract, 0, len(sm.constraintContracts))
		for _, contract := range sm.constraintContracts {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			cloned, err := cloneConstraintContractWithContext(ctx, contract)
			if err != nil {
				return nil, err
			}
			def.ConstraintContracts = append(def.ConstraintContracts, cloned)
		}
		if err := sortIndexCandidatesWithContext(ctx, def.ConstraintContracts, func(left, right ConstraintContract) bool {
			return left.Name < right.Name
		}); err != nil {
			return nil, err
		}
	}

	// Property type constraints.
	if len(sm.propertyTypeConstraints) > 0 {
		def.PropertyTypeConstraints = make([]PropertyTypeConstraint, 0, len(sm.propertyTypeConstraints))
		for _, c := range sm.propertyTypeConstraints {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			def.PropertyTypeConstraints = append(def.PropertyTypeConstraints, PropertyTypeConstraint{
				Name:         c.Name,
				EntityType:   c.EntityType,
				Label:        c.Label,
				Property:     c.Property,
				ExpectedType: c.ExpectedType,
			})
		}
		if err := sortIndexCandidatesWithContext(ctx, def.PropertyTypeConstraints, func(left, right PropertyTypeConstraint) bool {
			if left.Label != right.Label {
				return left.Label < right.Label
			}
			return left.Name < right.Name
		}); err != nil {
			return nil, err
		}
	}

	// Equality indexes split into the two persisted arrays for on-disk
	// compatibility: arity-1 (PropertyIndexes) and arity-N (CompositeIndexes).
	// Both load back into the single compositeIndexes map.
	if len(sm.compositeIndexes) > 0 {
		for _, idx := range sm.compositeIndexes {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if idx.OwningConstraint != "" {
				continue // derived from its constraint
			}
			props, err := copySliceWithContext(ctx, idx.Properties)
			if err != nil {
				return nil, err
			}
			if props == nil {
				props = make([]string, 0)
			}
			if len(props) == 1 {
				def.PropertyIndexes = append(def.PropertyIndexes, SchemaPropertyIndexDef{
					Name:       idx.Name,
					Label:      idx.Label,
					Properties: props,
				})
				continue
			}
			def.CompositeIndexes = append(def.CompositeIndexes, SchemaCompositeIndexDef{
				Name:       idx.Name,
				Label:      idx.Label,
				Properties: props,
			})
		}
		if err := sortIndexCandidatesWithContext(ctx, def.PropertyIndexes, func(left, right SchemaPropertyIndexDef) bool {
			if left.Label != right.Label {
				return left.Label < right.Label
			}
			return left.Name < right.Name
		}); err != nil {
			return nil, err
		}
		if err := sortIndexCandidatesWithContext(ctx, def.CompositeIndexes, func(left, right SchemaCompositeIndexDef) bool {
			if left.Label != right.Label {
				return left.Label < right.Label
			}
			return left.Name < right.Name
		}); err != nil {
			return nil, err
		}
	}

	// Fulltext indexes.
	if len(sm.fulltextIndexes) > 0 {
		def.FulltextIndexes = make([]FulltextIndex, 0, len(sm.fulltextIndexes))
		for _, idx := range sm.fulltextIndexes {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			labels, err := copySliceWithContext(ctx, idx.Labels)
			if err != nil {
				return nil, err
			}
			if labels == nil {
				labels = make([]string, 0)
			}
			relTypes, err := copySliceWithContext(ctx, idx.RelationshipTypes)
			if err != nil {
				return nil, err
			}
			if relTypes == nil {
				relTypes = make([]string, 0)
			}
			props, err := copySliceWithContext(ctx, idx.Properties)
			if err != nil {
				return nil, err
			}
			if props == nil {
				props = make([]string, 0)
			}
			def.FulltextIndexes = append(def.FulltextIndexes, FulltextIndex{
				Name:              idx.Name,
				Labels:            labels,
				RelationshipTypes: relTypes,
				Properties:        props,
			})
		}
		if err := sortIndexCandidatesWithContext(ctx, def.FulltextIndexes, func(left, right FulltextIndex) bool {
			return left.Name < right.Name
		}); err != nil {
			return nil, err
		}
	}

	// Vector indexes.
	if len(sm.vectorIndexes) > 0 {
		def.VectorIndexes = make([]VectorIndex, 0, len(sm.vectorIndexes))
		for _, idx := range sm.vectorIndexes {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			def.VectorIndexes = append(def.VectorIndexes, VectorIndex{
				Name:           idx.Name,
				Label:          idx.Label,
				Property:       idx.Property,
				Dimensions:     idx.Dimensions,
				SimilarityFunc: idx.SimilarityFunc,
				EntityType:     defaultConstraintEntityType(idx.EntityType),
			})
		}
		if err := sortIndexCandidatesWithContext(ctx, def.VectorIndexes, func(left, right VectorIndex) bool {
			return left.Name < right.Name
		}); err != nil {
			return nil, err
		}
	}

	// Range indexes.
	if len(sm.rangeIndexes) > 0 {
		def.RangeIndexes = make([]SchemaRangeIndexDef, 0, len(sm.rangeIndexes))
		for _, idx := range sm.rangeIndexes {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			properties, err := copySliceWithContext(ctx, idx.Properties)
			if err != nil {
				return nil, err
			}
			def.RangeIndexes = append(def.RangeIndexes, SchemaRangeIndexDef{
				Name:             idx.Name,
				Kind:             idx.Kind,
				Label:            idx.Label,
				Property:         idx.Property,
				Properties:       properties,
				EntityType:       idx.EntityType,
				OwningConstraint: idx.OwningConstraint,
			})
		}
		if err := sortIndexCandidatesWithContext(ctx, def.RangeIndexes, func(left, right SchemaRangeIndexDef) bool {
			return left.Name < right.Name
		}); err != nil {
			return nil, err
		}
	}

	// Decay profile bundles.
	if len(sm.decayProfileBundles) > 0 {
		def.DecayProfileBundles = make([]knowledgepolicy.DecayProfileBundle, 0, len(sm.decayProfileBundles))
		for _, b := range sm.decayProfileBundles {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			def.DecayProfileBundles = append(def.DecayProfileBundles, *b)
		}
		if err := sortIndexCandidatesWithContext(ctx, def.DecayProfileBundles, func(left, right knowledgepolicy.DecayProfileBundle) bool {
			return left.Name < right.Name
		}); err != nil {
			return nil, err
		}
	}

	// Decay profile bindings.
	if len(sm.decayProfileBindings) > 0 {
		def.DecayProfileBindings = make([]knowledgepolicy.DecayProfileBinding, 0, len(sm.decayProfileBindings))
		for _, b := range sm.decayProfileBindings {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			bc := *b
			if len(bc.TargetLabels) > 0 {
				labels, err := copySliceWithContext(ctx, bc.TargetLabels)
				if err != nil {
					return nil, err
				}
				if labels == nil {
					labels = make([]string, 0)
				}
				bc.TargetLabels = labels
			}
			if len(bc.PropertyRules) > 0 {
				rules, err := copySliceWithContext(ctx, bc.PropertyRules)
				if err != nil {
					return nil, err
				}
				if rules == nil {
					rules = make([]knowledgepolicy.DecayProfilePropertyRule, 0)
				}
				bc.PropertyRules = rules
			}
			def.DecayProfileBindings = append(def.DecayProfileBindings, bc)
		}
		if err := sortIndexCandidatesWithContext(ctx, def.DecayProfileBindings, func(left, right knowledgepolicy.DecayProfileBinding) bool {
			return left.Name < right.Name
		}); err != nil {
			return nil, err
		}
	}

	// Promotion profiles.
	if len(sm.promotionProfiles) > 0 {
		def.PromotionProfiles = make([]knowledgepolicy.PromotionProfileDef, 0, len(sm.promotionProfiles))
		for _, p := range sm.promotionProfiles {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			def.PromotionProfiles = append(def.PromotionProfiles, *p)
		}
		if err := sortIndexCandidatesWithContext(ctx, def.PromotionProfiles, func(left, right knowledgepolicy.PromotionProfileDef) bool {
			return left.Name < right.Name
		}); err != nil {
			return nil, err
		}
	}

	// Promotion policies.
	if len(sm.promotionPolicies) > 0 {
		def.PromotionPolicies = make([]knowledgepolicy.PromotionPolicyDef, 0, len(sm.promotionPolicies))
		for _, p := range sm.promotionPolicies {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			pc := *p
			if len(pc.TargetLabels) > 0 {
				labels, err := copySliceWithContext(ctx, pc.TargetLabels)
				if err != nil {
					return nil, err
				}
				if labels == nil {
					labels = make([]string, 0)
				}
				pc.TargetLabels = labels
			}
			if len(pc.WhenClauses) > 0 {
				clauses, err := copySliceWithContext(ctx, pc.WhenClauses)
				if err != nil {
					return nil, err
				}
				if clauses == nil {
					clauses = make([]knowledgepolicy.PromotionPolicyWhenClause, 0)
				}
				pc.WhenClauses = clauses
			}
			def.PromotionPolicies = append(def.PromotionPolicies, pc)
		}
		if err := sortIndexCandidatesWithContext(ctx, def.PromotionPolicies, func(left, right knowledgepolicy.PromotionPolicyDef) bool {
			return left.Name < right.Name
		}); err != nil {
			return nil, err
		}
	}

	return def, ctx.Err()
}

// ReplaceFromDefinition replaces the in-memory schema contents with the given definition.
//
// This does NOT persist anything, and it intentionally discards runtime caches
// (unique value maps, index maps, etc.). Those must be rebuilt from data.
func (sm *SchemaManager) ReplaceFromDefinition(def *SchemaDefinition) error {
	defer sm.trackPendingPairs()
	if def == nil {
		return nil
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.replaceFromDefinitionLocked(def)
}

func (sm *SchemaManager) replaceFromDefinitionLocked(def *SchemaDefinition) error {
	if def == nil {
		return nil
	}
	// A replacement may remove names, so no old snapshot may be used once the
	// replacement starts. Publish the final accepted order while still locked.
	sm.tokenSnapshot.Store(nil)
	defer sm.publishTokenSnapshotLocked(nil, nil)

	// Reset all collections.
	sm.uniqueConstraints = make(map[string]*UniqueConstraint)
	sm.constraints = make(map[string]Constraint)
	sm.constraintContracts = make(map[string]ConstraintContract)
	sm.propertyTypeConstraints = make(map[string]PropertyTypeConstraint)
	sm.compositeIndexes = make(map[string]*CompositeIndex)
	sm.fulltextIndexes = make(map[string]*FulltextIndex)
	sm.vectorIndexes = make(map[string]*VectorIndex)
	sm.rangeIndexes = make(map[string]*RangeIndex)
	sm.lookupIndexes = importLookupIndexes(def.LookupIndexes)
	sm.tokenOrder = newSchemaTokenOrder(def.LabelTokens, def.RelationshipTypeTokens)

	// Constraints.
	for _, c := range def.Constraints {
		props := make([]string, len(c.Properties))
		copy(props, c.Properties)
		var allowedVals []interface{}
		if len(c.AllowedValues) > 0 {
			allowedVals = make([]interface{}, len(c.AllowedValues))
			copy(allowedVals, c.AllowedValues)
		}
		cc := Constraint{
			Name:          c.Name,
			Type:          c.Type,
			EntityType:    c.EntityType,
			Label:         c.Label,
			Properties:    props,
			OwnedIndex:    c.OwnedIndex,
			AllowedValues: allowedVals,
			MaxCount:      c.MaxCount,
			Direction:     c.Direction,
			SourceLabel:   c.SourceLabel,
			TargetLabel:   c.TargetLabel,
			PolicyMode:    c.PolicyMode,
		}
		sm.constraints[cc.Name] = cc

		if cc.Type == ConstraintUnique && len(cc.Properties) == 1 {
			key := cc.Label + ":" + cc.Properties[0]
			sm.uniqueConstraints[key] = &UniqueConstraint{
				Name:     cc.Name,
				Label:    cc.Label,
				Property: cc.Properties[0],
				values:   make(map[interface{}]NodeID),
			}
		}
	}

	for _, contract := range def.ConstraintContracts {
		sm.constraintContracts[contract.Name] = cloneConstraintContract(contract)
	}

	// Property type constraints.
	for _, c := range def.PropertyTypeConstraints {
		sm.propertyTypeConstraints[c.Name] = PropertyTypeConstraint{
			Name:         c.Name,
			EntityType:   c.EntityType,
			Label:        c.Label,
			Property:     c.Property,
			ExpectedType: c.ExpectedType,
		}
	}

	// Equality indexes load into the single compositeIndexes map, keyed by
	// name: PropertyIndexes (arity-1) and CompositeIndexes (arity-N).
	for _, idx := range def.PropertyIndexes {
		props := make([]string, len(idx.Properties))
		copy(props, idx.Properties)
		if len(props) == 0 {
			continue
		}
		sm.compositeIndexes[idx.Name] = &CompositeIndex{
			Name:       idx.Name,
			Label:      idx.Label,
			Properties: props,
			values:     make(map[interface{}][]NodeID),
		}
	}

	// A constraint's own equality index (#875), unless an index of its own
	// already covers the property.
	for _, c := range sm.constraints {
		sm.addConstraintPropertyIndexLocked(c)
	}

	for _, idx := range def.CompositeIndexes {
		props := make([]string, len(idx.Properties))
		copy(props, idx.Properties)
		if len(props) < 2 {
			continue
		}
		sm.compositeIndexes[idx.Name] = &CompositeIndex{
			Name:        idx.Name,
			Label:       idx.Label,
			Properties:  props,
			values:      make(map[interface{}][]NodeID),
			fullIndex:   make(map[string][]NodeID),
			prefixIndex: make(map[string][]NodeID),
		}
	}

	// Fulltext indexes. RelationshipTypes is omitempty in the on-disk
	// JSON; databases written by older binaries (no relationship_types
	// field) load with relTypes == nil, which the runtime treats as
	// "node-scoped index" — backwards-compatible.
	for _, idx := range def.FulltextIndexes {
		labels := make([]string, len(idx.Labels))
		copy(labels, idx.Labels)
		relTypes := make([]string, len(idx.RelationshipTypes))
		copy(relTypes, idx.RelationshipTypes)
		props := make([]string, len(idx.Properties))
		copy(props, idx.Properties)
		sm.fulltextIndexes[idx.Name] = &FulltextIndex{
			Name:              idx.Name,
			Labels:            labels,
			RelationshipTypes: relTypes,
			Properties:        props,
		}
	}

	// Vector indexes.
	for _, idx := range def.VectorIndexes {
		sm.vectorIndexes[idx.Name] = &VectorIndex{
			Name:           idx.Name,
			Label:          idx.Label,
			Property:       idx.Property,
			Dimensions:     idx.Dimensions,
			SimilarityFunc: idx.SimilarityFunc,
			EntityType:     defaultConstraintEntityType(idx.EntityType),
		}
	}

	// Range indexes.
	for _, idx := range def.RangeIndexes {
		properties, err := copySliceWithContext(context.Background(), idx.Properties)
		if err != nil {
			return err
		}
		sm.rangeIndexes[idx.Name] = &RangeIndex{
			Name:             idx.Name,
			Kind:             idx.Kind,
			Label:            idx.Label,
			Property:         idx.Property,
			Properties:       properties,
			EntityType:       idx.EntityType,
			OwningConstraint: idx.OwningConstraint,
			entries:          make([]rangeEntry, 0),
			nodeValue:        make(map[NodeID]float64),
		}
	}

	// Knowledge-layer scoring objects.
	sm.decayProfileBundles = make(map[string]*knowledgepolicy.DecayProfileBundle)
	sm.decayProfileBindings = make(map[string]*knowledgepolicy.DecayProfileBinding)
	sm.promotionProfiles = make(map[string]*knowledgepolicy.PromotionProfileDef)
	sm.promotionPolicies = make(map[string]*knowledgepolicy.PromotionPolicyDef)

	for _, b := range def.DecayProfileBundles {
		bc := b
		sm.decayProfileBundles[bc.Name] = &bc
	}
	for _, b := range def.DecayProfileBindings {
		bc := b
		if len(bc.TargetLabels) > 0 {
			labels := make([]string, len(bc.TargetLabels))
			copy(labels, bc.TargetLabels)
			bc.TargetLabels = labels
		}
		if len(bc.PropertyRules) > 0 {
			rules := make([]knowledgepolicy.DecayProfilePropertyRule, len(bc.PropertyRules))
			copy(rules, bc.PropertyRules)
			bc.PropertyRules = rules
		}
		sm.decayProfileBindings[bc.Name] = &bc
	}
	for _, p := range def.PromotionProfiles {
		pc := p
		sm.promotionProfiles[pc.Name] = &pc
	}
	for _, p := range def.PromotionPolicies {
		pc := p
		if len(pc.TargetLabels) > 0 {
			labels := make([]string, len(pc.TargetLabels))
			copy(labels, pc.TargetLabels)
			pc.TargetLabels = labels
		}
		if len(pc.WhenClauses) > 0 {
			clauses := make([]knowledgepolicy.PromotionPolicyWhenClause, len(pc.WhenClauses))
			copy(clauses, pc.WhenClauses)
			pc.WhenClauses = clauses
		}
		sm.promotionPolicies[pc.Name] = &pc
	}

	sm.rebuildBindingTableLocked()

	return nil
}

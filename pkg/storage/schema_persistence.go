package storage

import (
	"sort"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
)

// SchemaDefinition is the persisted representation of NornicDB schema rules.
//
// It stores schema *definitions only* (constraints and index definitions), not
// derived/indexed data structures (like unique value maps), which are rebuilt
// from stored nodes/edges on startup.
//
// This mirrors Neo4j’s model:
//   - schema rules are durable metadata
//   - index contents can be rebuilt if needed
type SchemaDefinition struct {
	Version int `json:"version"`

	Constraints         []Constraint         `json:"constraints,omitempty"`
	ConstraintContracts []ConstraintContract `json:"constraint_contracts,omitempty"`

	PropertyTypeConstraints []PropertyTypeConstraint `json:"property_type_constraints,omitempty"`

	PropertyIndexes  []SchemaPropertyIndexDef  `json:"property_indexes,omitempty"`
	CompositeIndexes []SchemaCompositeIndexDef `json:"composite_indexes,omitempty"`
	FulltextIndexes  []FulltextIndex           `json:"fulltext_indexes,omitempty"`
	VectorIndexes    []VectorIndex             `json:"vector_indexes,omitempty"`
	RangeIndexes     []SchemaRangeIndexDef     `json:"range_indexes,omitempty"`

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
	def := &SchemaDefinition{Version: schemaDefinitionVersion}

	// Constraints (store as a sorted slice for stable persistence).
	if len(sm.constraints) > 0 {
		def.Constraints = make([]Constraint, 0, len(sm.constraints))
		for _, c := range sm.constraints {
			// Copy properties slice to avoid aliasing.
			props := make([]string, len(c.Properties))
			copy(props, c.Properties)
			var allowedVals []interface{}
			if len(c.AllowedValues) > 0 {
				allowedVals = make([]interface{}, len(c.AllowedValues))
				copy(allowedVals, c.AllowedValues)
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
		sort.Slice(def.Constraints, func(i, j int) bool {
			if def.Constraints[i].Label != def.Constraints[j].Label {
				return def.Constraints[i].Label < def.Constraints[j].Label
			}
			return def.Constraints[i].Name < def.Constraints[j].Name
		})
	}

	if len(sm.constraintContracts) > 0 {
		def.ConstraintContracts = make([]ConstraintContract, 0, len(sm.constraintContracts))
		for _, contract := range sm.constraintContracts {
			def.ConstraintContracts = append(def.ConstraintContracts, cloneConstraintContract(contract))
		}
		sort.Slice(def.ConstraintContracts, func(i, j int) bool {
			return def.ConstraintContracts[i].Name < def.ConstraintContracts[j].Name
		})
	}

	// Property type constraints.
	if len(sm.propertyTypeConstraints) > 0 {
		def.PropertyTypeConstraints = make([]PropertyTypeConstraint, 0, len(sm.propertyTypeConstraints))
		for _, c := range sm.propertyTypeConstraints {
			def.PropertyTypeConstraints = append(def.PropertyTypeConstraints, PropertyTypeConstraint{
				Name:         c.Name,
				EntityType:   c.EntityType,
				Label:        c.Label,
				Property:     c.Property,
				ExpectedType: c.ExpectedType,
			})
		}
		sort.Slice(def.PropertyTypeConstraints, func(i, j int) bool {
			if def.PropertyTypeConstraints[i].Label != def.PropertyTypeConstraints[j].Label {
				return def.PropertyTypeConstraints[i].Label < def.PropertyTypeConstraints[j].Label
			}
			return def.PropertyTypeConstraints[i].Name < def.PropertyTypeConstraints[j].Name
		})
	}

	// Property indexes.
	if len(sm.propertyIndexes) > 0 {
		def.PropertyIndexes = make([]SchemaPropertyIndexDef, 0, len(sm.propertyIndexes))
		for _, idx := range sm.propertyIndexes {
			props := make([]string, len(idx.Properties))
			copy(props, idx.Properties)
			def.PropertyIndexes = append(def.PropertyIndexes, SchemaPropertyIndexDef{
				Name:       idx.Name,
				Label:      idx.Label,
				Properties: props,
			})
		}
		sort.Slice(def.PropertyIndexes, func(i, j int) bool {
			if def.PropertyIndexes[i].Label != def.PropertyIndexes[j].Label {
				return def.PropertyIndexes[i].Label < def.PropertyIndexes[j].Label
			}
			return def.PropertyIndexes[i].Name < def.PropertyIndexes[j].Name
		})
	}

	// Composite indexes.
	if len(sm.compositeIndexes) > 0 {
		def.CompositeIndexes = make([]SchemaCompositeIndexDef, 0, len(sm.compositeIndexes))
		for _, idx := range sm.compositeIndexes {
			props := make([]string, len(idx.Properties))
			copy(props, idx.Properties)
			def.CompositeIndexes = append(def.CompositeIndexes, SchemaCompositeIndexDef{
				Name:       idx.Name,
				Label:      idx.Label,
				Properties: props,
			})
		}
		sort.Slice(def.CompositeIndexes, func(i, j int) bool {
			if def.CompositeIndexes[i].Label != def.CompositeIndexes[j].Label {
				return def.CompositeIndexes[i].Label < def.CompositeIndexes[j].Label
			}
			return def.CompositeIndexes[i].Name < def.CompositeIndexes[j].Name
		})
	}

	// Fulltext indexes.
	if len(sm.fulltextIndexes) > 0 {
		def.FulltextIndexes = make([]FulltextIndex, 0, len(sm.fulltextIndexes))
		for _, idx := range sm.fulltextIndexes {
			labels := make([]string, len(idx.Labels))
			copy(labels, idx.Labels)
			props := make([]string, len(idx.Properties))
			copy(props, idx.Properties)
			def.FulltextIndexes = append(def.FulltextIndexes, FulltextIndex{
				Name:       idx.Name,
				Labels:     labels,
				Properties: props,
			})
		}
		sort.Slice(def.FulltextIndexes, func(i, j int) bool {
			return def.FulltextIndexes[i].Name < def.FulltextIndexes[j].Name
		})
	}

	// Vector indexes.
	if len(sm.vectorIndexes) > 0 {
		def.VectorIndexes = make([]VectorIndex, 0, len(sm.vectorIndexes))
		for _, idx := range sm.vectorIndexes {
			def.VectorIndexes = append(def.VectorIndexes, VectorIndex{
				Name:           idx.Name,
				Label:          idx.Label,
				Property:       idx.Property,
				Dimensions:     idx.Dimensions,
				SimilarityFunc: idx.SimilarityFunc,
			})
		}
		sort.Slice(def.VectorIndexes, func(i, j int) bool {
			return def.VectorIndexes[i].Name < def.VectorIndexes[j].Name
		})
	}

	// Range indexes.
	if len(sm.rangeIndexes) > 0 {
		def.RangeIndexes = make([]SchemaRangeIndexDef, 0, len(sm.rangeIndexes))
		for _, idx := range sm.rangeIndexes {
			def.RangeIndexes = append(def.RangeIndexes, SchemaRangeIndexDef{
				Name:             idx.Name,
				Label:            idx.Label,
				Property:         idx.Property,
				Properties:       idx.Properties,
				EntityType:       idx.EntityType,
				OwningConstraint: idx.OwningConstraint,
			})
		}
		sort.Slice(def.RangeIndexes, func(i, j int) bool {
			return def.RangeIndexes[i].Name < def.RangeIndexes[j].Name
		})
	}

	// Decay profile bundles.
	if len(sm.decayProfileBundles) > 0 {
		def.DecayProfileBundles = make([]knowledgepolicy.DecayProfileBundle, 0, len(sm.decayProfileBundles))
		for _, b := range sm.decayProfileBundles {
			def.DecayProfileBundles = append(def.DecayProfileBundles, *b)
		}
		sort.Slice(def.DecayProfileBundles, func(i, j int) bool {
			return def.DecayProfileBundles[i].Name < def.DecayProfileBundles[j].Name
		})
	}

	// Decay profile bindings.
	if len(sm.decayProfileBindings) > 0 {
		def.DecayProfileBindings = make([]knowledgepolicy.DecayProfileBinding, 0, len(sm.decayProfileBindings))
		for _, b := range sm.decayProfileBindings {
			bc := *b
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
			def.DecayProfileBindings = append(def.DecayProfileBindings, bc)
		}
		sort.Slice(def.DecayProfileBindings, func(i, j int) bool {
			return def.DecayProfileBindings[i].Name < def.DecayProfileBindings[j].Name
		})
	}

	// Promotion profiles.
	if len(sm.promotionProfiles) > 0 {
		def.PromotionProfiles = make([]knowledgepolicy.PromotionProfileDef, 0, len(sm.promotionProfiles))
		for _, p := range sm.promotionProfiles {
			def.PromotionProfiles = append(def.PromotionProfiles, *p)
		}
		sort.Slice(def.PromotionProfiles, func(i, j int) bool {
			return def.PromotionProfiles[i].Name < def.PromotionProfiles[j].Name
		})
	}

	// Promotion policies.
	if len(sm.promotionPolicies) > 0 {
		def.PromotionPolicies = make([]knowledgepolicy.PromotionPolicyDef, 0, len(sm.promotionPolicies))
		for _, p := range sm.promotionPolicies {
			pc := *p
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
			def.PromotionPolicies = append(def.PromotionPolicies, pc)
		}
		sort.Slice(def.PromotionPolicies, func(i, j int) bool {
			return def.PromotionPolicies[i].Name < def.PromotionPolicies[j].Name
		})
	}

	return def
}

// ReplaceFromDefinition replaces the in-memory schema contents with the given definition.
//
// This does NOT persist anything, and it intentionally discards runtime caches
// (unique value maps, index maps, etc.). Those must be rebuilt from data.
func (sm *SchemaManager) ReplaceFromDefinition(def *SchemaDefinition) error {
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

	// Reset all collections.
	sm.uniqueConstraints = make(map[string]*UniqueConstraint)
	sm.constraints = make(map[string]Constraint)
	sm.constraintContracts = make(map[string]ConstraintContract)
	sm.propertyTypeConstraints = make(map[string]PropertyTypeConstraint)
	sm.propertyIndexes = make(map[string]*PropertyIndex)
	sm.compositeIndexes = make(map[string]*CompositeIndex)
	sm.fulltextIndexes = make(map[string]*FulltextIndex)
	sm.vectorIndexes = make(map[string]*VectorIndex)
	sm.rangeIndexes = make(map[string]*RangeIndex)

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

	// Property indexes.
	for _, idx := range def.PropertyIndexes {
		props := make([]string, len(idx.Properties))
		copy(props, idx.Properties)
		if len(props) == 0 {
			continue
		}
		key := idx.Label + ":" + props[0]
		sm.propertyIndexes[key] = &PropertyIndex{
			Name:       idx.Name,
			Label:      idx.Label,
			Properties: props,
			values:     make(map[interface{}][]NodeID),
		}
	}

	// Composite indexes.
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
			fullIndex:   make(map[string][]NodeID),
			prefixIndex: make(map[string][]NodeID),
		}
	}

	// Fulltext indexes.
	for _, idx := range def.FulltextIndexes {
		labels := make([]string, len(idx.Labels))
		copy(labels, idx.Labels)
		props := make([]string, len(idx.Properties))
		copy(props, idx.Properties)
		sm.fulltextIndexes[idx.Name] = &FulltextIndex{
			Name:       idx.Name,
			Labels:     labels,
			Properties: props,
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
		}
	}

	// Range indexes.
	for _, idx := range def.RangeIndexes {
		sm.rangeIndexes[idx.Name] = &RangeIndex{
			Name:             idx.Name,
			Label:            idx.Label,
			Property:         idx.Property,
			Properties:       idx.Properties,
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

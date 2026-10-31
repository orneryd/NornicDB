package storage

// publicationConstraintView owns the rule metadata used to derive a physical
// commit's dependencies. Capture it before batch assembly can take the commit
// gate; late derivation then uses restored operation images without schema
// locks. The first rule stays inline, requiring no allocation for 0/1 rules.
type publicationConstraintView struct {
	first     publicationConstraintRule
	rest      []publicationConstraintRule
	nodeRules int
}

// ruleAt addresses the inline-first rule sequence without copying rule strings
// or allocating a callback iterator. Index -1 names the inline slot.
func (v *publicationConstraintView) ruleAt(index int) *publicationConstraintRule {
	if index < 0 {
		return &v.first
	}
	return &v.rest[index]
}

type publicationConstraintRule struct {
	label       string
	property    string
	kind        ConstraintEntityType
	properties  []string // owned names for a composite relationship rule
	propertyKey string
}

func (rule *publicationConstraintRule) empty() bool { return rule.kind == "" }

func capturePublicationConstraintView(schema *SchemaManager) publicationConstraintView {
	var view publicationConstraintView
	if schema == nil {
		return view
	}
	schema.mu.RLock()
	defer schema.mu.RUnlock()
	nodeRules := len(schema.uniqueConstraints) > 0
	for _, c := range schema.constraints {
		switch c.EffectiveEntityType() {
		case ConstraintEntityNode:
			if !nodeRules || len(c.Properties) != 1 || (c.Type != ConstraintUnique && c.Type != ConstraintNodeKey) {
				continue
			}
			view.add(publicationConstraintRule{label: c.Label, property: c.Properties[0], kind: ConstraintEntityNode})
			view.nodeRules++
		case ConstraintEntityRelationship:
			if len(c.Properties) == 0 || (c.Type != ConstraintUnique && c.Type != ConstraintRelationshipKey) {
				continue
			}
			rule := publicationConstraintRule{label: c.Label, kind: ConstraintEntityRelationship, propertyKey: relationshipPropertyKey(c.Properties)}
			if len(c.Properties) == 1 {
				rule.property = c.Properties[0]
			} else {
				rule.properties = append([]string(nil), c.Properties...)
			}
			view.add(rule)
		}
	}
	return view
}

func (v *publicationConstraintView) add(rule publicationConstraintRule) {
	if v.first.empty() {
		v.first = rule
		return
	}
	v.rest = append(v.rest, rule)
}

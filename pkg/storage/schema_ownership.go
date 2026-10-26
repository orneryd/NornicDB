package storage

import (
	"slices"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
)

// Author: #1548 implementation lane (Codex). These are schema-definition
// snapshots, used at admission, export and reload. Runtime indexes, compiled
// policy identities and provider/view configuration keep their existing owners.
func cloneConstraint(c Constraint) Constraint {
	c.Properties = slices.Clone(c.Properties)
	if c.AllowedValues != nil {
		values := make([]any, len(c.AllowedValues))
		for i, value := range c.AllowedValues {
			values[i] = ownedMetadataValue(value)
		}
		c.AllowedValues = values
	}
	return c
}

func cloneDecayProfileBinding(b knowledgepolicy.DecayProfileBinding) knowledgepolicy.DecayProfileBinding {
	b.TargetLabels = slices.Clone(b.TargetLabels)
	b.PropertyRules = slices.Clone(b.PropertyRules)
	if b.VisibilityThreshold != nil {
		value := *b.VisibilityThreshold
		b.VisibilityThreshold = &value
	}
	return b
}

func clonePromotionPolicy(p knowledgepolicy.PromotionPolicyDef) knowledgepolicy.PromotionPolicyDef {
	p.TargetLabels = slices.Clone(p.TargetLabels)
	p.WhenClauses = slices.Clone(p.WhenClauses)
	if p.OnAccess != nil {
		block := *p.OnAccess
		block.Mutations = slices.Clone(block.Mutations)
		for i := range block.Mutations {
			if block.Mutations[i].Kalman != nil {
				config := *block.Mutations[i].Kalman
				block.Mutations[i].Kalman = &config
			}
		}
		p.OnAccess = &block
	}
	return p
}

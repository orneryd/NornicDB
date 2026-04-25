package knowledgepolicy

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

var ln2 = math.Log(2)

func bindingLabelKey(labels []string) string {
	sorted := make([]string, len(labels))
	copy(sorted, labels)
	sort.Strings(sorted)
	return strings.Join(sorted, "\x00")
}

func policyTargetKey(p *PromotionPolicyDef) string {
	if p.IsEdge {
		return "edge:" + p.TargetEdgeType
	}
	if p.IsWildcard {
		return "wild:node"
	}
	sorted := make([]string, len(p.TargetLabels))
	copy(sorted, p.TargetLabels)
	sort.Strings(sorted)
	return "node:" + strings.Join(sorted, "\x00")
}

func computeThresholdAgeNanos(fn DecayFunction, halfLifeNanos int64, threshold float64) int64 {
	if threshold <= 0 || halfLifeNanos <= 0 {
		return math.MaxInt64
	}
	switch fn {
	case DecayFunctionExponential:
		return int64(-float64(halfLifeNanos) * math.Log(threshold) / ln2)
	case DecayFunctionLinear:
		return int64((1.0 - threshold) * float64(halfLifeNanos) * 2.0)
	case DecayFunctionStep:
		return halfLifeNanos
	default:
		return math.MaxInt64
	}
}

func compileBinding(binding *DecayProfileBinding, bundle *DecayProfileBundle, bundles map[string]*DecayProfileBundle) *CompiledBinding {
	if binding.NoDecay {
		return &CompiledBinding{
			DecayBinding: binding,
			NoDecay:      true,
		}
	}

	vis := bundle.VisibilityThreshold
	if binding.VisibilityThreshold != nil {
		vis = *binding.VisibilityThreshold
	}

	halfLifeNanos := bundle.HalfLifeSeconds * 1e9
	fn := bundle.Function
	if fn == "" {
		fn = DecayFunctionExponential
	}

	cb := &CompiledBinding{
		DecayProfile:        bundle,
		DecayBinding:        binding,
		VisibilityThreshold: vis,
		ScoreFrom:           bundle.ScoreFrom,
		ScoreFromProperty:   bundle.ScoreFromProperty,
		Function:            fn,
		HalfLifeNanos:       halfLifeNanos,
		ThresholdAgeNanos:   computeThresholdAgeNanos(fn, halfLifeNanos, vis),
		DecayFloor:          bundle.ScoreFloor,
	}

	if len(binding.PropertyRules) > 0 {
		cb.CompiledPropertyRules = make(map[string]*CompiledPropertyOverride, len(binding.PropertyRules))
		for i := range binding.PropertyRules {
			rule := &binding.PropertyRules[i]
			override := &CompiledPropertyOverride{NoDecay: rule.NoDecay}
			if !rule.NoDecay {
				ruleFn := fn
				ruleHalfLife := halfLifeNanos
				ruleFloor := cb.DecayFloor

				if rule.ProfileRef != "" && bundles != nil {
					if refBundle, ok := bundles[rule.ProfileRef]; ok {
						ruleHalfLife = refBundle.HalfLifeSeconds * 1e9
						if refBundle.Function != "" {
							ruleFn = refBundle.Function
						}
						if refBundle.ScoreFloor > 0 {
							ruleFloor = refBundle.ScoreFloor
						}
					}
				}
				if rule.HalfLifeSeconds > 0 {
					ruleHalfLife = rule.HalfLifeSeconds * 1e9
				}
				if rule.ScoreFloor > 0 {
					ruleFloor = rule.ScoreFloor
				}
				override.Function = ruleFn
				override.HalfLifeNanos = ruleHalfLife
				override.ThresholdAgeNanos = computeThresholdAgeNanos(ruleFn, ruleHalfLife, vis)
				override.DecayFloor = ruleFloor
			}
			cb.CompiledPropertyRules[rule.PropertyPath] = override
		}
		for _, override := range cb.CompiledPropertyRules {
			if override.NoDecay {
				cb.HasNoDecayProperty = true
				break
			}
		}
	}

	return cb
}

// BuildBindingTable compiles all decay bindings and promotion policies into a
// BindingTable ready for the resolver. Returns an error if cross-references are
// invalid or if two bindings conflict (same target key and Order).
func BuildBindingTable(
	bundles map[string]*DecayProfileBundle,
	bindings map[string]*DecayProfileBinding,
	profiles map[string]*PromotionProfileDef,
	policies map[string]*PromotionPolicyDef,
) (*BindingTable, error) {
	bt := NewBindingTable()

	type seen struct {
		name  string
		order int
	}
	nodeConflicts := map[string]seen{}

	for _, binding := range bindings {
		if !binding.NoDecay && binding.ProfileRef != "" {
			if bundles == nil {
				return nil, fmt.Errorf("binding %q references profile %q but no profiles exist", binding.Name, binding.ProfileRef)
			}
			if _, ok := bundles[binding.ProfileRef]; !ok {
				return nil, fmt.Errorf("binding %q references unknown profile %q", binding.Name, binding.ProfileRef)
			}
		}

		for _, rule := range binding.PropertyRules {
			if rule.ProfileRef != "" {
				if bundles == nil {
					return nil, fmt.Errorf("property rule on binding %q references profile %q but no profiles exist", binding.Name, rule.ProfileRef)
				}
				if _, ok := bundles[rule.ProfileRef]; !ok {
					return nil, fmt.Errorf("property rule on binding %q references unknown profile %q", binding.Name, rule.ProfileRef)
				}
			}
		}
	}

	for _, policy := range policies {
		for _, wc := range policy.WhenClauses {
			if wc.ProfileRef != "" {
				if profiles == nil {
					return nil, fmt.Errorf("policy %q WHEN clause references profile %q but no promotion profiles exist", policy.Name, wc.ProfileRef)
				}
				if _, ok := profiles[wc.ProfileRef]; !ok {
					return nil, fmt.Errorf("policy %q WHEN clause references unknown promotion profile %q", policy.Name, wc.ProfileRef)
				}
			}
		}
	}

	for _, binding := range bindings {
		var bundle *DecayProfileBundle
		if !binding.NoDecay && binding.ProfileRef != "" {
			bundle = bundles[binding.ProfileRef]
		}

		var cb *CompiledBinding
		if binding.NoDecay || bundle != nil {
			cb = compileBinding(binding, bundle, bundles)
		} else {
			cb = &CompiledBinding{DecayBinding: binding, NoDecay: true}
		}

		if binding.IsWildcard {
			if binding.IsEdge {
				bt.SetWildEdge(cb)
			} else {
				bt.SetWildNode(cb)
			}
			continue
		}

		if binding.IsEdge {
			bt.SetEdge(binding.TargetEdgeType, cb)
			continue
		}

		key := bindingLabelKey(binding.TargetLabels)
		if prev, ok := nodeConflicts[key]; ok {
			if prev.order == binding.Order {
				return nil, fmt.Errorf("conflict: bindings %q and %q both target label set %v with Order %d",
					prev.name, binding.Name, binding.TargetLabels, binding.Order)
			}
			if binding.Order < prev.order {
				bt.SetNode(key, cb)
				nodeConflicts[key] = seen{name: binding.Name, order: binding.Order}
			}
		} else {
			bt.SetNode(key, cb)
			nodeConflicts[key] = seen{name: binding.Name, order: binding.Order}
		}
	}

	policyByKey := map[string]*PromotionPolicyDef{}
	for _, policy := range policies {
		if !policy.Enabled {
			continue
		}
		policyByKey[policyTargetKey(policy)] = policy
	}

	bt.mu.Lock()
	for key, cb := range bt.nodes {
		if p, ok := policyByKey["node:"+key]; ok {
			cb.PromotionPolicy = p
		}
	}
	for key, cb := range bt.edges {
		if p, ok := policyByKey["edge:"+key]; ok {
			cb.PromotionPolicy = p
		}
	}
	if bt.wildNode != nil {
		if p, ok := policyByKey["wild:node"]; ok {
			bt.wildNode.PromotionPolicy = p
		}
	}
	if bt.wildEdge != nil {
		if p, ok := policyByKey["wild:edge"]; ok {
			bt.wildEdge.PromotionPolicy = p
		}
	}
	bt.mu.Unlock()

	return bt, nil
}

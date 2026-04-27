package knowledgepolicy

import "math"

// Scorer computes decay and promotion scores using the shared resolver.
type Scorer struct {
	resolver     *Resolver
	decayEnabled bool
}

// NewScorer creates a Scorer. When decayEnabled is false, all Score* methods
// return NeutralResolution.
func NewScorer(r *Resolver, decayEnabled bool) *Scorer {
	return &Scorer{resolver: r, decayEnabled: decayEnabled}
}

func (s *Scorer) ScoreNode(
	targetID string,
	labels []string,
	accessMeta *AccessMetaEntry,
	createdAt, versionAt, nowNanos int64,
) ScoringResolution {
	return s.ScoreNodeWithProperties(targetID, labels, nil, accessMeta, createdAt, versionAt, nowNanos)
}

func (s *Scorer) ScoreNodeWithProperties(
	targetID string,
	labels []string,
	entityProps map[string]interface{},
	accessMeta *AccessMetaEntry,
	createdAt, versionAt, nowNanos int64,
) ScoringResolution {
	if !s.decayEnabled {
		return neutralFor(targetID, ScopeNode)
	}
	cb := s.resolver.ResolveNode(labels)
	return s.score(cb, targetID, ScopeNode, createdAt, versionAt, nowNanos, accessMeta, entityProps)
}

func (s *Scorer) ScoreEdge(
	targetID string,
	edgeType string,
	accessMeta *AccessMetaEntry,
	createdAt, versionAt, nowNanos int64,
) ScoringResolution {
	return s.ScoreEdgeWithProperties(targetID, edgeType, nil, accessMeta, createdAt, versionAt, nowNanos)
}

func (s *Scorer) ScoreEdgeWithProperties(
	targetID string,
	edgeType string,
	entityProps map[string]interface{},
	accessMeta *AccessMetaEntry,
	createdAt, versionAt, nowNanos int64,
) ScoringResolution {
	if !s.decayEnabled {
		return neutralFor(targetID, ScopeEdge)
	}
	cb := s.resolver.ResolveEdge(edgeType)
	return s.score(cb, targetID, ScopeEdge, createdAt, versionAt, nowNanos, accessMeta, entityProps)
}

func (s *Scorer) ScoreProperty(
	targetID string,
	labels []string,
	propertyPath string,
	accessMeta *AccessMetaEntry,
	createdAt, versionAt, nowNanos int64,
) ScoringResolution {
	if !s.decayEnabled {
		return neutralFor(targetID, ScopeProperty)
	}
	cb := s.resolver.ResolveProperty(labels, propertyPath)
	return s.score(cb, targetID, ScopeProperty, createdAt, versionAt, nowNanos, accessMeta, nil)
}

func (s *Scorer) ScoreEdgeProperty(
	targetID string,
	edgeType string,
	propertyPath string,
	accessMeta *AccessMetaEntry,
	createdAt, versionAt, nowNanos int64,
) ScoringResolution {
	if !s.decayEnabled {
		return neutralFor(targetID, ScopeProperty)
	}
	cb := s.resolver.ResolveEdgeProperty(edgeType, propertyPath)
	return s.score(cb, targetID, ScopeProperty, createdAt, versionAt, nowNanos, accessMeta, nil)
}

func neutralFor(targetID string, scope ScopeType) ScoringResolution {
	return ScoringResolution{
		TargetID:      targetID,
		TargetScope:   scope,
		EffectiveRate: 0,
		FinalScore:    1.0,
		NoDecay:       true,
	}
}

func (s *Scorer) score(
	cb *CompiledBinding,
	targetID string,
	scope ScopeType,
	createdAt, versionAt, nowNanos int64,
	accessMeta *AccessMetaEntry,
	entityProps map[string]interface{},
) ScoringResolution {
	if cb == nil {
		return neutralFor(targetID, scope)
	}
	if cb.NoDecay {
		res := neutralFor(targetID, scope)
		res.ResolvedDecayFunction = cb.Function
		res.ResolvedScoreFrom = cb.ScoreFrom
		res.EffectiveThreshold = cb.VisibilityThreshold
		res.EffectiveFloor = cb.DecayFloor
		if cb.DecayBinding != nil {
			res.ResolutionSourceChain = []string{cb.DecayBinding.Name}
		}
		if cb.DecayProfile != nil {
			res.ResolvedDecayProfileID = cb.DecayProfile.Name
			res.AppliedDecayProfileNames = []string{cb.DecayProfile.Name}
		}
		return res
	}

	anchor := resolveAnchor(cb, createdAt, versionAt, accessMeta)
	ageNanos := nowNanos - anchor
	if ageNanos < 0 {
		ageNanos = 0
	}

	baseScore := computeDecay(cb.Function, ageNanos, cb.HalfLifeNanos)

	multiplier := 1.0
	promoFloor := 0.0
	promoCap := 1.0
	promoProfileName := ""
	promoPolicyName := ""

	if cb.PromotionPolicy != nil && cb.PromotionPolicy.Enabled {
		promoPolicyName = cb.PromotionPolicy.Name
		selected := selectPromotionProfile(cb.CompiledPromotionRules, accessMeta, entityProps, nowNanos)
		if selected != nil {
			multiplier = selected.Multiplier
			promoFloor = selected.ScoreFloor
			promoCap = selected.ScoreCap
			promoProfileName = selected.Name
		}
	}

	finalScore := computeFinalScore(baseScore, multiplier, promoFloor, promoCap, cb.DecayFloor)

	var profileID string
	var sourceChain []string
	var profileNames []string
	if cb.DecayBinding != nil {
		sourceChain = []string{cb.DecayBinding.Name}
	}
	if cb.DecayProfile != nil {
		profileID = cb.DecayProfile.Name
		profileNames = []string{cb.DecayProfile.Name}
	}

	return ScoringResolution{
		TargetID:                    targetID,
		TargetScope:                 scope,
		ResolvedDecayProfileID:      profileID,
		ResolvedDecayFunction:       cb.Function,
		ResolvedScoreFrom:           cb.ScoreFrom,
		ResolutionSourceChain:       sourceChain,
		AppliedDecayProfileNames:    profileNames,
		AppliedPromotionPolicyName:  promoPolicyName,
		AppliedPromotionProfileName: promoProfileName,
		EffectiveRate:               effectiveRate(cb.HalfLifeNanos),
		EffectiveThreshold:          cb.VisibilityThreshold,
		EffectiveFloor:              cb.DecayFloor,
		EffectiveMultiplier:         multiplier,
		BaseScore:                   baseScore,
		FinalScore:                  finalScore,
		SuppressionEligible:         finalScore < cb.VisibilityThreshold && !cb.HasNoDecayProperty,
	}
}

func selectPromotionProfile(rules []CompiledPromotionRule, accessMeta *AccessMetaEntry, entityProps map[string]interface{}, nowNanos int64) *PromotionProfileDef {
	if len(rules) == 0 {
		return nil
	}
	ctx := onAccessEvalContext{entry: accessMeta, entityProps: entityProps, nowNanos: nowNanos, params: map[string]interface{}{}}
	var selected *PromotionProfileDef
	for _, rule := range rules {
		value, err := evalOnAccessExpression(rule.Predicate, ctx)
		if err != nil || !truthy(value) || rule.Profile == nil {
			continue
		}
		if selected == nil || rule.Profile.Multiplier > selected.Multiplier ||
			(rule.Profile.Multiplier == selected.Multiplier && rule.Profile.ScoreFloor > selected.ScoreFloor) {
			selected = rule.Profile
		}
	}
	return selected
}

func effectiveRate(halfLifeNanos int64) float64 {
	if halfLifeNanos <= 0 {
		return 0
	}
	return ln2 / float64(halfLifeNanos)
}

func resolveAnchor(cb *CompiledBinding, createdAt, versionAt int64, accessMeta *AccessMetaEntry) int64 {
	switch cb.ScoreFrom {
	case ScoreFromVersion:
		if versionAt > 0 {
			return versionAt
		}
		return createdAt
	case ScoreFromCustom:
		if accessMeta != nil && cb.ScoreFromProperty != "" {
			if v, ok := accessMeta.Overflow[cb.ScoreFromProperty]; ok {
				switch ts := v.(type) {
				case int64:
					return ts
				case float64:
					return int64(ts)
				}
			}
		}
		return createdAt
	default:
		return createdAt
	}
}

func computeDecay(fn DecayFunction, ageNanos, halfLifeNanos int64) float64 {
	switch fn {
	case DecayFunctionExponential:
		return exponentialDecay(ageNanos, halfLifeNanos)
	case DecayFunctionLinear:
		return linearDecay(ageNanos, halfLifeNanos)
	case DecayFunctionStep:
		return stepDecay(ageNanos, halfLifeNanos)
	default:
		return 1.0
	}
}

func exponentialDecay(ageNanos, halfLifeNanos int64) float64 {
	if halfLifeNanos <= 0 {
		return 1.0
	}
	return math.Exp(-float64(ageNanos) * ln2 / float64(halfLifeNanos))
}

func linearDecay(ageNanos, halfLifeNanos int64) float64 {
	if halfLifeNanos <= 0 {
		return 1.0
	}
	ratio := float64(ageNanos) / (2.0 * float64(halfLifeNanos))
	return math.Max(0, 1.0-ratio)
}

func stepDecay(ageNanos, halfLifeNanos int64) float64 {
	if halfLifeNanos <= 0 {
		return 1.0
	}
	if ageNanos < halfLifeNanos {
		return 1.0
	}
	return 0.0
}

func computeFinalScore(baseDecayScore, multiplier, promoFloor, promoCap, decayFloor float64) float64 {
	promoted := baseDecayScore * multiplier
	floored := math.Max(promoted, promoFloor)
	capped := math.Min(floored, promoCap)
	return math.Max(capped, decayFloor)
}

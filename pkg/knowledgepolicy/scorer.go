package knowledgepolicy

import (
	"context"
	"math"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// Scorer computes decay and promotion scores using the shared resolver.
type Scorer struct {
	resolver     *Resolver
	decayEnabled bool

	// metrics is the knowledge-policy observability handle attached via
	// SetMetrics at construction (typically from pkg/nornicdb/db.go). May
	// be nil when observability is disabled or before the handle is
	// published — all IncScored / IncSuppression / ObserveDecayScoreSampled
	// calls are nil-safe.
	metrics *observability.KnowledgePolicyMetrics

	// database is the namespace/tenant label threaded onto metrics when the
	// observability bag was constructed with tenantLabelsEnabled=true. Zero
	// string is fine when the label is disabled.
	database string
}

// NewScorer creates a Scorer. When decayEnabled is false, all Score* methods
// return NeutralResolution.
func NewScorer(r *Resolver, decayEnabled bool) *Scorer {
	return &Scorer{resolver: r, decayEnabled: decayEnabled}
}

// SetMetrics attaches an observability handle to the Scorer. Safe to call
// after construction; passing nil falls back to the package-level global
// (observability.GetKnowledgePolicyMetrics) so late-initialised metrics
// still flow through. `database` is included on every per-tenant metric
// labelset when the bag was constructed with tenantLabelsEnabled=true.
func (s *Scorer) SetMetrics(m *observability.KnowledgePolicyMetrics, database string) {
	s.metrics = m
	s.database = database
}

// resolveMetrics returns the configured handle or falls back to the global
// ref. Allows metrics to be registered after Scorer construction — the
// common case when NornicDB main.go builds the Provider AFTER db.Open.
func (s *Scorer) resolveMetrics() *observability.KnowledgePolicyMetrics {
	if s.metrics != nil {
		return s.metrics
	}
	return observability.GetKnowledgePolicyMetrics()
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
		res := neutralFor(targetID, ScopeNode)
		s.recordScoringOutcome(res, "")
		return res
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
		res := neutralFor(targetID, ScopeEdge)
		s.recordScoringOutcome(res, "")
		return res
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
		res := neutralFor(targetID, ScopeProperty)
		s.recordScoringOutcome(res, "")
		return res
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
		res := neutralFor(targetID, ScopeProperty)
		s.recordScoringOutcome(res, "")
		return res
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

// scopeToEntityKind maps the internal ScopeType enum to the
// knowledge_policy subsystem's entity_kind closed enum. Kept here (rather
// than inside the catalog) so pkg/observability never needs to know about
// pkg/knowledgepolicy's uppercase enum values.
func scopeToEntityKind(scope ScopeType) string {
	switch scope {
	case ScopeNode:
		return "node"
	case ScopeEdge:
		return "edge"
	case ScopeProperty:
		return "property"
	default:
		return "node"
	}
}

// recordScoringOutcome emits the scored_total counter (always), the decay_score
// histogram (sampled), and the suppressions_total counter (only when a reason
// is supplied). Nil-safe on s.metrics.
func (s *Scorer) recordScoringOutcome(res ScoringResolution, reason string) {
	metrics := s.resolveMetrics()
	if metrics == nil {
		return
	}
	entityKind := scopeToEntityKind(res.TargetScope)
	result := "visible"
	if res.NoDecay {
		result = "no_decay"
	} else if res.SuppressionEligible {
		result = "suppressed"
	}
	metrics.IncScored(entityKind, result, s.database)
	if !res.NoDecay {
		metrics.ObserveDecayScoreSampled(context.Background(), entityKind, s.database, res.FinalScore)
	}
	if reason != "" {
		metrics.IncSuppression(entityKind, reason, s.database)
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
		res := neutralFor(targetID, scope)
		s.recordScoringOutcome(res, "")
		return res
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
		s.recordScoringOutcome(res, "")
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

	res := ScoringResolution{
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
	// Classify the suppression reason for the suppressions_total counter.
	// The order of checks matters: score_floor pins the final score *above*
	// the threshold but can still indicate a configured minimum being hit,
	// so it's a secondary dimension; below_threshold is the common case.
	reason := ""
	if res.SuppressionEligible {
		switch {
		case cb.DecayFloor > 0 && finalScore <= cb.DecayFloor:
			reason = "score_floor"
		default:
			reason = "below_threshold"
		}
	}
	s.recordScoringOutcome(res, reason)
	return res
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
	case ScoreFromLastAccessed:
		// Time-since-last-access is the "age" the curve consumes. With
		// an inverse half-life, idle time grows the score; an access
		// resets the anchor and drops the score back near zero. Falls
		// back to createdAt when no access has been recorded yet so
		// fresh entities still produce a deterministic score.
		if accessMeta != nil && accessMeta.Fixed.LastAccessedAt > 0 {
			return accessMeta.Fixed.LastAccessedAt
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

// computeDecay dispatches on the decay function family. A negative
// halfLifeNanos inverts the curve in place: the compiled score becomes
// `1 - f(age, |halfLife|)` instead of `f(age, halfLife)`. This is the
// Ebbinghaus-Roynard signal — paired with ScoreFromLastAccessed, the
// score grows with idle time and resets on access, with no new
// function/type/flag in the schema.
func computeDecay(fn DecayFunction, ageNanos, halfLifeNanos int64) float64 {
	inverse := halfLifeNanos < 0
	if inverse {
		halfLifeNanos = -halfLifeNanos
	}
	var score float64
	switch fn {
	case DecayFunctionExponential:
		score = exponentialDecay(ageNanos, halfLifeNanos)
	case DecayFunctionLinear:
		score = linearDecay(ageNanos, halfLifeNanos)
	case DecayFunctionStep:
		score = stepDecay(ageNanos, halfLifeNanos)
	default:
		score = 1.0
	}
	if inverse {
		return 1.0 - score
	}
	return score
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

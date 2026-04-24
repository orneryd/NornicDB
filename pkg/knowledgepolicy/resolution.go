package knowledgepolicy

// ScoringResolution is the result of resolving and scoring a target entity.
type ScoringResolution struct {
	TargetID                    string
	TargetScope                 ScopeType
	ResolvedDecayProfileID      string
	ResolvedScoreFrom           ScoreFromMode
	ResolutionSourceChain       []string
	AppliedDecayProfileNames    []string
	AppliedPromotionPolicyName  string
	AppliedPromotionProfileName string
	EffectiveRate               float64
	EffectiveThreshold          float64
	EffectiveMultiplier         float64
	BaseScore                   float64
	FinalScore                  float64
	NoDecay                     bool
	SuppressionEligible         bool
	Explanation                 string
}

// NeutralResolution is returned when decay is disabled or no profile matches.
var NeutralResolution = ScoringResolution{FinalScore: 1.0, NoDecay: true}

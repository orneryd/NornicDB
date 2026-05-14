package knowledgepolicy

// DecayFunction identifies a scoring function.
type DecayFunction string

const (
	DecayFunctionExponential DecayFunction = "exponential"
	DecayFunctionLinear      DecayFunction = "linear"
	DecayFunctionStep        DecayFunction = "step"
	DecayFunctionNone        DecayFunction = "none"
)

// ScoreFromMode identifies the score start-time anchor.
type ScoreFromMode string

const (
	ScoreFromCreated ScoreFromMode = "CREATED"
	ScoreFromVersion ScoreFromMode = "VERSION"
	ScoreFromCustom  ScoreFromMode = "CUSTOM"
	// ScoreFromLastAccessed anchors the score at AccessMetaEntry.Fixed.
	// LastAccessedAt. Time-since-last-access is the "age" the decay
	// function consumes — pairs with DecayFunctionInverseExponential
	// to implement the Ebbinghaus-Roynard consolidation curve. Falls
	// back to createdAt when no access has been recorded.
	ScoreFromLastAccessed ScoreFromMode = "LAST_ACCESSED"
)

// ScopeType identifies whether a profile/policy targets nodes, edges, or properties.
type ScopeType string

const (
	ScopeNode     ScopeType = "NODE"
	ScopeEdge     ScopeType = "EDGE"
	ScopeProperty ScopeType = "PROPERTY"
)

// DecayProfileBundle is a reusable parameter bundle (no FOR clause).
//
// HalfLifeSeconds carries the inversion signal: a negative value flips
// the chosen Function in place, so the compiled score becomes
// `1 - f(age, |halfLife|)` instead of `f(age, halfLife)`. The score
// then grows from 0 toward 1.0 as age increases, instead of falling
// toward 0. Combined with ScoreFromLastAccessed this implements the
// Ebbinghaus-Roynard consolidation curve: idle time strengthens the
// memory and an access resets the anchor (and thus the score) back
// to 0. The inversion is purely a curve property and composes with
// every Function family and every ScoreFrom anchor.
type DecayProfileBundle struct {
	Name                string        `json:"name" msgpack:"name"`
	HalfLifeSeconds     int64         `json:"halfLifeSeconds" msgpack:"halfLifeSeconds"`
	VisibilityThreshold float64       `json:"visibilityThreshold" msgpack:"visibilityThreshold"`
	ScoreFloor          float64       `json:"scoreFloor" msgpack:"scoreFloor"`
	Function            DecayFunction `json:"function" msgpack:"function"`
	Scope               ScopeType     `json:"scope" msgpack:"scope"`
	DecayEnabled        bool          `json:"decayEnabled" msgpack:"decayEnabled"`
	ScoreFrom           ScoreFromMode `json:"scoreFrom" msgpack:"scoreFrom"`
	ScoreFromProperty   string        `json:"scoreFromProperty,omitempty" msgpack:"scoreFromProperty,omitempty"`
	Enabled             bool          `json:"enabled" msgpack:"enabled"`
}

// DecayProfilePropertyRule is an inline property-level rule inside a binding.
type DecayProfilePropertyRule struct {
	PropertyPath    string  `json:"propertyPath" msgpack:"propertyPath"`
	NoDecay         bool    `json:"noDecay,omitempty" msgpack:"noDecay,omitempty"`
	ProfileRef      string  `json:"profileRef,omitempty" msgpack:"profileRef,omitempty"`
	HalfLifeSeconds int64   `json:"halfLifeSeconds,omitempty" msgpack:"halfLifeSeconds,omitempty"`
	ScoreFloor      float64 `json:"scoreFloor,omitempty" msgpack:"scoreFloor,omitempty"`
	Order           int     `json:"order" msgpack:"order"`
}

// DecayProfileBinding is a targeted binding (has FOR clause).
type DecayProfileBinding struct {
	Name                string                     `json:"name" msgpack:"name"`
	TargetLabels        []string                   `json:"targetLabels,omitempty" msgpack:"targetLabels,omitempty"`
	TargetEdgeType      string                     `json:"targetEdgeType,omitempty" msgpack:"targetEdgeType,omitempty"`
	IsWildcard          bool                       `json:"isWildcard" msgpack:"isWildcard"`
	IsEdge              bool                       `json:"isEdge" msgpack:"isEdge"`
	ProfileRef          string                     `json:"profileRef,omitempty" msgpack:"profileRef,omitempty"`
	NoDecay             bool                       `json:"noDecay,omitempty" msgpack:"noDecay,omitempty"`
	HalfLifeSeconds     int64                      `json:"halfLifeSeconds,omitempty" msgpack:"halfLifeSeconds,omitempty"`
	ScoreFloor          float64                    `json:"scoreFloor,omitempty" msgpack:"scoreFloor,omitempty"`
	VisibilityThreshold *float64                   `json:"visibilityThreshold,omitempty" msgpack:"visibilityThreshold,omitempty"`
	PropertyRules       []DecayProfilePropertyRule `json:"propertyRules,omitempty" msgpack:"propertyRules,omitempty"`
	Order               int                        `json:"order" msgpack:"order"`
}

// PromotionProfileDef is a reusable promotion parameter bundle.
//
// Multiplier > 1.0 boosts the decayed score; Multiplier < 1.0 dampens
// it (e.g. 0.5 halves the score). A dampening multiplier paired with
// an inverted DecayProfileBundle (negative HalfLifeSeconds) implements
// "punish frequent access" semantics: the inverted curve makes idle
// entries strong, the dampening multiplier knocks down hot-path entries
// once a WHEN predicate trips, so frequently-accessed nodes/edges decay
// faster while idle ones gain strength. ScoreFloor and ScoreCap clamp
// the final score after the multiplier is applied.
type PromotionProfileDef struct {
	Name       string    `json:"name" msgpack:"name"`
	Scope      ScopeType `json:"scope" msgpack:"scope"`
	Multiplier float64   `json:"multiplier" msgpack:"multiplier"`
	ScoreFloor float64   `json:"scoreFloor" msgpack:"scoreFloor"`
	ScoreCap   float64   `json:"scoreCap" msgpack:"scoreCap"`
	Enabled    bool      `json:"enabled" msgpack:"enabled"`
}

// PromotionPolicyWhenClause is a WHEN predicate inside a policy.
type PromotionPolicyWhenClause struct {
	PropertyPath string `json:"propertyPath,omitempty" msgpack:"propertyPath,omitempty"`
	Predicate    string `json:"predicate" msgpack:"predicate"`
	ProfileRef   string `json:"profileRef" msgpack:"profileRef"`
	Order        int    `json:"order" msgpack:"order"`
}

// KalmanMode identifies how the Kalman filter is configured for a mutation.
type KalmanMode string

const (
	KalmanModeNone   KalmanMode = ""
	KalmanModeAuto   KalmanMode = "auto"
	KalmanModeManual KalmanMode = "manual"
)

// KalmanConfig holds the Kalman filter configuration for an ON ACCESS mutation.
type KalmanConfig struct {
	Mode          KalmanMode `json:"mode" msgpack:"mode"`
	Q             float64    `json:"q" msgpack:"q"`
	R             float64    `json:"r,omitempty" msgpack:"r,omitempty"`
	VarianceScale float64    `json:"varianceScale,omitempty" msgpack:"varianceScale,omitempty"`
	WindowSize    int        `json:"windowSize,omitempty" msgpack:"windowSize,omitempty"`
}

// OnAccessMutation is a single SET expression inside an ON ACCESS block.
type OnAccessMutation struct {
	Expression string        `json:"expression" msgpack:"expression"`
	Kalman     *KalmanConfig `json:"kalman,omitempty" msgpack:"kalman,omitempty"`
}

// PromotionPolicyOnAccess is the ON ACCESS block definition.
type PromotionPolicyOnAccess struct {
	Mutations []OnAccessMutation `json:"mutations" msgpack:"mutations"`
}

// PromotionPolicyDef is a targeted promotion policy.
type PromotionPolicyDef struct {
	Name           string                      `json:"name" msgpack:"name"`
	TargetLabels   []string                    `json:"targetLabels,omitempty" msgpack:"targetLabels,omitempty"`
	TargetEdgeType string                      `json:"targetEdgeType,omitempty" msgpack:"targetEdgeType,omitempty"`
	IsWildcard     bool                        `json:"isWildcard" msgpack:"isWildcard"`
	IsEdge         bool                        `json:"isEdge" msgpack:"isEdge"`
	OnAccess       *PromotionPolicyOnAccess    `json:"onAccess,omitempty" msgpack:"onAccess,omitempty"`
	WhenClauses    []PromotionPolicyWhenClause `json:"whenClauses,omitempty" msgpack:"whenClauses,omitempty"`
	Enabled        bool                        `json:"enabled" msgpack:"enabled"`
}

// ValidDecayFunctions is the set of valid decay function identifiers.
var ValidDecayFunctions = map[DecayFunction]bool{
	DecayFunctionExponential: true,
	DecayFunctionLinear:      true,
	DecayFunctionStep:        true,
	DecayFunctionNone:        true,
}

// ValidScoreFromModes is the set of valid score-from mode identifiers.
var ValidScoreFromModes = map[ScoreFromMode]bool{
	ScoreFromCreated:      true,
	ScoreFromVersion:      true,
	ScoreFromCustom:       true,
	ScoreFromLastAccessed: true,
}

// ValidScopeTypes is the set of valid scope type identifiers.
var ValidScopeTypes = map[ScopeType]bool{
	ScopeNode:     true,
	ScopeEdge:     true,
	ScopeProperty: true,
}

package knowledgepolicy

// AccessMetaFixedFields is the fast-path fixed-layout struct.
type AccessMetaFixedFields struct {
	AccessCount     int64 `json:"accessCount" msgpack:"accessCount"`
	LastAccessedAt  int64 `json:"lastAccessedAt" msgpack:"lastAccessedAt"`
	TraversalCount  int64 `json:"traversalCount" msgpack:"traversalCount"`
	LastTraversedAt int64 `json:"lastTraversedAt" msgpack:"lastTraversedAt"`
}

// AccessMetaEntry is the full persisted entry per target.
type AccessMetaEntry struct {
	TargetID      string                          `json:"targetId" msgpack:"targetId"`
	TargetScope   ScopeType                       `json:"targetScope" msgpack:"targetScope"`
	Fixed         AccessMetaFixedFields           `json:"fixed" msgpack:"fixed"`
	Overflow      map[string]interface{}          `json:"overflow,omitempty" msgpack:"overflow,omitempty"`
	KalmanFilters map[string]*KalmanPropertyState `json:"kalmanFilters,omitempty" msgpack:"kalmanFilters,omitempty"`
	LastMutatedAt int64                           `json:"lastMutatedAt" msgpack:"lastMutatedAt"`
	MutationCount int64                           `json:"mutationCount" msgpack:"mutationCount"`
}

// KalmanPropertyState holds the Kalman filter state and variance tracker state
// for a single property that uses WITH KALMAN.
type KalmanPropertyState struct {
	FilteredValue float64               `json:"filteredValue" msgpack:"filteredValue"`
	Filter        KalmanFilterState     `json:"filter" msgpack:"filter"`
	Variance      *VarianceTrackerState `json:"variance,omitempty" msgpack:"variance,omitempty"`
}

// KalmanFilterState is the per-property Kalman filter state.
type KalmanFilterState struct {
	X             float64 `json:"x" msgpack:"x"`
	LastX         float64 `json:"lx" msgpack:"lx"`
	P             float64 `json:"p" msgpack:"p"`
	K             float64 `json:"k" msgpack:"k"`
	E             float64 `json:"e" msgpack:"e"`
	Q             float64 `json:"q" msgpack:"q"`
	R             float64 `json:"r" msgpack:"r"`
	VarianceScale float64 `json:"vs" msgpack:"vs"`
	Observations  int     `json:"n" msgpack:"n"`
}

// VarianceTrackerState is the serializable state for auto-R calculation.
type VarianceTrackerState struct {
	Window    []float64 `json:"w" msgpack:"w"`
	WindowIdx int       `json:"wi" msgpack:"wi"`
	SumMean   float64   `json:"sm" msgpack:"sm"`
	SumVar    float64   `json:"sv" msgpack:"sv"`
	Mean      float64   `json:"m" msgpack:"m"`
	Variance  float64   `json:"v" msgpack:"v"`
	InverseN  float64   `json:"in" msgpack:"in"`
}

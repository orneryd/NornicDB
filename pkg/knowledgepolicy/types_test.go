package knowledgepolicy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func roundTrip[T any](t *testing.T, original T) T {
	t.Helper()
	data, err := msgpack.Marshal(original)
	require.NoError(t, err)
	var decoded T
	err = msgpack.Unmarshal(data, &decoded)
	require.NoError(t, err)
	return decoded
}

func TestDecayProfileBundle_Msgpack(t *testing.T) {
	original := DecayProfileBundle{
		Name:                "test_bundle",
		HalfLifeSeconds:     604800,
		VisibilityThreshold: 0.10,
		ScoreFloor:          0.05,
		Function:            DecayFunctionExponential,
		Scope:               ScopeNode,
		DecayEnabled:        true,
		ScoreFrom:           ScoreFromCreated,
		ScoreFromProperty:   "",
		Enabled:             true,
	}
	decoded := roundTrip(t, original)
	assert.Equal(t, original, decoded)
}

func TestDecayProfileBundle_CustomScoreFrom(t *testing.T) {
	original := DecayProfileBundle{
		Name:              "custom_bundle",
		HalfLifeSeconds:   86400,
		Function:          DecayFunctionLinear,
		Scope:             ScopeNode,
		ScoreFrom:         ScoreFromCustom,
		ScoreFromProperty: "myTimestamp",
		Enabled:           true,
	}
	decoded := roundTrip(t, original)
	assert.Equal(t, original, decoded)
}

func TestDecayProfilePropertyRule_Msgpack(t *testing.T) {
	t.Run("NoDecay", func(t *testing.T) {
		original := DecayProfilePropertyRule{
			PropertyPath: "metadata",
			NoDecay:      true,
			Order:        0,
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original, decoded)
	})

	t.Run("ProfileRef", func(t *testing.T) {
		original := DecayProfilePropertyRule{
			PropertyPath:    "score",
			ProfileRef:      "slow_decay",
			HalfLifeSeconds: 3600,
			ScoreFloor:      0.01,
			Order:           1,
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original, decoded)
	})
}

func TestDecayProfileBinding_Msgpack(t *testing.T) {
	t.Run("NodeBinding", func(t *testing.T) {
		vt := 0.10
		original := DecayProfileBinding{
			Name:                "fact_binding",
			TargetLabels:        []string{"KnowledgeFact"},
			ProfileRef:          "fast_decay",
			VisibilityThreshold: &vt,
			PropertyRules: []DecayProfilePropertyRule{
				{PropertyPath: "content", NoDecay: true, Order: 0},
			},
			Order: 0,
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original, decoded)
	})

	t.Run("EdgeBinding", func(t *testing.T) {
		original := DecayProfileBinding{
			Name:           "edge_binding",
			TargetEdgeType: "RELATES_TO",
			IsEdge:         true,
			ProfileRef:     "slow_decay",
			Order:          1,
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original, decoded)
	})

	t.Run("Wildcard", func(t *testing.T) {
		original := DecayProfileBinding{
			Name:       "wildcard_binding",
			IsWildcard: true,
			NoDecay:    true,
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original, decoded)
	})
}

func TestPromotionProfileDef_Msgpack(t *testing.T) {
	original := PromotionProfileDef{
		Name:       "boost",
		Scope:      ScopeNode,
		Multiplier: 2.0,
		ScoreFloor: 0.3,
		ScoreCap:   1.0,
		Enabled:    true,
	}
	decoded := roundTrip(t, original)
	assert.Equal(t, original, decoded)
}

func TestPromotionPolicyWhenClause_Msgpack(t *testing.T) {
	t.Run("WithPropertyPath", func(t *testing.T) {
		original := PromotionPolicyWhenClause{
			PropertyPath: "accessCount",
			Predicate:    "n.accessCount > 10",
			ProfileRef:   "boost",
			Order:        0,
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original, decoded)
	})

	t.Run("EntityLevel", func(t *testing.T) {
		original := PromotionPolicyWhenClause{
			Predicate:  "n.mutationCount > 50",
			ProfileRef: "heavy_boost",
			Order:      1,
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original, decoded)
	})
}

func TestKalmanConfig_Msgpack(t *testing.T) {
	t.Run("AutoMode", func(t *testing.T) {
		original := KalmanConfig{
			Mode:          KalmanModeAuto,
			Q:             0.1,
			R:             88.0,
			VarianceScale: 10.0,
			WindowSize:    32,
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original, decoded)
	})

	t.Run("ManualMode", func(t *testing.T) {
		original := KalmanConfig{
			Mode: KalmanModeManual,
			Q:    0.05,
			R:    50.0,
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original, decoded)
	})

	t.Run("NoneMode", func(t *testing.T) {
		original := KalmanConfig{
			Mode: KalmanModeNone,
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original, decoded)
	})
}

func TestOnAccessMutation_Msgpack(t *testing.T) {
	t.Run("PlainWrite", func(t *testing.T) {
		original := OnAccessMutation{
			Expression: "n.accessCount = n.accessCount + 1",
			Kalman:     nil,
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original, decoded)
		assert.Nil(t, decoded.Kalman)
	})

	t.Run("WithKalmanAuto", func(t *testing.T) {
		original := OnAccessMutation{
			Expression: "n.confidenceScore = $evaluatedConfidence",
			Kalman: &KalmanConfig{
				Mode:          KalmanModeAuto,
				Q:             0.1,
				R:             88.0,
				VarianceScale: 10.0,
				WindowSize:    32,
			},
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original, decoded)
		require.NotNil(t, decoded.Kalman)
		assert.Equal(t, KalmanModeAuto, decoded.Kalman.Mode)
	})

	t.Run("WithKalmanManual", func(t *testing.T) {
		original := OnAccessMutation{
			Expression: "n.score = $rawScore",
			Kalman: &KalmanConfig{
				Mode: KalmanModeManual,
				Q:    0.1,
				R:    88.0,
			},
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original, decoded)
		assert.Equal(t, KalmanModeManual, decoded.Kalman.Mode)
	})
}

func TestPromotionPolicyOnAccess_Msgpack(t *testing.T) {
	original := PromotionPolicyOnAccess{
		Mutations: []OnAccessMutation{
			{Expression: "n.accessCount = n.accessCount + 1", Kalman: nil},
			{Expression: "n.confidence = $conf", Kalman: &KalmanConfig{Mode: KalmanModeAuto, Q: 0.1, R: 88.0, VarianceScale: 10.0, WindowSize: 32}},
		},
	}
	decoded := roundTrip(t, original)
	assert.Equal(t, original, decoded)
	assert.Nil(t, decoded.Mutations[0].Kalman)
	require.NotNil(t, decoded.Mutations[1].Kalman)
}

func TestPromotionPolicyDef_Msgpack(t *testing.T) {
	t.Run("WithOnAccess", func(t *testing.T) {
		original := PromotionPolicyDef{
			Name:         "fact_promo",
			TargetLabels: []string{"KnowledgeFact"},
			OnAccess: &PromotionPolicyOnAccess{
				Mutations: []OnAccessMutation{
					{Expression: "n.accessCount = n.accessCount + 1"},
				},
			},
			Enabled: true,
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original, decoded)
	})

	t.Run("WithWhenClauses", func(t *testing.T) {
		original := PromotionPolicyDef{
			Name:         "when_promo",
			TargetLabels: []string{"KnowledgeFact"},
			WhenClauses: []PromotionPolicyWhenClause{
				{Predicate: "n.accessCount > 10", ProfileRef: "boost", Order: 0},
				{Predicate: "n.mutationCount > 50", ProfileRef: "heavy_boost", Order: 1},
			},
			Enabled: true,
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original, decoded)
	})
}

func TestAccessMetaFixedFields_Msgpack(t *testing.T) {
	original := AccessMetaFixedFields{
		AccessCount:     42,
		LastAccessedAt:  1713900000000000000,
		TraversalCount:  10,
		LastTraversedAt: 1713900000000000000,
	}
	decoded := roundTrip(t, original)
	assert.Equal(t, original, decoded)
}

func TestAccessMetaEntry_Msgpack(t *testing.T) {
	t.Run("WithKalmanFilters", func(t *testing.T) {
		original := AccessMetaEntry{
			TargetID:    "test:node-1",
			TargetScope: ScopeNode,
			Fixed: AccessMetaFixedFields{
				AccessCount:    100,
				LastAccessedAt: 1713900000000000000,
			},
			Overflow: map[string]interface{}{
				"customCounter": int64(5),
			},
			KalmanFilters: map[string]*KalmanPropertyState{
				"confidenceScore": {
					FilteredValue: 0.65,
					Filter: KalmanFilterState{
						X: 0.65, LastX: 0.63, P: 15.0, K: 0.4,
						E: 0.1, Q: 0.0001, R: 88.0, VarianceScale: 10.0,
						Observations: 50,
					},
					Variance: &VarianceTrackerState{
						Window:    []float64{0.6, 0.65, 0.62},
						WindowIdx: 3,
						SumMean:   1.87,
						SumVar:    0.001,
						Mean:      0.623,
						Variance:  0.0004,
						InverseN:  0.333,
					},
				},
			},
			LastMutatedAt: 1713900000000000000,
			MutationCount: 25,
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original.TargetID, decoded.TargetID)
		assert.Equal(t, original.TargetScope, decoded.TargetScope)
		assert.Equal(t, original.Fixed, decoded.Fixed)
		assert.Equal(t, original.LastMutatedAt, decoded.LastMutatedAt)
		assert.Equal(t, original.MutationCount, decoded.MutationCount)
		require.Contains(t, decoded.KalmanFilters, "confidenceScore")
		assert.Equal(t, original.KalmanFilters["confidenceScore"].FilteredValue, decoded.KalmanFilters["confidenceScore"].FilteredValue)
		assert.Equal(t, original.KalmanFilters["confidenceScore"].Filter.X, decoded.KalmanFilters["confidenceScore"].Filter.X)
		require.NotNil(t, decoded.KalmanFilters["confidenceScore"].Variance)
		assert.Equal(t, original.KalmanFilters["confidenceScore"].Variance.Window, decoded.KalmanFilters["confidenceScore"].Variance.Window)
	})

	t.Run("WithoutKalman", func(t *testing.T) {
		original := AccessMetaEntry{
			TargetID:    "test:node-2",
			TargetScope: ScopeEdge,
			Fixed: AccessMetaFixedFields{
				AccessCount:    5,
				LastAccessedAt: 1713900000000000000,
			},
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original, decoded)
		assert.Nil(t, decoded.KalmanFilters)
	})
}

func TestKalmanPropertyState_Msgpack(t *testing.T) {
	t.Run("WithVariance", func(t *testing.T) {
		original := KalmanPropertyState{
			FilteredValue: 0.72,
			Filter: KalmanFilterState{
				X: 0.72, LastX: 0.70, P: 12.0, K: 0.35,
				E: 0.08, Q: 0.00005, R: 50.0, VarianceScale: 10.0,
				Observations: 100,
			},
			Variance: &VarianceTrackerState{
				Window:    []float64{0.7, 0.71, 0.72, 0.73},
				WindowIdx: 4,
				SumMean:   2.86,
				SumVar:    0.0002,
				Mean:      0.715,
				Variance:  0.00005,
				InverseN:  0.25,
			},
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original, decoded)
	})

	t.Run("WithoutVariance", func(t *testing.T) {
		original := KalmanPropertyState{
			FilteredValue: 0.5,
			Filter: KalmanFilterState{
				X: 0.5, LastX: 0.48, P: 20.0, K: 0.5,
				Q: 0.0001, R: 88.0,
			},
		}
		decoded := roundTrip(t, original)
		assert.Equal(t, original, decoded)
		assert.Nil(t, decoded.Variance)
	})
}

func TestKalmanFilterState_Msgpack(t *testing.T) {
	original := KalmanFilterState{
		X: 5.5, LastX: 5.3, P: 15.0, K: 0.4,
		E: 0.1, Q: 0.0001, R: 88.0, VarianceScale: 10.0,
		Observations: 42,
	}
	decoded := roundTrip(t, original)
	assert.Equal(t, original, decoded)
}

func TestVarianceTrackerState_Msgpack(t *testing.T) {
	original := VarianceTrackerState{
		Window:    []float64{1.0, 2.0, 3.0, 4.0, 5.0},
		WindowIdx: 5,
		SumMean:   15.0,
		SumVar:    10.0,
		Mean:      3.0,
		Variance:  2.0,
		InverseN:  0.2,
	}
	decoded := roundTrip(t, original)
	assert.Equal(t, original, decoded)
}

package knowledgepolicy

import "math"

// ProcessKalmanMutation takes a raw measurement, loads or initializes Kalman
// state from the entity's AccessMetaEntry.KalmanFilters map, runs the filter,
// and returns the filtered value. The entry is modified in place.
//
// Inlined from pkg/filter/kalman.go:375-409 for zero-allocation hot path.
func ProcessKalmanMutation(
	propertyKey string,
	rawMeasurement float64,
	kalmanCfg *KalmanConfig,
	entry *AccessMetaEntry,
) float64 {
	if entry.KalmanFilters == nil {
		entry.KalmanFilters = make(map[string]*KalmanPropertyState)
	}

	state := entry.KalmanFilters[propertyKey]
	if state == nil {
		state = &KalmanPropertyState{
			FilteredValue: rawMeasurement,
			Filter: KalmanFilterState{
				X:             rawMeasurement,
				LastX:         rawMeasurement,
				P:             30.0,
				E:             1.0,
				Q:             kalmanCfg.Q * 0.001,
				R:             kalmanCfg.R,
				VarianceScale: kalmanCfg.VarianceScale,
			},
		}
		entry.KalmanFilters[propertyKey] = state
		return rawMeasurement
	}

	f := &state.Filter

	if kalmanCfg.Mode == KalmanModeAuto {
		updateVarianceTracker(state, kalmanCfg, rawMeasurement)
	}

	// Velocity projection
	velocity := f.X - f.LastX
	f.X += velocity
	f.LastX = f.X

	// Error boosting (setpoint-based, target=0)
	if f.LastX != 0.0 {
		f.E = math.Abs(1.0 - (0.0 / f.LastX))
	} else {
		f.E = 1.0
	}

	// Prediction: increase uncertainty
	f.P = f.P + (f.Q * f.E)

	// Measurement update
	f.K = f.P / (f.P + f.R)
	f.X += f.K * (rawMeasurement - f.X)
	f.P = (1.0 - f.K) * f.P

	f.Observations++

	state.FilteredValue = f.X
	return f.X
}

func updateVarianceTracker(state *KalmanPropertyState, cfg *KalmanConfig, sample float64) {
	windowSize := cfg.WindowSize
	if windowSize <= 0 {
		windowSize = 50
	}

	if state.Variance == nil {
		state.Variance = &VarianceTrackerState{
			Window:   make([]float64, windowSize),
			InverseN: 1.0 / float64(windowSize),
		}
	}

	v := state.Variance

	oldSample := v.Window[v.WindowIdx]
	v.Window[v.WindowIdx] = sample

	v.SumMean += sample - oldSample
	v.SumVar += (sample * sample) - (oldSample * oldSample)

	v.WindowIdx++
	if v.WindowIdx >= len(v.Window) {
		v.WindowIdx = 0
	}

	v.Mean = v.SumMean * v.InverseN
	v.Variance = math.Abs(v.SumVar*v.InverseN - (v.Mean * v.Mean))

	scale := cfg.VarianceScale
	if scale <= 0 {
		scale = 1.0
	}
	state.Filter.R = math.Sqrt(v.Variance) * scale
}

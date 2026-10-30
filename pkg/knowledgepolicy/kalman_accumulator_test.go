package knowledgepolicy

import (
	"math"
	"testing"
)

func TestKalmanMutation_InitializesState(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeManual, Q: 0.05, R: 50.0}

	val := ProcessKalmanMutation("confidence", 0.6, cfg, entry)

	if val != 0.6 {
		t.Errorf("first measurement should seed state, got %f", val)
	}
	state := entry.KalmanFilters["confidence"]
	if state == nil {
		t.Fatal("expected KalmanPropertyState")
	}
	if state.Filter.X != 0.6 {
		t.Errorf("X should be 0.6, got %f", state.Filter.X)
	}
	if state.Filter.P != 30.0 {
		t.Errorf("P should be 30.0, got %f", state.Filter.P)
	}
}

func TestKalmanMutation_ManualR_Convergence(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeManual, Q: 0.05, R: 50.0}

	target := 5.0
	for i := 0; i < 100; i++ {
		ProcessKalmanMutation("metric", target, cfg, entry)
	}

	state := entry.KalmanFilters["metric"]
	if math.Abs(state.FilteredValue-target) > 0.5 {
		t.Errorf("should converge to ~%f, got %f", target, state.FilteredValue)
	}
}

func TestKalmanMutation_SpikeDampening(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeManual, Q: 0.05, R: 50.0}

	for i := 0; i < 50; i++ {
		ProcessKalmanMutation("confidence", 5.0, cfg, entry)
	}

	preSpike := entry.KalmanFilters["confidence"].FilteredValue

	ProcessKalmanMutation("confidence", 100.0, cfg, entry)
	afterSpike := entry.KalmanFilters["confidence"].FilteredValue

	if afterSpike > preSpike*3 {
		t.Errorf("spike should be dampened: preSpike=%f, afterSpike=%f", preSpike, afterSpike)
	}

	for i := 0; i < 50; i++ {
		ProcessKalmanMutation("confidence", 5.0, cfg, entry)
	}

	recovered := entry.KalmanFilters["confidence"].FilteredValue
	if math.Abs(recovered-5.0) > 5.0 {
		t.Errorf("should recover after spike, got %f", recovered)
	}
}

func TestKalmanMutation_AutoR_VarianceTrackerInitialized(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeAuto, Q: 0.05, R: 50.0, VarianceScale: 2.0, WindowSize: 10}

	ProcessKalmanMutation("metric", 5.0, cfg, entry)
	ProcessKalmanMutation("metric", 5.1, cfg, entry)

	state := entry.KalmanFilters["metric"]
	if state.Variance == nil {
		t.Fatal("expected VarianceTracker for auto-R mode")
	}
	if len(state.Variance.Window) != 10 {
		t.Errorf("expected window size 10, got %d", len(state.Variance.Window))
	}
}

func TestKalmanMutation_AutoR_AdjustsR(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeAuto, Q: 0.05, R: 50.0, VarianceScale: 2.0, WindowSize: 10}

	ProcessKalmanMutation("metric", 5.0, cfg, entry)

	for i := 0; i < 20; i++ {
		ProcessKalmanMutation("metric", 5.0, cfg, entry)
	}
	rStable := entry.KalmanFilters["metric"].Filter.R

	for i := 0; i < 20; i++ {
		ProcessKalmanMutation("metric", float64(i)*10, cfg, entry)
	}
	rNoisy := entry.KalmanFilters["metric"].Filter.R

	if rNoisy <= rStable {
		t.Errorf("R should increase with noisy input: stable=%f, noisy=%f", rStable, rNoisy)
	}
}

func TestKalmanMutation_WindowWraps(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeAuto, Q: 0.05, R: 50.0, VarianceScale: 1.0, WindowSize: 5}

	ProcessKalmanMutation("m", 1.0, cfg, entry)

	for i := 0; i < 20; i++ {
		ProcessKalmanMutation("m", float64(i), cfg, entry)
	}

	state := entry.KalmanFilters["m"]
	if state.Variance.WindowIdx >= len(state.Variance.Window) {
		t.Errorf("WindowIdx should wrap, got %d", state.Variance.WindowIdx)
	}
}

func TestKalmanMutation_ManualR_NoVarianceTracker(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeManual, Q: 0.05, R: 50.0}

	ProcessKalmanMutation("metric", 5.0, cfg, entry)
	ProcessKalmanMutation("metric", 5.1, cfg, entry)

	state := entry.KalmanFilters["metric"]
	if state.Variance != nil {
		t.Error("manual-R mode should not create VarianceTracker")
	}
}

func TestKalmanMutation_ObservationsIncrement(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeManual, Q: 0.05, R: 50.0}

	ProcessKalmanMutation("m", 1.0, cfg, entry)
	for i := 0; i < 10; i++ {
		ProcessKalmanMutation("m", 1.0, cfg, entry)
	}

	state := entry.KalmanFilters["m"]
	if state.Filter.Observations != 10 {
		t.Errorf("expected 10 observations (first is seed), got %d", state.Filter.Observations)
	}
}

func TestKalmanMutation_DefaultWindowSize(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeAuto, Q: 0.05, R: 50.0, VarianceScale: 1.0}

	ProcessKalmanMutation("m", 1.0, cfg, entry)
	ProcessKalmanMutation("m", 1.0, cfg, entry)

	state := entry.KalmanFilters["m"]
	if len(state.Variance.Window) != 50 {
		t.Errorf("default window size should be 50, got %d", len(state.Variance.Window))
	}
}

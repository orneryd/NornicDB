package knowledgepolicy

import (
	"math"
	"testing"
)

func TestKalmanAntiSycophancy_HallucinatedSpike(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeManual, Q: 0.05, R: 50.0}

	for i := 0; i < 50; i++ {
		ProcessKalmanMutation("confidenceScore", 0.6, cfg, entry)
	}

	preSpike := entry.KalmanFilters["confidenceScore"].FilteredValue
	if math.Abs(preSpike-0.6) > 0.2 {
		t.Fatalf("pre-spike: expected ~0.6, got %f", preSpike)
	}

	ProcessKalmanMutation("confidenceScore", 0.99, cfg, entry)

	afterSpike := entry.KalmanFilters["confidenceScore"].FilteredValue
	if afterSpike > 0.8 {
		t.Errorf("hallucinated spike should be dampened: %f", afterSpike)
	}

	for i := 0; i < 5; i++ {
		ProcessKalmanMutation("confidenceScore", 0.6, cfg, entry)
	}
	recovered := entry.KalmanFilters["confidenceScore"].FilteredValue
	if math.Abs(recovered-0.6) > 0.15 {
		t.Errorf("expected recovery to ~0.6 by access 55, got %f", recovered)
	}
}

func TestKalmanAntiSycophancy_PolicyExposesDiagnostics(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeManual, Q: 0.05, R: 50.0}

	ProcessKalmanMutation("confidenceScore", 0.6, cfg, entry)
	for i := 0; i < 10; i++ {
		ProcessKalmanMutation("confidenceScore", 0.6, cfg, entry)
	}

	state := entry.KalmanFilters["confidenceScore"]
	if state == nil {
		t.Fatal("expected KalmanPropertyState")
	}
	if state.FilteredValue == 0 {
		t.Error("filteredValue should be non-zero")
	}
	if state.Filter.Observations != 10 {
		t.Errorf("expected 10 observations, got %d", state.Filter.Observations)
	}
	if state.Filter.K == 0 {
		t.Error("Kalman gain should be non-zero after processing")
	}
	if state.Filter.P == 0 {
		t.Error("covariance should be non-zero")
	}
}

func TestKalmanAntiSycophancy_SustainedSycophancyDampened(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeManual, Q: 0.05, R: 50.0}

	for i := 0; i < 30; i++ {
		ProcessKalmanMutation("agreement", 0.5, cfg, entry)
	}

	for i := 0; i < 10; i++ {
		ProcessKalmanMutation("agreement", 0.95, cfg, entry)
	}

	val := entry.KalmanFilters["agreement"].FilteredValue
	if val > 0.85 {
		t.Errorf("sustained sycophancy spike should be smoothed: %f", val)
	}
}

func TestKalmanAntiSycophancy_GenuineTrendTracked(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeManual, Q: 0.05, R: 10.0}

	for i := 0; i < 50; i++ {
		ProcessKalmanMutation("quality", 0.4, cfg, entry)
	}

	for i := 0; i < 100; i++ {
		ProcessKalmanMutation("quality", 0.8, cfg, entry)
	}

	val := entry.KalmanFilters["quality"].FilteredValue
	if val < 0.65 {
		t.Errorf("genuine persistent trend should be tracked: %f", val)
	}
}

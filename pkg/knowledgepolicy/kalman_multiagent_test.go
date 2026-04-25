package knowledgepolicy

import (
	"math"
	"testing"
)

// simulateSessionGatedAccess simulates the CASE WHEN n._lastSessionId <> $_session
// pattern from the plan. If the session is new, the measurement increments; otherwise
// it stays at the current value (gated out).
func simulateSessionGatedAccess(
	entry *AccessMetaEntry,
	sessionID string,
	cfg *KalmanConfig,
) float64 {
	if entry.Overflow == nil {
		entry.Overflow = make(map[string]interface{})
	}

	lastSession, _ := entry.Overflow["_lastSessionId"].(string)
	currentRate := 0.0
	if state := entry.KalmanFilters["crossSessionAccessRate"]; state != nil {
		currentRate = state.FilteredValue
	}

	var rawMeasurement float64
	if lastSession != sessionID {
		rawMeasurement = currentRate + 1.0
	} else {
		rawMeasurement = currentRate
	}

	filtered := ProcessKalmanMutation("crossSessionAccessRate", rawMeasurement, cfg, entry)

	entry.Overflow["_lastSessionId"] = sessionID

	return filtered
}

// TestMultiAgent_SessionGating_PlanTable2 reproduces Table 2 from
// kalman-behavioral-signals.md: 50 accesses from session-A (all gated out after
// the first), then new sessions B and C cause genuine increments smoothed by Kalman.
func TestMultiAgent_SessionGating_PlanTable2(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeAuto, Q: 0.1, R: 88.0, VarianceScale: 1.0, WindowSize: 50}

	// Access 1: session-A — new session, counts
	val := simulateSessionGatedAccess(entry, "session-A", cfg)
	if math.Abs(val-1.0) > 1e-9 {
		t.Errorf("access 1: expected 1.0 (first measurement seeds filter), got %f", val)
	}

	// Accesses 2-50: session-A — same session, gated out (measurement = current value)
	for i := 2; i <= 50; i++ {
		val = simulateSessionGatedAccess(entry, "session-A", cfg)
	}
	if math.Abs(val-1.0) > 0.3 {
		t.Errorf("access 50: expected ~1.0 (no new sessions), got %f", val)
	}

	// Access 51: session-B — new session, increment
	val = simulateSessionGatedAccess(entry, "session-B", cfg)
	if val <= 1.0 {
		t.Errorf("access 51 (new session-B): expected > 1.0, got %f", val)
	}
	if val > 2.5 {
		t.Errorf("access 51: Kalman should smooth increment, not jump to 2.0: got %f", val)
	}

	// Access 52: session-C — another new session, Kalman smooths the increment
	val = simulateSessionGatedAccess(entry, "session-C", cfg)
	if val <= 1.05 {
		t.Errorf("access 52 (new session-C): expected > 1.05 (genuine reinforcement), got %f", val)
	}
}

// TestMultiAgent_SessionGating_SameSessionNeverIncrements verifies that 1000
// accesses from the same session never move the gated counter past 1.0.
func TestMultiAgent_SessionGating_SameSessionNeverIncrements(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeAuto, Q: 0.1, R: 88.0, VarianceScale: 1.0, WindowSize: 50}

	var val float64
	for i := 0; i < 1000; i++ {
		val = simulateSessionGatedAccess(entry, "session-A", cfg)
	}

	if math.Abs(val-1.0) > 0.3 {
		t.Errorf("1000 same-session accesses should stay near 1.0, got %f", val)
	}
}

// TestMultiAgent_IndependentKalmanProperties verifies that confidenceScore
// (manual-R Kalman) and crossSessionAccessRate (auto-R Kalman) maintain
// independent filter states on the same AccessMetaEntry.
func TestMultiAgent_IndependentKalmanProperties(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	confCfg := &KalmanConfig{Mode: KalmanModeManual, Q: 0.05, R: 50.0}
	rateCfg := &KalmanConfig{Mode: KalmanModeAuto, Q: 0.1, R: 88.0, VarianceScale: 1.0, WindowSize: 50}

	// Process confidence: stable at 0.6
	for i := 0; i < 20; i++ {
		ProcessKalmanMutation("confidenceScore", 0.6, confCfg, entry)
	}

	// Process rate: 5 different sessions
	for i := 0; i < 5; i++ {
		simulateSessionGatedAccess(entry, "session-"+string(rune('A'+i)), rateCfg)
	}

	confState := entry.KalmanFilters["confidenceScore"]
	rateState := entry.KalmanFilters["crossSessionAccessRate"]

	if confState == nil || rateState == nil {
		t.Fatal("expected both Kalman states to exist")
	}

	if math.Abs(confState.FilteredValue-0.6) > 0.2 {
		t.Errorf("confidence should be ~0.6, got %f", confState.FilteredValue)
	}

	if rateState.FilteredValue < 1.0 {
		t.Errorf("rate should be >= 1.0 after 5 sessions, got %f", rateState.FilteredValue)
	}

	// Verify filter parameters are independent
	if confState.Filter.R == rateState.Filter.R {
		t.Errorf("manual and auto R should differ: conf.R=%f rate.R=%f",
			confState.Filter.R, rateState.Filter.R)
	}

	if confState.Variance != nil {
		t.Error("manual-mode confidenceScore should not have a variance tracker")
	}

	if rateState.Variance == nil {
		t.Error("auto-mode crossSessionAccessRate should have a variance tracker")
	}
}

// TestMultiAgent_AgentTracking verifies that _lastAgentId is correctly tracked
// in the Overflow map alongside Kalman state.
func TestMultiAgent_AgentTracking(t *testing.T) {
	entry := &AccessMetaEntry{
		TargetID: "n1",
		Overflow: make(map[string]interface{}),
	}
	confCfg := &KalmanConfig{Mode: KalmanModeManual, Q: 0.05, R: 50.0}

	agents := []string{"agent-planner", "agent-coder", "agent-reviewer"}
	for _, agent := range agents {
		ProcessKalmanMutation("confidenceScore", 0.6, confCfg, entry)
		entry.Overflow["_lastAgentId"] = agent
		entry.Overflow["_lastSessionId"] = "session-" + agent
	}

	lastAgent := entry.Overflow["_lastAgentId"]
	if lastAgent != "agent-reviewer" {
		t.Errorf("expected last agent 'agent-reviewer', got %v", lastAgent)
	}

	lastSession := entry.Overflow["_lastSessionId"]
	if lastSession != "session-agent-reviewer" {
		t.Errorf("expected last session 'session-agent-reviewer', got %v", lastSession)
	}

	if entry.KalmanFilters["confidenceScore"].Filter.Observations != 2 {
		t.Errorf("expected 2 observations (first seeds, 2 updates), got %d",
			entry.KalmanFilters["confidenceScore"].Filter.Observations)
	}
}

// TestMultiAgent_KalmanSmooths_NoisyMultiAgentConfidence simulates a scenario
// where 3 agents each provide noisy confidence evaluations and the Kalman filter
// smooths the noise to track the true underlying signal.
func TestMultiAgent_KalmanSmooths_NoisyMultiAgentConfidence(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeManual, Q: 0.05, R: 50.0}

	// True underlying confidence is ~0.7
	// Three agents send noisy evaluations
	agentMeasurements := map[string][]float64{
		"agent-A": {0.65, 0.72, 0.68, 0.75, 0.70, 0.63, 0.71, 0.69, 0.74, 0.66},
		"agent-B": {0.73, 0.60, 0.78, 0.55, 0.80, 0.68, 0.71, 0.66, 0.72, 0.69},
		"agent-C": {0.70, 0.70, 0.70, 0.70, 0.70, 0.70, 0.70, 0.70, 0.70, 0.70},
	}

	for round := 0; round < 10; round++ {
		for _, agent := range []string{"agent-A", "agent-B", "agent-C"} {
			ProcessKalmanMutation("confidenceScore", agentMeasurements[agent][round], cfg, entry)
			if entry.Overflow == nil {
				entry.Overflow = make(map[string]interface{})
			}
			entry.Overflow["_lastAgentId"] = agent
		}
	}

	filteredConf := entry.KalmanFilters["confidenceScore"].FilteredValue
	// After 30 noisy measurements centered around 0.7, filter should converge near 0.7
	if math.Abs(filteredConf-0.7) > 0.15 {
		t.Errorf("after 30 noisy measurements around 0.7, filter should be near 0.7, got %f", filteredConf)
	}
}

// TestMultiAgent_HallucinationRejection_PlanTable1 reproduces Table 1 from
// kalman-behavioral-signals.md: stable signal at 0.6, then a hallucinated spike
// to 0.99, then recovery.
func TestMultiAgent_HallucinationRejection_PlanTable1(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeManual, Q: 0.05, R: 50.0}

	// Accesses 1-4: stable at ~0.60
	vals := []float64{0.60, 0.62, 0.58, 0.61}
	var filtered float64
	for _, v := range vals {
		filtered = ProcessKalmanMutation("confidenceScore", v, cfg, entry)
	}
	if math.Abs(filtered-0.60) > 0.1 {
		t.Errorf("after 4 stable measurements, expected ~0.60, got %f", filtered)
	}

	// Access 5: hallucinated spike to 0.99
	filtered = ProcessKalmanMutation("confidenceScore", 0.99, cfg, entry)
	if filtered > 0.80 {
		t.Errorf("hallucinated spike should be dampened below 0.80, got %f", filtered)
	}

	// Access 6-7: recovery to normal
	for _, v := range []float64{0.59, 0.61} {
		filtered = ProcessKalmanMutation("confidenceScore", v, cfg, entry)
	}
	if math.Abs(filtered-0.62) > 0.15 {
		t.Errorf("after recovery, expected ~0.62, got %f", filtered)
	}
}

// TestMultiAgent_CrossSessionEdgeTraversal simulates the edge-level signal
// smoothing pattern from Example 3 in kalman-behavioral-signals.md.
func TestMultiAgent_CrossSessionEdgeTraversal(t *testing.T) {
	entry := &AccessMetaEntry{TargetID: "e1"}
	cfg := &KalmanConfig{Mode: KalmanModeAuto, Q: 0.1, R: 88.0, VarianceScale: 1.0, WindowSize: 50}

	// 100 traversals from session-A (only first counts)
	for i := 0; i < 100; i++ {
		simulateSessionGatedAccess(entry, "session-A", cfg)
	}
	afterSessionA := entry.KalmanFilters["crossSessionAccessRate"].FilteredValue
	if math.Abs(afterSessionA-1.0) > 0.3 {
		t.Errorf("after 100 traversals from same session, expected ~1.0, got %f", afterSessionA)
	}

	// 3 new sessions with genuine cross-session support
	for i := 0; i < 3; i++ {
		simulateSessionGatedAccess(entry, "session-"+string(rune('B'+i)), cfg)
	}
	afterNewSessions := entry.KalmanFilters["crossSessionAccessRate"].FilteredValue
	if afterNewSessions <= afterSessionA {
		t.Errorf("new sessions should increase rate: before=%f after=%f",
			afterSessionA, afterNewSessions)
	}
}

// TestMultiAgent_FullEpisodicRecallScenario simulates the complete
// episodic_recall_quality pattern from the plan with multiple agents,
// session gating, and Kalman filtering on multiple properties simultaneously.
func TestMultiAgent_FullEpisodicRecallScenario(t *testing.T) {
	entry := &AccessMetaEntry{
		TargetID: "n1",
		Overflow: make(map[string]interface{}),
	}
	confCfg := &KalmanConfig{Mode: KalmanModeManual, Q: 0.05, R: 50.0}
	rateCfg := &KalmanConfig{Mode: KalmanModeAuto, Q: 0.1, R: 88.0, VarianceScale: 1.0, WindowSize: 50}

	type access struct {
		session    string
		agent      string
		confidence float64
	}

	// Simulate a realistic multi-agent scenario:
	// - Agent-planner accesses 10 times in session-1 (sycophantic loop)
	// - Agent-coder accesses 3 times in session-2 with consistent confidence
	// - Agent-reviewer accesses 2 times in session-3 with high confidence
	accesses := []access{
		// Session 1: agent-planner, 10 rapid accesses
		{"session-1", "agent-planner", 0.65},
		{"session-1", "agent-planner", 0.67},
		{"session-1", "agent-planner", 0.64},
		{"session-1", "agent-planner", 0.66},
		{"session-1", "agent-planner", 0.63},
		{"session-1", "agent-planner", 0.68},
		{"session-1", "agent-planner", 0.65},
		{"session-1", "agent-planner", 0.64},
		{"session-1", "agent-planner", 0.67},
		{"session-1", "agent-planner", 0.66},
		// Session 2: agent-coder
		{"session-2", "agent-coder", 0.72},
		{"session-2", "agent-coder", 0.70},
		{"session-2", "agent-coder", 0.71},
		// Session 3: agent-reviewer
		{"session-3", "agent-reviewer", 0.80},
		{"session-3", "agent-reviewer", 0.82},
	}

	var accessCount int64
	for _, a := range accesses {
		accessCount++
		entry.Fixed.AccessCount = accessCount
		entry.Fixed.LastAccessedAt = testNow + int64(accessCount)*1e9

		ProcessKalmanMutation("confidenceScore", a.confidence, confCfg, entry)
		simulateSessionGatedAccess(entry, a.session, rateCfg)

		entry.Overflow["_lastSessionId"] = a.session
		entry.Overflow["_lastAgentId"] = a.agent
	}

	// Verify final state
	confFiltered := entry.KalmanFilters["confidenceScore"].FilteredValue
	rateFiltered := entry.KalmanFilters["crossSessionAccessRate"].FilteredValue

	// Confidence should track somewhere around the mean of all measurements (~0.69)
	if math.Abs(confFiltered-0.69) > 0.15 {
		t.Errorf("confidence should be near ~0.69, got %f", confFiltered)
	}

	// Rate should reflect 3 unique sessions, heavily smoothed by Kalman with high R
	if rateFiltered < 1.05 {
		t.Errorf("cross-session rate should reflect 3 sessions (smoothed > 1.05), got %f", rateFiltered)
	}

	// Access count should be 15
	if entry.Fixed.AccessCount != 15 {
		t.Errorf("expected 15 total accesses, got %d", entry.Fixed.AccessCount)
	}

	// Last agent should be agent-reviewer
	if entry.Overflow["_lastAgentId"] != "agent-reviewer" {
		t.Errorf("expected last agent 'agent-reviewer', got %v", entry.Overflow["_lastAgentId"])
	}

	// Last session should be session-3
	if entry.Overflow["_lastSessionId"] != "session-3" {
		t.Errorf("expected last session 'session-3', got %v", entry.Overflow["_lastSessionId"])
	}

	// Verify both filters have independent variance state
	confState := entry.KalmanFilters["confidenceScore"]
	rateState := entry.KalmanFilters["crossSessionAccessRate"]

	if confState.Variance != nil {
		t.Error("manual-mode confidence should not have variance tracker")
	}
	if rateState.Variance == nil {
		t.Error("auto-mode rate should have variance tracker")
	}
}

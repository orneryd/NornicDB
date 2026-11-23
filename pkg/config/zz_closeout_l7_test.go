package config

// Close-out lane L7 (scratch branch only; test-side, not product code).
// L-H7-011: TestPerNodeConfig_EnableDisable asserts that the per-node config
// flag is off when it starts, but the process default set by init()
// (feature_flags.go, "Per-Node Config: enabled by default") is on. It passes
// only when an earlier test has called ResetFeatureFlags.
//
// Run alone (-run '^TestL7011'), so the process state is the init() state:
//   TestL7011AVictimAtProcessStart  expected FAIL at feature_flags_auto_integration_test.go:222
//   TestL7011BVictimAfterReset      expected PASS (the victim's own deferred reset ran in A)
//   TestL7011CVictimAfterExplicitReset expected PASS (bypass: ResetFeatureFlags first)

import "testing"

func TestL7011AVictimAtProcessStart(t *testing.T) {
	t.Logf("L7_011 state at process start: IsPerNodeConfigEnabled=%v (init default)", IsPerNodeConfigEnabled())
	t.Run("TestPerNodeConfig_EnableDisable", TestPerNodeConfig_EnableDisable)
}

func TestL7011BVictimAfterReset(t *testing.T) {
	t.Logf("L7_011 state before victim: IsPerNodeConfigEnabled=%v", IsPerNodeConfigEnabled())
	t.Run("TestPerNodeConfig_EnableDisable", TestPerNodeConfig_EnableDisable)
}

func TestL7011CVictimAfterExplicitReset(t *testing.T) {
	EnablePerNodeConfig()
	t.Logf("L7_011 state set to the init default: IsPerNodeConfigEnabled=%v", IsPerNodeConfigEnabled())
	ResetFeatureFlags()
	t.Logf("L7_011 state after ResetFeatureFlags: IsPerNodeConfigEnabled=%v", IsPerNodeConfigEnabled())
	t.Run("TestPerNodeConfig_EnableDisable", TestPerNodeConfig_EnableDisable)
}

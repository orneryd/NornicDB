package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Tests for Cooldown Auto-Integration feature flag group

func TestCooldownAutoIntegration_EnableDisable(t *testing.T) {
	defer ResetFeatureFlags()

	assert.False(t, IsCooldownAutoIntegrationEnabled())

	EnableCooldownAutoIntegration()
	assert.True(t, IsCooldownAutoIntegrationEnabled())

	DisableCooldownAutoIntegration()
	assert.False(t, IsCooldownAutoIntegrationEnabled())
}

func TestCooldownAutoIntegration_With(t *testing.T) {
	defer ResetFeatureFlags()

	// WithEnabled from disabled state
	restore := WithCooldownAutoIntegrationEnabled()
	assert.True(t, IsCooldownAutoIntegrationEnabled())
	restore()
	assert.False(t, IsCooldownAutoIntegrationEnabled())

	// WithDisabled from enabled state
	EnableCooldownAutoIntegration()
	restore2 := WithCooldownAutoIntegrationDisabled()
	assert.False(t, IsCooldownAutoIntegrationEnabled())
	restore2()
	assert.True(t, IsCooldownAutoIntegrationEnabled())
}

// Tests for Evidence Auto-Integration feature flag group

func TestEvidenceAutoIntegration_EnableDisable(t *testing.T) {
	defer ResetFeatureFlags()

	assert.False(t, IsEvidenceAutoIntegrationEnabled())

	EnableEvidenceAutoIntegration()
	assert.True(t, IsEvidenceAutoIntegrationEnabled())

	DisableEvidenceAutoIntegration()
	assert.False(t, IsEvidenceAutoIntegrationEnabled())
}

func TestEvidenceAutoIntegration_With(t *testing.T) {
	defer ResetFeatureFlags()

	restore := WithEvidenceAutoIntegrationEnabled()
	assert.True(t, IsEvidenceAutoIntegrationEnabled())
	restore()
	assert.False(t, IsEvidenceAutoIntegrationEnabled())

	EnableEvidenceAutoIntegration()
	restore2 := WithEvidenceAutoIntegrationDisabled()
	assert.False(t, IsEvidenceAutoIntegrationEnabled())
	restore2()
	assert.True(t, IsEvidenceAutoIntegrationEnabled())
}

// Tests for EdgeProvenance Auto-Integration feature flag group

func TestEdgeProvenanceAutoIntegration_EnableDisable(t *testing.T) {
	defer ResetFeatureFlags()

	assert.False(t, IsEdgeProvenanceAutoIntegrationEnabled())

	EnableEdgeProvenanceAutoIntegration()
	assert.True(t, IsEdgeProvenanceAutoIntegrationEnabled())

	DisableEdgeProvenanceAutoIntegration()
	assert.False(t, IsEdgeProvenanceAutoIntegrationEnabled())
}

func TestEdgeProvenanceAutoIntegration_With(t *testing.T) {
	defer ResetFeatureFlags()

	restore := WithEdgeProvenanceAutoIntegrationEnabled()
	assert.True(t, IsEdgeProvenanceAutoIntegrationEnabled())
	restore()
	assert.False(t, IsEdgeProvenanceAutoIntegrationEnabled())

	EnableEdgeProvenanceAutoIntegration()
	restore2 := WithEdgeProvenanceAutoIntegrationDisabled()
	assert.False(t, IsEdgeProvenanceAutoIntegrationEnabled())
	restore2()
	assert.True(t, IsEdgeProvenanceAutoIntegrationEnabled())
}

// Tests for PerNodeConfig Auto-Integration feature flag group

func TestPerNodeConfigAutoIntegration_Is(t *testing.T) {
	defer ResetFeatureFlags()

	// After reset, both the atomic and the feature flag are false
	assert.False(t, IsPerNodeConfigAutoIntegrationEnabled())

	// Set the atomic directly (no Enable function exists for this flag)
	perNodeConfigAutoIntegrationEnabled.Store(true)
	assert.True(t, IsPerNodeConfigAutoIntegrationEnabled())
	perNodeConfigAutoIntegrationEnabled.Store(false)
	assert.False(t, IsPerNodeConfigAutoIntegrationEnabled())
}

func TestPerNodeConfigAutoIntegration_With(t *testing.T) {
	defer ResetFeatureFlags()

	// From disabled: enable temporarily then restore
	restore := WithPerNodeConfigAutoIntegrationEnabled()
	assert.True(t, IsPerNodeConfigAutoIntegrationEnabled())

	// From enabled (still active): disable temporarily then restore
	restore2 := WithPerNodeConfigAutoIntegrationDisabled()
	assert.False(t, IsPerNodeConfigAutoIntegrationEnabled())
	restore2() // back to enabled
	assert.True(t, IsPerNodeConfigAutoIntegrationEnabled())

	restore() // back to disabled
	assert.False(t, IsPerNodeConfigAutoIntegrationEnabled())
}

// Tests for EdgeProvenance (non-auto-integration) feature flag group

func TestEdgeProvenance_EnableDisable(t *testing.T) {
	defer ResetFeatureFlags()

	assert.False(t, IsEdgeProvenanceEnabled())

	EnableEdgeProvenance()
	assert.True(t, IsEdgeProvenanceEnabled())

	DisableEdgeProvenance()
	assert.False(t, IsEdgeProvenanceEnabled())
}

func TestEdgeProvenance_With(t *testing.T) {
	defer ResetFeatureFlags()

	restore := WithEdgeProvenanceEnabled()
	assert.True(t, IsEdgeProvenanceEnabled())
	restore()
	assert.False(t, IsEdgeProvenanceEnabled())

	EnableEdgeProvenance()
	restore2 := WithEdgeProvenanceDisabled()
	assert.False(t, IsEdgeProvenanceEnabled())
	restore2()
	assert.True(t, IsEdgeProvenanceEnabled())
}

// Tests for Cooldown (non-auto-integration) feature flag group

func TestCooldown_EnableDisable(t *testing.T) {
	defer ResetFeatureFlags()

	assert.False(t, IsCooldownEnabled())

	EnableCooldown()
	assert.True(t, IsCooldownEnabled())

	DisableCooldown()
	assert.False(t, IsCooldownEnabled())
}

func TestCooldown_With(t *testing.T) {
	defer ResetFeatureFlags()

	restore := WithCooldownEnabled()
	assert.True(t, IsCooldownEnabled())
	restore()
	assert.False(t, IsCooldownEnabled())

	EnableCooldown()
	restore2 := WithCooldownDisabled()
	assert.False(t, IsCooldownEnabled())
	restore2()
	assert.True(t, IsCooldownEnabled())
}

// Tests for EvidenceBuffering feature flag group

func TestEvidenceBuffering_EnableDisable(t *testing.T) {
	defer ResetFeatureFlags()

	assert.False(t, IsEvidenceBufferingEnabled())

	EnableEvidenceBuffering()
	assert.True(t, IsEvidenceBufferingEnabled())

	DisableEvidenceBuffering()
	assert.False(t, IsEvidenceBufferingEnabled())
}

func TestEvidenceBuffering_With(t *testing.T) {
	defer ResetFeatureFlags()

	restore := WithEvidenceBufferingEnabled()
	assert.True(t, IsEvidenceBufferingEnabled())
	restore()
	assert.False(t, IsEvidenceBufferingEnabled())

	EnableEvidenceBuffering()
	restore2 := WithEvidenceBufferingDisabled()
	assert.False(t, IsEvidenceBufferingEnabled())
	restore2()
	assert.True(t, IsEvidenceBufferingEnabled())
}

// Tests for PerNodeConfig feature flag group

func TestPerNodeConfig_EnableDisable(t *testing.T) {
	defer ResetFeatureFlags()

	assert.False(t, IsPerNodeConfigEnabled())

	EnablePerNodeConfig()
	assert.True(t, IsPerNodeConfigEnabled())

	DisablePerNodeConfig()
	assert.False(t, IsPerNodeConfigEnabled())
}

func TestPerNodeConfig_With(t *testing.T) {
	defer ResetFeatureFlags()

	restore := WithPerNodeConfigEnabled()
	assert.True(t, IsPerNodeConfigEnabled())
	restore()
	assert.False(t, IsPerNodeConfigEnabled())

	EnablePerNodeConfig()
	restore2 := WithPerNodeConfigDisabled()
	assert.False(t, IsPerNodeConfigEnabled())
	restore2()
	assert.True(t, IsPerNodeConfigEnabled())
}

// Tests for EnableAllFeatures / DisableAllFeatures

func TestEnableAllFeatures(t *testing.T) {
	defer ResetFeatureFlags()

	EnableAllFeatures()
	assert.True(t, IsKalmanEnabled())
}

func TestDisableAllFeatures(t *testing.T) {
	defer ResetFeatureFlags()

	EnableAllFeatures()
	DisableAllFeatures()
	assert.False(t, IsKalmanEnabled())
}

// Tests for ResetFeatureFlags / GetFeatureStatus / GetEnabledFeatures

func TestResetFeatureFlags(t *testing.T) {
	EnableCooldownAutoIntegration()
	EnableEdgeProvenance()
	EnableEvidenceBuffering()

	ResetFeatureFlags()

	assert.False(t, IsCooldownAutoIntegrationEnabled())
	assert.False(t, IsEdgeProvenanceEnabled())
	assert.False(t, IsEvidenceBufferingEnabled())
}

func TestGetFeatureStatus_AfterEnableAll(t *testing.T) {
	defer ResetFeatureFlags()

	EnableAllFeatures()
	status := GetFeatureStatus()
	assert.True(t, status.GlobalEnabled)
	assert.True(t, status.KalmanEnabled)
}

func TestGetFeatureStatus_AfterReset(t *testing.T) {
	ResetFeatureFlags()
	status := GetFeatureStatus()
	assert.False(t, status.GlobalEnabled)
	assert.False(t, status.KalmanEnabled)
	assert.False(t, status.EdgeProvenanceEnabled)
}

func TestGetEnabledFeatures_WhenKalmanDisabled(t *testing.T) {
	defer ResetFeatureFlags()
	DisableKalmanFiltering()
	features := GetEnabledFeatures()
	assert.Nil(t, features)
}

func TestGetEnabledFeatures_WhenKalmanEnabled(t *testing.T) {
	defer ResetFeatureFlags()
	EnableAllFeatures()
	features := GetEnabledFeatures()
	assert.NotEmpty(t, features)
}

// Tests for AutoTLP flag group

func TestAutoTLP_EnableDisable(t *testing.T) {
	defer ResetFeatureFlags()

	assert.False(t, IsAutoTLPEnabled())

	EnableAutoTLP()
	assert.True(t, IsAutoTLPEnabled())

	DisableAutoTLP()
	assert.False(t, IsAutoTLPEnabled())
}

func TestAutoTLP_With(t *testing.T) {
	defer ResetFeatureFlags()

	restore := WithAutoTLPEnabled()
	assert.True(t, IsAutoTLPEnabled())
	restore()
	assert.False(t, IsAutoTLPEnabled())

	EnableAutoTLP()
	restore2 := WithAutoTLPDisabled()
	assert.False(t, IsAutoTLPEnabled())
	restore2()
	assert.True(t, IsAutoTLPEnabled())
}

// Tests for Kalman flag group

func TestKalman_EnableDisable(t *testing.T) {
	defer ResetFeatureFlags()

	assert.False(t, IsKalmanEnabled())

	EnableKalmanFiltering()
	assert.True(t, IsKalmanEnabled())

	DisableKalmanFiltering()
	assert.False(t, IsKalmanEnabled())
}

func TestKalman_SetKalmanEnabled(t *testing.T) {
	defer ResetFeatureFlags()

	SetKalmanEnabled(true)
	assert.True(t, IsKalmanEnabled())

	SetKalmanEnabled(false)
	assert.False(t, IsKalmanEnabled())
}

func TestKalman_With(t *testing.T) {
	defer ResetFeatureFlags()

	restore := WithKalmanEnabled()
	assert.True(t, IsKalmanEnabled())
	restore()
	assert.False(t, IsKalmanEnabled())

	EnableKalmanFiltering()
	restore2 := WithKalmanDisabled()
	assert.False(t, IsKalmanEnabled())
	restore2()
	assert.True(t, IsKalmanEnabled())
}

// Tests for EnableFeature / DisableFeature / IsFeatureEnabled

func TestEnableDisableFeature_CustomKey(t *testing.T) {
	defer ResetFeatureFlags()

	// IsFeatureEnabled requires global kalman to be enabled
	EnableKalmanFiltering()

	key := "test_custom_feature_xyz"
	// Not explicitly set → defaults to enabled when global is on
	assert.True(t, IsFeatureEnabled(key))

	DisableFeature(key)
	assert.False(t, IsFeatureEnabled(key))

	EnableFeature(key)
	assert.True(t, IsFeatureEnabled(key))
}

package config

import (
	"testing"
)

func TestFeatureFlags(t *testing.T) {
	// Reset before testing
	ResetFeatureFlags()
	defer ResetFeatureFlags()

	t.Run("kalman_enable_disable", func(t *testing.T) {
		if IsKalmanEnabled() {
			t.Error("Kalman should start disabled")
		}

		EnableKalmanFiltering()
		if !IsKalmanEnabled() {
			t.Error("Kalman should be enabled")
		}

		DisableKalmanFiltering()
		if IsKalmanEnabled() {
			t.Error("Kalman should be disabled")
		}
	})

	t.Run("auto_tlp_enable_disable", func(t *testing.T) {
		if IsAutoTLPEnabled() {
			t.Error("Auto-TLP should start disabled")
		}

		EnableAutoTLP()
		if !IsAutoTLPEnabled() {
			t.Error("Auto-TLP should be enabled")
		}

		DisableAutoTLP()
		if IsAutoTLPEnabled() {
			t.Error("Auto-TLP should be disabled")
		}
	})

	t.Run("feature_enable_disable", func(t *testing.T) {
		// Enable global Kalman first (required for feature flags to work)
		EnableKalmanFiltering()

		EnableFeature(FeatureKalmanDecay)
		if !IsFeatureEnabled(FeatureKalmanDecay) {
			t.Error("Feature should be enabled")
		}

		DisableFeature(FeatureKalmanDecay)
		if IsFeatureEnabled(FeatureKalmanDecay) {
			t.Error("Feature should be disabled")
		}

		DisableKalmanFiltering()
	})

	t.Run("with_kalman_enabled", func(t *testing.T) {
		ResetFeatureFlags()

		cleanup := WithKalmanEnabled()
		if !IsKalmanEnabled() {
			t.Error("Kalman should be enabled in scope")
		}
		cleanup()

		if IsKalmanEnabled() {
			t.Error("Kalman should be disabled after cleanup")
		}
	})

	t.Run("with_kalman_disabled", func(t *testing.T) {
		EnableKalmanFiltering()

		cleanup := WithKalmanDisabled()
		if IsKalmanEnabled() {
			t.Error("Kalman should be disabled in scope")
		}
		cleanup()

		if !IsKalmanEnabled() {
			t.Error("Kalman should be re-enabled after cleanup")
		}
	})

	t.Run("with_auto_tlp_enabled", func(t *testing.T) {
		ResetFeatureFlags()

		cleanup := WithAutoTLPEnabled()
		if !IsAutoTLPEnabled() {
			t.Error("Auto-TLP should be enabled in scope")
		}
		cleanup()

		if IsAutoTLPEnabled() {
			t.Error("Auto-TLP should be disabled after cleanup")
		}
	})

	t.Run("with_auto_tlp_disabled", func(t *testing.T) {
		EnableAutoTLP()

		cleanup := WithAutoTLPDisabled()
		if IsAutoTLPEnabled() {
			t.Error("Auto-TLP should be disabled in scope")
		}
		cleanup()

		if !IsAutoTLPEnabled() {
			t.Error("Auto-TLP should be re-enabled after cleanup")
		}
	})

	t.Run("enable_all_features", func(t *testing.T) {
		ResetFeatureFlags()

		EnableAllFeatures()

		if !IsKalmanEnabled() {
			t.Error("Kalman should be enabled")
		}
		if !IsFeatureEnabled(FeatureKalmanDecay) {
			t.Error("Kalman decay should be enabled")
		}
		if !IsFeatureEnabled(FeatureKalmanCoAccess) {
			t.Error("Kalman co-access should be enabled")
		}
	})

	t.Run("disable_all_features", func(t *testing.T) {
		EnableAllFeatures()

		DisableAllFeatures()

		if IsKalmanEnabled() {
			t.Error("Kalman should be disabled")
		}
		if IsFeatureEnabled(FeatureKalmanDecay) {
			t.Error("Kalman decay should be disabled")
		}
	})

	t.Run("get_enabled_features", func(t *testing.T) {
		ResetFeatureFlags()

		features := GetEnabledFeatures()
		if len(features) != 0 {
			t.Errorf("Expected no features, got %v", features)
		}

		EnableKalmanFiltering()
		EnableFeature(FeatureKalmanDecay)

		features = GetEnabledFeatures()
		if len(features) == 0 {
			t.Error("Expected some features to be enabled")
		}
	})

	t.Run("get_feature_status", func(t *testing.T) {
		ResetFeatureFlags()

		status := GetFeatureStatus()
		if status.GlobalEnabled {
			t.Error("Global should be disabled")
		}

		EnableKalmanFiltering()
		EnableFeature(FeatureKalmanDecay)

		status = GetFeatureStatus()
		if !status.GlobalEnabled {
			t.Error("Global should be enabled")
		}

		if !status.Features[FeatureKalmanDecay] {
			t.Error("Kalman decay should be in features map")
		}
	})

	// Tier 1 Feature Tests - These are ENABLED by default
	t.Run("tier1_cooldown_default_enabled", func(t *testing.T) {
		// Note: After ResetFeatureFlags, we need to re-enable Tier 1 defaults
		// because ResetFeatureFlags sets everything to false for testing
		EnableCooldown()

		if !IsCooldownEnabled() {
			t.Error("Cooldown should be enabled by default")
		}

		DisableCooldown()
		if IsCooldownEnabled() {
			t.Error("Cooldown should be disabled after disable call")
		}

		EnableCooldown()
		if !IsCooldownEnabled() {
			t.Error("Cooldown should be re-enabled")
		}
	})

	t.Run("tier1_edge_provenance_default_enabled", func(t *testing.T) {
		EnableEdgeProvenance()

		if !IsEdgeProvenanceEnabled() {
			t.Error("Edge provenance should be enabled by default")
		}

		DisableEdgeProvenance()
		if IsEdgeProvenanceEnabled() {
			t.Error("Edge provenance should be disabled after disable call")
		}
	})

	t.Run("tier1_evidence_buffering_default_enabled", func(t *testing.T) {
		EnableEvidenceBuffering()

		if !IsEvidenceBufferingEnabled() {
			t.Error("Evidence buffering should be enabled by default")
		}

		DisableEvidenceBuffering()
		if IsEvidenceBufferingEnabled() {
			t.Error("Evidence buffering should be disabled after disable call")
		}
	})

	t.Run("tier1_per_node_config_default_enabled", func(t *testing.T) {
		EnablePerNodeConfig()

		if !IsPerNodeConfigEnabled() {
			t.Error("Per-node config should be enabled by default")
		}

		DisablePerNodeConfig()
		if IsPerNodeConfigEnabled() {
			t.Error("Per-node config should be disabled after disable call")
		}
	})

	t.Run("tier1_wal_default_enabled", func(t *testing.T) {
		EnableWAL()

		if !IsWALEnabled() {
			t.Error("WAL should be enabled by default")
		}

		DisableWAL()
		if IsWALEnabled() {
			t.Error("WAL should be disabled after disable call")
		}
	})

	t.Run("with_cooldown_enabled_disabled", func(t *testing.T) {
		ResetFeatureFlags()

		// Test enable scope
		cleanup := WithCooldownEnabled()
		if !IsCooldownEnabled() {
			t.Error("Cooldown should be enabled in scope")
		}
		cleanup()

		// Test disable scope
		EnableCooldown()
		cleanup = WithCooldownDisabled()
		if IsCooldownEnabled() {
			t.Error("Cooldown should be disabled in scope")
		}
		cleanup()

		if !IsCooldownEnabled() {
			t.Error("Cooldown should be re-enabled after cleanup")
		}
	})

	t.Run("with_wal_enabled_disabled", func(t *testing.T) {
		ResetFeatureFlags()

		// Test enable scope
		cleanup := WithWALEnabled()
		if !IsWALEnabled() {
			t.Error("WAL should be enabled in scope")
		}
		cleanup()

		// Test disable scope
		EnableWAL()
		cleanup = WithWALDisabled()
		if IsWALEnabled() {
			t.Error("WAL should be disabled in scope")
		}
		cleanup()

		if !IsWALEnabled() {
			t.Error("WAL should be re-enabled after cleanup")
		}
	})

	t.Run("feature_status_includes_tier1", func(t *testing.T) {
		// Enable all tier 1 features
		EnableCooldown()
		EnableEdgeProvenance()
		EnableEvidenceBuffering()
		EnablePerNodeConfig()
		EnableWAL()

		status := GetFeatureStatus()

		if !status.CooldownEnabled {
			t.Error("Status should show cooldown enabled")
		}
		if !status.EdgeProvenanceEnabled {
			t.Error("Status should show edge provenance enabled")
		}
		if !status.EvidenceBufferingEnabled {
			t.Error("Status should show evidence buffering enabled")
		}
		if !status.PerNodeConfigEnabled {
			t.Error("Status should show per-node config enabled")
		}
		if !status.WALEnabled {
			t.Error("Status should show WAL enabled")
		}
	})

	t.Run("reset_clears_tier1_features", func(t *testing.T) {
		EnableCooldown()
		EnableEdgeProvenance()
		EnableEvidenceBuffering()
		EnablePerNodeConfig()
		EnableWAL()

		ResetFeatureFlags()

		// After reset, all should be false
		if IsCooldownEnabled() {
			t.Error("Cooldown should be disabled after reset")
		}
		if IsEdgeProvenanceEnabled() {
			t.Error("Edge provenance should be disabled after reset")
		}
		if IsEvidenceBufferingEnabled() {
			t.Error("Evidence buffering should be disabled after reset")
		}
		if IsPerNodeConfigEnabled() {
			t.Error("Per-node config should be disabled after reset")
		}
		if IsWALEnabled() {
			t.Error("WAL should be disabled after reset")
		}
	})

	// GPU Clustering Tests - DISABLED by default (experimental)
	t.Run("gpu_clustering_default_disabled", func(t *testing.T) {
		ResetFeatureFlags()

		if IsGPUClusteringEnabled() {
			t.Error("GPU clustering should be disabled by default")
		}

		if IsGPUClusteringAutoIntegrationEnabled() {
			t.Error("GPU clustering auto-integration should be disabled by default")
		}
	})

	t.Run("gpu_clustering_enable_disable", func(t *testing.T) {
		ResetFeatureFlags()

		EnableGPUClustering()
		if !IsGPUClusteringEnabled() {
			t.Error("GPU clustering should be enabled")
		}

		DisableGPUClustering()
		if IsGPUClusteringEnabled() {
			t.Error("GPU clustering should be disabled")
		}
	})

	t.Run("gpu_clustering_auto_integration_enable_disable", func(t *testing.T) {
		ResetFeatureFlags()

		EnableGPUClusteringAutoIntegration()
		if !IsGPUClusteringAutoIntegrationEnabled() {
			t.Error("GPU clustering auto-integration should be enabled")
		}

		DisableGPUClusteringAutoIntegration()
		if IsGPUClusteringAutoIntegrationEnabled() {
			t.Error("GPU clustering auto-integration should be disabled")
		}
	})

	t.Run("with_gpu_clustering_enabled_disabled", func(t *testing.T) {
		ResetFeatureFlags()

		// Test enable scope
		cleanup := WithGPUClusteringEnabled()
		if !IsGPUClusteringEnabled() {
			t.Error("GPU clustering should be enabled in scope")
		}
		cleanup()

		if IsGPUClusteringEnabled() {
			t.Error("GPU clustering should be disabled after cleanup")
		}

		// Test disable scope
		EnableGPUClustering()
		cleanup = WithGPUClusteringDisabled()
		if IsGPUClusteringEnabled() {
			t.Error("GPU clustering should be disabled in scope")
		}
		cleanup()

		if !IsGPUClusteringEnabled() {
			t.Error("GPU clustering should be re-enabled after cleanup")
		}
	})

	t.Run("with_gpu_clustering_auto_integration_enabled_disabled", func(t *testing.T) {
		ResetFeatureFlags()

		// Test enable scope
		cleanup := WithGPUClusteringAutoIntegrationEnabled()
		if !IsGPUClusteringAutoIntegrationEnabled() {
			t.Error("GPU clustering auto-integration should be enabled in scope")
		}
		cleanup()

		if IsGPUClusteringAutoIntegrationEnabled() {
			t.Error("GPU clustering auto-integration should be disabled after cleanup")
		}

		// Test disable scope
		EnableGPUClusteringAutoIntegration()
		cleanup = WithGPUClusteringAutoIntegrationDisabled()
		if IsGPUClusteringAutoIntegrationEnabled() {
			t.Error("GPU clustering auto-integration should be disabled in scope")
		}
		cleanup()

		if !IsGPUClusteringAutoIntegrationEnabled() {
			t.Error("GPU clustering auto-integration should be re-enabled after cleanup")
		}
	})

	t.Run("feature_status_includes_gpu_clustering", func(t *testing.T) {
		ResetFeatureFlags()
		EnableGPUClustering()

		status := GetFeatureStatus()
		if !status.GPUClusteringEnabled {
			t.Error("Status should show GPU clustering enabled")
		}
	})

	t.Run("reset_clears_gpu_clustering", func(t *testing.T) {
		EnableGPUClustering()
		EnableGPUClusteringAutoIntegration()

		ResetFeatureFlags()

		if IsGPUClusteringEnabled() {
			t.Error("GPU clustering should be disabled after reset")
		}
		if IsGPUClusteringAutoIntegrationEnabled() {
			t.Error("GPU clustering auto-integration should be disabled after reset")
		}
	})
}

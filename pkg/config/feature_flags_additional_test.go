package config

import "testing"

func TestFeatureFlags_SetKalmanEnabled(t *testing.T) {
	ResetFeatureFlags()
	defer ResetFeatureFlags()

	SetKalmanEnabled(true)
	if !IsKalmanEnabled() {
		t.Fatalf("expected kalman enabled after SetKalmanEnabled(true)")
	}
	SetKalmanEnabled(false)
	if IsKalmanEnabled() {
		t.Fatalf("expected kalman disabled after SetKalmanEnabled(false)")
	}
}

func TestFeatureFlags_EdgeDecayTogglesAndScopes(t *testing.T) {
	ResetFeatureFlags()
	defer ResetFeatureFlags()

	if IsEdgeDecayEnabled() {
		t.Fatalf("edge decay should start disabled after reset")
	}

	EnableEdgeDecay()
	if !IsEdgeDecayEnabled() {
		t.Fatalf("expected edge decay enabled")
	}
	DisableEdgeDecay()
	if IsEdgeDecayEnabled() {
		t.Fatalf("expected edge decay disabled")
	}

	cleanup := WithEdgeDecayEnabled()
	if !IsEdgeDecayEnabled() {
		t.Fatalf("expected edge decay enabled in scoped helper")
	}
	cleanup()
	if IsEdgeDecayEnabled() {
		t.Fatalf("expected scoped cleanup to restore disabled state")
	}

	EnableEdgeDecay()
	cleanup = WithEdgeDecayDisabled()
	if IsEdgeDecayEnabled() {
		t.Fatalf("expected edge decay disabled in scoped helper")
	}
	cleanup()
	if !IsEdgeDecayEnabled() {
		t.Fatalf("expected scoped cleanup to restore enabled state")
	}
}

func TestFeatureFlags_AutoTLPLLMFlagsAndScopes(t *testing.T) {
	ResetFeatureFlags()
	defer ResetFeatureFlags()

	if IsAutoTLPLLMQCEnabled() || IsAutoTLPLLMAugmentEnabled() {
		t.Fatalf("LLM flags should start disabled after reset")
	}

	EnableAutoTLPLLMQC()
	if !IsAutoTLPLLMQCEnabled() {
		t.Fatalf("expected qc enabled")
	}
	DisableAutoTLPLLMQC()
	if IsAutoTLPLLMQCEnabled() {
		t.Fatalf("expected qc disabled")
	}

	cleanupQC := WithAutoTLPLLMQCEnabled()
	if !IsAutoTLPLLMQCEnabled() {
		t.Fatalf("expected qc enabled in scope")
	}
	cleanupQC()
	if IsAutoTLPLLMQCEnabled() {
		t.Fatalf("expected qc restored after scope")
	}

	EnableAutoTLPLLMQC()
	cleanupQC = WithAutoTLPLLMQCDisabled()
	if IsAutoTLPLLMQCEnabled() {
		t.Fatalf("expected qc disabled in scope")
	}
	cleanupQC()
	if !IsAutoTLPLLMQCEnabled() {
		t.Fatalf("expected qc restored to enabled state")
	}

	EnableAutoTLPLLMAugment()
	if !IsAutoTLPLLMAugmentEnabled() {
		t.Fatalf("expected augment enabled")
	}
	DisableAutoTLPLLMAugment()
	if IsAutoTLPLLMAugmentEnabled() {
		t.Fatalf("expected augment disabled")
	}

	cleanupAug := WithAutoTLPLLMAugmentEnabled()
	if !IsAutoTLPLLMAugmentEnabled() {
		t.Fatalf("expected augment enabled in scope")
	}
	cleanupAug()
	if IsAutoTLPLLMAugmentEnabled() {
		t.Fatalf("expected augment restored after scope")
	}

	EnableAutoTLPLLMAugment()
	cleanupAug = WithAutoTLPLLMAugmentDisabled()
	if IsAutoTLPLLMAugmentEnabled() {
		t.Fatalf("expected augment disabled in scope")
	}
	cleanupAug()
	if !IsAutoTLPLLMAugmentEnabled() {
		t.Fatalf("expected augment restored to enabled state")
	}
}

func TestFeatureFlags_ParserTypeSettersAndScopes(t *testing.T) {
	prev := GetParserType()
	defer SetParserType(prev)

	SetParserType("antlr")
	if !IsANTLRParser() || IsNornicParser() {
		t.Fatalf("expected antlr parser after SetParserType(antlr)")
	}

	SetParserType("nornic")
	if !IsNornicParser() || IsANTLRParser() {
		t.Fatalf("expected nornic parser after SetParserType(nornic)")
	}

	// Unknown parser must fall back to nornic.
	SetParserType("unknown")
	if !IsNornicParser() {
		t.Fatalf("expected fallback to nornic for unknown parser type")
	}

	cleanupANTLR := WithANTLRParser()
	if !IsANTLRParser() {
		t.Fatalf("expected antlr parser in WithANTLRParser scope")
	}
	cleanupANTLR()
	if !IsNornicParser() {
		t.Fatalf("expected parser restored after WithANTLRParser cleanup")
	}

	SetParserType("antlr")
	cleanupNornic := WithNornicParser()
	if !IsNornicParser() {
		t.Fatalf("expected nornic parser in WithNornicParser scope")
	}
	cleanupNornic()
	if !IsANTLRParser() {
		t.Fatalf("expected parser restored to antlr after cleanup")
	}
}

func TestFeatureFlags_GPUClusteringFlagsAndScopes(t *testing.T) {
	ResetFeatureFlags()
	defer ResetFeatureFlags()

	if IsGPUClusteringEnabled() {
		t.Fatalf("gpu clustering should start disabled after reset")
	}

	EnableGPUClustering()
	if !IsGPUClusteringEnabled() {
		t.Fatalf("expected gpu clustering enabled")
	}
	DisableGPUClustering()
	if IsGPUClusteringEnabled() {
		t.Fatalf("expected gpu clustering disabled")
	}

	cleanup := WithGPUClusteringEnabled()
	if !IsGPUClusteringEnabled() {
		t.Fatalf("expected gpu clustering enabled in scoped helper")
	}
	cleanup()
	if IsGPUClusteringEnabled() {
		t.Fatalf("expected scoped cleanup to restore disabled state")
	}

	EnableGPUClustering()
	cleanup = WithGPUClusteringDisabled()
	if IsGPUClusteringEnabled() {
		t.Fatalf("expected gpu clustering disabled in scoped helper")
	}
	cleanup()
	if !IsGPUClusteringEnabled() {
		t.Fatalf("expected scoped cleanup to restore enabled state")
	}
}

func TestFeatureFlags_GPUAutoIntegrationFlagsAndScopes(t *testing.T) {
	ResetFeatureFlags()
	defer ResetFeatureFlags()

	if IsGPUClusteringAutoIntegrationEnabled() {
		t.Fatalf("gpu auto-integration should start disabled after reset")
	}

	EnableGPUClusteringAutoIntegration()
	if !IsGPUClusteringAutoIntegrationEnabled() {
		t.Fatalf("expected gpu auto-integration enabled")
	}
	DisableGPUClusteringAutoIntegration()
	if IsGPUClusteringAutoIntegrationEnabled() {
		t.Fatalf("expected gpu auto-integration disabled")
	}

	cleanup := WithGPUClusteringAutoIntegrationEnabled()
	if !IsGPUClusteringAutoIntegrationEnabled() {
		t.Fatalf("expected gpu auto-integration enabled in scoped helper")
	}
	cleanup()
	if IsGPUClusteringAutoIntegrationEnabled() {
		t.Fatalf("expected scoped cleanup to restore disabled state")
	}

	EnableGPUClusteringAutoIntegration()
	cleanup = WithGPUClusteringAutoIntegrationDisabled()
	if IsGPUClusteringAutoIntegrationEnabled() {
		t.Fatalf("expected gpu auto-integration disabled in scoped helper")
	}
	cleanup()
	if !IsGPUClusteringAutoIntegrationEnabled() {
		t.Fatalf("expected scoped cleanup to restore enabled state")
	}
}

// Feature flags for experimental functionality in NornicDB.
//
// Centralized feature flag management. All flags are loaded from environment
// variables via Config.Features and can be toggled at runtime for testing.
//
// DEFAULTS:
//   - Tier 1 features (Cooldown, Edge Provenance, Evidence Buffering, Per-Node Config, WAL)
//     are ENABLED BY DEFAULT for production safety
//   - Kalman and Topology features are DISABLED by default (experimental)
//
// Usage:
//
//	// Load from environment
//	config := config.LoadFromEnv()
//	if config.Features.KalmanEnabled {
//		// Use Kalman filtering
//	}
//
//	// Runtime toggles (for tests)
//	config.EnableKalmanFiltering()
//	if config.IsKalmanEnabled() { ... }
//
// Environment variables (to ENABLE experimental features):
//
//	NORNICDB_KALMAN_ENABLED=true
//	NORNICDB_TOPOLOGY_LINK_PREDICTION_ENABLED=true
//
// Environment variables (to DISABLE default-on features if problems occur):
//
//	NORNICDB_EDGE_PROVENANCE_ENABLED=false
//	NORNICDB_COOLDOWN_ENABLED=false
//	NORNICDB_EVIDENCE_BUFFERING_ENABLED=false
//	NORNICDB_PER_NODE_CONFIG_ENABLED=false
//	NORNICDB_WAL_ENABLED=false
package config

import (
	"os"
	"sync"
	"sync/atomic"
	// Note: Import config package for centralized flags
	// Commented to avoid circular import in some cases
	// Use globalConfig pattern instead
)

// Feature flag keys
const (
	// EnvKalmanEnabled is the environment variable to enable Kalman filtering
	EnvKalmanEnabled = "NORNICDB_KALMAN_ENABLED"

	// EnvTopologyLinkPredictionEnabled is the environment variable to enable topology link prediction
	EnvTopologyLinkPredictionEnabled = "NORNICDB_TOPOLOGY_LINK_PREDICTION_ENABLED"

	// EnvCooldownAutoIntegrationEnabled is the environment variable to enable automatic cooldown in inference
	EnvCooldownAutoIntegrationEnabled = "NORNICDB_COOLDOWN_AUTO_INTEGRATION_ENABLED"

	// EnvEvidenceAutoIntegrationEnabled is the environment variable to enable automatic evidence buffering in inference
	EnvEvidenceAutoIntegrationEnabled = "NORNICDB_EVIDENCE_AUTO_INTEGRATION_ENABLED"

	// EnvEdgeProvenanceAutoIntegrationEnabled is the environment variable to enable automatic provenance logging in inference
	EnvEdgeProvenanceAutoIntegrationEnabled = "NORNICDB_EDGE_PROVENANCE_AUTO_INTEGRATION_ENABLED"

	// EnvPerNodeConfigAutoIntegrationEnabled is the environment variable to enable automatic per-node config in inference
	EnvPerNodeConfigAutoIntegrationEnabled = "NORNICDB_PER_NODE_CONFIG_AUTO_INTEGRATION_ENABLED"

	// EnvEdgeProvenanceEnabled is the environment variable to enable edge provenance logging
	EnvEdgeProvenanceEnabled = "NORNICDB_EDGE_PROVENANCE_ENABLED"

	// EnvCooldownEnabled is the environment variable to enable cooldown logic
	EnvCooldownEnabled = "NORNICDB_COOLDOWN_ENABLED"

	// EnvEvidenceBufferingEnabled is the environment variable to enable evidence buffering
	EnvEvidenceBufferingEnabled = "NORNICDB_EVIDENCE_BUFFERING_ENABLED"

	// EnvPerNodeConfigEnabled is the environment variable to enable per-node configuration
	EnvPerNodeConfigEnabled = "NORNICDB_PER_NODE_CONFIG_ENABLED"

	// EnvWALEnabled is the environment variable to enable write-ahead logging
	EnvWALEnabled = "NORNICDB_WAL_ENABLED"

	// FeatureKalmanDecay enables Kalman filtering for memory decay prediction
	FeatureKalmanDecay = "kalman_decay"

	// FeatureKalmanCoAccess enables Kalman filtering for co-access confidence
	FeatureKalmanCoAccess = "kalman_coaccess"

	// FeatureKalmanLatency enables Kalman filtering for latency prediction
	FeatureKalmanLatency = "kalman_latency"

	// FeatureKalmanSimilarity enables Kalman filtering for similarity smoothing
	FeatureKalmanSimilarity = "kalman_similarity"

	// FeatureKalmanTemporal enables Kalman filtering for temporal patterns
	FeatureKalmanTemporal = "kalman_temporal"

	// FeatureTopologyAutoIntegration enables AUTOMATIC topology integration with inference engine
	// NOTE: Topology algorithms (CALL gds.linkPrediction.*) are ALWAYS available
	// This only controls automatic use in inference.Engine.OnStore()
	FeatureTopologyAutoIntegration = "topology_auto_integration"

	// FeatureCooldownAutoIntegration enables AUTOMATIC cooldown in inference engine
	// NOTE: CooldownTable is ALWAYS available for direct use
	// This only controls automatic use in inference.Engine.ProcessSuggestion()
	FeatureCooldownAutoIntegration = "cooldown_auto_integration"

	// FeatureEvidenceAutoIntegration enables AUTOMATIC evidence buffering in inference engine
	// NOTE: EvidenceBuffer is ALWAYS available for direct use
	// This only controls automatic use in inference.Engine.ProcessSuggestion()
	FeatureEvidenceAutoIntegration = "evidence_auto_integration"

	// FeatureEdgeProvenanceAutoIntegration enables AUTOMATIC provenance logging in inference engine
	// NOTE: EdgeMetaStore is ALWAYS available for direct use
	// This only controls automatic logging in inference.Engine.ProcessSuggestion()
	FeatureEdgeProvenanceAutoIntegration = "edge_provenance_auto_integration"

	// FeaturePerNodeConfigAutoIntegration enables AUTOMATIC per-node config in inference engine
	// NOTE: NodeConfigStore is ALWAYS available for direct use
	// This only controls automatic checking in inference.Engine.ProcessSuggestion()
	FeaturePerNodeConfigAutoIntegration = "per_node_config_auto_integration"

	// FeatureEdgeProvenance enables edge provenance logging for audit trails
	// Tracks why edges were created, when, and what evidence supports them
	FeatureEdgeProvenance = "edge_provenance"

	// FeatureCooldown enables cooldown logic to prevent echo chambers
	// Prevents rapid re-materialization of the same edge pairs
	FeatureCooldown = "cooldown"

	// FeatureEvidenceBuffering enables evidence buffering before materialization
	// Only materializes edges after accumulating sufficient evidence
	FeatureEvidenceBuffering = "evidence_buffering"

	// FeaturePerNodeConfig enables per-node configuration (pins, denies, caps)
	// Allows fine-grained control over edge materialization per node
	FeaturePerNodeConfig = "per_node_config"

	// FeatureWAL enables write-ahead logging for durability
	// Provides crash recovery via WAL + snapshots
	FeatureWAL = "wal"
)

var (
	// Global feature flag state
	kalmanEnabled                        atomic.Bool
	topologyLinkPredictionEnabled        atomic.Bool
	cooldownAutoIntegrationEnabled       atomic.Bool
	evidenceAutoIntegrationEnabled       atomic.Bool
	edgeProvenanceAutoIntegrationEnabled atomic.Bool
	perNodeConfigAutoIntegrationEnabled  atomic.Bool
	edgeProvenanceEnabled                atomic.Bool
	cooldownEnabled                      atomic.Bool
	evidenceBufferingEnabled             atomic.Bool
	perNodeConfigEnabled                 atomic.Bool
	walEnabled                           atomic.Bool
	featureFlags                         = make(map[string]bool)
	featureFlagsMu                       sync.RWMutex
	initOnce                             sync.Once
)

func init() {
	// Check environment variables on startup
	initOnce.Do(func() {
		// Experimental features - DISABLED by default, enable with "true" or "1"
		if env := os.Getenv(EnvKalmanEnabled); env == "true" || env == "1" {
			kalmanEnabled.Store(true)
		}
		if env := os.Getenv(EnvTopologyLinkPredictionEnabled); env == "true" || env == "1" {
			topologyLinkPredictionEnabled.Store(true)
		}

		// Auto-Integration features - ENABLED by default for inference engine
		// These control automatic use in inference.Engine.ProcessSuggestion()
		// Users can disable with "false" or "0" if problems occur

		// Cooldown Auto-Integration: enabled by default
		cooldownAutoIntegrationEnabled.Store(true)
		if env := os.Getenv(EnvCooldownAutoIntegrationEnabled); env == "false" || env == "0" {
			cooldownAutoIntegrationEnabled.Store(false)
		}

		// Evidence Auto-Integration: enabled by default
		evidenceAutoIntegrationEnabled.Store(true)
		if env := os.Getenv(EnvEvidenceAutoIntegrationEnabled); env == "false" || env == "0" {
			evidenceAutoIntegrationEnabled.Store(false)
		}

		// Edge Provenance Auto-Integration: enabled by default
		edgeProvenanceAutoIntegrationEnabled.Store(true)
		if env := os.Getenv(EnvEdgeProvenanceAutoIntegrationEnabled); env == "false" || env == "0" {
			edgeProvenanceAutoIntegrationEnabled.Store(false)
		}

		// Per-Node Config Auto-Integration: enabled by default
		perNodeConfigAutoIntegrationEnabled.Store(true)
		if env := os.Getenv(EnvPerNodeConfigAutoIntegrationEnabled); env == "false" || env == "0" {
			perNodeConfigAutoIntegrationEnabled.Store(false)
		}

		// Tier 1 features - ENABLED by default for production safety
		// Users can disable with "false" or "0" if problems occur

		// Edge Provenance: enabled by default for audit trails
		edgeProvenanceEnabled.Store(true)
		if env := os.Getenv(EnvEdgeProvenanceEnabled); env == "false" || env == "0" {
			edgeProvenanceEnabled.Store(false)
		}

		// Cooldown: enabled by default to prevent echo chambers
		cooldownEnabled.Store(true)
		if env := os.Getenv(EnvCooldownEnabled); env == "false" || env == "0" {
			cooldownEnabled.Store(false)
		}

		// Evidence Buffering: enabled by default to reduce false positives
		evidenceBufferingEnabled.Store(true)
		if env := os.Getenv(EnvEvidenceBufferingEnabled); env == "false" || env == "0" {
			evidenceBufferingEnabled.Store(false)
		}

		// Per-Node Config: enabled by default for fine-grained control
		perNodeConfigEnabled.Store(true)
		if env := os.Getenv(EnvPerNodeConfigEnabled); env == "false" || env == "0" {
			perNodeConfigEnabled.Store(false)
		}

		// WAL: enabled by default for durability
		walEnabled.Store(true)
		if env := os.Getenv(EnvWALEnabled); env == "false" || env == "0" {
			walEnabled.Store(false)
		}
	})
}

// EnableKalmanFiltering globally enables Kalman filtering.
// This is the master switch - individual features can still be disabled.
func EnableKalmanFiltering() {
	kalmanEnabled.Store(true)
}

// DisableKalmanFiltering globally disables Kalman filtering.
func DisableKalmanFiltering() {
	kalmanEnabled.Store(false)
}

// IsKalmanEnabled returns true if Kalman filtering is globally enabled.
func IsKalmanEnabled() bool {
	return kalmanEnabled.Load()
}

// SetKalmanEnabled sets the global Kalman filtering state.
func SetKalmanEnabled(enabled bool) {
	kalmanEnabled.Store(enabled)
}

// WithKalmanEnabled temporarily enables Kalman filtering and returns a cleanup function.
// Useful for tests that need to enable/disable filtering.
//
// Example:
//
//	cleanup := filter.WithKalmanEnabled()
//	defer cleanup()
//	// ... test code with Kalman enabled ...
func WithKalmanEnabled() func() {
	prev := kalmanEnabled.Load()
	kalmanEnabled.Store(true)
	return func() {
		kalmanEnabled.Store(prev)
	}
}

// WithKalmanDisabled temporarily disables Kalman filtering and returns a cleanup function.
func WithKalmanDisabled() func() {
	prev := kalmanEnabled.Load()
	kalmanEnabled.Store(false)
	return func() {
		kalmanEnabled.Store(prev)
	}
}

// EnableFeature enables a specific Kalman feature.
func EnableFeature(feature string) {
	featureFlagsMu.Lock()
	defer featureFlagsMu.Unlock()
	featureFlags[feature] = true
}

// DisableFeature disables a specific Kalman feature.
func DisableFeature(feature string) {
	featureFlagsMu.Lock()
	defer featureFlagsMu.Unlock()
	featureFlags[feature] = false
}

// IsFeatureEnabled returns true if a specific feature is enabled.
// Both the global Kalman flag AND the specific feature must be enabled.
func IsFeatureEnabled(feature string) bool {
	if !kalmanEnabled.Load() {
		return false
	}
	featureFlagsMu.RLock()
	defer featureFlagsMu.RUnlock()
	enabled, exists := featureFlags[feature]
	// If feature not explicitly set, default to enabled when global is on
	if !exists {
		return true
	}
	return enabled
}

// EnableTopologyAutoIntegration enables automatic topology integration.
func EnableTopologyAutoIntegration() {
	topologyLinkPredictionEnabled.Store(true)
	EnableFeature(FeatureTopologyAutoIntegration)
}

// DisableTopologyAutoIntegration disables automatic topology integration.
func DisableTopologyAutoIntegration() {
	topologyLinkPredictionEnabled.Store(false)
	DisableFeature(FeatureTopologyAutoIntegration)
}

// IsTopologyAutoIntegrationEnabled returns true if automatic topology integration is enabled.
// Note: This does NOT affect Cypher procedures - they are always available.
func IsTopologyAutoIntegrationEnabled() bool {
	return topologyLinkPredictionEnabled.Load() || IsFeatureEnabled(FeatureTopologyAutoIntegration)
}

// WithTopologyEnabled temporarily enables topology and returns cleanup function.
// Useful for A/B testing in unit tests.
//
// Example:
//
//	cleanup := filter.WithTopologyEnabled()
//	defer cleanup()
//	// ... test code with topology enabled ...
func WithTopologyEnabled() func() {
	prev := topologyLinkPredictionEnabled.Load()
	topologyLinkPredictionEnabled.Store(true)
	EnableFeature(FeatureTopologyAutoIntegration)
	return func() {
		topologyLinkPredictionEnabled.Store(prev)
		if !prev {
			DisableFeature(FeatureTopologyAutoIntegration)
		}
	}
}

// WithTopologyDisabled temporarily disables topology and returns cleanup function.
func WithTopologyDisabled() func() {
	prev := topologyLinkPredictionEnabled.Load()
	topologyLinkPredictionEnabled.Store(false)
	DisableFeature(FeatureTopologyAutoIntegration)
	return func() {
		topologyLinkPredictionEnabled.Store(prev)
		if prev {
			EnableFeature(FeatureTopologyAutoIntegration)
		}
	}
}

// EnableCooldownAutoIntegration enables automatic cooldown in inference engine.
func EnableCooldownAutoIntegration() {
	cooldownAutoIntegrationEnabled.Store(true)
	EnableFeature(FeatureCooldownAutoIntegration)
}

// DisableCooldownAutoIntegration disables automatic cooldown in inference engine.
func DisableCooldownAutoIntegration() {
	cooldownAutoIntegrationEnabled.Store(false)
	DisableFeature(FeatureCooldownAutoIntegration)
}

// IsCooldownAutoIntegrationEnabled returns true if automatic cooldown is enabled in inference.
//
// When enabled, ProcessSuggestion() will automatically check cooldown before allowing
// edge materialization. When disabled, you must manually check cooldown if desired.
//
// Example (auto-integration enabled - default):
//
//	result := engine.ProcessSuggestion(suggestion, "session-123")
//	if result.ShouldMaterialize {  // Cooldown already checked!
//	    db.CreateEdge(...)
//	}
//
// Example (auto-integration disabled - manual control):
//
//	// Disable auto-integration
//	os.Setenv("NORNICDB_COOLDOWN_AUTO_INTEGRATION_ENABLED", "false")
//	
//	// Manually check cooldown
//	if engine.GetCooldownTable().CanMaterialize(src, dst, label) {
//	    db.CreateEdge(...)
//	    engine.GetCooldownTable().RecordMaterialization(src, dst, label)
//	}
func IsCooldownAutoIntegrationEnabled() bool {
	return cooldownAutoIntegrationEnabled.Load()
}

// WithCooldownAutoIntegrationEnabled temporarily enables cooldown auto-integration.
func WithCooldownAutoIntegrationEnabled() func() {
	prev := cooldownAutoIntegrationEnabled.Load()
	cooldownAutoIntegrationEnabled.Store(true)
	EnableFeature(FeatureCooldownAutoIntegration)
	return func() {
		cooldownAutoIntegrationEnabled.Store(prev)
		if !prev {
			DisableFeature(FeatureCooldownAutoIntegration)
		}
	}
}

// WithCooldownAutoIntegrationDisabled temporarily disables cooldown auto-integration.
func WithCooldownAutoIntegrationDisabled() func() {
	prev := cooldownAutoIntegrationEnabled.Load()
	cooldownAutoIntegrationEnabled.Store(false)
	DisableFeature(FeatureCooldownAutoIntegration)
	return func() {
		cooldownAutoIntegrationEnabled.Store(prev)
		if prev {
			EnableFeature(FeatureCooldownAutoIntegration)
		}
	}
}

// EnableEvidenceAutoIntegration enables automatic evidence buffering in inference engine.
func EnableEvidenceAutoIntegration() {
	evidenceAutoIntegrationEnabled.Store(true)
	EnableFeature(FeatureEvidenceAutoIntegration)
}

// DisableEvidenceAutoIntegration disables automatic evidence buffering in inference engine.
func DisableEvidenceAutoIntegration() {
	evidenceAutoIntegrationEnabled.Store(false)
	DisableFeature(FeatureEvidenceAutoIntegration)
}

// IsEvidenceAutoIntegrationEnabled returns true if automatic evidence buffering is enabled.
// Note: This does NOT affect EvidenceBuffer direct use - it's always available.
func IsEvidenceAutoIntegrationEnabled() bool {
	return evidenceAutoIntegrationEnabled.Load() || IsFeatureEnabled(FeatureEvidenceAutoIntegration)
}

// WithEvidenceAutoIntegrationEnabled temporarily enables evidence auto-integration.
func WithEvidenceAutoIntegrationEnabled() func() {
	prev := evidenceAutoIntegrationEnabled.Load()
	evidenceAutoIntegrationEnabled.Store(true)
	EnableFeature(FeatureEvidenceAutoIntegration)
	return func() {
		evidenceAutoIntegrationEnabled.Store(prev)
		if !prev {
			DisableFeature(FeatureEvidenceAutoIntegration)
		}
	}
}

// WithEvidenceAutoIntegrationDisabled temporarily disables evidence auto-integration.
func WithEvidenceAutoIntegrationDisabled() func() {
	prev := evidenceAutoIntegrationEnabled.Load()
	evidenceAutoIntegrationEnabled.Store(false)
	DisableFeature(FeatureEvidenceAutoIntegration)
	return func() {
		evidenceAutoIntegrationEnabled.Store(prev)
		if prev {
			EnableFeature(FeatureEvidenceAutoIntegration)
		}
	}
}

// EnableEdgeProvenanceAutoIntegration enables automatic provenance logging in inference engine.
func EnableEdgeProvenanceAutoIntegration() {
	edgeProvenanceAutoIntegrationEnabled.Store(true)
	EnableFeature(FeatureEdgeProvenanceAutoIntegration)
}

// DisableEdgeProvenanceAutoIntegration disables automatic provenance logging in inference engine.
func DisableEdgeProvenanceAutoIntegration() {
	edgeProvenanceAutoIntegrationEnabled.Store(false)
	DisableFeature(FeatureEdgeProvenanceAutoIntegration)
}

// IsEdgeProvenanceAutoIntegrationEnabled returns true if automatic provenance logging is enabled.
// Note: This does NOT affect EdgeMetaStore direct use - it's always available.
func IsEdgeProvenanceAutoIntegrationEnabled() bool {
	return edgeProvenanceAutoIntegrationEnabled.Load() || IsFeatureEnabled(FeatureEdgeProvenanceAutoIntegration)
}

// WithEdgeProvenanceAutoIntegrationEnabled temporarily enables edge provenance auto-integration.
func WithEdgeProvenanceAutoIntegrationEnabled() func() {
	prev := edgeProvenanceAutoIntegrationEnabled.Load()
	edgeProvenanceAutoIntegrationEnabled.Store(true)
	EnableFeature(FeatureEdgeProvenanceAutoIntegration)
	return func() {
		edgeProvenanceAutoIntegrationEnabled.Store(prev)
		if !prev {
			DisableFeature(FeatureEdgeProvenanceAutoIntegration)
		}
	}
}

// WithEdgeProvenanceAutoIntegrationDisabled temporarily disables edge provenance auto-integration.
func WithEdgeProvenanceAutoIntegrationDisabled() func() {
	prev := edgeProvenanceAutoIntegrationEnabled.Load()
	edgeProvenanceAutoIntegrationEnabled.Store(false)
	DisableFeature(FeatureEdgeProvenanceAutoIntegration)
	return func() {
		edgeProvenanceAutoIntegrationEnabled.Store(prev)
		if prev {
			EnableFeature(FeatureEdgeProvenanceAutoIntegration)
		}
	}
}

// IsPerNodeConfigAutoIntegrationEnabled returns true if per-node config is auto-integrated.
//
// When enabled, ProcessSuggestion() automatically checks deny lists, edge caps, and
// trust levels before allowing edge materialization.
//
// Example (auto-integration enabled - default):
//
//	// Set up node config
//	userConfig := storage.NewNodeConfig("user-123")
//	userConfig.MaxOutEdges = 50
//	userConfig.DenyList = []string{"spam-node"}
//	engine.GetNodeConfigStore().Set(userConfig)
//	
//	// ProcessSuggestion automatically enforces limits
//	result := engine.ProcessSuggestion(suggestion, "session-123")
//	if result.NodeConfigBlocked {
//	    log.Printf("Blocked by node config: %s", result.Reason)
//	}
//
// Example (auto-integration disabled - manual control):
//
//	os.Setenv("NORNICDB_PER_NODE_CONFIG_AUTO_INTEGRATION_ENABLED", "false")
//	
//	// Manually check node config
//	store := engine.GetNodeConfigStore()
//	if allowed, _ := store.IsEdgeAllowedWithReason(src, dst, label); allowed {
//	    db.CreateEdge(...)
//	}
//
// Note: This does NOT affect NodeConfigStore direct use - it's always available.
func IsPerNodeConfigAutoIntegrationEnabled() bool {
	return perNodeConfigAutoIntegrationEnabled.Load() || IsFeatureEnabled(FeaturePerNodeConfigAutoIntegration)
}

// WithPerNodeConfigAutoIntegrationEnabled temporarily enables per-node config auto-integration.
func WithPerNodeConfigAutoIntegrationEnabled() func() {
	prev := perNodeConfigAutoIntegrationEnabled.Load()
	perNodeConfigAutoIntegrationEnabled.Store(true)
	EnableFeature(FeaturePerNodeConfigAutoIntegration)
	return func() {
		perNodeConfigAutoIntegrationEnabled.Store(prev)
		if !prev {
			DisableFeature(FeaturePerNodeConfigAutoIntegration)
		}
	}
}

// WithPerNodeConfigAutoIntegrationDisabled temporarily disables per-node config auto-integration.
func WithPerNodeConfigAutoIntegrationDisabled() func() {
	prev := perNodeConfigAutoIntegrationEnabled.Load()
	perNodeConfigAutoIntegrationEnabled.Store(false)
	DisableFeature(FeaturePerNodeConfigAutoIntegration)
	return func() {
		perNodeConfigAutoIntegrationEnabled.Store(prev)
		if prev {
			EnableFeature(FeaturePerNodeConfigAutoIntegration)
		}
	}
}

// EnableAllFeatures enables all Kalman features.
func EnableAllFeatures() {
	EnableKalmanFiltering()
	EnableFeature(FeatureKalmanDecay)
	EnableFeature(FeatureKalmanCoAccess)
	EnableFeature(FeatureKalmanLatency)
	EnableFeature(FeatureKalmanSimilarity)
	EnableFeature(FeatureKalmanTemporal)
}

// DisableAllFeatures disables all Kalman features.
func DisableAllFeatures() {
	DisableKalmanFiltering()
	DisableFeature(FeatureKalmanDecay)
	DisableFeature(FeatureKalmanCoAccess)
	DisableFeature(FeatureKalmanLatency)
	DisableFeature(FeatureKalmanSimilarity)
	DisableFeature(FeatureKalmanTemporal)
}

// ResetFeatureFlags resets all feature flags to defaults.
func ResetFeatureFlags() {
	kalmanEnabled.Store(false)
	topologyLinkPredictionEnabled.Store(false)
	cooldownAutoIntegrationEnabled.Store(false)
	evidenceAutoIntegrationEnabled.Store(false)
	edgeProvenanceAutoIntegrationEnabled.Store(false)
	edgeProvenanceEnabled.Store(false)
	cooldownEnabled.Store(false)
	evidenceBufferingEnabled.Store(false)
	perNodeConfigEnabled.Store(false)
	walEnabled.Store(false)
	featureFlagsMu.Lock()
	defer featureFlagsMu.Unlock()
	featureFlags = make(map[string]bool)
}

// GetEnabledFeatures returns a list of enabled features.
func GetEnabledFeatures() []string {
	if !kalmanEnabled.Load() {
		return nil
	}
	featureFlagsMu.RLock()
	defer featureFlagsMu.RUnlock()

	var enabled []string
	allFeatures := []string{
		FeatureKalmanDecay,
		FeatureKalmanCoAccess,
		FeatureKalmanLatency,
		FeatureKalmanSimilarity,
		FeatureKalmanTemporal,
	}

	for _, f := range allFeatures {
		flag, exists := featureFlags[f]
		if !exists || flag {
			enabled = append(enabled, f)
		}
	}
	return enabled
}

// FeatureStatus returns the current status of all features.
type FeatureStatus struct {
	GlobalEnabled            bool
	KalmanEnabled            bool
	TopologyEnabled          bool
	EdgeProvenanceEnabled    bool
	CooldownEnabled          bool
	EvidenceBufferingEnabled bool
	PerNodeConfigEnabled     bool
	WALEnabled               bool
	Features                 map[string]bool
}

// GetFeatureStatus returns the complete feature status.
func GetFeatureStatus() FeatureStatus {
	featureFlagsMu.RLock()
	defer featureFlagsMu.RUnlock()

	status := FeatureStatus{
		GlobalEnabled:            kalmanEnabled.Load(),
		KalmanEnabled:            kalmanEnabled.Load(),
		TopologyEnabled:          topologyLinkPredictionEnabled.Load(),
		EdgeProvenanceEnabled:    edgeProvenanceEnabled.Load(),
		CooldownEnabled:          cooldownEnabled.Load(),
		EvidenceBufferingEnabled: evidenceBufferingEnabled.Load(),
		PerNodeConfigEnabled:     perNodeConfigEnabled.Load(),
		WALEnabled:               walEnabled.Load(),
		Features:                 make(map[string]bool),
	}

	for k, v := range featureFlags {
		status.Features[k] = v
	}
	return status
}

// FilteredValue represents a value that may or may not have been filtered.
// Useful for A/B testing and comparison.
// Note: Kalman-specific methods have been moved to pkg/filter package.
type FilteredValue struct {
	Raw         float64 // Original unfiltered value
	Filtered    float64 // Filtered value (same as Raw if disabled)
	WasFiltered bool    // True if filtering was applied
	Feature     string  // Which feature flag controlled this
}

// EnableEdgeProvenance enables edge provenance logging.
func EnableEdgeProvenance() {
	edgeProvenanceEnabled.Store(true)
	EnableFeature(FeatureEdgeProvenance)
}

// DisableEdgeProvenance disables edge provenance logging.
func DisableEdgeProvenance() {
	edgeProvenanceEnabled.Store(false)
	DisableFeature(FeatureEdgeProvenance)
}

// IsEdgeProvenanceEnabled returns true if edge provenance is enabled.
func IsEdgeProvenanceEnabled() bool {
	return edgeProvenanceEnabled.Load() || IsFeatureEnabled(FeatureEdgeProvenance)
}

// WithEdgeProvenanceEnabled temporarily enables edge provenance and returns cleanup function.
func WithEdgeProvenanceEnabled() func() {
	prev := edgeProvenanceEnabled.Load()
	edgeProvenanceEnabled.Store(true)
	EnableFeature(FeatureEdgeProvenance)
	return func() {
		edgeProvenanceEnabled.Store(prev)
		if !prev {
			DisableFeature(FeatureEdgeProvenance)
		}
	}
}

// WithEdgeProvenanceDisabled temporarily disables edge provenance and returns cleanup function.
func WithEdgeProvenanceDisabled() func() {
	prev := edgeProvenanceEnabled.Load()
	edgeProvenanceEnabled.Store(false)
	DisableFeature(FeatureEdgeProvenance)
	return func() {
		edgeProvenanceEnabled.Store(prev)
		if prev {
			EnableFeature(FeatureEdgeProvenance)
		}
	}
}

// EnableCooldown enables cooldown logic.
func EnableCooldown() {
	cooldownEnabled.Store(true)
	EnableFeature(FeatureCooldown)
}

// DisableCooldown disables cooldown logic.
func DisableCooldown() {
	cooldownEnabled.Store(false)
	DisableFeature(FeatureCooldown)
}

// IsCooldownEnabled returns true if cooldown is enabled.
func IsCooldownEnabled() bool {
	return cooldownEnabled.Load() || IsFeatureEnabled(FeatureCooldown)
}

// WithCooldownEnabled temporarily enables cooldown and returns cleanup function.
func WithCooldownEnabled() func() {
	prev := cooldownEnabled.Load()
	cooldownEnabled.Store(true)
	EnableFeature(FeatureCooldown)
	return func() {
		cooldownEnabled.Store(prev)
		if !prev {
			DisableFeature(FeatureCooldown)
		}
	}
}

// WithCooldownDisabled temporarily disables cooldown and returns cleanup function.
func WithCooldownDisabled() func() {
	prev := cooldownEnabled.Load()
	cooldownEnabled.Store(false)
	DisableFeature(FeatureCooldown)
	return func() {
		cooldownEnabled.Store(prev)
		if prev {
			EnableFeature(FeatureCooldown)
		}
	}
}

// EnableEvidenceBuffering enables evidence buffering.
func EnableEvidenceBuffering() {
	evidenceBufferingEnabled.Store(true)
	EnableFeature(FeatureEvidenceBuffering)
}

// DisableEvidenceBuffering disables evidence buffering.
func DisableEvidenceBuffering() {
	evidenceBufferingEnabled.Store(false)
	DisableFeature(FeatureEvidenceBuffering)
}

// IsEvidenceBufferingEnabled returns true if evidence buffering is enabled.
func IsEvidenceBufferingEnabled() bool {
	return evidenceBufferingEnabled.Load() || IsFeatureEnabled(FeatureEvidenceBuffering)
}

// WithEvidenceBufferingEnabled temporarily enables evidence buffering and returns cleanup function.
func WithEvidenceBufferingEnabled() func() {
	prev := evidenceBufferingEnabled.Load()
	evidenceBufferingEnabled.Store(true)
	EnableFeature(FeatureEvidenceBuffering)
	return func() {
		evidenceBufferingEnabled.Store(prev)
		if !prev {
			DisableFeature(FeatureEvidenceBuffering)
		}
	}
}

// WithEvidenceBufferingDisabled temporarily disables evidence buffering and returns cleanup function.
func WithEvidenceBufferingDisabled() func() {
	prev := evidenceBufferingEnabled.Load()
	evidenceBufferingEnabled.Store(false)
	DisableFeature(FeatureEvidenceBuffering)
	return func() {
		evidenceBufferingEnabled.Store(prev)
		if prev {
			EnableFeature(FeatureEvidenceBuffering)
		}
	}
}

// EnablePerNodeConfig enables per-node configuration.
func EnablePerNodeConfig() {
	perNodeConfigEnabled.Store(true)
	EnableFeature(FeaturePerNodeConfig)
}

// DisablePerNodeConfig disables per-node configuration.
func DisablePerNodeConfig() {
	perNodeConfigEnabled.Store(false)
	DisableFeature(FeaturePerNodeConfig)
}

// IsPerNodeConfigEnabled returns true if per-node config is enabled.
func IsPerNodeConfigEnabled() bool {
	return perNodeConfigEnabled.Load() || IsFeatureEnabled(FeaturePerNodeConfig)
}

// WithPerNodeConfigEnabled temporarily enables per-node config and returns cleanup function.
func WithPerNodeConfigEnabled() func() {
	prev := perNodeConfigEnabled.Load()
	perNodeConfigEnabled.Store(true)
	EnableFeature(FeaturePerNodeConfig)
	return func() {
		perNodeConfigEnabled.Store(prev)
		if !prev {
			DisableFeature(FeaturePerNodeConfig)
		}
	}
}

// WithPerNodeConfigDisabled temporarily disables per-node config and returns cleanup function.
func WithPerNodeConfigDisabled() func() {
	prev := perNodeConfigEnabled.Load()
	perNodeConfigEnabled.Store(false)
	DisableFeature(FeaturePerNodeConfig)
	return func() {
		perNodeConfigEnabled.Store(prev)
		if prev {
			EnableFeature(FeaturePerNodeConfig)
		}
	}
}

// EnableWAL enables write-ahead logging.
func EnableWAL() {
	walEnabled.Store(true)
	EnableFeature(FeatureWAL)
}

// DisableWAL disables write-ahead logging.
func DisableWAL() {
	walEnabled.Store(false)
	DisableFeature(FeatureWAL)
}

// IsWALEnabled returns true if WAL is enabled.
func IsWALEnabled() bool {
	return walEnabled.Load() || IsFeatureEnabled(FeatureWAL)
}

// WithWALEnabled temporarily enables WAL and returns cleanup function.
func WithWALEnabled() func() {
	prev := walEnabled.Load()
	walEnabled.Store(true)
	EnableFeature(FeatureWAL)
	return func() {
		walEnabled.Store(prev)
		if !prev {
			DisableFeature(FeatureWAL)
		}
	}
}

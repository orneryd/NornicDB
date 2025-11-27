# NornicDB Feature Flags

Complete reference for all feature flags in NornicDB. Feature flags allow you to enable/disable experimental features and control production safety mechanisms.

---

## Quick Reference

| Category | Feature | Default | Environment Variable |
|----------|---------|---------|---------------------|
| **Tier 1 - Production Safety** | | | |
| | Edge Provenance | ✅ Enabled | `NORNICDB_EDGE_PROVENANCE_ENABLED` |
| | Cooldown | ✅ Enabled | `NORNICDB_COOLDOWN_ENABLED` |
| | Evidence Buffering | ✅ Enabled | `NORNICDB_EVIDENCE_BUFFERING_ENABLED` |
| | Per-Node Config | ✅ Enabled | `NORNICDB_PER_NODE_CONFIG_ENABLED` |
| | WAL (Write-Ahead Log) | ✅ Enabled | `NORNICDB_WAL_ENABLED` |
| **Auto-Integration** | | | |
| | Cooldown Auto-Integration | ✅ Enabled | `NORNICDB_COOLDOWN_AUTO_INTEGRATION_ENABLED` |
| | Evidence Auto-Integration | ✅ Enabled | `NORNICDB_EVIDENCE_AUTO_INTEGRATION_ENABLED` |
| | Edge Provenance Auto-Integration | ✅ Enabled | `NORNICDB_EDGE_PROVENANCE_AUTO_INTEGRATION_ENABLED` |
| | Per-Node Config Auto-Integration | ✅ Enabled | `NORNICDB_PER_NODE_CONFIG_AUTO_INTEGRATION_ENABLED` |
| **Experimental** | | | |
| | Kalman Filtering | ❌ Disabled | `NORNICDB_KALMAN_ENABLED` |
| | Topology Link Prediction | ❌ Disabled | `NORNICDB_TOPOLOGY_LINK_PREDICTION_ENABLED` |

---

## Tier 1 Features (Production Safety)

These features are **enabled by default** to ensure production stability and safety. Disable them only if experiencing specific issues.

### Edge Provenance Logging

**Purpose**: Tracks why edges were created, when, and what evidence supports them. Essential for audit trails and debugging auto-generated relationships.

**Environment Variable**: `NORNICDB_EDGE_PROVENANCE_ENABLED`

**Default**: ✅ Enabled

```bash
# Disable if causing performance issues (not recommended)
export NORNICDB_EDGE_PROVENANCE_ENABLED=false
```

**Go API**:
```go
import "github.com/orneryd/nornicdb/pkg/config"

// Check status
if config.IsEdgeProvenanceEnabled() {
    // Provenance logging is active
}

// Toggle at runtime
config.EnableEdgeProvenance()
config.DisableEdgeProvenance()

// Scoped disable for testing
cleanup := config.WithEdgeProvenanceDisabled()
defer cleanup()
// ... test code without provenance ...
```

**What it does**:
- Logs every edge creation with source, target, label, score, signal type
- Records timestamp, session ID, evidence count, decay state
- Supports filtering by signal type (coaccess, similarity, topology, llm-infer)
- Enables audit queries like "why does this edge exist?"

---

### Cooldown Logic

**Purpose**: Prevents rapid re-materialization of the same edge pairs, avoiding echo chambers from burst activity.

**Environment Variable**: `NORNICDB_COOLDOWN_ENABLED`

**Default**: ✅ Enabled

```bash
# Disable if edges aren't being created when expected
export NORNICDB_COOLDOWN_ENABLED=false
```

**Go API**:
```go
import "github.com/orneryd/nornicdb/pkg/config"

// Check status
if config.IsCooldownEnabled() {
    // Cooldown is active
}

// Toggle at runtime
config.EnableCooldown()
config.DisableCooldown()

// Scoped disable for testing
cleanup := config.WithCooldownDisabled()
defer cleanup()
```

**What it does**:
- Tracks when edges were last materialized
- Enforces minimum time between same-pair edge creations
- Configurable per-label cooldown durations:
  - `relates_to`: 5 minutes
  - `similar_to`: 10 minutes
  - `coaccess`: 1 minute
  - `topology`: 15 minutes

---

### Evidence Buffering

**Purpose**: Requires multiple corroborating signals before materializing edges, reducing false positives.

**Environment Variable**: `NORNICDB_EVIDENCE_BUFFERING_ENABLED`

**Default**: ✅ Enabled

```bash
# Disable if edges aren't appearing after expected activity
export NORNICDB_EVIDENCE_BUFFERING_ENABLED=false
```

**Go API**:
```go
import "github.com/orneryd/nornicdb/pkg/config"

// Check status
if config.IsEvidenceBufferingEnabled() {
    // Evidence buffering is active
}

// Toggle at runtime
config.EnableEvidenceBuffering()
config.DisableEvidenceBuffering()

// Scoped disable for testing
cleanup := config.WithEvidenceBufferingDisabled()
defer cleanup()
```

**What it does**:
- Accumulates evidence signals before edge materialization
- Configurable thresholds per label:
  - `MinCount`: Minimum evidence occurrences (default: 3)
  - `MinScore`: Minimum cumulative score (default: 0.5)
  - `MinSessions`: Minimum unique sessions (default: 2)
  - `MaxAge`: Evidence expiration time

---

### Per-Node Configuration

**Purpose**: Fine-grained control over edge materialization per node - pins, denies, caps, and trust levels.

**Environment Variable**: `NORNICDB_PER_NODE_CONFIG_ENABLED`

**Default**: ✅ Enabled

```bash
# Disable if per-node rules are causing unexpected behavior
export NORNICDB_PER_NODE_CONFIG_ENABLED=false
```

**Go API**:
```go
import "github.com/orneryd/nornicdb/pkg/config"

// Check status
if config.IsPerNodeConfigEnabled() {
    // Per-node config is active
}

// Toggle at runtime
config.EnablePerNodeConfig()
config.DisablePerNodeConfig()

// Scoped disable for testing
cleanup := config.WithPerNodeConfigDisabled()
defer cleanup()
```

**What it does**:
- **Pin List**: Edges to pinned targets never decay
- **Deny List**: Never create edges to denied targets
- **Edge Caps**: Maximum edges per node (in/out/total)
- **Trust Levels**: Affects confidence thresholds
  - `TrustLevelPinned`: Always allow
  - `TrustLevelHigh`: Lower threshold (-10%)
  - `TrustLevelDefault`: Standard threshold
  - `TrustLevelLow`: Higher threshold (+20%)

---

### Write-Ahead Logging (WAL)

**Purpose**: Durability and crash recovery. All mutations are logged before execution.

**Environment Variable**: `NORNICDB_WAL_ENABLED`

**Default**: ✅ Enabled

```bash
# Disable for maximum performance (data loss risk on crash)
export NORNICDB_WAL_ENABLED=false
```

**Go API**:
```go
import "github.com/orneryd/nornicdb/pkg/config"

// Check status
if config.IsWALEnabled() {
    // WAL is active
}

// Toggle at runtime
config.EnableWAL()
config.DisableWAL()

// Scoped disable for testing
cleanup := config.WithWALDisabled()
defer cleanup()
```

**What it does**:
- Logs all mutations before execution (create, update, delete)
- Supports three sync modes:
  - `immediate`: fsync after each write (safest, slowest)
  - `batch`: periodic fsync (balanced)
  - `none`: no fsync (fastest, data loss risk)
- Point-in-time snapshots for efficient recovery
- Recovery: Load snapshot + replay WAL entries

---

## Auto-Integration Features

These flags control **automatic integration** into the inference engine's `ProcessSuggestion()` flow. The underlying features remain available for direct use even when auto-integration is disabled.

### Cooldown Auto-Integration

**Purpose**: Automatically check cooldown before materializing edges in inference engine.

**Environment Variable**: `NORNICDB_COOLDOWN_AUTO_INTEGRATION_ENABLED`

**Default**: ✅ Enabled

```bash
# Disable automatic integration (manual use still works)
export NORNICDB_COOLDOWN_AUTO_INTEGRATION_ENABLED=false
```

**Go API**:
```go
import "github.com/orneryd/nornicdb/pkg/config"

if config.IsCooldownAutoIntegrationEnabled() {
    // Cooldown automatically checked in ProcessSuggestion()
}

// Direct access to CooldownTable always works
engine.GetCooldownTable().CanMaterialize(src, dst, label)
```

---

### Evidence Auto-Integration

**Purpose**: Automatically use evidence buffering in inference engine.

**Environment Variable**: `NORNICDB_EVIDENCE_AUTO_INTEGRATION_ENABLED`

**Default**: ✅ Enabled

```bash
# Disable automatic integration
export NORNICDB_EVIDENCE_AUTO_INTEGRATION_ENABLED=false
```

**Go API**:
```go
import "github.com/orneryd/nornicdb/pkg/config"

if config.IsEvidenceAutoIntegrationEnabled() {
    // Evidence automatically tracked in ProcessSuggestion()
}

// Direct access to EvidenceBuffer always works
engine.GetEvidenceBuffer().AddEvidence(src, dst, label, score, signalType, sessionID)
```

---

### Edge Provenance Auto-Integration

**Purpose**: Automatically log edge provenance in inference engine.

**Environment Variable**: `NORNICDB_EDGE_PROVENANCE_AUTO_INTEGRATION_ENABLED`

**Default**: ✅ Enabled

```bash
# Disable automatic logging
export NORNICDB_EDGE_PROVENANCE_AUTO_INTEGRATION_ENABLED=false
```

**Go API**:
```go
import "github.com/orneryd/nornicdb/pkg/config"

if config.IsEdgeProvenanceAutoIntegrationEnabled() {
    // Provenance automatically logged in ProcessSuggestion()
}

// Direct access to EdgeMetaStore always works
engine.GetEdgeMetaStore().Append(ctx, meta)
```

---

### Per-Node Config Auto-Integration

**Purpose**: Automatically check per-node rules in inference engine.

**Environment Variable**: `NORNICDB_PER_NODE_CONFIG_AUTO_INTEGRATION_ENABLED`

**Default**: ✅ Enabled

```bash
# Disable automatic checking
export NORNICDB_PER_NODE_CONFIG_AUTO_INTEGRATION_ENABLED=false
```

**Go API**:
```go
import "github.com/orneryd/nornicdb/pkg/config"

if config.IsPerNodeConfigAutoIntegrationEnabled() {
    // Per-node rules automatically checked in ProcessSuggestion()
}

// Direct access to NodeConfigStore always works
engine.GetNodeConfigStore().IsEdgeAllowed(src, dst, label)
```

---

## Experimental Features

These features are **disabled by default**. Enable them to experiment with advanced functionality.

### Kalman Filtering

**Purpose**: Signal smoothing and noise reduction for decay prediction, co-access confidence, latency estimation, and similarity scoring.

**Environment Variable**: `NORNICDB_KALMAN_ENABLED`

**Default**: ❌ Disabled

```bash
# Enable Kalman filtering
export NORNICDB_KALMAN_ENABLED=true
```

**Go API**:
```go
import "github.com/orneryd/nornicdb/pkg/config"

// Check status
if config.IsKalmanEnabled() {
    // Kalman filtering is active
}

// Enable all Kalman features
config.EnableKalmanFiltering()

// Enable specific sub-features
config.EnableFeature(config.FeatureKalmanDecay)
config.EnableFeature(config.FeatureKalmanCoAccess)
config.EnableFeature(config.FeatureKalmanLatency)
config.EnableFeature(config.FeatureKalmanSimilarity)
config.EnableFeature(config.FeatureKalmanTemporal)

// Scoped enable for testing
cleanup := config.WithKalmanEnabled()
defer cleanup()
```

**Sub-features**:
| Feature | Description |
|---------|-------------|
| `kalman_decay` | Smooth decay score predictions |
| `kalman_coaccess` | Smooth co-access confidence scores |
| `kalman_latency` | Predict and smooth latency measurements |
| `kalman_similarity` | Reduce noise in similarity calculations |
| `kalman_temporal` | Detect and predict temporal patterns |

---

### Topology Link Prediction

**Purpose**: Graph-structure-based link prediction using topological algorithms (Common Neighbors, Jaccard, Adamic-Adar, etc.).

**Environment Variable**: `NORNICDB_TOPOLOGY_LINK_PREDICTION_ENABLED`

**Default**: ❌ Disabled

```bash
# Enable topology link prediction
export NORNICDB_TOPOLOGY_LINK_PREDICTION_ENABLED=true
```

**Go API**:
```go
import "github.com/orneryd/nornicdb/pkg/config"

// Check status
if config.IsTopologyLinkPredictionEnabled() {
    // Topology integration active in inference engine
}

// Toggle at runtime
config.EnableTopologyLinkPrediction()
config.DisableTopologyLinkPrediction()

// Scoped enable for testing
cleanup := config.WithTopologyLinkPredictionEnabled()
defer cleanup()
```

**Note**: The topology algorithms themselves (`CALL gds.linkPrediction.*`) are always available. This flag only controls automatic integration into the inference engine.

---

## Complete Example: Docker Compose Configuration

```yaml
# docker-compose.yml
services:
  nornicdb:
    image: nornicdb:latest
    environment:
      # Tier 1 - All enabled by default, uncomment to disable
      # NORNICDB_EDGE_PROVENANCE_ENABLED: "false"
      # NORNICDB_COOLDOWN_ENABLED: "false"
      # NORNICDB_EVIDENCE_BUFFERING_ENABLED: "false"
      # NORNICDB_PER_NODE_CONFIG_ENABLED: "false"
      # NORNICDB_WAL_ENABLED: "false"
      
      # Auto-Integration - All enabled by default, uncomment to disable
      # NORNICDB_COOLDOWN_AUTO_INTEGRATION_ENABLED: "false"
      # NORNICDB_EVIDENCE_AUTO_INTEGRATION_ENABLED: "false"
      # NORNICDB_EDGE_PROVENANCE_AUTO_INTEGRATION_ENABLED: "false"
      # NORNICDB_PER_NODE_CONFIG_AUTO_INTEGRATION_ENABLED: "false"
      
      # Experimental - Disabled by default, uncomment to enable
      # NORNICDB_KALMAN_ENABLED: "true"
      # NORNICDB_TOPOLOGY_LINK_PREDICTION_ENABLED: "true"
```

---

## Complete Example: Go Configuration

```go
package main

import (
    "github.com/orneryd/nornicdb/pkg/config"
    "github.com/orneryd/nornicdb/pkg/inference"
    "github.com/orneryd/nornicdb/pkg/storage"
)

func main() {
    // Check current state
    status := config.GetFeatureStatus()
    
    // Tier 1 features (enabled by default)
    println("Edge Provenance:", status.EdgeProvenanceEnabled)    // true
    println("Cooldown:", status.CooldownEnabled)                  // true
    println("Evidence Buffering:", status.EvidenceBufferingEnabled) // true
    println("Per-Node Config:", status.PerNodeConfigEnabled)      // true
    println("WAL:", status.WALEnabled)                            // true
    
    // Experimental features (disabled by default)
    println("Kalman:", status.KalmanEnabled)                      // false
    println("Topology:", status.TopologyLinkPredictionEnabled)    // false
    
    // Runtime configuration
    config.EnableKalmanFiltering()
    config.EnableTopologyLinkPrediction()
    
    // Disable specific feature if issues occur
    config.DisableCooldown()
    
    // Check specific feature
    if config.IsWALEnabled() {
        // Set up WAL-backed storage
        wal, _ := storage.NewWAL("/data/wal", nil)
        engine := storage.NewWALEngine(storage.NewMemoryEngine(), wal)
        _ = engine
    }
    
    // Use inference engine with auto-integration
    inferEngine := inference.New(nil)
    
    // All Tier 1 features automatically integrate:
    // - Cooldown checked before materialization
    // - Evidence accumulated before materialization
    // - Provenance logged for audit trails
    // - Per-node rules enforced
    
    result := inferEngine.ProcessSuggestion(inference.EdgeSuggestion{
        SourceID:   "node-A",
        TargetID:   "node-B",
        Type:       "RELATES_TO",
        Confidence: 0.85,
    }, "session-123")
    
    println("Should Materialize:", result.ShouldMaterialize)
    println("Cooldown Blocked:", result.CooldownBlocked)
    println("Evidence Pending:", result.EvidencePending)
    println("Node Config Blocked:", result.NodeConfigBlocked)
}
```

---

## Feature Status API

```go
// Get complete feature status
status := config.GetFeatureStatus()

// FeatureStatus struct
type FeatureStatus struct {
    GlobalEnabled                bool // Kalman master switch
    TopologyLinkPredictionEnabled bool
    EdgeProvenanceEnabled        bool
    CooldownEnabled              bool
    EvidenceBufferingEnabled     bool
    PerNodeConfigEnabled         bool
    WALEnabled                   bool
    KalmanEnabled                bool
    Features                     map[string]bool // All sub-features
}

// List enabled features
features := config.GetEnabledFeatures()
for _, f := range features {
    println("Enabled:", f)
}
```

---

## Testing Utilities

All features have scoped enable/disable helpers for testing:

```go
func TestWithKalman(t *testing.T) {
    // Temporarily enable
    cleanup := config.WithKalmanEnabled()
    defer cleanup()
    
    // Test code with Kalman enabled
}

func TestWithoutWAL(t *testing.T) {
    // Temporarily disable
    cleanup := config.WithWALDisabled()
    defer cleanup()
    
    // Test code without WAL
}

func TestResetAll(t *testing.T) {
    // Reset all flags to defaults
    config.ResetFeatureFlags()
}
```

---

## Summary

| Feature Category | Purpose | Default | When to Disable |
|-----------------|---------|---------|-----------------|
| **Tier 1** | Production safety | Enabled | Only if causing specific issues |
| **Auto-Integration** | Seamless inference integration | Enabled | For manual control of features |
| **Experimental** | Advanced capabilities | Disabled | Enable to experiment |

---

_Last updated: November 2025_

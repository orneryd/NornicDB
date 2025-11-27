// Package storage provides per-node configuration for NornicDB inference.
//
// Per-node config allows fine-grained control over edge materialization:
// - Pin list: edges to these targets never decay
// - Deny list: never create edges to these targets
// - Edge caps: maximum edges per node (in/out/total)
// - Per-label caps: limits on specific edge types
// - Trust level: affects confidence thresholds
//
// Feature flags:
//   - NORNICDB_PER_NODE_CONFIG_ENABLED=true (enabled by default)
//   - NORNICDB_PER_NODE_CONFIG_AUTO_INTEGRATION_ENABLED=true (enabled by default)
//
// Real-World Example 1: User preferences (social network)
//
//	// User wants max 50 friends, never connect to blocked users
//	store := storage.NewNodeConfigStore()
//	userConfig := storage.NewNodeConfig("user-alice")
//	userConfig.MaxOutEdges = 50  // Max 50 outgoing friendships
//	userConfig.DenyList = []string{"user-spammer", "user-troll"}  // Blocked users
//	userConfig.PinList = []string{"user-bestfriend"}  // Never decay this friendship
//	store.Set(userConfig)
//	
//	// Later, inference engine checks before creating edge
//	if allowed, _ := store.IsEdgeAllowedWithReason("user-alice", "user-bob", "friend"); allowed {
//	    db.CreateEdge("user-alice", "user-bob", "friend")
//	    store.RecordEdgeCreation("user-alice", "user-bob")  // Update count (now at 23/50)
//	}
//
// Real-World Example 2: Document categorization limits (knowledge base)
//
//	// Document should have max 5 categories, but unlimited references
//	docConfig := storage.NewNodeConfig("doc-123")
//	docConfig.LabelConfigs = map[string]storage.LabelConfig{
//	    "category": {MaxEdges: 5},        // Max 5 category tags
//	    "references": {MaxEdges: 0},      // Unlimited references (0 = no limit)
//	    "deprecated": {Disabled: true},   // Never create deprecated edges
//	}
//	store.Set(docConfig)
//	
//	// Try to add 6th category - denied!
//	allowed, reason := store.IsEdgeAllowedWithReason("doc-123", "category-ai", "category")
//	// → (false, "label 'category' at max capacity (5/5)")
//
// Real-World Example 3: Low-trust nodes (spam prevention)
//
//	// New users start with low trust - require higher confidence
//	newUserConfig := storage.NewNodeConfig("user-newbie")
//	newUserConfig.TrustLevel = storage.TrustLevelLow  // Requires +20% confidence
//	newUserConfig.MaxOutEdges = 10  // Limited connections until trust increases
//	store.Set(newUserConfig)
//	
//	// After user proves trustworthy, upgrade trust
//	if userIsActive && userNotSpamming {
//	    newUserConfig.TrustLevel = storage.TrustLevelDefault
//	    newUserConfig.MaxOutEdges = 100
//	    store.Set(newUserConfig)  // Update config
//	}
//
// ELI12 (Explain Like I'm 12):
//
// Per-node config is like house rules for each person:
//
// **Pin List**: "These are my best friends - never unfriend them!"
//   - In NornicDB: Edges in pin list never decay, always kept
//
// **Deny List**: "I never want to talk to these people"
//   - In NornicDB: Never create edges to denied nodes (like blocking someone)
//
// **Edge Caps**: "I can only have 50 friends max"
//   - In NornicDB: Limits prevent one node from connecting to everything
//
// **Trust Level**: "I just moved here, so teachers are extra careful with me"
//   - Low trust: Requires stronger evidence before creating edges
//   - High trust: Can create edges more easily
//   - Pinned: Always trust (like family members)
//
// Think of it like a bouncer at a party:
//   - Deny list = banned from party
//   - Pin list = VIP, always allowed
//   - Max edges = party capacity (50 people max)
//   - Trust level = how strict the bouncer is checking IDs
package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
)

// TrustLevel defines how trustworthy a node is for edge materialization.
type TrustLevel int

const (
	// TrustLevelDefault is the standard trust level
	TrustLevelDefault TrustLevel = 0
	// TrustLevelLow requires higher confidence for edges
	TrustLevelLow TrustLevel = -1
	// TrustLevelHigh allows lower confidence edges
	TrustLevelHigh TrustLevel = 1
	// TrustLevelPinned edges never decay
	TrustLevelPinned TrustLevel = 2
)

// String returns the string representation of TrustLevel.
func (t TrustLevel) String() string {
	switch t {
	case TrustLevelLow:
		return "low"
	case TrustLevelDefault:
		return "default"
	case TrustLevelHigh:
		return "high"
	case TrustLevelPinned:
		return "pinned"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}

// ConfidenceAdjustment returns the confidence threshold adjustment for this trust level.
// Positive values increase required confidence, negative values decrease it.
func (t TrustLevel) ConfidenceAdjustment() float64 {
	switch t {
	case TrustLevelLow:
		return 0.2 // Require 20% higher confidence
	case TrustLevelHigh:
		return -0.1 // Allow 10% lower confidence
	case TrustLevelPinned:
		return -1.0 // Always allow (confidence effectively 0)
	default:
		return 0.0
	}
}

// LabelConfig defines per-label edge limits.
type LabelConfig struct {
	Label       string        `json:"label"`
	MaxEdges    int           `json:"max_edges"`     // 0 = unlimited
	MinConfidence float64     `json:"min_confidence"` // Override default threshold
	Cooldown    time.Duration `json:"cooldown"`      // Override default cooldown
	Disabled    bool          `json:"disabled"`      // Completely disable this label
}

// NodeConfig stores per-node edge materialization settings.
type NodeConfig struct {
	// Node identification
	NodeID    string    `json:"node_id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	// Pin/Deny lists
	PinList  []string `json:"pin_list"`  // Target node IDs that never decay
	DenyList []string `json:"deny_list"` // Target node IDs to never connect to

	// Edge caps
	MaxOutEdges   int `json:"max_out_edges"`   // Maximum outgoing edges (0 = unlimited)
	MaxInEdges    int `json:"max_in_edges"`    // Maximum incoming edges (0 = unlimited)
	MaxTotalEdges int `json:"max_total_edges"` // Maximum total edges (0 = unlimited)

	// Current edge counts (for cap enforcement)
	CurrentOutEdges   int `json:"current_out_edges"`
	CurrentInEdges    int `json:"current_in_edges"`
	CurrentTotalEdges int `json:"current_total_edges"`

	// Per-label configuration
	LabelConfigs map[string]LabelConfig `json:"label_configs"`

	// Trust level
	TrustLevel TrustLevel `json:"trust_level"`

	// Global overrides
	MinConfidence float64       `json:"min_confidence"` // Override default threshold (0 = use default)
	Cooldown      time.Duration `json:"cooldown"`       // Override default cooldown (0 = use default)
	Disabled      bool          `json:"disabled"`       // Completely disable edge creation to/from this node

	// Metadata
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// NewNodeConfig creates a new node config with defaults.
func NewNodeConfig(nodeID string) *NodeConfig {
	return &NodeConfig{
		NodeID:       nodeID,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		PinList:      make([]string, 0),
		DenyList:     make([]string, 0),
		LabelConfigs: make(map[string]LabelConfig),
		TrustLevel:   TrustLevelDefault,
		Metadata:     make(map[string]interface{}),
	}
}

// IsPinned returns true if the target is in the pin list.
func (c *NodeConfig) IsPinned(targetID string) bool {
	for _, id := range c.PinList {
		if id == targetID {
			return true
		}
	}
	return false
}

// IsDenied returns true if the target is in the deny list.
func (c *NodeConfig) IsDenied(targetID string) bool {
	for _, id := range c.DenyList {
		if id == targetID {
			return true
		}
	}
	return false
}

// AddToPin adds a target to the pin list.
func (c *NodeConfig) AddToPin(targetID string) {
	if !c.IsPinned(targetID) {
		c.PinList = append(c.PinList, targetID)
		c.UpdatedAt = time.Now()
	}
}

// RemoveFromPin removes a target from the pin list.
func (c *NodeConfig) RemoveFromPin(targetID string) bool {
	for i, id := range c.PinList {
		if id == targetID {
			c.PinList = append(c.PinList[:i], c.PinList[i+1:]...)
			c.UpdatedAt = time.Now()
			return true
		}
	}
	return false
}

// AddToDeny adds a target to the deny list.
func (c *NodeConfig) AddToDeny(targetID string) {
	if !c.IsDenied(targetID) {
		c.DenyList = append(c.DenyList, targetID)
		c.UpdatedAt = time.Now()
	}
}

// RemoveFromDeny removes a target from the deny list.
func (c *NodeConfig) RemoveFromDeny(targetID string) bool {
	for i, id := range c.DenyList {
		if id == targetID {
			c.DenyList = append(c.DenyList[:i], c.DenyList[i+1:]...)
			c.UpdatedAt = time.Now()
			return true
		}
	}
	return false
}

// CanAddOutEdge returns true if another outgoing edge can be added.
func (c *NodeConfig) CanAddOutEdge() bool {
	if c.MaxOutEdges <= 0 {
		return true // No limit
	}
	return c.CurrentOutEdges < c.MaxOutEdges
}

// CanAddInEdge returns true if another incoming edge can be added.
func (c *NodeConfig) CanAddInEdge() bool {
	if c.MaxInEdges <= 0 {
		return true // No limit
	}
	return c.CurrentInEdges < c.MaxInEdges
}

// CanAddEdge returns true if another edge can be added (checks all caps).
func (c *NodeConfig) CanAddEdge(isOutgoing bool) bool {
	// Check total cap
	if c.MaxTotalEdges > 0 && c.CurrentTotalEdges >= c.MaxTotalEdges {
		return false
	}

	// Check directional cap
	if isOutgoing {
		return c.CanAddOutEdge()
	}
	return c.CanAddInEdge()
}

// IncrementEdgeCount increments the edge count.
func (c *NodeConfig) IncrementEdgeCount(isOutgoing bool) {
	c.CurrentTotalEdges++
	if isOutgoing {
		c.CurrentOutEdges++
	} else {
		c.CurrentInEdges++
	}
	c.UpdatedAt = time.Now()
}

// DecrementEdgeCount decrements the edge count.
func (c *NodeConfig) DecrementEdgeCount(isOutgoing bool) {
	if c.CurrentTotalEdges > 0 {
		c.CurrentTotalEdges--
	}
	if isOutgoing && c.CurrentOutEdges > 0 {
		c.CurrentOutEdges--
	} else if !isOutgoing && c.CurrentInEdges > 0 {
		c.CurrentInEdges--
	}
	c.UpdatedAt = time.Now()
}

// GetLabelConfig returns the config for a specific label.
func (c *NodeConfig) GetLabelConfig(label string) (LabelConfig, bool) {
	cfg, ok := c.LabelConfigs[label]
	return cfg, ok
}

// SetLabelConfig sets the config for a specific label.
func (c *NodeConfig) SetLabelConfig(label string, cfg LabelConfig) {
	if c.LabelConfigs == nil {
		c.LabelConfigs = make(map[string]LabelConfig)
	}
	cfg.Label = label
	c.LabelConfigs[label] = cfg
	c.UpdatedAt = time.Now()
}

// GetEffectiveConfidence returns the effective minimum confidence threshold.
// Considers trust level adjustment and per-label overrides.
func (c *NodeConfig) GetEffectiveConfidence(label string, baseThreshold float64) float64 {
	// Start with base threshold
	threshold := baseThreshold

	// Apply trust level adjustment
	threshold += c.TrustLevel.ConfidenceAdjustment()

	// Apply node-level override if set
	if c.MinConfidence > 0 {
		threshold = c.MinConfidence
	}

	// Apply label-specific override if set
	if labelCfg, ok := c.LabelConfigs[label]; ok && labelCfg.MinConfidence > 0 {
		threshold = labelCfg.MinConfidence
	}

	// Clamp to valid range
	if threshold < 0 {
		threshold = 0
	}
	if threshold > 1 {
		threshold = 1
	}

	return threshold
}

// NodeConfigStore manages per-node configurations.
// Thread-safe for concurrent access.
type NodeConfigStore struct {
	mu      sync.RWMutex
	configs map[string]*NodeConfig // nodeID -> config

	// Stats
	totalConfigs int64
	totalChecks  int64
	totalBlocked int64
}

// NodeConfigStats provides observability into per-node config state.
type NodeConfigStats struct {
	TotalConfigs int64
	TotalChecks  int64
	TotalBlocked int64
	BlockRate    float64
}

// NewNodeConfigStore creates a new per-node config store.
func NewNodeConfigStore() *NodeConfigStore {
	return &NodeConfigStore{
		configs: make(map[string]*NodeConfig),
	}
}

// Get returns the config for a node, or nil if none exists.
func (s *NodeConfigStore) Get(nodeID string) *NodeConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cfg, exists := s.configs[nodeID]
	if !exists {
		return nil
	}

	// Return a copy to prevent mutation
	return copyNodeConfig(cfg)
}

// GetOrCreate returns the config for a node, creating one if it doesn't exist.
func (s *NodeConfigStore) GetOrCreate(nodeID string) *NodeConfig {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg, exists := s.configs[nodeID]
	if !exists {
		cfg = NewNodeConfig(nodeID)
		s.configs[nodeID] = cfg
		s.totalConfigs++
	}

	return copyNodeConfig(cfg)
}

// Set stores a node config.
func (s *NodeConfigStore) Set(cfg NodeConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg.UpdatedAt = time.Now()
	if cfg.CreatedAt.IsZero() {
		cfg.CreatedAt = time.Now()
	}

	_, exists := s.configs[cfg.NodeID]
	if !exists {
		s.totalConfigs++
	}

	s.configs[cfg.NodeID] = &cfg
}

// Delete removes a node config.
func (s *NodeConfigStore) Delete(nodeID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.configs[nodeID]
	if exists {
		delete(s.configs, nodeID)
		s.totalConfigs--
		return true
	}
	return false
}

// IsEdgeAllowed checks if an edge from source to target is allowed.
// Considers: feature flag, disabled state, deny list, and edge caps.
func (s *NodeConfigStore) IsEdgeAllowed(sourceID, targetID, label string) bool {
	// Feature flag check
	if !config.IsPerNodeConfigEnabled() {
		return true
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	s.totalChecks++

	// Check source config
	if srcCfg, exists := s.configs[sourceID]; exists {
		if srcCfg.Disabled {
			s.totalBlocked++
			return false
		}
		if srcCfg.IsDenied(targetID) {
			s.totalBlocked++
			return false
		}
		if !srcCfg.CanAddEdge(true) {
			s.totalBlocked++
			return false
		}
		// Check label-specific config
		if labelCfg, ok := srcCfg.LabelConfigs[label]; ok {
			if labelCfg.Disabled {
				s.totalBlocked++
				return false
			}
		}
	}

	// Check target config (incoming edge)
	if tgtCfg, exists := s.configs[targetID]; exists {
		if tgtCfg.Disabled {
			s.totalBlocked++
			return false
		}
		if !tgtCfg.CanAddEdge(false) {
			s.totalBlocked++
			return false
		}
	}

	return true
}

// IsEdgeAllowedWithReason checks if an edge is allowed and returns the reason.
func (s *NodeConfigStore) IsEdgeAllowedWithReason(sourceID, targetID, label string) (bool, string) {
	if !config.IsPerNodeConfigEnabled() {
		return true, "per-node config feature disabled"
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check source config
	if srcCfg, exists := s.configs[sourceID]; exists {
		if srcCfg.Disabled {
			return false, fmt.Sprintf("source node %s is disabled", sourceID)
		}
		if srcCfg.IsDenied(targetID) {
			return false, fmt.Sprintf("target %s is in source's deny list", targetID)
		}
		if !srcCfg.CanAddOutEdge() {
			return false, fmt.Sprintf("source %s has reached max outgoing edges (%d/%d)",
				sourceID, srcCfg.CurrentOutEdges, srcCfg.MaxOutEdges)
		}
		if srcCfg.MaxTotalEdges > 0 && srcCfg.CurrentTotalEdges >= srcCfg.MaxTotalEdges {
			return false, fmt.Sprintf("source %s has reached max total edges (%d/%d)",
				sourceID, srcCfg.CurrentTotalEdges, srcCfg.MaxTotalEdges)
		}
		if labelCfg, ok := srcCfg.LabelConfigs[label]; ok && labelCfg.Disabled {
			return false, fmt.Sprintf("label %s is disabled for source %s", label, sourceID)
		}
	}

	// Check target config
	if tgtCfg, exists := s.configs[targetID]; exists {
		if tgtCfg.Disabled {
			return false, fmt.Sprintf("target node %s is disabled", targetID)
		}
		if !tgtCfg.CanAddInEdge() {
			return false, fmt.Sprintf("target %s has reached max incoming edges (%d/%d)",
				targetID, tgtCfg.CurrentInEdges, tgtCfg.MaxInEdges)
		}
	}

	return true, "edge allowed"
}

// IsPinned checks if an edge should never decay.
func (s *NodeConfigStore) IsPinned(sourceID, targetID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if srcCfg, exists := s.configs[sourceID]; exists {
		if srcCfg.IsPinned(targetID) {
			return true
		}
		if srcCfg.TrustLevel == TrustLevelPinned {
			return true
		}
	}

	return false
}

// GetEffectiveConfidence returns the effective confidence threshold for an edge.
func (s *NodeConfigStore) GetEffectiveConfidence(sourceID, targetID, label string, baseThreshold float64) float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	threshold := baseThreshold

	// Apply source config adjustments
	if srcCfg, exists := s.configs[sourceID]; exists {
		threshold = srcCfg.GetEffectiveConfidence(label, threshold)
	}

	return threshold
}

// RecordEdgeCreation updates edge counts after an edge is created.
func (s *NodeConfigStore) RecordEdgeCreation(sourceID, targetID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if srcCfg, exists := s.configs[sourceID]; exists {
		srcCfg.IncrementEdgeCount(true)
	}

	if tgtCfg, exists := s.configs[targetID]; exists {
		tgtCfg.IncrementEdgeCount(false)
	}
}

// RecordEdgeDeletion updates edge counts after an edge is deleted.
func (s *NodeConfigStore) RecordEdgeDeletion(sourceID, targetID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if srcCfg, exists := s.configs[sourceID]; exists {
		srcCfg.DecrementEdgeCount(true)
	}

	if tgtCfg, exists := s.configs[targetID]; exists {
		tgtCfg.DecrementEdgeCount(false)
	}
}

// AddToNodePinList adds a target to a node's pin list.
func (s *NodeConfigStore) AddToNodePinList(nodeID, targetID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg, exists := s.configs[nodeID]
	if !exists {
		cfg = NewNodeConfig(nodeID)
		s.configs[nodeID] = cfg
		s.totalConfigs++
	}

	cfg.AddToPin(targetID)
}

// AddToNodeDenyList adds a target to a node's deny list.
func (s *NodeConfigStore) AddToNodeDenyList(nodeID, targetID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg, exists := s.configs[nodeID]
	if !exists {
		cfg = NewNodeConfig(nodeID)
		s.configs[nodeID] = cfg
		s.totalConfigs++
	}

	cfg.AddToDeny(targetID)
}

// SetNodeTrustLevel sets the trust level for a node.
func (s *NodeConfigStore) SetNodeTrustLevel(nodeID string, level TrustLevel) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg, exists := s.configs[nodeID]
	if !exists {
		cfg = NewNodeConfig(nodeID)
		s.configs[nodeID] = cfg
		s.totalConfigs++
	}

	cfg.TrustLevel = level
	cfg.UpdatedAt = time.Now()
}

// SetNodeEdgeCaps sets edge caps for a node.
func (s *NodeConfigStore) SetNodeEdgeCaps(nodeID string, maxOut, maxIn, maxTotal int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg, exists := s.configs[nodeID]
	if !exists {
		cfg = NewNodeConfig(nodeID)
		s.configs[nodeID] = cfg
		s.totalConfigs++
	}

	cfg.MaxOutEdges = maxOut
	cfg.MaxInEdges = maxIn
	cfg.MaxTotalEdges = maxTotal
	cfg.UpdatedAt = time.Now()
}

// DisableNode disables all edge creation to/from a node.
func (s *NodeConfigStore) DisableNode(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg, exists := s.configs[nodeID]
	if !exists {
		cfg = NewNodeConfig(nodeID)
		s.configs[nodeID] = cfg
		s.totalConfigs++
	}

	cfg.Disabled = true
	cfg.UpdatedAt = time.Now()
}

// EnableNode enables edge creation to/from a node.
func (s *NodeConfigStore) EnableNode(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cfg, exists := s.configs[nodeID]; exists {
		cfg.Disabled = false
		cfg.UpdatedAt = time.Now()
	}
}

// Stats returns current store statistics.
func (s *NodeConfigStore) Stats() NodeConfigStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := NodeConfigStats{
		TotalConfigs: s.totalConfigs,
		TotalChecks:  s.totalChecks,
		TotalBlocked: s.totalBlocked,
	}

	if stats.TotalChecks > 0 {
		stats.BlockRate = float64(stats.TotalBlocked) / float64(stats.TotalChecks)
	}

	return stats
}

// Size returns the number of configured nodes.
func (s *NodeConfigStore) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.configs)
}

// Clear removes all node configs.
func (s *NodeConfigStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configs = make(map[string]*NodeConfig)
	s.totalConfigs = 0
	s.totalChecks = 0
	s.totalBlocked = 0
}

// GetAllNodeIDs returns all configured node IDs.
func (s *NodeConfigStore) GetAllNodeIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]string, 0, len(s.configs))
	for id := range s.configs {
		ids = append(ids, id)
	}
	return ids
}

// GetPinnedTargets returns all pinned targets for a node.
func (s *NodeConfigStore) GetPinnedTargets(nodeID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if cfg, exists := s.configs[nodeID]; exists {
		result := make([]string, len(cfg.PinList))
		copy(result, cfg.PinList)
		return result
	}
	return []string{}
}

// GetDeniedTargets returns all denied targets for a node.
func (s *NodeConfigStore) GetDeniedTargets(nodeID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if cfg, exists := s.configs[nodeID]; exists {
		result := make([]string, len(cfg.DenyList))
		copy(result, cfg.DenyList)
		return result
	}
	return []string{}
}

// Export returns all configs for backup/export.
func (s *NodeConfigStore) Export() []*NodeConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*NodeConfig, 0, len(s.configs))
	for _, cfg := range s.configs {
		result = append(result, copyNodeConfig(cfg))
	}
	return result
}

// Import loads configs from backup/import.
// Existing configs are NOT cleared - use Clear() first if needed.
func (s *NodeConfigStore) Import(configs []*NodeConfig) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	imported := 0
	for _, cfg := range configs {
		if cfg == nil || cfg.NodeID == "" {
			continue
		}

		_, exists := s.configs[cfg.NodeID]
		if !exists {
			s.totalConfigs++
		}

		s.configs[cfg.NodeID] = copyNodeConfig(cfg)
		imported++
	}
	return imported
}

// copyNodeConfig creates a deep copy of a NodeConfig.
func copyNodeConfig(cfg *NodeConfig) *NodeConfig {
	if cfg == nil {
		return nil
	}

	copy := &NodeConfig{
		NodeID:            cfg.NodeID,
		CreatedAt:         cfg.CreatedAt,
		UpdatedAt:         cfg.UpdatedAt,
		MaxOutEdges:       cfg.MaxOutEdges,
		MaxInEdges:        cfg.MaxInEdges,
		MaxTotalEdges:     cfg.MaxTotalEdges,
		CurrentOutEdges:   cfg.CurrentOutEdges,
		CurrentInEdges:    cfg.CurrentInEdges,
		CurrentTotalEdges: cfg.CurrentTotalEdges,
		TrustLevel:        cfg.TrustLevel,
		MinConfidence:     cfg.MinConfidence,
		Cooldown:          cfg.Cooldown,
		Disabled:          cfg.Disabled,
	}

	// Copy slices
	copy.PinList = make([]string, len(cfg.PinList))
	for i, v := range cfg.PinList {
		copy.PinList[i] = v
	}

	copy.DenyList = make([]string, len(cfg.DenyList))
	for i, v := range cfg.DenyList {
		copy.DenyList[i] = v
	}

	// Copy maps
	copy.LabelConfigs = make(map[string]LabelConfig, len(cfg.LabelConfigs))
	for k, v := range cfg.LabelConfigs {
		copy.LabelConfigs[k] = v
	}

	copy.Metadata = make(map[string]interface{}, len(cfg.Metadata))
	for k, v := range cfg.Metadata {
		copy.Metadata[k] = v
	}

	return copy
}

// NodeConfigStoreOption configures a NodeConfigStore.
type NodeConfigStoreOption func(*NodeConfigStore)

// NewNodeConfigStoreWithOptions creates a store with functional options.
func NewNodeConfigStoreWithOptions(opts ...NodeConfigStoreOption) *NodeConfigStore {
	s := NewNodeConfigStore()
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Global singleton for convenience
var globalNodeConfigStore *NodeConfigStore
var globalNodeConfigOnce sync.Once

// GlobalNodeConfigStore returns the global node config store singleton.
func GlobalNodeConfigStore() *NodeConfigStore {
	globalNodeConfigOnce.Do(func() {
		globalNodeConfigStore = NewNodeConfigStore()
	})
	return globalNodeConfigStore
}

// ResetGlobalNodeConfigStore resets the global node config store.
// Primarily for testing.
func ResetGlobalNodeConfigStore() {
	globalNodeConfigOnce = sync.Once{}
	globalNodeConfigStore = nil
}

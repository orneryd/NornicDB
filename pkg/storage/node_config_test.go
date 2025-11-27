package storage

import (
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
)

func TestTrustLevel_String(t *testing.T) {
	tests := []struct {
		level    TrustLevel
		expected string
	}{
		{TrustLevelLow, "low"},
		{TrustLevelDefault, "default"},
		{TrustLevelHigh, "high"},
		{TrustLevelPinned, "pinned"},
		{TrustLevel(99), "unknown(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.level.String(); got != tt.expected {
				t.Errorf("TrustLevel.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTrustLevel_ConfidenceAdjustment(t *testing.T) {
	tests := []struct {
		level    TrustLevel
		expected float64
	}{
		{TrustLevelLow, 0.2},
		{TrustLevelDefault, 0.0},
		{TrustLevelHigh, -0.1},
		{TrustLevelPinned, -1.0},
		{TrustLevel(99), 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.level.String(), func(t *testing.T) {
			if got := tt.level.ConfidenceAdjustment(); got != tt.expected {
				t.Errorf("ConfidenceAdjustment() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestNewNodeConfig(t *testing.T) {
	cfg := NewNodeConfig("test-node")

	if cfg.NodeID != "test-node" {
		t.Errorf("NodeID = %v, want test-node", cfg.NodeID)
	}
	if cfg.TrustLevel != TrustLevelDefault {
		t.Errorf("TrustLevel = %v, want TrustLevelDefault", cfg.TrustLevel)
	}
	if len(cfg.PinList) != 0 {
		t.Errorf("PinList should be empty")
	}
	if len(cfg.DenyList) != 0 {
		t.Errorf("DenyList should be empty")
	}
	if cfg.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set")
	}
}

func TestNodeConfig_PinList(t *testing.T) {
	cfg := NewNodeConfig("test")

	// Initially not pinned
	if cfg.IsPinned("target") {
		t.Error("target should not be pinned initially")
	}

	// Add to pin list
	cfg.AddToPin("target")
	if !cfg.IsPinned("target") {
		t.Error("target should be pinned after AddToPin")
	}

	// Adding again should be idempotent
	cfg.AddToPin("target")
	if len(cfg.PinList) != 1 {
		t.Errorf("PinList should have 1 entry, got %d", len(cfg.PinList))
	}

	// Remove from pin list
	if !cfg.RemoveFromPin("target") {
		t.Error("RemoveFromPin should return true")
	}
	if cfg.IsPinned("target") {
		t.Error("target should not be pinned after RemoveFromPin")
	}

	// Removing non-existent should return false
	if cfg.RemoveFromPin("nonexistent") {
		t.Error("RemoveFromPin should return false for non-existent")
	}
}

func TestNodeConfig_DenyList(t *testing.T) {
	cfg := NewNodeConfig("test")

	// Initially not denied
	if cfg.IsDenied("target") {
		t.Error("target should not be denied initially")
	}

	// Add to deny list
	cfg.AddToDeny("target")
	if !cfg.IsDenied("target") {
		t.Error("target should be denied after AddToDeny")
	}

	// Adding again should be idempotent
	cfg.AddToDeny("target")
	if len(cfg.DenyList) != 1 {
		t.Errorf("DenyList should have 1 entry, got %d", len(cfg.DenyList))
	}

	// Remove from deny list
	if !cfg.RemoveFromDeny("target") {
		t.Error("RemoveFromDeny should return true")
	}
	if cfg.IsDenied("target") {
		t.Error("target should not be denied after RemoveFromDeny")
	}

	// Removing non-existent should return false
	if cfg.RemoveFromDeny("nonexistent") {
		t.Error("RemoveFromDeny should return false for non-existent")
	}
}

func TestNodeConfig_EdgeCaps(t *testing.T) {
	cfg := NewNodeConfig("test")

	// No limits by default
	if !cfg.CanAddOutEdge() {
		t.Error("should allow out edge with no limit")
	}
	if !cfg.CanAddInEdge() {
		t.Error("should allow in edge with no limit")
	}
	if !cfg.CanAddEdge(true) {
		t.Error("should allow edge with no limits")
	}
	if !cfg.CanAddEdge(false) {
		t.Error("should allow edge with no limits")
	}

	// Set caps
	cfg.MaxOutEdges = 2
	cfg.MaxInEdges = 3
	cfg.MaxTotalEdges = 4

	// Increment edge counts
	cfg.IncrementEdgeCount(true)
	if cfg.CurrentOutEdges != 1 || cfg.CurrentTotalEdges != 1 {
		t.Error("IncrementEdgeCount(true) failed")
	}

	cfg.IncrementEdgeCount(false)
	if cfg.CurrentInEdges != 1 || cfg.CurrentTotalEdges != 2 {
		t.Error("IncrementEdgeCount(false) failed")
	}

	// Still under limits
	if !cfg.CanAddOutEdge() {
		t.Error("should still allow out edge (1 < 2)")
	}

	// Add more to reach out limit
	cfg.IncrementEdgeCount(true)
	if cfg.CanAddOutEdge() {
		t.Error("should NOT allow out edge (2 >= 2)")
	}

	// Check total cap
	cfg.IncrementEdgeCount(false)
	if cfg.CanAddEdge(false) {
		t.Error("should NOT allow any edge (4 >= 4 total)")
	}

	// Decrement
	cfg.DecrementEdgeCount(true)
	if cfg.CurrentOutEdges != 1 {
		t.Errorf("CurrentOutEdges after decrement = %d, want 1", cfg.CurrentOutEdges)
	}

	cfg.DecrementEdgeCount(false)
	if cfg.CurrentInEdges != 1 {
		t.Errorf("CurrentInEdges after decrement = %d, want 1", cfg.CurrentInEdges)
	}

	// Decrement past zero should stay at zero
	cfg.CurrentOutEdges = 0
	cfg.CurrentTotalEdges = 0
	cfg.DecrementEdgeCount(true)
	if cfg.CurrentOutEdges != 0 {
		t.Error("CurrentOutEdges should not go below 0")
	}
}

func TestNodeConfig_LabelConfig(t *testing.T) {
	cfg := NewNodeConfig("test")

	// No config initially
	_, ok := cfg.GetLabelConfig("relates_to")
	if ok {
		t.Error("should not have label config initially")
	}

	// Set config
	labelCfg := LabelConfig{
		MaxEdges:      10,
		MinConfidence: 0.8,
		Disabled:      false,
	}
	cfg.SetLabelConfig("relates_to", labelCfg)

	// Get config
	got, ok := cfg.GetLabelConfig("relates_to")
	if !ok {
		t.Error("should have label config after SetLabelConfig")
	}
	if got.MaxEdges != 10 {
		t.Errorf("MaxEdges = %d, want 10", got.MaxEdges)
	}
	if got.Label != "relates_to" {
		t.Errorf("Label = %s, want relates_to", got.Label)
	}

	// SetLabelConfig with nil map
	cfg2 := &NodeConfig{NodeID: "test2"}
	cfg2.SetLabelConfig("test", LabelConfig{MaxEdges: 5})
	if cfg2.LabelConfigs == nil {
		t.Error("LabelConfigs should be initialized")
	}
}

func TestNodeConfig_GetEffectiveConfidence(t *testing.T) {
	cfg := NewNodeConfig("test")
	baseThreshold := 0.5

	// Default trust level - no adjustment
	got := cfg.GetEffectiveConfidence("any", baseThreshold)
	if got != 0.5 {
		t.Errorf("got %f, want 0.5", got)
	}

	// Low trust level - higher threshold
	cfg.TrustLevel = TrustLevelLow
	got = cfg.GetEffectiveConfidence("any", baseThreshold)
	if got != 0.7 {
		t.Errorf("got %f, want 0.7", got)
	}

	// High trust level - lower threshold
	cfg.TrustLevel = TrustLevelHigh
	got = cfg.GetEffectiveConfidence("any", baseThreshold)
	if got != 0.4 {
		t.Errorf("got %f, want 0.4", got)
	}

	// Pinned - always allow
	cfg.TrustLevel = TrustLevelPinned
	got = cfg.GetEffectiveConfidence("any", baseThreshold)
	if got != 0 {
		t.Errorf("got %f, want 0 (clamped)", got)
	}

	// Node-level override
	cfg.TrustLevel = TrustLevelDefault
	cfg.MinConfidence = 0.9
	got = cfg.GetEffectiveConfidence("any", baseThreshold)
	if got != 0.9 {
		t.Errorf("got %f, want 0.9", got)
	}

	// Label-specific override takes precedence
	cfg.SetLabelConfig("special", LabelConfig{MinConfidence: 0.3})
	got = cfg.GetEffectiveConfidence("special", baseThreshold)
	if got != 0.3 {
		t.Errorf("got %f, want 0.3", got)
	}

	// Test clamping to 1.0
	cfg.MinConfidence = 1.5
	cfg.TrustLevel = TrustLevelDefault
	got = cfg.GetEffectiveConfidence("other", baseThreshold)
	if got != 1.0 {
		t.Errorf("got %f, want 1.0 (clamped)", got)
	}
}

func TestNodeConfigStore_BasicOperations(t *testing.T) {
	store := NewNodeConfigStore()

	// Get non-existent returns nil
	if store.Get("nonexistent") != nil {
		t.Error("Get should return nil for non-existent")
	}

	// Set config
	cfg := NodeConfig{
		NodeID:      "node-1",
		MaxOutEdges: 10,
	}
	store.Set(cfg)

	// Get returns copy
	got := store.Get("node-1")
	if got == nil {
		t.Fatal("Get should return config")
	}
	if got.MaxOutEdges != 10 {
		t.Errorf("MaxOutEdges = %d, want 10", got.MaxOutEdges)
	}

	// Verify it's a copy
	got.MaxOutEdges = 99
	original := store.Get("node-1")
	if original.MaxOutEdges != 10 {
		t.Error("Get should return copy, not original")
	}

	// Delete
	if !store.Delete("node-1") {
		t.Error("Delete should return true")
	}
	if store.Get("node-1") != nil {
		t.Error("Get after Delete should return nil")
	}

	// Delete non-existent
	if store.Delete("nonexistent") {
		t.Error("Delete should return false for non-existent")
	}
}

func TestNodeConfigStore_GetOrCreate(t *testing.T) {
	store := NewNodeConfigStore()

	// GetOrCreate creates new
	cfg := store.GetOrCreate("node-1")
	if cfg == nil {
		t.Fatal("GetOrCreate should return config")
	}
	if cfg.NodeID != "node-1" {
		t.Errorf("NodeID = %s, want node-1", cfg.NodeID)
	}

	// GetOrCreate returns existing
	store.SetNodeEdgeCaps("node-1", 5, 10, 15)
	cfg = store.GetOrCreate("node-1")
	if cfg.MaxOutEdges != 5 {
		t.Errorf("MaxOutEdges = %d, want 5", cfg.MaxOutEdges)
	}

	// Size should be 1
	if store.Size() != 1 {
		t.Errorf("Size = %d, want 1", store.Size())
	}
}

func TestNodeConfigStore_IsEdgeAllowed(t *testing.T) {
	// Enable feature flag for this test
	defer config.WithPerNodeConfigEnabled()()

	store := NewNodeConfigStore()

	// Without any config, everything is allowed
	if !store.IsEdgeAllowed("src", "tgt", "relates_to") {
		t.Error("should allow edge with no config")
	}

	// Disable source node
	store.DisableNode("src")
	if store.IsEdgeAllowed("src", "tgt", "relates_to") {
		t.Error("should NOT allow edge from disabled source")
	}

	// Re-enable
	store.EnableNode("src")
	if !store.IsEdgeAllowed("src", "tgt", "relates_to") {
		t.Error("should allow edge after re-enabling")
	}

	// Add to deny list
	store.AddToNodeDenyList("src", "bad-target")
	if store.IsEdgeAllowed("src", "bad-target", "relates_to") {
		t.Error("should NOT allow edge to denied target")
	}
	if !store.IsEdgeAllowed("src", "other-target", "relates_to") {
		t.Error("should allow edge to non-denied target")
	}

	// Edge caps - test outgoing cap
	store.SetNodeEdgeCaps("limited", 2, 0, 0) // max 2 outgoing
	if !store.IsEdgeAllowed("limited", "t1", "relates_to") {
		t.Error("should allow first edge")
	}
	store.RecordEdgeCreation("limited", "t1")
	if !store.IsEdgeAllowed("limited", "t2", "relates_to") {
		t.Error("should allow second edge (1/2 out)")
	}
	store.RecordEdgeCreation("limited", "t2")
	if store.IsEdgeAllowed("limited", "t3", "relates_to") {
		t.Error("should NOT allow third edge (at out cap)")
	}

	// Edge caps - test total cap
	store.Clear()
	store.SetNodeEdgeCaps("total-limited", 0, 0, 2) // max 2 total
	store.RecordEdgeCreation("total-limited", "t1")
	store.RecordEdgeCreation("total-limited", "t2")
	if store.IsEdgeAllowed("total-limited", "t3", "relates_to") {
		t.Error("should NOT allow edge at total cap")
	}

	// Target disabled
	store.DisableNode("disabled-target")
	if store.IsEdgeAllowed("src", "disabled-target", "relates_to") {
		t.Error("should NOT allow edge to disabled target")
	}

	// Target at incoming cap
	store.Clear()
	store.SetNodeEdgeCaps("capped-target", 0, 1, 0)
	store.RecordEdgeCreation("any", "capped-target")
	if store.IsEdgeAllowed("src", "capped-target", "relates_to") {
		t.Error("should NOT allow edge to target at incoming cap")
	}

	// Label disabled
	store.Clear()
	cfg := NewNodeConfig("src2")
	cfg.SetLabelConfig("disabled_label", LabelConfig{Disabled: true})
	store.Set(*cfg)
	if store.IsEdgeAllowed("src2", "tgt", "disabled_label") {
		t.Error("should NOT allow edge with disabled label")
	}
}

func TestNodeConfigStore_IsEdgeAllowed_FeatureDisabled(t *testing.T) {
	// Disable feature flag
	defer config.WithPerNodeConfigDisabled()()

	store := NewNodeConfigStore()
	store.DisableNode("src")

	// With feature disabled, always allow
	if !store.IsEdgeAllowed("src", "tgt", "relates_to") {
		t.Error("should allow edge when feature is disabled")
	}
}

func TestNodeConfigStore_IsEdgeAllowedWithReason(t *testing.T) {
	defer config.WithPerNodeConfigEnabled()()

	store := NewNodeConfigStore()

	// No config
	allowed, reason := store.IsEdgeAllowedWithReason("src", "tgt", "rel")
	if !allowed || reason != "edge allowed" {
		t.Errorf("got %v, %s", allowed, reason)
	}

	// Disabled source
	store.DisableNode("disabled-src")
	allowed, reason = store.IsEdgeAllowedWithReason("disabled-src", "tgt", "rel")
	if allowed {
		t.Error("should not be allowed")
	}
	if reason == "" {
		t.Error("should have reason")
	}

	// Deny list
	store.Clear()
	store.AddToNodeDenyList("src", "denied")
	allowed, reason = store.IsEdgeAllowedWithReason("src", "denied", "rel")
	if allowed {
		t.Error("should not be allowed")
	}

	// Max out edges
	store.Clear()
	store.SetNodeEdgeCaps("capped", 1, 0, 0)
	store.RecordEdgeCreation("capped", "t1")
	allowed, reason = store.IsEdgeAllowedWithReason("capped", "t2", "rel")
	if allowed {
		t.Error("should not be allowed - at out cap")
	}

	// Max total edges
	store.Clear()
	store.SetNodeEdgeCaps("total-capped", 0, 0, 1)
	store.RecordEdgeCreation("total-capped", "t1")
	allowed, reason = store.IsEdgeAllowedWithReason("total-capped", "t2", "rel")
	if allowed {
		t.Error("should not be allowed - at total cap")
	}

	// Label disabled
	store.Clear()
	cfg := NewNodeConfig("label-src")
	cfg.SetLabelConfig("blocked", LabelConfig{Disabled: true})
	store.Set(*cfg)
	allowed, reason = store.IsEdgeAllowedWithReason("label-src", "tgt", "blocked")
	if allowed {
		t.Error("should not be allowed - label disabled")
	}

	// Target disabled
	store.Clear()
	store.DisableNode("disabled-tgt")
	allowed, reason = store.IsEdgeAllowedWithReason("src", "disabled-tgt", "rel")
	if allowed {
		t.Error("should not be allowed - target disabled")
	}

	// Target at in cap
	store.Clear()
	store.SetNodeEdgeCaps("in-capped", 0, 1, 0)
	store.RecordEdgeCreation("other", "in-capped")
	allowed, reason = store.IsEdgeAllowedWithReason("src", "in-capped", "rel")
	if allowed {
		t.Error("should not be allowed - target at in cap")
	}

	// Feature disabled
	defer config.WithPerNodeConfigDisabled()()
	allowed, reason = store.IsEdgeAllowedWithReason("any", "any", "any")
	if !allowed || reason != "per-node config feature disabled" {
		t.Error("should be allowed when feature disabled")
	}
}

func TestNodeConfigStore_IsPinned(t *testing.T) {
	store := NewNodeConfigStore()

	// Not pinned by default
	if store.IsPinned("src", "tgt") {
		t.Error("should not be pinned by default")
	}

	// Add to pin list
	store.AddToNodePinList("src", "tgt")
	if !store.IsPinned("src", "tgt") {
		t.Error("should be pinned after AddToNodePinList")
	}

	// Pinned trust level
	store.Clear()
	store.SetNodeTrustLevel("high-trust", TrustLevelPinned)
	if !store.IsPinned("high-trust", "any-target") {
		t.Error("should be pinned with TrustLevelPinned")
	}
}

func TestNodeConfigStore_GetEffectiveConfidence(t *testing.T) {
	store := NewNodeConfigStore()
	base := 0.5

	// No config - returns base
	got := store.GetEffectiveConfidence("src", "tgt", "rel", base)
	if got != base {
		t.Errorf("got %f, want %f", got, base)
	}

	// With config
	store.SetNodeTrustLevel("low-trust", TrustLevelLow)
	got = store.GetEffectiveConfidence("low-trust", "tgt", "rel", base)
	if got != 0.7 {
		t.Errorf("got %f, want 0.7", got)
	}
}

func TestNodeConfigStore_EdgeCounting(t *testing.T) {
	store := NewNodeConfigStore()

	// Create configs first
	store.GetOrCreate("src")
	store.GetOrCreate("tgt")

	// Record creation
	store.RecordEdgeCreation("src", "tgt")

	srcCfg := store.Get("src")
	if srcCfg.CurrentOutEdges != 1 {
		t.Errorf("src out edges = %d, want 1", srcCfg.CurrentOutEdges)
	}

	tgtCfg := store.Get("tgt")
	if tgtCfg.CurrentInEdges != 1 {
		t.Errorf("tgt in edges = %d, want 1", tgtCfg.CurrentInEdges)
	}

	// Record deletion
	store.RecordEdgeDeletion("src", "tgt")

	srcCfg = store.Get("src")
	if srcCfg.CurrentOutEdges != 0 {
		t.Errorf("src out edges after delete = %d, want 0", srcCfg.CurrentOutEdges)
	}
}

func TestNodeConfigStore_ConvenienceMethods(t *testing.T) {
	store := NewNodeConfigStore()

	// AddToNodePinList creates config
	store.AddToNodePinList("new-node", "target")
	cfg := store.Get("new-node")
	if cfg == nil || !cfg.IsPinned("target") {
		t.Error("AddToNodePinList should create config and add pin")
	}

	// AddToNodeDenyList creates config
	store.AddToNodeDenyList("deny-node", "bad")
	cfg = store.Get("deny-node")
	if cfg == nil || !cfg.IsDenied("bad") {
		t.Error("AddToNodeDenyList should create config and add deny")
	}

	// SetNodeTrustLevel creates config
	store.SetNodeTrustLevel("trust-node", TrustLevelHigh)
	cfg = store.Get("trust-node")
	if cfg == nil || cfg.TrustLevel != TrustLevelHigh {
		t.Error("SetNodeTrustLevel should create config and set level")
	}

	// SetNodeEdgeCaps creates config
	store.SetNodeEdgeCaps("cap-node", 1, 2, 3)
	cfg = store.Get("cap-node")
	if cfg == nil || cfg.MaxOutEdges != 1 || cfg.MaxInEdges != 2 || cfg.MaxTotalEdges != 3 {
		t.Error("SetNodeEdgeCaps should create config and set caps")
	}

	// DisableNode creates config
	store.DisableNode("disable-node")
	cfg = store.Get("disable-node")
	if cfg == nil || !cfg.Disabled {
		t.Error("DisableNode should create config and disable")
	}

	// EnableNode on non-existent does nothing
	store.EnableNode("nonexistent")
	if store.Get("nonexistent") != nil {
		t.Error("EnableNode should not create config")
	}
}

func TestNodeConfigStore_Stats(t *testing.T) {
	defer config.WithPerNodeConfigEnabled()()

	store := NewNodeConfigStore()

	// Initial stats
	stats := store.Stats()
	if stats.TotalConfigs != 0 {
		t.Errorf("TotalConfigs = %d, want 0", stats.TotalConfigs)
	}

	// Add some configs
	store.GetOrCreate("n1")
	store.GetOrCreate("n2")

	stats = store.Stats()
	if stats.TotalConfigs != 2 {
		t.Errorf("TotalConfigs = %d, want 2", stats.TotalConfigs)
	}

	// Make some checks
	store.DisableNode("n1")
	store.IsEdgeAllowed("n1", "x", "rel") // Blocked
	store.IsEdgeAllowed("n2", "x", "rel") // Allowed

	stats = store.Stats()
	if stats.TotalBlocked != 1 {
		t.Errorf("TotalBlocked = %d, want 1", stats.TotalBlocked)
	}
	if stats.BlockRate == 0 {
		t.Error("BlockRate should be > 0")
	}
}

func TestNodeConfigStore_Lists(t *testing.T) {
	store := NewNodeConfigStore()

	store.AddToNodePinList("n1", "p1")
	store.AddToNodePinList("n1", "p2")
	store.AddToNodeDenyList("n1", "d1")

	pinned := store.GetPinnedTargets("n1")
	if len(pinned) != 2 {
		t.Errorf("got %d pinned, want 2", len(pinned))
	}

	denied := store.GetDeniedTargets("n1")
	if len(denied) != 1 {
		t.Errorf("got %d denied, want 1", len(denied))
	}

	// Non-existent node returns empty
	pinned = store.GetPinnedTargets("nonexistent")
	if len(pinned) != 0 {
		t.Error("should return empty for non-existent")
	}

	denied = store.GetDeniedTargets("nonexistent")
	if len(denied) != 0 {
		t.Error("should return empty for non-existent")
	}
}

func TestNodeConfigStore_GetAllNodeIDs(t *testing.T) {
	store := NewNodeConfigStore()

	store.GetOrCreate("n1")
	store.GetOrCreate("n2")
	store.GetOrCreate("n3")

	ids := store.GetAllNodeIDs()
	if len(ids) != 3 {
		t.Errorf("got %d IDs, want 3", len(ids))
	}
}

func TestNodeConfigStore_ExportImport(t *testing.T) {
	store := NewNodeConfigStore()

	// Create some configs
	store.GetOrCreate("n1")
	store.AddToNodePinList("n1", "pinned")
	store.SetNodeTrustLevel("n2", TrustLevelHigh)

	// Export
	exported := store.Export()
	if len(exported) != 2 {
		t.Errorf("exported %d, want 2", len(exported))
	}

	// Import to new store
	store2 := NewNodeConfigStore()
	imported := store2.Import(exported)
	if imported != 2 {
		t.Errorf("imported %d, want 2", imported)
	}

	// Verify
	cfg := store2.Get("n1")
	if cfg == nil || !cfg.IsPinned("pinned") {
		t.Error("imported config should have pinned target")
	}

	cfg = store2.Get("n2")
	if cfg == nil || cfg.TrustLevel != TrustLevelHigh {
		t.Error("imported config should have trust level")
	}

	// Import handles nil and empty NodeID
	store3 := NewNodeConfigStore()
	imported = store3.Import([]*NodeConfig{nil, {NodeID: ""}, {NodeID: "valid"}})
	if imported != 1 {
		t.Errorf("imported %d, want 1 (skipping nil and empty)", imported)
	}
}

func TestNodeConfigStore_Clear(t *testing.T) {
	store := NewNodeConfigStore()

	store.GetOrCreate("n1")
	store.GetOrCreate("n2")

	store.Clear()

	if store.Size() != 0 {
		t.Errorf("Size after Clear = %d, want 0", store.Size())
	}
	if store.Get("n1") != nil {
		t.Error("Get after Clear should return nil")
	}
}

func TestNodeConfigStore_Concurrent(t *testing.T) {
	defer config.WithPerNodeConfigEnabled()()

	store := NewNodeConfigStore()
	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			nodeID := "node-" + string(rune('A'+n%26))
			store.GetOrCreate(nodeID)
			store.AddToNodePinList(nodeID, "target")
			store.IsEdgeAllowed(nodeID, "target", "rel")
		}(i)
	}

	wg.Wait()

	// Should not panic
	stats := store.Stats()
	if stats.TotalConfigs == 0 {
		t.Error("should have some configs")
	}
}

func TestGlobalNodeConfigStore(t *testing.T) {
	// Reset any previous state
	ResetGlobalNodeConfigStore()

	store1 := GlobalNodeConfigStore()
	store2 := GlobalNodeConfigStore()

	if store1 != store2 {
		t.Error("GlobalNodeConfigStore should return same instance")
	}

	store1.GetOrCreate("test")
	if store2.Size() != 1 {
		t.Error("should be the same store")
	}

	// Reset
	ResetGlobalNodeConfigStore()
	store3 := GlobalNodeConfigStore()
	if store3.Size() != 0 {
		t.Error("Reset should clear global store")
	}
}

func TestNewNodeConfigStoreWithOptions(t *testing.T) {
	store := NewNodeConfigStoreWithOptions()
	if store == nil {
		t.Error("should return store")
	}
}

func TestCopyNodeConfig(t *testing.T) {
	// Test nil copy
	if copyNodeConfig(nil) != nil {
		t.Error("copyNodeConfig(nil) should return nil")
	}

	// Test deep copy
	original := NewNodeConfig("test")
	original.PinList = []string{"a", "b"}
	original.DenyList = []string{"x"}
	original.LabelConfigs["rel"] = LabelConfig{MaxEdges: 5}
	original.Metadata["key"] = "value"

	copy := copyNodeConfig(original)

	// Modify original
	original.PinList[0] = "modified"
	original.DenyList[0] = "modified"
	original.LabelConfigs["rel"] = LabelConfig{MaxEdges: 99}
	original.Metadata["key"] = "modified"

	// Copy should be unchanged
	if copy.PinList[0] != "a" {
		t.Error("copy PinList should be independent")
	}
	if copy.DenyList[0] != "x" {
		t.Error("copy DenyList should be independent")
	}
	if copy.LabelConfigs["rel"].MaxEdges != 5 {
		t.Error("copy LabelConfigs should be independent")
	}
	if copy.Metadata["key"] != "value" {
		t.Error("copy Metadata should be independent")
	}
}

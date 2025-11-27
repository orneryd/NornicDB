// Package inference provides edge materialization with cooldown protection.
//
// Cooldown logic prevents echo chambers from rapid co-access bursts by
// enforcing minimum time between materializations of the same edge pair.
//
// Feature flag: NORNICDB_COOLDOWN_ENABLED=true (enabled by default)
//
// Usage Example 1: Basic cooldown check
//
//	table := NewCooldownTable()
//	if table.CanMaterialize("nodeA", "nodeB", "relates_to") {
//	    db.CreateEdge("nodeA", "nodeB", "relates_to")
//	    table.RecordMaterialization("nodeA", "nodeB", "relates_to")
//	}
//
// Usage Example 2: With custom cooldown durations
//
//	customCooldowns := map[string]time.Duration{
//	    "important_link": 30 * time.Minute,  // Longer cooldown for important edges
//	    "casual_link":    1 * time.Minute,    // Shorter cooldown for casual edges
//	}
//	table := NewCooldownTableWithConfig(customCooldowns)
//
// Usage Example 3: Check with reason (for debugging)
//
//	canMat, reason := table.CanMaterializeWithReason("nodeA", "nodeB", "relates_to")
//	if !canMat {
//	    log.Printf("Cannot materialize: %s", reason)  // "cooldown active, 3m2s remaining"
//	}
//
// ELI12 (Explain Like I'm 12):
//
// Imagine you're playing tag at recess. After you tag someone:
//   - Cooldown prevents you from immediately tagging them again (no "tag-backs")
//   - You must wait 30 seconds before you can tag that same person
//   - This prevents annoying rapid-fire tagging (echo chamber)
//
// In NornicDB:
//   - You suggest edge A→B based on co-access
//   - Edge gets created
//   - 5 seconds later, they're accessed together again → same suggestion!
//   - Cooldown says "no, wait 5 minutes before suggesting A→B again"
//   - This prevents creating duplicate edges or flip-flopping
//
// Without cooldown, rapid co-access could create hundreds of duplicate suggestions!
package inference

import (
	"fmt"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
)

// DefaultCooldowns defines standard cooldown durations per edge label.
// These can be overridden per-table or globally.
var DefaultCooldowns = map[string]time.Duration{
	"relates_to":    5 * time.Minute,
	"similar_to":    10 * time.Minute,
	"coaccess":      1 * time.Minute,
	"topology":      15 * time.Minute, // For TLP-generated edges
	"depends_on":    30 * time.Minute,
	"references":    5 * time.Minute,
	"semantic_link": 10 * time.Minute,
}

// DefaultCooldown is used when no label-specific cooldown is configured.
const DefaultCooldown = 5 * time.Minute

// CooldownEntry tracks the last materialization time for an edge pair.
type CooldownEntry struct {
	LastMaterialized time.Time
	Count            int64 // Total materializations for this pair
}

// CooldownTable tracks when edges were last materialized to prevent spam.
// Thread-safe for concurrent access.
type CooldownTable struct {
	mu       sync.RWMutex
	entries  map[string]*CooldownEntry // key: "src:dst:label"
	defaults map[string]time.Duration  // per-label defaults

	// Stats
	totalChecks    int64
	totalBlocked   int64
	totalAllowed   int64
}

// CooldownStats provides observability into cooldown behavior.
type CooldownStats struct {
	TotalEntries int64
	TotalChecks  int64
	TotalBlocked int64
	TotalAllowed int64
	BlockRate    float64 // Blocked / Checks (0.0 - 1.0)
}

// NewCooldownTable creates a new cooldown table with default settings.
func NewCooldownTable() *CooldownTable {
	return &CooldownTable{
		entries:  make(map[string]*CooldownEntry),
		defaults: copyDefaults(DefaultCooldowns),
	}
}

// NewCooldownTableWithConfig creates a cooldown table with custom defaults.
func NewCooldownTableWithConfig(labelCooldowns map[string]time.Duration) *CooldownTable {
	ct := NewCooldownTable()
	for label, duration := range labelCooldowns {
		ct.defaults[label] = duration
	}
	return ct
}

// cooldownKey creates a consistent key for an edge pair.
func cooldownKey(src, dst, label string) string {
	return fmt.Sprintf("%s:%s:%s", src, dst, label)
}

// CanMaterialize checks if an edge can be materialized without violating cooldown.
// Returns true if cooldown period has passed or if cooldown feature is disabled.
func (ct *CooldownTable) CanMaterialize(src, dst, label string) bool {
	// Feature flag check - if disabled, always allow
	if !config.IsCooldownEnabled() {
		return true
	}

	ct.mu.RLock()
	defer ct.mu.RUnlock()

	ct.totalChecks++

	key := cooldownKey(src, dst, label)
	entry, exists := ct.entries[key]
	if !exists {
		ct.totalAllowed++
		return true
	}

	cooldown := ct.getCooldownDuration(label)
	elapsed := time.Since(entry.LastMaterialized)

	if elapsed >= cooldown {
		ct.totalAllowed++
		return true
	}

	ct.totalBlocked++
	return false
}

// CanMaterializeWithReason returns whether materialization is allowed and the reason.
// Useful for debugging and audit logs.
func (ct *CooldownTable) CanMaterializeWithReason(src, dst, label string) (bool, string) {
	if !config.IsCooldownEnabled() {
		return true, "cooldown feature disabled"
	}

	ct.mu.RLock()
	defer ct.mu.RUnlock()

	key := cooldownKey(src, dst, label)
	entry, exists := ct.entries[key]
	if !exists {
		return true, "first materialization for this pair"
	}

	cooldown := ct.getCooldownDuration(label)
	elapsed := time.Since(entry.LastMaterialized)

	if elapsed >= cooldown {
		return true, fmt.Sprintf("cooldown expired (elapsed: %s, required: %s)", elapsed, cooldown)
	}

	remaining := cooldown - elapsed
	return false, fmt.Sprintf("cooldown active (remaining: %s, last: %s ago)",
		remaining.Round(time.Second), elapsed.Round(time.Second))
}

// RecordMaterialization records that an edge was materialized.
// Should be called after successfully creating an edge.
func (ct *CooldownTable) RecordMaterialization(src, dst, label string) {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	key := cooldownKey(src, dst, label)
	entry, exists := ct.entries[key]
	if !exists {
		entry = &CooldownEntry{}
		ct.entries[key] = entry
	}

	entry.LastMaterialized = time.Now()
	entry.Count++
}

// RecordMaterializationAt records a materialization at a specific time.
// Useful for replaying events or testing.
func (ct *CooldownTable) RecordMaterializationAt(src, dst, label string, t time.Time) {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	key := cooldownKey(src, dst, label)
	entry, exists := ct.entries[key]
	if !exists {
		entry = &CooldownEntry{}
		ct.entries[key] = entry
	}

	entry.LastMaterialized = t
	entry.Count++
}

// getCooldownDuration returns the cooldown duration for a label.
// Must be called with at least a read lock held.
func (ct *CooldownTable) getCooldownDuration(label string) time.Duration {
	if duration, ok := ct.defaults[label]; ok {
		return duration
	}
	return DefaultCooldown
}

// SetLabelCooldown sets the cooldown duration for a specific label.
func (ct *CooldownTable) SetLabelCooldown(label string, duration time.Duration) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.defaults[label] = duration
}

// GetLabelCooldown returns the cooldown duration for a label.
func (ct *CooldownTable) GetLabelCooldown(label string) time.Duration {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	return ct.getCooldownDuration(label)
}

// TimeUntilAllowed returns how long until a materialization will be allowed.
// Returns 0 if already allowed.
func (ct *CooldownTable) TimeUntilAllowed(src, dst, label string) time.Duration {
	if !config.IsCooldownEnabled() {
		return 0
	}

	ct.mu.RLock()
	defer ct.mu.RUnlock()

	key := cooldownKey(src, dst, label)
	entry, exists := ct.entries[key]
	if !exists {
		return 0
	}

	cooldown := ct.getCooldownDuration(label)
	elapsed := time.Since(entry.LastMaterialized)

	if elapsed >= cooldown {
		return 0
	}

	return cooldown - elapsed
}

// GetEntry returns the cooldown entry for an edge pair.
// Returns nil if no entry exists.
func (ct *CooldownTable) GetEntry(src, dst, label string) *CooldownEntry {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	key := cooldownKey(src, dst, label)
	entry, exists := ct.entries[key]
	if !exists {
		return nil
	}

	// Return a copy to prevent mutation
	return &CooldownEntry{
		LastMaterialized: entry.LastMaterialized,
		Count:            entry.Count,
	}
}

// Clear removes all cooldown entries.
// Useful for testing or resetting state.
func (ct *CooldownTable) Clear() {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.entries = make(map[string]*CooldownEntry)
	ct.totalChecks = 0
	ct.totalBlocked = 0
	ct.totalAllowed = 0
}

// Cleanup removes expired entries to prevent memory growth.
// Should be called periodically (e.g., every 10 minutes).
func (ct *CooldownTable) Cleanup() int {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	removed := 0
	now := time.Now()

	for key, entry := range ct.entries {
		// Extract label from key (format: "src:dst:label")
		// Find the last colon to get the label
		lastColon := len(key) - 1
		for lastColon >= 0 && key[lastColon] != ':' {
			lastColon--
		}
		label := ""
		if lastColon >= 0 && lastColon < len(key)-1 {
			label = key[lastColon+1:]
		}

		cooldown := ct.getCooldownDuration(label)
		// Remove entries that have been expired for longer than the cooldown period
		// This gives a 2x grace period before cleanup
		if now.Sub(entry.LastMaterialized) > cooldown*2 {
			delete(ct.entries, key)
			removed++
		}
	}

	return removed
}

// Stats returns current cooldown table statistics.
func (ct *CooldownTable) Stats() CooldownStats {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	stats := CooldownStats{
		TotalEntries: int64(len(ct.entries)),
		TotalChecks:  ct.totalChecks,
		TotalBlocked: ct.totalBlocked,
		TotalAllowed: ct.totalAllowed,
	}

	if stats.TotalChecks > 0 {
		stats.BlockRate = float64(stats.TotalBlocked) / float64(stats.TotalChecks)
	}

	return stats
}

// Size returns the number of tracked edge pairs.
func (ct *CooldownTable) Size() int {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	return len(ct.entries)
}

// copyDefaults creates a copy of the default cooldowns map.
func copyDefaults(defaults map[string]time.Duration) map[string]time.Duration {
	copy := make(map[string]time.Duration, len(defaults))
	for k, v := range defaults {
		copy[k] = v
	}
	return copy
}

// CooldownTableOption configures a CooldownTable.
type CooldownTableOption func(*CooldownTable)

// WithLabelCooldown sets a specific label cooldown during initialization.
func WithLabelCooldown(label string, duration time.Duration) CooldownTableOption {
	return func(ct *CooldownTable) {
		ct.defaults[label] = duration
	}
}

// WithDefaultCooldown sets the fallback cooldown for unknown labels.
func WithDefaultCooldown(duration time.Duration) CooldownTableOption {
	return func(ct *CooldownTable) {
		// Store as empty string key for default lookup
		ct.defaults[""] = duration
	}
}

// NewCooldownTableWithOptions creates a table with functional options.
func NewCooldownTableWithOptions(opts ...CooldownTableOption) *CooldownTable {
	ct := NewCooldownTable()
	for _, opt := range opts {
		opt(ct)
	}
	return ct
}

// Global singleton for convenience
var globalCooldownTable *CooldownTable
var globalCooldownOnce sync.Once

// GlobalCooldownTable returns the global cooldown table singleton.
// Lazily initialized on first call.
func GlobalCooldownTable() *CooldownTable {
	globalCooldownOnce.Do(func() {
		globalCooldownTable = NewCooldownTable()
	})
	return globalCooldownTable
}

// ResetGlobalCooldownTable resets the global cooldown table.
// Primarily for testing.
func ResetGlobalCooldownTable() {
	globalCooldownOnce = sync.Once{}
	globalCooldownTable = nil
}

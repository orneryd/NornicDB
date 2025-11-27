package inference

import (
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCooldownTable(t *testing.T) {
	ct := NewCooldownTable()
	require.NotNil(t, ct)
	assert.Empty(t, ct.entries)
	assert.NotEmpty(t, ct.defaults)
	assert.Equal(t, 5*time.Minute, ct.defaults["relates_to"])
}

func TestCooldownTable_CanMaterialize_FeatureDisabled(t *testing.T) {
	// Ensure feature is disabled
	cleanup := config.WithCooldownDisabled()
	defer cleanup()

	ct := NewCooldownTable()

	// Record a materialization
	ct.RecordMaterialization("a", "b", "relates_to")

	// Should still allow even immediately after
	assert.True(t, ct.CanMaterialize("a", "b", "relates_to"))
}

func TestCooldownTable_CanMaterialize_FeatureEnabled(t *testing.T) {
	// Enable cooldown feature
	cleanup := config.WithCooldownEnabled()
	defer cleanup()

	ct := NewCooldownTable()

	// First materialization should always be allowed
	assert.True(t, ct.CanMaterialize("a", "b", "relates_to"))

	// Record the materialization
	ct.RecordMaterialization("a", "b", "relates_to")

	// Immediately after should be blocked
	assert.False(t, ct.CanMaterialize("a", "b", "relates_to"))

	// Different pair should be allowed
	assert.True(t, ct.CanMaterialize("a", "c", "relates_to"))
	assert.True(t, ct.CanMaterialize("x", "y", "relates_to"))
}

func TestCooldownTable_CanMaterialize_AfterCooldown(t *testing.T) {
	cleanup := config.WithCooldownEnabled()
	defer cleanup()

	ct := NewCooldownTable()
	// Set a very short cooldown for testing
	ct.SetLabelCooldown("test_label", 10*time.Millisecond)

	// Record materialization
	ct.RecordMaterialization("a", "b", "test_label")

	// Should be blocked immediately
	assert.False(t, ct.CanMaterialize("a", "b", "test_label"))

	// Wait for cooldown to expire
	time.Sleep(15 * time.Millisecond)

	// Now should be allowed
	assert.True(t, ct.CanMaterialize("a", "b", "test_label"))
}

func TestCooldownTable_CanMaterializeWithReason(t *testing.T) {
	cleanup := config.WithCooldownEnabled()
	defer cleanup()

	ct := NewCooldownTable()
	ct.SetLabelCooldown("test_label", 1*time.Second)

	// First check - should be allowed
	allowed, reason := ct.CanMaterializeWithReason("a", "b", "test_label")
	assert.True(t, allowed)
	assert.Contains(t, reason, "first materialization")

	// Record and check again
	ct.RecordMaterialization("a", "b", "test_label")
	allowed, reason = ct.CanMaterializeWithReason("a", "b", "test_label")
	assert.False(t, allowed)
	assert.Contains(t, reason, "cooldown active")
}

func TestCooldownTable_RecordMaterializationAt(t *testing.T) {
	cleanup := config.WithCooldownEnabled()
	defer cleanup()

	ct := NewCooldownTable()
	ct.SetLabelCooldown("test_label", 1*time.Hour)

	// Record at a time in the past (2 hours ago)
	pastTime := time.Now().Add(-2 * time.Hour)
	ct.RecordMaterializationAt("a", "b", "test_label", pastTime)

	// Should be allowed because cooldown has expired
	assert.True(t, ct.CanMaterialize("a", "b", "test_label"))

	// Record at current time
	ct.RecordMaterialization("a", "b", "test_label")

	// Now should be blocked
	assert.False(t, ct.CanMaterialize("a", "b", "test_label"))
}

func TestCooldownTable_TimeUntilAllowed(t *testing.T) {
	cleanup := config.WithCooldownEnabled()
	defer cleanup()

	ct := NewCooldownTable()
	ct.SetLabelCooldown("test_label", 1*time.Second)

	// No entry - should return 0
	assert.Equal(t, time.Duration(0), ct.TimeUntilAllowed("a", "b", "test_label"))

	// Record and check
	ct.RecordMaterialization("a", "b", "test_label")
	remaining := ct.TimeUntilAllowed("a", "b", "test_label")
	assert.True(t, remaining > 0 && remaining <= 1*time.Second)
}

func TestCooldownTable_GetEntry(t *testing.T) {
	ct := NewCooldownTable()

	// No entry
	assert.Nil(t, ct.GetEntry("a", "b", "test"))

	// After recording
	ct.RecordMaterialization("a", "b", "test")
	entry := ct.GetEntry("a", "b", "test")
	require.NotNil(t, entry)
	assert.Equal(t, int64(1), entry.Count)
	assert.False(t, entry.LastMaterialized.IsZero())

	// Record again
	ct.RecordMaterialization("a", "b", "test")
	entry = ct.GetEntry("a", "b", "test")
	assert.Equal(t, int64(2), entry.Count)
}

func TestCooldownTable_Clear(t *testing.T) {
	ct := NewCooldownTable()
	ct.RecordMaterialization("a", "b", "test")
	ct.RecordMaterialization("c", "d", "test")

	assert.Equal(t, 2, ct.Size())

	ct.Clear()
	assert.Equal(t, 0, ct.Size())
}

func TestCooldownTable_Cleanup(t *testing.T) {
	ct := NewCooldownTable()
	ct.SetLabelCooldown("test_label", 10*time.Millisecond)

	// Record some entries
	ct.RecordMaterializationAt("a", "b", "test_label", time.Now().Add(-1*time.Hour))
	ct.RecordMaterializationAt("c", "d", "test_label", time.Now().Add(-1*time.Hour))
	ct.RecordMaterialization("e", "f", "test_label") // Recent, should not be cleaned

	assert.Equal(t, 3, ct.Size())

	// Cleanup should remove expired entries
	removed := ct.Cleanup()
	assert.Equal(t, 2, removed)
	assert.Equal(t, 1, ct.Size())
}

func TestCooldownTable_Stats(t *testing.T) {
	cleanup := config.WithCooldownEnabled()
	defer cleanup()

	ct := NewCooldownTable()
	ct.SetLabelCooldown("test", 100*time.Millisecond)

	// Initial stats
	stats := ct.Stats()
	assert.Equal(t, int64(0), stats.TotalEntries)
	assert.Equal(t, int64(0), stats.TotalChecks)

	// Make some checks
	ct.CanMaterialize("a", "b", "test") // Allowed (first)
	ct.RecordMaterialization("a", "b", "test")
	ct.CanMaterialize("a", "b", "test") // Blocked

	stats = ct.Stats()
	assert.Equal(t, int64(1), stats.TotalEntries)
	assert.Equal(t, int64(2), stats.TotalChecks)
	assert.Equal(t, int64(1), stats.TotalAllowed)
	assert.Equal(t, int64(1), stats.TotalBlocked)
	assert.Equal(t, 0.5, stats.BlockRate)
}

func TestCooldownTable_DifferentLabels(t *testing.T) {
	cleanup := config.WithCooldownEnabled()
	defer cleanup()

	ct := NewCooldownTable()
	ct.SetLabelCooldown("fast", 10*time.Millisecond)
	ct.SetLabelCooldown("slow", 1*time.Hour)

	// Record same pair with different labels
	ct.RecordMaterialization("a", "b", "fast")
	ct.RecordMaterialization("a", "b", "slow")

	// Wait for fast cooldown to expire
	time.Sleep(15 * time.Millisecond)

	// Fast label should be allowed, slow should not
	assert.True(t, ct.CanMaterialize("a", "b", "fast"))
	assert.False(t, ct.CanMaterialize("a", "b", "slow"))
}

func TestCooldownTable_Concurrent(t *testing.T) {
	cleanup := config.WithCooldownEnabled()
	defer cleanup()

	ct := NewCooldownTable()
	ct.SetLabelCooldown("test", 100*time.Millisecond)

	var wg sync.WaitGroup
	iterations := 100

	// Concurrent reads
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ct.CanMaterialize("a", "b", "test")
		}()
	}

	// Concurrent writes
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ct.RecordMaterialization("a", "b", "test")
		}()
	}

	wg.Wait()

	// Should not panic and entry should exist
	entry := ct.GetEntry("a", "b", "test")
	require.NotNil(t, entry)
	assert.True(t, entry.Count > 0)
}

func TestCooldownTableWithOptions(t *testing.T) {
	ct := NewCooldownTableWithOptions(
		WithLabelCooldown("custom", 30*time.Second),
		WithLabelCooldown("another", 1*time.Minute),
	)

	assert.Equal(t, 30*time.Second, ct.GetLabelCooldown("custom"))
	assert.Equal(t, 1*time.Minute, ct.GetLabelCooldown("another"))
	// Default should still work
	assert.Equal(t, 5*time.Minute, ct.GetLabelCooldown("relates_to"))
}

func TestGlobalCooldownTable(t *testing.T) {
	ResetGlobalCooldownTable()

	ct1 := GlobalCooldownTable()
	ct2 := GlobalCooldownTable()

	// Should be same instance
	assert.Same(t, ct1, ct2)

	// Reset and get new instance
	ResetGlobalCooldownTable()
	ct3 := GlobalCooldownTable()
	assert.NotSame(t, ct1, ct3)
}

func TestCooldownTable_DefaultCooldowns(t *testing.T) {
	ct := NewCooldownTable()

	// Verify all default cooldowns are set
	assert.Equal(t, 5*time.Minute, ct.GetLabelCooldown("relates_to"))
	assert.Equal(t, 10*time.Minute, ct.GetLabelCooldown("similar_to"))
	assert.Equal(t, 1*time.Minute, ct.GetLabelCooldown("coaccess"))
	assert.Equal(t, 15*time.Minute, ct.GetLabelCooldown("topology"))
	assert.Equal(t, 30*time.Minute, ct.GetLabelCooldown("depends_on"))

	// Unknown label should get default
	assert.Equal(t, DefaultCooldown, ct.GetLabelCooldown("unknown_label"))
}

func TestCooldownTable_FeatureDisabledTimeUntilAllowed(t *testing.T) {
	cleanup := config.WithCooldownDisabled()
	defer cleanup()

	ct := NewCooldownTable()
	ct.RecordMaterialization("a", "b", "test")

	// Should return 0 when feature is disabled
	assert.Equal(t, time.Duration(0), ct.TimeUntilAllowed("a", "b", "test"))
}

func TestCooldownTable_FeatureDisabledReason(t *testing.T) {
	cleanup := config.WithCooldownDisabled()
	defer cleanup()

	ct := NewCooldownTable()
	ct.RecordMaterialization("a", "b", "test")

	allowed, reason := ct.CanMaterializeWithReason("a", "b", "test")
	assert.True(t, allowed)
	assert.Contains(t, reason, "feature disabled")
}

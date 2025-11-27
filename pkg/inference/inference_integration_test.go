package inference

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessSuggestion_WithAutoIntegration(t *testing.T) {
	// Enable both auto-integrations
	cleanupCooldown := config.WithCooldownAutoIntegrationEnabled()
	defer cleanupCooldown()
	cleanupEvidence := config.WithEvidenceAutoIntegrationEnabled()
	defer cleanupEvidence()

	engine := New(nil)
	require.NotNil(t, engine.cooldownTable)
	require.NotNil(t, engine.evidenceBuffer)

	// Configure thresholds for testing
	engine.GetEvidenceBuffer().SetThreshold("RELATES_TO", EvidenceThreshold{
		MinCount:    3,
		MinScore:    0.5,
		MinSessions: 2,
		MaxAge:      1 * time.Hour,
	})

	suggestion := EdgeSuggestion{
		SourceID:   "node-A",
		TargetID:   "node-B",
		Type:       "RELATES_TO",
		Confidence: 0.8,
		Method:     "similarity",
	}

	// First suggestion - evidence not met yet
	result := engine.ProcessSuggestion(suggestion, "session-1")
	assert.False(t, result.ShouldMaterialize, "Should not materialize on first evidence")
	assert.True(t, result.EvidencePending, "Should be pending evidence")
	assert.Contains(t, result.Reason, "evidence")

	// Second suggestion - different session
	result = engine.ProcessSuggestion(suggestion, "session-2")
	assert.False(t, result.ShouldMaterialize)

	// Third suggestion - should now materialize
	result = engine.ProcessSuggestion(suggestion, "session-2")
	assert.True(t, result.ShouldMaterialize, "Should materialize after meeting threshold")
	assert.Equal(t, "passed all checks", result.Reason)

	// Record the materialization
	engine.RecordMaterialization(suggestion.SourceID, suggestion.TargetID, suggestion.Type)

	// Try again immediately - should be blocked by cooldown
	result = engine.ProcessSuggestion(suggestion, "session-3")
	assert.False(t, result.ShouldMaterialize, "Should be blocked by cooldown")
	assert.True(t, result.CooldownBlocked, "Should report cooldown blocked")
	assert.Contains(t, result.Reason, "cooldown")
}

func TestProcessSuggestion_CooldownDisabled(t *testing.T) {
	// Enable evidence, disable cooldown
	cleanupCooldown := config.WithCooldownAutoIntegrationDisabled()
	defer cleanupCooldown()
	cleanupEvidence := config.WithEvidenceAutoIntegrationEnabled()
	defer cleanupEvidence()

	engine := New(nil)

	// Set very easy evidence threshold
	engine.GetEvidenceBuffer().SetThreshold("RELATES_TO", EvidenceThreshold{
		MinCount:    1,
		MinScore:    0.1,
		MinSessions: 1,
		MaxAge:      1 * time.Hour,
	})

	suggestion := EdgeSuggestion{
		SourceID:   "node-A",
		TargetID:   "node-B",
		Type:       "RELATES_TO",
		Confidence: 0.8,
		Method:     "similarity",
	}

	// Should materialize immediately (evidence threshold low, cooldown disabled)
	result := engine.ProcessSuggestion(suggestion, "session-1")
	assert.True(t, result.ShouldMaterialize)

	// Record and try again - should still be allowed (cooldown disabled)
	engine.RecordMaterialization(suggestion.SourceID, suggestion.TargetID, suggestion.Type)
	result = engine.ProcessSuggestion(suggestion, "session-2")
	assert.True(t, result.ShouldMaterialize, "Should not be blocked when cooldown disabled")
}

func TestProcessSuggestion_EvidenceDisabled(t *testing.T) {
	// Enable cooldown, disable evidence
	cleanupCooldown := config.WithCooldownAutoIntegrationEnabled()
	defer cleanupCooldown()
	cleanupEvidence := config.WithEvidenceAutoIntegrationDisabled()
	defer cleanupEvidence()

	engine := New(nil)

	// Set short cooldown for testing
	engine.GetCooldownTable().SetLabelCooldown("RELATES_TO", 10*time.Millisecond)

	suggestion := EdgeSuggestion{
		SourceID:   "node-A",
		TargetID:   "node-B",
		Type:       "RELATES_TO",
		Confidence: 0.8,
		Method:     "similarity",
	}

	// Should materialize immediately (evidence disabled)
	result := engine.ProcessSuggestion(suggestion, "session-1")
	assert.True(t, result.ShouldMaterialize)

	// Record and try again - should be blocked by cooldown
	engine.RecordMaterialization(suggestion.SourceID, suggestion.TargetID, suggestion.Type)
	result = engine.ProcessSuggestion(suggestion, "session-2")
	assert.False(t, result.ShouldMaterialize)
	assert.True(t, result.CooldownBlocked)

	// Wait for cooldown to expire
	time.Sleep(15 * time.Millisecond)
	result = engine.ProcessSuggestion(suggestion, "session-3")
	assert.True(t, result.ShouldMaterialize, "Should be allowed after cooldown expires")
}

func TestProcessSuggestion_BothDisabled(t *testing.T) {
	// Disable both
	cleanupCooldown := config.WithCooldownAutoIntegrationDisabled()
	defer cleanupCooldown()
	cleanupEvidence := config.WithEvidenceAutoIntegrationDisabled()
	defer cleanupEvidence()

	engine := New(nil)

	suggestion := EdgeSuggestion{
		SourceID:   "node-A",
		TargetID:   "node-B",
		Type:       "RELATES_TO",
		Confidence: 0.8,
		Method:     "similarity",
	}

	// Should always materialize when both disabled
	for i := 0; i < 10; i++ {
		result := engine.ProcessSuggestion(suggestion, "session-1")
		assert.True(t, result.ShouldMaterialize, "Should always materialize when both disabled")
		engine.RecordMaterialization(suggestion.SourceID, suggestion.TargetID, suggestion.Type)
	}
}

func TestCleanupTier1(t *testing.T) {
	engine := New(nil)

	// Add some data that will expire
	engine.cooldownTable.SetLabelCooldown("test", 1*time.Millisecond)
	engine.cooldownTable.RecordMaterialization("a", "b", "test")
	engine.cooldownTable.RecordMaterialization("c", "d", "test")

	engine.evidenceBuffer.SetThreshold("test", EvidenceThreshold{
		MinCount:    1,
		MinScore:    0.1,
		MinSessions: 1,
		MaxAge:      1 * time.Millisecond,
	})
	engine.evidenceBuffer.AddEvidence("a", "b", "test", 0.5, "similarity", "s1")
	engine.evidenceBuffer.AddEvidence("c", "d", "test", 0.5, "similarity", "s1")

	// Wait for expiry
	time.Sleep(5 * time.Millisecond)

	// Run cleanup
	cooldownRemoved, evidenceRemoved := engine.CleanupTier1()
	assert.Equal(t, 2, cooldownRemoved, "Should remove expired cooldown entries")
	assert.Equal(t, 2, evidenceRemoved, "Should remove expired evidence entries")
}

func TestEngine_GetSetCooldownTable(t *testing.T) {
	engine := New(nil)
	original := engine.GetCooldownTable()
	require.NotNil(t, original)

	// Set custom table
	custom := NewCooldownTableWithOptions(
		WithLabelCooldown("custom", 1*time.Hour),
	)
	engine.SetCooldownTable(custom)
	assert.Same(t, custom, engine.GetCooldownTable())
}

func TestEngine_GetSetEvidenceBuffer(t *testing.T) {
	engine := New(nil)
	original := engine.GetEvidenceBuffer()
	require.NotNil(t, original)

	// Set custom buffer
	custom := NewEvidenceBufferWithOptions(
		WithThreshold("custom", EvidenceThreshold{MinCount: 10}),
	)
	engine.SetEvidenceBuffer(custom)
	assert.Same(t, custom, engine.GetEvidenceBuffer())
}

func TestProcessSuggestion_NodeConfigBlocking(t *testing.T) {
	// Enable per-node config auto-integration
	cleanupNodeConfig := config.WithPerNodeConfigAutoIntegrationEnabled()
	defer cleanupNodeConfig()
	// Disable other auto-integrations to isolate this test
	cleanupCooldown := config.WithCooldownAutoIntegrationDisabled()
	defer cleanupCooldown()
	cleanupEvidence := config.WithEvidenceAutoIntegrationDisabled()
	defer cleanupEvidence()

	engine := New(nil)
	require.NotNil(t, engine.nodeConfigStore)

	suggestion := EdgeSuggestion{
		SourceID:   "node-A",
		TargetID:   "node-B",
		Type:       "RELATES_TO",
		Confidence: 0.8,
		Method:     "similarity",
	}

	// Initially should be allowed
	result := engine.ProcessSuggestion(suggestion, "session-1")
	assert.True(t, result.ShouldMaterialize, "Should materialize without restrictions")

	// Add target to deny list
	engine.nodeConfigStore.AddToNodeDenyList("node-A", "node-B")

	// Now should be blocked
	result = engine.ProcessSuggestion(suggestion, "session-1")
	assert.False(t, result.ShouldMaterialize, "Should NOT materialize - in deny list")
	assert.True(t, result.NodeConfigBlocked, "Should be blocked by node config")
	assert.Contains(t, result.Reason, "node-config")

	// Clear and test disabled node
	engine.nodeConfigStore.Clear()
	engine.nodeConfigStore.DisableNode("node-A")

	result = engine.ProcessSuggestion(suggestion, "session-1")
	assert.False(t, result.ShouldMaterialize, "Should NOT materialize - source disabled")
	assert.True(t, result.NodeConfigBlocked)

	// Clear and test edge caps
	engine.nodeConfigStore.Clear()
	engine.nodeConfigStore.SetNodeEdgeCaps("node-A", 1, 0, 0) // max 1 out
	engine.nodeConfigStore.RecordEdgeCreation("node-A", "node-X")

	result = engine.ProcessSuggestion(suggestion, "session-1")
	assert.False(t, result.ShouldMaterialize, "Should NOT materialize - at out cap")
	assert.True(t, result.NodeConfigBlocked)
}

func TestEngine_GetSetNodeConfigStore(t *testing.T) {
	engine := New(nil)
	original := engine.GetNodeConfigStore()
	require.NotNil(t, original)

	// Set custom store
	custom := storage.NewNodeConfigStore()
	custom.DisableNode("blocked")
	engine.SetNodeConfigStore(custom)
	assert.Same(t, custom, engine.GetNodeConfigStore())

	// Verify the custom store is used
	cfg := engine.GetNodeConfigStore().Get("blocked")
	assert.True(t, cfg.Disabled)
}

func TestRecordMaterialization_UpdatesNodeConfig(t *testing.T) {
	// Enable per-node config auto-integration
	cleanupNodeConfig := config.WithPerNodeConfigAutoIntegrationEnabled()
	defer cleanupNodeConfig()

	engine := New(nil)

	// Pre-create node configs
	engine.nodeConfigStore.GetOrCreate("src")
	engine.nodeConfigStore.GetOrCreate("tgt")

	// Record materialization
	engine.RecordMaterialization("src", "tgt", "RELATES_TO")

	// Verify edge counts updated
	srcCfg := engine.nodeConfigStore.Get("src")
	assert.Equal(t, 1, srcCfg.CurrentOutEdges, "Source out edges should be incremented")

	tgtCfg := engine.nodeConfigStore.Get("tgt")
	assert.Equal(t, 1, tgtCfg.CurrentInEdges, "Target in edges should be incremented")
}

package inference

import (
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEvidenceBuffer(t *testing.T) {
	eb := NewEvidenceBuffer()
	require.NotNil(t, eb)
	assert.Empty(t, eb.entries)
	assert.NotEmpty(t, eb.thresholds)
}

func TestEvidenceBuffer_FeatureDisabled(t *testing.T) {
	// Disable feature
	cleanup := config.WithEvidenceBufferingDisabled()
	defer cleanup()

	eb := NewEvidenceBuffer()

	// Single evidence should immediately return true (materialize)
	result := eb.AddEvidence("a", "b", "relates_to", 0.5, "coaccess", "session-1")
	assert.True(t, result, "Should materialize immediately when feature disabled")
}

func TestEvidenceBuffer_RequiresThreshold(t *testing.T) {
	// Enable feature
	cleanup := config.WithEvidenceBufferingEnabled()
	defer cleanup()

	eb := NewEvidenceBuffer()
	// Use custom low threshold for testing
	eb.SetThreshold("test_label", EvidenceThreshold{
		MinCount:    3,
		MinScore:    0.5,
		MinSessions: 2,
		MaxAge:      1 * time.Hour,
	})

	// First evidence - should not materialize
	result := eb.AddEvidence("a", "b", "test_label", 0.6, "coaccess", "session-1")
	assert.False(t, result, "Should not materialize with only 1 evidence")

	// Second evidence - different session, still not enough
	result = eb.AddEvidence("a", "b", "test_label", 0.6, "similarity", "session-2")
	assert.False(t, result, "Should not materialize with only 2 evidence")

	// Third evidence - now meets count and session threshold
	result = eb.AddEvidence("a", "b", "test_label", 0.6, "topology", "session-2")
	assert.True(t, result, "Should materialize after meeting threshold")
}

func TestEvidenceBuffer_LowScoreDoesNotMaterialize(t *testing.T) {
	cleanup := config.WithEvidenceBufferingEnabled()
	defer cleanup()

	eb := NewEvidenceBuffer()
	eb.SetThreshold("test_label", EvidenceThreshold{
		MinCount:    2,
		MinScore:    0.7, // High threshold
		MinSessions: 1,
		MaxAge:      1 * time.Hour,
	})

	// Add enough count but low scores
	eb.AddEvidence("a", "b", "test_label", 0.3, "coaccess", "session-1")
	result := eb.AddEvidence("a", "b", "test_label", 0.3, "similarity", "session-1")

	assert.False(t, result, "Should not materialize with low average score")

	// Check evidence
	ev := eb.GetEvidence("a", "b", "test_label")
	assert.Equal(t, 2, ev.Count)
	assert.Equal(t, 0.3, ev.ScoreAvg) // Average of 0.3 and 0.3
}

func TestEvidenceBuffer_CheckThreshold(t *testing.T) {
	cleanup := config.WithEvidenceBufferingEnabled()
	defer cleanup()

	eb := NewEvidenceBuffer()
	eb.SetThreshold("test_label", EvidenceThreshold{
		MinCount:    2,
		MinScore:    0.5,
		MinSessions: 1,
		MaxAge:      1 * time.Hour,
	})

	// No evidence
	met, reason := eb.CheckThreshold("a", "b", "test_label")
	assert.False(t, met)
	assert.Contains(t, reason, "no evidence")

	// Add some evidence
	eb.AddEvidence("a", "b", "test_label", 0.6, "coaccess", "session-1")

	met, reason = eb.CheckThreshold("a", "b", "test_label")
	assert.False(t, met)
	assert.Contains(t, reason, "insufficient count")

	// Add enough evidence
	eb.AddEvidence("a", "b", "test_label", 0.6, "similarity", "session-1")

	met, reason = eb.CheckThreshold("a", "b", "test_label")
	assert.True(t, met)
	assert.Contains(t, reason, "threshold met")
}

func TestEvidenceBuffer_ExpiredEvidence(t *testing.T) {
	cleanup := config.WithEvidenceBufferingEnabled()
	defer cleanup()

	eb := NewEvidenceBuffer()
	eb.SetThreshold("test_label", EvidenceThreshold{
		MinCount:    2,
		MinScore:    0.5,
		MinSessions: 1,
		MaxAge:      1 * time.Millisecond, // Very short for testing
	})

	// Add evidence
	eb.AddEvidence("a", "b", "test_label", 0.6, "coaccess", "session-1")

	// Wait for expiry
	time.Sleep(5 * time.Millisecond)

	// Add second evidence - should not materialize because first is expired
	result := eb.AddEvidence("a", "b", "test_label", 0.6, "similarity", "session-1")
	assert.False(t, result, "Should not materialize with expired evidence")
}

func TestEvidenceBuffer_GetEvidence(t *testing.T) {
	eb := NewEvidenceBuffer()

	// No evidence
	ev := eb.GetEvidence("a", "b", "test")
	assert.Nil(t, ev)

	// Add evidence
	eb.AddEvidence("a", "b", "test", 0.7, "coaccess", "session-1")
	eb.AddEvidence("a", "b", "test", 0.8, "similarity", "session-2")

	ev = eb.GetEvidence("a", "b", "test")
	require.NotNil(t, ev)
	assert.Equal(t, 2, ev.Count)
	assert.Equal(t, 1.5, ev.ScoreSum)
	assert.Equal(t, 0.75, ev.ScoreAvg)
	assert.Len(t, ev.Sessions, 2)
	assert.Len(t, ev.Signals, 2)
}

func TestEvidenceBuffer_ClearEntry(t *testing.T) {
	eb := NewEvidenceBuffer()
	eb.AddEvidence("a", "b", "test", 0.5, "coaccess", "s1")
	eb.AddEvidence("c", "d", "test", 0.5, "coaccess", "s1")

	assert.Equal(t, 2, eb.Size())

	eb.ClearEntry("a", "b", "test")
	assert.Equal(t, 1, eb.Size())
	assert.Nil(t, eb.GetEvidence("a", "b", "test"))
	assert.NotNil(t, eb.GetEvidence("c", "d", "test"))
}

func TestEvidenceBuffer_Clear(t *testing.T) {
	eb := NewEvidenceBuffer()
	eb.AddEvidence("a", "b", "test", 0.5, "coaccess", "s1")
	eb.AddEvidence("c", "d", "test", 0.5, "coaccess", "s1")

	assert.Equal(t, 2, eb.Size())

	eb.Clear()
	assert.Equal(t, 0, eb.Size())
}

func TestEvidenceBuffer_Cleanup(t *testing.T) {
	eb := NewEvidenceBuffer()
	eb.SetThreshold("test", EvidenceThreshold{
		MinCount:    1,
		MinScore:    0.1,
		MinSessions: 1,
		MaxAge:      1 * time.Millisecond,
	})

	eb.AddEvidence("a", "b", "test", 0.5, "coaccess", "s1")
	eb.AddEvidence("c", "d", "test", 0.5, "coaccess", "s1")

	// Wait for expiry
	time.Sleep(5 * time.Millisecond)

	// Add fresh entry
	eb.SetThreshold("fresh", EvidenceThreshold{
		MinCount:    1,
		MinScore:    0.1,
		MinSessions: 1,
		MaxAge:      1 * time.Hour,
	})
	eb.AddEvidence("e", "f", "fresh", 0.5, "coaccess", "s1")

	assert.Equal(t, 3, eb.Size())

	removed := eb.Cleanup()
	assert.Equal(t, 2, removed)
	assert.Equal(t, 1, eb.Size())
}

func TestEvidenceBuffer_Stats(t *testing.T) {
	cleanup := config.WithEvidenceBufferingEnabled()
	defer cleanup()

	eb := NewEvidenceBuffer()
	eb.SetThreshold("test", EvidenceThreshold{
		MinCount:    2,
		MinScore:    0.5,
		MinSessions: 1,
		MaxAge:      1 * time.Hour,
	})

	// Initial stats
	stats := eb.Stats()
	assert.Equal(t, int64(0), stats.TotalEntries)
	assert.Equal(t, int64(0), stats.TotalAdded)

	// Add evidence
	eb.AddEvidence("a", "b", "test", 0.6, "coaccess", "s1")
	eb.AddEvidence("a", "b", "test", 0.6, "similarity", "s1") // This should materialize

	stats = eb.Stats()
	assert.Equal(t, int64(1), stats.TotalEntries)
	assert.Equal(t, int64(2), stats.TotalAdded)
	assert.Equal(t, int64(1), stats.TotalMaterialized)
	assert.Equal(t, 0.5, stats.MaterializeRate)
}

func TestEvidenceBuffer_Concurrent(t *testing.T) {
	cleanup := config.WithEvidenceBufferingEnabled()
	defer cleanup()

	eb := NewEvidenceBuffer()
	eb.SetThreshold("test", EvidenceThreshold{
		MinCount:    100,
		MinScore:    0.1,
		MinSessions: 1,
		MaxAge:      1 * time.Hour,
	})

	var wg sync.WaitGroup
	iterations := 50

	// Concurrent writes
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eb.AddEvidence("a", "b", "test", 0.5, "coaccess", "session-1")
		}(i)
	}

	// Concurrent reads
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			eb.GetEvidence("a", "b", "test")
		}()
	}

	wg.Wait()

	ev := eb.GetEvidence("a", "b", "test")
	require.NotNil(t, ev)
	assert.Equal(t, iterations, ev.Count)
}

func TestEvidenceBuffer_DifferentLabels(t *testing.T) {
	cleanup := config.WithEvidenceBufferingEnabled()
	defer cleanup()

	eb := NewEvidenceBuffer()
	eb.SetThreshold("easy", EvidenceThreshold{
		MinCount:    1,
		MinScore:    0.1,
		MinSessions: 1,
		MaxAge:      1 * time.Hour,
	})
	eb.SetThreshold("hard", EvidenceThreshold{
		MinCount:    10,
		MinScore:    0.9,
		MinSessions: 5,
		MaxAge:      1 * time.Hour,
	})

	// Same edge pair, different labels
	resultEasy := eb.AddEvidence("a", "b", "easy", 0.5, "coaccess", "s1")
	resultHard := eb.AddEvidence("a", "b", "hard", 0.5, "coaccess", "s1")

	assert.True(t, resultEasy, "Easy label should materialize")
	assert.False(t, resultHard, "Hard label should not materialize")
}

func TestEvidenceBuffer_WithMetadata(t *testing.T) {
	eb := NewEvidenceBuffer()

	metadata := map[string]interface{}{
		"source":    "api",
		"timestamp": time.Now().Unix(),
	}

	eb.AddEvidenceWithMetadata("a", "b", "test", 0.5, "coaccess", "s1", metadata)

	ev := eb.GetEvidence("a", "b", "test")
	require.NotNil(t, ev)
	assert.Equal(t, "api", ev.Metadata["source"])
}

func TestEvidenceBuffer_GetPendingEdges(t *testing.T) {
	cleanup := config.WithEvidenceBufferingEnabled()
	defer cleanup()

	eb := NewEvidenceBuffer()
	eb.SetThreshold("test", EvidenceThreshold{
		MinCount:    4,
		MinScore:    0.5,
		MinSessions: 2,
		MaxAge:      1 * time.Hour,
	})

	// Add partial evidence (50% progress roughly)
	eb.AddEvidence("a", "b", "test", 0.6, "coaccess", "s1")
	eb.AddEvidence("a", "b", "test", 0.6, "similarity", "s2")

	// Get pending edges with at least 50% progress
	pending := eb.GetPendingEdges(0.5)
	assert.Len(t, pending, 1)
	assert.Equal(t, "a", pending[0].Key.Src)
}

func TestGlobalEvidenceBuffer(t *testing.T) {
	ResetGlobalEvidenceBuffer()

	eb1 := GlobalEvidenceBuffer()
	eb2 := GlobalEvidenceBuffer()

	assert.Same(t, eb1, eb2)

	ResetGlobalEvidenceBuffer()
	eb3 := GlobalEvidenceBuffer()
	assert.NotSame(t, eb1, eb3)
}

func TestEvidenceBuffer_DefaultThresholds(t *testing.T) {
	eb := NewEvidenceBuffer()

	// Verify default thresholds are set
	threshold := eb.GetThreshold("relates_to")
	assert.Equal(t, 3, threshold.MinCount)
	assert.Equal(t, 0.5, threshold.MinScore)
	assert.Equal(t, 2, threshold.MinSessions)

	// Unknown label should get default threshold
	threshold = eb.GetThreshold("unknown_label")
	assert.Equal(t, DefaultEvidenceThreshold.MinCount, threshold.MinCount)
}

func TestEvidenceKey_String(t *testing.T) {
	key := EvidenceKey{Src: "node1", Dst: "node2", Label: "relates_to"}
	assert.Equal(t, "node1:node2:relates_to", key.String())
}

func TestEvidenceBufferWithOptions(t *testing.T) {
	eb := NewEvidenceBufferWithOptions(
		WithThreshold("custom", EvidenceThreshold{
			MinCount:    5,
			MinScore:    0.8,
			MinSessions: 3,
			MaxAge:      2 * time.Hour,
		}),
	)

	threshold := eb.GetThreshold("custom")
	assert.Equal(t, 5, threshold.MinCount)
	assert.Equal(t, 0.8, threshold.MinScore)
}

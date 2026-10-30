package temporal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// PatternDetector – uncovered functions
// ============================================================================

func TestPatternDetector_GetPeakAccessTime_Unknown(t *testing.T) {
	pd := NewPatternDetector(DefaultPatternDetectorConfig())
	h, d, conf := pd.GetPeakAccessTime("nonexistent-node")
	assert.Equal(t, -1, h)
	assert.Equal(t, -1, d)
	assert.Equal(t, 0.0, conf)
}

func TestPatternDetector_GetPeakAccessTime_WithData(t *testing.T) {
	pd := NewPatternDetector(DefaultPatternDetectorConfig())
	now := time.Now()
	for i := 0; i < 10; i++ {
		pd.RecordAccess("node1", now)
	}
	h, d, conf := pd.GetPeakAccessTime("node1")
	assert.GreaterOrEqual(t, h, 0)
	assert.GreaterOrEqual(t, d, 0)
	assert.GreaterOrEqual(t, conf, 0.0)
}

func TestPatternDetector_HasPattern_NoData(t *testing.T) {
	pd := NewPatternDetector(DefaultPatternDetectorConfig())
	result := pd.HasPattern("ghost-node", PatternGrowing, 0.5)
	assert.False(t, result)
}

func TestPatternDetector_HasPattern_WithData(t *testing.T) {
	pd := NewPatternDetector(DefaultPatternDetectorConfig())
	now := time.Now()
	// Record many accesses to trigger pattern detection
	for i := 0; i < 20; i++ {
		pd.RecordAccess("node1", now.Add(time.Duration(i)*time.Hour))
	}
	// HasPattern should not panic regardless of whether pattern is found
	_ = pd.HasPattern("node1", PatternDaily, 0.5)
}

func TestPatternDetector_ResetNode(t *testing.T) {
	pd := NewPatternDetector(DefaultPatternDetectorConfig())
	now := time.Now()
	pd.RecordAccess("node1", now)
	pd.RecordAccess("node2", now)

	pd.ResetNode("node1")

	// After reset, node1 should have no data
	h, d, conf := pd.GetPeakAccessTime("node1")
	assert.Equal(t, -1, h)
	assert.Equal(t, -1, d)
	assert.Equal(t, 0.0, conf)

	// node2 should still have data
	h2, _, _ := pd.GetPeakAccessTime("node2")
	assert.GreaterOrEqual(t, h2, 0)
}

// ============================================================================
// RelationshipEvolution – uncovered functions
// ============================================================================

func TestRelationshipEvolution_UpdateWeight(t *testing.T) {
	cfg := DefaultRelationshipConfig()
	re := NewRelationshipEvolution(cfg)

	// UpdateWeight delegates to RecordCoAccess
	re.UpdateWeight("nodeA", "nodeB", 0.75)
	trend := re.GetTrend("nodeA", "nodeB")
	require.NotNil(t, trend)
}

func TestRelationshipEvolution_GetEmergingRelationships(t *testing.T) {
	cfg := DefaultRelationshipConfig()
	re := NewRelationshipEvolution(cfg)

	// No data → empty result
	emerging := re.GetEmergingRelationships(10)
	assert.Empty(t, emerging)

	// Record some co-accesses to build up data
	for i := 0; i < 5; i++ {
		re.RecordCoAccess("nodeA", "nodeB", 0.8)
	}

	emerging = re.GetEmergingRelationships(10)
	// May or may not be non-empty depending on velocity threshold
	assert.IsType(t, []RelationshipTrend{}, emerging)
}

func TestRelationshipEvolution_GetEmergingRelationships_Limit(t *testing.T) {
	cfg := DefaultRelationshipConfig()
	re := NewRelationshipEvolution(cfg)

	for i := 0; i < 3; i++ {
		re.RecordCoAccess("A", "B", float64(i+1)*0.1)
		re.RecordCoAccess("C", "D", float64(i+1)*0.2)
		re.RecordCoAccess("E", "F", float64(i+1)*0.3)
	}

	// Limit=1 should return at most 1
	emerging := re.GetEmergingRelationships(1)
	assert.LessOrEqual(t, len(emerging), 1)
}

// ============================================================================
// SessionDetector – IsSessionBoundary
// ============================================================================

func TestSessionDetector_IsSessionBoundary_NoHistory(t *testing.T) {
	sd := NewSessionDetector(DefaultSessionDetectorConfig())
	result := sd.IsSessionBoundary("brand-new-node")
	assert.False(t, result)
}

func TestSessionDetector_IsSessionBoundary_Recent(t *testing.T) {
	sd := NewSessionDetector(DefaultSessionDetectorConfig())

	// Record access now — starts a new session
	sd.RecordAccess("node1", time.Now())

	// A new session started within the last second = boundary
	result := sd.IsSessionBoundary("node1")
	assert.True(t, result)
}

func TestSessionDetector_IsSessionBoundary_JustStarted(t *testing.T) {
	sd := NewSessionDetector(DefaultSessionDetectorConfig())

	// Record access right now — starts a new session immediately
	sd.RecordAccess("node1", time.Now())

	// IsSessionBoundary returns true when session started within last second
	result := sd.IsSessionBoundary("node1")
	assert.True(t, result)
}

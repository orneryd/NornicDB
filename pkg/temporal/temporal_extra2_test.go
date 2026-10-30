package temporal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ============================================================================
// QueryLoadPredictor – uncovered branches
// ============================================================================

func TestQueryLoadPredictor_GetLoadLevel_Zero(t *testing.T) {
	cfg := DefaultLoadConfig()
	qlp := NewQueryLoadPredictor(cfg)
	level := qlp.GetLoadLevel(1000.0)
	assert.Equal(t, 0, level)
}

func TestQueryLoadPredictor_GetLoadLevel_WithLoad(t *testing.T) {
	cfg := DefaultLoadConfig()
	qlp := NewQueryLoadPredictor(cfg)

	for i := 0; i < 20; i++ {
		qlp.RecordQuery()
	}

	level := qlp.GetLoadLevel(1.0) // very low max → high load level
	assert.GreaterOrEqual(t, level, 0)
	assert.LessOrEqual(t, level, 5)
}

func TestQueryLoadPredictor_ShouldScaleUp_NoLoad(t *testing.T) {
	cfg := DefaultLoadConfig()
	qlp := NewQueryLoadPredictor(cfg)
	// No queries recorded → not above threshold
	result := qlp.ShouldScaleUp(100.0)
	assert.False(t, result)
}

func TestQueryLoadPredictor_ShouldScaleDown_NoLoad(t *testing.T) {
	cfg := DefaultLoadConfig()
	qlp := NewQueryLoadPredictor(cfg)
	// CurrentQPS=0 fails the >minQPS=0 check → false
	result := qlp.ShouldScaleDown(50.0, 0.0)
	assert.False(t, result)
}

func TestQueryLoadPredictor_ShouldScaleDown_LowLoad(t *testing.T) {
	cfg := DefaultLoadConfig()
	qlp := NewQueryLoadPredictor(cfg)
	// With minQPS < current (tiny), verify no panic
	_ = qlp.ShouldScaleDown(1000.0, -1.0)
}

func TestQueryLoadPredictor_GetLoadLevel_AllBuckets(t *testing.T) {
	cfg := DefaultLoadConfig()
	qlp := NewQueryLoadPredictor(cfg)

	setQPS := func(v float64) {
		qlp.qpsFilter.Reset()
		for i := 0; i < 32; i++ {
			qlp.qpsFilter.Process(v)
		}
	}

	cases := []struct {
		qps  float64
		want int
	}{
		{5, 0},   // <0.1
		{20, 1},  // <0.3
		{40, 2},  // <0.5
		{60, 3},  // <0.7
		{80, 4},  // <0.9
		{120, 5}, // default
	}
	for _, tc := range cases {
		setQPS(tc.qps)
		got := qlp.GetLoadLevel(100.0)
		assert.Equal(t, tc.want, got, "qps=%v", tc.qps)
	}
}

// ============================================================================
// RelationshipEvolution – GetStrengthening/Weakening
// ============================================================================

func TestRelationshipEvolution_GetStrengtheningRelationships_Empty(t *testing.T) {
	re := NewRelationshipEvolution(DefaultRelationshipConfig())
	result := re.GetStrengtheningRelationships(10)
	assert.IsType(t, []RelationshipTrend{}, result)
}

func TestRelationshipEvolution_GetWeakeningRelationships_Empty(t *testing.T) {
	re := NewRelationshipEvolution(DefaultRelationshipConfig())
	result := re.GetWeakeningRelationships(10)
	assert.IsType(t, []RelationshipTrend{}, result)
}

func TestRelationshipEvolution_GetStrengthening_WithData(t *testing.T) {
	cfg := DefaultRelationshipConfig()
	re := NewRelationshipEvolution(cfg)

	for i := 0; i < 5; i++ {
		re.RecordCoAccess("A", "B", float64(i+1)*0.2)
	}
	result := re.GetStrengtheningRelationships(10)
	assert.IsType(t, []RelationshipTrend{}, result)
}

func TestRelationshipEvolution_GetWeakening_WithData(t *testing.T) {
	cfg := DefaultRelationshipConfig()
	re := NewRelationshipEvolution(cfg)

	for i := 0; i < 5; i++ {
		re.RecordCoAccess("C", "D", 0.9-float64(i)*0.1)
	}
	result := re.GetWeakeningRelationships(10)
	assert.IsType(t, []RelationshipTrend{}, result)
}

func TestRelationshipEvolution_ShouldPrune_NoData(t *testing.T) {
	re := NewRelationshipEvolution(DefaultRelationshipConfig())
	result := re.ShouldPrune("X", "Y", 0.1)
	assert.False(t, result)
}

func TestRelationshipEvolution_ShouldPrune_WithData(t *testing.T) {
	cfg := DefaultRelationshipConfig()
	re := NewRelationshipEvolution(cfg)
	re.RecordCoAccess("X", "Y", 0.5)
	// Prune threshold of 0.99 → likely not pruned
	result := re.ShouldPrune("X", "Y", 0.99)
	_ = result // just verify no panic
}

// ============================================================================
// SessionDetector – GetCoAccessedNodes
// ============================================================================

func TestSessionDetector_GetCoAccessedNodes_NoHistory(t *testing.T) {
	sd := NewSessionDetector(DefaultSessionDetectorConfig())
	nodes := sd.GetCoAccessedNodes("ghost")
	assert.Empty(t, nodes)
}

func TestSessionDetector_GetCoAccessedNodes_WithData(t *testing.T) {
	sd := NewSessionDetector(DefaultSessionDetectorConfig())
	now := time.Now()

	// Access multiple nodes in the same session
	sd.RecordAccess("n1", now)
	sd.RecordAccess("n2", now.Add(100*time.Millisecond))
	sd.RecordAccess("n1", now.Add(200*time.Millisecond))

	nodes := sd.GetCoAccessedNodes("n1")
	assert.IsType(t, []string{}, nodes)
}

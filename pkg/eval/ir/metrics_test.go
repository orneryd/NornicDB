package ir

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompute(t *testing.T) {
	metrics := Compute([]string{"d2", "noise", "d1", "d3"}, map[string]int{"d1": 3, "d2": 1, "d3": 2})
	assert.Equal(t, 1.0, metrics.RecallAt10)
	assert.Equal(t, 1.0, metrics.RecallAt100)
	assert.Equal(t, 1.0, metrics.MRRAt10)
	assert.InDelta(t, (1+2.0/3+3.0/4)/3, metrics.MAPAt100, 0.000001)
	ideal := 7 + 3/math.Log2(3) + 1/math.Log2(4)
	actual := 1 + 7/math.Log2(4) + 3/math.Log2(5)
	assert.InDelta(t, actual/ideal, metrics.NDCGAt10, 0.000001)
}

func TestComputeNoQrels(t *testing.T) {
	assert.Equal(t, Metrics{}, Compute([]string{"d1"}, nil))
}

package search

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClampCosine(t *testing.T) {
	require.Equal(t, 1.0, clampCosine(1.0000001))
	require.Equal(t, -1.0, clampCosine(-1.0000001))
	require.Equal(t, 0.25, clampCosine(0.25))
	require.True(t, math.IsNaN(clampCosine(math.NaN())))
}

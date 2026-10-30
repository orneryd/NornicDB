package angle

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// Values Neo4j 5.26.30 returns for radians() / degrees() (#907); the
// two-rounding forms x * π / 180 and x * 180 / π give the next float.
func TestConversionsMatchJava(t *testing.T) {
	require.Equal(t, math.Pi, ToRadians(180))
	require.Equal(t, 180.0, ToDegrees(math.Pi))
	require.Equal(t, 0.017453292519943295, ToRadians(1))
	require.Equal(t, 57.29577951308232, ToDegrees(1))
	// Inputs where the two-rounding forms differ from Neo4j.
	require.Equal(t, 0.12707542563620103, ToRadians(7.280885568782862))
	require.Equal(t, 58.730485252116864, ToDegrees(1.0250403389434113))
}

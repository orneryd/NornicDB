package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGDSNumericConfigurationKinds(t *testing.T) {
	for _, value := range []interface{}{int8(8), int16(8), int32(8), uint8(8), uint16(8), uint32(8), uint64(8)} {
		cfg, err := fastRPConfiguration(map[string]interface{}{"embeddingDimension": value, "randomSeed": value, "iterationWeights": []interface{}{value}, "propertyRatio": uint8(1), "featureProperties": []interface{}{"f"}})
		require.NoError(t, err, "%T", value)
		require.Equal(t, 8, cfg.EmbeddingDimension)
		require.Equal(t, int64(8), cfg.RandomSeed)
		require.Equal(t, []float64{8}, cfg.IterationWeights)
		require.Equal(t, 1.0, cfg.PropertyRatio)
		projection, _, _, err := gdsAggregationConfiguration(map[string]interface{}{"readConcurrency": value})
		require.NoError(t, err, "%T", value)
		require.Equal(t, int64(8), projection["readConcurrency"])
		count, ok := fastRPConfigInt(value)
		require.True(t, ok)
		require.Equal(t, 8, count)
	}
	// Preserve APOC's existing integral-double and numeric-string coercion.
	for _, value := range []interface{}{3.0, "3"} {
		n, ok := fastRPConfigInt(value)
		require.True(t, ok)
		require.Equal(t, 3, n)
	}
}

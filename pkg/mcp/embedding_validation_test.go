package mcp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateAndConvertEmbedding(t *testing.T) {
	config := DefaultServerConfig()
	config.EmbeddingDimensions = 1024
	config.EmbeddingModel = "mxbai-embed-large"

	server := NewServer(nil, config)

	t.Run("accepts float64 slices", func(t *testing.T) {
		input := make([]float64, 1024)
		for i := range input {
			input[i] = float64(i) / 1024.0
		}

		result, err := server.validateAndConvertEmbedding(input)
		require.NoError(t, err)
		require.Len(t, result, 1024)
		require.InDelta(t, input[17], float64(result[17]), 1e-6)
	})

	t.Run("accepts float32 slices without reallocating shape", func(t *testing.T) {
		input := make([]float32, 1024)
		for i := range input {
			input[i] = float32(i) / 1024.0
		}

		result, err := server.validateAndConvertEmbedding(input)
		require.NoError(t, err)
		require.Len(t, result, 1024)
		require.Equal(t, input[21], result[21])
	})

	t.Run("accepts interface slices from json payloads", func(t *testing.T) {
		input := make([]interface{}, 1024)
		for i := range input {
			switch i % 4 {
			case 0:
				input[i] = float64(i) / 1024.0
			case 1:
				input[i] = float32(i) / 1024.0
			case 2:
				input[i] = i
			default:
				input[i] = int64(i)
			}
		}

		result, err := server.validateAndConvertEmbedding(input)
		require.NoError(t, err)
		require.Len(t, result, 1024)
		require.Equal(t, float32(2), result[2])
		require.Equal(t, float32(3), result[3])
	})

	t.Run("rejects wrong dimensions", func(t *testing.T) {
		_, err := server.validateAndConvertEmbedding(make([]float64, 768))
		require.ErrorContains(t, err, "invalid embedding dimensions: expected 1024, got 768")
		require.ErrorContains(t, err, "mxbai-embed-large")
	})

	t.Run("rejects empty arrays", func(t *testing.T) {
		_, err := server.validateAndConvertEmbedding([]float64{})
		require.ErrorContains(t, err, "cannot be empty array")
	})

	t.Run("rejects non-array input", func(t *testing.T) {
		_, err := server.validateAndConvertEmbedding("not an array")
		require.ErrorContains(t, err, "must be an array of numbers")
	})

	t.Run("rejects non-numeric array elements", func(t *testing.T) {
		_, err := server.validateAndConvertEmbedding([]interface{}{1.0, 2.0, "not a number", 4.0})
		require.ErrorContains(t, err, "element 2 is not a number")
	})

	t.Run("accepts any size when dimensions are not configured", func(t *testing.T) {
		noDimConfig := DefaultServerConfig()
		noDimConfig.EmbeddingDimensions = 0

		noDimServer := NewServer(nil, noDimConfig)
		result, err := noDimServer.validateAndConvertEmbedding(make([]float64, 768))
		require.NoError(t, err)
		require.Len(t, result, 768)
	})
}

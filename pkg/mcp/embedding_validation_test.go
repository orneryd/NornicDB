package mcp

import (
	"testing"
)

func TestValidateAndConvertEmbedding(t *testing.T) {
	// Create server with configured dimensions
	config := DefaultServerConfig()
	config.EmbeddingDimensions = 1024
	config.EmbeddingModel = "mxbai-embed-large"

	server := NewServer(nil, config)

	t.Run("ValidFloat64Array", func(t *testing.T) {
		input := make([]float64, 1024)
		for i := range input {
			input[i] = float64(i) / 1024.0
		}

		result, err := server.validateAndConvertEmbedding(input)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(result) != 1024 {
			t.Errorf("Expected 1024 dimensions, got %d", len(result))
		}
	})

	t.Run("ValidFloat32Array", func(t *testing.T) {
		input := make([]float32, 1024)
		for i := range input {
			input[i] = float32(i) / 1024.0
		}

		result, err := server.validateAndConvertEmbedding(input)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(result) != 1024 {
			t.Errorf("Expected 1024 dimensions, got %d", len(result))
		}
	})

	t.Run("ValidInterfaceArray", func(t *testing.T) {
		// This is how JSON comes in
		input := make([]interface{}, 1024)
		for i := range input {
			input[i] = float64(i) / 1024.0
		}

		result, err := server.validateAndConvertEmbedding(input)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(result) != 1024 {
			t.Errorf("Expected 1024 dimensions, got %d", len(result))
		}
	})

	t.Run("WrongDimensions", func(t *testing.T) {
		input := make([]float64, 768) // Wrong size

		_, err := server.validateAndConvertEmbedding(input)
		if err == nil {
			t.Fatal("Expected error for wrong dimensions")
		}
		expected := "invalid embedding dimensions: expected 1024, got 768"
		if err.Error()[:len(expected)] != expected {
			t.Errorf("Wrong error message: %v", err)
		}
		t.Logf("✓ Correct error: %v", err)
	})

	t.Run("EmptyArray", func(t *testing.T) {
		input := []float64{}

		_, err := server.validateAndConvertEmbedding(input)
		if err == nil {
			t.Fatal("Expected error for empty array")
		}
		t.Logf("✓ Correct error: %v", err)
	})

	t.Run("InvalidType", func(t *testing.T) {
		input := "not an array"

		_, err := server.validateAndConvertEmbedding(input)
		if err == nil {
			t.Fatal("Expected error for invalid type")
		}
		t.Logf("✓ Correct error: %v", err)
	})

	t.Run("ArrayWithNonNumbers", func(t *testing.T) {
		input := []interface{}{1.0, 2.0, "not a number", 4.0}

		_, err := server.validateAndConvertEmbedding(input)
		if err == nil {
			t.Fatal("Expected error for non-number element")
		}
		t.Logf("✓ Correct error: %v", err)
	})

	t.Run("NoDimensionConfig", func(t *testing.T) {
		// Server without dimension config should accept any size
		noDimConfig := DefaultServerConfig()
		noDimConfig.EmbeddingDimensions = 0 // Not configured

		noDimServer := NewServer(nil, noDimConfig)

		input := make([]float64, 768) // Any size
		result, err := noDimServer.validateAndConvertEmbedding(input)
		if err != nil {
			t.Fatalf("Should accept any dimensions when not configured: %v", err)
		}
		if len(result) != 768 {
			t.Errorf("Expected 768 dimensions, got %d", len(result))
		}
		t.Log("✓ Accepts any dimensions when not configured")
	})
}

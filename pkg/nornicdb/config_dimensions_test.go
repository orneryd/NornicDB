package nornicdb

import (
	"os"
	"testing"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDBSearchServiceDimensionsMatchConfig verifies that the search service
// is created with the EXACT dimensions from the DB config.
func TestDBSearchServiceDimensionsMatchConfig(t *testing.T) {
	tests := []struct {
		name         string
		configDims   int
		expectedDims int
	}{
		{"512 dims (Apple Intelligence)", 512, 512},
		{"1024 dims (bge-m3)", 1024, 1024},
		{"384 dims (MiniLM)", 384, 384},
		{"0 dims (fallback to 1024)", 0, 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", "nornicdb-dims-test-*")
			require.NoError(t, err)
			defer os.RemoveAll(tmpDir)

			cfg := &Config{
				Database: nornicConfig.DatabaseConfig{
					DataDir: tmpDir,
				},
				Memory: nornicConfig.MemoryConfig{
					EmbeddingDimensions: tt.configDims,
				},
			}

			t.Logf("Opening DB with config.Memory.EmbeddingDimensions = %d", cfg.Memory.EmbeddingDimensions)

			db, err := Open(tmpDir, cfg)
			require.NoError(t, err)
			defer db.Close()

			// Check the search service's vector index dimensions
			svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
			require.NoError(t, err)
			actualDims := svc.VectorIndexDimensions()
			t.Logf("Search service vector index dimensions: %d", actualDims)

			assert.Equal(t, tt.expectedDims, actualDims,
				"Search service should have %d dimensions, got %d",
				tt.expectedDims, actualDims)
		})
	}
}

// TestDBConfigVsServerConfigDimensions simulates the full flow where
// both DB and Server configs are set from the same source variable.
func TestDBConfigVsServerConfigDimensions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "nornicdb-dual-config-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// This simulates what main.go does:
	// Both dbConfig and serverConfig get embeddingDim from the same variable
	embeddingDim := 512 // Simulating YAML config override

	// Create DB config (like main.go line 342-349)
	dbConfig := DefaultConfig()
	dbConfig.Database.DataDir = tmpDir
	dbConfig.Memory.EmbeddingDimensions = embeddingDim
	t.Logf("dbConfig.Memory.EmbeddingDimensions = %d", dbConfig.Memory.EmbeddingDimensions)

	// Open DB
	db, err := Open(tmpDir, dbConfig)
	require.NoError(t, err)
	defer db.Close()

	// Check search service dimensions (this is what does vector search)
	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)
	searchServiceDims := svc.VectorIndexDimensions()
	t.Logf("Search service vector index dimensions: %d", searchServiceDims)

	// Both should be 512
	assert.Equal(t, 512, dbConfig.Memory.EmbeddingDimensions, "DB config should have 512")
	assert.Equal(t, 512, searchServiceDims, "Search service should have 512")
}

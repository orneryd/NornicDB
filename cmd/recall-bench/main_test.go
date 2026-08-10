package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabaseRetrieverRRFOptions(t *testing.T) {
	t.Run("default preset uses equal weights", func(t *testing.T) {
		opts, err := (databaseRetriever{rrfPreset: "default", minRRFScore: -1}).rrfOptions("long scientific claim query")
		require.NoError(t, err)
		assert.Equal(t, 1.0, opts.VectorWeight)
		assert.Equal(t, 1.0, opts.BM25Weight)
		assert.Equal(t, 0.01, opts.MinRRFScore)
	})

	t.Run("overrides replace selected preset", func(t *testing.T) {
		opts, err := (databaseRetriever{rrfPreset: "adaptive", rrfK: 30, vectorWeight: 0.25, bm25Weight: 1.75, minRRFScore: 0}).rrfOptions("long scientific claim query")
		require.NoError(t, err)
		assert.Equal(t, 30.0, opts.RRFK)
		assert.Equal(t, 0.25, opts.VectorWeight)
		assert.Equal(t, 1.75, opts.BM25Weight)
		assert.Equal(t, 0.0, opts.MinRRFScore)
	})

	_, err := (databaseRetriever{rrfPreset: "unknown"}).rrfOptions("query")
	require.Error(t, err)
}

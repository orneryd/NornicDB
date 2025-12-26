package vectorspace

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorSpaceKeyCanonicalization(t *testing.T) {
	key := VectorSpaceKey{
		DB:         "MainDB ",
		Type:       " Documents",
		VectorName: " Title ",
		Dims:       1536,
		Distance:   DistanceMetric("COSINE"),
	}

	canonical, err := key.Canonical()
	require.NoError(t, err)

	assert.Equal(t, "maindb", canonical.DB)
	assert.Equal(t, "documents", canonical.Type)
	assert.Equal(t, "title", canonical.VectorName)
	assert.Equal(t, 1536, canonical.Dims)
	assert.Equal(t, DistanceCosine, canonical.Distance)

	hash, err := canonical.Hash()
	require.NoError(t, err)
	assert.Equal(t, "maindb|documents|title|1536|cosine", hash)
}

func TestVectorSpaceKeyDefaultingAndValidation(t *testing.T) {
	t.Run("defaults when optional fields are empty", func(t *testing.T) {
		key := VectorSpaceKey{
			DB:   "Alpha",
			Type: "Nodes",
			Dims: 128,
		}

		canonical, err := key.Canonical()
		require.NoError(t, err)

		assert.Equal(t, DefaultVectorName, canonical.VectorName)
		assert.Equal(t, DistanceCosine, canonical.Distance)
	})

	t.Run("rejects invalid distance", func(t *testing.T) {
		_, err := (VectorSpaceKey{
			DB:         "Alpha",
			Type:       "Nodes",
			VectorName: "custom",
			Dims:       3,
			Distance:   DistanceMetric("manhattan"),
		}).Canonical()
		require.Error(t, err)
	})

	t.Run("rejects zero dimensions", func(t *testing.T) {
		_, err := (VectorSpaceKey{
			DB:         "Alpha",
			Type:       "Nodes",
			VectorName: "custom",
			Distance:   DistanceCosine,
		}).Canonical()
		require.Error(t, err)
	})
}

func TestIndexRegistryLifecycle(t *testing.T) {
	registry := NewIndexRegistry()

	key := VectorSpaceKey{
		DB:         "Prod",
		Type:       "Collection",
		VectorName: ChunkVectorName,
		Dims:       256,
		Distance:   DistanceEuclidean,
	}

	space, err := registry.CreateSpace(key, BackendHNSW)
	require.NoError(t, err)
	require.NotNil(t, space)
	assert.Equal(t, BackendHNSW, space.Backend)

	fetched, ok := registry.GetSpace(VectorSpaceKey{
		DB:         "prod",
		Type:       "collection",
		VectorName: "chunks",
		Dims:       256,
		Distance:   DistanceMetric("EUCLIDEAN"),
	})
	require.True(t, ok)
	assert.Equal(t, space, fetched)

	stats := registry.Stats()
	require.Len(t, stats, 1)
	assert.Equal(t, int64(0), stats[0].VectorCount)
	assert.Equal(t, BackendHNSW, stats[0].Backend)
	assert.Equal(t, DistanceEuclidean, stats[0].Distance)
	assert.Equal(t, 256, stats[0].Dimensions)

	deleted := registry.DeleteSpace(key)
	assert.True(t, deleted)
	_, ok = registry.GetSpace(key)
	assert.False(t, ok)
	assert.Empty(t, registry.Stats())
}

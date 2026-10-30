package vectorspace

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validKey() VectorSpaceKey {
	return VectorSpaceKey{DB: "testdb", Type: "Document", VectorName: "embedding", Dims: 128, Distance: "cosine"}
}

func TestVectorSpaceKey_Hash_Valid(t *testing.T) {
	k := validKey()
	h, err := k.Hash()
	require.NoError(t, err)
	assert.NotEmpty(t, h)
	// Same key must produce same hash
	h2, err := k.Hash()
	require.NoError(t, err)
	assert.Equal(t, h, h2)
}

func TestVectorSpaceKey_Hash_InvalidDistance(t *testing.T) {
	k := VectorSpaceKey{DB: "db", Type: "T", VectorName: "f", Dims: 64, Distance: "invalid_metric"}
	_, err := k.Hash()
	assert.Error(t, err)
}

func TestNormalizeBackend_Empty(t *testing.T) {
	result := normalizeBackend("")
	assert.Equal(t, BackendAuto, result)
}

func TestNormalizeBackend_NonEmpty(t *testing.T) {
	result := normalizeBackend("QDRANT")
	assert.Equal(t, BackendKind("qdrant"), result)
}

func TestIndexRegistry_CreateSpace_Idempotent(t *testing.T) {
	r := NewIndexRegistry()
	k := validKey()

	s1, err := r.CreateSpace(k, BackendAuto)
	require.NoError(t, err)
	require.NotNil(t, s1)

	// Second create → same space
	s2, err := r.CreateSpace(k, BackendAuto)
	require.NoError(t, err)
	assert.Equal(t, s1, s2)
}

func TestIndexRegistry_CreateSpace_InvalidKey(t *testing.T) {
	r := NewIndexRegistry()
	k := VectorSpaceKey{DB: "db", Type: "T", VectorName: "f", Dims: 64, Distance: "bad_metric"}
	_, err := r.CreateSpace(k, BackendAuto)
	assert.Error(t, err)
}

func TestIndexRegistry_GetSpace_NotFound(t *testing.T) {
	r := NewIndexRegistry()
	_, ok := r.GetSpace(validKey())
	assert.False(t, ok)
}

func TestIndexRegistry_GetSpace_Found(t *testing.T) {
	r := NewIndexRegistry()
	k := validKey()
	_, err := r.CreateSpace(k, BackendAuto)
	require.NoError(t, err)

	space, ok := r.GetSpace(k)
	assert.True(t, ok)
	assert.NotNil(t, space)
}

func TestIndexRegistry_DeleteSpace_NotFound(t *testing.T) {
	r := NewIndexRegistry()
	ok := r.DeleteSpace(validKey())
	assert.False(t, ok)
}

func TestIndexRegistry_DeleteSpace_Found(t *testing.T) {
	r := NewIndexRegistry()
	k := validKey()
	_, err := r.CreateSpace(k, BackendAuto)
	require.NoError(t, err)

	ok := r.DeleteSpace(k)
	assert.True(t, ok)

	// Should be gone now
	_, exists := r.GetSpace(k)
	assert.False(t, exists)
}

func TestIndexRegistry_DeleteSpace_InvalidKey(t *testing.T) {
	r := NewIndexRegistry()
	k := VectorSpaceKey{DB: "db", Type: "T", VectorName: "f", Dims: 64, Distance: "bad"}
	ok := r.DeleteSpace(k)
	assert.False(t, ok)
}

func TestIndexRegistry_Stats_Empty(t *testing.T) {
	r := NewIndexRegistry()
	stats := r.Stats()
	assert.Empty(t, stats)
}

func TestIndexRegistry_Stats_WithSpaces(t *testing.T) {
	r := NewIndexRegistry()
	k1 := VectorSpaceKey{DB: "db1", Type: "A", VectorName: "emb", Dims: 64, Distance: "cosine"}
	k2 := VectorSpaceKey{DB: "db2", Type: "B", VectorName: "emb", Dims: 128, Distance: "cosine"}
	_, _ = r.CreateSpace(k1, BackendAuto)
	_, _ = r.CreateSpace(k2, BackendAuto)

	stats := r.Stats()
	assert.Len(t, stats, 2)
}

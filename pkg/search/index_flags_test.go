package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSetIndexFlags — defaults are (true, true); SetIndexFlags reports change
// when at least one value differs from the previous.
func TestSetIndexFlags(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	require.True(t, svc.BM25Enabled())
	require.True(t, svc.VectorEnabled())

	changed := svc.SetIndexFlags(true, true)
	assert.False(t, changed, "no-op when values match")

	changed = svc.SetIndexFlags(false, true)
	assert.True(t, changed)
	assert.False(t, svc.BM25Enabled())
	assert.True(t, svc.VectorEnabled())

	changed = svc.SetIndexFlags(false, false)
	assert.True(t, changed)
	assert.False(t, svc.VectorEnabled())
}

// TestBuildIndexes_BothDisabled — when both indexes are off, BuildIndexes
// returns immediately, marks ready, leaves vectorIndex nil, and uses the
// no-op BM25 stub.
func TestBuildIndexes_BothDisabled(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	svc.SetIndexFlags(false, false)

	require.NoError(t, svc.BuildIndexes(context.Background()))
	assert.True(t, svc.IsReady())
	assert.Equal(t, 0, svc.fulltextIndex.Count(), "no-op stub returns 0")

	// vectorIndex stays non-nil so existing GetDimensions/Count calls
	// don't panic; the flag guards in IndexNode and Search are what
	// disable behaviour.
	assert.False(t, svc.VectorEnabled())
	assert.Equal(t, 0, svc.EmbeddingCount())
}

// TestBuildIndexes_VectorDisabled — vector off only; BM25 still builds. The
// vectorIndex is nil after warmupVectorPipeline early-returns.
func TestBuildIndexes_VectorDisabled(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	svc.SetIndexFlags(true, false)

	require.NoError(t, svc.BuildIndexes(context.Background()))
	assert.True(t, svc.IsReady())
	assert.False(t, svc.VectorEnabled())
	assert.Equal(t, 0, svc.EmbeddingCount(), "no embeddings should populate when vector is disabled")
}

// TestBuildIndexes_BM25Disabled — BM25 off only; the no-op stub replaces the
// real fulltext index so existing code paths that call .Count() / .Search()
// don't panic.
func TestBuildIndexes_BM25Disabled(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	svc.SetIndexFlags(false, true)

	require.NoError(t, svc.BuildIndexes(context.Background()))
	assert.True(t, svc.IsReady())
	// The no-op stub reports zero count and empty searches.
	assert.Equal(t, 0, svc.fulltextIndex.Count())
	results := svc.fulltextIndex.Search("anything", 10)
	assert.Empty(t, results)
}

// TestIndexNode_BothDisabled — IndexNode is a no-op when both flags are off.
func TestIndexNode_BothDisabled(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	svc.SetIndexFlags(false, false)
	require.NoError(t, svc.BuildIndexes(context.Background()))

	node := &storage.Node{
		ID:         storage.NodeID("n1"),
		Properties: map[string]interface{}{"name": "alice"},
	}
	require.NoError(t, svc.IndexNode(node))
	assert.Equal(t, 0, svc.fulltextIndex.Count())
}

// TestMarkReadyDisabled — explicit short-circuit for the boot-orchestrator
// "both disabled" path.
func TestMarkReadyDisabled(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	svc.SetIndexFlags(false, false)
	svc.MarkReadyDisabled()

	assert.True(t, svc.IsReady())
	assert.Equal(t, 0, svc.fulltextIndex.Count(), "no-op stub reports zero")
	assert.Equal(t, 0, svc.EmbeddingCount(), "no embeddings populated")
}

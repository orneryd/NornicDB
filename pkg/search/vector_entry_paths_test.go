package search

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// undecodableVectorKey starts with the key marker but is not a valid
// encoding, so it has no owner.
const undecodableVectorKey = "\x00z\x01x"

// A key without an owner is never attributed to a node: every reader that
// maps raw keys to nodes skips it.
func TestUndecodableVectorKeysHaveNoOwnerInEveryReader(t *testing.T) {
	results := collapseIndexResultsByNodeID([]indexResult{
		{ID: undecodableVectorKey, MatchID: undecodableVectorKey, Score: 1},
		{ID: nodeChunkVectorKey("doc", 1), MatchID: nodeChunkVectorKey("doc", 1), Score: 0.5},
	})
	assert.Equal(t, []indexResult{{ID: "doc", MatchID: nodeChunkVectorKey("doc", 1), Score: 0.5}}, results)
	assert.Equal(t, []SearchCandidate{{ID: "doc", Score: 0.5}}, collapseCandidatesByNodeID([]SearchCandidate{
		{ID: undecodableVectorKey, Score: 1}, {ID: "doc", Score: 0.5},
	}))

	hints := nodeLexicalHints{"x": {ID: "x", Rank: 1}}
	_, ok := lexicalHintForVectorID(undecodableVectorKey, hints)
	assert.False(t, ok)
	hint, ok := lexicalHintForVectorID(nodeChunkVectorKey("x", 2), hints)
	assert.True(t, ok)
	assert.Equal(t, hints["x"], hint)
	assert.False(t, vectorIDInSeedNodeSet(undecodableVectorKey, map[string]struct{}{"x": {}}))
	assert.True(t, vectorIDInSeedNodeSet(nodeNamedVectorKey("x", "v"), map[string]struct{}{"x": {}}))

	graph := NewHNSWIndex(2, DefaultHNSWConfig())
	require.NoError(t, graph.Add(undecodableVectorKey, []float32{1, 0}))
	require.NoError(t, graph.Add(nodeChunkVectorKey("doc", 1), []float32{0.9, 0.1}))
	graph.SetBuildLexicalHints([]LexicalSeedHint{{ID: nodeChunkVectorKey("doc", 1), Rank: 1}, {ID: "x", Rank: 2}})
	for internalID, key := range graph.internalToID {
		hint, ok := graph.buildLexicalHintLocked(uint32(internalID))
		if key == undecodableVectorKey {
			assert.False(t, ok, "an undecodable key gets no lexical hint")
		} else {
			assert.True(t, ok)
			assert.Equal(t, vectorNodeID("doc"), hint.ID)
		}
	}
	candidates, err := NewHNSWCandidateGen(graph).SearchCandidates(context.Background(), []float32{1, 0}, 5, -1)
	require.NoError(t, err)
	if assert.Len(t, candidates, 1) {
		assert.Equal(t, nodeChunkVectorKey("doc", 1), candidates[0].ID)
	}

}

// forEachNodeVectorEntry stops as soon as its callback does, in every entry
// kind, and visits entries in kind order.
func TestForEachNodeVectorEntryStopsWhenAsked(t *testing.T) {
	svc := NewService(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	for name, node := range map[string]*storage.Node{
		"named":    {ID: "n", NamedEmbeddings: map[string][]float32{"a": {1, 0}, "b": {0, 1}}},
		"chunk":    {ID: "n", ChunkEmbeddings: [][]float32{{1, 0}, {0, 1}}},
		"property": {ID: "n", Properties: map[string]any{"a": []float32{1, 0}, "b": []float32{0, 1}}},
	} {
		t.Run(name, func(t *testing.T) {
			visited := 0
			svc.forEachNodeVectorEntry(node, 2, func(nodeVectorEntry) bool {
				visited++
				return false
			})
			assert.Equal(t, 1, visited)
		})
	}

	var kinds []vectorEntryKind
	svc.forEachNodeVectorEntry(&storage.Node{
		ID:              "n",
		NamedEmbeddings: map[string][]float32{"v": {1, 0}, "empty": nil},
		ChunkEmbeddings: [][]float32{{1, 0}, nil, {0, 1}},
		Properties:      map[string]any{"p": []float32{1, 0}, "short": []float32{1}},
	}, 2, func(item nodeVectorEntry) bool {
		kinds = append(kinds, item.entry.Kind)
		decoded, ok := decodeVectorEntryKey(item.key)
		require.True(t, ok)
		assert.Equal(t, item.entry, decoded, "the entry and its key must agree")
		return true
	})
	assert.Equal(t, []vectorEntryKind{vectorEntryNamed, vectorEntryMain, vectorEntryChunk, vectorEntryProperty}, kinds)
}

// Release admission rejects an explicit dimension mismatch before mutation.
// The rejection must preserve acknowledged entries in every destination, while
// capacity estimation still charges only canonical entries of valid dimensions.
func TestIndexNodeRejectsMismatchedEntriesBeforeMutation(t *testing.T) {
	svc := NewService(newNamespacedEngine(t), 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	embCfg := gpu.DefaultEmbeddingIndexConfig(2)
	embCfg.GPUEnabled = false
	embCfg.AutoSync = false
	svc.gpuEmbeddingIndex = gpu.NewEmbeddingIndex(nil, embCfg)
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "seed", ChunkEmbeddings: [][]float32{{0, 1}}}))
	invalid := &storage.Node{
		ID:              "n",
		NamedEmbeddings: map[string][]float32{"wide": {1, 0, 0}, "v": {1, 0}},
		ChunkEmbeddings: [][]float32{{0, 1}},
		Properties:      map[string]any{"text": "rejectedtoken"},
	}
	// A rejected create must not admit even its valid entries.
	require.ErrorIs(t, svc.IndexNode(invalid), ErrDimensionMismatch)
	require.Equal(t, 1, svc.EmbeddingCount())
	require.Equal(t, 1, svc.gpuEmbeddingIndex.Count())
	require.Empty(t, svc.nodeNamedVector["n"])
	require.Empty(t, svc.nodeChunkVectors["n"])
	_, exists := svc.fulltext().GetDocument("n")
	require.False(t, exists)
	valid := &storage.Node{
		ID:              "n",
		NamedEmbeddings: map[string][]float32{"v": {0, 1}},
		ChunkEmbeddings: [][]float32{{1, 0}},
		Properties:      map[string]any{"text": "originaltoken", "short": []float32{1}},
	}
	for range 2 {
		require.NoError(t, svc.IndexNode(valid))
		capacity := svc.indexCapacityByNode["n"]
		revision := svc.indexRevision.Load()
		require.ErrorIs(t, svc.IndexNode(invalid), ErrDimensionMismatch)
		assert.Equal(t, revision, svc.indexRevision.Load(), "rejection must not publish a partial revision")
		assert.Equal(t, capacity, svc.indexCapacityByNode["n"], "rejection must not replace capacity ownership")
		assert.Equal(t, 3, svc.EmbeddingCount())
		assert.Equal(t, map[string]string{"v": nodeNamedVectorKey("n", "v")}, svc.nodeNamedVector["n"])
		assert.Equal(t, []string{nodeMainVectorKey("n")}, svc.nodeChunkVectors["n"])
		assert.Equal(t, 3, svc.gpuEmbeddingIndex.Count())
		for key, expected := range map[string][]float32{nodeMainVectorKey("n"): {1, 0}, nodeNamedVectorKey("n", "v"): {0, 1}, "seed": {0, 1}} {
			got, ok := svc.getVectorLookup()(key)
			require.True(t, ok)
			assert.Equal(t, expected, got, "failed updates preserve the previous vector")
		}
		text, ok := svc.fulltext().GetDocument("n")
		require.True(t, ok)
		assert.Equal(t, "originaltoken", text)
		svc.indexMu.RLock()
		unchanged := svc.nodeVectorStateUnchangedLocked(invalid)
		svc.indexMu.RUnlock()
		assert.False(t, unchanged, "rejected entries must never match the indexed state")
	}
	svc.indexMu.RLock()
	usage, err := svc.checkNodeIndexCapacityLocked(invalid, nodeFulltext{})
	svc.indexMu.RUnlock()
	require.NoError(t, err)
	assert.Equal(t, int64(len("n")+16+len(nodeNamedVectorKey("n", "v"))+16+len("n")+16), usage.vectorMetadata,
		"the capacity estimate charges only canonical entries with the destination dimension")
}

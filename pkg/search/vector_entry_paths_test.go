package search

import (
	"context"
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

	hints := map[string]LexicalSeedHint{"x": {ID: "x", Rank: 1}}
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
	graph.buildLexicalHints = map[string]LexicalSeedHint{"doc": {ID: "doc", Rank: 1}, "x": {ID: "x", Rank: 2}}
	for internalID, key := range graph.internalToID {
		hint, ok := graph.buildLexicalHintLocked(uint32(internalID))
		if key == undecodableVectorKey {
			assert.False(t, ok, "an undecodable key gets no lexical hint")
		} else {
			assert.True(t, ok)
			assert.Equal(t, "doc", hint.ID)
		}
	}
	candidates, err := NewHNSWCandidateGen(graph).SearchCandidates(context.Background(), []float32{1, 0}, 5, -1)
	require.NoError(t, err)
	if assert.Len(t, candidates, 1) {
		assert.Equal(t, nodeChunkVectorKey("doc", 1), candidates[0].ID)
	}

	// Capacity reconciliation still charges an undecodable resident key, to
	// the key itself, so the totals stay exact.
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.vectorIndex.Add(undecodableVectorKey, []float32{1, 0}))
	svc.indexMu.Lock()
	svc.reconcileVectorSourceCapacityLocked(svc.vectorIndex)
	svc.indexMu.Unlock()
	assert.Positive(t, svc.indexCapacityByNode[undecodableVectorKey].vectorResident)
	assert.Equal(t, svc.indexCapacityByNode[undecodableVectorKey].vectorResident, svc.vectorResidentBytes)
}

// forEachNodeVectorEntry stops as soon as its callback does, in every entry
// kind, and visits entries in kind order.
func TestForEachNodeVectorEntryStopsWhenAsked(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
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

// An entry of the wrong dimension is rejected and logged while the node's
// other entries are indexed (also into the GPU index), and re-indexing the
// same node is never treated as unchanged.
func TestIndexNodeSkipsMismatchedEntriesAndKeepsTheRest(t *testing.T) {
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 2)
	t.Cleanup(func() { _ = svc.Close() })
	embCfg := gpu.DefaultEmbeddingIndexConfig(2)
	embCfg.GPUEnabled = false
	embCfg.AutoSync = false
	svc.gpuEmbeddingIndex = gpu.NewEmbeddingIndex(nil, embCfg)
	node := &storage.Node{
		ID:              "n",
		NamedEmbeddings: map[string][]float32{"wide": {1, 0, 0}, "v": {0, 1}},
		ChunkEmbeddings: [][]float32{{1, 0}},
	}
	for range 2 {
		require.NoError(t, svc.IndexNode(node))
		assert.Equal(t, 2, svc.EmbeddingCount(), "the mismatched named vector is skipped")
		assert.Equal(t, map[string]string{"v": nodeNamedVectorKey("n", "v")}, svc.nodeNamedVector["n"])
		assert.Equal(t, []string{"n"}, svc.nodeChunkVectors["n"])
		assert.Equal(t, 2, svc.gpuEmbeddingIndex.Count())
		svc.indexMu.RLock()
		unchanged := svc.nodeVectorStateUnchangedLocked(node)
		svc.indexMu.RUnlock()
		assert.False(t, unchanged, "a node with a rejected entry never matches its indexed state")
	}
	svc.indexMu.RLock()
	usage := svc.estimateNodeIndexCapacityLocked(node, nodeFulltext{})
	svc.indexMu.RUnlock()
	assert.Equal(t, nodeVectorMetadataBytes("n", nil)+vectorEntryMetadataBytes(nodeNamedVectorKey("n", "v"))+vectorEntryMetadataBytes("n"), usage.vectorMetadata,
		"the capacity estimate charges only the entries IndexNode stores")
}

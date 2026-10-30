package search

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHNSWNeighborSelectionKeepsDiverseConnections(t *testing.T) {
	config := DefaultHNSWConfig()
	config.M = 2
	index := NewHNSWIndex(2, config)
	require.NoError(t, index.Add("near", []float32{0.9950042, 0.0998334}))
	require.NoError(t, index.Add("redundant", []float32{0.9939561, 0.1097783}))
	require.NoError(t, index.Add("diverse", []float32{0.8775826, -0.4794255}))

	index.mu.Lock()
	selected := index.selectNeighborsInto(
		[]float32{1, 0},
		[]uint32{index.idToInternal["near"], index.idToInternal["redundant"], index.idToInternal["diverse"]},
		2,
		nil,
	)
	selectedIDs := make([]string, len(selected))
	for i, internalID := range selected {
		selectedIDs[i] = index.internalToID[internalID]
	}
	index.mu.Unlock()

	require.Equal(t, []string{"near", "diverse"}, selectedIDs)
}

func TestHNSWBaseLayerSupportsTwiceConfiguredConnections(t *testing.T) {
	config := DefaultHNSWConfig()
	config.M = 2
	index := NewHNSWIndex(2, config)
	for id, vector := range map[string][]float32{
		"base": {1, 0},
		"a":    {0.9, 0.1},
		"b":    {0.8, 0.2},
		"c":    {0.7, 0.3},
		"d":    {0.6, 0.4},
	} {
		require.NoError(t, index.Add(id, vector))
	}

	index.mu.Lock()
	baseID := index.idToInternal["base"]
	neighbors := []uint32{
		index.idToInternal["a"],
		index.idToInternal["b"],
		index.idToInternal["c"],
		index.idToInternal["d"],
	}
	index.setNeighborsAtLevelLocked(baseID, 0, neighbors)
	stored, ok := index.neighborsAtLevelLocked(baseID, 0)
	stored = append([]uint32(nil), stored...)
	index.mu.Unlock()

	require.True(t, ok)
	require.Len(t, stored, 2*config.M)
}

func TestHNSWLoadRejectsGraphsBuiltWithPreviousTopology(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hnsw")
	writeHNSWSnapshot(t, path, &hnswIndexSnapshot{
		Version:      "1.1.0",
		Config:       DefaultHNSWConfig(),
		Dimensions:   2,
		InternalToID: []string{"node"},
		IDToInternal: map[string]uint32{"node": 0},
		NodeLevel:    []uint16{0},
		Deleted:      []bool{false},
		LiveCount:    1,
	})

	loaded, err := LoadHNSWIndex(path, func(string) ([]float32, bool) {
		return []float32{1, 0}, true
	})
	require.NoError(t, err)
	require.Nil(t, loaded)
}

func TestHNSWSearchUsesLexicalEntryPointsToReachDisconnectedRegions(t *testing.T) {
	index := NewHNSWIndex(2, HNSWConfig{
		M:               2,
		EfConstruction:  4,
		EfSearch:        1,
		LevelMultiplier: 1,
	})
	require.NoError(t, index.Add("lexically-unrelated", []float32{1, 0}))
	require.NoError(t, index.Add("lexical-match", []float32{-1, 0}))

	index.mu.Lock()
	index.entryPoint = index.idToInternal["lexically-unrelated"]
	index.maxLevel = 0
	index.setNeighborsAtLevelLocked(index.idToInternal["lexically-unrelated"], 0, nil)
	index.setNeighborsAtLevelLocked(index.idToInternal["lexical-match"], 0, nil)
	index.mu.Unlock()

	withoutSeeds, _, err := index.searchWithEfExhaustion(context.Background(), []float32{-1, 0}, 1, -1, 1)
	require.NoError(t, err)
	require.Len(t, withoutSeeds, 1, "the only reachable vector is exactly opposite to the query; minSimilarity -1 must not drop it")
	require.Equal(t, "lexically-unrelated", withoutSeeds[0].ID)

	withSeeds, _, err := index.searchWithEfExhaustionFromEntries(
		context.Background(), []float32{-1, 0}, 1, -1, 1,
		[]string{"missing", "lexical-match", "lexical-match"},
	)
	require.NoError(t, err)
	require.Len(t, withSeeds, 1)
	require.Equal(t, "lexical-match", withSeeds[0].ID)
}

func TestHNSWSearchMinSimilarityMinusOneKeepsExactlyOppositeVector(t *testing.T) {
	index := NewHNSWIndex(2, HNSWConfig{M: 2, EfConstruction: 4, EfSearch: 1, LevelMultiplier: 1})
	require.NoError(t, index.Add("opposite", []float32{1, 0}))
	for _, threshold := range []float64{-1, -1.5} {
		results, _, err := index.searchWithEfExhaustion(context.Background(), []float32{-1, 0}, 1, threshold, 1)
		require.NoError(t, err)
		require.Len(t, results, 1, "threshold %v", threshold)
		require.Equal(t, "opposite", results[0].ID)
	}
	results, _, err := index.searchWithEfExhaustion(context.Background(), []float32{-1, 0}, 1, -0.5, 1)
	require.NoError(t, err)
	require.Empty(t, results, "a real threshold still filters")
}

func TestHNSWNeighborSelectionUsesLexicalDiversityForEqualVectorDistances(t *testing.T) {
	index := NewHNSWIndex(2, HNSWConfig{
		M:               2,
		EfConstruction:  4,
		EfSearch:        2,
		LevelMultiplier: 1,
	})
	index.SetBuildLexicalHints([]LexicalSeedHint{
		{ID: "same-topic", Rank: 0, Signature: 0x00},
		{ID: "same-topic-2", Rank: 1, Signature: 0x00},
		{ID: "different-topic", Rank: 2, Signature: 0xff},
	})
	require.NoError(t, index.Add("same-topic", []float32{0, 1}))
	require.NoError(t, index.Add("same-topic-2", []float32{0, 1}))
	require.NoError(t, index.Add("different-topic", []float32{0, -1}))

	selected := index.selectNeighborsInto([]float32{1, 0}, []uint32{
		index.idToInternal["same-topic"],
		index.idToInternal["same-topic-2"],
		index.idToInternal["different-topic"],
	}, 2, nil)
	require.Equal(t, []uint32{
		index.idToInternal["same-topic"],
		index.idToInternal["different-topic"],
	}, selected)
}

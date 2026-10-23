package search

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// requireSearchFindsEveryLiveVector searches with a beam as wide as the live
// population, which visits every live vector the graph reaches, and requires
// all of them.
func requireSearchFindsEveryLiveVector(t *testing.T, index *HNSWIndex, want []string) {
	t.Helper()
	query := make([]float32, index.GetDimensions())
	for i := range query {
		query[i] = 1
	}
	results, err := index.searchWithEf(context.Background(), query, len(want), -1, len(want))
	require.NoError(t, err)
	got := make([]string, 0, len(results))
	for _, result := range results {
		got = append(got, result.ID)
	}
	require.ElementsMatch(t, want, got)
}

// Runs of neighbouring removed vectors leave paths of several tombstones
// between live ones. Search routes through them.
func TestHNSWSearchRoutesThroughTombstoneRuns(t *testing.T) {
	t.Run("collinear chain", func(t *testing.T) {
		index := NewHNSWIndex(2, DefaultHNSWConfig())
		removed := map[int]bool{}
		for _, run := range [][2]int{{0, 3}, {10, 20}, {30, 35}, {40, 42}} {
			for i := run[0]; i < run[1]; i++ {
				removed[i] = true
			}
		}
		for i := 50; i < 60; i += 3 {
			removed[i] = true
		}
		var want []string
		for i := 0; i < 60; i++ {
			id := fmt.Sprintf("v-%d", i)
			require.NoError(t, index.Add(id, []float32{1, float32(i) / 60}))
			if !removed[i] {
				want = append(want, id)
			}
		}
		for i := range removed {
			index.Remove(fmt.Sprintf("v-%d", i))
		}
		requireSearchFindsEveryLiveVector(t, index, want)
	})
	t.Run("random, removed regions", func(t *testing.T) {
		rng := rand.New(rand.NewSource(7))
		vectors := make([][]float32, 1500)
		index := NewHNSWIndex(16, DefaultHNSWConfig())
		for i := range vectors {
			vectors[i] = make([]float32, 16)
			for j := range vectors[i] {
				vectors[i][j] = float32(rng.NormFloat64())
			}
			require.NoError(t, index.Add(fmt.Sprintf("v-%d", i), vectors[i]))
		}
		// Reachable before any removal: the fixture's own baseline.
		all := make([]string, len(vectors))
		for i := range all {
			all[i] = fmt.Sprintf("v-%d", i)
		}
		requireSearchFindsEveryLiveVector(t, index, all)

		removed := map[int]bool{}
		for _, center := range vectors[:3] {
			order := make([]int, len(vectors))
			score := make([]float32, len(vectors))
			for i, v := range vectors {
				order[i] = i
				for j := range v {
					score[i] += v[j] * center[j]
				}
			}
			slices.SortFunc(order, func(a, b int) int {
				if score[a] != score[b] {
					if score[a] > score[b] {
						return -1
					}
					return 1
				}
				return a - b
			})
			for _, i := range order[:300] {
				removed[i] = true
			}
		}
		var want []string
		for i := range vectors {
			if removed[i] {
				index.Remove(fmt.Sprintf("v-%d", i))
			} else {
				want = append(want, fmt.Sprintf("v-%d", i))
			}
		}
		requireSearchFindsEveryLiveVector(t, index, want)
	})
}

// Removing runs of neighbouring documents leaves paths of several tombstones
// between live ones; continuation must still return every live document.
func TestContinuationAfterTombstoneRunsReturnsEveryLiveDocument(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0") // HNSW at any size
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic")
	service := NewServiceWithDimensions(engine, 2)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for i := 0; i < 30; i++ {
		node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("doc-%d", i)), ChunkEmbeddings: [][]float32{{1, float32(i) / 30}}}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, service.IndexNode(node))
	}
	_, err := service.getOrCreateHNSWIndex(context.Background(), 2)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return !service.hnswRebuildInFlight.Load() }, 5*time.Second, time.Millisecond)
	removed := map[string]bool{}
	for _, run := range [][2]int{{5, 10}, {15, 21}} {
		for i := run[0]; i < run[1]; i++ {
			id := fmt.Sprintf("doc-%d", i)
			require.NoError(t, engine.DeleteNode(storage.NodeID(id)))
			require.NoError(t, service.RemoveNode(storage.NodeID(id)))
			removed[id] = true
		}
	}

	embed := func(context.Context, string) ([]float32, error) { return []float32{1, 0}, nil }
	request := SearchContinuationRequest{Owner: "alice", N: 4}
	page, err := service.SearchTextContinuation(context.Background(), "", &SearchOptions{Limit: 4}, request, nil, embed, service.Search, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	ids := map[string]bool{}
	for {
		require.True(t, strings.HasPrefix(page.SearchMethod, "vector_hnsw"), "search method %q", page.SearchMethod)
		for _, result := range page.Results {
			id := searchResultID(result)
			require.False(t, ids[id], "duplicate %s", id)
			require.False(t, removed[id], "removed %s returned", id)
			ids[id] = true
		}
		if !page.HasMore {
			break
		}
		request.QID = page.QID
		page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
		require.NoError(t, err)
	}
	require.Len(t, ids, 30-len(removed))
	require.Equal(t, SearchContinuationCollectionComplete, page.Completion)
}

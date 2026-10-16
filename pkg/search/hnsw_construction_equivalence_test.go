package search

import (
	"context"
	"math/bits"
	"math/rand"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/stretchr/testify/require"
)

// Independent, deliberately slow definition of the existing HNSW heuristic:
// distance then lexical rank, prefer the largest minimum signature distance
// within a tie, and reject a neighbour closer to a selected neighbour than to
// the query. This pins the decisions while their implementation is optimized.
func referenceHNSWSelection(h *HNSWIndex, query []float32, ids []uint32, m int) []uint32 {
	type item struct {
		id       uint32
		distance float32
		hint     LexicalSeedHint
		hinted   bool
	}
	var candidates []item
	for _, id := range ids {
		if h.deleted[id] {
			continue
		}
		hint, hinted := h.buildLexicalHintLocked(id)
		candidates = append(candidates, item{id, 1 - vector.DotProductSIMD(query, h.vectorAtLocked(id)), hint, hinted})
	}
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.distance != b.distance {
			return a.distance < b.distance
		}
		if a.hinted != b.hinted {
			return a.hinted
		}
		if a.hinted && a.hint.Rank != b.hint.Rank {
			return a.hint.Rank < b.hint.Rank
		}
		return a.id < b.id
	})
	var selected []uint32
	for start := range candidates {
		if len(selected) == m {
			break
		}
		best, bestDiversity, bestRank := start, -1, ^uint32(0)
		for i := start; i < len(candidates) && candidates[i].distance == candidates[start].distance; i++ {
			if !candidates[i].hinted {
				continue
			}
			diversity, compared := 64, false
			for _, id := range selected {
				hint, ok := h.buildLexicalHintLocked(id)
				if ok {
					compared = true
					diversity = min(diversity, bits.OnesCount64(hint.Signature^candidates[i].hint.Signature))
				}
			}
			if compared && (diversity > bestDiversity || diversity == bestDiversity && candidates[i].hint.Rank < bestRank) {
				best, bestDiversity, bestRank = i, diversity, candidates[i].hint.Rank
			}
		}
		candidates[start], candidates[best] = candidates[best], candidates[start]
		c := candidates[start]
		accept := true
		for _, id := range selected {
			if 1-vector.DotProductSIMD(h.vectorAtLocked(c.id), h.vectorAtLocked(id)) < c.distance {
				accept = false
				break
			}
		}
		if accept {
			selected = append(selected, c.id)
		}
	}
	return selected
}

func TestHNSWConstructionHeuristicEquivalent(t *testing.T) {
	rng := rand.New(rand.NewSource(86))
	idx := NewHNSWIndex(8, DefaultHNSWConfig())
	var hints []LexicalSeedHint
	for i := 0; i < 64; i++ {
		id := itoa(i)
		v := make([]float32, 8)
		// Several exact distance ties, plus random vectors and missing hints.
		if i%3 == 0 {
			v[i%8] = 1
		} else {
			for j := range v {
				v[j] = rng.Float32()*2 - 1
			}
		}
		require.NoError(t, idx.Add(id, v))
		if i%5 != 0 {
			hints = append(hints, LexicalSeedHint{ID: id, Rank: uint32(i % 7), Signature: rng.Uint64()})
		}
	}
	idx.SetBuildLexicalHints(hints)
	idx.Remove("5")
	for iteration := 0; iteration < 100; iteration++ {
		query := make([]float32, 8)
		if iteration%2 == 0 {
			query[0] = 1
		} else {
			for j := range query {
				query[j] = rng.Float32()*2 - 1
			}
			vector.NormalizeInPlace(query)
		}
		ids := make([]uint32, 64)
		for i, id := range rng.Perm(64) {
			ids[i] = uint32(id)
		}
		for _, m := range []int{1, 4, 16, 32} {
			require.Equal(t, referenceHNSWSelection(idx, query, ids, m), idx.selectNeighborsInto(query, ids, m, nil))
		}
	}
}

func TestHNSWConstructionCachePreservesGraphAndReleasesViews(t *testing.T) {
	for _, identical := range []bool{false, true} {
		vi, pairs := hnswBuildFixture(96, identical)
		config := DefaultHNSWConfig()
		reference := NewHNSWIndex(2048, config)
		reference.SetVectorLookup(vi.getVectorRef)
		for _, p := range pairs {
			require.NoError(t, reference.Add(p.id, p.vec))
		}
		built, _, err := buildHNSWCPU(context.Background(), 2048, config, vi.getVectorRef, len(pairs), hnswPairSliceIterator(pairs))
		require.NoError(t, err)
		require.Equal(t, reference.nodeLevel, built.nodeLevel)
		require.Equal(t, reference.neighborCountsArena, built.neighborCountsArena)
		require.Equal(t, reference.neighborsArena, built.neighborsArena)
		require.Nil(t, built.buildVectors)
		require.Nil(t, built.buildDistances)
		// Runtime reads must see replacements after the construction views go away.
		replacement := make([]float32, 2048)
		replacement[0] = 1
		require.NoError(t, vi.Add(pairs[0].id, replacement))
		want, _ := vi.getVectorRef(pairs[0].id)
		require.Equal(t, want, built.vectorAtLocked(0))
	}
}

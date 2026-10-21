package search

import (
	"context"
	"math"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func mainKeyEvidenceIndex(ids ...string) *IVFPQIndex {
	weak := []float32{.1, float32(math.Sqrt(.99))}
	return &IVFPQIndex{
		profile:   IVFPQProfile{Dimensions: 2, NProbe: 1, PQSegments: 1, PQBits: 1},
		centroids: [][]float32{weak}, centroidNorm: [][]float32{weak},
		codebooks:     []ivfpqCodebook{{SubDim: 2, Codeword: [][]float32{{0, 0}}}},
		lists:         []ivfpqList{{IDs: ids, CodeSize: 1, Codes: make([]byte, len(ids))}},
		residualKnown: true,
	}
}

func TestIVFPQMainKeyEvidence(t *testing.T) {
	for _, tc := range []struct {
		name              string
		ids               []string
		overflow, overlay string
		want              bool
	}{
		{name: "main", ids: []string{"base"}, want: true},
		{name: "literal suffix main", ids: []string{"base-chunk-1"}, want: true},
		{name: "escaped main", ids: []string{nodeMainVectorKey("\x00base")}, want: true},
		{name: "updated main", ids: []string{"base"}, overlay: "fresh", want: true},
		{name: "base alias", ids: []string{"base", nodeChunkVectorKey("base", 1)}},
		{name: "overflow alias", ids: []string{"base"}, overflow: nodeNamedVectorKey("base", "old")},
		{name: "live overlay alias", ids: []string{"base"}, overlay: nodePropertyVectorKey("base", "new")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			idx := mainKeyEvidenceIndex(tc.ids...)
			if tc.overflow != "" {
				idx.overflow = []ivfpqOverflowVector{{ID: tc.overflow, Vector: []float32{.1, float32(math.Sqrt(.99))}}}
			}
			idx.population()
			path := filepath.Join(t.TempDir(), "vectors")
			require.NoError(t, SaveIVFPQBundle(path, idx))
			loaded, err := LoadIVFPQBundle(path)
			require.NoError(t, err)
			require.Equal(t, idx.mainKeysOnly, loaded.mainKeysOnly, "key provenance is rebuilt before publication")
			for _, index := range []*IVFPQIndex{idx, loaded} {
				overlay := newANNMutationOverlay()
				if tc.overlay != "" {
					overlay.Add(tc.overlay, []float32{1, 0})
				}
				gen := NewIVFPQCandidateGenWithOverlay(index, 1, overlay)
				pool, evidence, err := gen.searchCandidatesWithEvidence(context.Background(), []float32{1, 0}, 8, -1)
				require.NoError(t, err)
				require.NotEmpty(t, pool)
				require.Equal(t, tc.want, evidence.mainKeysOnly)
				if evidence.mainKeysOnly {
					for _, candidate := range pool {
						entry, valid := decodeVectorEntryKey(candidate.ID)
						require.True(t, valid)
						require.Equal(t, vectorEntryMain, entry.Kind)
					}
				}
			}
		})
	}
}

// Main-key provenance concerns the surfaced pool only. Published node members
// can still require expansion, and an immutable stale alias must not grant it.
func TestIVFPQMainKeyEvidenceDoesNotHideMembers(t *testing.T) {
	for _, hidden := range []bool{false, true} {
		t.Run(map[bool]string{false: "retired alias", true: "hidden named member"}[hidden], func(t *testing.T) {
			t.Setenv(EnvSearchResultCacheEnabled, "false")
			t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "100000")
			engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
			svc := NewServiceWithDimensions(engine, 2)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			node := &storage.Node{ID: "winner", ChunkEmbeddings: [][]float32{{1, 0}}}
			candidateID := nodeNamedVectorKey("winner", "old")
			if hidden {
				node.ChunkEmbeddings = [][]float32{{.1, float32(math.Sqrt(.99))}}
				node.NamedEmbeddings = map[string][]float32{"best": {1, 0}}
				candidateID = "winner"
			}
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
			require.NoError(t, svc.IndexNode(node))
			idx := mainKeyEvidenceIndex(candidateID)
			idx.population()
			gen := NewIVFPQCandidateGen(idx, 1)
			_, evidence, err := gen.searchCandidatesWithEvidence(context.Background(), []float32{1, 0}, 1, -1)
			require.NoError(t, err)
			require.Equal(t, hidden, evidence.mainKeysOnly)
			svc.vectorPipeline = NewVectorSearchPipeline(gen, NewCPUExactScorer(svc.vectorIndex))
			response, err := memberIndexSearch(svc)
			require.NoError(t, err)
			require.Len(t, response.Results, 1)
			require.Equal(t, "winner", response.Results[0].ID)
			require.InDelta(t, 1, response.Results[0].Similarity, 1e-6)
		})
	}
}

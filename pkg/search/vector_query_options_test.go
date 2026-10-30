package search

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"

	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// efRecordingGen is a candidate generator that records the HNSW ef its search
// carried and returns fixed candidates.
type efRecordingGen struct {
	ef    int
	calls int
	cands []Candidate
}

func (g *efRecordingGen) SearchCandidates(ctx context.Context, _ []float32, _ int, _ float64) ([]Candidate, error) {
	g.ef = hnswEfFromContext(ctx)
	g.calls++
	return g.cands, nil
}

func newOptionsTestService(t *testing.T, dims int, nodes ...*storage.Node) *Service {
	t.Helper()
	engine := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "test")
	svc := NewService(engine, dims, nil)
	t.Cleanup(func() { _ = svc.Close() })
	for _, node := range nodes {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(node))
	}
	return svc
}

func pointNode(id string, vec []float32, props map[string]any) *storage.Node {
	return &storage.Node{ID: storage.NodeID(id), Labels: []string{"Point"}, Properties: props,
		NamedEmbeddings: map[string][]float32{"default": vec}}
}

// A query's own HNSW ef (SearchOptions.HNSWEf) reaches candidate generation
// through the public vector query, the path the Qdrant API's hnsw_ef takes; a
// query without one leaves the configured preset.
func TestVectorQueryNodesWithOptions_HNSWEfReachesCandidateGeneration(t *testing.T) {
	svc := newOptionsTestService(t, 2, pointNode("n1", []float32{1, 0}, nil))
	gen := &efRecordingGen{cands: []Candidate{{ID: nodeNamedVectorKey("n1", "default"), Score: 1}}}
	svc.pipelineMu.Lock()
	svc.vectorPipeline = NewVectorSearchPipeline(gen, NewCPUExactScorer(svc.vectorIndex))
	svc.pipelineMu.Unlock()
	spec := VectorQuerySpec{Label: "Point", Property: "default", Limit: 1, ScoreScale: VectorScoreSimilarity}

	hits, err := svc.VectorQueryNodesWithOptions(context.Background(), []float32{1, 0}, spec, &SearchOptions{HNSWEf: 77})
	require.NoError(t, err)
	require.Equal(t, 77, gen.ef)
	require.Len(t, hits, 1)
	require.Equal(t, "n1", hits[0].ID)

	_, err = svc.VectorQueryNodesWithOptions(context.Background(), []float32{1, 0}, spec, nil)
	require.NoError(t, err)
	require.Zero(t, gen.ef, "no request ef: the preset applies")
}

// The HNSW beam is the request's ef, widened only to hold the candidates asked
// for; without one it is the preset rule (EfSearch or limit × beam factor).
func TestHNSWCandidateGen_SearchBeam(t *testing.T) {
	cfg := DefaultHNSWConfig()
	cfg.EfSearch = 50
	cfg.SearchBeamFactor = 4
	gen := NewHNSWCandidateGen(NewHNSWIndex(4, cfg))
	ctx := context.Background()

	require.Equal(t, 400, gen.searchBeam(ctx, 100), "preset: limit × beam factor")
	require.Equal(t, 50, gen.searchBeam(ctx, 5), "preset: EfSearch")
	require.Equal(t, 16, gen.searchBeam(withHNSWEf(ctx, 16), 10), "request ef")
	require.Equal(t, 40, gen.searchBeam(withHNSWEf(ctx, 16), 40), "request ef widened to the candidates asked for")
	require.Equal(t, 50, gen.searchBeam(withHNSWEf(ctx, 0), 5), "ef 0 is no request ef")
}

// A request's own ef sets the HNSW search depth itself: the generator's
// preferred depth floor (at least 200 candidates) no longer widens it.
func TestResolveVectorAdaptiveOverfetch_RequestEfSkipsHNSWDepthFloor(t *testing.T) {
	pipeline := NewVectorSearchPipeline(NewHNSWCandidateGen(NewHNSWIndex(4, DefaultHNSWConfig())), nil)
	// As a vector query resolves its options (vectorQueryNodesIndexedWithOptions).
	opts := func(ef int) *SearchOptions {
		o := DefaultSearchOptions()
		o.Limit, o.CandidateTarget, o.MaxOverfetchRatio, o.HNSWEf = 10, 10, math.MaxFloat64, ef
		return o
	}
	require.GreaterOrEqual(t, resolveVectorAdaptiveOverfetch(opts(0), pipeline).initialLimit, 200)
	require.Less(t, resolveVectorAdaptiveOverfetch(opts(16), pipeline).initialLimit, 200)
}

// The raw similarity scale reports Qdrant's scores and Neo4j's scale is
// unchanged, on the indexed (cosine) and exact (euclidean) paths alike; the
// order of the hits is the same on both.
func TestVectorQueryNodes_ScoreScale(t *testing.T) {
	svc := newOptionsTestService(t, 2,
		pointNode("same", []float32{1, 0}, nil),
		pointNode("orthogonal", []float32{0, 1}, nil))
	ctx := context.Background()
	query := []float32{1, 0}
	scores := func(similarity string, scale VectorScoreScale) map[string]float64 {
		hits, err := svc.VectorQueryNodes(ctx, query, VectorQuerySpec{Label: "Point", Property: "default", Similarity: similarity, Limit: 2, ScoreScale: scale})
		require.NoError(t, err)
		require.Len(t, hits, 2)
		require.Equal(t, "same", hits[0].ID)
		out := map[string]float64{}
		for _, hit := range hits {
			out[hit.ID] = hit.Score
		}
		return out
	}

	raw := scores("cosine", VectorScoreSimilarity)
	require.InDelta(t, 1.0, raw["same"], 1e-6)
	require.InDelta(t, 0.0, raw["orthogonal"], 1e-6)
	neo4j := scores("cosine", VectorScoreNeo4j)
	require.InDelta(t, 1.0, neo4j["same"], 1e-6)
	require.InDelta(t, 0.5, neo4j["orthogonal"], 1e-6, "(1 + cos) / 2")

	rawEuclid := scores("euclidean", VectorScoreSimilarity)
	require.InDelta(t, 1/(1+1.4142135), rawEuclid["orthogonal"], 1e-5, "1 / (1 + d)")
	neo4jEuclid := scores("euclidean", VectorScoreNeo4j)
	require.InDelta(t, 1.0/3.0, neo4jEuclid["orthogonal"], 1e-5, "1 / (1 + d²)")
}

// Where admits candidate nodes before the top-K cut: a selective filter finds
// a node far from the query among many close ones, on the indexed (cosine)
// and exact (dot) paths.
func TestVectorQueryNodesWithOptions_WhereFindsSelectiveMatch(t *testing.T) {
	nodes := make([]*storage.Node, 0, 301)
	for i := 0; i < 300; i++ {
		nodes = append(nodes, pointNode(fmt.Sprintf("close-%03d", i), []float32{1, float32(i) / 1000, 0, 0}, map[string]any{"tag": "common"}))
	}
	nodes = append(nodes, pointNode("rare", []float32{0, 0, 0, 1}, map[string]any{"tag": "rare"}))
	svc := newOptionsTestService(t, 4, nodes...)
	opts := &SearchOptions{Where: func(node *storage.Node) bool { return node.Properties["tag"] == "rare" }}

	for _, similarity := range []string{"cosine", "dot"} {
		hits, err := svc.VectorQueryNodesWithOptions(context.Background(), []float32{1, 0, 0, 0},
			VectorQuerySpec{Label: "Point", Property: "default", Similarity: similarity, Limit: 1, ScoreScale: VectorScoreSimilarity}, opts)
		require.NoError(t, err, similarity)
		require.Len(t, hits, 1, similarity)
		require.Equal(t, "rare", hits[0].ID, similarity)
	}
}

func TestVectorQueryExactBypassesApproximateGenerator(t *testing.T) {
	svc := newOptionsTestService(t, 2, pointNode("best", []float32{1, 0}, nil), pointNode("other", []float32{0, 1}, nil))
	gen := &efRecordingGen{cands: []Candidate{{ID: nodeNamedVectorKey("other", "default"), Score: 0}}}
	svc.pipelineMu.Lock()
	svc.vectorPipeline = NewVectorSearchPipeline(gen, NewCPUExactScorer(svc.vectorIndex))
	svc.pipelineMu.Unlock()
	spec := VectorQuerySpec{Property: "default", Limit: 1, ScoreScale: VectorScoreSimilarity}
	// The deliberately incomplete generator models ANN's bounded candidate set.
	hits, err := svc.VectorQueryNodes(context.Background(), []float32{1, 0}, spec)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, "other", hits[0].ID)
	// An exact request must evaluate the complete indexed population.
	spec.Exact = true
	hits, err = svc.VectorQueryNodes(context.Background(), []float32{1, 0}, spec)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, "best", hits[0].ID)
	require.Equal(t, 1, gen.calls)
}

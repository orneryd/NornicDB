package search

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// restrictedCorpus is a library where hidden items rank ahead of visible
// ones everywhere a restricted search could let them: hidden terms sort
// ahead in the prefix lexicon ("prefixaa…" before "prefixzzz"), hidden
// documents share the visible ones' terms (BM25 statistics), and hidden
// vectors lie nearer the query (vector ranks, and so reciprocal ranks).
var restrictedCorpus = struct {
	nodes         map[string]string
	nodeVectors   map[string][]float32
	relationships map[string][3]string // id -> start, end, reason
	hidden        map[string]bool
}{
	nodes: map[string]string{
		"v1": "alpha prefixzzz household budget",
		"v2": "beta alpha household",
		"v3": "gamma river household",
		"h1": "alpha alpha prefixaaa",
		"h2": "alpha beta prefixaab household",
		"h3": "alpha household household",
	},
	nodeVectors: map[string][]float32{
		"v1": {0.8, 0.6, 0}, "v2": {0.6, 0.8, 0}, "v3": {0, 1, 0},
		"h1": {1, 0, 0}, "h2": {0.99, 0.14, 0}, "h3": {0.97, 0, 0.24},
	},
	relationships: map[string][3]string{
		"rv1": {"v1", "v2", "alpha prefixzzz bridge household"},
		"rv2": {"v2", "v3", "beta river household"},
		"rv3": {"v1", "v3", "gamma span"},
		"rh1": {"h1", "h2", "alpha alpha prefixaaa bridge"},
		"rh2": {"h2", "h3", "alpha prefixaab bridge bridge household"},
	},
	hidden: map[string]bool{"h1": true, "h2": true, "h3": true, "rh1": true, "rh2": true},
}

// restrictedCorpusService is a service over the corpus: every item, or with
// visibleOnly the visible items alone (the reference a restricted search must
// equal).
func restrictedCorpusService(t *testing.T, visibleOnly bool) *Service {
	t.Helper()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	engine := storage.NewNamespacedEngine(base, "test")
	corpus := restrictedCorpus
	for id, text := range corpus.nodes {
		if visibleOnly && corpus.hidden[id] {
			continue
		}
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"},
			Properties: map[string]any{"content": text}, ChunkEmbeddings: [][]float32{corpus.nodeVectors[id]}})
		require.NoError(t, err)
	}
	relationships := 0
	for id, edge := range corpus.relationships {
		if visibleOnly && corpus.hidden[id] {
			continue
		}
		require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID(id), StartNode: storage.NodeID(edge[0]), EndNode: storage.NodeID(edge[1]),
			Type: "LINKS", Properties: map[string]any{"reason": edge[2]}}))
		relationships++
	}
	svc := startRelationshipService(t, engine, (&countingEmbed{}).embed, nil)
	nodes, err := engine.AllNodes()
	require.NoError(t, err)
	for _, node := range nodes {
		require.NoError(t, svc.IndexNode(node))
	}
	waitRelationshipVectors(t, svc, relationships)
	return svc
}

// comparable is a result's ranking: its identity, scores and ranks.
func comparableResults(results []SearchResult) []string {
	out := make([]string, 0, len(results))
	for _, r := range results {
		round := func(v float64) float64 { return math.Round(v*1e9) / 1e9 }
		out = append(out, fmt.Sprintf("%s score=%v rrf=%v sim=%v vector=%d bm25=%d",
			r.ID, round(r.Score), round(r.RRFScore), round(r.Similarity), r.VectorRank, r.BM25Rank))
	}
	return out
}

// A restricted search is the search of a corpus holding only what the
// reader may see: for node, relationship and graph search, on every BM25
// engine (V1, V2 exact, V2 bounded prefix), keyword, vector and hybrid, it
// returns the same results with the same scores, ranks and order, and the
// same top-K, as a service whose corpus is the visible items alone. Hidden
// items rank ahead of visible ones in every population a search builds
// (prefix plan, BM25 statistics, vector ranks, reciprocal ranks), so any
// population built before visibility applies shows up as a difference.
func TestRestrictedSearchEqualsVisibleOnlyCorpus(t *testing.T) {
	engines := map[string][2]string{ // name -> BM25 engine, prefix expansions
		"v1":                {"v1", "0"},
		"v2 exact":          {"v2", "0"},
		"v2 bounded prefix": {"v2", "1"},
	}
	modes := map[string]struct {
		query     string
		embedding []float32
	}{
		"keyword": {"alpha prefix household", nil},
		// Only a prefix expansion (prefixzzz) reaches the visible items.
		"prefix keyword": {"prefix", nil},
		"vector":         {"", []float32{1, 0, 0}},
		"hybrid":         {"alpha prefix household", []float32{1, 0, 0}},
	}
	consumers := map[string]func(*SearchOptions){
		"node":         func(*SearchOptions) {},
		"relationship": func(o *SearchOptions) { o.Relationships = true },
		"graph":        func(o *SearchOptions) { o.Graph = true },
	}
	for engineName, engine := range engines {
		t.Run(engineName, func(t *testing.T) {
			t.Setenv(EnvSearchBM25Engine, engine[0])
			t.Setenv("NORNICDB_BM25_PREFIX_MAX_EXPANSIONS", engine[1])
			t.Setenv("NORNICDB_BM25_PREFIX_MIN_LEN", "3")
			full := restrictedCorpusService(t, false)
			reference := restrictedCorpusService(t, true)
			hidden := restrictedCorpus.hidden
			for consumerName, configure := range consumers {
				for modeName, mode := range modes {
					for _, limit := range []int{1, 3, 10} {
						name := fmt.Sprintf("%s/%s/limit %d", consumerName, modeName, limit)
						options := func(restricted bool) *SearchOptions {
							opts := DefaultSearchOptions()
							opts.Limit = limit
							opts.MinSimilarity = nil
							opts.SkipMatches = true
							configure(opts)
							if restricted {
								opts.Restricted = &RestrictedReader{
									Node:         func(id string) bool { return !hidden[id] },
									Relationship: func(id string) bool { return !hidden[id] },
								}
							}
							return opts
						}
						ctx := context.Background()
						want, err := reference.Search(ctx, mode.query, mode.embedding, options(false))
						require.NoError(t, err, name)
						got, err := full.Search(ctx, mode.query, mode.embedding, options(true))
						require.NoError(t, err, name)
						require.Equal(t, comparableResults(want.Results), comparableResults(got.Results), name)
						for _, r := range got.Results {
							require.False(t, hidden[r.ID], "%s returned hidden %s", name, r.ID)
						}
					}
				}
			}
		})
	}
}

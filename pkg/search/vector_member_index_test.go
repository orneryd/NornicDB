package search

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"math"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func memberIndexService(t *testing.T) (*Service, *storage.Node) {
	t.Helper()
	t.Setenv(EnvSearchResultCacheEnabled, "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "100000")
	engine := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "nornic")
	svc := NewService(engine, 2, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	var candidates []Candidate
	for n := 0; n < 2*stageBatch; n++ {
		id := fmt.Sprintf("rival-%03d", n)
		node := &storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{.8, .6}}}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(node))
		candidates = append(candidates, Candidate{ID: id, Score: .8})
	}
	node := &storage.Node{ID: "winner", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{.1, float32(math.Sqrt(.99))}}, NamedEmbeddings: map[string][]float32{"best": {1, 0}}, Properties: map[string]interface{}{"embedding": []float32{.95, float32(math.Sqrt(1 - .95*.95))}}}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(node))
	candidates = append(candidates, Candidate{ID: "winner", Score: .1})
	svc.vectorPipeline = NewVectorSearchPipeline(memberBoundGenerator{candidates}, NewCPUExactScorer(svc.vectorIndex))
	return svc, node
}

func memberIndexSearch(svc *Service) (*SearchResponse, error) {
	opts := DefaultSearchOptions()
	threshold, fallback := .9, false
	opts.MinSimilarity, opts.FallbackEnabled = &threshold, &fallback
	opts.Limit, opts.MaxCandidateLimit = 10, 65
	return svc.Search(context.Background(), "", []float32{1, 0}, opts)
}

func TestMemberIndexTracksUpdatesAndPropertyRemoval(t *testing.T) {
	svc, node := memberIndexService(t)
	response, err := memberIndexSearch(svc)
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	require.Equal(t, "winner", response.Results[0].ID)
	require.InDelta(t, 1, response.Results[0].Similarity, 1e-6)
	node.NamedEmbeddings = nil
	require.NoError(t, svc.IndexNode(node))
	response, err = memberIndexSearch(svc)
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	require.InDelta(t, .95, response.Results[0].Similarity, 1e-6)
	svc.RemovePropertyVectorIndex("embedding")
	response, err = memberIndexSearch(svc)
	require.NoError(t, err)
	require.Empty(t, response.Results, "removed membership must not survive in the shared index")
	node.Properties = nil
	node.NamedEmbeddings = map[string][]float32{"replacement": {1, 0}}
	require.NoError(t, svc.IndexNode(node))
	response, err = memberIndexSearch(svc)
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	require.InDelta(t, 1, response.Results[0].Similarity, 1e-6)
	require.NoError(t, svc.RemoveNode(node.ID))
	response, err = memberIndexSearch(svc)
	require.NoError(t, err)
	require.Empty(t, response.Results)
	require.NoError(t, svc.ClearVectorIndex())
	require.Empty(t, svc.vectorMembers.nodes, "reset must retire the shared membership index")
	require.Empty(t, svc.vectorMembers.expanded)
}

func TestMemberIndexConcurrentSearchAndUpdates(t *testing.T) {
	svc, node := memberIndexService(t)
	errors := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for n := 0; n < 100; n++ {
			if _, err := memberIndexSearch(svc); err != nil {
				errors <- err
				return
			}
		}
	}()
	go func() {
		defer workers.Done()
		for n := 0; n < 100; n++ {
			node.NamedEmbeddings = nil
			if n%2 == 0 {
				node.NamedEmbeddings = map[string][]float32{"best": {1, 0}}
			}
			if err := svc.IndexNode(node); err != nil {
				errors <- err
				return
			}
		}
	}()
	workers.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	node.NamedEmbeddings = map[string][]float32{"final": {1, 0}}
	require.NoError(t, svc.IndexNode(node))
	response, err := memberIndexSearch(svc)
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	require.InDelta(t, 1, response.Results[0].Similarity, 1e-6)
}

// A retired chunk/named/property ID may surface after its node becomes a
// single main vector. The cheap rejection must never treat the alias score
// as a bound for that different, live member.
func TestMemberIndexWeakAliasStillExpandsCurrentMembers(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		for _, alias := range []string{nodeChunkVectorKey("winner", 1), nodeNamedVectorKey("winner", "old"), nodePropertyVectorKey("winner", "old")} {
			t.Run(fmt.Sprintf("mixed=%t/%s", mixed, alias), func(t *testing.T) {
				t.Setenv(EnvSearchResultCacheEnabled, "false")
				t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "100000")
				engine := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "nornic")
				svc := NewService(engine, 2, nil)
				t.Cleanup(func() { require.NoError(t, svc.Close()) })
				id := "winner"
				node := &storage.Node{ID: storage.NodeID(id), ChunkEmbeddings: [][]float32{{1, 0}}}
				_, err := engine.CreateNode(node)
				require.NoError(t, err)
				require.NoError(t, svc.IndexNode(node))
				if mixed {
					other := &storage.Node{ID: "unrelated", ChunkEmbeddings: [][]float32{{0, 1}, {0, 1}}}
					_, err := engine.CreateNode(other)
					require.NoError(t, err)
					require.NoError(t, svc.IndexNode(other))
				}
				score := .1
				svc.vectorPipeline = NewVectorSearchPipeline(memberBoundGenerator{[]Candidate{{ID: alias, Score: score}}}, NewCPUExactScorer(svc.vectorIndex))
				response, err := memberIndexSearch(svc)
				require.NoError(t, err)
				require.Len(t, response.Results, 1)
				require.Equal(t, id, response.Results[0].ID)
				require.InDelta(t, 1, response.Results[0].Similarity, 1e-6)
			})
		}
	}
}

// Canonical keys keep a literal suffix ID and a marker-prefixed ID distinct
// from another node's member. The hidden winning member must still expand.
func TestMemberIndexCanonicalEntryOwnership(t *testing.T) {
	for _, id := range []string{"winner", "winner-chunk-1", "\x00winner"} {
		t.Run(id, func(t *testing.T) {
			index := NewVectorIndex(2)
			main, best := nodeMainVectorKey(id), nodeNamedVectorKey(id, "best")
			require.NoError(t, index.Add(main, []float32{.1, float32(math.Sqrt(.99))}))
			require.NoError(t, index.Add(best, []float32{1, 0}))
			members := &vectorMemberIndex{nodes: map[string][]string{id: {main, best}}}
			members.setExpandedLocked(id, members.nodes[id])
			pipeline := NewVectorSearchPipeline(memberBoundGenerator{[]Candidate{{ID: main, Score: .1}}}, NewCPUExactScorer(index))
			got, err := pipeline.Search(withVectorMemberIndex(context.Background(), members), []float32{1, 0}, 1, .9)
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Equal(t, best, got[0].ID)
			require.InDelta(t, 1, got[0].Score, 1e-6)
			members.nodes[id] = []string{main}
			members.setExpandedLocked(id, members.nodes[id])
			require.NotContains(t, members.expanded, id, "a sole canonical main entry needs no expansion")
		})
	}
}

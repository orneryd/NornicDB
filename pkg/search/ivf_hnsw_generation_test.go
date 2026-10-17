package search

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Growth makes automatic k-means choose more clusters (22 for 1,000 vectors,
// 24 for 1,200). While the replacement generation's graphs are built, public
// queries keep the previous complete generation; once it is published they use
// the new routing and graphs together, including the new clusters.
func TestIVFHNSWPublicQueriesSpanGrowthDrivenReclustering(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_IVF_HNSW_ENABLED", "true")
	t.Setenv("NORNICDB_KMEANS_NPROBE", "64") // every query routes to every cluster
	const dimensions, initial, grown = 8, 1000, 200
	ctx := context.Background()
	rng := rand.New(rand.NewSource(1372))
	vectors := make(map[string][]float32, initial+grown)
	newNode := func(id string) *storage.Node {
		vec := make([]float32, dimensions)
		for d := range vec {
			vec[d] = float32(rng.NormFloat64())
		}
		vectors[id] = vec
		return &storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, NamedEmbeddings: map[string][]float32{"default": vec}}
	}
	engine := newNamespacedEngine(t)
	for i := 0; i < initial; i++ {
		_, err := engine.CreateNode(newNode(fmt.Sprintf("initial-%04d", i)))
		require.NoError(t, err)
	}
	svc := NewServiceWithDimensions(engine, dimensions)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	svc.EnableClustering(nil, 0)
	svc.SetMinEmbeddingsForClustering(1)
	require.NoError(t, svc.BuildIndexes(ctx))
	require.Equal(t, 22, svc.clusterIndex.NumClusters())
	pipeline, err := svc.getOrCreateVectorPipeline(ctx)
	require.NoError(t, err)
	require.IsType(t, &IVFHNSWCandidateGen{}, pipeline.candidateGen)

	for i := 0; i < grown; i++ {
		node := newNode(fmt.Sprintf("grown-%04d", i))
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(node))
	}

	// A wide rescoring pool keeps the check about graph membership in every
	// compression mode (1-bit codes of 8-D vectors tie heavily).
	membership := &SearchOptions{QuantizationOversampling: 64}
	findsItself := func(id string) {
		t.Helper()
		hits, err := svc.VectorQueryNodesWithOptions(ctx, vectors[id], VectorQuerySpec{Limit: 3}, membership)
		require.NoError(t, err, "query for %s", id)
		ids := make([]string, len(hits))
		for i, hit := range hits {
			ids[i] = hit.ID
		}
		require.Contains(t, ids, id)
	}

	building, release := make(chan struct{}), make(chan struct{})
	ivfHNSWBuildHook = func() {
		close(building)
		<-release
	}
	t.Cleanup(func() { ivfHNSWBuildHook = nil })
	done := make(chan error, 1)
	go func() { done <- svc.TriggerClustering(ctx) }()
	select {
	case <-building:
	case err := <-done:
		t.Fatalf("reclustering finished without building graphs: %v", err)
	case <-time.After(2 * time.Minute):
		t.Fatal("reclustering did not start building graphs")
	}
	// The 24-cluster generation exists but its graphs are not built yet.
	for i := 0; i < 40; i++ {
		findsItself(fmt.Sprintf("initial-%04d", i*25))
		candidates, err := svc.VectorSearchCandidates(ctx, vectors[fmt.Sprintf("grown-%04d", i)], DefaultSearchOptions())
		require.NoError(t, err)
		require.NotEmpty(t, candidates)
	}
	require.Equal(t, 22, svc.clusterIndex.NumClusters())
	close(release)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Minute):
		t.Fatal("reclustering did not finish")
	}

	require.Equal(t, 24, svc.clusterIndex.NumClusters())
	svc.clusterStateMu.RLock()
	require.Len(t, svc.clusterHNSW, 24)
	svc.clusterStateMu.RUnlock()
	pipeline, err = svc.getOrCreateVectorPipeline(ctx)
	require.NoError(t, err)
	require.IsType(t, &IVFHNSWCandidateGen{}, pipeline.candidateGen)
	// The new generation's graphs hold the vectors added before reclustering.
	for i := 40; i < grown; i += 4 {
		findsItself(fmt.Sprintf("grown-%04d", i))
	}
}

// A dimension change (first vector of a new dimension while the in-memory
// index is empty, as with file-backed vectors) can replace the cluster index
// while a clustering generation is built. That generation describes the
// retired population and is discarded, never published.
func TestIVFHNSWGenerationForReplacedClusterIndexIsDiscarded(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_IVF_HNSW_ENABLED", "true")
	const dimensions = 4
	ctx := context.Background()
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), dimensions)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
	svc.SetPersistenceEnabled(true)
	svc.EnableClustering(nil, 2)
	svc.SetMinEmbeddingsForClustering(1)
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 40; i++ {
		vec := make([]float32, dimensions)
		for d := range vec {
			vec[d] = float32(rng.NormFloat64())
		}
		require.NoError(t, svc.IndexNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("n-%02d", i)), ChunkEmbeddings: [][]float32{vec}}))
	}
	original := svc.clusterIndex
	require.Zero(t, svc.vectorIndex.Count(), "file-backed vectors leave the in-memory index empty")

	ivfHNSWBuildHook = func() { svc.maybeAutoSetVectorDimensions(dimensions + 1) }
	t.Cleanup(func() { ivfHNSWBuildHook = nil })
	require.NoError(t, svc.TriggerClustering(ctx))

	require.NotSame(t, original, svc.clusterIndex, "the dimension change replaced the cluster index")
	require.False(t, svc.clusterIndex.IsClustered())
	svc.clusterStateMu.RLock()
	require.Nil(t, svc.clusterHNSW, "the superseded generation's graphs are not published")
	svc.clusterStateMu.RUnlock()
	svc.clusterLexicalMu.RLock()
	require.Empty(t, svc.clusterLexicalProfiles)
	svc.clusterLexicalMu.RUnlock()
}

// Every replacement or clearing of the cluster generation takes
// clusterStateMu, which the installer holds from its identity check through
// publication. A replacement that starts after the check therefore waits for
// the publication and then retires it; the retired index is never left
// published (sol r9: TestSolR9ReplacementAfterCheck).
func TestClusterGenerationReplacementAfterCheckRetiresPublication(t *testing.T) {
	for _, path := range []struct {
		name      string
		replace   func(*Service) error
		retired   bool // the computed-for index itself is cleared or replaced
		replacing bool // clusterIndex points to a new index afterwards
	}{
		{name: "dimension change", replace: func(s *Service) error { s.maybeAutoSetVectorDimensions(5); return nil }, retired: true, replacing: true},
		{name: "clustering mode switch", replace: func(s *Service) error {
			s.mu.Lock()
			s.clusterUsesGPU = true // the next EnableClustering(nil) switches back to CPU
			s.mu.Unlock()
			s.EnableClustering(nil, 1)
			return nil
		}, replacing: true},
		{name: "ClearVectorIndex", replace: func(s *Service) error { return s.ClearVectorIndex() }, retired: true},
		{name: "ClearIndexes", replace: func(s *Service) error { return s.ClearIndexes() }, retired: true},
		{name: "build reset", replace: func(s *Service) error { s.resetANNForBuild(); return nil }},
	} {
		t.Run(path.name, func(t *testing.T) {
			t.Setenv("NORNICDB_VECTOR_IVF_HNSW_ENABLED", "true")
			svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			svc.EnableClustering(nil, 1)
			original := svc.clusterIndex
			require.NoError(t, original.Add("a", []float32{1, 0, 0, 0}))
			clustering, err := original.ComputeClustering(context.Background())
			require.NoError(t, err)

			replaced := make(chan error, 1)
			clusterPublishHook = func() {
				go func() { replaced <- path.replace(svc) }()
				// The replacement waits for this publication.
				require.Never(t, func() bool { return len(replaced) > 0 }, 200*time.Millisecond, 10*time.Millisecond)
			}
			t.Cleanup(func() { clusterPublishHook = nil })
			require.NoError(t, svc.installClusterGeneration(context.Background(), original, clustering, false))
			select {
			case err := <-replaced:
				require.NoError(t, err)
			case <-time.After(30 * time.Second):
				t.Fatal("the replacement did not finish after publication")
			}

			svc.clusterStateMu.RLock()
			defer svc.clusterStateMu.RUnlock()
			require.Nil(t, svc.clusterHNSW, "the published graphs retire with the generation")
			if path.retired {
				require.False(t, original.IsClustered(), "a retired index is not left published")
			}
			if path.replacing {
				require.NotSame(t, original, svc.clusterIndex)
				require.False(t, svc.clusterIndex.IsClustered())
				svc.clusterLexicalMu.RLock()
				require.Empty(t, svc.clusterLexicalProfiles)
				svc.clusterLexicalMu.RUnlock()
			}
		})
	}
}

// pauseAfterSnapshotContext pauses clustering right after k-means copied the
// population and released the index (the third Err call of a TriggerClustering
// run: admission check, check under the index locks, first initializer check).
type pauseAfterSnapshotContext struct {
	context.Context
	calls          atomic.Int32
	paused, resume chan struct{}
}

func (c *pauseAfterSnapshotContext) Err() error {
	if c.calls.Add(1) == 3 {
		close(c.paused)
		<-c.resume
	}
	return c.Context.Err()
}

func graphHolds(index *HNSWIndex, id string) bool {
	index.mu.RLock()
	defer index.mu.RUnlock()
	internal, ok := index.idToInternal[id]
	return ok && !index.deleted[internal]
}

// Vectors admitted or removed while a generation is computed or its graphs
// are built reach the published membership and graphs together: an added
// vector is a member, is in its cluster's graph and is found by a public
// query; a removed vector is in neither (sol r8:
// TestReviewR8ReclusterIncludesAdmissionDuringCompute).
func TestIVFHNSWReclusteringReconcilesAdmissionsDuringCompute(t *testing.T) {
	for _, phase := range []string{"compute", "build"} {
		for _, mutation := range []string{"add", "remove"} {
			t.Run(phase+"/"+mutation, func(t *testing.T) {
				t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
				t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
				t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
				t.Setenv("NORNICDB_VECTOR_IVF_HNSW_ENABLED", "true")
				t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", "none")
				t.Setenv("NORNICDB_KMEANS_NPROBE", "64")
				const dims = 8
				engine := newNamespacedEngine(t)
				rng := rand.New(rand.NewSource(137208))
				for i := 0; i < 1000; i++ {
					vec := make([]float32, dims)
					for d := range vec {
						vec[d] = float32(rng.NormFloat64())
					}
					_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("seed-%04d", i)), ChunkEmbeddings: [][]float32{vec}})
					require.NoError(t, err)
				}
				svc := NewServiceWithDimensions(engine, dims)
				t.Cleanup(func() { require.NoError(t, svc.Close()) })
				svc.EnableClustering(nil, 0)
				svc.SetMinEmbeddingsForClustering(1)
				require.NoError(t, svc.BuildIndexes(context.Background()))
				require.Equal(t, 22, svc.clusterIndex.NumClusters())

				late := &storage.Node{ID: "late", ChunkEmbeddings: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}
				mutate := func() error {
					if mutation == "add" {
						if _, err := engine.CreateNode(late); err != nil {
							return err
						}
						return svc.IndexNode(late)
					}
					return svc.RemoveNode("seed-0007")
				}
				var mutateErr error
				ctx := &pauseAfterSnapshotContext{Context: context.Background(), paused: make(chan struct{}), resume: make(chan struct{})}
				if phase == "build" {
					close(ctx.resume) // no pause in compute
					ctx.calls.Store(-1 << 20)
					ivfHNSWBuildHook = func() { mutateErr = mutate() }
					t.Cleanup(func() { ivfHNSWBuildHook = nil })
				}
				done := make(chan error, 1)
				go func() { done <- svc.TriggerClustering(ctx) }()
				if phase == "compute" {
					select {
					case <-ctx.paused:
					case err := <-done:
						t.Fatalf("clustering finished without pausing after its snapshot: %v", err)
					case <-time.After(time.Minute):
						t.Fatal("clustering did not reach its snapshot")
					}
					mutateErr = mutate()
					close(ctx.resume)
				}
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(2 * time.Minute):
					t.Fatal("reclustering did not finish")
				}
				require.NoError(t, mutateErr)

				id := string(late.ID)
				if mutation == "remove" {
					id = "seed-0007"
				}
				member := false
				for cid := 0; cid < svc.clusterIndex.NumClusters(); cid++ {
					for _, m := range svc.clusterIndex.GetClusterMemberIDsForCluster(cid) {
						member = member || m == id
					}
				}
				svc.clusterStateMu.RLock()
				inGraph := false
				for _, graph := range svc.clusterHNSW {
					inGraph = inGraph || graphHolds(graph, id)
				}
				svc.clusterStateMu.RUnlock()
				if mutation == "remove" {
					require.False(t, member, "a removed vector leaves the published membership")
					require.False(t, inGraph, "a removed vector leaves the published graphs")
					return
				}
				require.True(t, member, "an admitted vector joins the published membership")
				require.True(t, inGraph, "an admitted vector joins its cluster's published graph")
				hits, err := svc.VectorQueryNodesWithOptions(context.Background(), late.ChunkEmbeddings[0], VectorQuerySpec{Limit: 5}, &SearchOptions{HNSWEf: 256})
				require.NoError(t, err)
				ids := make([]string, len(hits))
				for i, hit := range hits {
					ids[i] = hit.ID
				}
				require.Contains(t, ids, id, "the published generation answers queries for the admitted vector")
			})
		}
	}
}

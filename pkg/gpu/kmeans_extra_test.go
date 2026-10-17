package gpu

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
)

func TestClusterIndexConfigReturnsCopyAndDefaultFallback(t *testing.T) {
	custom := &KMeansConfig{NumClusters: 7, MaxIterations: 11, InitMethod: "random"}
	ci := NewClusterIndex(nil, embConfig(2), custom)

	got := ci.Config()
	if got.NumClusters != 7 || got.MaxIterations != 11 || got.InitMethod != "random" {
		t.Fatalf("Config() = %+v, want custom values", got)
	}

	got.NumClusters = 99
	if ci.Config().NumClusters != 7 {
		t.Fatalf("Config() exposed mutable state")
	}

	ci.config = nil
	fallback := ci.Config()
	defaults := DefaultKMeansConfig()
	if !reflect.DeepEqual(fallback, *defaults) {
		t.Fatalf("Config() fallback = %+v, want %+v", fallback, *defaults)
	}
}

func TestClusterIndexRestoreClusteringState(t *testing.T) {
	t.Run("validates inputs", func(t *testing.T) {
		ci := NewClusterIndex(nil, embConfig(2), nil)
		if err := ci.RestoreClusteringState(nil, map[string]int{"a": 0}); err == nil {
			t.Fatalf("expected error for missing centroids")
		}
		if err := ci.RestoreClusteringState([][]float32{{1, 0}}, nil); err == nil {
			t.Fatalf("expected error for missing cluster map")
		}
		if err := ci.RestoreClusteringState([][]float32{{1, 0}}, map[string]int{"a": 0}); err == nil {
			t.Fatalf("expected error before embeddings are loaded")
		}
		requireNoError(t, ci.Add("a", []float32{1, 0}))
		if err := ci.RestoreClusteringState([][]float32{{1, 0, 0}}, map[string]int{"a": 0}); err == nil {
			t.Fatalf("expected centroid dimension error")
		}
	})

	t.Run("restores assignments and copies centroids", func(t *testing.T) {
		ci := NewClusterIndex(nil, embConfig(2), nil)
		requireNoError(t, ci.AddBatch(
			[]string{"a", "b", "c"},
			[][]float32{{1, 0}, {0, 1}, {0.2, 0.8}},
		))

		centroids := [][]float32{{1, 0}, {0, 1}}
		requireNoError(t, ci.RestoreClusteringState(centroids, map[string]int{"a": 0, "b": 1}))

		if !ci.IsClustered() {
			t.Fatalf("expected restored index to be clustered")
		}
		if got := ci.NumClusters(); got != 2 {
			t.Fatalf("NumClusters() = %d, want 2", got)
		}
		if got := ci.GetClusterMemberIDsForCluster(0); !reflect.DeepEqual(got, []string{"a", "c"}) {
			t.Fatalf("cluster 0 members = %v, want [a c]", got)
		}
		if got := ci.GetClusterMemberIDs([]int{1, 99}); !reflect.DeepEqual(got, []string{"b"}) {
			t.Fatalf("cluster 1 members = %v, want [b]", got)
		}

		gotCentroids := ci.GetCentroids()
		if !reflect.DeepEqual(gotCentroids, centroids) {
			t.Fatalf("GetCentroids() = %v, want %v", gotCentroids, centroids)
		}
		gotCentroids[0][0] = 42
		if ci.GetCentroids()[0][0] != 1 {
			t.Fatalf("GetCentroids() did not return a deep copy")
		}
	})
}

func TestClusterIndexAssignmentAndCentroidHelpers(t *testing.T) {
	ci := NewClusterIndex(nil, embConfig(2), nil)
	ci.nodeIDs = []string{"a", "b", "c"}
	ci.cpuVectors = []float32{1, 0, 0.8, 0, 0, 1}
	ci.centroids = [][]float32{{1, 0}, {0, 1}}
	ci.assignments = []int{1, 1, 0}

	if changed := ci.assignToCentroids(); changed != 3 {
		t.Fatalf("assignToCentroids changed %d assignments, want 3", changed)
	}
	if !reflect.DeepEqual(ci.assignments, []int{0, 0, 1}) {
		t.Fatalf("assignments = %v, want [0 0 1]", ci.assignments)
	}

	ci.updateCentroids()
	assertFloat32Near(t, ci.centroids[0][0], 0.9)
	assertFloat32Near(t, ci.centroids[0][1], 0)
	assertFloat32Near(t, ci.centroids[1][0], 0)
	assertFloat32Near(t, ci.centroids[1][1], 1)

	ci.buildClusterMap()
	if !reflect.DeepEqual(ci.clusterMap, map[int][]int{0: {0, 1}, 1: {2}}) {
		t.Fatalf("clusterMap = %v", ci.clusterMap)
	}

	ci.removeFromClusterMap(0, 1)
	if !reflect.DeepEqual(ci.clusterMap[0], []int{0}) {
		t.Fatalf("cluster 0 after removal = %v, want [0]", ci.clusterMap[0])
	}
	ci.removeFromClusterMap(0, 99)
	if !reflect.DeepEqual(ci.clusterMap[0], []int{0}) {
		t.Fatalf("missing removal changed cluster 0: %v", ci.clusterMap[0])
	}
	ci.addToClusterMap(1, 1)
	if !reflect.DeepEqual(ci.clusterMap[1], []int{2, 1}) {
		t.Fatalf("cluster 1 after add = %v, want [2 1]", ci.clusterMap[1])
	}
}

func TestClusterIndexUpdateCentroidsWithBufferKeepsEmptyClusters(t *testing.T) {
	ci := NewClusterIndex(nil, embConfig(2), nil)
	ci.nodeIDs = []string{"a", "b"}
	ci.cpuVectors = []float32{1, 3, 5, 7}
	ci.assignments = []int{0, 1}
	ci.centroids = [][]float32{{0, 0}, {0, 0}, {9, 9}}

	sums := [][]float64{{100, 100}, {100, 100}, {100, 100}}
	counts := []int{8, 8, 8}
	ci.updateCentroidsWithBuffer(sums, counts)

	if !reflect.DeepEqual(counts, []int{1, 1, 0}) {
		t.Fatalf("counts = %v, want [1 1 0]", counts)
	}
	if !reflect.DeepEqual(ci.centroids, [][]float32{{1, 3}, {5, 7}, {9, 9}}) {
		t.Fatalf("centroids = %v", ci.centroids)
	}
}

func TestClusterIndexSearchWithClustersEmptyClusterState(t *testing.T) {
	ci := NewClusterIndex(nil, embConfig(2), nil)
	ci.clustered = true

	results, err := ci.SearchWithClusters([]float32{1, 0}, 5, 2)
	if err != nil {
		t.Fatalf("SearchWithClusters() error = %v", err)
	}
	if results != nil {
		t.Fatalf("SearchWithClusters() = %v, want nil when clustered state has no centroids", results)
	}
}

func TestEmbeddingIndexScoreSubsetCPUByIDs(t *testing.T) {
	ei := NewEmbeddingIndex(nil, embConfig(3))
	requireNoError(t, ei.AddBatch(
		[]string{"x", "y", "z"},
		[][]float32{{1, 0, 0}, {0, 1, 0}, {0.5, 0.5, 0}},
	))

	results := ei.scoreSubsetCPUByIDs([]string{"missing", "z", "y", "x"}, []float32{1, 0, 0})
	if len(results) != 3 {
		t.Fatalf("scoreSubsetCPUByIDs returned %d results, want 3", len(results))
	}
	if got := []string{results[0].ID, results[1].ID, results[2].ID}; !reflect.DeepEqual(got, []string{"x", "z", "y"}) {
		t.Fatalf("result order = %v, want [x z y]", got)
	}
	assertFloat32Near(t, results[0].Score, 1)
	assertFloat32Near(t, results[2].Score, 0)
}

func TestNormalizeFlatVectors(t *testing.T) {
	vectors := []float32{3, 4, 0, 0, 0, 0}
	normalizeFlatVectors(vectors, 3)
	assertFloat32Near(t, vectors[0], 0.6)
	assertFloat32Near(t, vectors[1], 0.8)
	assertFloat32Near(t, vectors[2], 0)
	if !reflect.DeepEqual(vectors[3:], []float32{0, 0, 0}) {
		t.Fatalf("zero vector changed to %v", vectors[3:])
	}

	invalid := []float32{3, 4, 5}
	normalizeFlatVectors(invalid, 0)
	if !reflect.DeepEqual(invalid, []float32{3, 4, 5}) {
		t.Fatalf("invalid dims changed vector to %v", invalid)
	}
	normalizeFlatVectors(invalid, 2)
	if !reflect.DeepEqual(invalid, []float32{3, 4, 5}) {
		t.Fatalf("non-divisible vector changed to %v", invalid)
	}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertFloat32Near(t *testing.T, got, want float32) {
	t.Helper()
	const tolerance = 0.00001
	if got < want-tolerance || got > want+tolerance {
		t.Fatalf("got %f, want %f", got, want)
	}
}

// A computed clustering changes nothing until it is published, so callers can
// build state from its membership first; embeddings added in between join
// their nearest centroid at publication.
func TestClusterIndexComputeThenPublishClustering(t *testing.T) {
	t.Run("empty index computes nothing", func(t *testing.T) {
		ci := NewClusterIndex(nil, embConfig(2), &KMeansConfig{NumClusters: 2, MaxIterations: 5, InitMethod: "random"})
		clustering, err := ci.ComputeClustering(nil)
		requireNoError(t, err)
		if clustering != nil {
			t.Fatalf("ComputeClustering() on an empty index = %+v, want nil", clustering)
		}
	})

	t.Run("cancelled context", func(t *testing.T) {
		ci := NewClusterIndex(nil, embConfig(2), &KMeansConfig{NumClusters: 2, MaxIterations: 5, InitMethod: "random"})
		requireNoError(t, ci.Add("a", []float32{1, 0}))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := ci.ComputeClustering(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("ComputeClustering() error = %v, want context.Canceled", err)
		}
		if ci.IsClustered() {
			t.Fatalf("a cancelled computation must not publish")
		}
	})

	t.Run("publishes routing and membership together", func(t *testing.T) {
		ci := NewClusterIndex(nil, embConfig(2), &KMeansConfig{NumClusters: 2, MaxIterations: 10, InitMethod: "kmeans++"})
		requireNoError(t, ci.AddBatch(
			[]string{"a1", "a2", "b1", "b2"},
			[][]float32{{1, 0}, {0.9, 0.1}, {0, 1}, {0.1, 0.9}},
		))
		clustering, err := ci.ComputeClustering(context.Background())
		requireNoError(t, err)
		if clustering.NumClusters() != 2 {
			t.Fatalf("NumClusters() = %d, want 2", clustering.NumClusters())
		}
		if ci.IsClustered() || ci.NumClusters() != 0 {
			t.Fatalf("computing must not publish")
		}
		var members []string
		for cid := 0; cid < clustering.NumClusters(); cid++ {
			ids := clustering.MemberIDs(cid)
			if len(ids) != 2 || ids[0][0] != ids[1][0] {
				t.Fatalf("cluster %d members = %v, want one topic", cid, ids)
			}
			members = append(members, ids...)
		}
		if len(members) != 4 {
			t.Fatalf("members = %v, want all four", members)
		}

		requireNoError(t, ci.Add("a3", []float32{0.95, 0.05}))
		ci.PublishClustering(clustering)
		if !ci.IsClustered() || ci.NumClusters() != 2 {
			t.Fatalf("published index: clustered=%t clusters=%d", ci.IsClustered(), ci.NumClusters())
		}
		stats := ci.ClusterStats()
		if stats.Iterations == 0 {
			t.Fatalf("a computed clustering publishes its iterations")
		}
		cluster := ci.FindNearestCentroid([]float32{1, 0})
		got := ci.GetClusterMemberIDsForCluster(cluster)
		if len(got) != 3 {
			t.Fatalf("cluster %d members = %v, want a1, a2 and the added a3", cluster, got)
		}
	})
}

// An unpublished clustering takes embeddings added or removed after its
// snapshot by node ID, and publication follows the index's current embeddings
// by ID, so a swap-delete removal in the index cannot misalign membership.
func TestClusteringReconcilesMembershipByID(t *testing.T) {
	ci := NewClusterIndex(nil, embConfig(2), &KMeansConfig{NumClusters: 2, MaxIterations: 10, InitMethod: "kmeans++"})
	requireNoError(t, ci.AddBatch(
		[]string{"a1", "a2", "b1", "b2"},
		[][]float32{{1, 0}, {0.9, 0.1}, {0, 1}, {0.1, 0.9}},
	))
	clustering, err := ci.ComputeClustering(context.Background())
	requireNoError(t, err)
	clusterOf := func(id string) int {
		for cid := 0; cid < clustering.NumClusters(); cid++ {
			for _, member := range clustering.MemberIDs(cid) {
				if member == id {
					return cid
				}
			}
		}
		return -1
	}
	aCluster, bCluster := clusterOf("a1"), clusterOf("b1")
	if aCluster < 0 || bCluster < 0 || aCluster == bCluster {
		t.Fatalf("clusters a=%d b=%d, want two distinct topics", aCluster, bCluster)
	}

	if _, ok := ci.AssignToClustering(clustering, "unknown"); ok {
		t.Fatalf("an id without an embedding cannot be assigned")
	}
	requireNoError(t, ci.Add("a3", []float32{0.95, 0.05}))
	if cid, ok := ci.AssignToClustering(clustering, "a3"); !ok || cid != aCluster {
		t.Fatalf("AssignToClustering(a3) = %d, %t; want %d", cid, ok, aCluster)
	}
	requireNoError(t, ci.Add("a2", []float32{0.05, 0.95})) // a2 moved next to b
	if cid, ok := ci.AssignToClustering(clustering, "a2"); !ok || cid != bCluster || clusterOf("a2") != bCluster {
		t.Fatalf("a moved embedding is reassigned once: cluster %d, ok %t", cid, ok)
	}
	if cid, ok := clustering.Remove("b2"); !ok || cid != bCluster {
		t.Fatalf("Remove(b2) = %d, %t; want %d", cid, ok, bCluster)
	}
	if _, ok := clustering.Remove("b2"); ok {
		t.Fatalf("removing an absent id is a no-op")
	}
	ci.Remove("b2") // swap-delete moves the last embedding into b2's slot

	ci.PublishClustering(clustering)
	if got := ci.GetClusterMemberIDsForCluster(aCluster); !reflect.DeepEqual(sortedIDs(got), []string{"a1", "a3"}) {
		t.Fatalf("cluster a members = %v, want [a1 a3]", got)
	}
	if got := ci.GetClusterMemberIDsForCluster(bCluster); !reflect.DeepEqual(sortedIDs(got), []string{"a2", "b1"}) {
		t.Fatalf("cluster b members = %v, want [a2 b1]", got)
	}
}

func sortedIDs(ids []string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
}

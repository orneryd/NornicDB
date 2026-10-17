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
		// c is not in the saved map: it joins its nearest centroid (1), not cluster 0.
		if got := ci.GetClusterMemberIDsForCluster(0); !reflect.DeepEqual(got, []string{"a"}) {
			t.Fatalf("cluster 0 members = %v, want [a]", got)
		}
		if got := ci.GetClusterMemberIDs([]int{1, 99}); !reflect.DeepEqual(got, []string{"b", "c"}) {
			t.Fatalf("cluster 1 members = %v, want [b c]", got)
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

// AssignedClustering places every embedding at the centroid nearest its
// current coordinates, without publishing.
func TestClusterIndexAssignedClustering(t *testing.T) {
	ci := NewClusterIndex(nil, embConfig(2), nil)
	if _, err := ci.AssignedClustering(nil, [][]float32{{1, 0}}); err == nil {
		t.Fatalf("expected error before embeddings are loaded")
	}
	requireNoError(t, ci.AddBatch([]string{"a", "b", "c"}, [][]float32{{1, 0}, {0, 1}, {0.9, 0.2}}))
	if _, err := ci.AssignedClustering(context.Background(), nil); err == nil {
		t.Fatalf("expected error without centroids")
	}
	if _, err := ci.AssignedClustering(context.Background(), [][]float32{{1, 0, 0}}); err == nil {
		t.Fatalf("expected centroid dimension error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ci.AssignedClustering(ctx, [][]float32{{1, 0}, {0, 1}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("AssignedClustering(cancelled) = %v, want context.Canceled", err)
	}
	c, err := ci.AssignedClustering(context.Background(), [][]float32{{1, 0}, {0, 1}})
	requireNoError(t, err)
	if got := sortedIDs(c.MemberIDs(0)); !reflect.DeepEqual(got, []string{"a", "c"}) {
		t.Fatalf("cluster 0 = %v, want [a c]", got)
	}
	if got := c.MemberIDs(1); !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatalf("cluster 1 = %v, want [b]", got)
	}
	if ci.IsClustered() {
		t.Fatalf("AssignedClustering must not publish")
	}
}

// Membership is keyed by node ID: moves and swap-removals keep every
// member's position index consistent.
func TestClusterMembershipByID(t *testing.T) {
	m := newClusterMembership(0)
	for i, id := range []string{"a", "b", "c", "d"} {
		if _, was := m.set(id, i%2); was {
			t.Fatalf("set(%s) reported a previous cluster", id)
		}
	}
	if prev, was := m.set("a", 1); !was || prev != 0 {
		t.Fatalf("moving a = %d, %t; want 0, true", prev, was)
	}
	if got := sortedIDs(m.ids(1)); !reflect.DeepEqual(got, []string{"a", "b", "d"}) {
		t.Fatalf("cluster 1 = %v", got)
	}
	if cluster, ok := m.remove("b"); !ok || cluster != 1 {
		t.Fatalf("remove(b) = %d, %t", cluster, ok)
	}
	if _, ok := m.remove("b"); ok {
		t.Fatalf("removing an absent id is a no-op")
	}
	for cluster, members := range m.members {
		for i, id := range members {
			if m.pos[id] != i || m.idToCluster[id] != cluster {
				t.Fatalf("%s: pos %d cluster %d, want %d %d", id, m.pos[id], m.idToCluster[id], i, cluster)
			}
		}
	}
	if _, ok := m.clusterOf("b"); ok {
		t.Fatalf("a removed id has no cluster")
	}
}

func TestClusterIndexSearchWithClustersEmptyClusterState(t *testing.T) {
	ci := NewClusterIndex(nil, embConfig(2), nil)
	ci.routing = &Clustering{membership: newClusterMembership(0)}

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

	if _, _, _, ok := ci.AssignToClustering(clustering, "unknown"); ok {
		t.Fatalf("an id without an embedding cannot be assigned")
	}
	requireNoError(t, ci.Add("a3", []float32{0.95, 0.05}))
	if _, was, cid, ok := ci.AssignToClustering(clustering, "a3"); !ok || was || cid != aCluster {
		t.Fatalf("AssignToClustering(a3) = %d, %t; want %d", cid, ok, aCluster)
	}
	requireNoError(t, ci.Add("a2", []float32{0.05, 0.95})) // a2 moved next to b
	if prev, was, cid, ok := ci.AssignToClustering(clustering, "a2"); !ok || !was || prev != aCluster || cid != bCluster || clusterOf("a2") != bCluster {
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

// A published clustering admits, moves and removes members live, by ID,
// through the same AssignToClustering/Remove an unpublished one uses; a
// superseded clustering keeps its routing.
func TestClusterIndexLiveMembership(t *testing.T) {
	ci := NewClusterIndex(nil, embConfig(2), &KMeansConfig{NumClusters: 2, MaxIterations: 10, InitMethod: "kmeans++"})
	requireNoError(t, ci.Add("a1", []float32{1, 0}))
	if ci.PublishedClustering() != nil {
		t.Fatalf("an unclustered index routes through no clustering")
	}
	requireNoError(t, ci.AddBatch([]string{"a2", "b1", "b2"}, [][]float32{{0.9, 0.1}, {0, 1}, {0.1, 0.9}}))
	requireNoError(t, ci.Cluster())
	published := ci.PublishedClustering()
	aCluster, _ := ci.ClusterOf("a1")
	bCluster, _ := ci.ClusterOf("b1")
	if aCluster == bCluster {
		t.Fatalf("two topics in one cluster")
	}

	requireNoError(t, ci.Add("a3", []float32{0.95, 0.05}))
	if prev, was, cid, ok := ci.AssignToClustering(published, "a3"); !ok || was || cid != aCluster {
		t.Fatalf("AssignToClustering(a3) = %d %t %d %t", prev, was, cid, ok)
	}
	if cid, _ := ci.ClusterOf("a3"); cid != aCluster {
		t.Fatalf("the index routes through the published clustering")
	}
	requireNoError(t, ci.Add("a3", []float32{0.05, 0.95})) // moves next to b
	if prev, was, cid, ok := ci.AssignToClustering(published, "a3"); !ok || !was || prev != aCluster || cid != bCluster {
		t.Fatalf("moving a3 = %d %t %d %t", prev, was, cid, ok)
	}
	if _, _, _, ok := ci.AssignToClustering(published, "unknown"); ok {
		t.Fatalf("an id without an embedding cannot be assigned")
	}
	if got := published.NearestClusters([]float32{0, 1}, 1); len(got) != 1 || got[0] != bCluster {
		t.Fatalf("NearestClusters = %v, want [%d]", got, bCluster)
	}
	if got := sortedIDs(published.MemberIDsOf([]int{bCluster})); !reflect.DeepEqual(got, []string{"a3", "b1", "b2"}) {
		t.Fatalf("MemberIDsOf = %v", got)
	}
	if cid, ok := published.Remove("a3"); !ok || cid != bCluster {
		t.Fatalf("Remove(a3) = %d %t", cid, ok)
	}

	if _, ok := ci.ClusterOf("a3"); ok {
		t.Fatalf("an unassigned id has no cluster")
	}
	if !ci.Remove("a1") {
		t.Fatalf("Remove(a1) found no embedding")
	}
	if _, ok := ci.ClusterOf("a1"); ok {
		t.Fatalf("a removed embedding leaves the membership")
	}
	if got := ci.GetClusterMembers([]int{aCluster}); len(got) != 1 {
		t.Fatalf("cluster a member indices = %v, want one (a2)", got)
	}
	stats := ci.ClusterStats()
	if stats.MinClusterSize != 1 || stats.MaxClusterSize != 2 {
		t.Fatalf("stats = %+v", stats)
	}

	// Publishing a newer clustering leaves the superseded one as it was.
	before := published.MemberIDsOf([]int{0, 1})
	requireNoError(t, ci.Cluster())
	if ci.PublishedClustering() == published {
		t.Fatalf("a new publication replaces the routing")
	}
	requireNoError(t, ci.Add("a4", []float32{1, 0}))
	ci.AssignToClustering(ci.PublishedClustering(), "a4")
	if _, ok := published.ClusterOf("a4"); ok {
		t.Fatalf("a superseded clustering receives no admissions")
	}
	if got := published.MemberIDsOf([]int{0, 1}); !reflect.DeepEqual(sortedIDs(got), sortedIDs(before)) {
		t.Fatalf("superseded membership changed: %v, was %v", got, before)
	}
}

// Live updates through OnNodeUpdate track members that change cluster, and
// UpdateCentroidsBatch moves those clusters' centroids to their members' mean.
func TestClusterIndexNodeUpdatesMoveCentroids(t *testing.T) {
	ci := NewClusterIndex(nil, embConfig(2), &KMeansConfig{NumClusters: 2, MaxIterations: 10, InitMethod: "kmeans++"})
	requireNoError(t, ci.OnNodeUpdate("a1", []float32{1, 0})) // unclustered: embedding only
	if _, ok := ci.ClusterOf("a1"); ok {
		t.Fatalf("an unclustered index has no membership")
	}
	requireNoError(t, ci.AddBatch([]string{"a2", "b1", "b2"}, [][]float32{{0.9, 0.1}, {0, 1}, {0.1, 0.9}}))
	requireNoError(t, ci.Cluster())
	if got := sortedIDs(ci.NodeIDs()); !reflect.DeepEqual(got, []string{"a1", "a2", "b1", "b2"}) {
		t.Fatalf("NodeIDs = %v", got)
	}
	aCluster, _ := ci.ClusterOf("a1")
	bCluster, _ := ci.ClusterOf("b1")

	requireNoError(t, ci.OnNodeUpdate("a2", []float32{0.05, 0.95})) // moves to b
	if cid, _ := ci.ClusterOf("a2"); cid != bCluster {
		t.Fatalf("a2 in cluster %d, want %d", cid, bCluster)
	}
	requireNoError(t, ci.OnNodeUpdate("b3", []float32{0, 1})) // new member: no move recorded
	if len(ci.pendingUpdates) != 1 {
		t.Fatalf("pending updates = %v, want one move", ci.pendingUpdates)
	}
	ci.UpdateCentroidsBatch()
	ci.UpdateCentroidsBatch() // nothing pending
	centroids := ci.GetCentroids()
	assertFloat32Near(t, centroids[aCluster][0], 1)
	assertFloat32Near(t, centroids[aCluster][1], 0)
	if centroids[bCluster][0] > 0.1 || centroids[bCluster][1] < 0.9 {
		t.Fatalf("cluster b centroid = %v, want near [0.04 0.96]", centroids[bCluster])
	}

	// A move out of a cluster that leaves it empty keeps its centroid; a
	// member whose embedding is gone does not count toward the mean's sum.
	ci.EmbeddingIndex.Remove("b3")
	requireNoError(t, ci.OnNodeUpdate("a1", []float32{0, 1}))
	ci.UpdateCentroidsBatch()
	assertFloat32Near(t, ci.GetCentroids()[aCluster][0], 1)
	stats := ci.ClusterStats()
	if stats.MinClusterSize != 5 || stats.MaxClusterSize != 5 {
		t.Fatalf("stats over the one populated cluster = %+v", stats)
	}

	ci.Clear()
	if ci.IsClustered() || len(ci.NodeIDs()) != 0 {
		t.Fatalf("Clear leaves an empty, unclustered index")
	}
	if _, ok := ci.ClusterOf("b1"); ok {
		t.Fatalf("Clear drops the membership")
	}
}

// An unpublished clustering answers membership by ID.
func TestClusteringClusterOf(t *testing.T) {
	c := newClustering([][]float32{{1, 0}, {0, 1}}, []string{"a", "b"}, []int{0, 1})
	if cid, ok := c.ClusterOf("b"); !ok || cid != 1 {
		t.Fatalf("ClusterOf(b) = %d %t", cid, ok)
	}
	if _, ok := c.ClusterOf("missing"); ok {
		t.Fatalf("an absent id has no cluster")
	}
}

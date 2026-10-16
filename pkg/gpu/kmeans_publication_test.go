package gpu

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// twoGroupClusterIndex holds a0..a3 near (1,0) and b0..b3 near (0,1). With
// random initialization over two well-separated groups, k=2 converges to the
// groups; tests identify clusters by membership, never by cluster ID.
func twoGroupClusterIndex(t *testing.T) *ClusterIndex {
	t.Helper()
	ci := NewClusterIndex(nil, embConfig(2), &KMeansConfig{NumClusters: 2, MaxIterations: 20, Tolerance: 0.001, InitMethod: "kmeans++"})
	for i := 0; i < 4; i++ {
		if err := ci.Add(fmt.Sprintf("a%d", i), []float32{1, float32(i) * 0.01}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ {
		if err := ci.Add(fmt.Sprintf("b%d", i), []float32{float32(i) * 0.01, 1}); err != nil {
			t.Fatal(err)
		}
	}
	return ci
}

// clusterOf returns the cluster listing id and how many clusters list it.
func clusterOf(ci *ClusterIndex, id string) (int, int) {
	found, count := -1, 0
	for cid := 0; cid < ci.NumClusters(); cid++ {
		for _, member := range ci.GetClusterMemberIDsForCluster(cid) {
			if member == id {
				found = cid
				count++
			}
		}
	}
	return found, count
}

func requireSameCluster(t *testing.T, ci *ClusterIndex, ids ...string) {
	t.Helper()
	want := -1
	for _, id := range ids {
		cid, count := clusterOf(ci, id)
		if count != 1 {
			t.Fatalf("%s is listed in %d clusters, want exactly 1", id, count)
		}
		if want == -1 {
			want = cid
		} else if cid != want {
			t.Fatalf("%s is in cluster %d, want %d with %v", id, cid, want, ids)
		}
	}
}

func requireAbsent(t *testing.T, ci *ClusterIndex, id string) {
	t.Helper()
	if _, count := clusterOf(ci, id); count != 0 {
		t.Fatalf("removed %s is still listed in %d clusters", id, count)
	}
}

func TestClusterPublicationRejectsClearedPopulation(t *testing.T) {
	ci := twoGroupClusterIndex(t)
	beforeClusterPublish = func(ci *ClusterIndex) {
		ci.Clear()
		_ = ci.Add("late", []float32{1, 0})
	}
	defer func() { beforeClusterPublish = nil }()

	err := ci.ClusterWithContext(context.Background())
	if !errors.Is(err, ErrClusteringSuperseded) {
		t.Fatalf("ClusterWithContext() error = %v, want ErrClusteringSuperseded", err)
	}
	if ci.IsClustered() {
		t.Fatal("assignments computed for the cleared population were published")
	}
	if got := ci.GetClusterMemberIDs([]int{0, 1}); len(got) != 0 {
		t.Fatalf("cleared population routes members %v", got)
	}
}

func TestClusterPublicationMapsAssignmentsByNodeID(t *testing.T) {
	ci := twoGroupClusterIndex(t)
	beforeClusterPublish = func(ci *ClusterIndex) {
		// Removing a0 swap-moves b3 into slot 0; late arrives after the copy.
		ci.EmbeddingIndex.Remove("a0")
		_ = ci.Add("late", []float32{0, 1})
	}
	defer func() { beforeClusterPublish = nil }()

	if err := ci.ClusterWithContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireAbsent(t, ci, "a0")
	requireSameCluster(t, ci, "a1", "a2", "a3")
	requireSameCluster(t, ci, "b0", "b1", "b2", "b3", "late")
	if a, _ := clusterOf(ci, "a1"); a == mustCluster(t, ci, "b0") {
		t.Fatal("separated groups share a cluster")
	}
}

func TestClusterRemovalKeepsMovedMembership(t *testing.T) {
	ci := twoGroupClusterIndex(t)
	if err := ci.ClusterWithContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	// A vector added after clustering has no assignment until it is moved.
	if err := ci.Add("late", []float32{0.02, 1}); err != nil {
		t.Fatal(err)
	}
	if !ci.Remove("a0") { // moves late into slot 0
		t.Fatal("Remove(a0) = false")
	}
	requireAbsent(t, ci, "a0")
	requireSameCluster(t, ci, "a1", "a2", "a3")
	requireSameCluster(t, ci, "b0", "b1", "b2", "b3", "late")

	if !ci.Remove("b1") { // moves b3 (assigned) into b1's slot
		t.Fatal("Remove(b1) = false")
	}
	requireAbsent(t, ci, "b1")
	requireSameCluster(t, ci, "b0", "b2", "b3", "late")
	requireSameCluster(t, ci, "a1", "a2", "a3")
}

func mustCluster(t *testing.T, ci *ClusterIndex, id string) int {
	t.Helper()
	cid, count := clusterOf(ci, id)
	if count != 1 {
		t.Fatalf("%s is listed in %d clusters", id, count)
	}
	return cid
}

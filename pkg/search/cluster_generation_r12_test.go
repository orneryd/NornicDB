package search

import (
	"context"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Regressions for sol r12 on F-1372-CLUSTER (adopted from the reviewer's
// probes; the probe each one comes from is named). The four guard bypass
// shapes of TestSolR12GuardBypasses are fixtures of
// TestClusterGenerationOwnerAllowList.

// cgAxis is the dimension of vec's largest coordinate: the fixture topic a
// vector is built around.
func cgAxis(vec []float32) int {
	axis := 0
	for d := range vec {
		if vec[d] > vec[axis] {
			axis = d
		}
	}
	return axis
}

// cgSquaredEuclidean is the distance the generation's nearest-centroid rules
// compare.
func cgSquaredEuclidean(a, b []float32) float64 {
	var sum float64
	for i := range a {
		diff := float64(a[i] - b[i])
		sum += diff * diff
	}
	return sum
}

// cgClusterPoint returns new coordinates for a member of cluster cid: the
// published centroid of cid displaced a quarter of the distance to the nearest
// other centroid, away from it. The generation therefore places them in cid by
// a margin under both of its nearest rules — routing (nearest populated
// cluster) and admission (nearest centroid, which may target an unused one) —
// while coordinates built by hand inside the member's own blob are not
// guaranteed to: k-means can converge with a blob split across two centroids,
// or leave a centroid at its seed point
// (TestClusterGenerationMoveCoordinatesFollowTheGeneration).
func cgClusterPoint(t *testing.T, routing *gpu.Clustering, cid int, current []float32) []float32 {
	t.Helper()
	centroids := routing.Centroids()
	require.GreaterOrEqual(t, cid, 0)
	require.Less(t, cid, len(centroids))
	require.GreaterOrEqual(t, len(centroids), 2, "the probe keeps clear of a second centroid")
	other := -1
	for c := range centroids {
		if c == cid {
			continue
		}
		if other < 0 || cgSquaredEuclidean(centroids[cid], centroids[c]) < cgSquaredEuclidean(centroids[cid], centroids[other]) {
			other = c
		}
	}
	vec := make([]float32, len(centroids[cid]))
	for d := range vec {
		vec[d] = centroids[cid][d] + 0.25*(centroids[cid][d]-centroids[other][d])
	}
	require.NotEmpty(t, vec)
	require.NotEqual(t, current, vec, "the move changes the member's coordinates")
	return vec
}

// moveClusterMember updates the first sorted member through the public write
// path, with new coordinates the generation's own routing places in `to`. A
// same-cluster move changes its coordinates without changing its owner; a
// cross-cluster move moves it to the destination.
func moveClusterMember(t *testing.T, f *cgFixture, from, to int) (string, []float32) {
	t.Helper()
	ids := f.svc.clusterIndex.GetClusterMemberIDsForCluster(from)
	sort.Strings(ids)
	require.NotEmpty(t, ids)
	require.NotEmpty(t, f.svc.clusterIndex.GetClusterMemberIDsForCluster(to))
	id := ids[0]
	routing := f.svc.clusterIndex.PublishedClustering()
	require.NotNil(t, routing)
	current, found := f.svc.getVectorLookup()(id)
	require.True(t, found)
	require.NotEmpty(t, current)
	vec := cgClusterPoint(t, routing, to, current)
	require.Equal(t, []int{to}, routing.NearestClusters(vec, 1), "routing places the new coordinates in the destination cluster")
	require.Equal(t, to, f.svc.clusterIndex.FindNearestCentroid(vec), "an admission places the new coordinates in the destination cluster")
	node, err := f.engine.GetNode(t.Context(), storage.NodeID(vectorOwnerForTest(t, id)))
	require.NoError(t, err)
	node.ChunkEmbeddings = [][]float32{vec}
	require.NoError(t, f.write(node))
	cid, found := f.svc.clusterIndex.ClusterOf(id)
	require.True(t, found)
	require.Equal(t, to, cid)
	return id, vec
}

// r12StoredCode is the graph's quantized code for id and the code its
// canonical vector encodes to.
func r12StoredCode(t *testing.T, f *cgFixture, id string) (stored, canonical []byte) {
	t.Helper()
	cid, ok := f.svc.clusterGen.routing.ClusterOf(id)
	require.True(t, ok)
	graph := f.svc.clusterGen.graphs[cid]
	vec, _ := f.svc.getVectorLookup()(id)
	graph.mu.RLock()
	internal := graph.idToInternal[id]
	q := graph.quantized
	require.NotNil(t, q, "the default policy quantizes cluster graphs")
	stored = append([]byte(nil), q.codes[int(internal)*q.stride:(int(internal)+1)*q.stride]...)
	cfg := q.config
	graph.mu.RUnlock()
	expected, err := newHNSWQuantizedVectors(context.Background(), cfg, 4, nil)
	require.NoError(t, err)
	expected.set(0, vec)
	return stored, expected.codes
}

// F1 (TestSolR12RestoreSameClusterCoordinates, TestSolR12RestoreSameClusterPublicQuery):
// a member whose vector changed after the save within the same cluster is
// re-inserted at restore with its current coordinates: its code encodes them
// and a public query for it still returns it first.
func TestClusterGenerationRestoreRefreshesRetainedCoordinates(t *testing.T) {
	f := newCGFixture(t, 20, false)
	t.Setenv("NORNICDB_KMEANS_NPROBE", "1")
	path := filepath.Join(t.TempDir(), "hnsw")
	require.NoError(t, SaveIVFHNSW(path, f.svc.clusterGen.graphs))
	cid := cgPopulatedClusters(t, f, 1)[0]
	id, vec := moveClusterMember(t, f, cid, cid)
	before, err := f.svc.VectorQueryNodesWithOptions(context.Background(), vec, VectorQuerySpec{Limit: 1}, nil)
	require.NoError(t, err)
	require.Len(t, before, 1)
	require.Equal(t, vectorOwnerForTest(t, id), before[0].ID)

	r11Restore(t, f, path)
	moved, ok := f.svc.clusterGen.routing.ClusterOf(id)
	require.True(t, ok)
	require.Equal(t, cid, moved)
	stored, canonical := r12StoredCode(t, f, id)
	require.Equal(t, canonical, stored, "the restored code encodes the member's current coordinates")
	after, err := f.svc.VectorQueryNodesWithOptions(context.Background(), vec, VectorQuerySpec{Limit: 1}, nil)
	require.NoError(t, err)
	require.Len(t, after, 1)
	require.Equal(t, vectorOwnerForTest(t, id), after[0].ID, "the public self-neighbour query keeps its top result after restore")
}

// Members whose vectors did not change are kept as saved (not re-inserted),
// so a restore costs only the changed members.
func TestClusterGenerationRestoreKeepsUnchangedMembers(t *testing.T) {
	f := newCGFixture(t, 20, false)
	path := filepath.Join(t.TempDir(), "hnsw")
	require.NoError(t, SaveIVFHNSW(path, f.svc.clusterGen.graphs))
	cid := cgPopulatedClusters(t, f, 1)[0]
	id, _ := moveClusterMember(t, f, cid, cid)
	saved, err := readIVFHNSWManifest(ivfHNSWDir(path))
	require.NoError(t, err)
	savedGraph, err := saved.loadCluster(cid, f.svc.getVectorLookup())
	require.NoError(t, err)
	stale := savedGraph.staleMembers(f.svc.getVectorLookup())
	require.Equal(t, []string{id}, stale, "only the moved member's saved coordinates are stale")
}

// The probes above move a member with coordinates the generation places in the
// destination cluster, under its routing rule and under the nearest-centroid
// rule an admission applies. That contract is what lets a probe assert the
// member's cluster after a write: k-means does not guarantee one centroid per
// blob, so coordinates built by hand inside the member's own blob can be placed
// in another cluster.
func TestClusterGenerationMoveCoordinatesFollowTheGeneration(t *testing.T) {
	f := newCGFixture(t, 20, false)
	routing := f.svc.clusterIndex.PublishedClustering()
	require.NotNil(t, routing)
	for cid := 0; cid < routing.NumClusters(); cid++ {
		ids := routing.MemberIDs(cid)
		if len(ids) == 0 {
			continue // an unused centroid owns no coordinates to move
		}
		current, found := f.svc.getVectorLookup()(ids[0])
		require.True(t, found)
		vec := cgClusterPoint(t, routing, cid, current)
		require.Equal(t, []int{cid}, routing.NearestClusters(vec, 1), "routing places a move's coordinates in its destination cluster")
		require.Equal(t, cid, routing.NearestCentroid(vec), "an admission places a move's coordinates in its destination cluster")
	}

	// A generation k-means can converge to: cluster 1 is a second centroid
	// inside cluster 0's blob (a split). Coordinates built by hand inside that
	// blob are placed in cluster 1, which is why the probes derive the
	// destination coordinates from the generation instead.
	crafted, err := f.svc.clusterIndex.AssignedClustering(context.Background(), cgSplitCentroids(t, f, routing))
	require.NoError(t, err)
	own := crafted.MemberIDs(0)
	require.NotEmpty(t, own, "the crafted generation keeps cluster 0 populated")
	sort.Strings(own)
	current, found := f.svc.getVectorLookup()(own[0])
	require.True(t, found)
	built := make([]float32, len(current))
	for d := range built {
		built[d] = 0.55
	}
	built[cgAxis(current)] = 1
	require.Equal(t, []int{1}, crafted.NearestClusters(built, 1),
		"the split centroid owns coordinates built inside cluster 0's blob")
	vec := cgClusterPoint(t, crafted, 0, current)
	require.Equal(t, []int{0}, crafted.NearestClusters(vec, 1))
	require.Equal(t, 0, crafted.NearestCentroid(vec))
}

// cgSplitCentroids derives, from the fixture's own vectors, the centroids of a
// generation k-means can converge to: cluster 0 is the blob on axis 0, cluster
// 1 is that blob's most off-axis member (a split), and clusters 2 and 3 are the
// blobs on those axes. The fixture seeds its vectors, so the shape is
// deterministic.
func cgSplitCentroids(t *testing.T, f *cgFixture, routing *gpu.Clustering) [][]float32 {
	t.Helper()
	lookup := f.svc.getVectorLookup()
	blobs := make(map[int][]string)
	for cid := 0; cid < routing.NumClusters(); cid++ {
		for _, id := range routing.MemberIDs(cid) {
			vec, found := lookup(id)
			require.True(t, found)
			axis := cgAxis(vec)
			blobs[axis] = append(blobs[axis], id)
		}
	}
	for axis := range blobs {
		sort.Strings(blobs[axis])
	}
	require.Len(t, blobs, 4, "the fixture builds one blob per topic")
	for _, axis := range []int{0, 1, 2, 3} {
		require.NotEmpty(t, blobs[axis], "blob %d is populated", axis)
	}
	split := blobs[0][0]
	for _, id := range blobs[0][1:] {
		if cgOffAxis(t, lookup, id, 0) > cgOffAxis(t, lookup, split, 0) {
			split = id
		}
	}
	return [][]float32{
		cgBlobCentroid(t, lookup, blobs[0]),
		cgVectorOf(t, lookup, split),
		cgBlobCentroid(t, lookup, blobs[2]),
		cgBlobCentroid(t, lookup, blobs[3]),
	}
}

// cgOffAxis is the sum of vec's coordinates off axis: how far it leans towards
// the other blobs.
func cgOffAxis(t *testing.T, lookup VectorLookup, id string, axis int) float64 {
	t.Helper()
	vec := cgVectorOf(t, lookup, id)
	sum := 0.0
	for d := range vec {
		if d != axis {
			sum += float64(vec[d])
		}
	}
	return sum
}

func cgVectorOf(t *testing.T, lookup VectorLookup, id string) []float32 {
	t.Helper()
	vec, found := lookup(id)
	require.True(t, found)
	require.NotEmpty(t, vec)
	return append([]float32(nil), vec...)
}

// cgBlobCentroid is the mean of ids' vectors.
func cgBlobCentroid(t *testing.T, lookup VectorLookup, ids []string) []float32 {
	t.Helper()
	require.NotEmpty(t, ids)
	var centroid []float32
	for _, id := range ids {
		vec := cgVectorOf(t, lookup, id)
		if centroid == nil {
			centroid = make([]float32, len(vec))
		}
		require.Len(t, vec, len(centroid))
		for d := range vec {
			centroid[d] += vec[d] / float32(len(ids))
		}
	}
	return centroid
}

// A graph saved without coordinate fingerprints is rebuilt rather than
// trusted at restore.
func TestClusterGenerationRestoreRebuildsUnfingerprintedGraphs(t *testing.T) {
	f := newCGFixture(t, 20, false)
	graphs := map[int]*HNSWIndex{}
	for cid := range f.svc.clusterGen.graphs {
		g := NewHNSWIndex(4, f.svc.hnswConfig())
		g.SetVectorLookup(f.svc.getVectorLookup())
		for _, id := range f.svc.clusterGen.routing.MemberIDs(cid) {
			vec, _ := f.svc.getVectorLookup()(id)
			require.NoError(t, g.Add(id, vec))
		}
		graphs[cid] = g
	}
	path := filepath.Join(t.TempDir(), "hnsw")
	require.NoError(t, SaveIVFHNSW(path, graphs))
	r11Restore(t, f, path)
	for cid, g := range f.svc.clusterGen.graphs {
		require.NotNil(t, g.fingerprints, "cluster %d was rebuilt (with fingerprints), not loaded", cid)
	}
}

// cgPanicAnalyzer panics when routing analyzes the query text.
type cgPanicAnalyzer struct{ Analyzer }

func (cgPanicAnalyzer) Analyze(string) []string { panic("routing analyzer") }

// F2 (TestSolR12RoutingPanicReleasesLock): a panic while a query routes (here
// in the configured analyzer) unwinds the routing lock and the query's hold
// on its generation, for every clustered generator.
func TestClusterGenerationRoutingPanicReleasesLock(t *testing.T) {
	for _, ivf := range []string{"true", "false"} {
		t.Run("ivf="+ivf, func(t *testing.T) {
			// More documents than a default search asks for, so it routes
			// through the selector rather than every cluster.
			f := newCGFixture(t, 100, false)
			t.Setenv("NORNICDB_VECTOR_IVF_HNSW_ENABLED", ivf)
			f.svc.publishClusterServing() // resolve the strategy under this setting
			vec, _ := f.svc.getVectorLookup()("n-0001")
			f.svc.bm25Analyzer = cgPanicAnalyzer{DefaultTextAnalyzer()}
			var caught any
			func() {
				defer func() { caught = recover() }()
				_, _ = f.svc.vectorSearchOnly(withQueryText(context.Background(), "alpha"), vec, DefaultSearchOptions())
			}()
			require.NotNil(t, caught, "the query panicked in routing")
			locked := !f.svc.clusterStateMu.TryLock()
			if locked {
				f.svc.clusterStateMu.RUnlock() // repair for cleanup
			} else {
				f.svc.clusterStateMu.Unlock()
			}
			require.False(t, locked, "a recovered routing panic leaves clusterStateMu unlocked")
			released := make(chan struct{})
			go func() {
				for f.svc.clusterGen.holders.Active() != 0 {
					time.Sleep(time.Millisecond)
				}
				close(released)
			}()
			select {
			case <-released:
			case <-time.After(10 * time.Second):
				t.Fatal("the panicking query kept its hold on the generation")
			}
		})
	}
}

// TestSolR12CancelledQueryReleasesHolder (control): a cancelled query
// releases its hold.
func TestClusterGenerationCancelledQueryReleasesHolder(t *testing.T) {
	f := newCGFixture(t, 20, false)
	vec, _ := f.svc.getVectorLookup()("n-0001")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := f.svc.vectorSearchOnly(ctx, vec, DefaultSearchOptions())
	require.ErrorIs(t, err, context.Canceled)
	released := make(chan struct{})
	go func() {
		for f.svc.clusterGen.holders.Active() != 0 {
			time.Sleep(time.Millisecond)
		}
		close(released)
	}()
	select {
	case <-released:
	case <-time.After(10 * time.Second):
		t.Fatal("the cancelled query kept its hold")
	}
}

// F3 (TestSolR12CloseRetiresGeneration): Close retires the generation and
// its index through the owner. A query still holding the generation keeps
// it; the index is released after that query finishes; nothing is published
// or admitted after Close, and Close stays idempotent.
func TestClusterGenerationCloseRetiresGeneration(t *testing.T) {
	f := newCGFixture(t, 20, false)
	var released atomic.Pointer[gpu.ClusterIndex]
	clusterIndexReleaseHook = func(ci *gpu.ClusterIndex) { released.Store(ci) }
	t.Cleanup(func() { clusterIndexReleaseHook = nil })
	index := f.svc.clusterIndex
	_, gen, release := f.svc.acquireClusterSnapshot(context.Background())
	require.NotNil(t, gen)

	require.NoError(t, f.svc.Close())
	f.svc.clusterStateMu.RLock()
	published, current := f.svc.clusterGen, f.svc.clusterIndex
	f.svc.clusterStateMu.RUnlock()
	require.Nil(t, published, "Close retires the generation")
	require.Nil(t, current, "Close leaves no cluster index")
	require.Never(t, func() bool { return released.Load() == index }, 200*time.Millisecond, 10*time.Millisecond,
		"the index stays while a query holds its generation")
	release()
	require.Eventually(t, func() bool { return released.Load() == index }, 5*time.Second, 10*time.Millisecond,
		"the index is released after the last holder")

	// After Close nothing reinstalls an index or a generation.
	f.svc.EnableClustering(nil, 4)
	require.Nil(t, f.svc.currentClusterIndex(), "a closed service enables no clustering")
	require.NoError(t, f.svc.Close(), "Close is idempotent")
}

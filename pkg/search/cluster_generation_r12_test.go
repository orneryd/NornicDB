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

// r12MoveWithinCluster saves the generation, then moves the first member of
// cluster 0 to a new position that stays in cluster 0; it returns the save
// path, the member's vector ID and its new vector.
func r12MoveWithinCluster(t *testing.T, f *cgFixture) (string, string, []float32) {
	t.Helper()
	ids := f.svc.clusterGen.routing.MemberIDs(0)
	sort.Strings(ids)
	id := ids[0]
	path := filepath.Join(t.TempDir(), "hnsw")
	require.NoError(t, SaveIVFHNSW(path, f.svc.clusterGen.graphs))
	old, _ := f.svc.getVectorLookup()(id)
	topic := 0
	for d := range old {
		if old[d] > old[topic] {
			topic = d
		}
	}
	vec := []float32{0.55, 0.55, 0.55, 0.55}
	vec[topic] = 1
	node, err := f.engine.GetNode(storage.NodeID(decodeVectorKey(id).node))
	require.NoError(t, err)
	node.ChunkEmbeddings = [][]float32{vec}
	require.NoError(t, f.write(node))
	require.Equal(t, 0, f.svc.clusterGen.routing.NearestClusters(vec, 1)[0])
	cid, _ := f.svc.clusterGen.routing.ClusterOf(id)
	require.Equal(t, 0, cid, "the move stays within cluster 0")
	return path, id, vec
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
	path, id, vec := r12MoveWithinCluster(t, f)
	before, err := f.svc.VectorQueryNodesWithOptions(context.Background(), vec, VectorQuerySpec{Limit: 1}, nil)
	require.NoError(t, err)
	require.Len(t, before, 1)
	require.Equal(t, decodeVectorKey(id).node, before[0].ID)

	r11Restore(t, f, path)
	cid, ok := f.svc.clusterGen.routing.ClusterOf(id)
	require.True(t, ok)
	require.Equal(t, 0, cid)
	stored, canonical := r12StoredCode(t, f, id)
	require.Equal(t, canonical, stored, "the restored code encodes the member's current coordinates")
	after, err := f.svc.VectorQueryNodesWithOptions(context.Background(), vec, VectorQuerySpec{Limit: 1}, nil)
	require.NoError(t, err)
	require.Len(t, after, 1)
	require.Equal(t, decodeVectorKey(id).node, after[0].ID, "the public self-neighbour query keeps its top result after restore")
}

// Members whose vectors did not change are kept as saved (not re-inserted),
// so a restore costs only the changed members.
func TestClusterGenerationRestoreKeepsUnchangedMembers(t *testing.T) {
	f := newCGFixture(t, 20, false)
	path, id, _ := r12MoveWithinCluster(t, f)
	saved, err := readIVFHNSWManifest(ivfHNSWDir(path))
	require.NoError(t, err)
	savedGraph, err := saved.loadCluster(0, f.svc.getVectorLookup())
	require.NoError(t, err)
	stale := savedGraph.staleMembers(f.svc.getVectorLookup())
	require.Equal(t, []string{id}, stale, "only the moved member's saved coordinates are stale")
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
			f := newCGFixture(t, 20, false)
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
			go func() { f.svc.clusterGen.holders.Wait(); close(released) }()
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
	go func() { f.svc.clusterGen.holders.Wait(); close(released) }()
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

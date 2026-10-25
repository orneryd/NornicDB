package search

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Regressions for sol r11 on F-1372-CLUSTER (adopted from the reviewer's
// probes; the probe each one comes from is named). Every owner input is the
// canonical population at the moment the owner applies it: the served
// fulltext (F1), the live persisted membership placed by current coordinates
// (F2), one captured population per saved generation (F3), the store's
// vector at admission (F4), the policy the graphs are in (F5), and the
// settings the persisted generation was built with (F6).

// F1 (TestSolR11FirstPersistentBuildKeepsLexicalProfiles): the first
// persistent build derives lexical state from the reloaded BM25.
func TestClusterGenerationFirstPersistentBuildKeepsLexicalProfiles(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_IVF_HNSW_ENABLED", "true")
	f := &cgFixture{t: t, engine: newNamespacedEngine(t), dir: t.TempDir(), rng: rand.New(rand.NewSource(1372))}
	f.svc = f.open(true)
	for topic := 0; topic < 4; topic++ {
		for i := 0; i < 20; i++ {
			f.next++
			_, err := f.engine.CreateNode(f.node(fmt.Sprintf("fresh-%02d", f.next), topic, cgTopics[topic]+" document"))
			require.NoError(t, err)
		}
	}
	require.NoError(t, f.svc.BuildIndexes(context.Background()))
	require.Equal(t, 80, f.svc.fulltext().Count())
	assertClusterGenerationCoherent(t, f.svc, cgAllChecks)
	view := cgView(f.svc)
	for cid := 0; cid < view.numClusters; cid++ {
		if len(f.svc.clusterIndex.GetClusterMemberIDsForCluster(cid)) == 0 {
			require.Empty(t, view.profiles[cid], "an empty cluster has no lexical population")
			continue
		}
		require.NotEmpty(t, view.profiles[cid], "cluster %d has a lexical profile after the first build", cid)
	}
}

// r11ClusterPair chooses distinct populated clusters in label order. K-means
// may leave a numbered centroid empty; these probes test cross-cluster state,
// not a requirement that every requested centroid own a document.
func r11ClusterPair(t *testing.T, f *cgFixture) (int, int) {
	t.Helper()
	var populated []int
	for cid := 0; cid < f.svc.clusterIndex.NumClusters(); cid++ {
		if len(f.svc.clusterIndex.GetClusterMemberIDsForCluster(cid)) > 0 {
			populated = append(populated, cid)
		}
	}
	require.GreaterOrEqual(t, len(populated), 2, "the probe requires two distinct live populations")
	return populated[0], populated[len(populated)-1]
}

// r11Context runs fn at its at-th Err call.
type r11Context struct {
	context.Context
	call, at int
	fn       func()
}

func (c *r11Context) Err() error {
	c.call++
	if c.call == c.at {
		c.fn()
	}
	return nil
}

// F3 (TestSolR11PersistenceSnapshotsOnePopulation): a node move while the
// generation's files are written leaves the saved generation one population,
// the one published when it was captured.
func TestClusterGenerationPersistenceSnapshotsOnePopulation(t *testing.T) {
	for _, path := range []struct {
		name string
		save func(f *cgFixture, ctx context.Context, hnswPath string)
	}{
		{"service persistence", func(f *cgFixture, ctx context.Context, hnswPath string) {
			f.svc.persistIVFHNSWBackground(ctx, hnswPath)
		}},
		{"SaveIVFHNSWWithContext", func(f *cgFixture, ctx context.Context, hnswPath string) {
			require.NoError(t, SaveIVFHNSWWithContext(ctx, hnswPath, f.svc.clusterGen.graphs))
		}},
	} {
		t.Run(path.name, func(t *testing.T) {
			f := newCGFixture(t, 20, false)
			f.t = t
			before := cgView(f.svc).membership
			var moved string
			from, to := r11ClusterPair(t, f)
			ctx := &r11Context{Context: context.Background(), at: 4, fn: func() { moved, _ = moveClusterMember(t, f, from, to) }}
			dir := t.TempDir()
			path.save(f, ctx, filepath.Join(dir, "hnsw"))
			require.NotEmpty(t, moved, "the move ran between cluster files")
			persisted, duplicates := cgPersistedMembership(t, dir)
			require.Empty(t, duplicates, "a live member is saved in one cluster")
			require.Equal(t, before, persisted, "the saved generation is the population captured at the save")
		})
	}
}

// F2 (TestSolR11RestoreUsesLiveMembership): a moved vector's tombstone in its
// old cluster's graph does not decide its restored membership.
func TestClusterGenerationRestoreUsesLiveMembership(t *testing.T) {
	for _, withEmpty := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty_label=%t", withEmpty), func(t *testing.T) {
			f := newCGFixture(t, 20, false)
			emptyCID := -1
			if withEmpty {
				for cid := 0; cid < f.svc.clusterIndex.NumClusters(); cid++ {
					if len(f.svc.clusterIndex.GetClusterMemberIDsForCluster(cid)) == 0 {
						emptyCID = cid
						break
					}
				}
				if emptyCID < 0 {
					// A public removal can leave a requested centroid unoccupied.
					// Preserve the remaining noisy vectors and their exact ranks.
					emptyCID = 0
					for _, id := range f.svc.clusterIndex.GetClusterMemberIDsForCluster(emptyCID) {
						owner := storage.NodeID(vectorOwnerForTest(t, id))
						require.NoError(t, f.engine.DeleteNode(owner))
						require.NoError(t, f.svc.RemoveNode(owner))
					}
				}
				require.Empty(t, f.svc.clusterIndex.GetClusterMemberIDsForCluster(emptyCID))
			}
			to, from := r11ClusterPair(t, f)
			moved, _ := moveClusterMember(t, f, from, to)
			path := filepath.Join(t.TempDir(), "hnsw")
			require.NoError(t, SaveIVFHNSW(path, f.svc.clusterGen.graphs))
			_, membership, err := DeriveIVFCentroidsFromClusters(path, f.svc.getVectorLookup())
			require.NoError(t, err)
			require.Contains(t, membership, moved, "a missing entry must not pass as the zero-valued cluster ID")
			require.Equal(t, to, membership[moved], "restored membership follows the live graph entry, not the tombstone")
		})
	}
}

// A saved generation with a live member in two clusters is not one
// population: it is rejected, not made authoritative.
func TestClusterGenerationRestoreRejectsDuplicateMembers(t *testing.T) {
	f := newCGFixture(t, 20, false)
	source, target := r11ClusterPair(t, f)
	ids := f.svc.clusterIndex.GetClusterMemberIDsForCluster(source)
	require.NotEmpty(t, ids)
	vec, ok := f.svc.getVectorLookup()(ids[0])
	require.True(t, ok)
	graphs := map[int]*HNSWIndex{}
	for cid, g := range f.svc.clusterGen.graphs {
		graphs[cid] = g
	}
	dup := NewHNSWIndex(4, DefaultHNSWConfig())
	for _, id := range f.svc.clusterIndex.GetClusterMemberIDsForCluster(target) {
		v, _ := f.svc.getVectorLookup()(id)
		require.NoError(t, dup.Add(id, v))
	}
	require.NoError(t, dup.Add(ids[0], vec))
	graphs[target] = dup
	path := filepath.Join(t.TempDir(), "hnsw")
	require.NoError(t, SaveIVFHNSW(path, graphs))
	_, _, err := DeriveIVFCentroidsFromClusters(path, f.svc.getVectorLookup())
	require.ErrorContains(t, err, "live member of clusters")
	f.svc.SetHNSWIndexPath(path)
	f.svc.SetPersistenceEnabled(true)
	require.False(t, f.svc.tryRestoreClusteredWarmupFromDisk(context.Background(), f.svc.clusterIndex, path, nil))
}

// r11PublicSelfHit reports whether a public query for vec (one probed
// cluster) returns nodeID.
func r11PublicSelfHit(t *testing.T, f *cgFixture, vec []float32, nodeID string) bool {
	t.Helper()
	hits, err := f.svc.VectorQueryNodesWithOptions(context.Background(), vec, VectorQuerySpec{Limit: 100}, nil)
	require.NoError(t, err)
	for _, h := range hits {
		if h.ID == nodeID {
			return true
		}
	}
	return false
}

func r11Restore(t *testing.T, f *cgFixture, path string) {
	t.Helper()
	f.svc.SetHNSWIndexPath(path)
	f.svc.SetPersistenceEnabled(true)
	require.True(t, f.svc.tryRestoreClusteredWarmupFromDisk(context.Background(), f.svc.clusterIndex, path, nil))
	assertClusterGenerationCoherent(t, f.svc, cgAllChecks)
}

// F2 (TestSolR11RestoreNewCanonicalVector): a vector newer than the saved
// generation is placed at its nearest restored centroid, not in cluster 0.
func TestClusterGenerationRestorePlacesNewCanonicalVector(t *testing.T) {
	f := newCGFixture(t, 20, false)
	t.Setenv("NORNICDB_KMEANS_NPROBE", "1")
	_, target := r11ClusterPair(t, f)
	require.NotZero(t, target, "the probe must reject default-cluster-zero placement")
	ids := f.svc.clusterIndex.GetClusterMemberIDsForCluster(target)
	require.NotEmpty(t, ids)
	routing := f.svc.clusterIndex.PublishedClustering()
	require.NotNil(t, routing)
	vec := routing.Centroids()[target]
	path := filepath.Join(t.TempDir(), "hnsw")
	require.NoError(t, SaveIVFHNSW(path, f.svc.clusterGen.graphs))
	node := &storage.Node{ID: "late-restored", Properties: map[string]any{"text": "newcomer"}, ChunkEmbeddings: [][]float32{append([]float32(nil), vec...)}}
	require.NoError(t, f.write(node))
	before, found := f.svc.clusterIndex.ClusterOf("late-restored")
	require.True(t, found)
	require.Equal(t, target, before)
	r11Restore(t, f, path)
	after, found := f.svc.clusterIndex.ClusterOf("late-restored")
	require.True(t, found)
	require.Equal(t, target, after, "placed at its nearest restored centroid")
	require.True(t, r11PublicSelfHit(t, f, vec, "late-restored"))
}

// F2 (TestSolR11RestoreMovedPublicQuery): a vector moved before the save is
// restored in its new cluster and found by a public query probing it.
func TestClusterGenerationRestoreMovedPublicQuery(t *testing.T) {
	f := newCGFixture(t, 20, false)
	t.Setenv("NORNICDB_KMEANS_NPROBE", "1")
	to, from := r11ClusterPair(t, f)
	moved, _ := moveClusterMember(t, f, from, to)
	vec, ok := f.svc.getVectorLookup()(moved)
	require.True(t, ok)
	path := filepath.Join(t.TempDir(), "hnsw")
	require.NoError(t, SaveIVFHNSW(path, f.svc.clusterGen.graphs))
	r11Restore(t, f, path)
	cid, found := f.svc.clusterIndex.ClusterOf(moved)
	require.True(t, found)
	require.Equal(t, to, cid)
	require.True(t, r11PublicSelfHit(t, f, vec, vectorOwnerForTest(t, moved)))
}

// A vector moved after the save (the store is newer than the saved
// generation) is placed by its current coordinates, so a query for it finds
// it, and its old cluster's saved graph no longer holds it.
func TestClusterGenerationRestorePlacesVectorsUpdatedAfterSave(t *testing.T) {
	f := newCGFixture(t, 20, false)
	t.Setenv("NORNICDB_KMEANS_NPROBE", "1")
	path := filepath.Join(t.TempDir(), "hnsw")
	require.NoError(t, SaveIVFHNSW(path, f.svc.clusterGen.graphs))
	from, to := r11ClusterPair(t, f)
	require.NotZero(t, to, "the probe must reject default-cluster-zero placement")
	moved, _ := moveClusterMember(t, f, from, to)
	vec, ok := f.svc.getVectorLookup()(moved)
	require.True(t, ok)
	r11Restore(t, f, path)
	cid, found := f.svc.clusterIndex.ClusterOf(moved)
	require.True(t, found)
	require.Equal(t, to, cid, "placed by its current coordinates, not its saved cluster")
	require.True(t, r11PublicSelfHit(t, f, vec, vectorOwnerForTest(t, moved)))
}

// parkReconcile holds the admission boundary, starts a population reconcile
// and returns once it waits for that boundary (it has listed the IDs).
func parkReconcile(t *testing.T, svc *Service, target int) chan struct{} {
	t.Helper()
	svc.indexMu.Lock()
	done := make(chan struct{})
	go func() {
		svc.reconcileClusterPopulation(target)
		close(done)
	}()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, g := range allGoroutineStacks() {
			if strings.Contains(g, "reconcileClusterPopulation") && strings.Contains(g, "(*RWMutex).Lock") && !strings.Contains(g, "admitClusterVectorsLocked") {
				return done
			}
		}
		time.Sleep(time.Millisecond)
	}
	svc.indexMu.Unlock()
	t.Fatal("the reconcile did not reach the admission boundary")
	return nil
}

func waitReconcile(t *testing.T, done chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Minute):
		t.Fatal("the reconcile did not complete")
	}
}

// F4 (TestSolR11BackfillDoesNotOverwriteNewAdmission): a vector written, or a
// node removed, after the backfill listed its IDs is admitted as the store
// holds it then: never overwritten with an older vector, never resurrected.
func TestClusterGenerationBackfillUsesStoreAtAdmission(t *testing.T) {
	for _, fileBacked := range []bool{false, true} {
		t.Run(fmt.Sprintf("file-backed=%t", fileBacked), func(t *testing.T) {
			f := newCGFixture(t, 20, fileBacked)
			f.t = t
			svc := f.svc
			node, err := f.engine.GetNode("n-0001")
			require.NoError(t, err)
			node.ChunkEmbeddings = [][]float32{{0, 0, 0, 1}}
			require.NoError(t, f.engine.UpdateNode(node))
			done := parkReconcile(t, svc, svc.EmbeddingCount()+1)
			err = svc.indexNodeLocked(node, false, storage.CaptureVectorWorkOrigin())
			svc.removeNodeLocked("n-0002")
			svc.indexMu.Unlock()
			require.NoError(t, err)
			waitReconcile(t, done)

			canonical, ok := svc.getVectorLookup()("n-0001")
			require.True(t, ok)
			admitted, ok := svc.clusterIndex.Get("n-0001")
			require.True(t, ok)
			require.Equal(t, canonical, admitted, "the backfill keeps the newer admitted vector")
			_, ok = svc.clusterIndex.Get("n-0002")
			require.False(t, ok, "the backfill does not resurrect a removed vector")
			assertClusterGenerationCoherent(t, svc, cgAllChecks)
		})
	}
}

// The backfill also retires an entry the store no longer holds.
func TestClusterGenerationBackfillRetiresEntriesTheStoreLacks(t *testing.T) {
	f := newCGFixture(t, 20, false)
	f.svc.vectorIndex.Remove("n-0003")
	f.svc.reconcileClusterPopulation(f.svc.EmbeddingCount())
	_, ok := f.svc.clusterIndex.Get("n-0003")
	require.False(t, ok)
	_, ok = f.svc.clusterIndex.ClusterOf("n-0003")
	require.False(t, ok)
}

// cancelInPolicyApply cancels when the cluster graphs' policy is applied.
type cancelInPolicyApply struct {
	context.Context
	cancel context.CancelFunc
}

func (c cancelInPolicyApply) Err() error {
	buf := make([]byte, 32768)
	if strings.Contains(string(buf[:runtime.Stack(buf, false)]), "applyClusterPolicyLocked") && c.cancel != nil {
		c.cancel()
	}
	return c.Context.Err()
}

// F5 (TestSolR11PolicyRetryAfterCancellation, TestSolR11PolicyRetryRealCancellation):
// a policy change cancelled while the cluster graphs are re-encoded is
// completed by a retry with the same configuration.
func TestClusterGenerationPolicyRetryCompletesCancelledChange(t *testing.T) {
	f := newCGFixture(t, 20, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := HNSWQuantizationConfig{Mode: HNSWQuantizationBinary}
	require.ErrorIs(t, f.svc.ConfigureHNSWQuantization(cancelInPolicyApply{ctx, cancel}, cfg), context.Canceled)
	require.NoError(t, f.svc.ConfigureHNSWQuantization(context.Background(), cfg))
	assertClusterGenerationCoherent(t, f.svc, cgAllChecks)
	// A same-value call on a complete policy changes nothing.
	revision := f.svc.indexRevision.Load()
	require.NoError(t, f.svc.ConfigureHNSWQuantization(context.Background(), cfg))
	require.Equal(t, revision, f.svc.indexRevision.Load())
}

// F6 (TestSolR11SettingsResetRebuildsClusterGraphs): a persisted generation
// built with other HNSW settings is not restored after a restart.
func TestClusterGenerationSettingsChangeRebuildsClusterGraphs(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_HNSW_M", "8")
	f := newCGFixture(t, 20, true)
	require.NoError(t, f.svc.BuildIndexes(context.Background()))
	require.NoError(t, f.svc.Close())
	t.Setenv("NORNICDB_VECTOR_HNSW_M", "24")
	f.svc = f.open(true)
	require.NoError(t, f.svc.BuildIndexes(context.Background()))
	require.Equal(t, 24, f.svc.hnswConfig().M)
	for cid, g := range f.svc.clusterGen.graphs {
		require.Equal(t, 24, g.Config().M, "cluster %d is rebuilt under the new settings", cid)
	}
	assertClusterGenerationCoherent(t, f.svc, cgAllChecks)
}

// A routing/k-means settings change rejects the persisted generation too.
func TestClusterGenerationRoutingSettingsChangeRejectsRestore(t *testing.T) {
	f := newCGFixture(t, 20, true)
	f.svc.PersistIndexesToDisk()
	hnswPath := filepath.Join(f.dir, "hnsw")
	require.True(t, f.svc.tryRestoreClusteredWarmupFromDisk(context.Background(), f.svc.clusterIndex, hnswPath, nil))
	t.Setenv("NORNICDB_KMEANS_MAX_ITERATIONS", "9")
	require.False(t, f.svc.tryRestoreClusteredWarmupFromDisk(context.Background(), f.svc.clusterIndex, hnswPath, nil))
}

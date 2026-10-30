package search

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// F-1372-CLUSTER behavioural matrix: every way the population or the cluster
// generation changes (b-paths) must leave the published generation coherent
// with the population (assertClusterGenerationCoherent). State is read
// through the view adapter in cluster_generation_view_test.go.

var cgTopics = []string{"alpha", "beta", "gamma", "delta"}

type cgFixture struct {
	t      *testing.T
	svc    *Service
	engine storage.Engine
	dir    string
	rng    *rand.Rand
	next   int
}

// cgVector places a point near topic's axis so k-means finds the topics.
func (f *cgFixture) cgVector(topic int) []float32 {
	vec := make([]float32, 4)
	vec[topic%4] = 1
	for d := range vec {
		vec[d] += float32(f.rng.NormFloat64()) / 10
	}
	return vec
}

func (f *cgFixture) node(id string, topic int, text string) *storage.Node {
	return &storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"},
		Properties: map[string]any{"text": text}, ChunkEmbeddings: [][]float32{f.cgVector(topic)}}
}

// write stores and indexes node (create or update), as a live mutation.
func (f *cgFixture) write(node *storage.Node) error {
	if _, err := f.engine.CreateNode(node); err != nil {
		if err := f.engine.UpdateNode(storage.NodeUpdateRequest{After: node}); err != nil {
			return err
		}
	}
	return f.svc.IndexNode(node)
}

func (f *cgFixture) add(topic int, text string) string {
	f.t.Helper()
	f.next++
	id := fmt.Sprintf("n-%04d", f.next)
	require.NoError(f.t, f.write(f.node(id, topic, text)))
	return id
}

func (f *cgFixture) recluster() {
	f.t.Helper()
	require.NoError(f.t, f.svc.TriggerClustering(context.Background()))
}

// newCGFixture builds a clustered IVF-HNSW service over 4 topics × perTopic
// documents. fileBacked selects file-backed vectors and persistence in dir.
func newCGFixture(t *testing.T, perTopic int, fileBacked bool) *cgFixture {
	t.Helper()
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_IVF_HNSW_ENABLED", "true")
	t.Setenv("NORNICDB_KMEANS_NPROBE", "64")
	f := &cgFixture{t: t, engine: newNamespacedEngine(t), dir: t.TempDir(), rng: rand.New(rand.NewSource(1372))}
	f.svc = f.open(fileBacked)
	for topic := 0; topic < 4; topic++ {
		for i := 0; i < perTopic; i++ {
			f.add(topic, cgTopics[topic]+" document")
		}
	}
	f.recluster()
	return f
}

func (f *cgFixture) open(fileBacked bool) *Service {
	f.t.Helper()
	svc := NewService(f.engine, 4, nil)
	f.t.Cleanup(func() { _ = svc.Close() })
	if fileBacked {
		svc.SetFulltextIndexPath(filepath.Join(f.dir, "bm25"))
		svc.SetVectorIndexPath(filepath.Join(f.dir, "vectors"))
		svc.SetHNSWIndexPath(filepath.Join(f.dir, "hnsw"))
		svc.SetPersistenceEnabled(true)
	}
	svc.EnableClustering(nil, 4)
	svc.SetMinEmbeddingsForClustering(1)
	return svc
}

// cgExpectedProfile derives a cluster's lexical routing profile from its
// members' documents independently of production code.
func cgExpectedProfile(svc *Service, members []string) map[string]float64 {
	ft := svc.fulltext()
	if ft == nil {
		return nil
	}
	analyzer := normalizeAnalyzer(svc.bm25Analyzer)
	tf := map[string]float64{}
	total := 0.0
	for _, id := range members {
		owner, valid := vectorEntryOwner(id)
		if !valid {
			continue
		}
		text, ok := ft.GetDocument(owner)
		if !ok || text == "" {
			continue
		}
		for _, tok := range analyzer.Analyze(text) {
			tf[tok]++
			total++
		}
	}
	if total == 0 {
		return nil
	}
	for tok := range tf {
		tf[tok] /= total
	}
	return topNTokenWeights(tf, 64)
}

type cgCheck struct {
	ivf      bool // graphs must mirror membership
	policy   bool // graph policy must equal the configured policy
	profiles bool // profiles must equal a re-derivation from members
}

var cgAllChecks = cgCheck{ivf: true, policy: true, profiles: true}

// assertClusterGenerationCoherent checks the published generation against
// the population: membership by ID covers exactly the population with no
// duplicates; each cluster graph holds exactly its members; each profile
// equals a re-derivation from its members; graphs use the configured policy.
func assertClusterGenerationCoherent(t *testing.T, svc *Service, check cgCheck) {
	t.Helper()
	view := cgView(svc)
	if !view.clustered {
		require.Empty(t, view.graphs, "no generation: no graphs")
		require.Empty(t, view.profiles, "no generation: no profiles")
		return
	}
	require.Empty(t, view.duplicates, "an ID is a member of more than one cluster")
	population := cgPopulation(svc)
	var missing, stale []string
	for _, id := range population {
		if _, ok := view.membership[id]; !ok {
			missing = append(missing, id)
		}
	}
	popSet := make(map[string]struct{}, len(population))
	for _, id := range population {
		popSet[id] = struct{}{}
	}
	for id := range view.membership {
		if _, ok := popSet[id]; !ok {
			stale = append(stale, id)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	require.Empty(t, missing, "population vectors without a cluster (read-your-write gap)")
	require.Empty(t, stale, "members that are not in the population")
	members := map[int][]string{}
	for id, cid := range view.membership {
		members[cid] = append(members[cid], id)
	}
	if check.ivf {
		require.NotNil(t, view.graphs, "IVF-HNSW enabled: the generation has graphs")
		for cid := 0; cid < view.numClusters; cid++ {
			want := append([]string(nil), members[cid]...)
			got := append([]string(nil), view.graphs[cid]...)
			sort.Strings(want)
			sort.Strings(got)
			require.Equal(t, want, got, "cluster %d graph holds exactly its members", cid)
		}
	}
	if check.profiles {
		for cid := 0; cid < view.numClusters; cid++ {
			want := cgExpectedProfile(svc, members[cid])
			got := view.profiles[cid]
			require.Equal(t, len(want), len(got), "cluster %d profile terms", cid)
			for tok, w := range want {
				require.InDelta(t, w, got[tok], 1e-9, "cluster %d profile weight of %q", cid, tok)
			}
		}
	}
	if check.policy && view.policySupported {
		for cid, mode := range view.graphPolicies {
			require.Equal(t, view.configuredPolicy, mode, "cluster %d graph uses the configured policy", cid)
		}
	}
	// A query routes on this generation's own routing with its probe count.
	clustered, ivf, served := cgServing(t, svc)
	require.True(t, clustered, "a published generation is served by a clustered strategy")
	require.Same(t, view.index, served.index, "queries score on the published generation's population")
	require.Same(t, view.index.PublishedClustering(), served.routing, "queries route on the published generation's routing")
	require.Equal(t, cgExpectedProbes(view.numClusters), served.probes, "probe count follows the published generation")
	if check.ivf {
		require.True(t, ivf, "a generation with graphs is served by IVF-HNSW")
	}
}

// assertPersistedGenerationMatches checks the generation persisted in dir
// against the published one: one live entry per member, in its cluster.
func assertPersistedGenerationMatches(t *testing.T, svc *Service, dir string) {
	t.Helper()
	persisted, duplicates := cgPersistedMembership(t, dir)
	require.Empty(t, duplicates, "a persisted live member is in one cluster")
	require.Equal(t, cgView(svc).membership, persisted, "the persisted generation is the published one")
}

// cgPausingContext pauses a clustering run right after k-means copied the
// population (third Err call of TriggerClustering).
type cgPausingContext struct {
	context.Context
	calls          atomic.Int32
	paused, resume chan struct{}
}

func newCGPausingContext() *cgPausingContext {
	return &cgPausingContext{Context: context.Background(), paused: make(chan struct{}), resume: make(chan struct{})}
}

func (c *cgPausingContext) Err() error {
	if c.calls.Add(1) == 3 {
		close(c.paused)
		<-c.resume
	}
	return c.Context.Err()
}

// duringCompute runs mutate while a clustering run is paused after its
// snapshot, then lets the run finish.
func (f *cgFixture) duringCompute(mutate func()) {
	f.t.Helper()
	ctx := newCGPausingContext()
	done := make(chan error, 1)
	go func() { done <- f.svc.TriggerClustering(ctx) }()
	select {
	case <-ctx.paused:
	case err := <-done:
		f.t.Fatalf("clustering finished without pausing: %v", err)
	case <-time.After(time.Minute):
		f.t.Fatal("clustering did not reach its snapshot")
	}
	mutate()
	close(ctx.resume)
	select {
	case err := <-done:
		require.NoError(f.t, err)
	case <-time.After(2 * time.Minute):
		f.t.Fatal("clustering did not finish")
	}
}

// duringBuild runs mutate when the replacement graphs start building. Trees
// without that seam skip the row.
func (f *cgFixture) duringBuild(mutate func()) {
	f.t.Helper()
	var mutateErr error
	restore, ok := cgSetBuildHook(func() {
		defer func() {
			if r := recover(); r != nil {
				mutateErr = fmt.Errorf("mutation panicked: %v", r)
			}
		}()
		mutate()
	})
	if !ok {
		f.t.Skip("this tree has no graph-build seam")
	}
	defer restore()
	f.recluster()
	require.NoError(f.t, mutateErr)
}

func TestClusterGenerationMatrix(t *testing.T) {
	type row struct {
		name  string
		check cgCheck
		run   func(f *cgFixture)
	}
	rows := []row{
		{"b1 add between generations", cgAllChecks, func(f *cgFixture) { f.add(1, "beta newcomer") }},
		{"b1 add during compute", cgAllChecks, func(f *cgFixture) { f.duringCompute(func() { f.add(2, "gamma late") }) }},
		{"b1 add during build", cgAllChecks, func(f *cgFixture) { f.duringBuild(func() { f.add(2, "gamma late") }) }},
		{"b1 vector move between generations", cgAllChecks, func(f *cgFixture) {
			require.NoError(f.t, f.write(f.node("n-0001", 3, "alpha document")))
		}},
		{"b1 vector move during build", cgAllChecks, func(f *cgFixture) {
			f.duringBuild(func() { require.NoError(f.t, f.write(f.node("n-0001", 3, "alpha document"))) })
		}},
		{"b2 remove between generations", cgAllChecks, func(f *cgFixture) {
			require.NoError(f.t, f.svc.RemoveNode("n-0002"))
			require.NoError(f.t, f.svc.RemoveNode("n-0007"))
		}},
		{"b2 remove during compute", cgAllChecks, func(f *cgFixture) {
			f.duringCompute(func() { require.NoError(f.t, f.svc.RemoveNode("n-0002")) })
		}},
		{"b2 remove during build", cgAllChecks, func(f *cgFixture) {
			f.duringBuild(func() { require.NoError(f.t, f.svc.RemoveNode("n-0002")) })
		}},
		{"b3 text-only update between generations", cgAllChecks, func(f *cgFixture) {
			node, err := f.engine.GetNode("n-0003")
			require.NoError(f.t, err)
			node.Properties["text"] = "zebraword retexted"
			require.NoError(f.t, f.write(node))
		}},
		{"b3 text-only update during build", cgAllChecks, func(f *cgFixture) {
			node, err := f.engine.GetNode("n-0003")
			require.NoError(f.t, err)
			node.Properties["text"] = "zebraword retexted"
			f.duringBuild(func() { require.NoError(f.t, f.write(node)) })
		}},
		{"b4 named vector between generations", cgAllChecks, func(f *cgFixture) {
			node := f.node("named-1", 0, "alpha named")
			node.ChunkEmbeddings = nil
			node.NamedEmbeddings = map[string][]float32{"title": f.cgVector(0), "body": f.cgVector(1)}
			require.NoError(f.t, f.write(node))
		}},
		{"b5 ClearVectorIndex then writes", cgAllChecks, func(f *cgFixture) {
			require.NoError(f.t, cgClearVectors(f.svc))
			f.add(0, "alpha after clear")
		}},
		{"b5 ClearIndexes then writes", cgAllChecks, func(f *cgFixture) {
			if !cgClearAll(f.svc) {
				f.t.Skip("this tree has no ClearIndexes")
			}
			f.add(0, "alpha after clear")
		}},
		{"b7 BuildIndexes rebuild then writes", cgAllChecks, func(f *cgFixture) {
			require.NoError(f.t, f.svc.BuildIndexes(context.Background()))
			f.add(3, "delta after rebuild")
		}},
		{"b8 clustering re-enabled after writes", cgAllChecks, func(f *cgFixture) {
			f.add(1, "beta before switch")
			cgSwitchClusteringMode(f.svc)
			f.add(1, "beta after switch")
		}},
		{"b9 policy change", cgAllChecks, func(f *cgFixture) {
			if !cgConfigurePolicy(f.svc, "binary") {
				f.t.Skip("this tree has no compression policy")
			}
		}},
		{"b11 cancelled compute keeps the previous generation", cgAllChecks, func(f *cgFixture) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_ = f.svc.TriggerClustering(ctx)
			f.add(2, "gamma after cancel")
		}},
		{"b12 concurrent second clustering", cgAllChecks, func(f *cgFixture) {
			var wg sync.WaitGroup
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); _ = f.svc.TriggerClustering(context.Background()) }()
			}
			wg.Wait()
		}},
		{"b13 backfill when clustering is enabled late", cgAllChecks, func(f *cgFixture) {
			cgSwitchClusteringMode(f.svc)
			f.recluster()
		}},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			f := newCGFixture(t, 20, false)
			f.t = t
			r.run(f)
			assertClusterGenerationCoherent(t, f.svc, r.check)
		})
	}
}

// File-backed rows: dimension change, restart/restore, stale files, crash
// between cluster files and the manifest.
func TestClusterGenerationPersistenceMatrix(t *testing.T) {
	t.Run("b10 restart restores a coherent generation", func(t *testing.T) {
		f := newCGFixture(t, 20, true)
		f.add(1, "beta newcomer")
		f.svc.PersistIndexesToDisk()
		assertPersistedGenerationMatches(t, f.svc, f.dir)
		require.NoError(t, f.svc.Close())
		f.svc = f.open(true)
		require.NoError(t, f.svc.BuildIndexes(context.Background()))
		assertClusterGenerationCoherent(t, f.svc, cgAllChecks)
		gen := cgPersisted(f.dir)
		require.True(t, gen.manifest, "a persisted generation is described by a manifest written last")
		require.Equal(t, cgView(f.svc).numClusters, len(gen.clusterFiles), "exactly one file per published cluster")
	})
	t.Run("b10 files above K are deleted", func(t *testing.T) {
		f := newCGFixture(t, 20, true)
		f.svc.PersistIndexesToDisk()
		ivf := filepath.Join(f.dir, "hnsw_ivf")
		require.NoError(t, os.WriteFile(filepath.Join(ivf, "99"), []byte("stale cluster file"), 0o644))
		f.add(0, "alpha more")
		f.recluster()
		f.svc.PersistIndexesToDisk()
		_, err := os.Stat(filepath.Join(ivf, "99"))
		require.True(t, os.IsNotExist(err), "a file for a cluster beyond K is removed when a generation is saved")
	})
	t.Run("b10 crash between cluster files and the manifest", func(t *testing.T) {
		f := newCGFixture(t, 20, true)
		f.svc.PersistIndexesToDisk()
		before := cgPersisted(f.dir)
		if !before.manifest {
			t.Fatal("no manifest: a partially written generation cannot be told apart")
		}
		require.NoError(t, cgSimulateCrashBeforeManifest(f.dir), "simulate a new generation's files without its manifest")
		require.NoError(t, f.svc.Close())
		f.svc = f.open(true)
		require.NoError(t, f.svc.BuildIndexes(context.Background()))
		assertClusterGenerationCoherent(t, f.svc, cgAllChecks)
	})
	t.Run("b6 dimension change", func(t *testing.T) {
		f := newCGFixture(t, 20, true)
		f.svc.maybeAutoSetVectorDimensions(5)
		assertClusterGenerationCoherent(t, f.svc, cgAllChecks)
	})
	t.Run("a7 publication advances the index revision", func(t *testing.T) {
		f := newCGFixture(t, 20, false)
		before := f.svc.indexRevision.Load()
		f.recluster()
		require.Greater(t, f.svc.indexRevision.Load(), before, "publishing a generation is an index change")
	})
}

// Concurrent property test: random admissions, removals and re-texts during
// repeated reclustering; once quiet, the generation is coherent.
func TestClusterGenerationConcurrentProperty(t *testing.T) {
	f := newCGFixture(t, 15, false)
	stop := make(chan struct{})
	var writers sync.WaitGroup
	var nextID atomic.Int32
	nextID.Store(1000)
	for w := 0; w < 3; w++ {
		writers.Add(1)
		go func(seed int64) {
			defer writers.Done()
			rng := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				switch rng.Intn(3) {
				case 0:
					id := fmt.Sprintf("p-%04d", nextID.Add(1))
					topic := rng.Intn(4)
					vec := []float32{0, 0, 0, 0}
					vec[topic] = 1
					node := &storage.Node{ID: storage.NodeID(id), Properties: map[string]any{"text": cgTopics[topic] + " concurrent"}, ChunkEmbeddings: [][]float32{vec}}
					if _, err := f.engine.CreateNode(node); err == nil {
						_ = f.svc.IndexNode(node)
					}
				case 1:
					_ = f.svc.RemoveNode(storage.NodeID(fmt.Sprintf("n-%04d", 1+rng.Intn(60))))
				default:
					if node, err := f.engine.GetNode(storage.NodeID(fmt.Sprintf("n-%04d", 1+rng.Intn(60)))); err == nil {
						node.Properties["text"] = fmt.Sprintf("retext %d", rng.Intn(5))
						_ = f.engine.UpdateNode(storage.NodeUpdateRequest{After: node})
						_ = f.svc.IndexNode(node)
					}
				}
			}
		}(int64(w))
	}
	for i := 0; i < 4; i++ {
		f.recluster()
	}
	close(stop)
	writers.Wait()
	assertClusterGenerationCoherent(t, f.svc, cgAllChecks)
}

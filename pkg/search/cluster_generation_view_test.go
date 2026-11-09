package search

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/stretchr/testify/require"
)

// View adapter for the F-1372-CLUSTER matrix: how this tree stores a cluster
// generation (Option L: one clusterGeneration owner, manifest persistence).
// The matrix and property tests are identical on every tree; only this file
// differs (baseline trees read the pre-owner fields).

type cgGenerationView struct {
	index            *gpu.ClusterIndex
	clustered        bool
	numClusters      int
	membership       map[string]int
	duplicates       []string
	graphs           map[int][]string // nil when the generation has no graphs
	profiles         map[int]map[string]float64
	policySupported  bool
	configuredPolicy HNSWQuantization
	graphPolicies    map[int]HNSWQuantization
}

func cgGraphIDs(g *HNSWIndex) []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	ids := make([]string, 0, len(g.idToInternal))
	for id, internal := range g.idToInternal {
		if int(internal) < len(g.deleted) && g.deleted[internal] {
			continue
		}
		ids = append(ids, id)
	}
	return ids
}

func cgView(svc *Service) cgGenerationView {
	v := cgGenerationView{membership: map[string]int{}, profiles: map[int]map[string]float64{}}
	svc.mu.RLock()
	ci := svc.clusterIndex
	svc.mu.RUnlock()
	svc.clusterStateMu.RLock()
	defer svc.clusterStateMu.RUnlock()
	if gen := svc.clusterGen; gen != nil {
		if gen.graphs != nil {
			v.graphs = map[int][]string{}
			v.graphPolicies = map[int]HNSWQuantization{}
			for cid, g := range gen.graphs {
				v.graphs[cid] = cgGraphIDs(g)
				g.mu.RLock()
				v.graphPolicies[cid] = g.config.Quantization.Mode
				g.mu.RUnlock()
			}
		}
		for cid, p := range gen.profiles {
			v.profiles[cid] = p
		}
	}
	v.policySupported = true
	v.configuredPolicy = svc.hnswConfig().Quantization.Mode
	if ci == nil || !ci.IsClustered() {
		return v
	}
	v.index = ci
	v.clustered = true
	v.numClusters = ci.NumClusters()
	for cid := 0; cid < v.numClusters; cid++ {
		for _, id := range ci.GetClusterMemberIDsForCluster(cid) {
			if _, dup := v.membership[id]; dup {
				v.duplicates = append(v.duplicates, id)
			}
			v.membership[id] = cid
		}
	}
	return v
}

// cgPopulation is the canonical vector population: every vector ID a query
// can return.
func cgPopulation(svc *Service) []string {
	svc.mu.RLock()
	vfs, vi := svc.vectorFileStore, svc.vectorIndex
	svc.mu.RUnlock()
	var ids []string
	if vfs != nil {
		vfs.mu.RLock()
		for id := range vfs.idToOrdinal {
			ids = append(ids, id)
		}
		vfs.mu.RUnlock()
		return ids
	}
	if vi != nil {
		vi.mu.RLock()
		for id := range vi.vectors {
			ids = append(ids, id)
		}
		vi.mu.RUnlock()
	}
	return ids
}

func cgSetBuildHook(fn func()) (func(), bool) {
	ivfHNSWBuildHook = fn
	return func() { ivfHNSWBuildHook = nil }, true
}

func cgConfigurePolicy(svc *Service, mode string) bool {
	return svc.ConfigureHNSWQuantization(context.Background(), HNSWQuantizationConfig{Mode: HNSWQuantization(mode)}) == nil
}

func cgClearAll(svc *Service) bool { return svc.ClearIndexes() == nil }

// cgSwitchClusteringMode makes the next EnableClustering a CPU<->GPU switch,
// which replaces the cluster index.
func cgSwitchClusteringMode(svc *Service) {
	svc.mu.Lock()
	svc.clusterUsesGPU = true
	svc.mu.Unlock()
	svc.EnableClustering(nil, 4)
}

type cgPersistedGeneration struct {
	manifest     bool
	clusterFiles []string
}

// cgPersisted reports the cluster files of the persisted generation: the
// files its manifest names.
func cgPersisted(dir string) cgPersistedGeneration {
	var gen cgPersistedGeneration
	m, err := readIVFHNSWManifest(filepath.Join(dir, "hnsw_ivf"))
	if err != nil || m == nil {
		return gen
	}
	gen.manifest = true
	for _, name := range m.Files {
		if name != "" {
			gen.clusterFiles = append(gen.clusterFiles, name)
		}
	}
	return gen
}

// cgSimulateCrashBeforeManifest leaves the next generation's cluster files
// (unreadable, as if torn) without the manifest that would make it current.
func cgSimulateCrashBeforeManifest(dir string) error {
	ivfDir := filepath.Join(dir, "hnsw_ivf")
	m, err := readIVFHNSWManifest(ivfDir)
	if err != nil {
		return err
	}
	if m == nil {
		return os.ErrNotExist
	}
	for cid := range m.Files {
		name := "g" + strconv.FormatUint(m.Seq+1, 10) + "-" + strconv.Itoa(cid)
		if err := os.WriteFile(filepath.Join(ivfDir, name), []byte("torn cluster file"), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func cgClearVectors(svc *Service) error { return svc.ClearVectorIndex() }

// cgServing starts a query (its pipeline and cluster generation) and reports
// how its clustered generator routes: whether it serves IVF-HNSW, the
// routing and population it uses and the probe count. clustered is false for
// a non-clustered strategy.
func cgServing(t *testing.T, svc *Service) (clustered, ivf bool, view clusterView) {
	t.Helper()
	ctx, pipeline, release, err := svc.vectorPipelineForQuery(context.Background())
	require.NoError(t, err)
	defer release()
	serving := cgGeneratorServing(pipeline)
	if serving == nil {
		return false, false, clusterView{}
	}
	_, ivf = pipeline.candidateGen.(*IVFHNSWCandidateGen)
	_, view, unlock, done := serving.open(ctx, clusterView{})
	unlock()
	done()
	return true, ivf, view
}

// cgGeneratorServing is a clustered generator's serving source, or nil.
func cgGeneratorServing(pipeline *VectorSearchPipeline) *clusterServing {
	switch gen := pipeline.candidateGen.(type) {
	case *IVFHNSWCandidateGen:
		return gen.serving
	case *KMeansCandidateGen:
		return gen.serving
	case *GPUKMeansCandidateGen:
		return gen.serving
	}
	return nil
}

// cgExpectedProbes is the probe count for a generation of numClusters:
// NORNICDB_KMEANS_NPROBE clamped to [1, K], by default the IVF rule.
func cgExpectedProbes(numClusters int) int {
	probes := envutil.GetInt("NORNICDB_KMEANS_NPROBE", defaultListsToProbe(numClusters))
	return min(max(probes, 1), max(1, numClusters))
}

// cgPersistedMembership is the live membership of the persisted generation
// the manifest names, with any ID live in more than one cluster.
func cgPersistedMembership(t *testing.T, dir string) (map[string]int, []string) {
	t.Helper()
	m, err := readIVFHNSWManifest(filepath.Join(dir, "hnsw_ivf"))
	require.NoError(t, err)
	require.NotNil(t, m, "a persisted generation")
	membership := map[string]int{}
	var duplicates []string
	for cid := range m.Files {
		ids, err := loadIVFClusterMemberIDs(m.clusterPath(cid))
		require.NoError(t, err)
		for _, id := range ids {
			if _, dup := membership[id]; dup {
				duplicates = append(duplicates, id)
			}
			membership[id] = cid
		}
	}
	return membership, duplicates
}

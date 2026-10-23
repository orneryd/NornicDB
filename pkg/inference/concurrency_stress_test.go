package inference

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/temporal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stress runs every operation in ops from workers goroutines each, all
// released by one barrier, iterations times per goroutine. Under -race, Go's
// race detector reports any two conflicting accesses that are not ordered by
// synchronization (it tracks happens-before, not physical overlap), so every
// operation pair executed here is checked whatever the interleaving.
func stress(t *testing.T, workers, iterations int, ops map[string]func(i int)) {
	t.Helper()
	start := make(chan struct{})
	var wg sync.WaitGroup
	for name, op := range ops {
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(name string, op func(int), w int) {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("%s panicked: %v", name, r)
					}
				}()
				<-start
				for i := 0; i < iterations; i++ {
					op(w*iterations + i)
				}
			}(name, op, w)
		}
	}
	close(start)
	wg.Wait()
}

// requireEveryMethod requires ops to cover exactly the exported method set of
// value's type, so a method added later is not silently left out.
func requireEveryMethod(t *testing.T, value any, ops map[string]func(i int)) {
	t.Helper()
	typ := reflect.TypeOf(value)
	var methods, covered []string
	for i := 0; i < typ.NumMethod(); i++ {
		methods = append(methods, typ.Method(i).Name)
	}
	for name := range ops {
		covered = append(covered, name)
	}
	sort.Strings(methods)
	sort.Strings(covered)
	assert.Equal(t, methods, covered, "%s: the stress operations must be exactly its exported methods", typ)
}

func stressNodeIDs(i int) (string, string) {
	return fmt.Sprintf("n%d", i%5), fmt.Sprintf("n%d", (i+1)%5)
}

func TestCooldownTableConcurrentAPI(t *testing.T) {
	defer config.WithCooldownEnabled()()
	ct := NewCooldownTable()
	ops := map[string]func(int){
		"CanMaterialize":           func(i int) { a, b := stressNodeIDs(i); ct.CanMaterialize(a, b, "relates_to") },
		"CanMaterializeWithReason": func(i int) { a, b := stressNodeIDs(i); ct.CanMaterializeWithReason(a, b, "relates_to") },
		"RecordMaterialization":    func(i int) { a, b := stressNodeIDs(i); ct.RecordMaterialization(a, b, "relates_to") },
		"RecordMaterializationAt": func(i int) {
			a, b := stressNodeIDs(i)
			ct.RecordMaterializationAt(a, b, "relates_to", time.Now().Add(-time.Hour))
		},
		"SetLabelCooldown": func(i int) { ct.SetLabelCooldown("relates_to", time.Duration(i%3+1)*time.Minute) },
		"GetLabelCooldown": func(int) { ct.GetLabelCooldown("relates_to") },
		"TimeUntilAllowed": func(i int) { a, b := stressNodeIDs(i); ct.TimeUntilAllowed(a, b, "relates_to") },
		"GetEntry":         func(i int) { a, b := stressNodeIDs(i); ct.GetEntry(a, b, "relates_to") },
		"Clear": func(i int) {
			if i%20 == 0 {
				ct.Clear()
			}
		},
		"Cleanup": func(int) { ct.Cleanup() },
		"Stats":   func(int) { ct.Stats() },
		"Size":    func(int) { ct.Size() },
	}
	requireEveryMethod(t, ct, ops)
	stress(t, 3, 60, ops)
}

// Readers iterate the containers of returned evidence while writers keep
// adding sessions, signals and metadata to the same entries.
func TestEvidenceBufferConcurrentAPI(t *testing.T) {
	eb := NewEvidenceBuffer()
	pending := EvidenceThreshold{MinCount: 1 << 30, MinScore: 1e12, MinSessions: 1 << 30, MaxAge: time.Hour}
	eb.SetThreshold("stress", pending)
	readContainers := func(ev Evidence) {
		for range ev.Sessions {
		}
		for range ev.Metadata {
		}
		_ = len(ev.Signals)
	}
	ops := map[string]func(int){
		"AddEvidence": func(i int) {
			a, b := stressNodeIDs(i)
			eb.AddEvidence(a, b, "stress", 0.5, "coaccess", fmt.Sprintf("s%d", i))
		},
		"AddEvidenceWithMetadata": func(i int) {
			a, b := stressNodeIDs(i)
			eb.AddEvidenceWithMetadata(a, b, "stress", 0.5, "similarity", fmt.Sprintf("s%d", i), map[string]interface{}{fmt.Sprintf("k%d", i): i})
		},
		"SetThreshold": func(int) { eb.SetThreshold("stress", pending) },
		"GetThreshold": func(int) { eb.GetThreshold("stress") },
		"GetEvidence": func(i int) {
			a, b := stressNodeIDs(i)
			if ev := eb.GetEvidence(a, b, "stress"); ev != nil {
				readContainers(*ev)
			}
		},
		"CheckThreshold": func(i int) { a, b := stressNodeIDs(i); eb.CheckThreshold(a, b, "stress") },
		"Clear": func(i int) {
			if i%25 == 0 {
				eb.Clear()
			}
		},
		"ClearEntry": func(i int) {
			if i%10 == 0 {
				a, b := stressNodeIDs(i)
				eb.ClearEntry(a, b, "stress")
			}
		},
		"Cleanup": func(int) { eb.Cleanup() },
		"Stats":   func(int) { eb.Stats() },
		"Size":    func(int) { eb.Size() },
		"GetPendingEdges": func(int) {
			for _, ev := range eb.GetPendingEdges(0) {
				readContainers(ev)
			}
		},
	}
	requireEveryMethod(t, eb, ops)
	stress(t, 3, 60, ops)
}

// GetPendingEdges hands out independent copies: evidence added afterwards
// does not change an entry already returned. A shared Sessions or Metadata
// map would change under the caller, and reading it would race the writer.
func TestPendingEdgesAreIndependentCopies(t *testing.T) {
	eb := NewEvidenceBuffer()
	eb.SetThreshold("stress", EvidenceThreshold{MinCount: 100, MinScore: 1000, MinSessions: 100, MaxAge: time.Hour})
	eb.AddEvidenceWithMetadata("a", "b", "stress", 0.5, "coaccess", "s1", map[string]interface{}{"k1": 1})
	pending := eb.GetPendingEdges(0)
	require.Len(t, pending, 1)

	eb.AddEvidenceWithMetadata("a", "b", "stress", 0.5, "similarity", "s2", map[string]interface{}{"k2": 2})
	require.Len(t, pending[0].Sessions, 1, "a returned entry's sessions must not change")
	require.Len(t, pending[0].Metadata, 1, "a returned entry's metadata must not change")
	require.Len(t, eb.GetPendingEdges(0)[0].Sessions, 2, "the buffer itself has both sessions")
}

func TestClusterIntegrationConcurrentAPI(t *testing.T) {
	cfg := testClusterConfig()
	cfg.AutoRecluster = true
	cfg.ReclusterThreshold = 0.01
	ci := NewClusterIntegration(testGPUManager(), cfg, testKMeansConfig(3), testEmbConfig())
	for i := 0; i < 20; i++ {
		require.NoError(t, ci.AddEmbedding(fmt.Sprintf("seed-%d", i), generateRandomEmbedding(64)))
	}
	configs := []*ClusterConfig{testClusterConfig(), DefaultClusterConfig()}
	ops := map[string]func(int){
		"AddEmbedding": func(i int) { _ = ci.AddEmbedding(fmt.Sprintf("n-%d", i), generateRandomEmbedding(64)) },
		"OnIndexComplete": func(i int) {
			if i%15 == 0 {
				_ = ci.OnIndexComplete()
			}
		},
		"Search":          func(int) { _, _ = ci.Search(context.Background(), generateRandomEmbedding(64), 3) },
		"OnNodeUpdate":    func(i int) { _ = ci.OnNodeUpdate(fmt.Sprintf("seed-%d", i%20), generateRandomEmbedding(64)) },
		"ShouldRecluster": func(int) { ci.ShouldRecluster() },
		"Recluster": func(i int) {
			if i%15 == 0 {
				_ = ci.Recluster()
			}
		},
		"Stats":           func(int) { ci.Stats() },
		"IsEnabled":       func(int) { ci.IsEnabled() },
		"IsClustered":     func(int) { ci.IsClustered() },
		"GetClusterIndex": func(int) { ci.GetClusterIndex() },
		"SetConfig":       func(i int) { ci.SetConfig(configs[i%2]) },
		"GetConfig":       func(int) { _ = ci.GetConfig().Enabled },
	}
	requireEveryMethod(t, ci, ops)
	stress(t, 2, 30, ops)
}

func TestEdgeDecayConcurrentAPI(t *testing.T) {
	cfg := DefaultEdgeDecayConfig()
	cfg.Enabled = true
	cfg.ScanInterval = time.Hour
	ed := NewEdgeDecay(cfg, fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)))
	ops := map[string]func(int){
		"Start":         func(int) { ed.Start(context.Background()) },
		"Stop":          func(int) { ed.Stop() },
		"ReinforceEdge": func(i int) { ed.ReinforceEdge(storage.EdgeID(fmt.Sprintf("e%d", i%5))) },
		"GetStats":      func(int) { ed.GetStats() },
		"TriggerScan":   func(int) { _, _, _, _ = ed.TriggerScan(context.Background()) },
	}
	requireEveryMethod(t, ed, ops)
	stress(t, 3, 40, ops)
	ed.Stop()
}

func TestHeimdallQCConcurrentAPI(t *testing.T) {
	defer config.WithAutoTLPLLMQCEnabled()()
	cfg := DefaultHeimdallQCConfig()
	cfg.Enabled = true
	cfg.CacheDecisions = true
	cfg.MinConfidenceToReview = 0
	qc := NewHeimdallQC(func(context.Context, string) (string, error) {
		return `{"approved": [0], "augment": []}`, nil
	}, cfg)
	source := NodeSummary{ID: "n0", Labels: []string{"Doc"}}
	ops := map[string]func(int){
		"ReviewBatch": func(i int) {
			_, _, _ = qc.ReviewBatch(context.Background(), source,
				[]EdgeSuggestion{{SourceID: "n0", TargetID: fmt.Sprintf("n%d", i%4+1), Confidence: 0.9}}, nil)
		},
		"GetStats": func(int) { qc.GetStats() },
		"ClearCache": func(i int) {
			if i%10 == 0 {
				qc.ClearCache()
			}
		},
	}
	requireEveryMethod(t, qc, ops)
	stress(t, 3, 40, ops)
}

// The engine's setters replace components while every operation reads them;
// replacements are non-nil, so the test shows synchronization rather than
// nil handling.
func TestEngineConcurrentAPI(t *testing.T) {
	defer config.WithCooldownAutoIntegrationEnabled()()
	defer config.WithEvidenceAutoIntegrationEnabled()()
	defer config.WithEdgeProvenanceAutoIntegrationEnabled()()
	defer config.WithPerNodeConfigAutoIntegrationEnabled()()
	defer config.WithCooldownEnabled()()

	store := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "test")
	setupTestGraph(t, store)
	topoCfg := DefaultTopologyConfig()
	topoCfg.Enabled = true
	e := New(DefaultConfig())
	search := func(context.Context, []float32, int) ([]SimilarityResult, error) {
		return []SimilarityResult{{ID: "bob", Score: 0.95}, {ID: "carol", Score: 0.9}}, nil
	}
	e.SetSimilaritySearch(search)
	topologies := []*TopologyIntegration{NewTopologyIntegration(store, topoCfg), NewTopologyIntegration(store, topoCfg)}
	clusters := []*ClusterIntegration{
		NewClusterIntegration(testGPUManager(), testClusterConfig(), testKMeansConfig(3), testEmbConfig()),
		NewClusterIntegration(testGPUManager(), testClusterConfig(), testKMeansConfig(3), testEmbConfig()),
	}
	adapters := []*KalmanAdapter{NewKalmanAdapter(e, DefaultKalmanAdapterConfig()), NewKalmanAdapter(e, DefaultKalmanAdapterConfig())}
	qcs := []*HeimdallQC{NewHeimdallQC(nil, nil), NewHeimdallQC(nil, nil)}
	tables := []*CooldownTable{NewCooldownTable(), NewCooldownTable()}
	buffers := []*EvidenceBuffer{NewEvidenceBuffer(), NewEvidenceBuffer()}
	metas := []*storage.EdgeMetaStore{storage.NewEdgeMetaStore(), storage.NewEdgeMetaStore()}
	nodeConfigs := []*storage.NodeConfigStore{storage.NewNodeConfigStore(), storage.NewNodeConfigStore()}
	suggestion := func(i int) EdgeSuggestion {
		a, b := stressNodeIDs(i)
		return EdgeSuggestion{SourceID: a, TargetID: b, Type: "RELATES_TO", Confidence: 0.9, Method: "similarity"}
	}
	ops := map[string]func(int){
		"SetSimilaritySearch":    func(int) { e.SetSimilaritySearch(search) },
		"SetTopologyIntegration": func(i int) { e.SetTopologyIntegration(topologies[i%2]) },
		"GetTopologyIntegration": func(int) { e.GetTopologyIntegration() },
		"SetClusterIntegration":  func(i int) { e.SetClusterIntegration(clusters[i%2]) },
		"GetClusterIntegration":  func(int) { e.GetClusterIntegration() },
		"SetKalmanAdapter":       func(i int) { e.SetKalmanAdapter(adapters[i%2]) },
		"GetKalmanAdapter":       func(int) { e.GetKalmanAdapter() },
		"SetHeimdallQC":          func(i int) { e.SetHeimdallQC(qcs[i%2]) },
		"GetHeimdallQC":          func(int) { e.GetHeimdallQC() },
		"OnStore":                func(i int) { _, _ = e.OnStore(context.Background(), "alice", []float32{1, 0, 0}) },
		"OnStoreBestOfChunks": func(int) {
			_, _ = e.OnStoreBestOfChunks(context.Background(), "alice", [][]float32{{1, 0, 0}, {0, 1, 0}})
		},
		"OnAccess": func(i int) { a, _ := stressNodeIDs(i); e.OnAccess(context.Background(), a) },
		"SuggestTransitive": func(int) {
			e.SuggestTransitive(context.Background(), []ExistingEdge{{SourceID: "a", TargetID: "b", Confidence: 0.9}, {SourceID: "b", TargetID: "c", Confidence: 0.9}})
		},
		"GetStats":                   func(int) { e.GetStats() },
		"ProcessSuggestion":          func(i int) { e.ProcessSuggestion(suggestion(i), "s") },
		"RecordMaterialization":      func(i int) { s := suggestion(i); e.RecordMaterialization(s.SourceID, s.TargetID, s.Type) },
		"GetCooldownTable":           func(int) { e.GetCooldownTable().Size() },
		"GetEvidenceBuffer":          func(int) { e.GetEvidenceBuffer().Size() },
		"SetCooldownTable":           func(i int) { e.SetCooldownTable(tables[i%2]) },
		"SetEvidenceBuffer":          func(i int) { e.SetEvidenceBuffer(buffers[i%2]) },
		"GetEdgeMetaStore":           func(int) { e.GetEdgeMetaStore() },
		"SetEdgeMetaStore":           func(i int) { e.SetEdgeMetaStore(metas[i%2]) },
		"GetNodeConfigStore":         func(int) { e.GetNodeConfigStore() },
		"SetNodeConfigStore":         func(i int) { e.SetNodeConfigStore(nodeConfigs[i%2]) },
		"CleanupTier1":               func(int) { e.CleanupTier1() },
		"CleanupTier1WithProvenance": func(int) { e.CleanupTier1WithProvenance(time.Hour) },
	}
	requireEveryMethod(t, e, ops)
	stress(t, 2, 25, ops)
}

func TestKalmanAdapterConcurrentAPI(t *testing.T) {
	e := New(DefaultConfig())
	ka := NewKalmanAdapter(e, DefaultKalmanAdapterConfig())
	trackers := []*temporal.Tracker{temporal.NewTracker(temporal.DefaultConfig()), temporal.NewTracker(temporal.DefaultConfig())}
	ops := map[string]func(int){
		"SetSessionDetector":            func(int) { ka.SetSessionDetector(nil) },
		"SetTracker":                    func(i int) { ka.SetTracker(trackers[i%2]) },
		"OnAccess":                      func(i int) { a, _ := stressNodeIDs(i); _, _ = ka.OnAccess(context.Background(), a) },
		"OnStore":                       func(i int) { a, _ := stressNodeIDs(i); _, _ = ka.OnStore(context.Background(), a, []float32{1, 0}) },
		"GetRelationshipStrength":       func(i int) { a, b := stressNodeIDs(i); ka.GetRelationshipStrength(a, b) },
		"GetStrengtheningRelationships": func(int) { ka.GetStrengtheningRelationships(0) },
		"GetWeakeningRelationships":     func(int) { ka.GetWeakeningRelationships(0) },
		"PredictFutureRelationships":    func(int) { ka.PredictFutureRelationships(0.5) },
		"GetStats":                      func(int) { ka.GetStats() },
		"GetEngine":                     func(int) { ka.GetEngine() },
		"Reset": func(i int) {
			if i%15 == 0 {
				ka.Reset()
			}
		},
	}
	requireEveryMethod(t, ka, ops)
	stress(t, 3, 40, ops)
}

func TestTopologyIntegrationConcurrentAPI(t *testing.T) {
	store := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "test")
	setupTestGraph(t, store)
	cfg := DefaultTopologyConfig()
	cfg.Enabled = true
	cfg.GraphRefreshInterval = 3
	topo := NewTopologyIntegration(store, cfg)
	ops := map[string]func(int){
		"SuggestTopological": func(int) { _, _ = topo.SuggestTopological(context.Background(), "alice") },
		"OnNodeAdded":        func(i int) { topo.OnNodeAdded(storage.NodeID(fmt.Sprintf("x%d", i))) },
		"OnNodeRemoved":      func(i int) { topo.OnNodeRemoved(storage.NodeID(fmt.Sprintf("x%d", i-1))) },
		"OnEdgeAdded":        func(i int) { topo.OnEdgeAdded("alice", storage.NodeID(fmt.Sprintf("x%d", i))) },
		"OnEdgeRemoved":      func(i int) { topo.OnEdgeRemoved("alice", storage.NodeID(fmt.Sprintf("x%d", i-1))) },
		"CombinedSuggestions": func(int) {
			topo.CombinedSuggestions([]EdgeSuggestion{{TargetID: "bob", Confidence: 0.8}}, []EdgeSuggestion{{TargetID: "bob", Confidence: 0.6}})
		},
		"InvalidateCache": func(i int) {
			if i%10 == 0 {
				topo.InvalidateCache()
			}
		},
		"Stats": func(int) { topo.Stats() },
	}
	requireEveryMethod(t, topo, ops)
	stress(t, 2, 30, ops)
}

func TestGlobalSingletonsConcurrentAPI(t *testing.T) {
	defer ResetGlobalCooldownTable()
	defer ResetGlobalEvidenceBuffer()
	stress(t, 3, 50, map[string]func(int){
		"GlobalCooldownTable":       func(int) { GlobalCooldownTable().Size() },
		"ResetGlobalCooldownTable":  func(int) { ResetGlobalCooldownTable() },
		"GlobalEvidenceBuffer":      func(int) { GlobalEvidenceBuffer().Size() },
		"ResetGlobalEvidenceBuffer": func(int) { ResetGlobalEvidenceBuffer() },
	})
}

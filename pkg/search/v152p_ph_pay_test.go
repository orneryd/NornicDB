package search

// Research only (packet-h-runs, observation 2), 2dbc67e96 only (b6d8e762a has
// no catch-up). What a write recorded in a running HNSW catch-up
// (hnsw_catch_up.go: recorded), what it left pending, and how many graph
// inserts its payment made, per kind of write; then the time of such writes
// at 1024 dimensions against the same writes with no catch-up running.

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func zzphVec(r *rand.Rand, dims int) []float32 {
	v := make([]float32, dims)
	for i := range v {
		v[i] = r.Float32()*2 - 1
	}
	return v
}

type zzphPayment struct {
	recorded, pendingBefore, pendingAfter, inserts int
	ownPending                                      bool
	elapsed                                         time.Duration
}

// zzphUnderCatchUp starts a catch-up of graph with backlog (vectors), runs
// write, and reports what it recorded, the pending change count before and
// after, the graph inserts made during the write, and whether key is still
// pending afterwards.
func zzphUnderCatchUp(svc *Service, graph *HNSWIndex, backlog map[string][]float32, key string, write func() error) (zzphPayment, error) {
	svc.indexMu.Lock()
	svc.hnswCatchUp.begin(graph, backlog, nil)
	svc.hnswRebuildInFlight.Store(true)
	svc.indexMu.Unlock()
	var p zzphPayment
	p.pendingBefore = svc.hnswCatchUp.pending()
	size := graph.Size()
	mark := svc.hnswCatchUp.owedMark()
	start := time.Now()
	err := write()
	p.elapsed = time.Since(start)
	p.recorded = int(svc.hnswCatchUp.owedMark() - mark)
	p.inserts = graph.Size() - size
	p.pendingAfter = svc.hnswCatchUp.pending()
	svc.hnswCatchUp.mu.Lock()
	p.ownPending = svc.hnswCatchUp.isPendingLocked(key)
	svc.hnswCatchUp.mu.Unlock()
	svc.indexMu.Lock()
	svc.hnswRebuildInFlight.Store(false)
	svc.hnswCatchUp.end()
	svc.indexMu.Unlock()
	return p, err
}

func zzphBacklog(n, dims int, r *rand.Rand) map[string][]float32 {
	b := make(map[string][]float32, n)
	for i := 0; i < n; i++ {
		b[fmt.Sprintf("backlog-%d", i)] = zzphVec(r, dims)
	}
	return b
}

func TestZZPHPaymentKinds(t *testing.T) {
	servingTestEnv(t)
	r := rand.New(rand.NewSource(7))
	for _, c := range []struct {
		name string
		key  string
		prep func(svc *Service, engine storage.Engine)
		do   func(svc *Service, engine storage.Engine) error
	}{
		{"rewrite existing 1-chunk node (IndexNode)", "hot",
			func(svc *Service, _ storage.Engine) {
				require.NoError(t, svc.IndexNode(&storage.Node{ID: "hot", ChunkEmbeddings: [][]float32{{1, 0}}}))
			},
			func(svc *Service, _ storage.Engine) error {
				return svc.IndexNode(&storage.Node{ID: "hot", ChunkEmbeddings: [][]float32{{0, 1}}})
			}},
		{"index a new 1-chunk node (IndexNode)", "fresh",
			func(svc *Service, _ storage.Engine) {},
			func(svc *Service, _ storage.Engine) error {
				return svc.IndexNode(&storage.Node{ID: "fresh", ChunkEmbeddings: [][]float32{{0, 1}}})
			}},
		{"rewrite existing 3-chunk node (IndexNode)", "multi",
			func(svc *Service, _ storage.Engine) {
				require.NoError(t, svc.IndexNode(&storage.Node{ID: "multi", ChunkEmbeddings: [][]float32{{1, 0}, {1, 1}, {0, 1}}}))
			},
			func(svc *Service, _ storage.Engine) error {
				return svc.IndexNode(&storage.Node{ID: "multi", ChunkEmbeddings: [][]float32{{0, 1}, {1, 1}, {1, 0}}})
			}},
		{"rewrite existing 1-chunk node with unchanged vector (IndexNode)", "same",
			func(svc *Service, _ storage.Engine) {
				require.NoError(t, svc.IndexNode(&storage.Node{ID: "same", ChunkEmbeddings: [][]float32{{1, 0}}}))
			},
			func(svc *Service, _ storage.Engine) error {
				return svc.IndexNode(&storage.Node{ID: "same", ChunkEmbeddings: [][]float32{{1, 0}}})
			}},
		{"remove existing 1-chunk node (RemoveNode)", "gone",
			func(svc *Service, _ storage.Engine) {
				require.NoError(t, svc.IndexNode(&storage.Node{ID: "gone", ChunkEmbeddings: [][]float32{{1, 0}}}))
			},
			func(svc *Service, _ storage.Engine) error { return svc.RemoveNode("gone") }},
		{"live refresh of a stored 1-chunk node after its vector changed (RefreshNode)", "live",
			func(svc *Service, engine storage.Engine) {
				_, err := engine.CreateNode(t.Context(), &storage.Node{ID: "live", Labels: []string{"Doc"}, Properties: map[string]any{"text": "t"}, ChunkEmbeddings: [][]float32{{1, 0}}})
				require.NoError(t, err)
				require.NoError(t, svc.RefreshNode(t.Context(), "live"))
				n, err := engine.GetNode(t.Context(), "live")
				require.NoError(t, err)
				after := *n
				after.ChunkEmbeddings = [][]float32{{0, 1}}
				require.NoError(t, engine.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: &after}))
			},
			func(svc *Service, _ storage.Engine) error { return svc.RefreshNode(t.Context(), "live") }},
	} {
		engine := newNamespacedEngine(t)
		svc := NewService(engine, 2, nil)
		c.prep(svc, engine)
		graph := NewHNSWIndex(2, DefaultHNSWConfig())
		p, err := zzphUnderCatchUp(svc, graph, zzphBacklog(10, 2, r), c.key, func() error { return c.do(svc, engine) })
		t.Logf("ZZPH OBS2 %s: err=%v recorded=%d pending %d -> %d (net %+d) graph inserts during the write=%d; written key still pending=%v; elapsed=%s",
			c.name, err, p.recorded, p.pendingBefore, p.pendingAfter, p.pendingAfter-p.pendingBefore+p.inserts, p.inserts, p.ownPending, p.elapsed)
		_ = svc.Close()
	}
}

func TestZZPHPaymentCost(t *testing.T) {
	servingTestEnv(t)
	const dims, graphSize, backlogN, writes = 1024, 5000, 3000, 200
	r := rand.New(rand.NewSource(11))
	engine := newNamespacedEngine(t)
	svc := NewService(engine, dims, nil)
	t.Cleanup(func() { _ = svc.Close() })
	for i := 0; i < writes; i++ {
		require.NoError(t, svc.IndexNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("w-%d", i)), ChunkEmbeddings: [][]float32{zzphVec(r, dims)}}))
	}
	graph := NewHNSWIndex(dims, DefaultHNSWConfig())
	start := time.Now()
	for i := 0; i < graphSize; i++ {
		require.NoError(t, graph.Add(fmt.Sprintf("g-%d", i), zzphVec(r, dims)))
	}
	built := time.Since(start)
	t.Logf("ZZPH OBS2 cost: graph of %d x %d dims built in %s (%.3f ms per insert)", graphSize, dims, built, float64(built.Microseconds())/1000/float64(graphSize))

	// Baseline: the same rewrites with no catch-up running.
	start = time.Now()
	for i := 0; i < writes; i++ {
		require.NoError(t, svc.IndexNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("w-%d", i)), ChunkEmbeddings: [][]float32{zzphVec(r, dims)}}))
	}
	base := time.Since(start)
	t.Logf("ZZPH OBS2 cost: %d rewrites, no catch-up: total %s, %.3f ms per write", writes, base, float64(base.Microseconds())/1000/writes)

	// The same rewrites during a catch-up with a backlog.
	svc.indexMu.Lock()
	svc.hnswCatchUp.begin(graph, zzphBacklog(backlogN, dims, r), nil)
	svc.hnswRebuildInFlight.Store(true)
	svc.indexMu.Unlock()
	size := graph.Size()
	mark := svc.hnswCatchUp.owedMark()
	pend := svc.hnswCatchUp.pending()
	start = time.Now()
	for i := 0; i < writes; i++ {
		require.NoError(t, svc.IndexNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("w-%d", i)), ChunkEmbeddings: [][]float32{zzphVec(r, dims)}}))
	}
	during := time.Since(start)
	recorded := svc.hnswCatchUp.owedMark() - mark
	inserts := graph.Size() - size
	t.Logf("ZZPH OBS2 cost: %d rewrites during a catch-up (backlog %d): total %s, %.3f ms per write; recorded=%d; graph inserts by their payments=%d; pending %d -> %d",
		writes, backlogN, during, float64(during.Microseconds())/1000/writes, recorded, inserts, pend, svc.hnswCatchUp.pending())

	// One builder step for comparison (what a payment of 1 per write would cost).
	start = time.Now()
	steps := 0
	for ; steps < writes; steps++ {
		if more, err := svc.hnswCatchUp.step(); !more || err != nil {
			break
		}
	}
	stepTime := time.Since(start)
	t.Logf("ZZPH OBS2 cost: %d builder steps alone: total %s, %.3f ms per step", steps, stepTime, float64(stepTime.Microseconds())/1000/float64(max(steps, 1)))
	svc.indexMu.Lock()
	svc.hnswRebuildInFlight.Store(false)
	svc.hnswCatchUp.end()
	svc.indexMu.Unlock()
}

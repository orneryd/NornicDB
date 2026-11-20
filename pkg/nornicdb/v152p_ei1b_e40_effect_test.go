package nornicdb

// Untracked probe (batch 1b, packet G item 2 / entries 40 and 41): the
// situation of entry 40 built through the exported storage API (as the
// previous-build probe of entry 40 does), then, on the rebuilt store, (a) the
// embedding worker is started and (b) the search service runs vector searches
// with a B vector and an A vector.
//
// Vectors are chosen so that cosine search can tell them apart (entry 40's own
// fixture uses all-positive 8-dimension vectors that are nearly parallel):
// A = 2 vectors along axis 0, B = 3 vectors along axis 1, and the stand-in
// embedder returns C along axis 2. 8 dimensions.
//
// Steps: node test:n {text: "alpha"}; worker view read; client update with A
// and {has_embedding, chunk_count 2}; worker result B stored beside the node
// (UpdateNodeEmbeddingSidecar); snapshot by (auto) the journal wrapper's
// automatic snapshot (seam EI1bAutoSnapshot) or (full) wal.CreateSnapshot +
// SaveSnapshot; crash (journal left open); RecoverIntoEngine into an empty
// engine. A control node test:fresh {text: "beta"} with no vectors is created
// in the rebuilt store before the worker starts.
// Identical file on both builds; API differences in zz_ei1b_e40_adapt_test.go.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const zzEI1bDims = 8

func zzEI1bAxis(axis int, n int) [][]float32 {
	out := make([][]float32, n)
	for i := range out {
		v := make([]float32, zzEI1bDims)
		v[axis] = 1
		v[7] = float32(i) * 0.01
		out[i] = v
	}
	return out
}

type zzEI1bEmbedder struct{ calls int }

func (e *zzEI1bEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	e.calls++
	return zzEI1bAxis(2, 1)[0], nil
}

func (e *zzEI1bEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i], _ = e.Embed(ctx, texts[i])
	}
	return out, nil
}

func (e *zzEI1bEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return []string{text}, nil
}

func (e *zzEI1bEmbedder) Dimensions() int { return zzEI1bDims }

func (e *zzEI1bEmbedder) Model() string { return "zz-stand-in" }

func (e *zzEI1bEmbedder) Backend() string { return "zz-stand-in" }

func zzEI1bAxisOf(vs [][]float32) string {
	var parts []string
	for _, v := range vs {
		axis := -1
		for i := 0; i < 7; i++ {
			if v[i] == 1 {
				axis = i
			}
		}
		parts = append(parts, fmt.Sprintf("axis%d", axis))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ",")
}

func TestZZEI1bE40Effect(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	A, B := zzEI1bAxis(0, 2), zzEI1bAxis(1, 3)
	for _, snap := range []string{"auto", "full"} {
		t.Run(snap, func(t *testing.T) {
			dir := t.TempDir()
			engine, err := storage.NewBadgerEngine(filepath.Join(dir, "data"))
			require.NoError(t, err)
			engine.SetEmbeddingTextPolicy(storage.EmbeddingTextPolicy{Include: []string{"text"}})
			cfg := storage.DefaultWALConfig()
			cfg.Dir = filepath.Join(dir, "wal")
			cfg.SnapshotInterval = 100 * time.Millisecond // port: drive the automatic snapshot through EnableAutoCompaction
			wal, err := storage.NewWAL(cfg.Dir, cfg)
			require.NoError(t, err)
			w := storage.NewWALEngine(engine, wal)
			require.NoError(t, ei1bCreate(w, &storage.Node{ID: "test:n", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha"}}))
			workerView, err := ei1bGet(w, "test:n")
			require.NoError(t, err)
			workerView = storage.CopyNode(workerView)
			client, err := ei1bGet(w, "test:n")
			require.NoError(t, err)
			client = storage.CopyNode(client)
			client.ChunkEmbeddings = A
			client.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": 2, "embedding_dimensions": zzEI1bDims, "embedding_model": "synthetic"}
			require.NoError(t, ei1bUpdate(w, client))
			workerView.ChunkEmbeddings = B
			workerView.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": 3, "embedding_dimensions": zzEI1bDims, "embedding_model": "synthetic"}
			werr := ei1bSidecar(w, workerView)
			live, err := ei1bGet(w, "test:n")
			require.NoError(t, err)
			var snapshot string
			if snap == "auto" {
				sdir := filepath.Join(dir, "snapshots")
				require.NoError(t, os.MkdirAll(sdir, 0o755))
				v152AutoSnapshot(t, w, sdir)
				matches, _ := filepath.Glob(filepath.Join(sdir, "*"))
				require.Len(t, matches, 1)
				snapshot = matches[0]
			} else {
				s, err := wal.CreateSnapshot(engine)
				require.NoError(t, err)
				snapshot = filepath.Join(dir, "full-snapshot.json")
				require.NoError(t, storage.SaveSnapshot(s, snapshot))
			}
			// crash: neither the journal nor the engine is closed before the rebuild
			dst, err := storage.NewBadgerEngine(filepath.Join(t.TempDir(), "rebuilt"))
			require.NoError(t, err)
			t.Cleanup(func() { _ = dst.Close() })
			dst.SetEmbeddingTextPolicy(storage.EmbeddingTextPolicy{Include: []string{"text"}})
			applied, failed, err := ei1bRecover(dst, cfg.Dir, snapshot)
			require.NoError(t, err)
			// The rebuild above read the files as a crash leaves them; the
			// source journal and engine are closed only at cleanup.
			t.Cleanup(func() { _ = wal.Close(); _ = engine.Close() })
			ns := storage.NewNamespacedEngine(dst, "test")
			got, err := ei1bGet(ns, "n")
			require.NoError(t, err)
			t.Logf("E40EFFECT %s workerErr=%v replay applied=%d failed=%d live=%s rebuilt=%s rebuiltMeta=%v needs=%v", snap, werr, applied, failed,
				zzEI1bAxisOf(live.ChunkEmbeddings), zzEI1bAxisOf(got.ChunkEmbeddings), got.EmbedMeta, storage.NodeNeedsEmbedding(got))

			// (a) the embedding worker on the rebuilt store. Its own node
			// discovery (findNodeWithoutEmbedding) is asked first with only the
			// rebuilt node in the store, then after a control node with no
			// vectors is added; then its processing step is driven directly as
			// the package's tests do (no background workers).
			emb := &zzEI1bEmbedder{}
			worker := NewEmbedWorker(emb, ns, &EmbedWorkerConfig{NumWorkers: 0, ScanInterval: time.Hour, BatchDelay: time.Millisecond,
				MaxRetries: 1, ChunkSize: 512, ChunkOverlap: 50, PropertiesInclude: []string{"text"}})
			found := ei1bFind(worker)
			t.Logf("E40EFFECT %s worker discovery with only the rebuilt node: %s; paused=%v", snap, found, worker.embeddingPaused())
			require.NoError(t, ei1bCreate(ns, &storage.Node{ID: "fresh", Labels: []string{"Doc"}, Properties: map[string]any{"text": "beta"}}))
			t.Logf("E40EFFECT %s worker discovery after adding the control node: %s", snap, ei1bFind(worker))
			ctrl, _ := ei1bGet(ns, "fresh")
			t.Logf("E40EFFECT %s NodeNeedsEmbedding: rebuilt node=%v, control node=%v; storage scan helper (the worker's fallback discovery): %s",
				snap, storage.NodeNeedsEmbedding(got), storage.NodeNeedsEmbedding(ctrl), ei1bScan(ns))
			var batches []bool
			for i := 0; i < 6; i++ {
				more := worker.processNextBatch()
				batches = append(batches, more)
				if !more {
					break
				}
			}
			stats := worker.Stats()
			worker.Close()
			t.Logf("E40EFFECT %s worker processNextBatch results=%v", snap, batches)
			after, err := ei1bGet(ns, "n")
			require.NoError(t, err)
			fresh, _ := ei1bGet(ns, "fresh")
			freshVec := "missing"
			if fresh != nil {
				freshVec = zzEI1bAxisOf(fresh.ChunkEmbeddings)
			}
			t.Logf("E40EFFECT %s worker: stats=%+v embedderCalls=%d test:n after=%s meta=%v needs=%v; control test:fresh after=%s", snap, stats, emb.calls,
				zzEI1bAxisOf(after.ChunkEmbeddings), after.EmbedMeta, storage.NodeNeedsEmbedding(after), freshVec)

			// (b) vector search on the rebuilt store.
			svc, err := ei1bSearchService(ns, zzEI1bDims)
			require.NoError(t, err)
			for _, q := range []struct {
				name string
				v    []float32
			}{{"B vector (axis1)", B[0]}, {"A vector (axis0)", A[0]}, {"C vector (axis2, the stand-in embedder)", zzEI1bAxis(2, 1)[0]}} {
				hits, err := ei1bVectorSearch(svc, q.v)
				t.Logf("E40EFFECT %s search %s: %v err=%v", snap, q.name, hits, err)
			}
		})
	}
}

// v152AutoSnapshot replaces the probe seam EI1bAutoSnapshot (a non-test hook in
// pkg/storage): it takes the journal wrapper's automatic snapshot through the
// exported EnableAutoCompaction with a short configured interval, waits for the
// first snapshot file, then stops the loop.
func v152AutoSnapshot(t *testing.T, w *storage.WALEngine, dir string) {
	t.Helper()
	require.NoError(t, w.EnableAutoCompaction(dir))
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if m, _ := filepath.Glob(filepath.Join(dir, "*")); len(m) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	w.DisableAutoCompaction()
}

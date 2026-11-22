//go:build pd_w161_2_12

package server

// E38.17: GET /nornicdb/embed/failures reads the whole embedding key space
// on every call, whatever its limit.
//
// Light form: two stores in one process, same 20 failure markers, the second
// with 2x the nodes and 16x the chunk vectors per node (17x the embedding
// keys). A listing that scans the whole embedding key space slows down by
// about that factor; a listing that only reads failure markers (or that skips
// chunk-vector keys) does not.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const l2E38_17Dims = 128

// l2E38_17Embedder is an idle provider: attached so the worker exists with a
// provider, never asked for anything because no node is pending.
type l2E38_17Embedder struct{}

func (l2E38_17Embedder) Embed(context.Context, string) ([]float32, error) {
	v := make([]float32, l2E38_17Dims)
	v[0] = 1
	return v, nil
}
func (e l2E38_17Embedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i], _ = e.Embed(ctx, texts[i])
	}
	return out, nil
}
func (l2E38_17Embedder) ChunkText(text string, _, _ int) ([]string, error) {
	return []string{text}, nil
}
func (l2E38_17Embedder) Dimensions() int { return l2E38_17Dims }
func (l2E38_17Embedder) Model() string   { return "l2-e38-17-idle" }
func (l2E38_17Embedder) Backend() string { return "cpu" }

type l2E38_17Store struct {
	name   string
	nodes  int
	chunks int
	srv    *Server
	token  string
}

// l2E38_17Open: the server fixture of the package (setupTestServerWithDatabaseConfig)
// with the entry's database assignments; nodes Doc with text, `chunks` stored
// chunk vectors each written through the worker's storage call
// (UpdateNodeEmbeddingSidecar, the embedding key space); 20 nodes with the
// worker's permanent-failure marker and no vector. Failure-marker node IDs
// sort before the documents ("a-fail-*" < "doc-*").
func l2E38_17Open(t *testing.T, name string, nodes, chunks int) *l2E38_17Store {
	t.Helper()
	s, authenticator := setupTestServerWithDatabaseConfig(t, func(c *nornicdb.Config) {
		c.Memory.EmbeddingEnabled = true
		c.Memory.EmbeddingDimensions = l2E38_17Dims
		c.Memory.AutoLinksEnabled = false
		c.EmbeddingWorker.PropertiesInclude = []string{"text"}
		c.EmbeddingWorker.IncludeLabels = false
		c.Database.WALAutoCompactionEnabled = false
		c.Memory.DecayEnabled = false
		c.EmbeddingWorker.NumWorkers = 1
	}, nil)
	ctx := context.Background()
	engine := s.db.GetStorage()
	sidecar, ok := engine.(storage.EmbeddingSidecarUpdater)
	if !ok {
		t.Fatalf("storage %T has no embedding sidecar writer", engine)
	}
	text := strings.Repeat("contract invoice ", 64) // ~1.1 KB
	for i := 0; i < nodes; i++ {
		id := storage.NodeID(fmt.Sprintf("doc-%06d", i))
		if _, err := engine.CreateNode(ctx, &storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": text}}); err != nil {
			t.Fatalf("%s: CreateNode %s: %v", name, id, err)
		}
		node, err := engine.GetNode(ctx, id)
		if err != nil {
			t.Fatalf("%s: GetNode %s: %v", name, id, err)
		}
		node = storage.CopyNode(node)
		node.ChunkEmbeddings = make([][]float32, chunks)
		for c := range node.ChunkEmbeddings {
			v := make([]float32, l2E38_17Dims)
			for j := range v {
				v[j] = float32((i*31+c*7+j)%97+1) / 97
			}
			node.ChunkEmbeddings[c] = v
		}
		node.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": chunks, "embedding_dimensions": l2E38_17Dims, "embedding_model": "l2-e38-17-idle"}
		if err := sidecar.UpdateNodeEmbeddingSidecar(ctx, node); err != nil {
			t.Fatalf("%s: UpdateNodeEmbeddingSidecar %s: %v", name, id, err)
		}
	}
	for i := 0; i < 20; i++ {
		id := storage.NodeID(fmt.Sprintf("a-fail-%02d", i))
		if _, err := engine.CreateNode(ctx, &storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": text}}); err != nil {
			t.Fatalf("%s: CreateNode %s: %v", name, id, err)
		}
		node, err := engine.GetNode(ctx, id)
		if err != nil {
			t.Fatalf("%s: GetNode %s: %v", name, id, err)
		}
		node = storage.CopyNode(node)
		node.ChunkEmbeddings = nil
		node.EmbedMeta = map[string]any{
			"has_embedding":       false,
			"embedding_failed":    true,
			"embedding_error":     "provider rejected input",
			"embedding_failed_at": "2026-01-01T00:00:00Z",
		}
		if err := sidecar.UpdateNodeEmbeddingSidecar(ctx, node); err != nil {
			t.Fatalf("%s: failure marker %s: %v", name, id, err)
		}
	}
	s.db.SetEmbedder(l2E38_17Embedder{})
	return &l2E38_17Store{name: name, nodes: nodes, chunks: chunks, srv: s, token: "Bearer " + getAuthToken(t, authenticator, "admin")}
}

// l2E38_17Get runs one listing; returns its duration and the "returned" and
// "count" fields.
func l2E38_17Get(t *testing.T, st *l2E38_17Store, limit int) (time.Duration, int, int) {
	t.Helper()
	start := time.Now()
	resp := makeRequest(t, st.srv, http.MethodGet, fmt.Sprintf("/nornicdb/embed/failures?limit=%d", limit), nil, st.token)
	elapsed := time.Since(start)
	if resp.Code != http.StatusOK {
		t.Fatalf("%s: GET embed/failures limit=%d: %d %s", st.name, limit, resp.Code, resp.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &payload); err != nil {
		t.Fatalf("%s: decode: %v (%s)", st.name, err, resp.Body.String())
	}
	returned, _ := payload["returned"].(float64)
	count, _ := payload["count"].(float64)
	return elapsed, int(returned), int(count)
}

func l2E38_17Median(ds []time.Duration) time.Duration {
	c := append([]time.Duration(nil), ds...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	return c[len(c)/2]
}

// l2E38_17Measure: one warm-up per store, then 7 interleaved calls per store
// and limit; medians.
func l2E38_17Measure(t *testing.T, small, large *l2E38_17Store, limit int) (time.Duration, time.Duration, []time.Duration, []time.Duration) {
	t.Helper()
	l2E38_17Get(t, small, limit)
	l2E38_17Get(t, large, limit)
	var s, l []time.Duration
	for i := 0; i < 7; i++ {
		d, _, _ := l2E38_17Get(t, small, limit)
		s = append(s, d)
		d, _, _ = l2E38_17Get(t, large, limit)
		l = append(l, d)
	}
	return l2E38_17Median(s), l2E38_17Median(l), s, l
}

// Small store: 2,000 nodes x 1 chunk (4,000 embedding keys: 2,000 metadata
// records, 2,000 chunk vectors, plus 20 failure records). Large store:
// 4,000 nodes x 16 chunks (68,000 keys). A whole-key-space scan scales ~17x;
// a scan of metadata records only scales 2x; a failure index 1x.
// Threshold: DEFECT when the limit=1 median ratio large/small is >= 5 (well
// above the 2x of a metadata-only scan, well below the ~17x key ratio).
const (
	l2E38_17SmallNodes, l2E38_17SmallChunks = 2000, 1
	l2E38_17LargeNodes, l2E38_17LargeChunks = 4000, 16
	l2E38_17RatioThreshold                  = 5.0
)

func TestL2_e38_17_Repro(t *testing.T) {
	small := l2E38_17Open(t, "small", l2E38_17SmallNodes, l2E38_17SmallChunks)
	large := l2E38_17Open(t, "large", l2E38_17LargeNodes, l2E38_17LargeChunks)

	sm1, lg1, s1, l1 := l2E38_17Measure(t, small, large, 1)
	sm1000, lg1000, s1000, l1000 := l2E38_17Measure(t, small, large, 1000)
	ratio1 := float64(lg1) / float64(sm1)
	ratio1000 := float64(lg1000) / float64(sm1000)
	t.Logf("limit=1 small=%v large=%v (all: %v | %v)", sm1, lg1, s1, l1)
	t.Logf("limit=1000 small=%v large=%v (all: %v | %v)", sm1000, lg1000, s1000, l1000)
	line := fmt.Sprintf("small=%dx%d large=%dx%d limit1_median_small=%v limit1_median_large=%v ratio1=%.2f limit1000_median_small=%v limit1000_median_large=%v ratio1000=%.2f threshold=%.1f",
		l2E38_17SmallNodes, l2E38_17SmallChunks, l2E38_17LargeNodes, l2E38_17LargeChunks, sm1, lg1, ratio1, sm1000, lg1000, ratio1000, l2E38_17RatioThreshold)
	if ratio1 >= l2E38_17RatioThreshold {
		t.Logf("L2RESULT E38.17 DEFECT %s", line)
		t.Errorf("GET /nornicdb/embed/failures?limit=1 slows %.2fx with %.0fx more embedding keys and the same 20 failures", ratio1, float64(l2E38_17LargeNodes*(l2E38_17LargeChunks+1))/float64(l2E38_17SmallNodes*(l2E38_17SmallChunks+1)))
		return
	}
	t.Logf("L2RESULT E38.17 OK %s", line)
}

// Control: the listing is correct on both stores: limit=1 returns 1 of 20,
// limit=1000 returns 20 of 20.
func TestL2_e38_17_Control(t *testing.T) {
	small := l2E38_17Open(t, "small", 200, 1)
	for _, tc := range []struct{ limit, want int }{{1, 1}, {1000, 20}} {
		_, returned, count := l2E38_17Get(t, small, tc.limit)
		t.Logf("control limit=%d returned=%d count=%d", tc.limit, returned, count)
		if returned != tc.want || count != 20 {
			t.Errorf("limit=%d: returned %d count %d, want %d and 20", tc.limit, returned, count, tc.want)
		}
	}
}

// Cause: the handler (pkg/server/server_nornicdb.go handleEmbeddingFailures)
// calls DB.EmbeddingFailures -> EmbedWorker.ParkedEmbeddingFailures
// (pkg/nornicdb/embed_queue.go), whose limit only stops appending, over
// BadgerEngine.StreamParkedEmbeddingFailures
// (pkg/storage/badger_embedding_sidecar.go), which iterates the whole
// embedding prefix, chunk-vector keys included. This times, on the large
// store, (1) the worker listing with limit 1, (2) the storage stream run to
// the end, (3) the same stream stopped at the first failure (failures sort
// first): (1) ~ (2) >> (3) shows the scan runs to the end although the
// first failure was found at once.
func TestL2_e38_17_Cause(t *testing.T) {
	large := l2E38_17Open(t, "large", l2E38_17LargeNodes, l2E38_17LargeChunks)
	ctx := context.Background()
	streamer, ok := storage.UnwrapEngine(large.srv.db.GetBaseStorageForManager()).(storage.EmbeddingFailureStreamer)
	if !ok {
		t.Fatalf("base storage %T is not an EmbeddingFailureStreamer", large.srv.db.GetBaseStorageForManager())
	}
	var list, full, first []time.Duration
	for i := 0; i < 5; i++ {
		start := time.Now()
		failures, err := large.srv.db.EmbeddingFailures(ctx, 1)
		list = append(list, time.Since(start))
		if err != nil || len(failures) != 1 {
			t.Fatalf("EmbeddingFailures(1): %d, %v", len(failures), err)
		}
		start = time.Now()
		n, err := streamer.StreamParkedEmbeddingFailures(ctx, func(storage.NodeID, map[string]any) error { return nil })
		full = append(full, time.Since(start))
		if err != nil {
			t.Fatalf("full stream: %v", err)
		}
		start = time.Now()
		m, err := streamer.StreamParkedEmbeddingFailures(ctx, func(storage.NodeID, map[string]any) error { return storage.ErrIterationStopped })
		first = append(first, time.Since(start))
		if err != nil {
			t.Fatalf("stopped stream: %v", err)
		}
		if i == 0 {
			t.Logf("CAUSE full stream visited %d failures; stopped stream visited %d", n, m)
		}
	}
	t.Logf("CAUSE large store: worker listing limit=1 median=%v; storage stream to end median=%v; storage stream stopped at first failure median=%v",
		l2E38_17Median(list), l2E38_17Median(full), l2E38_17Median(first))
}

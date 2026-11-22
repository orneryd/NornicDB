package server

// Investigation probe (untracked): do HTTP transaction boundaries (BEGIN,
// COMMIT) wait while the embedding worker runs, and on what?
//
// Setup per size N: a store with N :Doc nodes that each carry one stored
// 2,048-dimension chunk vector (written through the embedding sidecar), plus
// P :Doc nodes without a vector. A stand-in embedder (instant, deterministic)
// is attached last, so the real embedding worker starts with P pending nodes,
// as after an upgrade with embedding switched on. While it runs, a client
// loop opens an HTTP transaction (POST /db/nornic/tx, BEGIN only), creates one
// node in it and commits (POST .../commit), timing each step. When any step
// is still in flight after SLOWB_DUMP_AFTER, the full goroutine dump is
// printed once per phase (filtered to NornicDB frames).
//
// Phases:
//   A  baseline: no embedder, no refresh, client loop only.
//   B  direct: RefreshPendingEmbeddingsIndex called on the inner engine in a
//      goroutine (what the worker calls), client loop alongside; refresh
//      duration measured.
//   C  worker: embedder attached, worker embeds P nodes and refreshes as it
//      does in production; client loop for SLOWB_WORKER_SECONDS.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type slowbEmbedder struct {
	dims  int
	calls atomic.Int64
}

func (e *slowbEmbedder) vec() []float32 {
	n := e.calls.Add(1)
	v := make([]float32, e.dims)
	for i := range v {
		v[i] = float32((int64(i)+n)%97) / 97
	}
	return v
}
func (e *slowbEmbedder) Embed(context.Context, string) ([]float32, error) { return e.vec(), nil }
func (e *slowbEmbedder) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i] = e.vec()
	}
	return out, nil
}
func (e *slowbEmbedder) ChunkText(text string, _, _ int) ([]string, error) { return []string{text}, nil }
func (e *slowbEmbedder) Dimensions() int                                  { return e.dims }
func (e *slowbEmbedder) Model() string                                    { return "slowb-standin" }
func (e *slowbEmbedder) Backend() string                                  { return "cpu" }

func slowbEnvInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func slowbSidecar(e storage.Engine, n *storage.Node) error {
	switch s := any(e).(type) {
	case interface {
		UpdateNodeEmbeddingSidecar(context.Context, *storage.Node) error
	}:
		return s.UpdateNodeEmbeddingSidecar(context.Background(), n)
	case interface {
		UpdateNodeEmbeddingSidecar(*storage.Node) error
	}:
		return s.UpdateNodeEmbeddingSidecar(n)
	}
	return fmt.Errorf("UpdateNodeEmbeddingSidecar: unknown signature")
}

func slowbNodesByLabel(e storage.Engine, label string) ([]*storage.Node, error) {
	switch s := any(e).(type) {
	case interface {
		GetNodesByLabel(context.Context, string) ([]*storage.Node, error)
	}:
		return s.GetNodesByLabel(context.Background(), label)
	case interface {
		GetNodesByLabel(string) ([]*storage.Node, error)
	}:
		return s.GetNodesByLabel(label)
	}
	return nil, fmt.Errorf("GetNodesByLabel: unknown signature")
}

// slowbCreateUser calls CreateUser with or without a leading context.
func slowbCreateUser(a *auth.Authenticator, name string) error {
	m := reflect.ValueOf(a).MethodByName("CreateUser")
	args := []reflect.Value{}
	if m.Type().NumIn() == 4 {
		args = append(args, reflect.ValueOf(context.Background()))
	}
	args = append(args, reflect.ValueOf(name), reflect.ValueOf("password123"), reflect.ValueOf([]auth.Role{auth.RoleAdmin}))
	out := m.Call(args)
	if err, _ := out[len(out)-1].Interface().(error); err != nil {
		return err
	}
	return nil
}

// slowbToken calls Authenticate with or without a leading context.
func slowbToken(a *auth.Authenticator) (string, error) {
	m := reflect.ValueOf(a).MethodByName("Authenticate")
	args := []reflect.Value{}
	if m.Type().NumIn() == 5 {
		args = append(args, reflect.ValueOf(context.Background()))
	}
	args = append(args, reflect.ValueOf("admin"), reflect.ValueOf("password123"), reflect.ValueOf("127.0.0.1"), reflect.ValueOf("slowb"))
	out := m.Call(args)
	if err, _ := out[len(out)-1].Interface().(error); err != nil {
		return "", err
	}
	return reflect.Indirect(out[0]).FieldByName("AccessToken").String(), nil
}

type slowbLogTee struct {
	mu    sync.Mutex
	start time.Time
}

func (l *slowbLogTee) Write(p []byte) (int, error) {
	s := string(p)
	if strings.Contains(s, "pending embeddings index refreshed") || strings.Contains(s, "slow query") || strings.Contains(s, "refresh pending") {
		l.mu.Lock()
		fmt.Printf("SLOWB LOG +%.3fs %s", time.Since(l.start).Seconds(), s)
		l.mu.Unlock()
	}
	return len(p), nil
}

type slowbLab struct {
	t      *testing.T
	db     *nornicdb.DB
	srv    *Server
	router http.Handler
	authz  string
	start  time.Time
	root   string
}

func (l *slowbLab) post(path string, body any) (map[string]any, int, time.Duration) {
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", l.authz)
	rec := httptest.NewRecorder()
	t0 := time.Now()
	l.router.ServeHTTP(rec, req)
	d := time.Since(t0)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return out, rec.Code, d
}

func (l *slowbLab) cypher(q string, params map[string]any) {
	l.t.Helper()
	out, code, _ := l.post("/db/nornic/tx/commit", map[string]any{"statements": []any{map[string]any{"statement": q, "parameters": params}}})
	if code >= 300 {
		l.t.Fatalf("%s: code %d %v", q, code, out)
	}
	if errs, _ := out["errors"].([]any); len(errs) > 0 {
		l.t.Fatalf("%s: %v", q, errs)
	}
}

var slowbDumpFilter = []string{"orneryd/nornicdb/pkg/"}

func slowbDump(tag string) {
	var buf bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&buf, 2)
	blocks := strings.Split(buf.String(), "\n\n")
	kept := 0
	fmt.Printf("SLOWB DUMP BEGIN %s (goroutines total %d)\n", tag, len(blocks))
	for _, b := range blocks {
		keep := false
		for _, f := range slowbDumpFilter {
			if strings.Contains(b, f) {
				keep = true
			}
		}
		if !keep {
			continue
		}
		kept++
		fmt.Println(b)
		fmt.Println()
	}
	fmt.Printf("SLOWB DUMP END %s (printed %d)\n", tag, kept)
}

func slowbSnapshots(root string) {
	entries, _ := os.ReadDir(root + "/snapshots")
	for _, e := range entries {
		info, err := e.Info()
		if err == nil {
			fmt.Printf("SLOWB SNAPSHOTFILE %s size=%d mod=%s\n", e.Name(), info.Size(), info.ModTime().UTC().Format("15:04:05.000"))
		}
	}
}

type slowbStats struct {
	name string
	ds   []time.Duration
}

func (s *slowbStats) add(d time.Duration) { s.ds = append(s.ds, d) }
func (s *slowbStats) String() string {
	if len(s.ds) == 0 {
		return s.name + " n=0"
	}
	c := append([]time.Duration(nil), s.ds...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	q := func(p float64) time.Duration { return c[int(p*float64(len(c)-1))] }
	over1 := 0
	for _, d := range c {
		if d > time.Second {
			over1++
		}
	}
	return fmt.Sprintf("%s n=%d p50=%v p90=%v p99=%v max=%v over1s=%d", s.name, len(c), q(.5).Round(time.Millisecond), q(.9).Round(time.Millisecond), q(.99).Round(time.Millisecond), c[len(c)-1].Round(time.Millisecond), over1)
}

// clientLoop runs BEGIN / CREATE / COMMIT cycles until stop closes.
func (l *slowbLab) clientLoop(phase string, stop <-chan struct{}, dumpAfter time.Duration) (begin, create, commit *slowbStats) {
	begin, create, commit = &slowbStats{name: "BEGIN"}, &slowbStats{name: "CREATE"}, &slowbStats{name: "COMMIT"}
	var dumped atomic.Int32
	i := 0
	for {
		select {
		case <-stop:
			return
		default:
		}
		i++
		step := func(name string, path string, body any, st *slowbStats) map[string]any {
			at := time.Since(l.start)
			timer := time.AfterFunc(dumpAfter, func() {
				if dumped.Add(1) <= int32(slowbEnvInt("SLOWB_MAX_DUMPS", 3)) {
					slowbDump(fmt.Sprintf("phase=%s step=%s cycle=%d started=+%.3fs in-flight>%v", phase, name, i, at.Seconds(), dumpAfter))
				}
			})
			out, code, d := l.post(path, body)
			timer.Stop()
			st.add(d)
			if d > dumpAfter/2 || code >= 300 {
				fmt.Printf("SLOWB SLOW phase=%s step=%s cycle=%d started=+%.3fs took=%v code=%d\n", phase, name, i, at.Seconds(), d.Round(time.Millisecond), code)
			}
			return out
		}
		opened := step("BEGIN", "/db/nornic/tx", map[string]any{"statements": []any{}}, begin)
		commitURL, _ := opened["commit"].(string)
		u, err := url.Parse(commitURL)
		if err != nil || commitURL == "" {
			fmt.Printf("SLOWB BEGIN-FAILED phase=%s cycle=%d %v\n", phase, i, opened)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		txPath := strings.TrimSuffix(u.Path, "/commit")
		step("CREATE", txPath, map[string]any{"statements": []any{map[string]any{"statement": "CREATE (p:Probe {i: $i, text: $t})", "parameters": map[string]any{"i": i, "t": fmt.Sprintf("probe %d", i)}}}}, create)
		step("COMMIT", u.Path, map[string]any{"statements": []any{}}, commit)
		time.Sleep(100 * time.Millisecond)
	}
}

func (l *slowbLab) runPhase(phase string, d time.Duration, dumpAfter time.Duration, during func(stop chan struct{})) {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var b, c, m *slowbStats
	wg.Add(1)
	go func() { defer wg.Done(); b, c, m = l.clientLoop(phase, stop, dumpAfter) }()
	if during != nil {
		during(stop)
	} else {
		time.Sleep(d)
		close(stop)
	}
	wg.Wait()
	fmt.Printf("SLOWB PHASE %s %s | %s | %s\n", phase, b, c, m)
}

func TestZZSlowBoundaries(t *testing.T) {
	n := slowbEnvInt("SLOWB_N", 20000)
	pending := slowbEnvInt("SLOWB_PENDING", 300)
	chunks := slowbEnvInt("SLOWB_CHUNKS", 1)
	workerSeconds := slowbEnvInt("SLOWB_WORKER_SECONDS", 120)
	dumpAfterMs := slowbEnvInt("SLOWB_DUMP_AFTER_MS", 2000)
	dumpAfter := time.Duration(dumpAfterMs) * time.Millisecond
	fmt.Printf("SLOWB START wall=%s\n", time.Now().UTC().Format("15:04:05.000"))
	fmt.Printf("SLOWB CONFIG N=%d pending=%d chunks=%d workerSeconds=%d dumpAfter=%v GOMAXPROCS=%d go=%s\n", n, pending, chunks, workerSeconds, dumpAfter, runtime.GOMAXPROCS(0), runtime.Version())

	root := t.TempDir()
	tee := &slowbLogTee{start: time.Now()}
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = true
	cfg.Memory.EmbeddingDimensions = 2048
	cfg.EmbeddingWorker.NumWorkers = 1
	cfg.EmbeddingWorker.PropertiesInclude = []string{"text"}
	cfg.EmbeddingWorker.IncludeLabels = false
	cfg.Logger = slog.New(slog.NewJSONHandler(tee, &slog.HandlerOptions{Level: slog.LevelInfo}))
	// SLOWB_SNAPSHOT_SECONDS: WAL auto-compaction interval (0 = product default 5m);
	// SLOWB_AUTOCOMPACT=off disables it. SLOWB_PHASES selects phases (default ABC).
	if s := slowbEnvInt("SLOWB_SNAPSHOT_SECONDS", 0); s > 0 {
		cfg.Database.WALSnapshotInterval = time.Duration(s) * time.Second
	}
	if os.Getenv("SLOWB_AUTOCOMPACT") == "off" {
		cfg.Database.WALAutoCompactionEnabled = false
	}
	fmt.Printf("SLOWB WALCONFIG autoCompaction=%v interval=%v syncMode=%s async=%v\n", cfg.Database.WALAutoCompactionEnabled, cfg.Database.WALSnapshotInterval, cfg.Database.WALSyncMode, cfg.Database.AsyncWritesEnabled)
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "test-remote-credential-key-32b!")
	db, err := nornicdb.Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	systemStorage := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system")
	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{SecurityEnabled: true, JWTSecret: []byte("test-secret-key-for-testing-only-32b")}, systemStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := slowbCreateUser(authenticator, "admin"); err != nil {
		t.Fatal(err)
	}
	scfg := DefaultConfig()
	scfg.Port = 0
	scfg.EmbeddingEnabled = false
	srv, err := New(db, authenticator, scfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	}()
	tok, err := slowbToken(authenticator)
	if err != nil {
		t.Fatal(err)
	}
	l := &slowbLab{t: t, db: db, srv: srv, router: srv.buildRouter(), authz: "Bearer " + tok, start: tee.start, root: root}

	// Seed: N documents (to get vectors) and P pending documents.
	seed0 := time.Now()
	text := strings.Repeat("lorem ipsum dolor sit amet ", slowbEnvInt("SLOWB_TEXT_BYTES", 1080)/27)
	batch := 1000
	if b := 2000000 / (len(text) + 64); b < batch {
		batch = b
	}
	for s := 0; s < n+pending; s += batch {
		e := s + batch
		if e > n+pending {
			e = n + pending
		}
		rows := make([]any, 0, e-s)
		for j := s; j < e; j++ {
			label := "Doc"
			if j >= n {
				label = "Fresh"
			}
			rows = append(rows, map[string]any{"id": fmt.Sprintf("d%d", j), "text": fmt.Sprintf("%d %s", j, text), "label": label})
		}
		l.cypher("UNWIND $rows AS r CREATE (d:Doc {id: r.id, text: r.text, kind: r.label})", map[string]any{"rows": rows})
	}
	fmt.Printf("SLOWB SEED nodes=%d in %v\n", n+pending, time.Since(seed0).Round(time.Millisecond))
	base := db.GetBaseStorageForManager()
	var inner storage.Engine = base
	if w, ok := base.(*storage.WALEngine); ok {
		inner = w.GetInnerEngine()
	}
	badgerEngine, _ := inner.(*storage.BadgerEngine)
	fmt.Printf("SLOWB ENGINE base=%T inner=%T\n", base, inner)
	docs, err := slowbNodesByLabel(storage.NewNamespacedEngine(base, "nornic"), "Doc")
	if err != nil {
		t.Fatal(err)
	}
	vec0 := time.Now()
	vectors := 0
	for _, d := range docs {
		if k, _ := d.Properties["kind"].(string); k != "Doc" {
			continue
		}
		embedded := storage.CopyNode(d)
		embedded.ID = storage.NodeID("nornic:" + string(d.ID))
		embedded.ChunkEmbeddings = make([][]float32, chunks)
		for c := range embedded.ChunkEmbeddings {
			v := make([]float32, 2048)
			for k := range v {
				v[k] = float32(vectors+c+1) + float32(k)/2048
			}
			embedded.ChunkEmbeddings[c] = v
		}
		embedded.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": chunks, "embedding_dimensions": 2048, "embedding_model": "slowb-standin"}
		if err := slowbSidecar(inner, embedded); err != nil {
			t.Fatal(err)
		}
		vectors++
	}
	fmt.Printf("SLOWB VECTORS stored=%d chunksPerNode=%d in %v\n", vectors, chunks, time.Since(vec0).Round(time.Millisecond))

	phases := os.Getenv("SLOWB_PHASES")
	if phases == "" {
		phases = "ABC"
	}
	// Phase A: baseline (SLOWB_A_SECONDS, default 15).
	if strings.Contains(phases, "A") {
		l.runPhase("A-baseline", time.Duration(slowbEnvInt("SLOWB_A_SECONDS", 15))*time.Second, dumpAfter, nil)
	}

	// Phase B: direct refresh calls, timed, with the client loop alongside.
	if !strings.Contains(phases, "B") {
	} else if badgerEngine != nil {
		l.runPhase("B-direct-refresh", 0, dumpAfter, func(stop chan struct{}) {
			for r := 0; r < 2; r++ {
				t0 := time.Now()
				added := badgerEngine.RefreshPendingEmbeddingsIndex()
				fmt.Printf("SLOWB REFRESH direct run=%d returned=%d took=%v at=+%.3fs\n", r, added, time.Since(t0).Round(time.Millisecond), time.Since(l.start).Seconds())
			}
			time.Sleep(2 * time.Second)
			close(stop)
		})
	} else {
		fmt.Printf("SLOWB REFRESH direct skipped: inner engine is %T\n", inner)
	}

	// Phase C: the real worker with the stand-in embedder.
	if !strings.Contains(phases, "C") {
		return
	}
	emb := &slowbEmbedder{dims: 2048}
	l.runPhase("C-worker", 0, dumpAfter, func(stop chan struct{}) {
		at := time.Since(l.start)
		ok := db.SetEmbedder(emb)
		fmt.Printf("SLOWB WORKER SetEmbedder=%v at=+%.3fs\n", ok, at.Seconds())
		deadline := time.Now().Add(time.Duration(workerSeconds) * time.Second)
		last := int64(-1)
		for time.Now().Before(deadline) {
			time.Sleep(5 * time.Second)
			if c := emb.calls.Load(); c != last {
				fmt.Printf("SLOWB WORKER embedCalls=%d at=+%.3fs\n", c, time.Since(l.start).Seconds())
				last = c
			}
		}
		close(stop)
	})
	_ = io.Discard
}

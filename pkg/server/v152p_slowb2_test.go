package server

// Investigation probe 2 (untracked). Uses the helpers of zz_slowb_probe_test.go.
//
//   TestZZSlowB2Failures   (a) GET /nornicdb/embed/failures timed at store sizes
//                              and limits (SLOWB2_SIZES="10000x1,40000x1,...").
//   TestZZSlowB2CDC        (b) CALL db.cdc.current() every 200 ms across WAL
//                              compaction ticks, goroutine dump when slow.
//   TestZZSlowB2Duplicates (d) provider calls per node while the pending-embeddings
//                              refresh runs (worker alone, then with extra refreshes).
//   TestZZSlowB2Startup    (e) GET /status polled while a reopened store starts.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
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

// ---- shared setup -------------------------------------------------------

type slowb2Opts struct {
	embedding  bool
	workers    int
	compaction time.Duration // 0 = product default, <0 = off
}

func slowb2Config(o slowb2Opts, tee *slowbLogTee) *nornicdb.Config {
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = o.embedding
	cfg.Memory.EmbeddingDimensions = 2048
	if o.workers > 0 {
		cfg.EmbeddingWorker.NumWorkers = o.workers
	}
	cfg.EmbeddingWorker.PropertiesInclude = []string{"text"}
	cfg.EmbeddingWorker.IncludeLabels = false
	cfg.Logger = slog.New(slog.NewJSONHandler(tee, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if o.compaction > 0 {
		cfg.Database.WALSnapshotInterval = o.compaction
	} else if o.compaction < 0 {
		cfg.Database.WALAutoCompactionEnabled = false
	}
	return cfg
}

// slowb2Open opens a DB at root and an HTTP server on it. The returned
// closer stops the server and closes the DB.
func slowb2Open(t *testing.T, root string, o slowb2Opts, tee *slowbLogTee) (*slowbLab, func(), time.Duration) {
	t.Helper()
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "test-remote-credential-key-32b!")
	cfg := slowb2Config(o, tee)
	fmt.Printf("SLOWB2 WALCONFIG autoCompaction=%v interval=%v embedding=%v workers=%d\n", cfg.Database.WALAutoCompactionEnabled, cfg.Database.WALSnapshotInterval, cfg.Memory.EmbeddingEnabled, cfg.EmbeddingWorker.NumWorkers)
	t0 := time.Now()
	db, err := nornicdb.Open(root, cfg)
	openTook := time.Since(t0)
	if err != nil {
		t.Fatal(err)
	}
	systemStorage := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system")
	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{SecurityEnabled: true, JWTSecret: []byte("test-secret-key-for-testing-only-32b")}, systemStorage)
	if err != nil {
		t.Fatal(err)
	}
	_ = slowbCreateUser(authenticator, "admin") // exists after a reopen
	scfg := DefaultConfig()
	scfg.Port = 0
	scfg.EmbeddingEnabled = false
	srv, err := New(db, authenticator, scfg)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := slowbToken(authenticator)
	if err != nil {
		t.Fatal(err)
	}
	l := &slowbLab{t: t, db: db, srv: srv, router: srv.buildRouter(), authz: "Bearer " + tok, start: tee.start, root: root}
	closed := false
	closer := func() {
		if closed {
			return
		}
		closed = true
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
		_ = db.Close()
	}
	t.Cleanup(closer)
	return l, closer, openTook
}

func (l *slowbLab) get(path string) (map[string]any, int, time.Duration, string) {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", l.authz)
	rec := httptest.NewRecorder()
	t0 := time.Now()
	l.router.ServeHTTP(rec, req)
	d := time.Since(t0)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return out, rec.Code, d, rec.Body.String()
}

func (l *slowbLab) cypherOn(db, q string, params map[string]any) (map[string]any, int, time.Duration) {
	return l.post("/db/"+db+"/tx/commit", map[string]any{"statements": []any{map[string]any{"statement": q, "parameters": params}}})
}

func slowb2Text(bytes int) string {
	return strings.Repeat("lorem ipsum dolor sit amet ", bytes/27)
}

// slowb2Seed creates :Doc nodes ids prefix+j for j in [from,to) with kind.
func slowb2Seed(l *slowbLab, prefix string, from, to int, text string, kind string) {
	batch := 1000
	if b := 2000000 / (len(text) + 64); b < batch {
		batch = b
	}
	for s := from; s < to; s += batch {
		e := s + batch
		if e > to {
			e = to
		}
		rows := make([]any, 0, e-s)
		for j := s; j < e; j++ {
			rows = append(rows, map[string]any{"id": fmt.Sprintf("%s%d", prefix, j), "text": fmt.Sprintf("%d %s", j, text), "label": kind})
		}
		l.cypher("UNWIND $rows AS r CREATE (d:Doc {id: r.id, text: r.text, kind: r.label})", map[string]any{"rows": rows})
	}
}

func slowb2Inner(db *nornicdb.DB) (storage.Engine, *storage.BadgerEngine) {
	base := db.GetBaseStorageForManager()
	var inner storage.Engine = base
	if w, ok := base.(*storage.WALEngine); ok {
		inner = w.GetInnerEngine()
	}
	b, _ := inner.(*storage.BadgerEngine)
	return inner, b
}

// slowb2Docs returns the stored :Doc nodes of the given kind.
func slowb2Docs(t *testing.T, db *nornicdb.DB, kind string) []*storage.Node {
	docs, err := slowbNodesByLabel(storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "nornic"), "Doc")
	if err != nil {
		t.Fatal(err)
	}
	out := make([]*storage.Node, 0, len(docs))
	for _, d := range docs {
		if k, _ := d.Properties["kind"].(string); k == kind {
			out = append(out, d)
		}
	}
	return out
}

// slowb2Vectors stores `chunks` 2,048-dim chunk vectors for every :Doc of kind "Doc".
func slowb2Vectors(t *testing.T, db *nornicdb.DB, chunks int) int {
	inner, _ := slowb2Inner(db)
	n := 0
	for _, d := range slowb2Docs(t, db, "Doc") {
		embedded := storage.CopyNode(d)
		embedded.ID = storage.NodeID("nornic:" + string(d.ID))
		embedded.ChunkEmbeddings = make([][]float32, chunks)
		for c := range embedded.ChunkEmbeddings {
			v := make([]float32, 2048)
			for k := range v {
				v[k] = float32(n+c+1) + float32(k)/2048
			}
			embedded.ChunkEmbeddings[c] = v
		}
		embedded.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": chunks, "embedding_dimensions": 2048, "embedding_model": "slowb-standin"}
		if err := slowbSidecar(inner, embedded); err != nil {
			t.Fatal(err)
		}
		n++
	}
	return n
}

// slowb2Park writes the permanent-failure marker the worker writes
// (markNodeEmbeddingFailed) for every :Doc of kind "Failed".
func slowb2Park(t *testing.T, db *nornicdb.DB) (int, error) {
	inner, _ := slowb2Inner(db)
	n := 0
	for _, d := range slowb2Docs(t, db, "Failed") {
		failed := storage.CopyNode(d)
		failed.ID = storage.NodeID("nornic:" + string(d.ID))
		failed.ChunkEmbeddings = nil
		failed.EmbedMeta = map[string]any{"has_embedding": false, "embedding_failed": true, "embedding_error": "slowb2 stand-in failure", "embedding_failed_at": time.Now().UTC().Format(time.RFC3339)}
		if err := slowbSidecar(inner, failed); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func slowb2EnvStr(name, def string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return def
}

func slowb2Durs(ds []time.Duration) string {
	c := append([]time.Duration(nil), ds...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	parts := make([]string, len(ds))
	for i, d := range ds {
		parts[i] = d.Round(time.Millisecond).String()
	}
	return fmt.Sprintf("min=%v median=%v max=%v all=[%s]", c[0].Round(time.Millisecond), c[len(c)/2].Round(time.Millisecond), c[len(c)-1].Round(time.Millisecond), strings.Join(parts, " "))
}

// ---- stand-in embedder that counts calls per node -------------------------

var slowb2NodeNum = regexp.MustCompile(`\d+`)

type slowb2Embedder struct {
	dims    int
	delay   time.Duration
	calls   atomic.Int64
	texts   atomic.Int64
	mu      sync.Mutex
	perNode map[string]int
}

func (e *slowb2Embedder) record(text string) {
	key := slowb2NodeNum.FindString(text)
	e.mu.Lock()
	if e.perNode == nil {
		e.perNode = map[string]int{}
	}
	e.perNode[key]++
	e.mu.Unlock()
	e.texts.Add(1)
}

func (e *slowb2Embedder) vec() []float32 {
	n := e.texts.Load()
	v := make([]float32, e.dims)
	for i := range v {
		v[i] = float32((int64(i)+n)%97) / 97
	}
	return v
}
func (e *slowb2Embedder) Embed(_ context.Context, text string) ([]float32, error) {
	e.calls.Add(1)
	e.record(text)
	time.Sleep(e.delay)
	return e.vec(), nil
}
func (e *slowb2Embedder) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	e.calls.Add(1)
	for _, t := range texts {
		e.record(t)
	}
	time.Sleep(e.delay)
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i] = e.vec()
	}
	return out, nil
}
func (e *slowb2Embedder) ChunkText(text string, _, _ int) ([]string, error) { return []string{text}, nil }
func (e *slowb2Embedder) Dimensions() int                                  { return e.dims }
func (e *slowb2Embedder) Model() string                                    { return "slowb-standin" }
func (e *slowb2Embedder) Backend() string                                  { return "cpu" }

// counts returns, for node numbers in [from,to): distinct nodes embedded,
// nodes embedded more than once, total texts, and the first repeated ones.
func (e *slowb2Embedder) counts(from, to int) (distinct, repeated, total int, sample []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	keys := make([]string, 0)
	for k, c := range e.perNode {
		n, err := strconv.Atoi(k)
		if err != nil || n < from || n >= to {
			continue
		}
		distinct++
		total += c
		if c > 1 {
			repeated++
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for i, k := range keys {
		if i >= 20 {
			break
		}
		sample = append(sample, fmt.Sprintf("d%s x%d", k, e.perNode[k]))
	}
	return
}

// ---- (a) embed/failures ---------------------------------------------------

func TestZZSlowB2Failures(t *testing.T) {
	parked := slowbEnvInt("SLOWB2_PARKED", 20)
	reps := slowbEnvInt("SLOWB2_REPS", 5)
	text := slowb2Text(slowbEnvInt("SLOWB_TEXT_BYTES", 1080))
	fmt.Printf("SLOWB2 START test=failures wall=%s\n", time.Now().UTC().Format("15:04:05.000"))
	for _, spec := range strings.Split(slowb2EnvStr("SLOWB2_SIZES", "10000x1,40000x1"), ",") {
		parts := strings.SplitN(spec, "x", 2)
		n, _ := strconv.Atoi(parts[0])
		chunks := 1
		if len(parts) == 2 {
			chunks, _ = strconv.Atoi(parts[1])
		}
		t.Run(spec, func(t *testing.T) {
			tee := &slowbLogTee{start: time.Now()}
			l, closer, _ := slowb2Open(t, t.TempDir(), slowb2Opts{embedding: true, workers: 1, compaction: -1}, tee)
			defer closer()
			t0 := time.Now()
			slowb2Seed(l, "d", 0, n, text, "Doc")
			slowb2Seed(l, "f", 0, parked, text, "Failed")
			vecs := slowb2Vectors(t, l.db, chunks)
			np, perr := slowb2Park(t, l.db)
			fmt.Printf("SLOWB2 FAILURES store=%s nodes=%d vectors=%d chunksPerNode=%d chunkKeys=%d parkedWritten=%d parkErr=%v seed=%v\n", spec, n+parked, vecs, chunks, vecs*chunks, np, perr, time.Since(t0).Round(time.Millisecond))
			ok := l.db.SetEmbedder(&slowb2Embedder{dims: 2048})
			time.Sleep(3 * time.Second) // let the worker start and settle
			fmt.Printf("SLOWB2 FAILURES store=%s SetEmbedder=%v pending=%d\n", spec, ok, l.db.PendingEmbeddingsCount())
			for _, limit := range []int{1, 1000} {
				ds := make([]time.Duration, 0, reps)
				var returned, count any
				var code int
				for r := 0; r < reps; r++ {
					out, c, d, body := l.get(fmt.Sprintf("/nornicdb/embed/failures?limit=%d", limit))
					code = c
					ds = append(ds, d)
					returned, count = out["returned"], out["count"]
					if c != 200 && r == 0 {
						fmt.Printf("SLOWB2 FAILURES non-200 body=%.300s\n", body)
					}
				}
				fmt.Printf("SLOWB2 FAILURES store=%s limit=%d code=%d returned=%v count=%v %s\n", spec, limit, code, returned, count, slowb2Durs(ds))
			}
		})
	}
}

// ---- (b) db.cdc.current() across compaction ticks -------------------------

func TestZZSlowB2CDC(t *testing.T) {
	n := slowbEnvInt("SLOWB_N", 40000)
	seconds := slowbEnvInt("SLOWB2_SECONDS", 100)
	dumpAfter := time.Duration(slowbEnvInt("SLOWB_DUMP_AFTER_MS", 700)) * time.Millisecond
	maxDumps := int32(slowbEnvInt("SLOWB_MAX_DUMPS", 3))
	snap := time.Duration(slowbEnvInt("SLOWB_SNAPSHOT_SECONDS", 30)) * time.Second
	text := slowb2Text(slowbEnvInt("SLOWB_TEXT_BYTES", 20000))
	fmt.Printf("SLOWB2 START test=cdc wall=%s N=%d textBytes=%d snapshotEvery=%v dumpAfter=%v\n", time.Now().UTC().Format("15:04:05.000"), n, len(text), snap, dumpAfter)
	tee := &slowbLogTee{start: time.Now()}
	l, closer, _ := slowb2Open(t, t.TempDir(), slowb2Opts{embedding: false, compaction: snap}, tee)
	defer closer()
	t0 := time.Now()
	slowb2Seed(l, "d", 0, n, text, "Doc")
	fmt.Printf("SLOWB2 CDC seeded nodes=%d in %v at=+%.3fs\n", n, time.Since(t0).Round(time.Millisecond), time.Since(l.start).Seconds())
	out, code, _ := l.cypherOn("system", "ALTER DATABASE nornic SET OPTION txLogEnrichment 'FULL'", nil)
	fmt.Printf("SLOWB2 CDC enable code=%d errors=%v\n", code, out["errors"])

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var dumpMu sync.Mutex
	dumpsBy := map[string]int32{}
	dumpOK := func(kind string) bool {
		dumpMu.Lock()
		defer dumpMu.Unlock()
		dumpsBy[kind]++
		return dumpsBy[kind] <= maxDumps
	}
	timed := func(kind string, i int, q string) (time.Duration, int, map[string]any) {
		at := time.Since(l.start)
		timer := time.AfterFunc(dumpAfter, func() {
			if dumpOK(kind) {
				slowbDump(fmt.Sprintf("kind=%s i=%d started=+%.3fs in-flight>%v", kind, i, at.Seconds(), dumpAfter))
			}
		})
		o, c, d := l.cypherOn("nornic", q, map[string]any{"i": i})
		timer.Stop()
		if d > dumpAfter/2 || c >= 300 {
			fmt.Printf("SLOWB2 SLOW kind=%s i=%d started=+%.3fs took=%v code=%d\n", kind, i, at.Seconds(), d.Round(time.Millisecond), c)
		}
		return d, c, o
	}
	// Writer: one auto-commit write per second, so every compaction tick has work.
	wg.Add(1)
	writes := &slowbStats{name: "WRITE"}
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(time.Second):
			}
			d, _, _ := timed("WRITE", i, "CREATE (:Probe {i: $i})")
			writes.add(d)
		}
	}()
	// Explicit BEGIN every 200 ms (SLOWB2_BEGIN=1), rolled back at once: does a
	// BEGIN wait on the journal lock while compaction truncates the WAL?
	begins := &slowbStats{name: "BEGIN"}
	if os.Getenv("SLOWB2_BEGIN") == "1" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				case <-time.After(200 * time.Millisecond):
				}
				at := time.Since(l.start)
				timer := time.AfterFunc(dumpAfter, func() {
					if dumpOK("BEGIN") {
						slowbDump(fmt.Sprintf("kind=BEGIN i=%d started=+%.3fs in-flight>%v", i, at.Seconds(), dumpAfter))
					}
				})
				o, c, d := l.post("/db/nornic/tx", map[string]any{"statements": []any{}})
				timer.Stop()
				begins.add(d)
				if d > dumpAfter/2 || c >= 300 {
					fmt.Printf("SLOWB2 SLOW kind=BEGIN i=%d started=+%.3fs took=%v code=%d\n", i, at.Seconds(), d.Round(time.Millisecond), c)
				}
				if commitURL, _ := o["commit"].(string); commitURL != "" {
					txPath := strings.TrimSuffix(commitURL, "/commit")
					if k := strings.Index(txPath, "/db/"); k >= 0 {
						txPath = txPath[k:]
					}
					req := httptest.NewRequest(http.MethodDelete, txPath, nil)
					req.Header.Set("Authorization", l.authz)
					l.router.ServeHTTP(httptest.NewRecorder(), req)
				}
			}
		}()
	}
	current := &slowbStats{name: "CDC_CURRENT"}
	var first map[string]any
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		d, c, o := timed("CDC_CURRENT", i, "CALL db.cdc.current()")
		if first == nil {
			first = o
			fmt.Printf("SLOWB2 CDC first code=%d response=%.300v\n", c, o)
		}
		current.add(d)
		time.Sleep(200 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	slowbSnapshots(l.root)
	fmt.Printf("SLOWB2 CDC RESULT %s | %s | %s\n", current, writes, begins)
}

// ---- (d) duplicate provider calls -----------------------------------------

func TestZZSlowB2Duplicates(t *testing.T) {
	n := slowbEnvInt("SLOWB_N", 10000)
	pending := slowbEnvInt("SLOWB_PENDING", 200)
	workers := slowbEnvInt("SLOWB2_WORKERS", 4)
	delay := time.Duration(slowbEnvInt("SLOWB2_EMBED_DELAY_MS", 150)) * time.Millisecond
	maxWait := time.Duration(slowbEnvInt("SLOWB2_MAX_WAIT_SECONDS", 300)) * time.Second
	text := slowb2Text(slowbEnvInt("SLOWB_TEXT_BYTES", 1080))
	fmt.Printf("SLOWB2 START test=duplicates wall=%s N=%d pending=%d workers=%d delay=%v\n", time.Now().UTC().Format("15:04:05.000"), n, pending, workers, delay)
	tee := &slowbLogTee{start: time.Now()}
	l, closer, _ := slowb2Open(t, t.TempDir(), slowb2Opts{embedding: true, workers: workers, compaction: -1}, tee)
	defer closer()
	slowb2Seed(l, "d", 0, n, text, "Doc")
	vecs := slowb2Vectors(t, l.db, 1)
	_, badgerEngine := slowb2Inner(l.db)
	if badgerEngine != nil {
		t0 := time.Now()
		added := badgerEngine.RefreshPendingEmbeddingsIndex()
		fmt.Printf("SLOWB2 DUP refresh-cost N=%d returned=%d took=%v\n", n, added, time.Since(t0).Round(time.Millisecond))
	}
	emb := &slowb2Embedder{dims: 2048, delay: delay}

	waitDrained := func(phase string, from, to int) {
		deadline := time.Now().Add(maxWait)
		drainedAt := time.Time{}
		for time.Now().Before(deadline) {
			time.Sleep(2 * time.Second)
			p := l.db.PendingEmbeddingsCount()
			distinct, repeated, total, _ := emb.counts(from, to)
			fmt.Printf("SLOWB2 DUP phase=%s at=+%.3fs pending=%d embeddedNodes=%d/%d repeatedNodes=%d texts=%d providerCalls=%d\n", phase, time.Since(l.start).Seconds(), p, distinct, to-from, repeated, total, emb.calls.Load())
			if p == 0 && distinct >= to-from {
				if drainedAt.IsZero() {
					drainedAt = time.Now()
				}
				if time.Since(drainedAt) > 45*time.Second { // past the 30 s refresh-on-empty
					break
				}
			}
		}
		distinct, repeated, total, sample := emb.counts(from, to)
		fmt.Printf("SLOWB2 DUP RESULT phase=%s nodes=%d embeddedNodes=%d repeatedNodes=%d texts=%d repeatedSample=%v\n", phase, to-from, distinct, repeated, total, sample)
	}

	// d1: worker alone, as in production (forced refresh at start, refresh on empty).
	slowb2Seed(l, "d", n, n+pending, text, "Fresh")
	fmt.Printf("SLOWB2 DUP vectors=%d fresh=%d at=+%.3fs\n", vecs, pending, time.Since(l.start).Seconds())
	ok := l.db.SetEmbedder(emb)
	fmt.Printf("SLOWB2 DUP SetEmbedder=%v at=+%.3fs\n", ok, time.Since(l.start).Seconds())
	waitDrained("d1-worker-only", n, n+pending)

	// d2: new pending nodes, with an extra RefreshPendingEmbeddingsIndex every
	// 3 s while the worker embeds them (what the worker's own refresh does when
	// it lands mid-pass).
	from, to := n+pending, n+2*pending
	stop := make(chan struct{})
	var wg sync.WaitGroup
	if badgerEngine != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				case <-time.After(3 * time.Second):
				}
				t0 := time.Now()
				added := badgerEngine.RefreshPendingEmbeddingsIndex()
				fmt.Printf("SLOWB2 DUP extra-refresh added=%d took=%v at=+%.3fs\n", added, time.Since(t0).Round(time.Millisecond), time.Since(l.start).Seconds())
			}
		}()
	}
	slowb2Seed(l, "d", from, to, text, "Fresh")
	waitDrained("d2-with-extra-refresh", from, to)
	close(stop)
	wg.Wait()
}

// ---- (e) /status while a reopened store starts ------------------------------

func TestZZSlowB2Startup(t *testing.T) {
	n := slowbEnvInt("SLOWB_N", 40000)
	chunks := slowbEnvInt("SLOWB_CHUNKS", 1)
	seconds := slowbEnvInt("SLOWB2_SECONDS", 90)
	dumpAfter := time.Duration(slowbEnvInt("SLOWB_DUMP_AFTER_MS", 2000)) * time.Millisecond
	maxDumps := int32(slowbEnvInt("SLOWB_MAX_DUMPS", 2))
	text := slowb2Text(slowbEnvInt("SLOWB_TEXT_BYTES", 1080))
	fmt.Printf("SLOWB2 START test=startup wall=%s N=%d chunks=%d\n", time.Now().UTC().Format("15:04:05.000"), n, chunks)
	root := t.TempDir()
	tee := &slowbLogTee{start: time.Now()}
	l, closer, _ := slowb2Open(t, root, slowb2Opts{embedding: true, workers: 1, compaction: -1}, tee)
	slowb2Seed(l, "d", 0, n, text, "Doc")
	vecs := slowb2Vectors(t, l.db, chunks)
	_, code, d, _ := l.get("/status")
	fmt.Printf("SLOWB2 STARTUP first-open vectors=%d status=%d took=%v\n", vecs, code, d.Round(time.Millisecond))
	closer()

	// Reopen and poll /status from the moment the server exists.
	tee2 := &slowbLogTee{start: time.Now()}
	reopen0 := time.Now()
	l2, closer2, openTook := slowb2Open(t, root, slowb2Opts{embedding: true, workers: 1, compaction: -1}, tee2)
	defer closer2()
	fmt.Printf("SLOWB2 STARTUP reopen: Open took=%v server ready at=+%.3fs\n", openTook.Round(time.Millisecond), time.Since(reopen0).Seconds())
	go func() {
		ok := l2.db.SetEmbedder(&slowb2Embedder{dims: 2048})
		fmt.Printf("SLOWB2 STARTUP SetEmbedder=%v at=+%.3fs\n", ok, time.Since(reopen0).Seconds())
	}()
	var dumped atomic.Int32
	readyStreak := 0
	stats := &slowbStats{name: "STATUS"}
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		at := time.Since(reopen0)
		timer := time.AfterFunc(dumpAfter, func() {
			if dumped.Add(1) <= maxDumps {
				slowbDump(fmt.Sprintf("status i=%d started=+%.3fs in-flight>%v", i, at.Seconds(), dumpAfter))
			}
		})
		out, c, d, body := l2.get("/status")
		timer.Stop()
		stats.add(d)
		phase := ""
		if st, ok := out["startup"].(map[string]any); ok {
			phase = fmt.Sprintf("%v building=%v ready=%v", st["phase"], st["search_building_databases"], st["search_ready_databases"])
		}
		emb := ""
		if e, ok := out["embeddings"].(map[string]any); ok {
			emb = fmt.Sprintf("%v pending=%v", e["status"], e["pending"])
		}
		line := fmt.Sprintf("SLOWB2 STATUS i=%d started=+%.3fs took=%v code=%d phase=[%s] embed=[%s]", i, at.Seconds(), d.Round(time.Millisecond), c, phase, emb)
		if c != 200 {
			line += fmt.Sprintf(" body=%.300s", strings.TrimSpace(body))
		}
		fmt.Println(line)
		if c == 200 && strings.HasPrefix(phase, "ready") {
			readyStreak++
		} else {
			readyStreak = 0
		}
		if readyStreak >= 20 && time.Since(reopen0) > 30*time.Second {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	fmt.Printf("SLOWB2 STARTUP RESULT %s\n", stats)
}

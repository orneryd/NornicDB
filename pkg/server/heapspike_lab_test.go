package server

// Lab-only reproduction of the 20 Nov 2026 engine heap spike (campaign lane
// pd-heap-spike). Not product code; overlaid on a pushed commit. Runs only
// with HS_RUN=1. Every figure is printed on lines starting "HS ".

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/pprof/profile"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ---------- settings ----------

func hsEnvF(k string, d float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return d
}
func hsEnvI(k string, d int) int { return int(hsEnvF(k, float64(d))) }
func hsEnvS(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

var hsStart = time.Now()

func hsLog(format string, args ...interface{}) {
	fmt.Printf("HS t=%7.2f "+format+"\n", append([]interface{}{time.Since(hsStart).Seconds()}, args...)...)
}

// ---------- fake 2048-dim embedder (no network; counts work) ----------

type hsEmbedder struct {
	dims        int
	calls       atomic.Int64
	texts       atomic.Int64
	chars       atomic.Int64
	chunkCalls  atomic.Int64
	chunkChars  atomic.Int64
	maxChunkIn  atomic.Int64
}

func (e *hsEmbedder) vec(text string) []float32 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(text[:min(len(text), 256)]))
	r := rand.New(rand.NewSource(int64(h.Sum64())))
	v := make([]float32, e.dims)
	for i := range v {
		v[i] = r.Float32() - 0.5
	}
	return v
}
func (e *hsEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	e.calls.Add(1)
	e.texts.Add(1)
	e.chars.Add(int64(len(text)))
	return e.vec(text), nil
}
func (e *hsEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	e.calls.Add(1)
	out := make([][]float32, len(texts))
	for i, t := range texts {
		e.texts.Add(1)
		e.chars.Add(int64(len(t)))
		out[i] = e.vec(t)
	}
	return out, nil
}
func (e *hsEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	e.chunkCalls.Add(1)
	e.chunkChars.Add(int64(len(text)))
	for {
		cur := e.maxChunkIn.Load()
		if int64(len(text)) <= cur || e.maxChunkIn.CompareAndSwap(cur, int64(len(text))) {
			break
		}
	}
	size := maxTokens * 4
	if size <= 0 {
		size = 32768
	}
	if len(text) <= size {
		return []string{text}, nil
	}
	var out []string
	for i := 0; i < len(text); i += size {
		out = append(out, text[i:min(len(text), i+size)])
	}
	return out, nil
}
func (e *hsEmbedder) Dimensions() int { return e.dims }
func (e *hsEmbedder) Model() string   { return "hs-fake-2048" }
func (e *hsEmbedder) Backend() string { return "cpu" }
func (e *hsEmbedder) Close() error    { return nil }

// ---------- memory sampler ----------

type hsSample struct {
	at                                  float64
	heapObj, live, goal, total, gcs     uint64
	rss, cgAnon, cgCur                  uint64
	phase                               string
}

type hsSampler struct {
	mu          sync.Mutex
	phase       string
	peak        map[string]hsSample // by metric name
	maxLive     uint64
	liveProf    []byte
	liveProfAt  float64
	maxHeapObj  uint64
	forcedLive  uint64
	forcedAt    float64
	forcedProf  []byte
	stop        chan struct{}
	done        chan struct{}
	gcPercent   uint64
	memLimit    uint64
	phaseObj    map[string]hsSample
	lastPrinted uint64
	phaseLive   map[string]hsSample
}

func hsReadKV(path, key string) uint64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, key) {
			fs := strings.Fields(strings.TrimPrefix(line, key))
			if len(fs) > 0 {
				v, _ := strconv.ParseUint(fs[0], 10, 64)
				if strings.HasSuffix(line, "kB") {
					v *= 1024
				}
				return v
			}
		}
	}
	return 0
}
func hsReadUint(path string) uint64 {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	v, _ := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	return v
}

func (s *hsSampler) setPhase(p string) {
	s.mu.Lock()
	s.phase = p
	s.mu.Unlock()
	hsLog("PHASE %s", p)
}

func (s *hsSampler) run(interval time.Duration, forceGC time.Duration) {
	names := []string{"/memory/classes/heap/objects:bytes", "/gc/heap/live:bytes", "/gc/heap/goal:bytes", "/memory/classes/total:bytes", "/gc/cycles/total:gc-cycles", "/gc/gogc:percent", "/gc/gomemlimit:bytes"}
	ms := make([]metrics.Sample, len(names))
	for i := range names {
		ms[i].Name = names[i]
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	lastPrint := time.Time{}
	lastForce := time.Now()
	for {
		select {
		case <-s.stop:
			close(s.done)
			return
		case <-tick.C:
		}
		if forceGC > 0 && time.Since(lastForce) >= forceGC {
			runtime.GC()
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			lastForce = time.Now()
			s.mu.Lock()
			newMax := m.HeapAlloc > s.forcedLive
			if newMax {
				s.forcedLive = m.HeapAlloc
				s.forcedAt = time.Since(hsStart).Seconds()
			}
			ph := s.phase
			s.mu.Unlock()
			if newMax {
				var buf bytes.Buffer
				if err := pprof.Lookup("heap").WriteTo(&buf, 0); err == nil {
					s.mu.Lock()
					s.forcedProf = buf.Bytes()
					s.mu.Unlock()
				}
			}
			hsLog("FORCEDGC live_after_gc_MiB=%d phase=%s", m.HeapAlloc>>20, ph)
		}
		metrics.Read(ms)
		v := func(i int) uint64 {
			if ms[i].Value.Kind() == metrics.KindUint64 {
				return ms[i].Value.Uint64()
			}
			return 0
		}
		smp := hsSample{at: time.Since(hsStart).Seconds(), heapObj: v(0), live: v(1), goal: v(2), total: v(3), gcs: v(4),
			rss: hsReadKV("/proc/self/status", "VmRSS:"), cgAnon: hsReadKV("/sys/fs/cgroup/memory.stat", "anon "), cgCur: hsReadUint("/sys/fs/cgroup/memory.current")}
		s.mu.Lock()
		s.gcPercent, s.memLimit = v(5), v(6)
		smp.phase = s.phase
		upd := func(name string, val uint64, get func(hsSample) uint64) {
			if cur, ok := s.peak[name]; !ok || val > get(cur) {
				s.peak[name] = smp
			}
		}
		if s.phaseObj == nil {
			s.phaseObj, s.phaseLive = map[string]hsSample{}, map[string]hsSample{}
		}
		if cur, ok := s.phaseObj[smp.phase]; !ok || smp.heapObj > cur.heapObj {
			s.phaseObj[smp.phase] = smp
		}
		if cur, ok := s.phaseLive[smp.phase]; !ok || smp.live > cur.live {
			s.phaseLive[smp.phase] = smp
		}
		upd("heapObjects", smp.heapObj, func(x hsSample) uint64 { return x.heapObj })
		upd("liveAtLastGC", smp.live, func(x hsSample) uint64 { return x.live })
		upd("goal", smp.goal, func(x hsSample) uint64 { return x.goal })
		upd("rss", smp.rss, func(x hsSample) uint64 { return x.rss })
		upd("cgroupAnon", smp.cgAnon, func(x hsSample) uint64 { return x.cgAnon })
		capture := smp.live > 128<<20 && float64(smp.live) > float64(s.maxLive)*1.10
		printNow := capture && os.Getenv("HS_PRINT_CAPTURES") == "1" && float64(smp.live) > float64(s.lastPrinted)*1.25
		if printNow {
			s.lastPrinted = smp.live
		}
		if smp.live > s.maxLive {
			s.maxLive = smp.live
		}
		if smp.heapObj > s.maxHeapObj {
			s.maxHeapObj = smp.heapObj
		}
		s.mu.Unlock()
		if capture {
			var buf bytes.Buffer
			if err := pprof.Lookup("heap").WriteTo(&buf, 0); err == nil {
				s.mu.Lock()
				s.liveProf = buf.Bytes()
				s.liveProfAt = smp.at
				s.mu.Unlock()
				if printNow {
					hsAnalyze(fmt.Sprintf("capture@%.1f-live%dMiB-%s", smp.at, smp.live>>20, smp.phase), buf.Bytes(), nil, "inuse_space", 12)
				}
			}
		}
		if time.Since(lastPrint) >= time.Second {
			lastPrint = time.Now()
			hsLog("MEM heapObj_MiB=%d live_MiB=%d goal_MiB=%d total_MiB=%d rss_MiB=%d cg_anon_MiB=%d cg_cur_MiB=%d gcs=%d phase=%s",
				smp.heapObj>>20, smp.live>>20, smp.goal>>20, smp.total>>20, smp.rss>>20, smp.cgAnon>>20, smp.cgCur>>20, smp.gcs, smp.phase)
		}
	}
}

// ---------- profile analysis ----------

func hsAnalyze(label string, prof []byte, base []byte, idxName string, top int) {
	p, err := profile.Parse(bytes.NewReader(prof))
	if err != nil {
		hsLog("PROFILE %s parse error %v", label, err)
		return
	}
	idx := -1
	for i, st := range p.SampleType {
		if st.Type == idxName {
			idx = i
		}
	}
	if idx < 0 {
		hsLog("PROFILE %s no %s", label, idxName)
		return
	}
	if base != nil {
		if bp, err := profile.Parse(bytes.NewReader(base)); err == nil {
			bp.Scale(-1)
			if merged, err := profile.Merge([]*profile.Profile{p, bp}); err == nil {
				p = merged
			}
		}
	}
	flat := map[string]int64{}
	cum := map[string]int64{}
	stacks := map[string]int64{}
	var total int64
	for _, s := range p.Sample {
		v := s.Value[idx]
		total += v
		seen := map[string]bool{}
		var frames []string
		first := true
		for _, loc := range s.Location {
			for _, ln := range loc.Line {
				if ln.Function == nil {
					continue
				}
				fn := ln.Function.Name
				if first {
					flat[fn] += v
					first = false
				}
				if !seen[fn] {
					seen[fn] = true
					cum[fn] += v
				}
				if strings.Contains(fn, "orneryd/nornicdb") && len(frames) < 10 {
					frames = append(frames, fmt.Sprintf("%s:%d", strings.TrimPrefix(fn, "github.com/orneryd/nornicdb/"), ln.Line))
				}
			}
		}
		stacks[strings.Join(frames, " < ")] += v
	}
	hsLog("PROFILE %s %s total_MiB=%d", label, idxName, total>>20)
	pr := func(kind string, m map[string]int64, filter bool) {
		type kv struct {
			k string
			v int64
		}
		var xs []kv
		for k, v := range m {
			if filter && !strings.Contains(k, "orneryd/nornicdb") {
				continue
			}
			xs = append(xs, kv{k, v})
		}
		sort.Slice(xs, func(i, j int) bool { return xs[i].v > xs[j].v })
		for i := 0; i < len(xs) && i < top; i++ {
			hsLog("PROFILE %s %s %s #%02d MiB=%d %s", label, idxName, kind, i+1, xs[i].v>>20, xs[i].k)
		}
	}
	pr("flat", flat, false)
	pr("cum", cum, true)
	pr("stack", stacks, false)
}

// ---------- data shape ----------

type hsColl struct {
	name                 string
	sources              int
	textChars, maxText   int
	propsPerNode         int
}

// Live figures of the four collections (report of 20 Nov, explicit read).
var hsCollections = []hsColl{
	{"disputes", 286, 8525368, 2664373, 48},
	{"max-docs-naturalisation", 1369, 25404905, 1488175, 51},
	{"toranos-evidence", 304, 2213282, 114147, 51},
	{"valena-llc", 104, 693371, 165422, 51},
}

type hsWords struct{ w []string }

func hsVocab(r *rand.Rand, n int) *hsWords {
	letters := "abcdefghijklmnopqrstuvwxyz"
	ws := make([]string, n)
	for i := range ws {
		l := 3 + r.Intn(8)
		b := make([]byte, l)
		for j := range b {
			b[j] = letters[r.Intn(26)]
		}
		ws[i] = string(b)
	}
	return &hsWords{ws}
}
func (v *hsWords) text(r *rand.Rand, n int) string {
	var b strings.Builder
	b.Grow(n + 16)
	for b.Len() < n {
		// Zipf-like: square of uniform skews to frequent words.
		f := r.Float64()
		b.WriteString(v.w[int(f*f*float64(len(v.w)))])
		b.WriteByte(' ')
	}
	return b.String()[:n]
}

func hsIntList(n, step int) []interface{} {
	out := make([]interface{}, n)
	for i := range out {
		out[i] = int64(i * step)
	}
	return out
}

// ---------- HTTP helpers (no t.Fatal from goroutines) ----------

type hsClient struct {
	h     http.Handler
	token string
	db    string
}

type hsResp struct {
	Commit string            `json:"commit"`
	Errors []json.RawMessage `json:"errors"`
}

func (c *hsClient) do(method, path string, body interface{}) (int, []byte, time.Duration, error) {
	var rd *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, 0, err
		}
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rd)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", c.token)
	rec := httptest.NewRecorder()
	st := time.Now()
	c.h.ServeHTTP(rec, req)
	return rec.Code, rec.Body.Bytes(), time.Since(st), nil
}

func hsStmts(q string, p map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{"statements": []map[string]interface{}{{"statement": q, "parameters": p}}}
}

func (c *hsClient) check(code int, body []byte) (*hsResp, error) {
	var r hsResp
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("status %d decode: %v: %.300s", code, err, body)
	}
	if code >= 300 || len(r.Errors) > 0 {
		return &r, fmt.Errorf("status %d errors: %.500s", code, body)
	}
	return &r, nil
}

func (c *hsClient) autocommit(q string, p map[string]interface{}) (time.Duration, int, error) {
	code, body, d, err := c.do(http.MethodPost, "/db/"+c.db+"/tx/commit", hsStmts(q, p))
	if err != nil {
		return d, 0, err
	}
	_, err = c.check(code, body)
	return d, len(body), err
}

type hsTx struct {
	c  *hsClient
	id string
}

func (c *hsClient) begin(q string, p map[string]interface{}) (*hsTx, time.Duration, int, error) {
	code, body, d, err := c.do(http.MethodPost, "/db/"+c.db+"/tx", hsStmts(q, p))
	if err != nil {
		return nil, d, 0, err
	}
	r, err := c.check(code, body)
	if err != nil {
		return nil, d, len(body), err
	}
	parts := strings.Split(r.Commit, "/")
	if len(parts) < 2 {
		return nil, d, len(body), fmt.Errorf("no commit url: %.200s", body)
	}
	return &hsTx{c, parts[len(parts)-2]}, d, len(body), nil
}
func (t *hsTx) run(q string, p map[string]interface{}) (time.Duration, int, error) {
	code, body, d, err := t.c.do(http.MethodPost, "/db/"+t.c.db+"/tx/"+t.id, hsStmts(q, p))
	if err != nil {
		return d, 0, err
	}
	_, err = t.c.check(code, body)
	return d, len(body), err
}
func (t *hsTx) commit() (time.Duration, error) {
	code, body, d, err := t.c.do(http.MethodPost, "/db/"+t.c.db+"/tx/"+t.id+"/commit", map[string]interface{}{"statements": []interface{}{}})
	if err != nil {
		return d, err
	}
	_, err = t.c.check(code, body)
	return d, err
}

// ---------- statements (shapes from the engine's slow-query log) ----------

const (
	hsTEXTS      = "UNWIND $ids AS id MATCH (s:PDSearchSource {id: id}) RETURN s.id AS id, s.text AS text, properties(s) AS properties"
	hsTEXTSNoProp = "UNWIND $ids AS id MATCH (s:PDSearchSource {id: id}) RETURN s.id AS id, s.text AS text"
	hsSIGNATURES = "UNWIND $sources AS x MATCH (s:PDSearchSource {id: x.source_id}) SET s.signature = x.signature, s.profile_run = x.run"
	// verbatim from the slow-query log (plan 66b94b92dc76549c)
	hsPROFILED = "UNWIND $sources AS x\nMATCH (s:PDSearchSource {id: x.source_id})\nSET s.set_keys = x.keys, s.set_weights = x.weights, s.set_total = x.total,\n    s.set_pool_runs = x.pool_runs, s.set_pool_keys = x.pool_keys,\n    s.set_pool_records = x.pool_records, s.profiled_current = x.current"
	hsGROUPS   = "UNWIND $groups AS g MERGE (grp:PDRecordGroup {id: g.id}) SET grp.keys = g.keys, grp.size = size(g.members) WITH grp, g UNWIND g.members AS m MATCH (s:PDSearchSource {id: m}) MERGE (s)-[:PD_IN_GROUP]->(grp)"
	// verbatim from the slow-query log (plan 7cd972a60e22e2ba)
	hsVIEWS = "UNWIND $sources AS x\nMATCH (s:PDSearchSource {id: x.source_id})\nSET s.view_text = x.view.view_text,\n    s.view_starts = x.view.view_starts,\n    s.view_ends = x.view.view_ends,\n    s.view_source_starts = x.view.view_source_starts,\n    s.view_source_ends = x.view.view_source_ends,\n    s.view_library = x.view.view_library,\n    s.view_own = x.view.view_own,\n    s.view_basis = x.basis,\n    s.read_observation = coalesce(x.observation, s.read_observation)"
	hsREADER = "MATCH (s:PDSearchSource) WHERE s.due_at IS NOT NULL WITH s ORDER BY s.id LIMIT 200 RETURN s.id AS id, s.view_text AS view, properties(s) AS properties"
)

// ---------- the test ----------

func TestHeapSpikeProfileWorkload(t *testing.T) {
	if os.Getenv("HS_RUN") != "1" {
		t.Skip("lab-only: set HS_RUN=1")
	}
	scales := hsEnvS("HS_SCALES", fmt.Sprintf("%g:%g", hsEnvF("HS_NODE_FRAC", 0.125), hsEnvF("HS_TEXT_FRAC", 0.25)))
	stopMiB := uint64(hsEnvI("HS_STOP_MIB", 7000))
	for _, sc := range strings.Split(scales, ",") {
		parts := strings.Split(sc, ":")
		nf, _ := strconv.ParseFloat(parts[0], 64)
		tf, _ := strconv.ParseFloat(parts[1], 64)
		var peak uint64
		t.Run(fmt.Sprintf("n%g_t%g", nf, tf), func(t *testing.T) { peak = hsRunOne(t, nf, tf) })
		runtime.GC()
		debug.FreeOSMemory()
		if peak>>20 > stopMiB {
			hsLog("STOP peak_MiB=%d exceeds HS_STOP_MIB=%d; larger scales skipped", peak>>20, stopMiB)
			break
		}
	}
}

func hsRunOne(t *testing.T, nodeFrac, textFrac float64) uint64 {
	hsStart = time.Now()
	chunkChars := hsEnvI("HS_CHUNK_CHARS", 2000)
	conc := hsEnvI("HS_CONC", 4)
	variant := hsEnvS("HS_VARIANT", "baseline")
	bgNodes := hsEnvI("HS_BG_NODES", 20000)
	forceGC := time.Duration(hsEnvI("HS_FORCE_GC_MS", 0)) * time.Millisecond
	readerEvery := time.Duration(hsEnvI("HS_READER_MS", 5000)) * time.Millisecond
	viewsPerWrite := hsEnvI("HS_VIEWS_PER_WRITE", 100)
	smallTx := hsEnvI("HS_SMALLTX", 50)
	memLimit := int64(hsEnvF("HS_MEMLIMIT", 59055800320))
	embedOn := hsEnvI("HS_EMBED", 1) == 1
	bgVectors := hsEnvI("HS_BG_VECTORS", 0) == 1

	// Production runtime memory settings: soft limit, GOGC unset (100).
	debug.SetMemoryLimit(memLimit)
	hsLog("CONFIG chunkChars=%d nodeFrac=%.4f textFrac=%.4f conc=%d variant=%s bgNodes=%d forceGC=%s readerEvery=%s viewsPerWrite=%d memLimit=%d embed=%v GOGC_env=%q gomaxprocs=%d",
		chunkChars, nodeFrac, textFrac, conc, variant, bgNodes, forceGC, readerEvery, viewsPerWrite, memLimit, embedOn, os.Getenv("GOGC"), runtime.GOMAXPROCS(0))

	emb := &hsEmbedder{dims: 2048}
	server, authenticator := setupTestServerWithDatabaseConfig(t, func(c *nornicdb.Config) {
		c.Database.MVCCRetentionMaxVersions = 100
		c.Database.WALSyncMode = "immediate"
		c.Database.AsyncWritesEnabled = false
		c.Database.MVCCLifecycleEnabled = true
		c.Database.MVCCLifecycleCycleInterval = 0
		c.Memory.EmbeddingDimensions = 2048
	}, nil)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	cl := &hsClient{h: server.buildRouter(), token: token, db: server.dbManager.DefaultDatabaseName()}
	store, err := server.dbManager.GetStorage(server.dbManager.DefaultDatabaseName())
	if err != nil {
		t.Fatal(err)
	}
	if embedOn {
		if !server.db.SetEmbedder(emb) {
			t.Fatal("SetEmbedder refused")
		}
	}
	for _, q := range []string{
		"CREATE INDEX hs_source_id IF NOT EXISTS FOR (s:PDSearchSource) ON (s.id)",
		"CREATE INDEX hs_group_id IF NOT EXISTS FOR (g:PDRecordGroup) ON (g.id)",
	} {
		if _, _, err := cl.autocommit(q, nil); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	// ---- load ----
	r := rand.New(rand.NewSource(20261120))
	vocab := hsVocab(r, 30000)
	ctx := context.Background()
	type src struct {
		id   string
		size int
	}
	collSrc := map[string][]src{}
	var totalText, totalNodes, totalChunks int
	for _, c := range hsCollections {
		n := int(float64(c.sources)*nodeFrac + 0.5)
		if n < 1 {
			n = 1
		}
		rest := 0
		if c.sources > 1 {
			rest = (c.textChars - c.maxText) / (c.sources - 1)
		}
		for i := 0; i < n; i++ {
			size := rest
			if i == 0 {
				size = c.maxText
			}
			size = int(float64(size) * textFrac)
			if size < 16 {
				size = 16
			}
			id := fmt.Sprintf("%s:src:%05d", c.name, i)
			text := vocab.text(r, size)
			props := map[string]interface{}{
				"id": id, "document_id": fmt.Sprintf("%s:doc:%05d", c.name, i), "collection": c.name,
				"text": text, "view_text": text, "due_at": "2026-11-20T04:00:00Z", "kind": "pdf", "title": "t " + id,
				"view_starts": hsIntList(size/2000+1, 2000), "view_ends": hsIntList(size/2000+1, 2000),
				"view_source_starts": hsIntList(size/2000+1, 2000), "view_source_ends": hsIntList(size/2000+1, 2000),
				"view_library": []interface{}{"a", "b", "c", "d", "e"}, "view_own": true, "view_basis": "basis", "read_observation": "obs",
			}
			for f := len(props); f < c.propsPerNode; f++ {
				props[fmt.Sprintf("f%02d", f)] = fmt.Sprintf("value-%d-%s", f, id)
			}
			var chunks [][]float32
			if chunkChars > 0 {
				for c0 := 0; c0 < size; c0 += chunkChars {
					chunks = append(chunks, emb.vec(text[c0:min(size, c0+chunkChars)]))
				}
				totalChunks += len(chunks)
			}
			if _, err := store.CreateNode(ctx, &storage.Node{ID: storage.NodeID(id), Labels: []string{"PDSearchSource"}, Properties: props, ChunkEmbeddings: chunks}); err != nil {
				t.Fatal(err)
			}
			collSrc[c.name] = append(collSrc[c.name], src{id, size})
			totalText += size
			totalNodes++
		}
	}
	for i := 0; i < bgNodes; i++ {
		id := fmt.Sprintf("bg:%06d", i)
		var bgVec [][]float32
		if bgVectors {
			bgVec = [][]float32{emb.vec(id)}
		}
		if _, err := store.CreateNode(ctx, &storage.Node{ID: storage.NodeID(id), Labels: []string{"PDOther"}, Properties: map[string]interface{}{"id": id, "name": vocab.text(r, 120)}, ChunkEmbeddings: bgVec}); err != nil {
			t.Fatal(err)
		}
	}
	hsLog("BGVECTORS %v", bgVectors)
	hsLog("LOADED sources=%d text_chars=%d view_chars=%d chunk_vectors=%d bg=%d", totalNodes, totalText, totalText, totalChunks, bgNodes)
	if embedOn {
		wctx, cancel := context.WithTimeout(ctx, 40*time.Minute)
		err := server.db.WaitForEmbeddings(wctx)
		cancel()
		hsLog("EMBED_LOAD err=%v calls=%d texts=%d chars=%d chunkCalls=%d chunkChars=%d maxChunkInput=%d", err, emb.calls.Load(), emb.texts.Load(), emb.chars.Load(), emb.chunkCalls.Load(), emb.chunkChars.Load(), emb.maxChunkIn.Load())
	}
	if err := server.db.BuildSearchIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)
	hsLog("BASELINE live_after_gc_MiB=%d sys_MiB=%d", m0.HeapAlloc>>20, m0.Sys>>20)
	var baseProf bytes.Buffer
	_ = pprof.Lookup("heap").WriteTo(&baseProf, 0)
	e0calls, e0texts, e0chars, e0chunk := emb.calls.Load(), emb.texts.Load(), emb.chars.Load(), emb.chunkChars.Load()

	// ---- workload ----
	smp := &hsSampler{peak: map[string]hsSample{}, stop: make(chan struct{}), done: make(chan struct{}), phase: "idle"}
	go smp.run(100*time.Millisecond, forceGC)
	stopAux := make(chan struct{})
	var auxWG sync.WaitGroup
	// reading selection, periodically
	auxWG.Add(2)
	go func() {
		defer auxWG.Done()
		tk := time.NewTicker(readerEvery)
		defer tk.Stop()
		for {
			select {
			case <-stopAux:
				return
			case <-tk.C:
			}
			d, n, err := cl.autocommit(hsREADER, nil)
			hsLog("READER dur_ms=%d bytes=%d err=%v", d.Milliseconds(), n, err)
		}
	}()
	// GET /nornicdb/embed/failures every 10 s
	go func() {
		defer auxWG.Done()
		tk := time.NewTicker(10 * time.Second)
		defer tk.Stop()
		for {
			select {
			case <-stopAux:
				return
			case <-tk.C:
			}
			code, body, d, _ := cl.do(http.MethodGet, "/nornicdb/embed/failures", nil)
			hsLog("EMBEDFAIL status=%d dur_ms=%d body=%.160s", code, d.Milliseconds(), body)
		}
	}()

	var mu sync.Mutex
	commitDur := map[string][]time.Duration{}
	var errs []string
	profileOne := func(coll string, srcs []src, rr *rand.Rand) error {
		ids := make([]interface{}, len(srcs))
		profiled := make([]interface{}, len(srcs))
		sigs := make([]interface{}, len(srcs))
		for i, s := range srcs {
			ids[i] = s.id
			keys, weights := make([]interface{}, 40), make([]interface{}, 40)
			for k := range keys {
				keys[k] = fmt.Sprintf("k%05d", rr.Intn(100000))
				weights[k] = rr.Float64()
			}
			strs := func(n int) []interface{} {
				o := make([]interface{}, n)
				for k := range o {
					o[k] = fmt.Sprintf("r%06d", rr.Intn(1000000))
				}
				return o
			}
			profiled[i] = map[string]interface{}{"source_id": s.id, "keys": keys, "weights": weights, "total": rr.Float64(), "pool_runs": strs(10), "pool_keys": strs(20), "pool_records": strs(20), "current": true}
			sigs[i] = map[string]interface{}{"source_id": s.id, "signature": fmt.Sprintf("sig-%d", rr.Int63()), "run": "run-1"}
		}
		var groups []interface{}
		for g := 0; g*10 < len(srcs); g++ {
			members := []interface{}{}
			for _, x := range ids[g*10 : min(len(ids), g*10+10)] {
				members = append(members, x)
			}
			groups = append(groups, map[string]interface{}{"id": fmt.Sprintf("%s:grp:%04d", coll, g), "keys": []interface{}{"a", "b"}, "members": members})
		}
		texts := hsTEXTS
		if variant == "noprops" {
			texts = hsTEXTSNoProp
		}
		batches := [][2]int{{0, len(srcs)}}
		if variant == "smalltx" {
			batches = nil
			for i := 0; i < len(srcs); i += smallTx {
				batches = append(batches, [2]int{i, min(len(srcs), i+smallTx)})
			}
		}
		for _, b := range batches {
			tx, d, n, err := cl.begin(texts, map[string]interface{}{"ids": ids[b[0]:b[1]]})
			if err != nil {
				return fmt.Errorf("TEXTS: %w", err)
			}
			hsLog("STEP %s TEXTS n=%d dur_ms=%d resp_bytes=%d", coll, b[1]-b[0], d.Milliseconds(), n)
			for _, st := range []struct {
				name, q string
				p       map[string]interface{}
			}{
				{"SIGNATURES", hsSIGNATURES, map[string]interface{}{"sources": sigs[b[0]:b[1]]}},
				{"PROFILED", hsPROFILED, map[string]interface{}{"sources": profiled[b[0]:b[1]]}},
			} {
				d, _, err := tx.run(st.q, st.p)
				if err != nil {
					return fmt.Errorf("%s: %w", st.name, err)
				}
				hsLog("STEP %s %s dur_ms=%d", coll, st.name, d.Milliseconds())
			}
			if b[0] == 0 {
				d, _, err := tx.run(hsGROUPS, map[string]interface{}{"groups": groups})
				if err != nil {
					return fmt.Errorf("GROUPS: %w", err)
				}
				hsLog("STEP %s GROUPS dur_ms=%d", coll, d.Milliseconds())
			}
			cd, err := tx.commit()
			if err != nil {
				return fmt.Errorf("COMMIT: %w", err)
			}
			mu.Lock()
			commitDur[coll] = append(commitDur[coll], cd)
			mu.Unlock()
			hsLog("STEP %s COMMIT dur_ms=%d", coll, cd.Milliseconds())
		}
		// views: VIEWS_PER_WRITE chunks, each its own transaction
		for i := 0; i < len(srcs); i += viewsPerWrite {
			var xs []interface{}
			for _, s := range srcs[i:min(len(srcs), i+viewsPerWrite)] {
				nt := vocab.text(rr, s.size)
				n := s.size/2000 + 1
				xs = append(xs, map[string]interface{}{"source_id": s.id, "basis": "b2", "observation": nil, "view": map[string]interface{}{
					"view_text": nt, "view_starts": hsIntList(n, 2000), "view_ends": hsIntList(n, 2000), "view_source_starts": hsIntList(n, 2000), "view_source_ends": hsIntList(n, 2000),
					"view_library": []interface{}{"a", "b", "c", "d", "e"}, "view_own": true}})
			}
			d, _, err := cl.autocommit(hsVIEWS, map[string]interface{}{"sources": xs})
			if err != nil {
				return fmt.Errorf("VIEWS: %w", err)
			}
			hsLog("STEP %s VIEWS n=%d dur_ms=%d", coll, len(xs), d.Milliseconds())
		}
		return nil
	}

	smp.setPhase("workload")
	wst := time.Now()
	sem := make(chan struct{}, max(1, conc))
	var wg sync.WaitGroup
	for ci, c := range hsCollections {
		wg.Add(1)
		go func(ci int, name string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := profileOne(name, collSrc[name], rand.New(rand.NewSource(int64(ci+7)))); err != nil {
				mu.Lock()
				errs = append(errs, name+": "+err.Error())
				mu.Unlock()
				hsLog("ERROR %s %v", name, err)
			}
		}(ci, c.name)
	}
	wg.Wait()
	hsLog("WORKLOAD_DONE dur_s=%.1f", time.Since(wst).Seconds())
	smp.setPhase("after-workload")
	var endProf bytes.Buffer
	_ = pprof.Lookup("heap").WriteTo(&endProf, 0)
	if embedOn {
		wctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
		err := server.db.WaitForEmbeddings(wctx)
		cancel()
		hsLog("EMBED_RUN err=%v calls=%d texts=%d chars=%d chunkChars=%d (deltas over workload+drain)", err, emb.calls.Load()-e0calls, emb.texts.Load()-e0texts, emb.chars.Load()-e0chars, emb.chunkChars.Load()-e0chunk)
	}
	smp.setPhase("drained")
	time.Sleep(15 * time.Second)
	close(stopAux)
	auxWG.Wait()
	close(smp.stop)
	<-smp.done
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	hsLog("END live_after_gc_MiB=%d", m1.HeapAlloc>>20)

	// ---- report ----
	for _, k := range []string{"heapObjects", "liveAtLastGC", "goal", "rss", "cgroupAnon"} {
		s := smp.peak[k]
		hsLog("PEAK %s at=%.1f heapObj_MiB=%d live_MiB=%d goal_MiB=%d rss_MiB=%d cg_anon_MiB=%d phase=%s", k, s.at, s.heapObj>>20, s.live>>20, s.goal>>20, s.rss>>20, s.cgAnon>>20, s.phase)
	}
	if forceGC > 0 {
		hsLog("PEAK forcedGCLive MiB=%d at=%.1f", smp.forcedLive>>20, smp.forcedAt)
	}
	for _, c := range hsCollections {
		var ms []string
		for _, d := range commitDur[c.name] {
			ms = append(ms, strconv.FormatInt(d.Milliseconds(), 10))
		}
		hsLog("COMMITS %s ms=[%s]", c.name, strings.Join(ms, ","))
	}
	hsLog("GC gogc_percent=%d gomemlimit=%d", smp.gcPercent, smp.memLimit)
	hsAnalyze("baseline", baseProf.Bytes(), nil, "inuse_space", 12)
	if smp.liveProf != nil {
		hsAnalyze(fmt.Sprintf("maxlive@%.1f", smp.liveProfAt), smp.liveProf, nil, "inuse_space", 25)
	}
	if smp.forcedProf != nil {
		hsAnalyze(fmt.Sprintf("forcedmax@%.1f", smp.forcedAt), smp.forcedProf, nil, "inuse_space", 25)
	}
	hsAnalyze("workload-allocs", endProf.Bytes(), baseProf.Bytes(), "alloc_space", 25)
	peak := smp.peak["heapObjects"]
	hsLog("SUMMARY variant=%s chunkChars=%d chunks=%d nodeFrac=%.4f textFrac=%.4f sources=%d text_chars=%d baseline_live_MiB=%d peak_heapObj_MiB=%d peak_live_MiB=%d peak_rss_MiB=%d peak_cg_anon_MiB=%d forced_live_MiB=%d workload_s=%.1f errors=%d",
		variant, chunkChars, totalChunks, nodeFrac, textFrac, totalNodes, totalText, m0.HeapAlloc>>20, peak.heapObj>>20, smp.maxLive>>20, smp.peak["rss"].rss>>20, smp.peak["cgroupAnon"].cgAnon>>20, smp.forcedLive>>20, time.Since(wst).Seconds(), len(errs))
	if len(errs) > 0 {
		t.Errorf("workload errors: %v", errs)
	}
	return peak.heapObj
}

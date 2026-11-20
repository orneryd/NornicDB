package server

// Helpers for the chunk-vector carry reproduction (chunk_vector_carry_test.go):
// a deterministic 2,048-dimension embedder, a heap sampler (runtime/metrics,
// RSS, cgroup), heap-profile summaries, a synthetic library and an HTTP
// transaction client. Every figure is printed on lines starting "HS ".

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
	"runtime/metrics"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/pprof/profile"
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
func (e *hsEmbedder) Model() string   { return "deterministic-2048" }
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

// The synthetic library: four collections with these source counts, total text,
// largest text and properties per source.
var hsCollections = []hsColl{
	{"collection-a", 286, 8525368, 2664373, 48},
	{"collection-b", 1369, 25404905, 1488175, 51},
	{"collection-c", 304, 2213282, 114147, 51},
	{"collection-d", 104, 693371, 165422, 51},
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


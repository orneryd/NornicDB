package server

// Commit-time cost of one records-profile transaction (lab harness, test only).
//
// Shape (as observed): one explicit HTTP transaction over N sources that carry
// chunk vectors of their view_text; statements: SIGNATURES (SET 7 non-embedded
// properties), PROFILED (SET 2), GROUPS and RUNSETS shapes with empty lists,
// then COMMIT. Embedding text from NORNICDB_EMBEDDING_PROPERTIES_INCLUDE
// (unset: engine default, the control). Runs only with CS_RUN=1.
//
// Settings (env):
//   CS_SIZES       comma list of N (sources updated), each run on a fresh server
//   CS_VIEW_CHARS  view_text chars per source (default 9780 = 13.19M/1349)
//   CS_TEXT_CHARS  text chars per source (default 18504 = 25.42M/1374)
//   CS_CHUNK_CHARS chars per chunk vector (default 512, ~128 tokens)
//   CS_EXTRA       extra sources loaded but not updated, as a multiple of N (default 1)
//   CS_SMALLTX     if >0, split the SETs into transactions of this many sources (control)
//   CS_DIMS        vector dimensions (default 2048)
//   CS_MVCC_RET    MVCC retention max versions (default 100)
//   CS_WAL_SYNC    WAL sync mode (default immediate)
//   CS_SEARCH      1: build search indexes before the transaction (default 1)
//   CS_PROFILE     1: CPU + alloc profiles of the COMMIT only (default 1)
//   CS_STACKS_MS   goroutine stack sample interval during COMMIT (default 2000, 0 off)
//   CS_BM25_PROPS  comma list: limit full-text indexing to these properties (default: engine default, all)
//   HS_MEMLIMIT    Go soft memory limit (default 18 GiB)

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/pprof/profile"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestCommitSpike(t *testing.T) {
	if os.Getenv("CS_RUN") != "1" {
		t.Skip("set CS_RUN=1 to run")
	}
	hsStart = time.Now()
	var sizes []int
	for _, s := range strings.Split(hsEnvS("CS_SIZES", "86,172,344"), ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && n > 0 {
			sizes = append(sizes, n)
		}
	}
	debug.SetMemoryLimit(int64(hsEnvF("HS_MEMLIMIT", 18*(1<<30))))
	for _, n := range sizes {
		n := n
		t.Run(fmt.Sprintf("N%d", n), func(t *testing.T) { csRunOne(t, n) })
		runtime.GC()
		debug.FreeOSMemory()
	}
}

func csAnalyze(label string, prof []byte, base []byte, idxName string, div int64, unit string, top int) {
	p, err := profile.Parse(bytes.NewReader(prof))
	if err != nil {
		hsLog("CSPROF %s parse error %v", label, err)
		return
	}
	idx := -1
	for i, st := range p.SampleType {
		if st.Type == idxName {
			idx = i
		}
	}
	if idx < 0 {
		hsLog("CSPROF %s no %s", label, idxName)
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
	cum := map[string]int64{}
	flat := map[string]int64{}
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
				if strings.Contains(fn, "orneryd/nornicdb") && len(frames) < 8 {
					frames = append(frames, fmt.Sprintf("%s:%d", strings.TrimPrefix(fn, "github.com/orneryd/nornicdb/"), ln.Line))
				}
			}
		}
		stacks[strings.Join(frames, " < ")] += v
	}
	hsLog("CSPROF %s %s total_%s=%d", label, idxName, unit, total/div)
	pr := func(kind string, m map[string]int64, filter bool, k int) {
		type kv struct {
			k string
			v int64
		}
		var xs []kv
		for key, v := range m {
			if filter && !strings.Contains(key, "orneryd/nornicdb") {
				continue
			}
			xs = append(xs, kv{key, v})
		}
		sort.Slice(xs, func(i, j int) bool { return xs[i].v > xs[j].v })
		for i := 0; i < len(xs) && i < k; i++ {
			hsLog("CSPROF %s %s %s #%02d %s=%d %s", label, idxName, kind, i+1, unit, xs[i].v/div, xs[i].k)
		}
	}
	pr("cum", cum, true, top)
	pr("flat", flat, false, 12)
	pr("stack", stacks, false, top)
}

func csRunOne(t *testing.T, n int) {
	viewChars := hsEnvI("CS_VIEW_CHARS", 9780)
	textChars := hsEnvI("CS_TEXT_CHARS", 18504)
	chunkChars := hsEnvI("CS_CHUNK_CHARS", 512)
	extra := hsEnvF("CS_EXTRA", 1)
	smallTx := hsEnvI("CS_SMALLTX", 0)
	dims := hsEnvI("CS_DIMS", 2048)
	mvccRet := hsEnvI("CS_MVCC_RET", 100)
	walSync := hsEnvS("CS_WAL_SYNC", "immediate")
	search := hsEnvI("CS_SEARCH", 1) == 1
	doProf := hsEnvI("CS_PROFILE", 1) == 1
	stacksMS := hsEnvI("CS_STACKS_MS", 2000)
	nExtra := int(float64(n) * extra)
	hsLog("CSCONFIG N=%d extra=%d view_chars=%d text_chars=%d chunk_chars=%d dims=%d smallTx=%d mvccRet=%d walSync=%s search=%v include=%q bm25=%q wal_env=%q gomaxprocs=%d",
		n, nExtra, viewChars, textChars, chunkChars, dims, smallTx, mvccRet, walSync, search, embedInclude(), hsEnvS("CS_BM25_PROPS", ""), os.Getenv("NORNICDB_WAL_ENABLED"), runtime.GOMAXPROCS(0))

	emb := &hsEmbedder{dims: dims}
	server, authenticator := setupTestServerWithDatabaseConfig(t, func(c *nornicdb.Config) {
		c.Database.MVCCRetentionMaxVersions = mvccRet
		c.Database.WALSyncMode = walSync
		c.Database.AsyncWritesEnabled = false
		c.Memory.EmbeddingDimensions = dims
		if inc := embedInclude(); inc != "" {
			c.EmbeddingWorker.PropertiesInclude = strings.Split(inc, ",")
		}
		if bp := hsEnvS("CS_BM25_PROPS", ""); bp != "" {
			c.Memory.SearchBM25Properties = strings.Split(bp, ",")
		}
	}, nil)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	cl := &hsClient{h: server.buildRouter(), token: token, db: server.dbManager.DefaultDatabaseName()}
	store, err := server.dbManager.GetStorage(server.dbManager.DefaultDatabaseName())
	if err != nil {
		t.Fatal(err)
	}
	if !server.db.SetEmbedder(emb) {
		t.Fatal("SetEmbedder refused")
	}
	for _, q := range []string{
		"CREATE INDEX cs_source_id IF NOT EXISTS FOR (s:Source) ON (s.id)",
		"CREATE INDEX cs_set_id IF NOT EXISTS FOR (g:RecordSet) ON (g.id)",
	} {
		if _, _, err := cl.autocommit(q, nil); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	ctx := context.Background()
	r := rand.New(rand.NewSource(20261121))
	vocab := hsVocab(r, 30000)
	var ids []string
	totalVectors, updVectors := 0, 0
	for i := 0; i < n+nExtra; i++ {
		id := fmt.Sprintf("src-%05d", i)
		text := vocab.text(r, textChars)
		view := vocab.text(r, viewChars)
		nk := min(2000, textChars/160+10)
		nc := min(68898, textChars/124)
		ns := textChars/830 + 1
		props := map[string]interface{}{
			"id": id, "document_id": "doc-" + id, "version_id": "ver-" + id, "text": text, "view_text": view,
			"view_starts": []interface{}{int64(0)}, "view_ends": []interface{}{int64(viewChars)},
			"view_source_starts": []interface{}{int64(0)}, "view_source_ends": []interface{}{int64(viewChars)},
			"view_library": []interface{}{"collection-b"}, "view_own": true, "view_basis": "vv|L|L|1|", "view_version": "vv", "read_observation": "obs",
			"set_keys": hs2Strs("k", nk, r), "set_weights": hs2Floats(nk, r), "set_total": r.Float64(),
			"set_pool_runs": hs2Strs("p", 5, r), "set_pool_keys": hs2Strs("q", 10, r), "set_pool_records": hs2Strs("w", 10, r),
			"cell_starts": hsIntList(nc, 124), "cell_ends": hsIntList(nc, 124), "cell_refs": hsIntList(nc, 1),
			"section_starts": hsIntList(ns, 830), "section_ends": hsIntList(ns, 830),
			"content_hash": "h" + id, "method": "parse", "page_count": int64(textChars/3000 + 1), "profiled_current": true,
			"profiled_run_layout": "L", "run_layout": "L", "record_profiled": int64(1), "search_hidden": false, "tool": "pdf",
		}
		var chunks [][]float32
		for c0 := 0; chunkChars > 0 && c0 < len(view); c0 += chunkChars {
			chunks = append(chunks, emb.vec(view[c0:min(len(view), c0+chunkChars)]))
		}
		if _, err := store.CreateNode(ctx, &storage.Node{ID: storage.NodeID(id), Labels: []string{"Source", "PDSearchSource"}, Properties: props, ChunkEmbeddings: chunks}); err != nil {
			t.Fatal(err)
		}
		totalVectors += len(chunks)
		if i < n {
			ids = append(ids, id)
			updVectors += len(chunks)
		}
	}
	if search {
		if err := server.db.BuildSearchIndexes(ctx); err != nil {
			t.Fatal(err)
		}
	}
	live := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	base := live()
	hsLog("CSLOADED N=%d sources_total=%d vectors_total=%d vectors_updated=%d live_MiB=%d", n, n+nExtra, totalVectors, updVectors, base>>20)

	smp := &hsSampler{peak: map[string]hsSample{}, stop: make(chan struct{}), done: make(chan struct{}), phase: "idle"}
	go smp.run(100*time.Millisecond, 0)
	defer func() {
		close(smp.stop)
		<-smp.done
	}()

	rr := rand.New(rand.NewSource(7))
	batch := n
	if smallTx > 0 {
		batch = smallTx
	}
	var stmtMS, commitMS []int64
	var liveInTx uint64
	for b0 := 0; b0 < n; b0 += batch {
		part := ids[b0:min(n, b0+batch)]
		var sigs, prof []interface{}
		for _, id := range part {
			nk := min(2000, textChars/160+10)
			sigs = append(sigs, map[string]interface{}{"source_id": id, "keys": hs2Strs("k", nk, rr), "weights": hs2Floats(nk, rr), "total": rr.Float64(),
				"pool_runs": hs2Strs("p", 5, rr), "pool_keys": hs2Strs("q", 10, rr), "pool_records": hs2Strs("w", 10, rr), "current": true})
			prof = append(prof, map[string]interface{}{"source_id": id, "generation": int64(2), "layout": "L2"})
		}
		smp.setPhase("stmts")
		tx, d0, _, err := cl.begin(hs2SIGNATURES, map[string]interface{}{"sources": sigs})
		if err != nil {
			t.Fatal(err)
		}
		d1, _, err := tx.run(hs2PROFILED, map[string]interface{}{"profiled": prof})
		if err != nil {
			t.Fatal(err)
		}
		d2, _, err := tx.run(hs2GROUPS, map[string]interface{}{"unlinked": []interface{}{}, "departed": []interface{}{}, "groups": []interface{}{}, "members": []interface{}{}, "links": []interface{}{}, "versions": []interface{}{}, "collection": "collection-b"})
		if err != nil {
			t.Fatal(err)
		}
		d3, _, err := tx.run(hs2RUNSETS, map[string]interface{}{"sources": []interface{}{}})
		if err != nil {
			t.Fatal(err)
		}
		stmtMS = append(stmtMS, d0.Milliseconds(), d1.Milliseconds(), d2.Milliseconds(), d3.Milliseconds())
		first := b0 == 0
		if first {
			liveInTx = live()
			hsLog("CSINTX N=%d live_MiB=%d held_MiB=%d held_per_updated_vector_KiB=%.1f held_per_node_KiB=%.1f stmt_ms=%d,%d,%d,%d",
				n, liveInTx>>20, (int64(liveInTx)-int64(base))>>20, float64(int64(liveInTx)-int64(base))/1024/float64(max(1, updVectors*len(part)/n)),
				float64(int64(liveInTx)-int64(base))/1024/float64(len(part)), d0.Milliseconds(), d1.Milliseconds(), d2.Milliseconds(), d3.Milliseconds())
		}
		var cpuBuf bytes.Buffer
		var allocBase []byte
		profiling := doProf && first
		if profiling {
			var ab bytes.Buffer
			runtime.GC()
			_ = pprof.Lookup("allocs").WriteTo(&ab, 0)
			allocBase = ab.Bytes()
			if err := pprof.StartCPUProfile(&cpuBuf); err != nil {
				hsLog("CSPROF cpu start error %v", err)
				profiling = false
			}
		}
		stopStacks := make(chan struct{})
		stacksDone := make(chan struct{})
		go func() {
			defer close(stacksDone)
			if stacksMS <= 0 || !first {
				return
			}
			tk := time.NewTicker(time.Duration(stacksMS) * time.Millisecond)
			defer tk.Stop()
			for {
				select {
				case <-stopStacks:
					return
				case <-tk.C:
				}
				var gb bytes.Buffer
				_ = pprof.Lookup("goroutine").WriteTo(&gb, 2)
				for _, g := range strings.Split(gb.String(), "\n\n") {
					hdr, _, _ := strings.Cut(g, "\n")
					mine := strings.Contains(g, "hsTx).commit")
					busy := strings.Contains(hdr, "[running]") || strings.Contains(hdr, "[runnable]")
					if !mine && !(busy && strings.Contains(g, "orneryd/nornicdb/pkg/")) {
						continue
					}
					var frames []string
					for _, ln := range strings.Split(g, "\n") {
						ln = strings.TrimSpace(ln)
						if strings.HasPrefix(ln, "github.com/orneryd/nornicdb/") && len(frames) < 14 {
							fn := strings.TrimPrefix(ln, "github.com/orneryd/nornicdb/")
							if i := strings.Index(fn, "("); i > 0 {
								fn = fn[:i]
							}
							frames = append(frames, fn)
						}
					}
					hsLog("CSSTACK N=%d mine=%v %s", n, mine, strings.Join(frames, " < "))
				}
			}
		}()
		smp.setPhase("commit")
		cd, err := tx.commit()
		close(stopStacks)
		<-stacksDone
		if profiling {
			pprof.StopCPUProfile()
		}
		smp.setPhase("after")
		if err != nil {
			t.Fatal(err)
		}
		commitMS = append(commitMS, cd.Milliseconds())
		if profiling {
			var aa bytes.Buffer
			_ = pprof.Lookup("allocs").WriteTo(&aa, 0)
			csAnalyze(fmt.Sprintf("commit-cpu-N%d", n), cpuBuf.Bytes(), nil, "cpu", 1e6, "ms", 30)
			csAnalyze(fmt.Sprintf("commit-alloc-N%d", n), aa.Bytes(), allocBase, "alloc_space", 1<<20, "MiB", 30)
		}
	}
	time.Sleep(300 * time.Millisecond)
	after := live()
	smp.mu.Lock()
	ps, pc := smp.phaseObj["stmts"], smp.phaseObj["commit"]
	ls, lc := smp.phaseLive["stmts"], smp.phaseLive["commit"]
	rss := smp.peak["rss"]
	prof, profAt := smp.liveProf, smp.liveProfAt
	smp.mu.Unlock()
	var sumC int64
	for _, c := range commitMS {
		sumC += c
	}
	hsLog("CSRESULT N=%d include=%q smallTx=%d vectors_updated=%d base_live_MiB=%d live_in_tx_MiB=%d stmts_peak_heapObj_MiB=%d stmts_peak_live_MiB=%d commit_peak_heapObj_MiB=%d commit_peak_live_MiB=%d commit_rise_heapObj_MiB=%d peak_rss_MiB=%d after_live_MiB=%d stmt_ms=%v commit_ms=%v commit_total_ms=%d commit_ms_per_node=%.1f commit_ms_per_vector=%.2f",
		n, embedInclude(), smallTx, updVectors, base>>20, liveInTx>>20, ps.heapObj>>20, ls.live>>20, pc.heapObj>>20, lc.live>>20,
		(int64(pc.heapObj)-int64(ps.heapObj))>>20, rss.rss>>20, after>>20, stmtMS, commitMS, sumC, float64(sumC)/float64(n), float64(sumC)/float64(max(1, updVectors)))
	if prof != nil {
		csAnalyze(fmt.Sprintf("maxlive@%.1f-N%d", profAt, n), prof, nil, "inuse_space", 1<<20, "MiB", 20)
	}
}

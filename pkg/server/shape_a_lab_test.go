package server

// Shape A (lab harness, test only): one SET of a property outside the embedded
// text, once per node, on a few very large embedded nodes, in one explicit HTTP
// transaction; then COMMIT. Also the same SETs with one node per transaction.
//
//   UNWIND $ids AS id MATCH (s:PDSearchSource {id: id}) SET s.pd_lab_probe = $n
//
// Settings (env; runs only with SA_RUN=1):
//   SA_N           nodes updated (default 12)
//   SA_TEXT_MAX    text characters of the largest node (default 1,488,175)
//   SA_TEXT_STEP   each further node is this many characters smaller (default 90,000)
//   SA_CHUNK_CHARS characters per chunk vector of the text (default 162)
//   SA_DIMS        vector dimensions (default 2048)
//   SA_MODES       comma list of one (one transaction) and pernode (one per node), each on a fresh server
//   SA_EXTRA       other embedded nodes loaded and not updated (default 0)
// Embedding text from NORNICDB_EMBEDDING_PROPERTIES_INCLUDE (text,search_text: the SET carries the vectors).
// Lines start "HS SA".

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/pprof/profile"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const saSET = "UNWIND $ids AS id MATCH (s:PDSearchSource {id: id}) SET s.pd_lab_probe = $n"

func TestShapeA(t *testing.T) {
	if os.Getenv("SA_RUN") != "1" {
		t.Skip("set SA_RUN=1 to run")
	}
	for _, m := range strings.Split(hsEnvS("SA_MODES", "one,pernode"), ",") {
		m := m
		t.Run(m, func(t *testing.T) { saRunOne(t, m) })
		runtime.GC()
	}
}

// saBucket classifies one sample's stack into the COMMIT cost owners.
func saBucket(fns []string) string {
	has := func(sub string) bool {
		for _, f := range fns {
			if strings.Contains(f, sub) {
				return true
			}
		}
		return false
	}
	switch {
	case has("pkg/storage.(*WAL)") || has("EncodeEntityTransaction") || has("AcceptEntityCommit") || has("marshalWALRecoveryPayload") || has("managedVectorAnchorBatch") || has("pkg/storage.encodeWALRecord") || has("walEntryPayload"):
		return "journal"
	case has("newMVCCNodeUndo") || has("mvccSnapshotNode"):
		return "mvcc_undo_image"
	case has("pkg/search."):
		return "search_refresh"
	case has("snapshotForNotification"):
		return "notification_snapshot"
	case has("replayedBodyLimitError") || has("storeValueLimitError"):
		return "size_check_encode"
	case has("loadNodeEmbeddingState") || has("decodeEmbedding") || has("loadEmbeddingChunksFrom"):
		return "vector_decode"
	case has("PrepareEntityCommit") && has("copyNode"):
		return "prepare_copy"
	case has("copyNode") || has("copyChunkVectors"):
		return "other_copyNode"
	case has("pkg/storage.") && (has("encodeNode") || has("encodeValue") || has("encodeEmbedding")):
		return "storage_encode"
	case has("badger"):
		return "badger"
	case has("runtime.gcBgMarkWorker") || has("runtime.gcDrain"):
		return "gc"
	}
	return "other"
}

func saBuckets(label string, prof []byte, base []byte, idxName string, div int64, unit string) {
	p, err := profile.Parse(bytes.NewReader(prof))
	if err != nil {
		hsLog("SAPROF %s parse error %v", label, err)
		return
	}
	idx := -1
	for i, st := range p.SampleType {
		if st.Type == idxName {
			idx = i
		}
	}
	if idx < 0 {
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
	b := map[string]int64{}
	var total int64
	for _, s := range p.Sample {
		var fns []string
		for _, loc := range s.Location {
			for _, ln := range loc.Line {
				if ln.Function != nil {
					fns = append(fns, ln.Function.Name)
				}
			}
		}
		v := s.Value[idx]
		total += v
		b[saBucket(fns)] += v
	}
	keys := make([]string, 0, len(b))
	for k := range b {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return b[keys[i]] > b[keys[j]] })
	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, b[k]/div))
	}
	hsLog("SABUCKETS %s %s total_%s=%d %s", label, idxName, unit, total/div, strings.Join(parts, " "))
}

// saMerge adds the profiles in plus and subtracts those in minus.
func saMerge(plus, minus [][]byte) []byte {
	var ps []*profile.Profile
	for i, list := range [][][]byte{plus, minus} {
		for _, b := range list {
			p, err := profile.Parse(bytes.NewReader(b))
			if err != nil {
				continue
			}
			if i == 1 {
				p.Scale(-1)
			}
			ps = append(ps, p)
		}
	}
	if len(ps) == 0 {
		return nil
	}
	m, err := profile.Merge(ps)
	if err != nil {
		hsLog("SAPROF merge error %v", err)
		return nil
	}
	var out bytes.Buffer
	_ = m.Write(&out)
	return out.Bytes()
}

func saRunOne(t *testing.T, mode string) {
	hsStart = time.Now()
	n := hsEnvI("SA_N", 12)
	textMax := hsEnvI("SA_TEXT_MAX", 1488175)
	textStep := hsEnvI("SA_TEXT_STEP", 90000)
	chunkChars := hsEnvI("SA_CHUNK_CHARS", 162)
	dims := hsEnvI("SA_DIMS", 2048)
	extra := hsEnvI("SA_EXTRA", 0)
	hsLog("SACONFIG mode=%s N=%d text_max=%d text_step=%d chunk_chars=%d dims=%d extra=%d include=%q gomaxprocs=%d",
		mode, n, textMax, textStep, chunkChars, dims, extra, embedInclude(), runtime.GOMAXPROCS(0))
	emb := &hsEmbedder{dims: dims}
	server, authenticator := setupTestServerWithDatabaseConfig(t, func(c *nornicdb.Config) {
		c.Database.MVCCRetentionMaxVersions = 100
		c.Database.WALSyncMode = "immediate"
		c.Database.AsyncWritesEnabled = false
		c.Memory.EmbeddingDimensions = dims
		if inc := embedInclude(); inc != "" {
			c.EmbeddingWorker.PropertiesInclude = strings.Split(inc, ",")
		}
	}, nil)
	cl := &hsClient{h: server.buildRouter(), token: "Bearer " + getAuthToken(t, authenticator, "admin"), db: server.dbManager.DefaultDatabaseName()}
	store, err := server.dbManager.GetStorage(server.dbManager.DefaultDatabaseName())
	if err != nil {
		t.Fatal(err)
	}
	if !server.db.SetEmbedder(emb) {
		t.Fatal("SetEmbedder refused")
	}
	if _, _, err := cl.autocommit("CREATE INDEX sa_source_id IF NOT EXISTS FOR (s:PDSearchSource) ON (s.id)", nil); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	r := rand.New(rand.NewSource(20261122))
	vocab := hsVocab(r, 30000)
	var ids []interface{}
	vectors, chars := 0, 0
	for i := 0; i < n+extra; i++ {
		id := fmt.Sprintf("pds-%04d", i)
		tc := max(20000, textMax-i*textStep)
		if i >= n {
			tc = 30000
		}
		text := vocab.text(r, tc)
		var chunks [][]float32
		for c0 := 0; c0 < len(text); c0 += chunkChars {
			chunks = append(chunks, emb.vec(text[c0:min(len(text), c0+chunkChars)]))
		}
		props := map[string]interface{}{"id": id, "document_id": "doc-" + id, "text": text, "search_text": text[:min(len(text), 4000)],
			"view_text": text[:min(len(text), tc/7)], "set_keys": hs2Strs("k", 80, r), "set_weights": hs2Floats(80, r),
			"cell_starts": hsIntList(min(68898, tc/124), 124), "cell_ends": hsIntList(min(68898, tc/124), 124), "content_hash": "h" + id, "page_count": int64(tc/3000 + 1)}
		if _, err := store.CreateNode(ctx, &storage.Node{ID: storage.NodeID(id), Labels: []string{"Source", "PDSearchSource"}, Properties: props, ChunkEmbeddings: chunks}); err != nil {
			t.Fatal(err)
		}
		if i < n {
			ids = append(ids, id)
			vectors += len(chunks)
			chars += tc
		}
	}
	if err := server.db.BuildSearchIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	live := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	base := live()
	hsLog("SALOADED mode=%s N=%d text_chars=%d vectors_updated=%d vector_MiB=%d live_MiB=%d embed_calls=%d",
		mode, n, chars, vectors, vectors*dims*4>>20, base>>20, emb.calls.Load())

	smp := &hsSampler{peak: map[string]hsSample{}, stop: make(chan struct{}), done: make(chan struct{}), phase: "idle"}
	go smp.run(100*time.Millisecond, 0)
	defer func() { close(smp.stop); <-smp.done }()

	// heap profile at the live maximum inside COMMIT phases
	var cmu sync.Mutex
	var commitProf []byte
	var commitProfLive uint64
	var commitProfAt float64
	inCommit := false
	stopCap := make(chan struct{})
	capDone := make(chan struct{})
	go func() {
		defer close(capDone)
		ms := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
		tk := time.NewTicker(100 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stopCap:
				return
			case <-tk.C:
			}
			cmu.Lock()
			on := inCommit
			cmu.Unlock()
			if !on {
				continue
			}
			metrics.Read(ms)
			lv := ms[0].Value.Uint64()
			if float64(lv) > float64(commitProfLive)*1.05 {
				var buf bytes.Buffer
				if pprof.Lookup("heap").WriteTo(&buf, 0) == nil {
					cmu.Lock()
					commitProf, commitProfLive, commitProfAt = buf.Bytes(), lv, time.Since(hsStart).Seconds()
					cmu.Unlock()
				}
			}
		}
	}()

	batch := n
	if mode == "pernode" {
		batch = 1
	}
	var allocAfter, allocBefore, cpus [][]byte
	var stmtMS, commitMS []int64
	var riseMax int64
	var mstat runtime.MemStats
	start := time.Now()
	for b0 := 0; b0 < n; b0 += batch {
		part := ids[b0:min(n, b0+batch)]
		smp.setPhase("stmt")
		tx, d0, _, err := cl.begin(saSET, map[string]interface{}{"ids": part, "n": int64(b0 + 1)})
		if err != nil {
			t.Fatal(err)
		}
		stmtMS = append(stmtMS, d0.Milliseconds())
		// COMMIT only: allocations between two GCs around it, CPU while it runs
		runtime.GC()
		var ab bytes.Buffer
		_ = pprof.Lookup("allocs").WriteTo(&ab, 0)
		allocBefore = append(allocBefore, ab.Bytes())
		runtime.ReadMemStats(&mstat)
		pre := int64(mstat.HeapAlloc)
		smp.mu.Lock()
		delete(smp.phaseObj, "commit")
		smp.mu.Unlock()
		var cpuBuf bytes.Buffer
		cpuOn := pprof.StartCPUProfile(&cpuBuf) == nil
		cmu.Lock()
		inCommit = true
		cmu.Unlock()
		smp.setPhase("commit")
		cd, err := tx.commit()
		cmu.Lock()
		inCommit = false
		cmu.Unlock()
		smp.setPhase("after")
		if cpuOn {
			pprof.StopCPUProfile()
			cpus = append(cpus, cpuBuf.Bytes())
		}
		if err != nil {
			t.Fatal(err)
		}
		commitMS = append(commitMS, cd.Milliseconds())
		smp.mu.Lock()
		pk := int64(smp.phaseObj["commit"].heapObj)
		smp.mu.Unlock()
		if rise := pk - pre; rise > riseMax {
			riseMax = rise
		}
		runtime.GC()
		runtime.GC()
		var aa bytes.Buffer
		_ = pprof.Lookup("allocs").WriteTo(&aa, 0)
		allocAfter = append(allocAfter, aa.Bytes())
		if mode == "pernode" {
			hsLog("SATX mode=%s node=%d stmt_ms=%d commit_ms=%d pre_live_MiB=%d commit_peak_heapObj_MiB=%d rise_MiB=%d", mode, b0, d0.Milliseconds(), cd.Milliseconds(), pre>>20, pk>>20, (pk-pre)>>20)
		}
	}
	wall := time.Since(start)
	close(stopCap)
	<-capDone
	allocProf := saMerge(allocAfter, allocBefore)
	cpuProf := saMerge(cpus, nil)
	after := live()
	smp.mu.Lock()
	pkObj, pkLive, rss := smp.peak["heapObjects"], smp.peak["liveAtLastGC"], smp.peak["rss"]
	smp.mu.Unlock()
	var sumS, sumC int64
	for _, v := range stmtMS {
		sumS += v
	}
	for _, v := range commitMS {
		sumC += v
	}
	hsLog("SARESULT mode=%s N=%d vectors_updated=%d base_live_MiB=%d peak_heapObj_MiB=%d (phase %s) peak_live_MiB=%d peak_rss_MiB=%d max_commit_rise_heapObj_MiB=%d stmt_total_ms=%d commit_total_ms=%d wall_ms=%d after_live_MiB=%d embed_calls=%d stmt_ms=%v commit_ms=%v",
		mode, n, vectors, base>>20, pkObj.heapObj>>20, pkObj.phase, pkLive.live>>20, rss.rss>>20, riseMax>>20, sumS, sumC, wall.Milliseconds(), after>>20, emb.calls.Load(), stmtMS, commitMS)
	label := "sa-" + mode
	saBuckets(label+"-commit-alloc", allocProf, nil, "alloc_space", 1<<20, "MiB")
	saBuckets(label+"-commit-cpu", cpuProf, nil, "cpu", 1e6, "ms")
	csAnalyze(label+"-commit-alloc", allocProf, nil, "alloc_space", 1<<20, "MiB", 25)
	csAnalyze(label+"-commit-cpu", cpuProf, nil, "cpu", 1e6, "ms", 25)
	cmu.Lock()
	cp, cpl, cpa := commitProf, commitProfLive, commitProfAt
	cmu.Unlock()
	if cp != nil {
		hsLog("SACOMMITHEAP mode=%s at=%.1f live_MiB=%d", mode, cpa, cpl>>20)
		saBuckets(label+"-commitheap", cp, nil, "inuse_space", 1<<20, "MiB")
		csAnalyze(label+"-commitheap", cp, nil, "inuse_space", 1<<20, "MiB", 20)
	}
}

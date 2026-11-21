package server

// Reproduction: one transaction that marks the summaries of one owner not
// current, removes their searchable label and deletes their COVERS
// relationships, 122 of which end at one large node (2,664,373 characters of
// embedded text, one 2,048-dimension chunk vector per 512 characters). The
// statement and its COMMIT are timed and profiled separately (CPU and
// allocation), with the live heap after a forced GC before, between and after.
// Runs only with HS2_RUN=1; CD_VARIANTS selects the variants (default all).

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/pprof/profile"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	cdStatement  = "MATCH (s:Summary {owner_kind: $kind, owner_id: $id}) SET s.current = false REMOVE s:Searchable WITH s OPTIONAL MATCH (s)-[r:COVERS]->() DELETE r"
	cdNoRemove   = "MATCH (s:Summary {owner_kind: $kind, owner_id: $id}) SET s.current = false WITH s OPTIONAL MATCH (s)-[r:COVERS]->() DELETE r"
	cdDeleteOnly = "MATCH (s:Summary {owner_kind: $kind, owner_id: $id}) WITH s OPTIONAL MATCH (s)-[r:COVERS]->() DELETE r"
	cdSetOnly    = "MATCH (s:Summary {owner_kind: $kind, owner_id: $id}) SET s.current = false REMOVE s:Searchable"
)

type cdVariant struct {
	name, statement string
	bigChars        int
	include         string // "" = engine default embedding text
	after           []string // further statements in the same transaction, before COMMIT
}

const (
	// the same summaries link to the large node again
	cdRelinkSame = "MATCH (s:Summary {owner_kind: $kind, owner_id: $id}) WHERE s.id < 'sum-122' MATCH (b:Source {id: 'big'}) MERGE (s)-[:COVERS]->(b)"
	// new summaries (searchable) are created and linked to the large node
	cdNewSummaries = "UNWIND range(0, 163) AS i MERGE (s:GraphNode {id: 'new-' + toString(i)}) SET s:Summary:Searchable, s.owner_kind = $kind, s.owner_id = 'doc-1-new', s.search_text = 'new summary ' + toString(i), s.current = true, s.n = i"
	cdNewLinks     = "MATCH (s:Summary {owner_id: 'doc-1-new'}) WHERE s.n < 122 MATCH (b:Source {id: 'big'}) MERGE (s)-[:COVERS]->(b)"
)

func cdVariants() []cdVariant {
	const big = 2664373
	const inc = "text,search_text"
	all := []cdVariant{
		{"base", cdStatement, big, inc, nil},
		{"no-remove", cdNoRemove, big, inc, nil},
		{"delete-only", cdDeleteOnly, big, inc, nil},
		{"set-only", cdSetOnly, big, inc, nil},
		{"small-target", cdStatement, 2000, inc, nil},
		{"default-embedding-text", cdStatement, big, "", nil},
		{"republish-relink-same", cdStatement, big, inc, []string{cdRelinkSame}},
		{"republish-new-summaries", cdStatement, big, inc, []string{cdNewSummaries, cdNewLinks}},
	}
	want := os.Getenv("CD_VARIANTS")
	if want == "" {
		return all
	}
	var out []cdVariant
	for _, v := range all {
		for _, w := range strings.Split(want, ",") {
			if v.name == w {
				out = append(out, v)
			}
		}
	}
	return out
}

// cdCPU prints the top functions of a CPU profile by cumulative and flat time.
func cdCPU(label string, prof []byte, top int) {
	p, err := profile.Parse(bytes.NewReader(prof))
	if err != nil || len(p.SampleType) < 2 {
		hsLog("CPU %s parse error %v", label, err)
		return
	}
	idx := len(p.SampleType) - 1 // cpu nanoseconds
	cum, flat := map[string]int64{}, map[string]int64{}
	var total int64
	for _, s := range p.Sample {
		v := s.Value[idx]
		total += v
		seen := map[string]bool{}
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
			}
		}
	}
	hsLog("CPU %s total_s=%.2f", label, float64(total)/1e9)
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
			hsLog("CPU %s %s #%02d s=%.2f pct=%.1f %s", label, kind, i+1, float64(xs[i].v)/1e9, 100*float64(xs[i].v)/float64(max(total, 1)), strings.TrimPrefix(xs[i].k, "github.com/orneryd/nornicdb/"))
		}
	}
	pr("cum", cum, true)
	pr("flat", flat, false)
}

// cdAllocBytes sums alloc_space over samples whose stack contains fn.
func cdAllocBytes(prof, base []byte, fn string) int64 {
	p, err := profile.Parse(bytes.NewReader(prof))
	if err != nil {
		return -1
	}
	if base != nil {
		if bp, err := profile.Parse(bytes.NewReader(base)); err == nil {
			bp.Scale(-1)
			if m, err := profile.Merge([]*profile.Profile{p, bp}); err == nil {
				p = m
			}
		}
	}
	idx := -1
	for i, st := range p.SampleType {
		if st.Type == "alloc_space" {
			idx = i
		}
	}
	var total int64
	for _, s := range p.Sample {
		hit := false
		for _, loc := range s.Location {
			for _, ln := range loc.Line {
				if ln.Function != nil && strings.Contains(ln.Function.Name, fn) {
					hit = true
				}
			}
		}
		if hit {
			total += s.Value[idx]
		}
	}
	return total
}

func TestCoversDeleteIntoLargeNode(t *testing.T) {
	if os.Getenv("HS2_RUN") != "1" {
		t.Skip("set HS2_RUN=1 to run")
	}
	runtime.MemProfileRate = 64 << 10
	for _, v := range cdVariants() {
		v := v
		t.Run(v.name, func(t *testing.T) { cdRun(t, v) })
		runtime.GC()
	}
}

func cdRun(t *testing.T, v cdVariant) {
	hsStart = time.Now()
	const chunkChars = 512
	const nSummaries, toBig, toSummaries, toOthers, nOthers = 164, 122, 163, 163, 50
	emb := &hsEmbedder{dims: 2048}
	server, authenticator := setupTestServerWithDatabaseConfig(t, func(c *nornicdb.Config) {
		c.Database.MVCCRetentionMaxVersions = 100
		c.Database.WALSyncMode = "immediate"
		c.Database.AsyncWritesEnabled = false
		c.Database.MVCCLifecycleEnabled = true
		c.Database.MVCCLifecycleCycleInterval = 0
		c.Memory.EmbeddingDimensions = 2048
		if v.include != "" {
			c.EmbeddingWorker.PropertiesInclude = strings.Split(v.include, ",")
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
		"CREATE INDEX cd_summary_owner IF NOT EXISTS FOR (s:Summary) ON (s.owner_id)",
		"CREATE INDEX cd_node_id IF NOT EXISTS FOR (n:GraphNode) ON (n.id)",
	} {
		if _, _, err := cl.autocommit(q, nil); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	r := rand.New(rand.NewSource(3))
	vocab := hsVocab(r, 30000)
	vecs := func(text string) [][]float32 {
		var out [][]float32
		for c0 := 0; c0 < len(text); c0 += chunkChars {
			out = append(out, emb.vec(text[c0:min(len(text), c0+chunkChars)]))
		}
		return out
	}
	mk := func(n *storage.Node) {
		if _, err := store.CreateNode(ctx, n); err != nil {
			t.Fatal(err)
		}
	}
	edge := func(id, from, to string) {
		if err := store.CreateEdge(ctx, &storage.Edge{ID: storage.EdgeID(id), StartNode: storage.NodeID(from), EndNode: storage.NodeID(to), Type: "COVERS"}); err != nil {
			t.Fatal(err)
		}
	}
	// The large target node.
	bigText := vocab.text(r, v.bigChars)
	bigProps := map[string]interface{}{"id": "big", "text": bigText, "view_text": bigText, "set_keys": hs2Strs("k", 210, r), "set_weights": hs2Floats(210, r)}
	for f := len(bigProps); f < 48; f++ {
		bigProps[fmt.Sprintf("p%02d", f)] = fmt.Sprintf("value-%d", f)
	}
	bigVectors := vecs(bigText)
	mk(&storage.Node{ID: "big", Labels: []string{"GraphNode", "Source"}, Properties: bigProps, ChunkEmbeddings: bigVectors})
	for i := 0; i < nOthers; i++ {
		id := fmt.Sprintf("other-%03d", i)
		text := vocab.text(r, 1500)
		mk(&storage.Node{ID: storage.NodeID(id), Labels: []string{"GraphNode", "Source"}, Properties: map[string]interface{}{"id": id, "text": text}, ChunkEmbeddings: vecs(text)})
	}
	for i := 0; i < nSummaries; i++ {
		id := fmt.Sprintf("sum-%03d", i)
		st := vocab.text(r, 69)
		mk(&storage.Node{ID: storage.NodeID(id), Labels: []string{"GraphNode", "Summary", "Searchable"}, Properties: map[string]interface{}{
			"id": id, "owner_kind": "document", "owner_id": "doc-1", "search_text": st, "current": true, "kind": "summary"}, ChunkEmbeddings: vecs(st)})
	}
	for i := 0; i < toBig; i++ {
		edge(fmt.Sprintf("cov-big-%03d", i), fmt.Sprintf("sum-%03d", i), "big")
	}
	for i := 0; i < toSummaries; i++ {
		edge(fmt.Sprintf("cov-sum-%03d", i), fmt.Sprintf("sum-%03d", i), fmt.Sprintf("sum-%03d", (i+1)%nSummaries))
	}
	for i := 0; i < toOthers; i++ {
		edge(fmt.Sprintf("cov-oth-%03d", i), fmt.Sprintf("sum-%03d", i%nSummaries), fmt.Sprintf("other-%03d", i%nOthers))
	}
	if err := server.db.BuildSearchIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	live := func() uint64 {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	allocs := func() uint64 {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.TotalAlloc
	}
	heapProf := func() []byte {
		var b bytes.Buffer
		runtime.GC()
		_ = pprof.Lookup("heap").WriteTo(&b, 0)
		return b.Bytes()
	}
	bigVecBytes := int64(len(bigVectors)) * 2048 * 4
	hsLog("CD_SETUP variant=%s include=%q big_chars=%d big_vectors=%d big_vector_MiB=%.1f summaries=%d covers=%d (to big %d)", v.name, v.include, v.bigChars, len(bigVectors), float64(bigVecBytes)/(1<<20), nSummaries, toBig+toSummaries+toOthers, toBig)

	smp := &hsSampler{peak: map[string]hsSample{}, stop: make(chan struct{}), done: make(chan struct{}), phase: "idle"}
	go smp.run(100*time.Millisecond, 0)
	l0 := live()
	p0 := heapProf()
	a0 := allocs()

	smp.setPhase("statement")
	var cpu1 bytes.Buffer
	_ = pprof.StartCPUProfile(&cpu1)
	tx, dStmt, _, err := cl.begin(v.statement, map[string]interface{}{"kind": "document", "id": "doc-1"})
	pprof.StopCPUProfile()
	if err != nil {
		t.Fatal(err)
	}
	for i, q := range v.after {
		d, _, err := tx.run(q, map[string]interface{}{"kind": "document", "id": "doc-1"})
		if err != nil {
			t.Fatal(err)
		}
		hsLog("CD_AFTER variant=%s statement=%d ms=%d", v.name, i+2, d.Milliseconds())
	}
	a1 := allocs()
	p1 := heapProf()
	l1 := live()

	smp.setPhase("commit")
	var cpu2 bytes.Buffer
	_ = pprof.StartCPUProfile(&cpu2)
	dCommit, err := tx.commit()
	pprof.StopCPUProfile()
	if err != nil {
		t.Fatal(err)
	}
	a2 := allocs()
	p2 := heapProf()
	l2 := live()
	smp.setPhase("done")
	close(smp.stop)
	<-smp.done

	// Remaining relationships, as a correctness check.
	_, n, err := cl.autocommit("MATCH (:Summary {owner_id: 'doc-1'})-[r:COVERS]->() RETURN count(r) AS left", nil)
	hsLog("CD_CHECK variant=%s response_bytes=%d err=%v", v.name, n, err)

	for _, ph := range []string{"statement", "commit"} {
		o := smp.phaseObj[ph]
		hsLog("CD_PHASEPEAK variant=%s phase=%s heapObj_MiB=%d liveAtLastGC_MiB=%d rss_MiB=%d", v.name, ph, o.heapObj>>20, smp.phaseLive[ph].live>>20, o.rss>>20)
	}
	for _, x := range []struct {
		label     string
		prof, bas []byte
	}{{"statement", p1, p0}, {"commit", p2, p1}} {
		vecLoad := cdAllocBytes(x.prof, x.bas, "storage.loadEmbeddingChunksFrom")
		copyN := cdAllocBytes(x.prof, x.bas, "storage.copyNode")
		hsLog("CD_ALLOC variant=%s phase=%s chunk_vector_load_MiB=%.0f (= %.1f loads of the large node's vectors) copyNode_MiB=%.0f (= %.1f copies)",
			v.name, x.label, float64(vecLoad)/(1<<20), float64(vecLoad)/float64(max(bigVecBytes, 1)), float64(copyN)/(1<<20), float64(copyN)/float64(max(bigVecBytes, 1)))
		hsAnalyze(v.name+"-"+x.label+"-allocs", x.prof, x.bas, "alloc_space", 15)
	}
	cdCPU(v.name+"-statement", cpu1.Bytes(), 20)
	cdCPU(v.name+"-commit", cpu2.Bytes(), 20)
	hsLog("CD_SUMMARY variant=%s include=%q big_vectors=%d statement_ms=%d commit_ms=%d stmt_alloc_MiB=%d commit_alloc_MiB=%d live_before_MiB=%d live_after_stmt_MiB=%d live_after_commit_MiB=%d",
		v.name, v.include, len(bigVectors), dStmt.Milliseconds(), dCommit.Milliseconds(), (a1-a0)>>20, (a2-a1)>>20, l0>>20, l1>>20, l2>>20)
}

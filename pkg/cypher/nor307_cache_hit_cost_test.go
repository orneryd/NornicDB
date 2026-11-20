package cypher

// NOR-307 reproduction (performance part): the cost of a query-result cache hit
// for large retained results.
//
// Claim: a cache hit delivers its retained result by structurally copying it.
// For a search result of 25 source nodes that each carry 81 chunk vectors of
// 2,048 dimensions, every hit then copies about 16.6 MB of vectors; for a
// 21,748-row list of projected maps, about 39.5 MB. Measured earlier on the
// application's statements against a generated 172,000-node store: cache-hit
// allocation per read went from 0.16-0.21 MB (b6d8e762a) to 16.9-39.7 MB, and
// hit latency from 0.36-2.6 ms to 4.9-48 ms.
//
// Statements: testdata/nor307_queries.json holds the application's three
// statements byte for byte (SHA-256 3beffc90... BM25_SEARCH, c64baa3a...
// PENDING, 3a03e8de... VECTOR_SEARCH). Parameters are synthetic: the private
// search term and query vector were not kept; query="contract", limit=25,
// status=null, wait_reason=null and a fixed 2,048-value vector.
//
// Data: generated from fixed formulas, no randomness. NOR307_SCALE selects:
//   - "shape" (default): the result shapes of the original store at a size
//     any machine can hold: 100 search sources with 81-82 chunk vectors of
//     2,048 dimensions each (every 2nd source matches "contract"), 21,951
//     identity cases (21,748 pending or failed), 100 documents, versions and
//     summaries, 4 open collections. Each statement returns the same number of
//     rows of the same shape as on the full store (25 / 21,748 / 25).
//   - "full": the full 172,000-node / 290,000-relationship store of the
//     earlier measurement (1,771 sources, 143,818 chunk vectors, 164 MB of
//     text). Heavy: expect well over 8 GiB.
//
// For each statement, with the normal result cache on: one miss, then 9 hits.
// Asserted: the cache really hits; every result is identical; and the median
// bytes allocated per cache hit stay within NOR307_MAX_HIT_BYTES (default
// 2 MiB: ten times the b6d8e762a figure, well under the 16.9 MB per hit of the
// claim). The bound is ours, not one written on the ticket. All figures print.
//
// Run (both parsers):
//
//	go test -tags=noui,nolocalllm ./pkg/cypher -run '^TestNOR307CacheHitCost$' -count=1 -v -timeout=30m
//	NORNICDB_PARSER=antlr go test -tags=noui,nolocalllm ./pkg/cypher -run '^TestNOR307CacheHitCost$' -count=1 -v -timeout=30m

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	nconfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type nor307Scale struct {
	name                                        string
	sources, documents, summaries, collections  int
	cases, filler, extraRelations, textRepeats  int
	contractEvery                               int
}

func nor307Layout(name string) nor307Scale {
	if name == "full" {
		return nor307Scale{name: "full", sources: 1771, documents: 1801, summaries: 2643, collections: 4, cases: 21951, filler: 172000 - 1771 - 21951 - 1801 - 1801 - 4 - 2643, extraRelations: 290000 - 1801 - 1801 - 1771, textRepeats: 600, contractEvery: 40}
	}
	return nor307Scale{name: "shape", sources: 100, documents: 100, summaries: 100, collections: 4, cases: 21951, filler: 0, extraRelations: 0, textRepeats: 60, contractEvery: 2}
}

func TestNOR307CacheHitCost(t *testing.T) {
	parser := "default"
	if nconfig.IsANTLRParser() {
		parser = "antlr"
	}
	scale := nor307Layout(os.Getenv("NOR307_SCALE"))
	maxHit := uint64(2 << 20)
	if v := os.Getenv("NOR307_MAX_HIT_BYTES"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		maxHit = n
	}
	ctx := context.Background()
	engine, err := storage.NewBadgerEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	s := storage.NewNamespacedEngine(engine, "personal_documents")
	e := NewStorageExecutor(s)
	if e.cache == nil {
		t.Fatal("result cache is not enabled by default")
	}

	// Node layout by index ranges: sources, cases, documents, versions,
	// collections, summaries, filler.
	docBase := scale.sources + scale.cases
	verBase := docBase + scale.documents
	colBase := verBase + scale.documents
	sumBase := colBase + scale.collections
	fillBase := sumBase + scale.summaries
	total := fillBase + scale.filler
	id := func(i int) string { return fmt.Sprintf("n%06d", i) }
	nodes := make([]*storage.Node, 0, 100)
	flush := func() {
		if len(nodes) > 0 {
			if err := s.BulkCreateNodes(ctx, nodes); err != nil {
				t.Fatal(err)
			}
			nodes = make([]*storage.Node, 0, 100)
		}
	}
	textBytes, vectors, pending := 0, 0, 0
	for i := 0; i < total; i++ {
		labels := []string{"PDGraphNode"}
		p := map[string]interface{}{"id": id(i), "current": true}
		var chunks [][]float32
		switch {
		case i < scale.sources:
			labels = []string{"PDSearchSource"}
			p["document_id"] = id(docBase + i%scale.documents)
			p["version_id"] = id(verBase + i%scale.documents)
			words := "routine correspondence meeting attachment reference schedule services account document "
			if i%scale.contractEvery == 0 {
				words = "contract invoice payment due research handoff " + words
			}
			p["text"] = strings.Repeat(words, scale.textRepeats+i%(scale.textRepeats*5/3))
			textBytes += len(p["text"].(string))
			count := 81
			if i < scale.sources*367/1771 {
				count++
			}
			chunks = make([][]float32, count)
			for k := range chunks {
				v := make([]float32, 2048)
				for j := range v {
					v[j] = float32((i*31+k*13+j*17)%101+1) / 101
				}
				chunks[k] = v
			}
			vectors += count
		case i < docBase:
			labels = []string{"PDIdentityCase"}
			c := i - scale.sources
			p["status"] = "pending"
			if c%17 == 0 {
				p["status"] = "failed"
			}
			if c >= 21748 {
				p["status"] = "decided"
			} else {
				pending++
			}
			p["planned_at"] = fmt.Sprintf("2026-10-%02dT12:%02d:00Z", 1+c%28, c%60)
			p["case_key"] = id(i)
			p["case_kind"] = "resolve_mention"
			p["subject_ids"] = []string{"synthetic"}
			p["member_of"] = []string{"library"}
			p["generation"] = int64(1)
			p["attempt"] = int64(0)
			p["cause"] = "synthetic rehearsal"
		case i < verBase:
			labels = []string{"PDDocument", "PDGraphNode"}
			p["current_version_id"] = id(verBase + i - docBase)
		case i < colBase:
			labels = []string{"PDVersion"}
			p["document_id"] = id(docBase + i - verBase)
			p["current_source_id"] = id((i - verBase) % scale.sources)
		case i < sumBase:
			labels = []string{"PDCollection"}
			p["archived"] = false
		case i < fillBase:
			labels = []string{"PDSummary", "PDSearchable"}
			p["owner_kind"] = "document"
			p["owner_id"] = id(docBase + i%scale.documents)
			p["source_ids"] = []string{id(i % scale.sources)}
		}
		n := &storage.Node{ID: storage.NodeID(id(i)), Labels: labels, Properties: p, ChunkEmbeddings: chunks}
		nodes = append(nodes, n)
		if len(nodes) == 100 {
			flush()
		}
	}
	flush()
	if pending != 21748 {
		t.Fatalf("pending cases = %d", pending)
	}
	edge := 0
	addEdge := func(a, z int, kind string) {
		if err := s.CreateEdge(ctx, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%06d", edge)), StartNode: storage.NodeID(id(a)), EndNode: storage.NodeID(id(z)), Type: kind,
			Properties: map[string]interface{}{"id": fmt.Sprintf("r%06d", edge)}}); err != nil {
			t.Fatal(err)
		}
		edge++
	}
	for d := 0; d < scale.documents; d++ {
		addEdge(docBase+d, colBase+d%scale.collections, "MEMBER_OF")
		addEdge(docBase+d, verBase+d, "HAS_VERSION")
	}
	for src := 0; src < scale.sources; src++ {
		addEdge(docBase+src%scale.documents, src, "HAS_SOURCE")
	}
	for r := 0; r < scale.extraRelations; r++ {
		addEdge(docBase+r%scale.documents, r%fillBase, "PD_RELATION")
	}
	t.Logf("NOR307 fixture scale=%s nodes=%d relationships=%d sources=%d chunk_vectors=%d dims=2048 identity_cases=%d pending_or_failed=%d text_bytes=%d",
		scale.name, total, edge, scale.sources, vectors, scale.cases, pending, textBytes)

	run := func(q string) {
		if _, err := e.Execute(ctx, q, nil); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, l := range []string{"PDSearchSource", "PDDocument", "PDVersion", "PDCollection", "PDIdentityCase", "PDGraphNode"} {
		run(fmt.Sprintf("CREATE CONSTRAINT nor307_%s IF NOT EXISTS FOR (n:%s) REQUIRE n.id IS UNIQUE", l, l))
	}
	for _, p := range []string{"status", "queue", "held_by", "wait_reason", "claim"} {
		run(fmt.Sprintf("CREATE INDEX nor307_case_%s FOR (n:PDIdentityCase) ON (n.%s)", p, p))
	}
	run("CREATE FULLTEXT INDEX pd_source_text IF NOT EXISTS FOR (n:PDSearchSource) ON EACH [n.text]")
	svc := search.NewService(s, 2048, nil)
	e.SetSearchService(svc)
	defer svc.Close()

	raw, err := os.ReadFile("testdata/nor307_queries.json")
	if err != nil {
		t.Fatal(err)
	}
	queries := map[string]string{}
	if err := json.Unmarshal(raw, &queries); err != nil {
		t.Fatal(err)
	}
	vec := make([]float64, 2048)
	for j := range vec {
		vec[j] = float64(float32((j*17)%101+1) / 101)
	}
	params := map[string]interface{}{"limit": int64(25), "query": "contract", "query_input": vec, "status": nil, "wait_reason": nil}

	for _, name := range []string{"BM25_SEARCH", "PENDING", "VECTOR_SEARCH"} {
		t.Run(name, func(t *testing.T) {
			q := queries[name]
			want := 25
			if name == "PENDING" {
				want = 21748
			}
			if name == "VECTOR_SEARCH" {
				svc.SetLazyWarming(true, search.WarmFunc(func() {
					if err := svc.BuildIndexes(context.Background()); err != nil {
						panic(err)
					}
				}))
			}
			hits0, _, _, _, _ := e.cache.Stats()
			var hash string
			var hitBytes []uint64
			var hitTimes []time.Duration
			for call := 0; call < 10; call++ {
				var a, z runtime.MemStats
				runtime.ReadMemStats(&a)
				start := time.Now()
				r, err := e.Execute(ctx, q, params)
				elapsed := time.Since(start)
				runtime.ReadMemStats(&z)
				if err != nil {
					t.Fatalf("call %d: %v", call, err)
				}
				if len(r.Rows) != want {
					t.Fatalf("call %d: %d rows, want %d", call, len(r.Rows), want)
				}
				rows := make([][]interface{}, len(r.Rows))
				for i, row := range r.Rows {
					rows[i] = append([]interface{}(nil), row...)
					for j, x := range rows[i] {
						if n, ok := x.(*storage.Node); ok {
							rows[i][j] = []interface{}{n.ID, n.Labels, n.Properties, len(n.ChunkEmbeddings)}
						}
					}
				}
				buf, err := json.Marshal(rows)
				if err != nil {
					t.Fatal(err)
				}
				h := fmt.Sprintf("%x", sha256.Sum256(buf))
				if hash == "" {
					hash = h
				} else if h != hash {
					t.Errorf("call %d: result %s differs from first call %s", call, h, hash)
				}
				t.Logf("NOR307 statement=%s parser=%s call=%d ms=%.3f alloc_bytes=%d rows=%d result=%s", name, parser, call, float64(elapsed.Nanoseconds())/1e6, z.TotalAlloc-a.TotalAlloc, len(r.Rows), h[:16])
				// VECTOR_SEARCH call 1 refills the cache after the lazily built
				// index changes the search generation; it is not a hit.
				if call >= 2 || (call == 1 && name != "VECTOR_SEARCH") {
					hitBytes = append(hitBytes, z.TotalAlloc-a.TotalAlloc)
					hitTimes = append(hitTimes, elapsed)
				}
			}
			hits1, misses, entries, _, _ := e.cache.Stats()
			sort.Slice(hitBytes, func(i, j int) bool { return hitBytes[i] < hitBytes[j] })
			sort.Slice(hitTimes, func(i, j int) bool { return hitTimes[i] < hitTimes[j] })
			medBytes, medTime := hitBytes[len(hitBytes)/2], hitTimes[len(hitTimes)/2]
			t.Logf("NOR307_SUMMARY statement=%s parser=%s scale=%s go=%s cache_hits=%d cache_misses_total=%d entries=%d hit_samples=%d median_hit_ms=%.3f median_hit_alloc_bytes=%d bound_bytes=%d",
				name, parser, scale.name, runtime.Version(), hits1-hits0, misses, entries, len(hitBytes), float64(medTime.Nanoseconds())/1e6, medBytes, maxHit)
			if hits1-hits0 < int64(len(hitBytes)) {
				t.Errorf("only %d cache hits for %d expected hit calls: the measurement is not of cache hits", hits1-hits0, len(hitBytes))
			}
			if medBytes > maxHit {
				t.Errorf("%s: a cache hit allocates %d bytes (median of %d hits, %.3f ms), over the %d-byte bound; the result is %d rows", name, medBytes, len(hitBytes), float64(medTime.Nanoseconds())/1e6, maxHit, want)
			}
			// Root-cause diagnostic (campaign lane B): the cost of the hit's
			// copy alone, cacheOwnedResult (cache.go:304) applied to a result of
			// the same shape, against the whole hit measured above.
			if delivered, err := e.Execute(ctx, q, params); err == nil {
				var copyBytes []uint64
				var copyTimes []time.Duration
				for i := 0; i < 7; i++ {
					var a, z runtime.MemStats
					runtime.ReadMemStats(&a)
					start := time.Now()
					owned := cacheOwnedResult(delivered)
					elapsed := time.Since(start)
					runtime.ReadMemStats(&z)
					if owned == nil || len(owned.Rows) != len(delivered.Rows) {
						t.Errorf("RCB307 copy changed the result")
					}
					copyBytes = append(copyBytes, z.TotalAlloc-a.TotalAlloc)
					copyTimes = append(copyTimes, elapsed)
				}
				sort.Slice(copyBytes, func(i, j int) bool { return copyBytes[i] < copyBytes[j] })
				sort.Slice(copyTimes, func(i, j int) bool { return copyTimes[i] < copyTimes[j] })
				cb, ct := copyBytes[len(copyBytes)/2], copyTimes[len(copyTimes)/2]
				t.Logf("RCB307 COPY statement=%s parser=%s rows=%d copy_median_alloc_bytes=%d copy_median_ms=%.3f hit_median_alloc_bytes=%d hit_median_ms=%.3f copy_share_alloc=%.3f copy_share_time=%.3f",
					name, parser, len(delivered.Rows), cb, float64(ct.Nanoseconds())/1e6, medBytes, float64(medTime.Nanoseconds())/1e6,
					float64(cb)/float64(medBytes), float64(ct.Nanoseconds())/float64(medTime.Nanoseconds()))
			} else {
				t.Logf("RCB307 COPY statement=%s err=%v", name, err)
			}
		})
	}
}

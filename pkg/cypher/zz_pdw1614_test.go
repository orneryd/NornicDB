package cypher

// Scratch investigation (pd-w161-4), not for merge. Observations 1 and 2:
//  1. the ranked graph projection of the application's SEARCH statement
//     (shape 3a6062aab17574b6) under concurrent import writes;
//  2. the application's links.rs evaluation statement under concurrent
//     import writes and after them.
// Both statements are the application's exact texts (testdata/zz_pdw1614);
// SEARCH's two retrieval procedure calls are replaced by parameter lists of
// the same hits, and nothing else changes. Each run is measured for wall
// time and for the on-CPU time of the goroutines that ran the statement
// (pprof label pdw=stmt), with the functions that CPU was spent in.
// Every deciding value is logged with the prefix PDW1614.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"regexp"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/pprof/profile"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	pdw1614SearchSHA   = "0996f6c60db07500"
	pdw1614EvaluateSHA = "2b96e08aa160cadf"
)

type pdw1614Sizes struct {
	docs, sourceChars, sections, views, pages, emailTexts int
	entities, links, shared                               int
	selected, selectedEntities                            int
	selectedLinks                                         []int
	hits, matches                                         int
	sets, membersPerSet, memberChars, memberMatches       int
}

func pdw1614DefaultSizes() pdw1614Sizes {
	return pdw1614Sizes{
		docs: 556, sourceChars: 20000, sections: 40, views: 40, pages: 10, emailTexts: 20,
		entities: 10, links: 40, shared: 300,
		selected: 5, selectedEntities: 50, selectedLinks: []int{470, 470, 470, 470, 468},
		hits: 20, matches: 30,
		sets: 25, membersPerSet: 10, memberChars: 4000, memberMatches: 5,
	}
}

type pdw1614Fixture struct {
	t        *testing.T
	dir      string
	engine   *storage.BadgerEngine
	store    storage.Engine
	sizes    pdw1614Sizes
	search   string
	evaluate string
	seeds    []any
	selected []any
}

func pdw1614Statement(t *testing.T, name, want string) string {
	t.Helper()
	text, err := os.ReadFile("testdata/zz_pdw1614/" + name + ".cypher")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(text)
	got := hex.EncodeToString(sum[:])[:16]
	t.Logf("PDW1614 statement %s bytes=%d sha256[:16]=%s want=%s exact=%v", name, len(text), got, want, got == want)
	if got != want {
		t.Fatalf("statement %s is not the application's text", name)
	}
	return string(text)
}

// pdw1614SearchWithoutRetrieval replaces db.rretrieve (the ranked page) and
// db.retrieve (the group members) with parameter lists of the same shape.
func pdw1614SearchWithoutRetrieval(t *testing.T, text string) string {
	t.Helper()
	ranked := regexp.MustCompile(`(?s)CALL db\.rretrieve\(.*?YIELD page\n  UNWIND CASE WHEN size\(page\.results\) = 0 THEN \[null\] ELSE page\.results END AS result\n`)
	if n := len(ranked.FindAllStringIndex(text, -1)); n != 1 {
		t.Fatalf("ranked retrieval found %d times", n)
	}
	text = ranked.ReplaceAllLiteralString(text, "WITH $zz_page AS page\n"+
		"  UNWIND CASE WHEN size(page.results) = 0 THEN [null] ELSE page.results END AS zz_result\n"+
		"  OPTIONAL MATCH (zz_node:PDSearchSource {id: zz_result.id})\n"+
		"  WITH page, CASE WHEN zz_result IS NULL THEN null ELSE {node: zz_node, score: zz_result.score, matches: zz_result.matches} END AS result\n")
	members := regexp.MustCompile(`(?s)CALL db\.retrieve\(.*?YIELD node AS member, score AS member_score, matches AS member_matches\n`)
	if n := len(members.FindAllStringIndex(text, -1)); n != 1 {
		t.Fatalf("member retrieval found %d times", n)
	}
	return members.ReplaceAllLiteralString(text, "UNWIND $zz_members AS zz_member\n"+
		"  MATCH (member:PDSearchSource {id: zz_member.id})\n"+
		"  WITH member, zz_member.score AS member_score, zz_member.matches AS member_matches\n")
}

func pdw1614Exec(t *testing.T, store storage.Engine, query string, params map[string]any) *ExecuteResult {
	t.Helper()
	result, err := NewStorageExecutor(store).Execute(context.Background(), query, params)
	if err != nil {
		t.Fatalf("statement %.160q: %v", query, err)
	}
	return result
}

func pdw1614Text(n int, salt string) string {
	sentence := "Lease renewal schedule for the premises, " + salt + ", with the payment terms and notices. "
	var b strings.Builder
	for b.Len() < n {
		b.WriteString(sentence)
	}
	return b.String()[:n]
}

func pdw1614Spans(total, n int) (starts, ends []any) {
	size := total / n
	for i := 0; i < n; i++ {
		starts = append(starts, int64(i*size))
		ends = append(ends, int64((i+1)*size))
	}
	return
}

// pdw1614Source is one email-shaped search source: sections with own,
// quoted and duplicate roles, a view mapping and pages.
func pdw1614Source(id, document, version string, chars, sections, views, pages, emailTexts int, ownPrefix, quotedPrefix string) map[string]any {
	secStarts, secEnds := pdw1614Spans(chars, sections)
	roles, keys, titles := []any{}, []any{}, []any{}
	for j := 0; j < sections; j++ {
		switch j % 4 {
		case 1:
			roles, keys = append(roles, "quoted"), append(keys, fmt.Sprintf("%s-%02d", quotedPrefix, j%emailTexts))
		case 3:
			roles, keys = append(roles, "duplicate"), append(keys, "")
		default:
			roles, keys = append(roles, "own"), append(keys, fmt.Sprintf("%s-%02d", ownPrefix, j%emailTexts))
		}
		titles = append(titles, fmt.Sprintf("Section %d", j))
	}
	viewStarts, viewEnds := pdw1614Spans(chars, views)
	library := []any{}
	for k := 0; k < views; k++ {
		library = append(library, k%5 == 0)
	}
	pageStarts, pageEnds := pdw1614Spans(chars, pages)
	numbers := []any{}
	for p := 0; p < pages; p++ {
		numbers = append(numbers, int64(p+1))
	}
	own := []any{}
	for e := 0; e < emailTexts; e++ {
		own = append(own, fmt.Sprintf("%s-%02d", ownPrefix, e))
	}
	return map[string]any{
		"id": id, "document_id": document, "version_id": version,
		"text": pdw1614Text(chars, id), "view_text": pdw1614Text(chars, "view "+id),
		"section_starts": secStarts, "section_ends": secEnds, "section_roles": roles,
		"section_content_keys": keys, "section_titles": titles,
		"view_starts": viewStarts, "view_ends": viewEnds, "view_source_starts": viewStarts,
		"view_source_ends": viewEnds, "view_library": library,
		"page_starts": pageStarts, "page_ends": pageEnds, "page_numbers": numbers,
		"email_own_keys": own, "run_layout": "", "profiled_run_layout": "",
	}
}

func pdw1614Matches(n, chars int, term string) []any {
	out := []any{}
	step := chars / (n + 1)
	for j := 0; j < n; j++ {
		property := "text"
		if j%2 == 1 {
			property = "view_text"
		}
		kind := "term"
		if j%3 == 2 {
			kind = "vector"
		}
		out = append(out, map[string]any{"kind": kind, "property": property, "start": int64(j*step + 10),
			"end": int64(j*step + 60), "score": 0.5 + float64(j)/1000, "term": term})
	}
	return out
}

var pdw1614Schema = []string{
	"CREATE CONSTRAINT pdw1614_graph IF NOT EXISTS FOR (n:PDGraphNode) REQUIRE n.id IS UNIQUE",
	"CREATE CONSTRAINT pdw1614_document IF NOT EXISTS FOR (n:PDDocument) REQUIRE n.id IS UNIQUE",
	"CREATE CONSTRAINT pdw1614_version IF NOT EXISTS FOR (n:PDVersion) REQUIRE n.id IS UNIQUE",
	"CREATE CONSTRAINT pdw1614_source IF NOT EXISTS FOR (n:PDSearchSource) REQUIRE n.id IS UNIQUE",
	"CREATE CONSTRAINT pdw1614_email IF NOT EXISTS FOR (n:PDEmailText) REQUIRE n.id IS UNIQUE",
	"CREATE CONSTRAINT pdw1614_collection IF NOT EXISTS FOR (n:PDCollection) REQUIRE n.id IS UNIQUE",
	"CREATE CONSTRAINT pdw1614_set IF NOT EXISTS FOR (n:PDRecordSet) REQUIRE n.id IS UNIQUE",
	"CREATE CONSTRAINT pdw1614_correction IF NOT EXISTS FOR (n:PDCorrection) REQUIRE n.id IS UNIQUE",
	"CREATE CONSTRAINT pdw1614_ocr IF NOT EXISTS FOR (n:PDOcrReport) REQUIRE n.id IS UNIQUE",
	"CREATE CONSTRAINT pdw1614_definition IF NOT EXISTS FOR (n:PDDefinition) REQUIRE n.id IS UNIQUE",
	"CREATE INDEX pdw1614_family IF NOT EXISTS FOR (n:PDDocument) ON (n.family_key)",
	"CREATE INDEX pdw1614_current_version IF NOT EXISTS FOR (n:PDDocument) ON (n.current_version_id)",
	"CREATE INDEX pdw1614_graph_document IF NOT EXISTS FOR (n:PDGraphNode) ON (n.document_id)",
	"CREATE INDEX pdw1614_version_document IF NOT EXISTS FOR (n:PDVersion) ON (n.document_id)",
}

const pdw1614CreateDocuments = "UNWIND $docs AS d " +
	"CREATE (doc:PDDocument:PDGraphNode {id: d.id, current_version_id: d.version, family_key: d.family, summary_status: 'ready'}) " +
	"CREATE (v:PDVersion {id: d.version, document_id: d.id, current_source_id: d.source.id}) " +
	"CREATE (doc)-[:HAS_VERSION]->(v) " +
	"CREATE (v)-[:HAS_ORIGINAL]->(:PDOriginal {id: d.version + ':original', content_hash: d.version}) " +
	"CREATE (s:PDSearchSource) SET s += d.source " +
	"WITH d, doc, s MATCH (c:PDCollection {id: d.collection}) CREATE (doc)-[:MEMBER_OF]->(c) " +
	"WITH d, s UNWIND d.emails AS e CREATE (s)-[:HAS_EMAIL_OWN]->(:PDEmailText {id: e}) RETURN count(*) AS created"

const pdw1614CreateEntities = "UNWIND $entities AS e CREATE (n:PDGraphNode:PDEntity) SET n += e RETURN count(n) AS created"

const pdw1614CreateLinks = "UNWIND $links AS l MATCH (a:PDGraphNode {id: l.from}) MATCH (b:PDGraphNode {id: l.to}) " +
	"CREATE (a)-[r:PD_RELATION]->(b) SET r += l.props RETURN count(r) AS created"

func pdw1614DocumentRow(s pdw1614Sizes, i int) map[string]any {
	id := fmt.Sprintf("doc-%04d", i)
	version := fmt.Sprintf("ver-%04d", i)
	source := pdw1614Source(fmt.Sprintf("src-%04d", i), id, version, s.sourceChars, s.sections, s.views, s.pages, s.emailTexts,
		fmt.Sprintf("et-%04d", i), fmt.Sprintf("et-%04d", (i+s.docs-1)%s.docs))
	return map[string]any{"id": id, "version": version, "family": fmt.Sprintf("fam-%04d", i/2), "source": source,
		"collection": fmt.Sprintf("coll-%d", i%10), "emails": source["email_own_keys"]}
}

func pdw1614Link(id, from, to, document string) map[string]any {
	return map[string]any{"from": from, "to": to, "props": map[string]any{"id": id, "kind": "involves", "origin": "automatic",
		"stage": "interpretation", "document_id": document, "current": true}}
}

func pdw1614Batches(t *testing.T, store storage.Engine, query, key string, rows []any, size int) {
	t.Helper()
	for start := 0; start < len(rows); start += size {
		end := start + size
		if end > len(rows) {
			end = len(rows)
		}
		pdw1614Exec(t, store, query, map[string]any{key: rows[start:end]})
	}
}

func pdw1614Build(t *testing.T) *pdw1614Fixture {
	t.Helper()
	f := &pdw1614Fixture{t: t, dir: t.TempDir(), sizes: pdw1614DefaultSizes()}
	f.open()
	t.Cleanup(func() { f.close() })
	s := f.sizes
	started := time.Now()
	for _, statement := range pdw1614Schema {
		pdw1614Exec(t, f.store, statement, nil)
	}
	pdw1614Exec(t, f.store, "UNWIND range(0, 9) AS i CREATE (:PDCollection {id: 'coll-' + toString(i), archived: false})", nil)
	docs := []any{}
	for i := 0; i < s.docs; i++ {
		docs = append(docs, pdw1614DocumentRow(s, i))
	}
	pdw1614Batches(t, f.store, pdw1614CreateDocuments, "docs", docs, 20)
	entities := []any{}
	for k := 0; k < s.shared; k++ {
		entities = append(entities, map[string]any{"id": fmt.Sprintf("shared-%03d", k), "current": true})
	}
	links := []any{}
	for i := 0; i < s.docs; i++ {
		document := fmt.Sprintf("doc-%04d", i)
		count, linkCount := s.entities, s.links
		if i < s.selected {
			count, linkCount = s.selectedEntities, s.selectedLinks[i]
			f.selected = append(f.selected, document)
		}
		for k := 0; k < count; k++ {
			entities = append(entities, map[string]any{"id": fmt.Sprintf("ent-%04d-%03d", i, k), "document_id": document, "current": true})
		}
		for m := 0; m < linkCount; m++ {
			from := fmt.Sprintf("ent-%04d-%03d", i, m%count)
			to := fmt.Sprintf("ent-%04d-%03d", i, (m+1+m/count)%count)
			if m%2 == 0 {
				to = fmt.Sprintf("shared-%03d", (i*7+m)%s.shared)
			}
			id := fmt.Sprintf("lnk-%04d-%03d", i, m)
			links = append(links, pdw1614Link(id, from, to, document))
			if i < s.selected {
				f.seeds = append(f.seeds, map[string]any{"from": from, "link": id})
			}
		}
	}
	pdw1614Batches(t, f.store, pdw1614CreateEntities, "entities", entities, 1000)
	pdw1614Batches(t, f.store, pdw1614CreateLinks, "links", links, 1000)
	// Record sets and their member sources (the GROUPS part of SEARCH).
	sets, members := []any{}, []any{}
	for g := 0; g < s.sets; g++ {
		setID := fmt.Sprintf("set-%02d", g)
		sets = append(sets, map[string]any{"id": setID, "document_id": fmt.Sprintf("doc-%04d", 100+g), "title": "Schedule " + setID,
			"records": int64(s.membersPerSet), "fields": []any{"date", "amount"}, "members": int64(s.membersPerSet)})
		for r := 0; r < s.membersPerSet; r++ {
			id := fmt.Sprintf("row-%02d-%02d", g, r)
			source := pdw1614Source(id, fmt.Sprintf("doc-%04d", 100+g), fmt.Sprintf("ver-%04d", 100+g), s.memberChars, 8, 8, 2, 4,
				fmt.Sprintf("et-row-%02d-%02d", g, r), fmt.Sprintf("et-%04d", 100+g))
			source["record_set_id"] = setID
			members = append(members, source)
		}
	}
	pdw1614Exec(t, f.store, "UNWIND $sets AS s CREATE (n:PDRecordSet) SET n += s", map[string]any{"sets": sets})
	pdw1614Batches(t, f.store, "UNWIND $members AS m CREATE (n:PDSearchSource) SET n += m "+
		"WITH n, m UNWIND m.email_own_keys AS e CREATE (n)-[:HAS_EMAIL_OWN]->(:PDEmailText {id: e}) RETURN count(*) AS created", "members", members, 50)
	f.search = pdw1614SearchWithoutRetrieval(t, pdw1614Statement(t, "SEARCH.stock", pdw1614SearchSHA))
	f.evaluate = pdw1614Statement(t, "EVALUATE.links", pdw1614EvaluateSHA)
	counts := pdw1614Exec(t, f.store, "MATCH (n) WITH count(n) AS nodes MATCH ()-[r]->() RETURN nodes, count(r) AS relationships", nil)
	t.Logf("PDW1614 fixture built in %v: docs=%d seeds=%d selected=%v counts=%v GOMAXPROCS=%d NumCPU=%d",
		time.Since(started).Round(time.Millisecond), s.docs, len(f.seeds), f.selected, counts.Rows, runtime.GOMAXPROCS(0), runtime.NumCPU())
	return f
}

func (f *pdw1614Fixture) open() {
	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: f.dir})
	if err != nil {
		f.t.Fatal(err)
	}
	f.engine = engine
	f.store = storage.NewNamespacedEngine(engine, "test")
}

func (f *pdw1614Fixture) close() {
	if f.engine != nil {
		_ = f.engine.Close()
		f.engine = nil
	}
}

// reopen closes and reopens the engine on the same data: open folds every
// persisted count delta.
func (f *pdw1614Fixture) reopen() {
	f.close()
	f.open()
}

func (f *pdw1614Fixture) searchParams(matches int, groups bool) map[string]any {
	s := f.sizes
	results := []any{}
	for h := 0; h < s.hits; h++ {
		results = append(results, map[string]any{"id": fmt.Sprintf("src-%04d", h*7+20), "score": 1.0 - float64(h)/100,
			"matches": pdw1614Matches(matches, s.sourceChars, "lease")})
	}
	members := []any{}
	if groups {
		for g := 0; g < s.sets; g++ {
			for r := 0; r < s.membersPerSet; r++ {
				members = append(members, map[string]any{"id": fmt.Sprintf("row-%02d-%02d", g, r), "score": 0.4,
					"matches": pdw1614Matches(s.memberMatches, s.memberChars, "lease")})
			}
		}
	}
	return map[string]any{
		"qid": nil, "anchors": []any{}, "query": "lease renewal schedule", "history": false, "page_size": int64(s.hits),
		"min_similarity": 0.5, "rerank_min_score": 0.0,
		"zz_page": map[string]any{"results": results, "search_method": "rrf_hybrid+rerank", "fallback_triggered": false,
			"fallback_reason": "", "query_chunks_searched": int64(0), "query_chunks_total": int64(0), "qid": "page-1",
			"has_more": true, "total": nil},
		"zz_members": members,
	}
}

func (f *pdw1614Fixture) evaluateParams() map[string]any {
	return map[string]any{"links": f.seeds, "node_ids": []any{}, "documents": []any{}, "correction_ids": []any{},
		"selected": f.selected, "gone_ids": []any{}}
}

// ---- measurement ----

type pdw1614Run struct {
	wall    time.Duration
	rows    int
	err     error
	summary string
}

type pdw1614Window struct {
	wall      time.Duration
	cpu       map[string]time.Duration
	total     time.Duration
	flat      map[string]int64
	cum       map[string]int64
	buckets   map[string]int64
	stmtNs    int64
	schedMean float64
	schedP99  float64
	mutexWait float64
	gcCPU     float64
}

var pdw1614BucketOrder = []string{"textScan", "evaluateRowValueText", "staticCheck", "parse", "programCache", "comprehension",
	"mapMaterialize", "countStore", "badgerIterator", "storageRead", "propertyDecode", "lifecycle", "mvcc", "gcAlloc", "subquery"}

var pdw1614Buckets = map[string]*regexp.Regexp{
	"textScan":             regexp.MustCompile(`internal/cyphertext\.|cypher\.(splitAtOperator|splitByOperatorOutsideCase|findTopLevelOperator|keywordIndexFrom|topLevelKeywordIndex|upperASCII|infixKeywordIndex|skipASCIISpaces|isRowReceiverIdentifierAt|containsFold)`),
	"evaluateRowValueText": regexp.MustCompile(`\.evaluateRowValueText$`),
	"staticCheck":          regexp.MustCompile(`staticOperatorChecker|[Ss]emantic[Vv]alidat|validateStatic`),
	"parse":                regexp.MustCompile(`/antlr\.|cypher\.parse[A-Z]|\.parse[A-Z][A-Za-z]*$`),
	"programCache":         regexp.MustCompile(`boundedCache|bounded_cache|programCache`),
	"comprehension":        regexp.MustCompile(`rowComprehensionLoop|rowQuantifierLoop|quantifierFold`),
	"mapMaterialize":       regexp.MustCompile(`materializePropertyEntries|materializePropertyExpressions`),
	"countStore":           regexp.MustCompile(`ReadCountStore|sumCountDeltasInTxn|traversalSeed|countStoreRead`),
	"badgerIterator":       regexp.MustCompile(`dgraph-io/badger/v4\.\(\*Iterator\)`),
	"storageRead":          regexp.MustCompile(`orneryd/nornicdb/pkg/storage\.`),
	"propertyDecode":       regexp.MustCompile(`pkg/storage\..*([Dd]ecode|[Uu]nmarshal|propertyCodec|ownedEntries|copyProperties)`),
	"lifecycle":            regexp.MustCompile(`pkg/storage/lifecycle\.`),
	"mvcc":                 regexp.MustCompile(`pkg/storage\..*(mvcc|MVCC|[Ss]napshot)`),
	"gcAlloc":              regexp.MustCompile(`^runtime\.(mallocgc|gcAssist|growslice|mapassign|makemap|newobject|makeslice|gcDrain|scanobject)`),
	"subquery":             regexp.MustCompile(`subquery|Subquery|[Ee]xists[A-Z]`),
}

func pdw1614Metrics() (hist *metrics.Float64Histogram, mutex, gc float64) {
	samples := []metrics.Sample{{Name: "/sched/latencies:seconds"}, {Name: "/sync/mutex/wait/total:seconds"}, {Name: "/cpu/classes/gc/total:cpu-seconds"}}
	metrics.Read(samples)
	if samples[0].Value.Kind() == metrics.KindFloat64Histogram {
		h := samples[0].Value.Float64Histogram()
		hist = &metrics.Float64Histogram{Counts: append([]uint64(nil), h.Counts...), Buckets: h.Buckets}
	}
	if samples[1].Value.Kind() == metrics.KindFloat64 {
		mutex = samples[1].Value.Float64()
	}
	if samples[2].Value.Kind() == metrics.KindFloat64 {
		gc = samples[2].Value.Float64()
	}
	return
}

func pdw1614HistDelta(before, after *metrics.Float64Histogram) (mean, p99 float64) {
	if before == nil || after == nil {
		return math.NaN(), math.NaN()
	}
	var total uint64
	var sum float64
	counts := make([]uint64, len(after.Counts))
	for i := range after.Counts {
		counts[i] = after.Counts[i] - before.Counts[i]
		total += counts[i]
		lo, hi := after.Buckets[i], after.Buckets[i+1]
		if math.IsInf(hi, 1) {
			hi = lo
		}
		if math.IsInf(lo, -1) {
			lo = 0
		}
		sum += float64(counts[i]) * (lo + hi) / 2
	}
	if total == 0 {
		return 0, 0
	}
	var seen uint64
	for i, c := range counts {
		seen += c
		if float64(seen) >= 0.99*float64(total) {
			p99 = after.Buckets[i+1]
			break
		}
	}
	return sum / float64(total), p99
}

// pdw1614Measure runs fn under a CPU profile and returns the profile's
// on-CPU time by pdw label, and the statement goroutines' functions.
func pdw1614Measure(t *testing.T, fn func()) pdw1614Window {
	t.Helper()
	var buf bytes.Buffer
	hb, mb, gb := pdw1614Metrics()
	if err := pprof.StartCPUProfile(&buf); err != nil {
		t.Fatalf("cpu profile: %v", err)
	}
	start := time.Now()
	fn()
	wall := time.Since(start)
	pprof.StopCPUProfile()
	ha, ma, ga := pdw1614Metrics()
	w := pdw1614Window{wall: wall, cpu: map[string]time.Duration{}, flat: map[string]int64{}, cum: map[string]int64{}, buckets: map[string]int64{}}
	w.schedMean, w.schedP99 = pdw1614HistDelta(hb, ha)
	w.mutexWait, w.gcCPU = ma-mb, ga-gb
	prof, err := profile.Parse(&buf)
	if err != nil {
		t.Logf("PDW1614 profile parse: %v", err)
		return w
	}
	cpuIndex := len(prof.SampleType) - 1
	for i, st := range prof.SampleType {
		if st.Type == "cpu" {
			cpuIndex = i
		}
	}
	for _, sample := range prof.Sample {
		ns := sample.Value[cpuIndex]
		label := "unlabeled"
		if values := sample.Label["pdw"]; len(values) > 0 {
			label = values[0]
		}
		w.cpu[label] += time.Duration(ns)
		w.total += time.Duration(ns)
		if label != "stmt" {
			continue
		}
		w.stmtNs += ns
		seen := map[string]bool{}
		leaf := ""
		for _, location := range sample.Location {
			for _, line := range location.Line {
				if line.Function == nil {
					continue
				}
				name := line.Function.Name
				if leaf == "" {
					leaf = name
				}
				if !seen[name] {
					seen[name] = true
					w.cum[name] += ns
				}
			}
		}
		w.flat[leaf] += ns
		for bucket, pattern := range pdw1614Buckets {
			for name := range seen {
				if pattern.MatchString(name) {
					w.buckets[bucket] += ns
					break
				}
			}
		}
	}
	return w
}

func pdw1614Top(values map[string]int64, total int64, n int, filter func(string) bool) string {
	type kv struct {
		k string
		v int64
	}
	list := []kv{}
	for k, v := range values {
		if filter == nil || filter(k) {
			list = append(list, kv{k, v})
		}
	}
	sort.Slice(list, func(i, j int) bool { return list[i].v > list[j].v })
	parts := []string{}
	for i := 0; i < len(list) && i < n; i++ {
		name := list[i].k
		if at := strings.LastIndex(name, "/"); at >= 0 {
			name = name[at+1:]
		}
		parts = append(parts, fmt.Sprintf("%s=%.1f%%", name, 100*float64(list[i].v)/float64(max(total, 1))))
	}
	return strings.Join(parts, " ")
}

func (w pdw1614Window) log(t *testing.T, name string, statements int, detail bool) {
	t.Helper()
	buckets := []string{}
	for _, bucket := range pdw1614BucketOrder {
		buckets = append(buckets, fmt.Sprintf("%s=%.1f%%", bucket, 100*float64(w.buckets[bucket])/float64(max(w.stmtNs, 1))))
	}
	labels := []string{}
	for label, d := range w.cpu {
		labels = append(labels, fmt.Sprintf("%s=%v", label, d.Round(time.Millisecond)))
	}
	sort.Strings(labels)
	t.Logf("PDW1614 window %s wall=%v statements=%d stmtCPU=%v stmtCPU/statement=%v cpuByLabel{%s} processCPU=%v gcCPU=%.2fs schedLatencyMean=%.3fms schedLatencyP99=%.3fms mutexWait=%.3fs buckets{%s}",
		name, w.wall.Round(time.Millisecond), statements, time.Duration(w.stmtNs).Round(time.Millisecond),
		(time.Duration(w.stmtNs) / time.Duration(max(statements, 1))).Round(time.Millisecond), strings.Join(labels, " "),
		w.total.Round(time.Millisecond), w.gcCPU, w.schedMean*1000, w.schedP99*1000, w.mutexWait, strings.Join(buckets, " "))
	if detail {
		t.Logf("PDW1614 window %s flat{%s}", name, pdw1614Top(w.flat, w.stmtNs, 25, nil))
		t.Logf("PDW1614 window %s cum-nornicdb{%s}", name, pdw1614Top(w.cum, w.stmtNs, 45, func(n string) bool {
			return strings.Contains(n, "orneryd/nornicdb") && !strings.Contains(n, "testing.") && !strings.Contains(n, "zz_pdw1614") &&
				!strings.Contains(n, "pdw1614")
		}))
	}
}

// pdw1614Statement runs query labelled pdw=stmt, in an explicit transaction
// when tx is set (BEGIN, the statement, COMMIT), as one auto-commit
// statement otherwise.
func pdw1614RunStatement(store storage.Engine, query string, params map[string]any, tx bool) pdw1614Run {
	var run pdw1614Run
	pprof.Do(context.Background(), pprof.Labels("pdw", "stmt"), func(ctx context.Context) {
		exec := NewStorageExecutor(store)
		start := time.Now()
		if tx {
			if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
				run.err = err
				return
			}
		}
		result, err := exec.Execute(ctx, query, params)
		if tx {
			if err != nil {
				_, _ = exec.Execute(ctx, "ROLLBACK", nil)
			} else if _, commitErr := exec.Execute(ctx, "COMMIT", nil); commitErr != nil {
				err = commitErr
			}
		}
		run.wall = time.Since(start)
		run.err = err
		if result != nil {
			run.rows = len(result.Rows)
			if len(result.Rows) > 0 {
				run.summary = pdw1614RowSummary(result)
			}
		}
	})
	return run
}

func pdw1614RowSummary(result *ExecuteResult) string {
	row := result.Rows[0]
	for i, column := range result.Columns {
		if column == "evaluate_diag" {
			diag, _ := row[i].(map[string]interface{})
			stage := func(a, b string) any {
				x, okx := pdw1614Int(diag[a])
				y, oky := pdw1614Int(diag[b])
				if !okx || !oky {
					return "?"
				}
				return y - x
			}
			rows := 0
			for j, c := range result.Columns {
				if c == "rows" {
					if list, ok := row[j].([]interface{}); ok {
						rows = len(list)
					}
				}
			}
			return fmt.Sprintf("stages_ms{affected=%v candidates=%v verdicts=%v output=%v} found=%v seeds=%v outputRows=%d",
				stage("start", "affected"), stage("affected", "candidates"), stage("candidates", "verdicts"), stage("verdicts", "finished"),
				diag["found"], diag["seeds"], rows)
		}
	}
	return fmt.Sprintf("rows=%d columns=%v", len(result.Rows), result.Columns)
}

func pdw1614Int(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case float64:
		return int64(x), true
	}
	return 0, false
}

// ---- background load ----

type pdw1614Load struct {
	stop     chan struct{}
	wg       sync.WaitGroup
	commits  atomic.Int64
	failures atomic.Int64
	commitNs atomic.Int64
	started  time.Time
	lastErr  atomic.Value
}

// pdw1614StartWriters starts n import-shaped writers. Each transaction
// creates one document with its version, an email-shaped source of the
// fixture's size and its email texts, 10 entities and 40 links (half of
// them to shared entities when shared is set, else among the new
// entities), and updates one existing document, then commits.
func (f *pdw1614Fixture) startWriters(n int, shared bool, label string) *pdw1614Load {
	load := &pdw1614Load{stop: make(chan struct{}), started: time.Now()}
	s := f.sizes
	for g := 0; g < n; g++ {
		load.wg.Add(1)
		go pprof.Do(context.Background(), pprof.Labels("pdw", "writer"), func(ctx context.Context) {
			defer load.wg.Done()
			for seq := 0; ; seq++ {
				select {
				case <-load.stop:
					return
				default:
				}
				id := fmt.Sprintf("w-%s-%d-%05d", label, g, seq)
				version := id + ":v"
				source := pdw1614Source(id+":s", id, version, s.sourceChars, s.sections, s.views, s.pages, s.emailTexts, "et-"+id, fmt.Sprintf("et-%04d", seq%s.docs))
				entities, links := []any{}, []any{}
				for k := 0; k < 10; k++ {
					entities = append(entities, map[string]any{"id": fmt.Sprintf("%s:e%02d", id, k), "document_id": id, "current": true})
				}
				for m := 0; m < 40; m++ {
					to := fmt.Sprintf("%s:e%02d", id, (m+1)%10)
					if shared && m%2 == 0 {
						to = fmt.Sprintf("shared-%03d", (seq*13+m)%s.shared)
					}
					links = append(links, pdw1614Link(fmt.Sprintf("%s:l%02d", id, m), fmt.Sprintf("%s:e%02d", id, m%10), to, id))
				}
				exec := NewStorageExecutor(f.store)
				began := time.Now()
				err := func() error {
					if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
						return err
					}
					steps := []struct {
						q string
						p map[string]any
					}{
						{pdw1614CreateDocuments, map[string]any{"docs": []any{map[string]any{"id": id, "version": version, "family": "fam-" + id,
							"source": source, "collection": fmt.Sprintf("coll-%d", seq%10), "emails": source["email_own_keys"]}}}},
						{pdw1614CreateEntities, map[string]any{"entities": entities}},
						{pdw1614CreateLinks, map[string]any{"links": links}},
						{"MATCH (d:PDDocument {id: $id}) SET d.summary_status = 'writing', d.touched = $seq", map[string]any{"id": fmt.Sprintf("doc-%04d", (seq*31+g)%s.docs), "seq": int64(seq)}},
					}
					for _, step := range steps {
						if _, err := exec.Execute(ctx, step.q, step.p); err != nil {
							_, _ = exec.Execute(ctx, "ROLLBACK", nil)
							return err
						}
					}
					_, err := exec.Execute(ctx, "COMMIT", nil)
					return err
				}()
				if err != nil {
					load.failures.Add(1)
					load.lastErr.Store(err.Error())
					continue
				}
				load.commits.Add(1)
				load.commitNs.Add(int64(time.Since(began)))
			}
		})
	}
	return load
}

// pdw1614StartBurn starts n goroutines that only spin on the CPU.
func pdw1614StartBurn(n int) *pdw1614Load {
	load := &pdw1614Load{stop: make(chan struct{}), started: time.Now()}
	for g := 0; g < n; g++ {
		load.wg.Add(1)
		go pprof.Do(context.Background(), pprof.Labels("pdw", "burn"), func(context.Context) {
			defer load.wg.Done()
			x := uint64(g + 1)
			for {
				for i := 0; i < 1_000_000; i++ {
					x ^= x << 13
					x ^= x >> 7
					x ^= x << 17
				}
				select {
				case <-load.stop:
					if x == 42 {
						fmt.Print("")
					}
					return
				default:
				}
			}
		})
	}
	return load
}

func (l *pdw1614Load) halt(t *testing.T, name string) {
	t.Helper()
	if l == nil {
		return
	}
	close(l.stop)
	l.wg.Wait()
	elapsed := time.Since(l.started)
	commits := l.commits.Load()
	mean := time.Duration(0)
	if commits > 0 {
		mean = time.Duration(l.commitNs.Load() / commits)
	}
	last, _ := l.lastErr.Load().(string)
	t.Logf("PDW1614 load %s ran=%v commits=%d commits/s=%.2f meanTx=%v failures=%d lastError=%.200q",
		name, elapsed.Round(time.Millisecond), commits, float64(commits)/elapsed.Seconds(), mean.Round(time.Millisecond), l.failures.Load(), last)
}

// pdw1614CountProbe times count-store reads of the kinds the traversal
// seed planner makes.
func (f *pdw1614Fixture) countProbe(t *testing.T, name string) {
	t.Helper()
	reader, ok := f.store.(interface {
		ReadCountStore(context.Context, storage.CountStoreReadRequest) (storage.CountStoreRead, error)
	})
	if !ok {
		t.Logf("PDW1614 count-probe %s: store has no ReadCountStore", name)
		return
	}
	requests := []struct {
		name    string
		request storage.CountStoreReadRequest
	}{
		{"nodes:PDGraphNode", storage.CountStoreReadRequest{Kind: storage.CountStoreNodesByLabel, Name: "PDGraphNode"}},
		{"nodes:PDEmailText", storage.CountStoreReadRequest{Kind: storage.CountStoreNodesByLabel, Name: "PDEmailText"}},
		{"end:PDEmailText/HAS_EMAIL_OWN", storage.CountStoreReadRequest{Kind: storage.CountStoreRelationshipsByEndLabel, Name: "PDEmailText", Types: []string{"HAS_EMAIL_OWN"}}},
		{"start:PDGraphNode/PD_RELATION", storage.CountStoreReadRequest{Kind: storage.CountStoreRelationshipsByStartLabel, Name: "PDGraphNode", Types: []string{"PD_RELATION"}}},
	}
	parts := []string{}
	for _, r := range requests {
		const calls = 50
		start := time.Now()
		var answer storage.CountStoreRead
		var err error
		for i := 0; i < calls; i++ {
			answer, err = reader.ReadCountStore(context.Background(), r.request)
		}
		parts = append(parts, fmt.Sprintf("%s: %v/call count=%d describes=%v err=%v", r.name, (time.Since(start) / calls).Round(time.Microsecond), answer.Count, answer.DescribesRows, err))
	}
	t.Logf("PDW1614 count-probe %s {%s}", name, strings.Join(parts, "; "))
}

// ---- observation 1: SEARCH ----

func (f *pdw1614Fixture) searchRound(t *testing.T, phase string, concurrent int, matches int, groups bool, rounds int) {
	t.Helper()
	params := f.searchParams(matches, groups)
	runs := make([]pdw1614Run, 0, concurrent*rounds)
	var mu sync.Mutex
	w := pdw1614Measure(t, func() {
		for round := 0; round < rounds; round++ {
			var wg sync.WaitGroup
			for c := 0; c < concurrent; c++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					run := pdw1614RunStatement(f.store, f.search, params, false)
					mu.Lock()
					runs = append(runs, run)
					mu.Unlock()
				}()
			}
			wg.Wait()
		}
	})
	walls := []string{}
	for _, run := range runs {
		if run.err != nil {
			t.Fatalf("SEARCH %s: %v", phase, run.err)
		}
		walls = append(walls, fmt.Sprintf("%v(rows=%d)", run.wall.Round(time.Millisecond), run.rows))
	}
	name := fmt.Sprintf("search/%s concurrent=%d matches=%d groups=%v", phase, concurrent, matches, groups)
	t.Logf("PDW1614 runs %s walls=[%s]", name, strings.Join(walls, " "))
	w.log(t, name, len(runs), true)
}

func TestPDW1614SearchProjection(t *testing.T) {
	f := pdw1614Build(t)
	check := pdw1614RunStatement(f.store, f.search, f.searchParams(f.sizes.matches, true), false)
	if check.err != nil {
		t.Fatalf("SEARCH: %v", check.err)
	}
	t.Logf("PDW1614 search warm wall=%v rows=%d", check.wall, check.rows)
	f.countProbe(t, "quiet")
	f.searchRound(t, "quiet", 1, f.sizes.matches, false, 3)
	f.searchRound(t, "quiet", 1, f.sizes.matches, true, 3)
	f.searchRound(t, "quiet-double-matches", 1, 2*f.sizes.matches, false, 2)
	f.searchRound(t, "quiet", 4, f.sizes.matches, true, 2)

	writers := f.startWriters(2, true, "s1")
	time.Sleep(5 * time.Second)
	f.countProbe(t, "writers")
	f.searchRound(t, "writers", 1, f.sizes.matches, false, 3)
	f.searchRound(t, "writers", 1, f.sizes.matches, true, 3)
	f.searchRound(t, "writers", 4, f.sizes.matches, true, 2)
	f.countProbe(t, "writers-end")
	writers.halt(t, "search-writers")

	f.countProbe(t, "after")
	f.searchRound(t, "after", 1, f.sizes.matches, false, 3)
	f.searchRound(t, "after", 1, f.sizes.matches, true, 3)

	burn := pdw1614StartBurn(2)
	f.searchRound(t, "burn", 1, f.sizes.matches, false, 3)
	f.searchRound(t, "burn", 1, f.sizes.matches, true, 3)
	burn.halt(t, "search-burn")

	f.reopen()
	f.countProbe(t, "reopened")
	f.searchRound(t, "reopened", 1, f.sizes.matches, false, 3)
}

// ---- observation 2: EVALUATE ----

func (f *pdw1614Fixture) evaluateRound(t *testing.T, phase string, tx bool, runs int) {
	t.Helper()
	params := f.evaluateParams()
	results := []pdw1614Run{}
	w := pdw1614Measure(t, func() {
		for i := 0; i < runs; i++ {
			results = append(results, pdw1614RunStatement(f.store, f.evaluate, params, tx))
		}
	})
	name := fmt.Sprintf("evaluate/%s tx=%v", phase, tx)
	for i, run := range results {
		if run.err != nil {
			t.Fatalf("EVALUATE %s: %v", phase, run.err)
		}
		t.Logf("PDW1614 run %s #%d wall=%v %s", name, i, run.wall.Round(time.Millisecond), run.summary)
	}
	w.log(t, name, len(results), true)
}

func TestPDW1614EvaluateUnderWrites(t *testing.T) {
	f := pdw1614Build(t)
	params := f.evaluateParams()
	first := pdw1614RunStatement(f.store, f.evaluate, params, true)
	if first.err != nil {
		t.Fatalf("EVALUATE: %v", first.err)
	}
	t.Logf("PDW1614 evaluate first wall=%v %s", first.wall, first.summary)
	// Store each link's inputs as applied, so a later evaluation changes no
	// verdict (the observed call: 2,348 candidates, zero changed).
	result := pdw1614Exec(t, f.store, f.evaluate, params)
	rows, _ := result.Rows[0][0].([]interface{})
	applied := []any{}
	for _, row := range rows {
		m, _ := row.(map[string]interface{})
		applied = append(applied, map[string]any{"from": m["from"], "link": m["link"], "inputs": m["inputs"]})
	}
	pdw1614Batches(t, f.store, "UNWIND $rows AS row MATCH (s:PDGraphNode {id: row.from})-[r:PD_RELATION]->() WHERE r.id = row.link "+
		"SET r.eff_applied = row.inputs RETURN count(r) AS applied", "rows", applied, 500)
	t.Logf("PDW1614 evaluate applied inputs of %d links", len(applied))

	f.countProbe(t, "quiet")
	f.evaluateRound(t, "quiet", true, 3)
	f.evaluateRound(t, "quiet", false, 1)

	writers := f.startWriters(2, true, "e1")
	time.Sleep(5 * time.Second)
	f.countProbe(t, "writers-shared")
	f.evaluateRound(t, "writers-shared", true, 3)
	f.evaluateRound(t, "writers-shared", false, 1)
	f.countProbe(t, "writers-shared-end")
	writers.halt(t, "evaluate-writers-shared")

	f.countProbe(t, "after-shared")
	f.evaluateRound(t, "after-shared", true, 3)

	disjoint := f.startWriters(2, false, "e2")
	time.Sleep(5 * time.Second)
	f.evaluateRound(t, "writers-disjoint", true, 3)
	disjoint.halt(t, "evaluate-writers-disjoint")

	burn := pdw1614StartBurn(2)
	f.evaluateRound(t, "burn", true, 3)
	burn.halt(t, "evaluate-burn")

	// The application's condition: four searchers and two writers together.
	writers = f.startWriters(2, true, "e3")
	searchParams := f.searchParams(f.sizes.matches, true)
	searchers := &pdw1614Load{stop: make(chan struct{}), started: time.Now()}
	for c := 0; c < 4; c++ {
		searchers.wg.Add(1)
		go func() {
			defer searchers.wg.Done()
			for {
				select {
				case <-searchers.stop:
					return
				default:
				}
				if run := pdw1614RunStatementLabelled(f.store, f.search, searchParams, "searcher"); run.err == nil {
					searchers.commits.Add(1)
					searchers.commitNs.Add(int64(run.wall))
				} else {
					searchers.failures.Add(1)
					searchers.lastErr.Store(run.err.Error())
				}
			}
		}()
	}
	time.Sleep(5 * time.Second)
	f.evaluateRound(t, "writers+4searchers", true, 2)
	searchers.halt(t, "evaluate-searchers (commits=searches)")
	writers.halt(t, "evaluate-writers-e3")

	f.evaluateRound(t, "after-all", true, 3)
	f.reopen()
	f.countProbe(t, "reopened")
	f.evaluateRound(t, "reopened", true, 3)
}

// pdw1614RunStatementLabelled is pdw1614RunStatement with another label, for
// statements that are load rather than the measured statement.
func pdw1614RunStatementLabelled(store storage.Engine, query string, params map[string]any, label string) pdw1614Run {
	var run pdw1614Run
	pprof.Do(context.Background(), pprof.Labels("pdw", label), func(ctx context.Context) {
		start := time.Now()
		result, err := NewStorageExecutor(store).Execute(ctx, query, params)
		run.wall, run.err = time.Since(start), err
		if result != nil {
			run.rows = len(result.Rows)
		}
	})
	return run
}

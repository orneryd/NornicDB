package cypher

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

var pr1682QuerySequence atomic.Uint64

const (
	// pr1682GeneratedReadings and pr1682GeneratedRecords are the default sizes
	// of the generated Record/Reading workload that substitutes for the
	// unavailable original application fixture (NOR-214). The substitute
	// exercises the reported query shapes; it does not establish results for
	// that original application suite.
	pr1682GeneratedReadings = 10000
	pr1682GeneratedRecords  = 2000

	// pr1682GeneratedScanStart and pr1682GeneratedScanEnd bound the datetime
	// label scan. Reading i is created at 2026-01-01T00:00:00Z plus i hours, so
	// this window covers the 24 readings 168..191.
	pr1682GeneratedScanStart = "2026-01-08T00:00:00Z"
	pr1682GeneratedScanEnd   = "2026-01-09T00:00:00Z"
	pr1682GeneratedScanRows  = 24
	pr1682GeneratedScanFirst = "reading-168"
	pr1682GeneratedScanLast  = "reading-191"

	// pr1682GeneratedLookupCount is how many ids the scoped lookup starts from.
	pr1682GeneratedLookupCount = 25
)

var pr1682GeneratedCreatedAtBase = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

// pr1682GeneratedGraph is the deterministic in-memory Record/Reading graph
// shared by the NOR-214 generated query-shape benchmarks and correctness
// tests. Reading i carries a 1 KiB slice of repository documentation, the
// existing "reading-%d" identifier, and a created_at instant one hour after
// 2026-01-01T00:00:00Z. Record i points at reading (i*5).
type pr1682GeneratedGraph struct {
	exec *StorageExecutor
	doc  []byte
}

// newPR1682GeneratedGraph builds the generated workload on an ephemeral,
// test-owned in-memory engine. No remote database, production fixture, or
// original R17 dataset is involved.
func newPR1682GeneratedGraph(tb testing.TB, readings, records int) *pr1682GeneratedGraph {
	tb.Helper()
	root := newTestMemoryEngine(tb)
	store := storage.NewNamespacedEngine(root, "bench")
	doc := pr1682DocumentFixture(tb)
	ctx := context.Background()
	for i := 0; i < readings; i++ {
		start := (i * 97) % (len(doc) - 1024)
		_, err := store.CreateNode(ctx, &storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("reading-%d", i)),
			Labels: []string{"Reading"},
			Properties: map[string]any{
				"id":         fmt.Sprintf("reading-%d", i),
				"content":    string(doc[start : start+1024]),
				"source":     "repository documentation",
				"created_at": CypherDateTime{Time: pr1682GeneratedReadingCreatedAt(i)},
			},
		})
		if err != nil {
			tb.Fatalf("create generated reading %d: %v", i, err)
		}
	}
	for i := 0; i < records; i++ {
		_, err := store.CreateNode(ctx, &storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("record-%d", i)),
			Labels: []string{"Record"},
			Properties: map[string]any{
				"id":         i,
				"reading_id": fmt.Sprintf("reading-%d", i*5),
			},
		})
		if err != nil {
			tb.Fatalf("create generated record %d: %v", i, err)
		}
	}
	return &pr1682GeneratedGraph{exec: NewStorageExecutor(store), doc: doc}
}

// pr1682GeneratedReadingCreatedAt is the deterministic created_at of reading i.
func pr1682GeneratedReadingCreatedAt(i int) time.Time {
	return pr1682GeneratedCreatedAtBase.Add(time.Duration(i) * time.Hour)
}

// pr1682DocumentFixture reads the repository documentation payload. The
// optional PR1682_DOCUMENT_FIXTURE override exists so the parent can compare
// immutable baselines on the same bytes; the default is the repository README.
func pr1682DocumentFixture(tb testing.TB) []byte {
	tb.Helper()
	fixture := os.Getenv("PR1682_DOCUMENT_FIXTURE")
	if fixture == "" {
		fixture = "../../README.md"
	}
	doc, err := os.ReadFile(fixture)
	if err != nil || len(doc) < 1024 {
		tb.Fatalf("read documentation fixture: %v (bytes=%d)", err, len(doc))
	}
	return doc
}

// pr1682GeneratedLookupIDs returns the Reading ids belonging to the first 25
// generated Records, in the fixture's Record -> Reading id order.
func pr1682GeneratedLookupIDs() []interface{} {
	ids := make([]interface{}, pr1682GeneratedLookupCount)
	for i := range ids {
		ids[i] = fmt.Sprintf("reading-%d", i*5)
	}
	return ids
}

// pr1682GeneratedDatetimeScanQuery is the substituted NOR-214 datetime label
// scan: a label scan restricted by a datetime range. A distinct statement
// inlines a fresh constant predicate so the statement cache cannot serve an
// earlier text; the predicate is constant true and never changes the rows.
func pr1682GeneratedDatetimeScanQuery(distinct bool) string {
	var query strings.Builder
	query.WriteString("MATCH (r:Reading) WHERE r.created_at >= datetime('" + pr1682GeneratedScanStart + "') AND r.created_at < datetime('" + pr1682GeneratedScanEnd + "') ")
	if distinct {
		id := pr1682QuerySequence.Add(1)
		fmt.Fprintf(&query, "AND %d = %d ", id, id)
	}
	query.WriteString("RETURN count(r) AS readings, min(r.id) AS first_id, max(r.id) AS last_id")
	return query.String()
}

// pr1682GeneratedLookupQuery is the substituted NOR-214 scoped lookup shape:
// CALL { UNWIND $ids ... MATCH ... } resolving each supplied id. A distinct
// statement inlines a fresh constant predicate inside the subquery so the
// statement cache cannot serve an earlier text; it never changes the rows.
func pr1682GeneratedLookupQuery(distinct bool) string {
	var query strings.Builder
	query.WriteString("CALL { UNWIND $ids AS id MATCH (r:Reading {id: id}) ")
	if distinct {
		id := pr1682QuerySequence.Add(1)
		fmt.Fprintf(&query, "WHERE %d = %d ", id, id)
	}
	query.WriteString("RETURN r.id AS id, r.source AS source } RETURN id, source")
	return query.String()
}

// pr1682AssertDatetimeScanResult checks the exact substituted scan result; the
// correctness tests and both scan benchmarks share it.
func pr1682AssertDatetimeScanResult(tb testing.TB, result *ExecuteResult) {
	tb.Helper()
	if len(result.Rows) != 1 || len(result.Rows[0]) != 3 {
		tb.Fatalf("datetime scan rows: %#v", result.Rows)
	}
	row := result.Rows[0]
	if row[0] != int64(pr1682GeneratedScanRows) || row[1] != pr1682GeneratedScanFirst || row[2] != pr1682GeneratedScanLast {
		tb.Fatalf("datetime scan result: %#v", row)
	}
}

// pr1682AssertLookupResult checks the exact substituted scoped-lookup result.
func pr1682AssertLookupResult(tb testing.TB, result *ExecuteResult, ids []interface{}) {
	tb.Helper()
	if len(result.Rows) != len(ids) {
		tb.Fatalf("lookup rows: got %d, want %d (%#v)", len(result.Rows), len(ids), result.Rows)
	}
	seen := make(map[string]bool, len(ids))
	for _, row := range result.Rows {
		if len(row) != 2 {
			tb.Fatalf("lookup row width: %#v", row)
		}
		id, ok := row[0].(string)
		if !ok || row[1] != "repository documentation" {
			tb.Fatalf("lookup row values: %#v", row)
		}
		seen[id] = true
	}
	for _, want := range ids {
		if !seen[want.(string)] {
			tb.Fatalf("lookup result is missing %v: %#v", want, result.Rows)
		}
	}
}

// pr1682ProfileQuery preserves the PR1682_QUERY_PROFILE control: it writes the
// pre-run allocation profile, starts the CPU profile, and returns the cleanup
// that stops the CPU profile and writes the post-run allocation profile.
// Because it writes fixed file names, set it for one benchmark per run.
func pr1682ProfileQuery(tb testing.TB) func() {
	tb.Helper()
	prefix := os.Getenv("PR1682_QUERY_PROFILE")
	if prefix == "" {
		return func() {}
	}
	runtime.GC()
	before, err := os.Create(prefix + "-before.mem")
	if err != nil {
		tb.Fatalf("create allocation profile: %v", err)
	}
	if err := pprof.Lookup("allocs").WriteTo(before, 0); err != nil {
		tb.Fatalf("write allocation profile: %v", err)
	}
	if err := before.Close(); err != nil {
		tb.Fatalf("close allocation profile: %v", err)
	}
	cpu, err := os.Create(prefix + ".cpu")
	if err != nil {
		tb.Fatalf("create cpu profile: %v", err)
	}
	if err := pprof.StartCPUProfile(cpu); err != nil {
		tb.Fatalf("start cpu profile: %v", err)
	}
	return func() {
		// Profiler shutdown and artifact writes are not query work.
		if benchmark, ok := tb.(*testing.B); ok {
			benchmark.StopTimer()
		}
		pprof.StopCPUProfile()
		if err := cpu.Close(); err != nil {
			tb.Error(err)
		}
		after, err := os.Create(prefix + "-after.mem")
		if err != nil {
			tb.Error(err)
			return
		}
		if err := pprof.Lookup("allocs").WriteTo(after, 0); err != nil {
			tb.Error(err)
		}
		if err := after.Close(); err != nil {
			tb.Error(err)
		}
	}
}

// BenchmarkPR1682ScopedCallDistinctAliases varies identifiers so semantic
// validation cannot reuse a literal-normalized verdict from an earlier query.
func BenchmarkPR1682ScopedCallDistinctAliases(b *testing.B) {
	benchmarkPR1682ScopedCallStatements(b, true)
}

func BenchmarkPR1682ScopedCallStatements(b *testing.B) {
	benchmarkPR1682ScopedCallStatements(b, false)
}

func benchmarkPR1682ScopedCallStatements(b *testing.B, distinctAliases bool) {
	root := newTestMemoryEngine(b)
	store := storage.NewNamespacedEngine(root, "bench")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	defer pr1682ProfileQuery(b)()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := pr1682QuerySequence.Add(1)
		var query strings.Builder
		fmt.Fprintf(&query, "MATCH (d:Record {id: %d}) ", id)
		for n := 0; n < 20; n++ {
			fmt.Fprintf(&query, "CALL (d) { OPTIONAL MATCH (n:Reading {id: d.reading_id}) WITH collect(n.id) AS ids RETURN ids AS ids%d", n)
			if distinctAliases {
				fmt.Fprintf(&query, "_%d", id)
			}
			query.WriteString(" } ")
		}
		query.WriteString("RETURN d.id")
		result, err := exec.Execute(ctx, query.String(), nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 0 {
			b.Fatalf("expected no rows, got %d", len(result.Rows))
		}
	}
}

// BenchmarkPR1682ScopedCallOneRow measures the same shape after the outer
// MATCH admits a row, so subquery execution and imports are included.
func BenchmarkPR1682ScopedCallOneRow(b *testing.B) {
	root := newTestMemoryEngine(b)
	store := storage.NewNamespacedEngine(root, "bench")
	_, err := store.CreateNode(b.Context(), &storage.Node{ID: "record", Labels: []string{"Record"}, Properties: map[string]any{"reading_id": "missing"}})
	if err != nil {
		b.Fatal(err)
	}
	exec := NewStorageExecutor(store)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := pr1682QuerySequence.Add(1)
		var query strings.Builder
		fmt.Fprintf(&query, "MATCH (d:Record) WHERE %d = %d ", id, id)
		for n := 0; n < 20; n++ {
			fmt.Fprintf(&query, "CALL (d) { OPTIONAL MATCH (n:Reading {id: d.reading_id}) WITH collect(n.id) AS ids RETURN ids AS ids%d } ", n)
		}
		query.WriteString("RETURN d.reading_id")
		result, err := exec.Execute(context.Background(), query.String(), nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 {
			b.Fatalf("expected one row, got %d", len(result.Rows))
		}
	}
}

// BenchmarkPR1682GeneratedGraphScopedCall uses the shared deterministic local
// graph; no remote database, production fixture, or original R17 dataset is
// involved.
func BenchmarkPR1682GeneratedGraphScopedCall(b *testing.B) {
	benchmarkPR1682GeneratedGraphScopedCall(b, false)
}

// BenchmarkPR1682GeneratedGraphScopedCallIndexed uses the same graph and
// query with a property index built before timing starts.
func BenchmarkPR1682GeneratedGraphScopedCallIndexed(b *testing.B) {
	benchmarkPR1682GeneratedGraphScopedCall(b, true)
}

func benchmarkPR1682GeneratedGraphScopedCall(b *testing.B, indexed bool) {
	graph := newPR1682GeneratedGraph(b, pr1682GeneratedReadings, pr1682GeneratedRecords)
	if indexed {
		if _, err := graph.exec.Execute(context.Background(), "CREATE INDEX reading_id FOR (n:Reading) ON (n.id)", nil); err != nil {
			b.Fatal(err)
		}
	}
	defer pr1682ProfileQuery(b)()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := pr1682QuerySequence.Add(1)
		var query strings.Builder
		fmt.Fprintf(&query, "MATCH (d:Record {id: 7}) WHERE %d = %d ", id, id)
		for n := 0; n < 20; n++ {
			fmt.Fprintf(&query, "CALL (d) { OPTIONAL MATCH (n:Reading {id: d.reading_id}) WITH collect(n.id) AS ids RETURN ids AS ids%d } ", n)
		}
		query.WriteString("RETURN d.reading_id AS id, ids19 AS ids")
		result, err := graph.exec.Execute(context.Background(), query.String(), nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 || len(result.Rows[0]) != 2 || result.Rows[0][0] != "reading-35" || fmt.Sprint(result.Rows[0][1]) != "[reading-35]" {
			b.Fatalf("unexpected result: %#v", result.Rows)
		}
	}
}

// BenchmarkPR1682GeneratedDatetimeScanCold measures the substituted NOR-214
// datetime label scan with distinct statement text. Parser prediction state is
// warm, and literal-normalized semantic verdicts may still be reused.
func BenchmarkPR1682GeneratedDatetimeScanCold(b *testing.B) {
	benchmarkPR1682GeneratedDatetimeScan(b, true)
}

// BenchmarkPR1682GeneratedDatetimeScanWarmed measures the identical statement
// text repeatedly. The first execution is included in the sample; process-wide
// caches may already contain the statement from an earlier sample.
func BenchmarkPR1682GeneratedDatetimeScanWarmed(b *testing.B) {
	benchmarkPR1682GeneratedDatetimeScan(b, false)
}

func benchmarkPR1682GeneratedDatetimeScan(b *testing.B, distinct bool) {
	graph := newPR1682GeneratedGraph(b, pr1682GeneratedReadings, pr1682GeneratedRecords)
	defer pr1682ProfileQuery(b)()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := graph.exec.Execute(context.Background(), pr1682GeneratedDatetimeScanQuery(distinct), nil)
		if err != nil {
			b.Fatal(err)
		}
		pr1682AssertDatetimeScanResult(b, result)
	}
}

// BenchmarkPR1682GeneratedLookupCold measures the substituted NOR-214
// CALL { UNWIND $ids ... MATCH ... } lookup with statement text the executor
// has not seen before. Parser prediction state is warm, and literal-normalized
// semantic verdicts may still be reused.
func BenchmarkPR1682GeneratedLookupCold(b *testing.B) {
	benchmarkPR1682GeneratedLookup(b, true)
}

// BenchmarkPR1682GeneratedLookupWarmed measures the identical statement text
// and parameters repeatedly. The first execution is included in the sample;
// process-wide caches may already contain it from an earlier sample.
func BenchmarkPR1682GeneratedLookupWarmed(b *testing.B) {
	benchmarkPR1682GeneratedLookup(b, false)
}

func benchmarkPR1682GeneratedLookup(b *testing.B, distinct bool) {
	graph := newPR1682GeneratedGraph(b, pr1682GeneratedReadings, pr1682GeneratedRecords)
	ids := pr1682GeneratedLookupIDs()
	params := map[string]interface{}{"ids": ids}
	defer pr1682ProfileQuery(b)()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := graph.exec.Execute(context.Background(), pr1682GeneratedLookupQuery(distinct), params)
		if err != nil {
			b.Fatal(err)
		}
		pr1682AssertLookupResult(b, result, ids)
	}
}

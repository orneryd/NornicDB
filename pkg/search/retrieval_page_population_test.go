package search

// Author: NOR-406 retrieval-profile lane, 2026-11-01: instrumentation only.
// These probes measure where a ranked continuation's first page and its later
// pulls spend work, and which populations each step actually asks the retriever
// for. They change no production behavior: the issued depth, candidate counts
// and exhaustion evidence are read from the SearchOptions and SearchResponse
// the production stream builds, through the documented searchQuery callback.
//
// Workload: the existing 2,000-document ranked-continuation fixture
// (text_continuation_ranked_benchmark_test.go) with an adjustable document
// count. Documents sit on a unit circle in two dimensions, so a cosine
// threshold selects an exact number of eligible documents and the fixture is
// fully deterministic. Embedding is a stub and no reranker/provider is
// configured: no network is involved.
//
// Limits, disclosed: the two-dimensional vectors make each vector comparison
// far cheaper than a production embedding, and the per-document text shares no
// term with the probe query, so the lexical branch contributes no candidate and
// the measured retrieval cost is the vector pipeline with RRF and hydration
// over an empty BM25 pool. This isolates population scaling and step structure,
// not production absolute latency.

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	nor406VectorDims     = 2
	nor406PageSize       = 10
	nor406InitialDepth   = 50
	nor406QueryText      = "retrieval population probe"
	nor406DocumentPrefix = "fixture document"
	nor406AllEligibleMin = -1.0

	nor406ProbeOwner   = "nor406-probe"
	nor406BenchOwner   = "nor406-bench"
	nor406ProfileOwner = "nor406-profile"
)

// nor406BenchSizes is the document-count ladder. NOR406_BENCH_DOCS overrides it
// with a comma-separated list so one built test binary can run a subset.
func nor406BenchSizes() []int {
	if raw := strings.TrimSpace(os.Getenv("NOR406_BENCH_DOCS")); raw != "" {
		var sizes []int
		for _, field := range strings.Split(raw, ",") {
			value, err := strconv.Atoi(strings.TrimSpace(field))
			if err != nil || value <= 0 {
				continue
			}
			sizes = append(sizes, value)
		}
		if len(sizes) > 0 {
			return sizes
		}
	}
	return []int{1000, 2000, 12500, 25000, 49000}
}

// nor406Threshold returns the cosine threshold that exactly `eligible`
// documents of a `docs`-document circle fixture pass: document i sits at angle
// pi*i/docs, so a threshold at angle pi*(eligible-0.5)/docs admits i < eligible.
// Zero or "all documents" eligibility uses the minimal similarity the existing
// benchmark uses for its no-threshold case.
func nor406Threshold(docs, eligible int) float64 {
	if eligible <= 0 || eligible >= docs {
		return nor406AllEligibleMin
	}
	return math.Cos(math.Pi * (float64(eligible) - 0.5) / float64(docs))
}

// nor406Population is one built continuation fixture.
type nor406Population struct {
	service   *Service
	docs      int
	eligible  int
	threshold float64
}

// newNOR406Population builds the circle fixture and forces the HNSW strategy,
// mirroring BenchmarkSearchTextContinuationRankedHNSW. Ingestion happens here,
// outside every measured region.
func newNOR406Population(tb testing.TB, docs, eligible int) *nor406Population {
	tb.Helper()
	tb.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0") // HNSW at any size
	tb.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	engine := storage.NewNamespacedEngine(fixtureowner.Own(tb, fixtureowner.Track(tb, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "nor406")
	service := NewService(engine, nor406VectorDims, nil)
	service.SetRuntimeStrategyTransitionsEnabled(false)
	tb.Cleanup(func() { _ = service.Close() })
	for index := 0; index < docs; index++ {
		angle := math.Pi * float64(index) / float64(docs)
		node := &storage.Node{
			ID:              storage.NodeID(fmt.Sprintf("doc-%06d", index)),
			Labels:          []string{"Document"},
			Properties:      map[string]any{"content": fmt.Sprintf("%s %06d", nor406DocumentPrefix, index)},
			ChunkEmbeddings: [][]float32{{float32(math.Cos(angle)), float32(math.Sin(angle))}},
		}
		if _, err := engine.CreateNode(tb.Context(), node); err != nil {
			tb.Fatal(err)
		}
		if err := service.IndexNode(node); err != nil {
			tb.Fatal(err)
		}
	}
	if _, err := service.getOrCreateHNSWIndex(context.Background(), nor406VectorDims); err != nil {
		tb.Fatal(err)
	}
	for service.hnswRebuildInFlight.Load() {
	}
	return &nor406Population{service: service, docs: docs, eligible: eligible, threshold: nor406Threshold(docs, eligible)}
}

// fixedQueryEmbedding2D is the only embedding source in these probes: no
// provider. The bound package-level value keeps one Embed function for every
// search instead of building a method value per request.
var fixedQueryEmbedding2D = fixtureowner.FixedEmbedding2D{}.Embed

// nor406Step is one actual retrieval the continuation stream issued.
type nor406Step struct {
	depth         int
	rows          int
	vectorCands   int
	vectorRaw     int
	vectorRetries int
	bm25Cands     int
	bm25Raw       int
	bm25Retries   int
	fused         int
	elapsed       time.Duration
	exhausted     bool
	budget        bool
	method        string
}

// nor406Recorder keeps every issued step of one measured operation. The steps
// are recorded with fixtureowner.Append and read with fixtureowner.Snapshot on
// the recorder's own lock; reset drains in place under that same lock.
type nor406Recorder struct {
	mu    sync.Mutex
	steps []nor406Step
}

func (r *nor406Recorder) reset() {
	r.mu.Lock()
	r.steps = r.steps[:0]
	r.mu.Unlock()
}

// searchFunc records the issued depth and candidate evidence of every search
// the continuation stream runs. The options it receives are the step options
// the production stream built, so Limit is the depth actually requested.
func (p *nor406Population) searchFunc(recorder *nor406Recorder) SearchQueryFunc {
	return func(ctx context.Context, query string, embedding []float32, options *SearchOptions) (*SearchResponse, error) {
		depth := 0
		if options != nil {
			depth = options.Limit
		}
		started := time.Now()
		response, err := p.service.Search(ctx, query, embedding, options)
		step := nor406Step{depth: depth, elapsed: time.Since(started)}
		if response != nil {
			step.rows = len(response.Results)
			step.exhausted = response.RetrievalExhausted
			step.budget = response.CandidateBudgetReached
			step.method = response.SearchMethod
			step.fused = response.TotalCandidates
			if metrics := response.Metrics; metrics != nil {
				step.vectorCands = metrics.VectorCandidates
				step.vectorRaw = metrics.VectorRawCandidates
				step.vectorRetries = metrics.VectorOverfetchRetries
				step.bm25Cands = metrics.BM25Candidates
				step.bm25Raw = metrics.BM25RawCandidates
				step.bm25Retries = metrics.BM25OverfetchRetries
			}
		}
		if recorder != nil {
			fixtureowner.Append(&recorder.mu, &recorder.steps, step)
		}
		return response, err
	}
}

// initialOptions is the continuation's own initial depth: the caller's page
// size never lowers it (ownedOptions.Limit = max(Limit, N)).
func (p *nor406Population) initialOptions() *SearchOptions {
	threshold := p.threshold
	return &SearchOptions{Limit: nor406InitialDepth, MinSimilarity: &threshold}
}

func (p *nor406Population) start(ctx context.Context, recorder *nor406Recorder, request SearchContinuationRequest, options *SearchOptions) (*SearchContinuationPage, error) {
	return p.service.SearchTextContinuation(ctx, nor406QueryText, options, request, nil, fixedQueryEmbedding2D, p.searchFunc(recorder), ChunkedSearchErrorPolicy{Transport: "nor406-probe"})
}

func (p *nor406Population) pull(ctx context.Context, recorder *nor406Recorder, owner, qid string, n int) (*SearchContinuationPage, error) {
	request := SearchContinuationRequest{Owner: owner, QID: qid, N: n}
	return p.service.SearchTextContinuation(ctx, "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{Transport: "nor406-probe"})
}

func (p *nor406Population) discard(ctx context.Context, owner, qid string) error {
	request := SearchContinuationRequest{Owner: owner, QID: qid, N: nor406PageSize, Discard: true}
	_, err := p.service.SearchTextContinuation(ctx, "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{Transport: "nor406-probe"})
	return err
}

// eligibleCount measures the eligible population independently of the
// continuation: one search at a depth covering every document.
func (p *nor406Population) eligibleCount(tb testing.TB) int {
	tb.Helper()
	threshold := p.threshold
	response, err := p.service.Search(tb.Context(), nor406QueryText, []float32{1, 0}, &SearchOptions{Limit: p.docs, MinSimilarity: &threshold})
	if err != nil {
		tb.Fatalf("measure eligible population: %v", err)
	}
	return len(response.Results)
}

// TestSearchContinuationFirstPageAwaitsItsIssuedDepthPool characterizes the current
// first-page dependency with a blocked producer: the page cannot be published
// until the single full-depth retrieval the stream issued has finished, and the
// issued depth is the continuation's initial depth (50), not the caller's page
// size (10). No sleeps: the gate proves the search is in flight.
func TestSearchContinuationFirstPageAwaitsItsIssuedDepthPool(t *testing.T) {
	t.Setenv(EnvSearchResultCacheEnabled, "false")
	population := newNOR406Population(t, 1000, 469)
	eligible := population.eligibleCount(t)
	if eligible != population.eligible {
		t.Fatalf("eligible population = %d, want %d (fixture threshold is not exact)", eligible, population.eligible)
	}

	recorder := &nor406Recorder{}
	recordedSearch := population.searchFunc(recorder)
	gate := make(chan struct{})
	entered := make(chan struct{})
	var calls atomic.Int32
	var firstDepth atomic.Int64
	gatedSearch := func(ctx context.Context, query string, embedding []float32, options *SearchOptions) (*SearchResponse, error) {
		if calls.Add(1) == 1 {
			if options != nil {
				firstDepth.Store(int64(options.Limit))
			}
			close(entered)
			select {
			case <-gate:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return recordedSearch(ctx, query, embedding, options)
	}

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	request := SearchContinuationRequest{Owner: nor406ProbeOwner, N: nor406PageSize}
	options := population.initialOptions()
	type startOutcome struct {
		page *SearchContinuationPage
		err  error
	}
	published := make(chan startOutcome, 1)
	go func() {
		page, err := population.service.SearchTextContinuation(ctx, nor406QueryText, options, request, nil, fixedQueryEmbedding2D, gatedSearch, ChunkedSearchErrorPolicy{})
		published <- startOutcome{page: page, err: err}
	}()

	<-entered // the initial search is provably in flight and blocked
	select {
	case outcome := <-published:
		t.Fatalf("first page was published while its issued initial search was still running: page=%+v err=%v", outcome.page, outcome.err)
	default:
	}
	if depth := int(firstDepth.Load()); depth != nor406InitialDepth {
		t.Fatalf("initial issued depth = %d, want %d for page size %d", depth, nor406InitialDepth, nor406PageSize)
	}
	close(gate)

	outcome := <-published
	if outcome.err != nil {
		t.Fatalf("start continuation: %v", outcome.err)
	}
	page := outcome.page
	if page.Returned != nor406PageSize {
		t.Fatalf("first page rows = %d, want %d", page.Returned, nor406PageSize)
	}
	if calls.Load() != 1 {
		t.Fatalf("searches executed before the first page = %d, want 1 (no background producer owns the first page)", calls.Load())
	}
	steps := fixtureowner.Snapshot(&recorder.mu, &recorder.steps)
	if len(steps) != 1 {
		t.Fatalf("recorded steps before the first page = %d, want 1", len(steps))
	}
	t.Logf("first page: rows=%d hasMore=%t total=%v eligiblePopulation=%d initialDepth=%d collectedAtDepth=%d vectorRaw=%d vectorRetries=%d fused=%d exhausted=%t budget=%t method=%s searchMs=%.3f",
		page.Returned, page.HasMore, page.Total, eligible, firstDepth.Load(), steps[0].rows,
		steps[0].vectorRaw, steps[0].vectorRetries, steps[0].fused, steps[0].exhausted, steps[0].budget,
		steps[0].method, float64(steps[0].elapsed.Microseconds())/1000)

	// Drain the rest of the stream, discarding its cursor, and record what the
	// later pulls cost and which depths they issue.
	qid := ""
	if page.HasMore {
		qid = page.QID
	}
	seen := make(map[string]struct{}, eligible)
	recordPage := func(results []SearchResult) {
		for _, result := range results {
			id := searchResultID(result)
			if _, duplicate := seen[id]; duplicate {
				t.Fatalf("duplicate result across pages: %s", id)
			}
			seen[id] = struct{}{}
		}
	}
	recordPage(page.Results)
	totalRows := page.Returned
	drains := 0
	for qid != "" {
		pulled, err := population.pull(ctx, recorder, nor406ProbeOwner, qid, nor406PageSize)
		if err != nil {
			t.Fatalf("pull: %v", err)
		}
		recordPage(pulled.Results)
		totalRows += pulled.Returned
		drains++
		qid = ""
		if pulled.HasMore {
			qid = pulled.QID
		}
	}
	if qid != "" {
		if err := population.discard(ctx, nor406ProbeOwner, qid); err != nil {
			t.Fatalf("discard: %v", err)
		}
	}
	depths := make([]int, 0, len(fixtureowner.Snapshot(&recorder.mu, &recorder.steps)))
	for _, step := range fixtureowner.Snapshot(&recorder.mu, &recorder.steps) {
		depths = append(depths, step.depth)
	}
	t.Logf("drained: pulls=%d rows=%d issuedDepths=%v searches=%d", drains, totalRows, depths, calls.Load())
	if totalRows != eligible {
		t.Fatalf("drained rows = %d, want complete eligible population %d", totalRows, eligible)
	}
}

// TestSearchContinuationExpandsOnlyAfterBufferedPageIsConsumed pins the pull-side
// contract: every full buffered page returns without another search. The next
// pull that needs new rows issues a deeper retrieval.
func TestSearchContinuationExpandsOnlyAfterBufferedPageIsConsumed(t *testing.T) {
	t.Setenv(EnvSearchResultCacheEnabled, "false")
	population := newNOR406Population(t, 1000, 0)
	recorder := &nor406Recorder{}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	request := SearchContinuationRequest{Owner: nor406ProbeOwner, N: nor406PageSize}
	page, err := population.start(ctx, recorder, request, population.initialOptions())
	if err != nil {
		t.Fatalf("start continuation: %v", err)
	}
	steps := fixtureowner.Snapshot(&recorder.mu, &recorder.steps)
	if len(steps) != 1 {
		t.Fatalf("steps after start = %d, want 1", len(steps))
	}
	buffered := steps[0].rows
	if buffered < 2*nor406PageSize {
		t.Fatalf("initial buffered rows = %d, want at least %d", buffered, 2*nor406PageSize)
	}

	qid := ""
	if page.HasMore {
		qid = page.QID
	}
	position := page.Returned
	bufferedPulls := 0
	for qid != "" && position+nor406PageSize <= buffered {
		page, err = population.pull(ctx, recorder, nor406ProbeOwner, qid, nor406PageSize)
		if err != nil {
			t.Fatalf("buffered pull: %v", err)
		}
		if len(fixtureowner.Snapshot(&recorder.mu, &recorder.steps)) != 1 {
			t.Fatalf("a pull inside the %d-row buffer issued a search: %+v", buffered, fixtureowner.Snapshot(&recorder.mu, &recorder.steps))
		}
		position += page.Returned
		bufferedPulls++
		qid = ""
		if page.HasMore {
			qid = page.QID
		}
	}
	if qid == "" {
		t.Fatalf("stream ended inside its first %d-row buffer", buffered)
	}
	if _, err := population.pull(ctx, recorder, nor406ProbeOwner, qid, nor406PageSize); err != nil {
		t.Fatalf("edge pull: %v", err)
	}
	steps = fixtureowner.Snapshot(&recorder.mu, &recorder.steps)
	if len(steps) != 2 {
		t.Fatalf("searches after the edge pull = %d, want 2: %+v", len(steps), steps)
	}
	if steps[1].depth <= steps[0].depth {
		t.Fatalf("edge pull issued depth %d, want deeper than the initial %d", steps[1].depth, steps[0].depth)
	}
	t.Logf("pull lookahead: bufferedPulls=%d initialDepth=%d edgeDepth=%d initialRows=%d edgeRows=%d edgeVectorRaw=%d edgeRetries=%d edgeFused=%d",
		bufferedPulls, steps[0].depth, steps[1].depth, steps[0].rows, steps[1].rows,
		steps[1].vectorRaw, steps[1].vectorRetries, steps[1].fused)
}

// BenchmarkRetrievalContinuationPagePopulation separates the first page from
// the later pulls at growing document populations. Each operation starts one
// ranked continuation at page size 10 and initial depth 50, optionally pulls
// five more pages, and discards every cursor it was given.
func BenchmarkRetrievalContinuationPagePopulation(b *testing.B) {
	for _, docs := range nor406BenchSizes() {
		b.Run(fmt.Sprintf("docs=%d", docs), func(b *testing.B) {
			population := newNOR406Population(b, docs, 0)
			for _, cache := range []bool{false, true} {
				for _, profile := range []struct {
					name     string
					eligible int
				}{
					{"all-eligible", 0},
					{"selective-11", 11},
					{"half-eligible", docs / 2},
				} {
					for _, pulls := range []int{0, 5} {
						name := fmt.Sprintf("cache=%t/%s/pulls=%d", cache, profile.name, pulls)
						b.Run(name, func(b *testing.B) {
							benchmarkNOR406Continuation(b, population, cache, profile.eligible, pulls)
						})
					}
				}
			}
		})
	}
}

func benchmarkNOR406Continuation(b *testing.B, population *nor406Population, cache bool, eligible, pulls int) {
	b.Helper()
	// Cache on/off is a Service construction setting; the probe toggles the
	// same field NewService sets from EnvSearchResultCacheEnabled and restores
	// it, so one fixture serves both cases.
	saved := population.service.resultCache
	if !cache {
		population.service.resultCache = nil
	}
	b.Cleanup(func() { population.service.resultCache = saved })

	threshold := nor406Threshold(population.docs, eligible)
	options := &SearchOptions{Limit: nor406InitialDepth, MinSimilarity: &threshold}
	recorder := &nor406Recorder{}
	request := SearchContinuationRequest{Owner: nor406BenchOwner, N: nor406PageSize}
	ctx := context.Background()

	var firstPage, laterPulls time.Duration
	var rowsTotal, initialDepth, deepestDepth, vectorRaw, vectorRetries, fused, searchesTotal, exhaustedFirst int
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		recorder.reset()
		started := time.Now()
		page, err := population.start(ctx, recorder, request, options)
		firstPage += time.Since(started)
		if err != nil {
			b.Fatal(err)
		}
		rowsTotal += page.Returned
		qid := ""
		if page.HasMore {
			qid = page.QID
		}
		steps := fixtureowner.Snapshot(&recorder.mu, &recorder.steps)
		if len(steps) == 0 {
			b.Fatal("start issued no search")
		}
		initialDepth += steps[0].depth
		vectorRaw += steps[0].vectorRaw
		vectorRetries += steps[0].vectorRetries
		fused += steps[0].fused
		if steps[0].exhausted {
			exhaustedFirst++
		}
		pullStarted := time.Now()
		for pulled := 0; pulled < pulls && qid != ""; pulled++ {
			page, err = population.pull(ctx, recorder, nor406BenchOwner, qid, nor406PageSize)
			if err != nil {
				b.Fatal(err)
			}
			rowsTotal += page.Returned
			qid = ""
			if page.HasMore {
				qid = page.QID
			}
		}
		laterPulls += time.Since(pullStarted)
		if qid != "" {
			if err := population.discard(ctx, nor406BenchOwner, qid); err != nil {
				b.Fatal(err)
			}
		}
		steps = fixtureowner.Snapshot(&recorder.mu, &recorder.steps)
		searchesTotal += len(steps)
		if deepest := nor406DeepestDepth(steps); deepest > deepestDepth {
			deepestDepth = deepest
		}
	}
	b.StopTimer()

	operations := float64(b.N)
	b.ReportMetric(float64(firstPage.Nanoseconds())/operations, "first-page-ns")
	b.ReportMetric(float64(laterPulls.Nanoseconds())/operations, "later-pulls-ns")
	b.ReportMetric(float64(rowsTotal)/operations, "rows/op")
	b.ReportMetric(float64(searchesTotal)/operations, "searches/op")
	b.ReportMetric(float64(initialDepth)/operations, "initial-depth")
	b.ReportMetric(float64(deepestDepth), "deepest-depth")
	b.ReportMetric(float64(vectorRaw)/operations, "vector-raw/op")
	b.ReportMetric(float64(vectorRetries)/operations, "vector-retries/op")
	b.ReportMetric(float64(fused)/operations, "fused/op")
	b.ReportMetric(float64(exhaustedFirst)*100/operations, "%first-page-exhausted")
}

// nor406DeepestDepth returns the deepest depth any recorded step asked for.
func nor406DeepestDepth(steps []nor406Step) int {
	deepest := 0
	for _, step := range steps {
		if step.depth > deepest {
			deepest = step.depth
		}
	}
	return deepest
}

// TestSearchContinuationSearchPhaseCPUProfile writes a CPU profile of the search phase
// only: the fixture is built and warmed before profiling starts, so ingestion,
// startup and build are excluded. It runs only when NOR406_CPU_PROFILE names an
// output path (write it inside this worktree); NOR406_PROFILE_DOCS and
// NOR406_PROFILE_ITERATIONS tune the workload.
func TestSearchContinuationSearchPhaseCPUProfile(t *testing.T) {
	path := strings.TrimSpace(os.Getenv("NOR406_CPU_PROFILE"))
	if path == "" {
		t.Skip("set NOR406_CPU_PROFILE to write a search-phase CPU profile")
	}
	docs := 25000
	if raw := strings.TrimSpace(os.Getenv("NOR406_PROFILE_DOCS")); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil && value > 0 {
			docs = value
		}
	}
	iterations := 100
	if raw := strings.TrimSpace(os.Getenv("NOR406_PROFILE_ITERATIONS")); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil && value > 0 {
			iterations = value
		}
	}

	t.Setenv(EnvSearchResultCacheEnabled, "false")
	population := newNOR406Population(t, docs, 0)
	threshold := population.threshold
	options := &SearchOptions{Limit: nor406InitialDepth, MinSimilarity: &threshold}
	recorder := &nor406Recorder{}
	request := SearchContinuationRequest{Owner: nor406ProfileOwner, N: nor406PageSize}
	ctx := context.Background()

	run := func() {
		page, err := population.start(ctx, recorder, request, options)
		if err != nil {
			t.Fatalf("start continuation: %v", err)
		}
		qid := ""
		if page.HasMore {
			qid = page.QID
		}
		for pulled := 0; pulled < 5 && qid != ""; pulled++ {
			page, err = population.pull(ctx, recorder, nor406ProfileOwner, qid, nor406PageSize)
			if err != nil {
				t.Fatalf("pull: %v", err)
			}
			qid = ""
			if page.HasMore {
				qid = page.QID
			}
		}
		if qid != "" {
			if err := population.discard(ctx, nor406ProfileOwner, qid); err != nil {
				t.Fatalf("discard: %v", err)
			}
		}
	}

	recorder.reset()
	run() // warm lazy structures before profiling
	recorder.reset()

	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create profile: %v", err)
	}
	defer file.Close()
	if err := pprof.StartCPUProfile(file); err != nil {
		t.Fatalf("start cpu profile: %v", err)
	}
	profiled := time.Now()
	for index := 0; index < iterations; index++ {
		run()
	}
	pprof.StopCPUProfile()
	elapsed := time.Since(profiled)
	steps := fixtureowner.Snapshot(&recorder.mu, &recorder.steps)
	depths := make([]int, 0, len(steps))
	for _, step := range steps {
		depths = append(depths, step.depth)
	}
	t.Logf("profiled %d iterations over docs=%d in %s: searches=%d firstSearchMs=%.3f lastSearchMs=%.3f depths=%v",
		iterations, docs, elapsed, len(steps),
		float64(steps[0].elapsed.Microseconds())/1000,
		float64(steps[len(steps)-1].elapsed.Microseconds())/1000, depths)
}

package search

// Author: NOR-406 current-retrieval/cursor audit, reproduction only.
//
// These probes characterize the current release's ranked and complete-ranked
// continuation boundaries on the existing 1,000-document circle fixture
// (retrieval_page_population_test.go) with 469 eligible matches and page size
// 10, plus the registry count/byte ownership the cursor actually enforces.
// They assert current behavior; they change no production behavior and they
// are not a correction. NOR-406's progressive first-page proposal is
// conditional: the ranked mode already publishes its first page from the
// issued depth and long before the eligible population is complete, while the
// complete-ranked modes (the only modes that accept group_by) must finish
// their ranked phase first. Both halves are pinned here so the proposal cannot
// be read as an unconditional defect.
//
// Limits, disclosed: two-dimensional vectors make each comparison far cheaper
// than a production embedding; the fixture's document text shares no term with
// the probe query, so the lexical branch contributes no candidate. Provider
// callbacks are controlled stubs, so the counts and stage timings below are
// engine-side and stub-side work only. They do not reproduce the private
// application's 3.9-6.9 s provider-included retrieval or the unreported 16-way
// screening case.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/stretchr/testify/require"
)

// nor406AuditEligible is the ticket's reproduction shape: 1,000 items with 469
// eventual matches and N=10.
const nor406AuditEligible = 469

// nor406AuditResultIDs returns the canonical row identities of one page.
func nor406AuditResultIDs(results []SearchResult) []string {
	ids := make([]string, len(results))
	for index := range results {
		ids[index] = searchResultID(results[index])
	}
	return ids
}

// nor406AuditDrain pulls the same cursor until the stream reports that it has
// no more rows, returning every page it saw. It records observed sizes so a
// caller can assert the page sequence instead of only the row total.
func nor406AuditDrain(t *testing.T, population *nor406Population, recorder *nor406Recorder, ctx context.Context, start *SearchContinuationPage) []*SearchContinuationPage {
	t.Helper()
	pages := []*SearchContinuationPage{start}
	page := start
	for page.HasMore {
		pulled, err := population.pull(ctx, recorder, nor406ProbeOwner, page.QID, nor406PageSize)
		require.NoError(t, err, "pull after %d pages", len(pages))
		pages = append(pages, pulled)
		page = pulled
	}
	return pages
}

// TestNOR406RankedFirstPageIsGlobalTopNBeforePopulation completes the current
// contract at the ticket's reproduction shape. One ranked start issues a single
// retrieval at the continuation's own initial depth (50), publishes ten rows
// from that pool, and only later pulls deepen the stream toward the 469-item
// eligible population. The published ten are the global top ten of the same
// query at full depth, so the current first page is not a provisional
// "first ten discovered" page and is also not deferred until population ends.
func TestNOR406RankedFirstPageIsGlobalTopNBeforePopulation(t *testing.T) {
	t.Setenv(EnvSearchResultCacheEnabled, "false")
	population := newNOR406Population(t, 1000, nor406AuditEligible)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	require.Equal(t, nor406AuditEligible, population.eligibleCount(t), "fixture threshold is not exact")

	// The independent full-depth reference: one retrieval deep enough to hold
	// the whole eligible population, exactly what a one-shot search returns.
	threshold := population.threshold
	reference, err := population.service.Search(ctx, nor406QueryText, []float32{1, 0}, &SearchOptions{Limit: population.docs, MinSimilarity: &threshold})
	require.NoError(t, err)
	require.Len(t, reference.Results, nor406AuditEligible, "reference population")

	recorder := &nor406Recorder{}
	started := time.Now()
	first, err := population.start(ctx, recorder, SearchContinuationRequest{Owner: nor406ProbeOwner, N: nor406PageSize}, population.initialOptions())
	require.NoError(t, err)
	firstPage := time.Since(started)

	require.Equal(t, nor406PageSize, first.Returned)
	require.True(t, first.HasMore, "a %d-row population cannot be exhausted by the first %d rows", nor406AuditEligible, nor406PageSize)
	require.NotEmpty(t, first.QID)

	steps := fixtureowner.Snapshot(&recorder.mu, &recorder.steps)
	require.Len(t, steps, 1, "the first page must not run extra searches")
	require.Equal(t, nor406InitialDepth, steps[0].depth, "issued depth is the continuation's initial depth, not the caller's page size")
	require.Less(t, steps[0].rows, nor406AuditEligible, "the first page pool is smaller than the eligible population")

	pageIDs := nor406AuditResultIDs(first.Results)
	require.Equal(t, nor406AuditResultIDs(reference.Results[:nor406PageSize]), pageIDs,
		"the ranked first page must be the global top %d, not the first %d discovered", nor406PageSize, nor406PageSize)

	// Consume the stream: later pulls deepen the same snapshot, never repeat a
	// row, and the terminal page reports the complete eligible population.
	pages := nor406AuditDrain(t, population, recorder, ctx, first)
	seen := make(map[string]struct{}, nor406AuditEligible)
	sizes := make([]int, len(pages))
	for index, page := range pages {
		sizes[index] = page.Returned
		for _, id := range nor406AuditResultIDs(page.Results) {
			_, duplicate := seen[id]
			require.False(t, duplicate, "duplicate row %s across %d pages", id, len(pages))
			seen[id] = struct{}{}
		}
	}
	last := pages[len(pages)-1]
	require.False(t, last.HasMore, "terminal page still reports more rows")
	require.Len(t, seen, nor406AuditEligible, "drained a different population than the reference search")
	require.NotNil(t, last.Total, "terminal page must report its total")
	require.Equal(t, uint64(nor406AuditEligible), *last.Total)
	require.Equal(t, SearchContinuationCollectionComplete, last.Completion)
	require.True(t, last.CollectionExhausted)

	steps = fixtureowner.Snapshot(&recorder.mu, &recorder.steps)
	depths := make([]int, len(steps))
	for index := range steps {
		depths[index] = steps[index].depth
	}
	t.Logf("reproduction docs=%d eligible=%d N=%d: firstPageMs=%.3f rowsAtDepth=%d issuedDepths=%v pages=%v searches=%d",
		population.docs, nor406AuditEligible, nor406PageSize, float64(firstPage.Microseconds())/1000,
		steps[0].rows, depths, sizes, len(steps))
}

// TestNOR406CompleteRankedFirstPageWaitsForRankedPhase pins the other half of
// the conditional proposal. ranked_then_id (and, through it, every grouped
// request, because ranked mode refuses group_by) does not publish a first page
// until its ranked phase reaches a terminal state. A blocked second retrieval
// therefore holds the first page, exactly as a blocked initial retrieval holds
// it in ranked mode.
func TestNOR406CompleteRankedFirstPageWaitsForRankedPhase(t *testing.T) {
	t.Setenv(EnvSearchResultCacheEnabled, "false")
	population := newNOR406Population(t, 1000, nor406AuditEligible)

	recorder := &nor406Recorder{}
	recorded := population.searchFunc(recorder)
	var calls atomic.Int32
	gate := make(chan struct{})
	entered := make(chan struct{})
	releaseOnce := sync.Once{}
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	defer release()
	gated := func(ctx context.Context, query string, embedding []float32, options *SearchOptions) (*SearchResponse, error) {
		if calls.Add(1) == 2 {
			close(entered)
			select {
			case <-gate:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return recorded(ctx, query, embedding, options)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	request := SearchContinuationRequest{Owner: nor406ProbeOwner, N: nor406PageSize, Mode: SearchContinuationRankedThenID}
	type outcome struct {
		page *SearchContinuationPage
		err  error
	}
	published := make(chan outcome, 1)
	go func() {
		page, err := population.service.SearchTextContinuation(ctx, nor406QueryText, population.initialOptions(), request, nil, fixedQueryEmbedding2D, gated, ChunkedSearchErrorPolicy{})
		published <- outcome{page: page, err: err}
	}()

	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatalf("the ranked phase never issued its second retrieval: %v", ctx.Err())
	}
	select {
	case result := <-published:
		t.Fatalf("complete-ranked first page was published before its ranked phase finished: page=%+v err=%v", result.page, result.err)
	default:
	}
	release()

	var result outcome
	select {
	case result = <-published:
	case <-ctx.Done():
		t.Fatalf("complete-ranked start did not finish: %v", ctx.Err())
	}
	require.NoError(t, result.err)
	require.Equal(t, nor406PageSize, result.page.Returned)
	require.GreaterOrEqual(t, calls.Load(), int32(2), "the complete-ranked first page must await more than the initial retrieval")

	steps := fixtureowner.Snapshot(&recorder.mu, &recorder.steps)
	depths := make([]int, len(steps))
	for index := range steps {
		depths[index] = steps[index].depth
	}
	t.Logf("complete-ranked barrier docs=%d eligible=%d N=%d: searchesBeforeFirstPage=%d issuedDepths=%v",
		population.docs, nor406AuditEligible, nor406PageSize, len(steps), depths)
	if result.page.HasMore {
		require.NoError(t, population.discard(ctx, nor406ProbeOwner, result.page.QID))
	}
}

// TestNOR406ProviderAndEngineWorkPerStage attributes the canonical ranked
// continuation's work to the two owners that actually execute it: the engine
// retrieval the stream issues, and the controlled provider callbacks
// (chunking, embedding, reranking). It measures them apart, per stage, on the
// 1,000-document/469-match fixture, so a first-page or pull cost can be named
// as engine work or provider work instead of being summed into one number.
// The provider stubs do no network work, so their elapsed time is zero by
// construction: this is the call attribution NOR-406 asks for, not a provider
// latency measurement.
func TestNOR406ProviderAndEngineWorkPerStage(t *testing.T) {
	t.Setenv(EnvSearchResultCacheEnabled, "false")
	population := newNOR406Population(t, 1000, nor406AuditEligible)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	reranker := &windowRecordingReranker{}
	population.service.SetReranker(reranker)
	recorder := &nor406Recorder{}
	recorded := population.searchFunc(recorder)
	var chunkCalls, embedCalls atomic.Int32
	chunkQuery := func(context.Context, string) ([]string, error) {
		chunkCalls.Add(1)
		return []string{nor406QueryText}, nil
	}
	embedQuery := func(context.Context, string) ([]float32, error) {
		embedCalls.Add(1)
		return []float32{1, 0}, nil
	}

	options := population.initialOptions()
	options.RerankEnabled = true
	options.RerankTopK = nor406PageSize
	request := SearchContinuationRequest{Owner: nor406ProbeOwner, N: nor406PageSize}

	searchesFor := func() int { return len(fixtureowner.Snapshot(&recorder.mu, &recorder.steps)) }
	providerCallsFor := func() int { return len(reranker.recorded()) }

	started := time.Now()
	page, err := population.service.SearchTextContinuation(ctx, nor406QueryText, options, request, chunkQuery, embedQuery, recorded, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	firstPage := time.Since(started)
	require.Equal(t, nor406PageSize, page.Returned)
	require.True(t, page.HasMore)

	steps := fixtureowner.Snapshot(&recorder.mu, &recorder.steps)
	require.Len(t, steps, 1, "the first page issues one retrieval")
	initialRows := steps[0].rows
	require.Equal(t, 1, int(chunkCalls.Load()), "one chunking call starts the stream")
	require.Equal(t, 1, int(embedCalls.Load()), "one embedding call starts the stream")
	require.Equal(t, (initialRows+options.RerankTopK-1)/options.RerankTopK, providerCallsFor(),
		"the first page reranks its retrieved rows in bounded provider windows")

	t.Logf("stage=first-page engineSearches=%d issuedDepth=%d rowsAtDepth=%d rows=%d chunkCalls=%d embedCalls=%d providerCalls=%d elapsedMs=%.3f",
		searchesFor(), steps[0].depth, initialRows, page.Returned, chunkCalls.Load(), embedCalls.Load(), providerCallsFor(), float64(firstPage.Microseconds())/1000)

	// Buffered pulls serve replay rows: no retrieval and no provider call.
	for position := page.Returned; position < initialRows; position += nor406PageSize {
		searchesBefore, providerBefore := searchesFor(), providerCallsFor()
		pullStarted := time.Now()
		page, err = population.pull(ctx, recorder, nor406ProbeOwner, page.QID, nor406PageSize)
		pullElapsed := time.Since(pullStarted)
		require.NoError(t, err)
		require.Equal(t, nor406PageSize, page.Returned)
		require.Equal(t, searchesBefore, searchesFor(), "a buffered pull must not retrieve")
		require.Equal(t, providerBefore, providerCallsFor(), "a buffered pull must not call the provider")
		t.Logf("stage=buffered-pull position=%d engineSearches=%d providerCalls=%d elapsedMs=%.3f",
			position, searchesFor(), providerCallsFor(), float64(pullElapsed.Microseconds())/1000)
	}

	// The edge pull must deepen: one retrieval and provider work only for the
	// newly retrieved rows, once per candidate.
	searchesBefore, providerBefore := searchesFor(), providerCallsFor()
	edgeStarted := time.Now()
	page, err = population.pull(ctx, recorder, nor406ProbeOwner, page.QID, nor406PageSize)
	edgeElapsed := time.Since(edgeStarted)
	require.NoError(t, err)
	steps = fixtureowner.Snapshot(&recorder.mu, &recorder.steps)
	require.Equal(t, searchesBefore+1, len(steps), "the edge pull issues exactly one retrieval")
	deepened := steps[len(steps)-1]
	added := deepened.rows - initialRows
	require.Positive(t, added, "the edge pull must retrieve more rows")
	require.Equal(t, providerBefore+(added+options.RerankTopK-1)/options.RerankTopK, providerCallsFor(),
		"only newly retrieved rows reach the provider, in bounded windows")
	require.Equal(t, 1, int(chunkCalls.Load()), "expansions never re-chunk the query")
	require.Equal(t, 1, int(embedCalls.Load()), "expansions never re-embed the query")
	t.Logf("stage=edge-pull engineSearches=%d issuedDepth=%d addedRows=%d providerCalls=%d chunkCalls=%d embedCalls=%d elapsedMs=%.3f",
		len(steps), deepened.depth, added, providerCallsFor(), chunkCalls.Load(), embedCalls.Load(), float64(edgeElapsed.Microseconds())/1000)

	submitted := map[string]int{}
	for _, window := range reranker.recorded() {
		for _, id := range window {
			submitted[id]++
		}
	}
	for id, count := range submitted {
		require.Equal(t, 1, count, "provider input for %s across %d windows", id, providerCallsFor())
	}
	require.Len(t, submitted, initialRows+added, "every retrieved row reached the provider exactly once")
	if page.HasMore {
		require.NoError(t, population.discard(ctx, nor406ProbeOwner, page.QID))
	}
}

// TestNOR406TwoWayIndependentReaderControl is the bounded two-way control the
// audit is allowed to run locally. Two independent owners each start and drain
// their own cursor over the same 1,000-document/469-match fixture and must
// observe the identical, complete, duplicate-free ranked sequence. It reports
// contention only at two concurrent readers: the historical 16-way screening
// workload is deliberately not run here or on the remote qualification host.
func TestNOR406TwoWayIndependentReaderControl(t *testing.T) {
	t.Setenv(EnvSearchResultCacheEnabled, "false")
	population := newNOR406Population(t, 1000, nor406AuditEligible)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	const readers = 2
	type readerOutcome struct {
		ids      []string
		searches int
		elapsed  time.Duration
		err      error
	}
	outcomes := make([]readerOutcome, readers)
	begin := make(chan struct{})
	var group sync.WaitGroup
	for reader := 0; reader < readers; reader++ {
		group.Add(1)
		go func(reader int) {
			defer group.Done()
			recorder := &nor406Recorder{}
			owner := fmt.Sprintf("%s-%d", nor406ProbeOwner, reader)
			<-begin
			started := time.Now()
			page, err := population.start(ctx, recorder, SearchContinuationRequest{Owner: owner, N: nor406PageSize}, population.initialOptions())
			if err != nil {
				outcomes[reader].err = err
				return
			}
			ids := nor406AuditResultIDs(page.Results)
			for page.HasMore {
				next, err := population.pull(ctx, recorder, owner, page.QID, nor406PageSize)
				if err != nil {
					outcomes[reader].err = err
					return
				}
				ids = append(ids, nor406AuditResultIDs(next.Results)...)
				page = next
			}
			outcomes[reader] = readerOutcome{ids: ids, searches: len(fixtureowner.Snapshot(&recorder.mu, &recorder.steps)), elapsed: time.Since(started)}
		}(reader)
	}
	close(begin)
	group.Wait()

	for reader, outcome := range outcomes {
		require.NoError(t, outcome.err, "reader %d", reader)
		require.Len(t, outcome.ids, nor406AuditEligible, "reader %d drained a different population", reader)
		seen := make(map[string]struct{}, len(outcome.ids))
		for _, id := range outcome.ids {
			_, duplicate := seen[id]
			require.False(t, duplicate, "reader %d repeated %s", reader, id)
			seen[id] = struct{}{}
		}
		require.Equal(t, outcomes[0].searches, outcome.searches, "reader %d issued a different search count", reader)
		t.Logf("reader=%d rows=%d searches=%d elapsedMs=%.3f", reader, len(outcome.ids), outcome.searches, float64(outcome.elapsed.Microseconds())/1000)
	}
	require.Equal(t, outcomes[0].ids, outcomes[1].ids, "two concurrent readers must observe the same stable ranked sequence")
}

// BenchmarkNOR406SameCursorCost separates three pulls a caller can make
// against one cursor: a replay of a position whose rows are already buffered,
// an advance into the buffered prefix, and the pull that must deepen the
// stream. The first two are pure cursor cost (no retrieval), so a caller's
// repeated page cost is reported apart from population work. NOR406_BENCH_DOCS
// selects the document ladder; setup and index construction are outside the
// measured region and the cursor is discarded when the operation ends.
func BenchmarkNOR406SameCursorCost(b *testing.B) {
	for _, docs := range nor406BenchSizes() {
		b.Run(fmt.Sprintf("docs=%d", docs), func(b *testing.B) {
			population := newNOR406Population(b, docs, 0)
			options := population.initialOptions()
			recorder := &nor406Recorder{}
			request := SearchContinuationRequest{Owner: nor406BenchOwner, N: nor406PageSize}
			ctx := context.Background()

			var replayed, advanced, expanded time.Duration
			var searches, pulls, rows int
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				recorder.reset()
				first, err := population.start(ctx, recorder, request, options)
				if err != nil {
					b.Fatal(err)
				}
				started := fixtureowner.Snapshot(&recorder.mu, &recorder.steps)
				buffer := started[0].rows

				pullStarted := time.Now()
				advance, err := population.pull(ctx, recorder, nor406BenchOwner, first.QID, nor406PageSize)
				advanced += time.Since(pullStarted)
				if err != nil {
					b.Fatal(err)
				}
				if advance.Returned != first.Returned {
					b.Fatalf("advance returned %d rows for a %d-row page", advance.Returned, first.Returned)
				}
				rows += advance.Returned

				pullStarted = time.Now()
				page, err := population.pull(ctx, recorder, nor406BenchOwner, first.QID, nor406PageSize)
				replayed += time.Since(pullStarted)
				if err != nil {
					b.Fatal(err)
				}
				require.Equal(b, nor406AuditResultIDs(advance.Results), nor406AuditResultIDs(page.Results), "repeating the same token must replay the same page")
				require.Len(b, fixtureowner.Snapshot(&recorder.mu, &recorder.steps), 1, "buffered advance and replay must not retrieve")
				rows += page.Returned

				for position := 2 * nor406PageSize; position < buffer && page.HasMore; position += nor406PageSize {
					page, err = population.pull(ctx, recorder, nor406BenchOwner, page.QID, nor406PageSize)
					if err != nil {
						b.Fatal(err)
					}
					rows += page.Returned
					pulls++
				}

				pullStarted = time.Now()
				page, err = population.pull(ctx, recorder, nor406BenchOwner, page.QID, nor406PageSize)
				expanded += time.Since(pullStarted)
				if err != nil {
					b.Fatal(err)
				}
				rows += page.Returned
				if page.HasMore {
					if err := population.discard(ctx, nor406BenchOwner, page.QID); err != nil {
						b.Fatal(err)
					}
				}
				searches += len(fixtureowner.Snapshot(&recorder.mu, &recorder.steps))
			}
			b.StopTimer()

			operations := float64(b.N)
			b.ReportMetric(float64(replayed.Nanoseconds())/operations, "replay-ns")
			b.ReportMetric(float64(advanced.Nanoseconds())/operations, "advanced-ns")
			b.ReportMetric(float64(expanded.Nanoseconds())/operations, "deepen-ns")
			b.ReportMetric(float64(searches)/operations, "searches/op")
			b.ReportMetric(float64(pulls)/operations, "walked-pulls/op")
			b.ReportMetric(float64(rows)/operations, "rows/op")
		})
	}
}

// BenchmarkNOR406TwoWayContinuationControl is the bounded two-reader remote
// control: two goroutines each start their own ranked cursor and consume five
// pages of it over the shared fixture, then discard it. b.N operations are
// split between the two readers, so ns/op is one drain under two-way
// contention. Sixteen-way screening is deliberately not part of this control.
func BenchmarkNOR406TwoWayContinuationControl(b *testing.B) {
	for _, docs := range nor406BenchSizes() {
		b.Run(fmt.Sprintf("docs=%d", docs), func(b *testing.B) {
			population := newNOR406Population(b, docs, 0)
			options := population.initialOptions()
			var rowsTotal, searchesTotal atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			var group sync.WaitGroup
			for reader := 0; reader < 2; reader++ {
				group.Add(1)
				go func(reader int) {
					defer group.Done()
					owner := fmt.Sprintf("%s-%d", nor406BenchOwner, reader)
					iterations := b.N / 2
					if reader < b.N%2 {
						iterations++
					}
					for index := 0; index < iterations; index++ {
						recorder := &nor406Recorder{}
						page, err := population.start(context.Background(), recorder, SearchContinuationRequest{Owner: owner, N: nor406PageSize}, options)
						if err != nil {
							b.Error(err)
							return
						}
						rows := page.Returned
						for pulled := 0; pulled < 5 && page.HasMore; pulled++ {
							next, err := population.pull(context.Background(), recorder, owner, page.QID, nor406PageSize)
							if err != nil {
								b.Error(err)
								return
							}
							rows += next.Returned
							page = next
						}
						if page.HasMore {
							if err := population.discard(context.Background(), owner, page.QID); err != nil {
								b.Error(err)
								return
							}
						}
						rowsTotal.Add(int64(rows))
						searchesTotal.Add(int64(len(fixtureowner.Snapshot(&recorder.mu, &recorder.steps))))
					}
				}(reader)
			}
			group.Wait()
			b.StopTimer()
			operations := float64(b.N)
			b.ReportMetric(float64(rowsTotal.Load())/operations, "rows/op")
			b.ReportMetric(float64(searchesTotal.Load())/operations, "searches/op")
		})
	}
}

// TestNOR406RankedCursorCapacityIsStreamCountAndRetainedBytes verifies the
// capacity owner the cursor actually has today. Admission counts both live
// streams and retained row bytes per owner; a rejected start reserves nothing,
// a released stream admits a new one, and retained-byte growth during a pull
// fails the pull instead of retaining unbounded memory.
func TestNOR406RankedCursorCapacityIsStreamCountAndRetainedBytes(t *testing.T) {
	t.Setenv(EnvSearchResultCacheEnabled, "false")

	t.Run("stream count", func(t *testing.T) {
		population := newNOR406Population(t, 200, 0)
		registry, err := resultstream.NewRegistry(resultstream.Config{
			MaxStreams: 1, MaxStreamsPerOwner: 1, TTL: time.Minute,
		})
		require.NoError(t, err)
		t.Cleanup(registry.Close)
		population.service.SetContinuationRegistry(registry)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		first, err := population.start(ctx, nil, SearchContinuationRequest{Owner: nor406ProbeOwner, N: nor406PageSize}, population.initialOptions())
		require.NoError(t, err)
		require.True(t, first.HasMore)

		_, err = population.start(ctx, nil, SearchContinuationRequest{Owner: "nor406-other", N: nor406PageSize}, population.initialOptions())
		require.ErrorIs(t, err, resultstream.ErrCapacity, "the process-wide stream count is a real cap")

		require.NoError(t, population.discard(ctx, nor406ProbeOwner, first.QID))
		replacement, err := population.start(ctx, nil, SearchContinuationRequest{Owner: "nor406-other", N: nor406PageSize}, population.initialOptions())
		require.NoError(t, err, "discard must release the admission it held")
		require.NoError(t, population.discard(ctx, "nor406-other", replacement.QID))
	})

	t.Run("retained bytes", func(t *testing.T) {
		population := newNOR406Population(t, 200, 0)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		// Measure what one published ranked cursor retains through the
		// registry's own accounting, then rebuild the registry with exactly
		// that budget so admission succeeds and any growth must be refused.
		probe := &nor406CursorUsage{}
		probing, err := resultstream.NewRegistry(resultstream.Config{TTL: time.Minute})
		require.NoError(t, err)
		t.Cleanup(probing.Close)
		population.service.SetContinuationRegistry(probing)
		probing.SetObserver(probe)
		measured, err := population.start(ctx, nil, SearchContinuationRequest{Owner: nor406ProbeOwner, N: nor406PageSize}, population.initialOptions())
		require.NoError(t, err)
		_, retained := probe.snapshot()
		require.Positive(t, retained, "a published ranked cursor must retain row bytes")
		require.NoError(t, population.discard(ctx, nor406ProbeOwner, measured.QID))

		observing := &nor406CursorUsage{}
		bounded, err := resultstream.NewRegistry(resultstream.Config{TTL: time.Minute, MaxRetainedBytesPerOwner: retained})
		require.NoError(t, err)
		t.Cleanup(bounded.Close)
		population.service.SetContinuationRegistry(bounded)
		bounded.SetObserver(observing)

		page, err := population.start(ctx, nil, SearchContinuationRequest{Owner: nor406ProbeOwner, N: nor406PageSize}, population.initialOptions())
		require.NoError(t, err, "a cursor that fits the byte budget must be admitted")
		active, admitted := observing.snapshot()
		require.Equal(t, int64(1), active)
		require.Equal(t, retained, admitted, "the bounded registry must charge exactly the measured retained bytes")

		// Walk to the edge of the buffered prefix: the next pull has to deepen
		// and grow retained rows past the owner budget.
		for position := page.Returned; position < nor406InitialDepth; position += nor406PageSize {
			page, err = population.pull(ctx, nil, nor406ProbeOwner, page.QID, nor406PageSize)
			require.NoError(t, err)
		}
		_, err = population.pull(ctx, nil, nor406ProbeOwner, page.QID, nor406PageSize)
		require.ErrorIs(t, err, resultstream.ErrCapacity, "retained-byte growth must fail the pull")
		_, retainedAfterRefusal := observing.snapshot()
		require.Zero(t, retainedAfterRefusal, "a refused growth must release the retained bytes")
		_, err = population.pull(ctx, nil, nor406ProbeOwner, page.QID, nor406PageSize)
		require.ErrorIs(t, err, resultstream.ErrGoneQID, "the capacity-failed cursor must not stay registered")
	})
}

// nor406CursorUsage records the registry's own live-cursor accounting.
type nor406CursorUsage struct {
	mu            sync.Mutex
	events        []string
	active        int64
	retainedBytes int64
}

func (u *nor406CursorUsage) CursorEvent(outcome string) {
	u.mu.Lock()
	u.events = append(u.events, outcome)
	u.mu.Unlock()
}

func (u *nor406CursorUsage) CursorUsage(active, retainedBytes int64) {
	u.mu.Lock()
	u.active, u.retainedBytes = active, retainedBytes
	u.mu.Unlock()
}

func (u *nor406CursorUsage) snapshot() (int64, int64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.active, u.retainedBytes
}

// TestNOR406RankedCursorCancelRetryAndShutdown covers the lifetime boundaries
// the ticket requires for a live cursor: a cancelled expansion leaves the
// issued stream replayable, a retry with a fresh context resumes it without
// skipping or repeating rows, concurrent pulls at one position coalesce into a
// single expansion, and shutdown releases the retained rows.
func TestNOR406RankedCursorCancelRetryAndShutdown(t *testing.T) {
	t.Setenv(EnvSearchResultCacheEnabled, "false")

	t.Run("cancel and retry", func(t *testing.T) {
		population := newNOR406Population(t, 1000, 0)
		usage := &nor406CursorUsage{}
		registry, err := resultstream.NewRegistry(resultstream.Config{TTL: time.Minute})
		require.NoError(t, err)
		t.Cleanup(registry.Close)
		registry.SetObserver(usage)
		population.service.SetContinuationRegistry(registry)

		recorder := &nor406Recorder{}
		recorded := population.searchFunc(recorder)
		gate := make(chan struct{})
		entered := make(chan struct{})
		releaseOnce := sync.Once{}
		release := func() { releaseOnce.Do(func() { close(gate) }) }
		defer release()
		var calls atomic.Int32
		gated := func(ctx context.Context, query string, embedding []float32, options *SearchOptions) (*SearchResponse, error) {
			if calls.Add(1) == 2 {
				close(entered)
				select {
				case <-gate:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return recorded(ctx, query, embedding, options)
		}

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		request := SearchContinuationRequest{Owner: nor406ProbeOwner, N: nor406PageSize}
		page, err := population.service.SearchTextContinuation(ctx, nor406QueryText, population.initialOptions(), request, nil, fixedQueryEmbedding2D, gated, ChunkedSearchErrorPolicy{})
		require.NoError(t, err)
		seen := nor406AuditResultIDs(page.Results)
		for position := page.Returned; position < nor406InitialDepth; position += nor406PageSize {
			page, err = population.pull(ctx, recorder, nor406ProbeOwner, page.QID, nor406PageSize)
			require.NoError(t, err)
			seen = append(seen, nor406AuditResultIDs(page.Results)...)
		}
		require.Equal(t, int32(1), calls.Load(), "the last full buffered page must not expand")
		require.True(t, page.HasMore)
		edge := page.QID

		cancelCtx, cancelPull := context.WithCancel(ctx)
		pullResult := make(chan error, 1)
		go func() {
			_, pullErr := population.pull(cancelCtx, recorder, nor406ProbeOwner, edge, nor406PageSize)
			pullResult <- pullErr
		}()
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatalf("the expansion never started: %v", ctx.Err())
		}
		cancelPull()
		select {
		case pullErr := <-pullResult:
			require.ErrorIs(t, pullErr, context.Canceled, "a caller cancellation must surface as cancellation")
		case <-ctx.Done():
			t.Fatalf("the cancelled pull did not return: %v", ctx.Err())
		}
		active, retained := usage.snapshot()
		require.Equal(t, int64(1), active, "a cancelled pull must not release the issued stream")
		require.Positive(t, retained)

		release()
		retried, err := population.pull(ctx, recorder, nor406ProbeOwner, edge, nor406PageSize)
		require.NoError(t, err, "a fresh context must resume the issued stream")
		require.Equal(t, nor406PageSize, retried.Returned)
		// Initial retrieval, the cancelled attempt, and this retry: the retry
		// performs exactly one more retrieval and reuses no partial result.
		require.Equal(t, int32(3), calls.Load(), "one retry performs exactly one more retrieval")
		for _, id := range nor406AuditResultIDs(retried.Results) {
			require.NotContains(t, seen, id, "retry after cancellation repeated row %s", id)
		}

		require.NoError(t, population.discard(ctx, nor406ProbeOwner, retried.QID))
		active, retained = usage.snapshot()
		require.Zero(t, active, "discard must release the live cursor")
		require.Zero(t, retained, "discard must release the retained rows")
	})

	t.Run("concurrent same position", func(t *testing.T) {
		population := newNOR406Population(t, 1000, 0)
		recorder := &nor406Recorder{}
		recorded := population.searchFunc(recorder)
		gate := make(chan struct{})
		entered := make(chan struct{})
		releaseOnce := sync.Once{}
		release := func() { releaseOnce.Do(func() { close(gate) }) }
		defer release()
		var calls atomic.Int32
		gated := func(ctx context.Context, query string, embedding []float32, options *SearchOptions) (*SearchResponse, error) {
			if calls.Add(1) == 2 {
				close(entered)
				select {
				case <-gate:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return recorded(ctx, query, embedding, options)
		}

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		page, err := population.service.SearchTextContinuation(ctx, nor406QueryText, population.initialOptions(), SearchContinuationRequest{Owner: nor406ProbeOwner, N: nor406PageSize}, nil, fixedQueryEmbedding2D, gated, ChunkedSearchErrorPolicy{})
		require.NoError(t, err)
		for position := page.Returned; position < nor406InitialDepth; position += nor406PageSize {
			page, err = population.pull(ctx, recorder, nor406ProbeOwner, page.QID, nor406PageSize)
			require.NoError(t, err)
		}
		token := page.QID

		type outcome struct {
			page *SearchContinuationPage
			err  error
		}
		results := make(chan outcome, 2)
		for worker := 0; worker < 2; worker++ {
			go func() {
				pulled, pullErr := population.pull(ctx, recorder, nor406ProbeOwner, token, nor406PageSize)
				results <- outcome{page: pulled, err: pullErr}
			}()
		}
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatalf("the coalesced expansion never started: %v", ctx.Err())
		}
		release()
		first := <-results
		second := <-results
		require.NoError(t, first.err)
		require.NoError(t, second.err)
		require.Equal(t, nor406AuditResultIDs(first.page.Results), nor406AuditResultIDs(second.page.Results),
			"two pulls at one position must return the same page")
		require.Equal(t, int32(2), calls.Load(), "concurrent pulls at one position must share one expansion")
		if first.page.HasMore {
			require.NoError(t, population.discard(ctx, nor406ProbeOwner, first.page.QID))
		}
	})

	t.Run("shutdown releases retained rows", func(t *testing.T) {
		population := newNOR406Population(t, 200, 0)
		usage := &nor406CursorUsage{}
		registry, err := resultstream.NewRegistry(resultstream.Config{TTL: time.Minute})
		require.NoError(t, err)
		registry.SetObserver(usage)
		population.service.SetContinuationRegistry(registry)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		page, err := population.start(ctx, nil, SearchContinuationRequest{Owner: nor406ProbeOwner, N: nor406PageSize}, population.initialOptions())
		require.NoError(t, err)
		require.True(t, page.HasMore)
		active, retained := usage.snapshot()
		require.Equal(t, int64(1), active)
		require.Positive(t, retained)

		registry.Close()
		active, retained = usage.snapshot()
		require.Zero(t, active, "shutdown must release every live cursor")
		require.Zero(t, retained, "shutdown must release retained rows")
		_, err = population.pull(ctx, nil, nor406ProbeOwner, page.QID, nor406PageSize)
		require.ErrorIs(t, err, resultstream.ErrClosed, "a shut-down registry must reject pulls")

		// A service that owns its registry closes it on shutdown, so its
		// issued cursors cannot continue either.
		owned := newNOR406Population(t, 200, 0)
		ownedPage, err := owned.start(ctx, nil, SearchContinuationRequest{Owner: nor406ProbeOwner, N: nor406PageSize}, owned.initialOptions())
		require.NoError(t, err)
		require.NoError(t, owned.service.Close())
		_, err = owned.pull(ctx, nil, nor406ProbeOwner, ownedPage.QID, nor406PageSize)
		require.True(t,
			errors.Is(err, resultstream.ErrClosed) || errors.Is(err, resultstream.ErrGoneQID) || errors.Is(err, resultstream.ErrInvalidQID),
			"a closed service must reject its issued cursors, got %v", err)
	})
}

package cypher

// Author: N152-241 provider-inclusive retrieval qualification, 2026-11-22.
//
// The release screening benchmark (retrieval_screening_bench_test.go) measures
// db.retrieve and db.rretrieve over the in-repo 49K corpus with a request-supplied
// query vector and a controlled in-process reranker. That leaves the embedding
// client, the rerank client and their wire paths outside the measurement. This
// file is the qualification variant: the same corpus, the same correctness
// oracles and the same public procedures, but the query embedding and the
// reranking are produced by the real Voyage clients (pkg/embed's VoyageEmbedder
// and pkg/voyage's Reranker) talking HTTP to a deterministic loopback server.
//
// No external service, credential or corpus file is used or added: the workload
// stays in the repository and on the loopback interface. The API key below is a
// literal label sent to httptest's 127.0.0.1 listener; it is not a credential
// and grants nothing.
//
// Workload: exactly the NOR-406 49,000-source fixture, built by
// newNOR406RetrievalFixtureOnEngine from this package's own synthetic graph
// (356 source groups, 4 collections, 1024-dimensional deterministic source
// vectors, about 1 KB of content per source). The retrieval "where" predicate
// is the same nor406ExistsPredicate, so the previous release controls stay
// meaningful. Index construction and warmup are outside every measured
// operation, so this does not benchmark HNSW fixture construction.
//
// The loopback provider is deterministic, so both measured and control runs are
// reproducible:
//
//   - POST /v1/embeddings answers every input with the fixture's controlled
//     query vector (dimension 0 = 1, everything else 0). The corpus vectors
//     already live in the repo, so the server only has to reproduce the query
//     vector those vectors were built against. The request it receives is
//     captured and checked (model, input_type=query, output_dimension=1024), so
//     the embedding client's wire format is part of the qualification.
//   - POST /v1/rerank scores each submitted document from the "Source record N."
//     marker the fixture writes into every source's content, as 1 - N/10. The
//     score is injective and strictly decreasing in N, so the provider's order
//     is ascending source index with no ties, and every published row can be
//     checked against its own identity.
//
// Measured phases, per operation and per client count, reported separately:
// query embedding (embedding client round trip), engine initial (the first
// Execute minus the provider phases it contains), rerank provider (rerank
// client round trip), and pulls (each continuation page's Execute). p50/p95/p99
// are nearest-rank percentiles over the samples of one case; -benchtime=NNx
// selects the sample count and is the intended way to run this file.
//
// Correctness controls kept by this file (all on the same 49K fixture, both
// filtered and unfiltered):
//
//   - exact row identities and ranking, against independent rerank-disabled
//     control retrievals through the public procedure. The two shapes submit
//     different windows by contract, so each has its own control: a one-shot
//     retrieval submits the 100-candidate TopK window of the 50-row request, and
//     a ranked continuation submits the 50 rows it first admitted. The controls
//     reproduce each window because the request's candidate budget
//     (candidateTarget) is a public option: {limit: 100, candidateTarget: 100}
//     reproduces the one-shot window, {limit: 50} the continuation window. Both
//     controls are run before the measured shape, and the published rows are
//     compared with the deterministic provider order over that window, id by id.
//   - complete publication: five pages of ten rows, each row exactly once,
//     truthful returned/has_more accounting, and no row published twice.
//   - provider calls: exactly one embedding call and, for db.rretrieve, exactly
//     one rerank call per operation; the rerank request itself is checked
//     (query, model, document count).
//   - cancel: a pre-canceled request and a request canceled while the embedding
//     or rerank provider call is in flight.
//   - provider errors and recovery: embedding and rerank failures, fail-open and
//     fail-closed, each followed by the exact expected order once the failure is
//     cleared.
//
// The controls are computed in the same process and build as the publication
// they check, so the oracle proves the exact identities and ranking of this
// fixture's current fusion; it does not assert cross-process rank identity,
// which index construction may vary.
//
// Limits, disclosed: one machine, in-process calls to an httptest listener on
// the loopback interface (no TLS, no proxy, no remote latency, no provider
// model); one corpus size; the loopback handlers do no inference, so this
// qualifies the client/wire/engine integration and its tail latency, not a
// vendor's service latency. The benchmark measures the filtered (exists)
// request, the shape the release screening benchmark qualifies; that predicate's
// own engine work dominates the provider phases on this corpus, and the
// unfiltered engine baseline belongs to the screening benchmark, not here.
// HNSW search is approximate and the fused order depends on the request's
// candidate budget, so the benchmark's per-operation checks are identity,
// eligibility, ordering and completeness invariants that hold for any window
// the index returns, while the exact-window oracle is asserted by the
// sequential correctness test. The executor result cache and the search
// response cache are off, so every operation performs the retrieval again
// instead of replaying a cached page.
//
// Run the measurement (one finite sample set per case; 24 samples per case):
//
//	go test ./pkg/cypher -run '^$' -v \
//		-bench '^BenchmarkSearchSourceRetrievalProviderQualification$' -benchtime=24x -timeout 60m
//
// Run the correctness controls (about two minutes: one 49K fixture, then only
// provider round trips):
//
//	go test ./pkg/cypher -run '^TestSearchSourceRetrievalProviderQualification49K$' -v -count=1

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	providerembed "github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/voyage"
)

const (
	// nor241Population is the one corpus size this qualification covers: the
	// 49K level of the NOR-406 ladder, reused rather than rebuilt.
	nor241Population = 49000
	// nor241Limit is the terminal row count of one retrieval.
	nor241Limit = nor406Limit
	// nor241PageSize and nor241Pages are five pages of one ranked continuation.
	nor241PageSize = nor406PageSize
	nor241Pages    = nor406Pages
	// nor241RerankWindow is search's default Stage-2 TopK: the provider window
	// of one one-shot retrieval.
	nor241RerankWindow = 100
	// nor241CandidateTarget is the candidate budget the 50-row request's own
	// fusion uses (twice the limit). The one-shot window control states it
	// explicitly so its 100-row read reproduces that fusion instead of the
	// 100-row request's deeper one.
	nor241CandidateTarget = 2 * nor241Limit
	// nor241ProviderTimeout bounds one loopback provider call. The handler is
	// local, so exceeding it is a failure, not a slow provider.
	nor241ProviderTimeout = 5 * time.Second
	// nor241APIKey is a literal label for the loopback listener, not a
	// credential: it is sent only to 127.0.0.1 and grants nothing.
	nor241APIKey         = "nor241-loopback-qualification"
	nor241EmbeddingModel = "voyage-4-large"
	nor241RerankModel    = "rerank-2.5"
	// nor241ScoreEpsilon is the tolerance of a published provider score. The
	// score is 1 - N/10, which JSON round-trips exactly; the tolerance only
	// absorbs float64 formatting.
	nor241ScoreEpsilon = 1e-9
	// nor241SourceMarker is the fixture's per-source marker in every content
	// value (nor406SourceContent).
	nor241SourceMarker = "Source record "
)

// nor241EmbeddingRequestWire is the embedding request the Voyage client sends.
type nor241EmbeddingRequestWire struct {
	Input           []string `json:"input"`
	Model           string   `json:"model"`
	InputType       string   `json:"input_type"`
	OutputDimension int      `json:"output_dimension"`
	OutputDType     string   `json:"output_dtype"`
}

// nor241EmbeddingItemWire and nor241EmbeddingResponseWire are the normalized
// Voyage embeddings response shape the client accepts.
type nor241EmbeddingItemWire struct {
	Embedding []float32 `json:"embedding"`
	Index     int       `json:"index"`
}

type nor241EmbeddingResponseWire struct {
	Object string                    `json:"object"`
	Data   []nor241EmbeddingItemWire `json:"data"`
	Model  string                    `json:"model"`
	Usage  nor241UsageWire           `json:"usage"`
}

type nor241UsageWire struct {
	TotalTokens int `json:"total_tokens"`
}

// nor241RerankRequestWire is the rerank request the Voyage client sends.
type nor241RerankRequestWire struct {
	Query           string   `json:"query"`
	Documents       []string `json:"documents"`
	Model           string   `json:"model"`
	TopK            int      `json:"top_k"`
	ReturnDocuments bool     `json:"return_documents"`
	Truncation      *bool    `json:"truncation"`
}

type nor241RerankItemWire struct {
	Index          int     `json:"index"`
	RelevanceScore float64 `json:"relevance_score"`
}

type nor241RerankResponseWire struct {
	Data  []nor241RerankItemWire `json:"data"`
	Model string                 `json:"model"`
	Usage nor241UsageWire        `json:"usage"`
}

// nor241ProviderScore is the deterministic provider score of a source: strictly
// decreasing in the source index, so the provider's ranking is ascending source
// index with no ties and every published row can be checked against its own id.
func nor241ProviderScore(sourceIndex int) float64 {
	return 1 - float64(sourceIndex)/10
}

// nor241SourceIndexOf returns the fixture's source index for a published row id.
func nor241SourceIndexOf(id string) (int, error) {
	if !strings.HasPrefix(id, nor406SourceIDPrefix) {
		return 0, fmt.Errorf("row id %q is not a %s id", id, nor406SourceIDPrefix)
	}
	index, err := strconv.Atoi(strings.TrimPrefix(id, nor406SourceIDPrefix))
	if err != nil {
		return 0, fmt.Errorf("row id %q: %w", id, err)
	}
	return index, nil
}

// nor241ProviderScoreOfID is the provider score every published row of an id
// must carry.
func nor241ProviderScoreOfID(id string) (float64, error) {
	index, err := nor241SourceIndexOf(id)
	if err != nil {
		return 0, err
	}
	return nor241ProviderScore(index), nil
}

// nor241DocumentScore reads the source index out of one submitted rerank
// document. A document without the fixture's marker is a provider input the
// qualification cannot account for, so it fails the request instead of being
// ranked on a guess.
func nor241DocumentScore(document string) (float64, error) {
	position := strings.LastIndex(document, nor241SourceMarker)
	if position < 0 {
		return 0, fmt.Errorf("rerank document carries no %q marker", nor241SourceMarker)
	}
	rest := document[position+len(nor241SourceMarker):]
	end := strings.IndexByte(rest, '.')
	if end < 0 {
		return 0, fmt.Errorf("rerank document marker %q is not terminated", nor241SourceMarker)
	}
	index, err := strconv.Atoi(strings.TrimSpace(rest[:end]))
	if err != nil {
		return 0, fmt.Errorf("rerank document marker %q: %w", rest, err)
	}
	return nor241ProviderScore(index), nil
}

// nor241ProviderServer is the deterministic loopback embedding and rerank
// provider. Besides answering, it can fail a call (retry-free 400) or hold a
// call open until the caller cancels, and it records what the real clients sent.
type nor241ProviderServer struct {
	server *httptest.Server

	embeddingCalls atomic.Int64
	rerankCalls    atomic.Int64

	failEmbedding atomic.Bool
	failRerank    atomic.Bool
	holdEmbedding atomic.Bool
	holdRerank    atomic.Bool

	embeddingArrived chan struct{}
	rerankArrived    chan struct{}

	mu            sync.Mutex
	lastEmbedding nor241EmbeddingRequestWire
	lastRerank    nor241RerankRequestWire
}

func newNOR241ProviderServer(tb testing.TB) *nor241ProviderServer {
	tb.Helper()
	provider := &nor241ProviderServer{
		embeddingArrived: make(chan struct{}, 64),
		rerankArrived:    make(chan struct{}, 64),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/embeddings", provider.serveEmbeddings)
	mux.HandleFunc("/v1/rerank", provider.serveRerank)
	provider.server = httptest.NewServer(mux)
	tb.Cleanup(provider.server.Close)
	return provider
}

func (p *nor241ProviderServer) serveEmbeddings(response http.ResponseWriter, request *http.Request) {
	p.embeddingCalls.Add(1)
	var wire nor241EmbeddingRequestWire
	if err := json.NewDecoder(request.Body).Decode(&wire); err != nil {
		nor241ProviderError(response, "undecodable embedding request: "+err.Error())
		return
	}
	p.mu.Lock()
	p.lastEmbedding = wire
	p.mu.Unlock()
	nor241Signal(p.embeddingArrived)
	if p.holdEmbedding.Load() {
		// Stay in flight until the caller cancels, so the cancellation is
		// observed by the provider that is doing the work.
		<-request.Context().Done()
		return
	}
	if p.failEmbedding.Load() {
		nor241ProviderError(response, "nor241 injected embedding failure")
		return
	}
	answer := nor241EmbeddingResponseWire{
		Object: "list",
		Model:  wire.Model,
		Data:   make([]nor241EmbeddingItemWire, len(wire.Input)),
	}
	for index := range wire.Input {
		answer.Data[index] = nor241EmbeddingItemWire{Embedding: nor241QueryVector(), Index: index}
	}
	answer.Usage.TotalTokens = len(wire.Input)
	nor241ProviderJSON(response, answer)
}

func (p *nor241ProviderServer) serveRerank(response http.ResponseWriter, request *http.Request) {
	p.rerankCalls.Add(1)
	var wire nor241RerankRequestWire
	if err := json.NewDecoder(request.Body).Decode(&wire); err != nil {
		nor241ProviderError(response, "undecodable rerank request: "+err.Error())
		return
	}
	p.mu.Lock()
	p.lastRerank = wire
	p.mu.Unlock()
	nor241Signal(p.rerankArrived)
	if p.holdRerank.Load() {
		<-request.Context().Done()
		return
	}
	if p.failRerank.Load() {
		nor241ProviderError(response, "nor241 injected rerank failure")
		return
	}
	scores := make([]float64, len(wire.Documents))
	order := make([]int, len(wire.Documents))
	for index, document := range wire.Documents {
		score, err := nor241DocumentScore(document)
		if err != nil {
			nor241ProviderError(response, err.Error())
			return
		}
		scores[index], order[index] = score, index
	}
	sort.SliceStable(order, func(i, j int) bool { return scores[order[i]] > scores[order[j]] })
	ranked := order
	if wire.TopK > 0 && wire.TopK < len(ranked) {
		ranked = ranked[:wire.TopK]
	}
	answer := nor241RerankResponseWire{Model: wire.Model}
	answer.Usage.TotalTokens = len(wire.Documents)
	for _, index := range ranked {
		answer.Data = append(answer.Data, nor241RerankItemWire{Index: index, RelevanceScore: scores[index]})
	}
	nor241ProviderJSON(response, answer)
}

func (p *nor241ProviderServer) lastEmbeddingRequest() nor241EmbeddingRequestWire {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastEmbedding
}

func (p *nor241ProviderServer) lastRerankRequest() nor241RerankRequestWire {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastRerank
}

// setFailures publishes the injected provider failures.
func (p *nor241ProviderServer) setFailures(embedding, rerank bool) {
	p.failEmbedding.Store(embedding)
	p.failRerank.Store(rerank)
}

// setHold publishes whether a provider call stays in flight until canceled.
func (p *nor241ProviderServer) setHold(embedding, rerank bool) {
	p.holdEmbedding.Store(embedding)
	p.holdRerank.Store(rerank)
}

// drainArrivals drops arrival signals a previous phase left behind.
func (p *nor241ProviderServer) drainArrivals() {
	nor241Drain(p.embeddingArrived)
	nor241Drain(p.rerankArrived)
}

func nor241Signal(channel chan struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}

func nor241Drain(channel chan struct{}) {
	for {
		select {
		case <-channel:
		default:
			return
		}
	}
}

// awaitArrival waits for the provider to start one call, and reports a caller
// that never reached it instead of blocking the test.
func nor241AwaitArrival(t *testing.T, channel chan struct{}, what string) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(nor241ProviderTimeout):
		t.Fatalf("the %s provider call never reached the loopback server", what)
	}
}

func nor241ProviderJSON(response http.ResponseWriter, value any) {
	response.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(response).Encode(value); err != nil {
		http.Error(response, `{"detail":"encode provider response"}`, http.StatusInternalServerError)
	}
}

// nor241ProviderError answers 400: the Voyage client treats a request-validation
// failure as permanent, so an injected failure surfaces without retry delay.
func nor241ProviderError(response http.ResponseWriter, detail string) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(response).Encode(map[string]string{"detail": detail})
}

// nor241QueryVector is the fixture's controlled query vector, delivered through
// the embedding provider instead of the request.
func nor241QueryVector() []float32 {
	vector := make([]float32, nor406EmbeddingDimensions)
	vector[0] = 1
	return vector
}

// nor241PhaseRecorder collects the provider time and call count of one
// qualification operation. Provider wrappers look it up from the statement
// context, so a call is attributed to the operation that made it even when
// several operations run concurrently.
type nor241PhaseRecorder struct {
	mu             sync.Mutex
	embedding      time.Duration
	rerank         time.Duration
	embeddingCalls int
	rerankCalls    int
}

type nor241PhaseContextKey struct{}

func withNOR241Phases(ctx context.Context, phases *nor241PhaseRecorder) context.Context {
	return context.WithValue(ctx, nor241PhaseContextKey{}, phases)
}

func nor241PhasesFrom(ctx context.Context) *nor241PhaseRecorder {
	phases, _ := ctx.Value(nor241PhaseContextKey{}).(*nor241PhaseRecorder)
	return phases
}

func (p *nor241PhaseRecorder) addEmbedding(elapsed time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.embedding += elapsed
	p.embeddingCalls++
}

func (p *nor241PhaseRecorder) addRerank(elapsed time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rerank += elapsed
	p.rerankCalls++
}

func (p *nor241PhaseRecorder) read() (embedding, rerank time.Duration, embeddingCalls, rerankCalls int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.embedding, p.rerank, p.embeddingCalls, p.rerankCalls
}

// nor241TimedEmbedder is the real query embedder with its provider round trip
// recorded. It changes no behavior: it forwards to the Voyage embedding client
// and keeps the dimensions and model the executor validates.
type nor241TimedEmbedder struct {
	inner QueryEmbedder
}

func (e *nor241TimedEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	started := time.Now()
	vector, err := e.inner.Embed(ctx, text)
	if phases := nor241PhasesFrom(ctx); phases != nil {
		phases.addEmbedding(time.Since(started))
	}
	return vector, err
}

func (e *nor241TimedEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return e.inner.ChunkText(text, maxTokens, overlap)
}

func (e *nor241TimedEmbedder) Dimensions() int {
	if sized, ok := e.inner.(interface{ Dimensions() int }); ok {
		return sized.Dimensions()
	}
	return 0
}

func (e *nor241TimedEmbedder) Model() string {
	if named, ok := e.inner.(interface{ Model() string }); ok {
		return named.Model()
	}
	return ""
}

// nor241TimedReranker is the real rerank client with its provider round trip
// recorded. It keeps the provider's own name, enablement and availability.
type nor241TimedReranker struct {
	inner search.Reranker
}

func (r *nor241TimedReranker) Name() string { return r.inner.Name() }

func (r *nor241TimedReranker) Enabled() bool { return r.inner.Enabled() }

func (r *nor241TimedReranker) IsAvailable(ctx context.Context) bool { return r.inner.IsAvailable(ctx) }

func (r *nor241TimedReranker) Rerank(ctx context.Context, query string, candidates []search.RerankCandidate) ([]search.RerankResult, error) {
	started := time.Now()
	results, err := r.inner.Rerank(ctx, query, candidates)
	if phases := nor241PhasesFrom(ctx); phases != nil {
		phases.addRerank(time.Since(started))
	}
	return results, err
}

// nor241Harness is the 49K corpus, the loopback provider and the real clients.
// The corpus is built once per run; every case shares it read-only.
type nor241Harness struct {
	fixture  *nor406RetrievalFixture
	provider *nor241ProviderServer
	embedder *nor241TimedEmbedder
}

func newNOR241Harness(tb testing.TB) *nor241Harness {
	tb.Helper()
	tb.Setenv(search.EnvSearchResultCacheEnabled, "false")
	provider := newNOR241ProviderServer(tb)
	embedder, err := providerembed.NewVoyage(&providerembed.Config{
		Provider:   "voyage",
		APIURL:     provider.server.URL,
		APIPath:    "/v1/embeddings",
		APIKey:     nor241APIKey,
		Model:      nor241EmbeddingModel,
		Dimensions: nor406EmbeddingDimensions,
		Timeout:    nor241ProviderTimeout,
	})
	if err != nil {
		tb.Fatalf("create voyage embedding client: %v", err)
	}
	reranker, err := voyage.NewReranker(&voyage.RerankerConfig{
		Enabled: true,
		APIURL:  provider.server.URL,
		APIKey:  nor241APIKey,
		Model:   nor241RerankModel,
		Timeout: nor241ProviderTimeout,
	})
	if err != nil {
		tb.Fatalf("create voyage rerank client: %v", err)
	}
	fixture := newNOR406RetrievalFixtureOnEngine(tb, nor241Population, newTestMemoryEngine(tb))
	// Replace the screening benchmark's controlled in-process reranker with the
	// real client. The qualification measures the provider wire path, so the
	// fixture's own stand-in must not answer.
	fixture.service.SetReranker(&nor241TimedReranker{inner: reranker})
	return &nor241Harness{
		fixture:  fixture,
		provider: provider,
		embedder: &nor241TimedEmbedder{inner: embedder},
	}
}

// newExecutor builds one worker's public Cypher entry point: result cache off,
// the shared search service, and the real embedding client.
func (h *nor241Harness) newExecutor() *StorageExecutor {
	exec := h.fixture.newExecutor()
	exec.SetEmbedder(h.embedder)
	return exec
}

// oneShotRequest is the fixture's terminal request without its supplied
// embedding: the vector must come from the configured provider. rerankTopK is
// search's default window, stated explicitly.
func (h *nor241Harness) oneShotRequest(filtered bool) map[string]any {
	request := map[string]any{
		"query":       nor406QueryText,
		"limit":       int64(nor241Limit),
		"minRRFScore": float64(0),
		"rerankTopK":  int64(nor241RerankWindow),
	}
	if filtered {
		request["where"] = nor406ExistsPredicate
	}
	return request
}

// firstPageRequest starts the ranked continuation the fixture's screening
// benchmark already qualifies (ten rows, 50-row ranked limit).
func (h *nor241Harness) firstPageRequest(filtered bool) map[string]any {
	request := h.oneShotRequest(filtered)
	request["mode"] = string(search.SearchContinuationRanked)
	request["n"] = int64(nor241PageSize)
	return request
}

// controlRequest asks for the fused order with no reranking: db.retrieve never
// applies the configured reranker, so this is the independent population the
// provider window is built from. A positive candidateTarget states the budget
// explicitly, which is how the one-shot request's own fusion is reproduced.
func (h *nor241Harness) controlRequest(limit, candidateTarget int, filtered bool) map[string]any {
	request := map[string]any{
		"query":       nor406QueryText,
		"limit":       int64(limit),
		"minRRFScore": float64(0),
	}
	if candidateTarget > 0 {
		request["candidateTarget"] = int64(candidateTarget)
	}
	if filtered {
		request["where"] = nor406ExistsPredicate
	}
	return request
}

// nor241Row is one published row with the values the qualification checks.
type nor241Row struct {
	node           *storage.Node
	id             string
	score          float64
	fallback       bool
	fallbackReason string
}

// nor241Page is one published continuation page.
type nor241Page struct {
	qid            string
	hasMore        bool
	released       bool
	returned       int
	fallback       bool
	fallbackReason string
	rows           []nor241Row
}

// nor241ColumnIndex locates a procedure column by the name the owner declares,
// so the qualification reads the public projection and not a copy of its order.
func nor241ColumnIndex(result *ExecuteResult, name string) (int, error) {
	for index, column := range result.Columns {
		if column == name {
			return index, nil
		}
	}
	return 0, fmt.Errorf("result has no %q column (columns: %v)", name, result.Columns)
}

// nor241ReadOneShot reads the terminal retrieval projection: one row per hit.
func nor241ReadOneShot(result *ExecuteResult) ([]nor241Row, error) {
	nodeColumn, err := nor241ColumnIndex(result, "node")
	if err != nil {
		return nil, err
	}
	scoreColumn, err := nor241ColumnIndex(result, "score")
	if err != nil {
		return nil, err
	}
	fallbackColumn, err := nor241ColumnIndex(result, "fallback_triggered")
	if err != nil {
		return nil, err
	}
	reasonColumn, err := nor241ColumnIndex(result, "fallback_reason")
	if err != nil {
		return nil, err
	}
	rows := make([]nor241Row, 0, len(result.Rows))
	for index, values := range result.Rows {
		if len(values) != len(result.Columns) {
			return nil, fmt.Errorf("row %d has %d values for %d columns", index, len(values), len(result.Columns))
		}
		node, ok := values[nodeColumn].(*storage.Node)
		if !ok {
			return nil, fmt.Errorf("row %d node is %T, want *storage.Node", index, values[nodeColumn])
		}
		score, ok := values[scoreColumn].(float64)
		if !ok {
			return nil, fmt.Errorf("row %d score is %T, want float64", index, values[scoreColumn])
		}
		fallback, ok := values[fallbackColumn].(bool)
		if !ok {
			return nil, fmt.Errorf("row %d fallback_triggered is %T, want bool", index, values[fallbackColumn])
		}
		reason, ok := values[reasonColumn].(string)
		if !ok {
			return nil, fmt.Errorf("row %d fallback_reason is %T, want string", index, values[reasonColumn])
		}
		rows = append(rows, nor241Row{node: node, id: nor241NodeID(node), score: score, fallback: fallback, fallbackReason: reason})
	}
	return rows, nil
}

// nor241ReadPage reads the continuation page projection. The page's rows carry
// the delivered score, and the page map reports qid, accounting and fallback.
func nor241ReadPage(result *ExecuteResult) (*nor241Page, error) {
	page, err := nor406ReadPage(result)
	if err != nil {
		return nil, err
	}
	values, ok := result.Rows[0][0].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("page is %T, want map[string]any", result.Rows[0][0])
	}
	fallback, _ := values["fallback_triggered"].(bool)
	reason, _ := values["fallback_reason"].(string)
	entries, ok := values["results"].([]any)
	if !ok {
		return nil, fmt.Errorf("page results are %T, want []any", values["results"])
	}
	read := &nor241Page{
		qid:            page.qid,
		hasMore:        page.hasMore,
		released:       page.released,
		returned:       page.returned,
		fallback:       fallback,
		fallbackReason: reason,
		rows:           make([]nor241Row, 0, len(entries)),
	}
	for index, entry := range entries {
		row, ok := entry.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("page result %d is %T, want map[string]any", index, entry)
		}
		score, ok := row["score"].(float64)
		if !ok {
			return nil, fmt.Errorf("page result %d score is %T, want float64", index, row["score"])
		}
		read.rows = append(read.rows, nor241Row{node: page.nodes[index], id: nor241NodeID(page.nodes[index]), score: score})
	}
	return read, nil
}

func nor241NodeID(node *storage.Node) string {
	if raw, ok := node.Properties["id"].(string); ok && raw != "" {
		return raw
	}
	return string(node.ID)
}

// nor241RowIDs returns the published row identities in delivered order.
func nor241RowIDs(rows []nor241Row) []string {
	ids := make([]string, len(rows))
	for index := range rows {
		ids[index] = rows[index].id
	}
	return ids
}

func nor241Nodes(rows []nor241Row) []*storage.Node {
	nodes := make([]*storage.Node, len(rows))
	for index := range rows {
		nodes[index] = rows[index].node
	}
	return nodes
}

// nor241ExpectedOrder is the oracle: the ids of a fused window in the order the
// deterministic provider must publish them (ascending source index, which is
// descending 1 - N/10).
func nor241ExpectedOrder(rows []nor241Row) ([]string, error) {
	type scored struct {
		id    string
		index int
	}
	window := make([]scored, len(rows))
	for position, row := range rows {
		index, err := nor241SourceIndexOf(row.id)
		if err != nil {
			return nil, err
		}
		window[position] = scored{id: row.id, index: index}
	}
	sort.SliceStable(window, func(i, j int) bool { return window[i].index < window[j].index })
	ids := make([]string, len(window))
	for position := range window {
		ids[position] = window[position].id
	}
	return ids, nil
}

// nor241CheckRows applies the per-publication invariants: every row is a unique
// eligible SearchSource of the corpus, and a reranked publication carries the
// deterministic provider score of its own identity in strictly descending order.
func nor241CheckRows(rows []nor241Row, reranked bool, filtered bool) error {
	if err := nor406CheckSourceIDs(nor241Nodes(rows), filtered); err != nil {
		return err
	}
	if !reranked {
		return nil
	}
	previous := math.Inf(1)
	for _, row := range rows {
		expected, err := nor241ProviderScoreOfID(row.id)
		if err != nil {
			return err
		}
		if difference := math.Abs(row.score - expected); difference > nor241ScoreEpsilon {
			return fmt.Errorf("row %s published score %v, want %v (difference %v)", row.id, row.score, expected, difference)
		}
		if row.score >= previous {
			return fmt.Errorf("row %s published score %v does not descend below the previous row's %v", row.id, row.score, previous)
		}
		previous = row.score
	}
	return nil
}

// nor241Sample is one measured operation: the provider phases it contained and
// its engine work with those phases removed.
type nor241Sample struct {
	embedding      time.Duration
	rerank         time.Duration
	engineInitial  time.Duration
	initial        time.Duration
	pulls          time.Duration
	enginePulls    time.Duration
	embeddingCalls int
	rerankCalls    int
	pullCalls      int
}

// nor241Subtract removes the provider time a phase contained from its total.
func nor241Subtract(total, embedding, rerank time.Duration) time.Duration {
	remaining := total - embedding - rerank
	if remaining < 0 {
		return 0
	}
	return remaining
}

// nor241RunOneShot performs one terminal retrieval and checks what it published.
func (h *nor241Harness) nor241RunOneShot(ctx context.Context, exec *StorageExecutor, call string, reranked bool, filtered bool) (nor241Sample, error) {
	phases := &nor241PhaseRecorder{}
	started := time.Now()
	result, err := exec.Execute(withNOR241Phases(ctx, phases), call, h.fixture.parameters(h.oneShotRequest(filtered)))
	initial := time.Since(started)
	if err != nil {
		return nor241Sample{}, fmt.Errorf("one-shot retrieval: %w", err)
	}
	rows, err := nor241ReadOneShot(result)
	if err != nil {
		return nor241Sample{}, err
	}
	if len(rows) != nor241Limit {
		return nor241Sample{}, fmt.Errorf("one-shot retrieval returned %d rows, want %d", len(rows), nor241Limit)
	}
	if err := nor241CheckRows(rows, reranked, filtered); err != nil {
		return nor241Sample{}, err
	}
	embedding, rerank, embeddingCalls, rerankCalls := phases.read()
	if reranked && rerankCalls != 1 {
		return nor241Sample{}, fmt.Errorf("one-shot retrieval made %d rerank provider calls, want 1", rerankCalls)
	}
	if !reranked && rerankCalls != 0 {
		return nor241Sample{}, fmt.Errorf("one-shot retrieval made %d rerank provider calls on a procedure without configured rerank", rerankCalls)
	}
	if embeddingCalls != 1 {
		return nor241Sample{}, fmt.Errorf("one-shot retrieval made %d embedding provider calls, want 1", embeddingCalls)
	}
	return nor241Sample{
		embedding:      embedding,
		rerank:         rerank,
		initial:        initial,
		engineInitial:  nor241Subtract(initial, embedding, rerank),
		embeddingCalls: embeddingCalls,
		rerankCalls:    rerankCalls,
	}, nil
}

// nor241RunFivePages performs one ranked continuation of five pages, checks that
// every page is complete and unique, and discards the cursor.
func (h *nor241Harness) nor241RunFivePages(ctx context.Context, exec *StorageExecutor, call string, reranked bool, filtered bool) (_ nor241Sample, err error) {
	pageCall := call + " YIELD page"
	initialPhases := &nor241PhaseRecorder{}
	started := time.Now()
	result, err := exec.Execute(withNOR241Phases(ctx, initialPhases), pageCall, h.fixture.parameters(h.firstPageRequest(filtered)))
	initial := time.Since(started)
	if err != nil {
		return nor241Sample{}, fmt.Errorf("first page: %w", err)
	}
	page, err := nor241ReadPage(result)
	if err != nil {
		return nor241Sample{}, err
	}
	qid := page.qid
	defer func() {
		if qid == "" {
			return
		}
		if discardErr := nor406Discard(ctx, exec, pageCall, qid); discardErr != nil && err == nil {
			err = discardErr
		}
	}()
	seen := make(map[string]struct{}, nor241Limit)
	rows := make([]nor241Row, 0, nor241Limit)
	pullPhases := &nor241PhaseRecorder{}
	pulls := time.Duration(0)
	pullCalls := 0
	for pulled := 0; pulled < nor241Pages; pulled++ {
		if len(page.rows) != nor241PageSize {
			return nor241Sample{}, fmt.Errorf("page %d returned %d results, want %d", pulled+1, len(page.rows), nor241PageSize)
		}
		if page.returned != len(page.rows) {
			return nor241Sample{}, fmt.Errorf("page %d reports returned %d for %d results", pulled+1, page.returned, len(page.rows))
		}
		if pulled < nor241Pages-1 && (!page.hasMore || page.qid == "") {
			return nor241Sample{}, fmt.Errorf("continuation ended after %d pages with %d rows", pulled+1, len(rows))
		}
		for _, row := range page.rows {
			if _, duplicate := seen[row.id]; duplicate {
				return nor241Sample{}, fmt.Errorf("duplicate across continuation pages: %s", row.id)
			}
			seen[row.id] = struct{}{}
			rows = append(rows, row)
		}
		if pulled == nor241Pages-1 {
			break
		}
		pullStarted := time.Now()
		next, pullErr := exec.Execute(withNOR241Phases(ctx, pullPhases), pageCall, map[string]any{
			"request": map[string]any{"qid": page.qid, "n": int64(nor241PageSize)},
		})
		pulls += time.Since(pullStarted)
		pullCalls++
		if pullErr != nil {
			return nor241Sample{}, fmt.Errorf("pull %d: %w", pulled+1, pullErr)
		}
		page, pullErr = nor241ReadPage(next)
		if pullErr != nil {
			return nor241Sample{}, fmt.Errorf("pull %d: %w", pulled+1, pullErr)
		}
		qid = page.qid
	}
	if len(rows) != nor241Limit {
		return nor241Sample{}, fmt.Errorf("five pages returned %d rows, want %d", len(rows), nor241Limit)
	}
	if err := nor241CheckRows(rows, reranked, filtered); err != nil {
		return nor241Sample{}, err
	}
	embedding, rerank, embeddingCalls, rerankCalls := initialPhases.read()
	pullEmbedding, pullRerank, pullEmbeddingCalls, pullRerankCalls := pullPhases.read()
	embedding += pullEmbedding
	rerank += pullRerank
	embeddingCalls += pullEmbeddingCalls
	rerankCalls += pullRerankCalls
	if reranked && rerankCalls != 1 {
		return nor241Sample{}, fmt.Errorf("five pages made %d rerank provider calls, want 1", rerankCalls)
	}
	if !reranked && rerankCalls != 0 {
		return nor241Sample{}, fmt.Errorf("five pages made %d rerank provider calls on a procedure without configured rerank", rerankCalls)
	}
	if embeddingCalls != 1 {
		return nor241Sample{}, fmt.Errorf("five pages made %d embedding provider calls, want 1", embeddingCalls)
	}
	initialEmbedding, initialRerank, _, _ := initialPhases.read()
	return nor241Sample{
		embedding:      embedding,
		rerank:         rerank,
		initial:        initial,
		engineInitial:  nor241Subtract(initial, initialEmbedding, initialRerank),
		pulls:          pulls,
		enginePulls:    nor241Subtract(pulls, pullEmbedding, pullRerank),
		embeddingCalls: embeddingCalls,
		rerankCalls:    rerankCalls,
		pullCalls:      pullCalls,
	}, nil
}

// nor241RunOperation performs one measured operation of the requested shape.
func (h *nor241Harness) nor241RunOperation(ctx context.Context, exec *StorageExecutor, procedure string, pages bool, filtered bool) (nor241Sample, error) {
	call := "CALL db." + procedure + "($request)"
	reranked := procedure == "rretrieve"
	if pages {
		return h.nor241RunFivePages(ctx, exec, call, reranked, filtered)
	}
	return h.nor241RunOneShot(ctx, exec, call, reranked, filtered)
}

// nor241LatencySummary returns nearest-rank percentiles and the maximum.
func nor241LatencySummary(samples []time.Duration) (p50, p95, p99, max time.Duration) {
	if len(samples) == 0 {
		return 0, 0, 0, 0
	}
	sorted := make([]time.Duration, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	at := func(percent float64) time.Duration {
		rank := int(math.Ceil(percent/100*float64(len(sorted)))) - 1
		if rank < 0 {
			rank = 0
		}
		if rank > len(sorted)-1 {
			rank = len(sorted) - 1
		}
		return sorted[rank]
	}
	return at(50), at(95), at(99), sorted[len(sorted)-1]
}

func nor241Micros(value time.Duration) float64 {
	return float64(value.Nanoseconds()) / 1e3
}

// nor241Collect returns one phase of every sample.
func nor241Collect(samples []nor241Sample, phase func(nor241Sample) time.Duration) []time.Duration {
	values := make([]time.Duration, len(samples))
	for index := range samples {
		values[index] = phase(samples[index])
	}
	return values
}

// nor241Report publishes the measured phases of one case.
func nor241Report(b *testing.B, samples []nor241Sample, clients int, procedure string, pages bool, filtered bool) {
	b.Helper()
	if len(samples) == 0 {
		b.Fatal("no measured samples")
	}
	embedding := nor241Collect(samples, func(sample nor241Sample) time.Duration { return sample.embedding })
	initial := nor241Collect(samples, func(sample nor241Sample) time.Duration { return sample.engineInitial })
	rerank := nor241Collect(samples, func(sample nor241Sample) time.Duration { return sample.rerank })
	pulls := nor241Collect(samples, func(sample nor241Sample) time.Duration { return sample.pulls })
	enginePulls := nor241Collect(samples, func(sample nor241Sample) time.Duration { return sample.enginePulls })

	report := func(name string, values []time.Duration) (time.Duration, time.Duration, time.Duration, time.Duration) {
		p50, p95, p99, max := nor241LatencySummary(values)
		b.ReportMetric(nor241Micros(p50), name+"-p50-us")
		b.ReportMetric(nor241Micros(p95), name+"-p95-us")
		b.ReportMetric(nor241Micros(p99), name+"-p99-us")
		b.ReportMetric(nor241Micros(max), name+"-max-us")
		return p50, p95, p99, max
	}
	embedding50, embedding95, embedding99, embeddingMax := report("embed", embedding)
	initial50, initial95, initial99, initialMax := report("engine-initial", initial)
	rerank50, rerank95, rerank99, rerankMax := report("rerank", rerank)
	pulls50, pulls95, pulls99, pullsMax := report("pulls", pulls)
	enginePulls50, enginePulls95, enginePulls99, enginePullsMax := report("engine-pulls", enginePulls)

	var embeddingCalls, rerankCalls, pullCalls int
	for _, sample := range samples {
		embeddingCalls += sample.embeddingCalls
		rerankCalls += sample.rerankCalls
		pullCalls += sample.pullCalls
	}
	operations := float64(len(samples))
	b.ReportMetric(operations, "samples")
	b.ReportMetric(float64(embeddingCalls)/operations, "embedding-calls/op")
	b.ReportMetric(float64(rerankCalls)/operations, "rerank-calls/op")
	b.ReportMetric(float64(pullCalls)/operations, "pull-calls/op")

	b.Logf("population=%d procedure=db.%s shape=%s clients=%d filtered=%t samples=%d: "+
		"embedding p50 %s p95 %s p99 %s max %s (%.2f calls/op); "+
		"engine-initial p50 %s p95 %s p99 %s max %s; "+
		"rerank-provider p50 %s p95 %s p99 %s max %s (%.2f calls/op); "+
		"pulls p50 %s p95 %s p99 %s max %s (%.2f calls/op), engine-pulls p50 %s p95 %s p99 %s max %s",
		nor241Population, procedure, nor241ShapeName(pages), clients, filtered, len(samples),
		embedding50.Round(time.Microsecond), embedding95.Round(time.Microsecond), embedding99.Round(time.Microsecond), embeddingMax.Round(time.Microsecond),
		float64(embeddingCalls)/operations,
		initial50.Round(time.Microsecond), initial95.Round(time.Microsecond), initial99.Round(time.Microsecond), initialMax.Round(time.Microsecond),
		rerank50.Round(time.Microsecond), rerank95.Round(time.Microsecond), rerank99.Round(time.Microsecond), rerankMax.Round(time.Microsecond),
		float64(rerankCalls)/operations,
		pulls50.Round(time.Microsecond), pulls95.Round(time.Microsecond), pulls99.Round(time.Microsecond), pullsMax.Round(time.Microsecond),
		float64(pullCalls)/operations,
		enginePulls50.Round(time.Microsecond), enginePulls95.Round(time.Microsecond), enginePulls99.Round(time.Microsecond), enginePullsMax.Round(time.Microsecond))
}

func nor241ShapeName(pages bool) string {
	if pages {
		return "five-pages"
	}
	return "one-shot"
}

// nor241Measure runs one case: a fixed sample count (b.N, selected by
// -benchtime=NNx) at the requested concurrency, each operation checked for
// complete, unique, eligible and truthfully ordered publication.
func (h *nor241Harness) nor241Measure(b *testing.B, clients int, procedure string, pages bool) {
	b.Helper()
	filtered := true
	executors := make([]*StorageExecutor, clients)
	for index := range executors {
		executors[index] = h.newExecutor()
	}
	// Warmup: one operation per worker outside the measured region, so the
	// sample does not report first-call pipelines or HTTP connection setup.
	for _, exec := range executors {
		if _, err := h.nor241RunOperation(b.Context(), exec, procedure, pages, filtered); err != nil {
			b.Fatalf("warmup: %v", err)
		}
	}
	h.provider.drainArrivals()

	workers := min(clients, b.N)
	samples := make([]nor241Sample, 0, b.N)
	var mu sync.Mutex
	var group sync.WaitGroup
	b.ResetTimer()
	for index := 0; index < workers; index++ {
		operations, remainder := b.N/workers, b.N%workers
		if index < remainder {
			operations++
		}
		if operations == 0 {
			continue
		}
		group.Add(1)
		go func(exec *StorageExecutor, operations int) {
			defer group.Done()
			for iteration := 0; iteration < operations; iteration++ {
				sample, err := h.nor241RunOperation(b.Context(), exec, procedure, pages, filtered)
				if err != nil {
					b.Error(err)
					return
				}
				mu.Lock()
				samples = append(samples, sample)
				mu.Unlock()
			}
		}(executors[index], operations)
	}
	group.Wait()
	b.StopTimer()
	if len(samples) != b.N {
		b.Fatalf("recorded %d samples for %d operations", len(samples), b.N)
	}
	nor241Report(b, samples, clients, procedure, pages, filtered)
}

// BenchmarkSearchSourceRetrievalProviderQualification measures the
// provider-inclusive retrieval at the single 49K corpus, one and sixteen
// clients, both retrieval procedures and both published shapes. One benchmark
// operation is one retrieval (or one five-page continuation); b.N is the sample
// count, so -benchtime=24x is the intended finite, reproducible run.
func BenchmarkSearchSourceRetrievalProviderQualification(b *testing.B) {
	harness := newNOR241Harness(b)
	for _, clients := range []int{1, 16} {
		for _, procedure := range []string{"retrieve", "rretrieve"} {
			for _, shape := range []struct {
				name  string
				pages bool
			}{{"one-shot", false}, {"five-pages", true}} {
				b.Run(fmt.Sprintf("%s/%s/clients=%d", procedure, shape.name, clients), func(b *testing.B) {
					harness.nor241Measure(b, clients, procedure, shape.pages)
				})
			}
		}
	}
}

// TestSearchSourceRetrievalProviderQualification49K is the correctness half of
// the qualification: exact row identities and ranking against independent
// rerank-disabled control retrievals, complete publication over five pages, and
// cancel, provider-error and recovery behavior on the same 49K fixture.
func TestSearchSourceRetrievalProviderQualification49K(t *testing.T) {
	if testing.Short() {
		t.Skip("the provider qualification builds one 49K corpus; skipped in -short mode")
	}
	harness := newNOR241Harness(t)
	provider := harness.provider

	for _, filtered := range []bool{false, true} {
		t.Run(fmt.Sprintf("filtered=%t", filtered), func(t *testing.T) {
			ctx := t.Context()
			exec := harness.newExecutor()

			// Independent controls: the fused windows with no reranking, from
			// the public procedure that never applies the configured reranker.
			// The one-shot request submits the 100-candidate TopK window of its
			// own 50-row fusion, so its control states that candidate budget.
			window, err := harness.controlRows(ctx, exec, nor241RerankWindow, nor241CandidateTarget, filtered)
			if err != nil {
				t.Fatalf("one-shot window control: %v", err)
			}
			if len(window) != nor241RerankWindow {
				t.Fatalf("one-shot window control returned %d rows, want %d", len(window), nor241RerankWindow)
			}
			expectedWindow, err := nor241ExpectedOrder(window)
			if err != nil {
				t.Fatalf("expected window order: %v", err)
			}
			expectedOneShot := expectedWindow[:nor241Limit]
			// A ranked continuation submits the rows its first step admitted,
			// so its control is the 50-row request's own fusion.
			admitted, err := harness.controlRows(ctx, exec, nor241Limit, 0, filtered)
			if err != nil {
				t.Fatalf("continuation window control: %v", err)
			}
			if len(admitted) != nor241Limit {
				t.Fatalf("continuation window control returned %d rows, want %d", len(admitted), nor241Limit)
			}
			expectedPages, err := nor241ExpectedOrder(admitted)
			if err != nil {
				t.Fatalf("expected continuation order: %v", err)
			}

			t.Run("embedding wire", func(t *testing.T) {
				request := provider.lastEmbeddingRequest()
				if request.Model != nor241EmbeddingModel {
					t.Fatalf("embedding request model %q, want %q", request.Model, nor241EmbeddingModel)
				}
				if request.InputType != "query" {
					t.Fatalf("embedding request input_type %q, want %q", request.InputType, "query")
				}
				if request.OutputDimension != nor406EmbeddingDimensions {
					t.Fatalf("embedding request output_dimension %d, want %d", request.OutputDimension, nor406EmbeddingDimensions)
				}
				if len(request.Input) != 1 || request.Input[0] != nor406QueryText {
					t.Fatalf("embedding request inputs %v, want the qualification query", request.Input)
				}
				if provider.embeddingCalls.Load() == 0 {
					t.Fatal("no embedding provider call was made")
				}
			})

			t.Run("one-shot exact identities and ranking", func(t *testing.T) {
				embeddingBefore := provider.embeddingCalls.Load()
				rerankBefore := provider.rerankCalls.Load()
				result, err := exec.Execute(ctx, "CALL db.rretrieve($request)", harness.fixture.parameters(harness.oneShotRequest(filtered)))
				if err != nil {
					t.Fatalf("rretrieve one-shot: %v", err)
				}
				rows, err := nor241ReadOneShot(result)
				if err != nil {
					t.Fatalf("read one-shot rows: %v", err)
				}
				if len(rows) != nor241Limit {
					t.Fatalf("one-shot rretrieve returned %d rows, want %d", len(rows), nor241Limit)
				}
				for _, row := range rows {
					if row.fallback || row.fallbackReason != "" {
						t.Fatalf("row %s reports fallback %t/%q on a successful retrieval", row.id, row.fallback, row.fallbackReason)
					}
				}
				if got := nor241RowIDs(rows); !nor241EqualIDs(got, expectedOneShot) {
					t.Fatalf("one-shot rretrieve ids/order:\n got %v\nwant %v", got, expectedOneShot)
				}
				if err := nor241CheckRows(rows, true, filtered); err != nil {
					t.Fatalf("one-shot rows: %v", err)
				}
				if calls := provider.embeddingCalls.Load() - embeddingBefore; calls != 1 {
					t.Fatalf("one-shot rretrieve made %d embedding provider calls, want 1", calls)
				}
				if calls := provider.rerankCalls.Load() - rerankBefore; calls != 1 {
					t.Fatalf("one-shot rretrieve made %d rerank provider calls, want 1", calls)
				}
				rerankRequest := provider.lastRerankRequest()
				if len(rerankRequest.Documents) != nor241RerankWindow {
					t.Fatalf("rerank provider received %d documents, want the %d-candidate window", len(rerankRequest.Documents), nor241RerankWindow)
				}
				if rerankRequest.Query != nor406QueryText {
					t.Fatalf("rerank provider received query %q, want %q", rerankRequest.Query, nor406QueryText)
				}
				if rerankRequest.Model != nor241RerankModel {
					t.Fatalf("rerank provider received model %q, want %q", rerankRequest.Model, nor241RerankModel)
				}
			})

			t.Run("five-pages complete publication", func(t *testing.T) {
				call := "CALL db.rretrieve($request) YIELD page"
				embeddingBefore := provider.embeddingCalls.Load()
				rerankBefore := provider.rerankCalls.Load()
				result, err := exec.Execute(ctx, call, harness.fixture.parameters(harness.firstPageRequest(filtered)))
				if err != nil {
					t.Fatalf("first page: %v", err)
				}
				page, err := nor241ReadPage(result)
				if err != nil {
					t.Fatalf("read first page: %v", err)
				}
				published := make([]string, 0, nor241Limit)
				rows := make([]nor241Row, 0, nor241Limit)
				qid := page.qid
				for pulled := 0; pulled < nor241Pages; pulled++ {
					if len(page.rows) != nor241PageSize || page.returned != nor241PageSize {
						t.Fatalf("page %d published %d rows (returned %d), want %d", pulled+1, len(page.rows), page.returned, nor241PageSize)
					}
					if !page.hasMore || page.qid == "" {
						t.Fatalf("page %d reports has_more=%t qid=%q for a ranked prefix with more rows", pulled+1, page.hasMore, page.qid)
					}
					if page.fallback || page.fallbackReason != "" {
						t.Fatalf("page %d reports fallback %t/%q on a successful continuation", pulled+1, page.fallback, page.fallbackReason)
					}
					for _, row := range page.rows {
						published = append(published, row.id)
						rows = append(rows, row)
					}
					qid = page.qid
					if pulled == nor241Pages-1 {
						break
					}
					next, pullErr := exec.Execute(ctx, call, map[string]any{
						"request": map[string]any{"qid": qid, "n": int64(nor241PageSize)},
					})
					if pullErr != nil {
						t.Fatalf("pull %d: %v", pulled+1, pullErr)
					}
					page, pullErr = nor241ReadPage(next)
					if pullErr != nil {
						t.Fatalf("read pull %d: %v", pulled+1, pullErr)
					}
				}
				if len(published) != nor241Limit {
					t.Fatalf("five pages published %d rows, want %d", len(published), nor241Limit)
				}
				if !nor241EqualIDs(published, expectedPages) {
					t.Fatalf("five pages ids/order:\n got %v\nwant %v", published, expectedPages)
				}
				if err := nor241CheckRows(rows, true, filtered); err != nil {
					t.Fatalf("five pages rows: %v", err)
				}
				if calls := provider.rerankCalls.Load() - rerankBefore; calls != 1 {
					t.Fatalf("five pages made %d rerank provider calls, want 1", calls)
				}
				if calls := provider.embeddingCalls.Load() - embeddingBefore; calls != 1 {
					t.Fatalf("five pages made %d embedding provider calls, want 1", calls)
				}
				// The stream continues past the published prefix: a sixth page
				// publishes new rows, never a repeat.
				next, err := exec.Execute(ctx, call, map[string]any{
					"request": map[string]any{"qid": qid, "n": int64(nor241PageSize)},
				})
				if err != nil {
					t.Fatalf("sixth page: %v", err)
				}
				sixth, err := nor241ReadPage(next)
				if err != nil {
					t.Fatalf("read sixth page: %v", err)
				}
				seen := make(map[string]struct{}, len(published))
				for _, id := range published {
					seen[id] = struct{}{}
				}
				for _, row := range sixth.rows {
					if _, duplicate := seen[row.id]; duplicate {
						t.Fatalf("sixth page republished %s", row.id)
					}
				}
				if discardErr := nor406Discard(ctx, exec, call, sixth.qid); discardErr != nil {
					t.Fatalf("discard continuation: %v", discardErr)
				}
			})

			t.Run("capped continuation reports terminal accounting", func(t *testing.T) {
				const limit = 2*nor241PageSize + 1
				request := harness.firstPageRequest(filtered)
				request["maxResults"] = int64(limit)
				const call = "CALL db.rretrieve($request) YIELD page"
				seen := make(map[string]struct{}, limit)
				for pageIndex := 0; pageIndex < 3; pageIndex++ {
					position := len(seen)
					result, err := exec.Execute(ctx, call, harness.fixture.parameters(request))
					if err != nil {
						t.Fatalf("retrieve page %d: %v", pageIndex, err)
					}
					page, err := nor241ReadPage(result)
					if err != nil {
						t.Fatal(err)
					}
					values := result.Rows[0][0].(map[string]any)
					if values["position"] != int64(position) || page.returned != len(page.rows) || page.fallback {
						t.Fatalf("page %d has invalid accounting or fallback: position=%v, returned=%d, rows=%d, fallback=%t",
							pageIndex, values["position"], page.returned, len(page.rows), page.fallback)
					}
					for _, row := range page.rows {
						if _, exists := seen[row.id]; exists {
							t.Fatalf("capped continuation repeated %s", row.id)
						}
						seen[row.id] = struct{}{}
					}
					if !page.hasMore {
						if len(seen) != limit || values["total"] != int64(limit) ||
							values["completion"] != search.SearchContinuationMaxResultsComplete ||
							values["collection_exhausted"] != false || values["ranked_pool_exhausted"] != false {
							t.Fatalf("terminal accounting: count=%d total=%v completion=%v collection=%v pool=%v",
								len(seen), values["total"], values["completion"], values["collection_exhausted"], values["ranked_pool_exhausted"])
						}
						return
					}
					request = map[string]any{"qid": page.qid, "n": int64(nor241PageSize)}
				}
				t.Fatal("capped continuation did not terminate")
			})

			t.Run("embedding provider error fail-open then recovery", func(t *testing.T) {
				provider.setFailures(true, false)
				result, err := exec.Execute(ctx, "CALL db.rretrieve($request)", harness.fixture.parameters(harness.oneShotRequest(filtered)))
				provider.setFailures(false, false)
				if err != nil {
					t.Fatalf("fail-open embedding error must be reported in the result, got error %v", err)
				}
				rows, err := nor241ReadOneShot(result)
				if err != nil {
					t.Fatalf("read failed-embedding rows: %v", err)
				}
				if len(rows) == 0 {
					t.Fatal("the failed-embedding result published no rows to report its fallback")
				}
				for _, row := range rows {
					if !row.fallback || row.fallbackReason != "query_embedding_failed" {
						t.Fatalf("row %s reports fallback %t/%q, want true/query_embedding_failed", row.id, row.fallback, row.fallbackReason)
					}
				}
				harness.expectOneShotOrder(t, ctx, exec, filtered, expectedOneShot, "after embedding recovery")
			})

			t.Run("embedding provider error fail-closed then recovery", func(t *testing.T) {
				provider.setFailures(true, false)
				request := harness.oneShotRequest(filtered)
				request["failClosed"] = true
				_, err := exec.Execute(ctx, "CALL db.rretrieve($request)", harness.fixture.parameters(request))
				provider.setFailures(false, false)
				if err == nil {
					t.Fatal("a fail-closed embedding failure must be the procedure's error")
				}
				harness.expectOneShotOrder(t, ctx, exec, filtered, expectedOneShot, "after fail-closed recovery")
			})

			t.Run("rerank provider error then recovery", func(t *testing.T) {
				provider.setFailures(false, true)
				result, err := exec.Execute(ctx, "CALL db.rretrieve($request)", harness.fixture.parameters(harness.oneShotRequest(filtered)))
				provider.setFailures(false, false)
				if err != nil {
					t.Fatalf("a rerank provider failure must be reported in the result, got error %v", err)
				}
				rows, err := nor241ReadOneShot(result)
				if err != nil {
					t.Fatalf("read failed-rerank rows: %v", err)
				}
				if len(rows) != nor241Limit {
					t.Fatalf("the rerank fallback published %d rows, want %d", len(rows), nor241Limit)
				}
				for _, row := range rows {
					if !row.fallback || row.fallbackReason != "rerank_failed" {
						t.Fatalf("row %s reports fallback %t/%q, want true/rerank_failed", row.id, row.fallback, row.fallbackReason)
					}
				}
				// The fallback is the fused order it could not rerank, not an
				// invented one.
				if got := nor241RowIDs(rows); !nor241EqualIDs(got, nor241RowIDs(window)[:nor241Limit]) {
					t.Fatalf("rerank fallback ids/order:\n got %v\nwant the fused order %v", got, nor241RowIDs(window)[:nor241Limit])
				}
				harness.expectOneShotOrder(t, ctx, exec, filtered, expectedOneShot, "after rerank recovery")
			})

			t.Run("cancel", func(t *testing.T) {
				t.Run("canceled before execution", func(t *testing.T) {
					canceled, cancel := context.WithCancel(ctx)
					cancel()
					result, err := exec.Execute(canceled, "CALL db.rretrieve($request)", harness.fixture.parameters(harness.oneShotRequest(filtered)))
					if err == nil && result != nil && len(result.Rows) > 0 {
						t.Fatalf("a pre-canceled retrieval published %d rows", len(result.Rows))
					}
					if err == nil {
						t.Fatal("a pre-canceled retrieval returned no error")
					}
				})

				t.Run("cancel while the embedding provider is in flight", func(t *testing.T) {
					provider.drainArrivals()
					provider.setHold(true, false)
					defer provider.setHold(false, false)
					canceled, cancel := context.WithCancel(ctx)
					finished := make(chan nor241CancelOutcome, 1)
					go func() {
						result, err := exec.Execute(canceled, "CALL db.rretrieve($request)", harness.fixture.parameters(harness.oneShotRequest(filtered)))
						outcome := nor241CancelOutcome{err: err}
						if err == nil {
							outcome.rows, outcome.readErr = nor241ReadOneShot(result)
						}
						finished <- outcome
					}()
					nor241AwaitArrival(t, provider.embeddingArrived, "embedding")
					cancel()
					harness.expectCancelOutcome(t, finished, "query_embedding_failed")
				})

				t.Run("cancel while the rerank provider is in flight", func(t *testing.T) {
					provider.drainArrivals()
					provider.setHold(false, true)
					defer provider.setHold(false, false)
					canceled, cancel := context.WithCancel(ctx)
					finished := make(chan nor241CancelOutcome, 1)
					go func() {
						result, err := exec.Execute(canceled, "CALL db.rretrieve($request)", harness.fixture.parameters(harness.oneShotRequest(filtered)))
						outcome := nor241CancelOutcome{err: err}
						if err == nil {
							outcome.rows, outcome.readErr = nor241ReadOneShot(result)
						}
						finished <- outcome
					}()
					nor241AwaitArrival(t, provider.rerankArrived, "rerank")
					cancel()
					harness.expectCancelOutcome(t, finished, "rerank_failed")
				})
			})
		})
	}
}

// nor241CancelOutcome is what a canceled request produced.
type nor241CancelOutcome struct {
	err     error
	rows    []nor241Row
	readErr error
}

// expectCancelOutcome pins the cancellation contract: the canceled request is
// never published as a complete success. It either fails, or it reports the
// provider failure it fell back from; a silent partial ranking is a failure.
func (h *nor241Harness) expectCancelOutcome(t *testing.T, finished chan nor241CancelOutcome, reason string) {
	t.Helper()
	select {
	case outcome := <-finished:
		if outcome.err != nil {
			t.Logf("canceled request returned an error: %v", outcome.err)
			return
		}
		if outcome.readErr != nil {
			t.Fatalf("read canceled result: %v", outcome.readErr)
		}
		if len(outcome.rows) == 0 {
			t.Fatal("the canceled retrieval neither failed nor reported a fallback")
		}
		t.Logf("canceled request published %d fallback rows with reason %q", len(outcome.rows), outcome.rows[0].fallbackReason)
		for _, row := range outcome.rows {
			if !row.fallback || row.fallbackReason != reason {
				t.Fatalf("canceled retrieval published row %s with fallback %t/%q, want true/%s", row.id, row.fallback, row.fallbackReason, reason)
			}
		}
	case <-time.After(4 * nor241ProviderTimeout):
		t.Fatal("the canceled retrieval did not return")
	}
}

// expectOneShotOrder requires the exact expected one-shot publication.
func (h *nor241Harness) expectOneShotOrder(t *testing.T, ctx context.Context, exec *StorageExecutor, filtered bool, want []string, when string) {
	t.Helper()
	result, err := exec.Execute(ctx, "CALL db.rretrieve($request)", h.fixture.parameters(h.oneShotRequest(filtered)))
	if err != nil {
		t.Fatalf("%s: rretrieve: %v", when, err)
	}
	rows, err := nor241ReadOneShot(result)
	if err != nil {
		t.Fatalf("%s: read rows: %v", when, err)
	}
	if len(rows) != nor241Limit {
		t.Fatalf("%s: rretrieve returned %d rows, want %d", when, len(rows), nor241Limit)
	}
	if got := nor241RowIDs(rows); !nor241EqualIDs(got, want) {
		t.Fatalf("%s: ids/order:\n got %v\nwant %v", when, got, want)
	}
	if err := nor241CheckRows(rows, true, filtered); err != nil {
		t.Fatalf("%s: %v", when, err)
	}
}

// controlRows runs the rerank-disabled public procedure and returns the fused
// window in the order the engine fused it.
func (h *nor241Harness) controlRows(ctx context.Context, exec *StorageExecutor, limit, candidateTarget int, filtered bool) ([]nor241Row, error) {
	result, err := exec.Execute(ctx, "CALL db.retrieve($request)", h.fixture.parameters(h.controlRequest(limit, candidateTarget, filtered)))
	if err != nil {
		return nil, err
	}
	return nor241ReadOneShot(result)
}

// nor241EqualIDs compares two published id sequences.
func nor241EqualIDs(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

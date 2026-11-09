package cypher

// Author: NOR-406 retrieval-profile lane, 2026-11-01: public Cypher screening
// benchmark. It measures db.retrieve and db.rretrieve over a SearchSource
// population at three sizes, as a one-shot search, as the first page of a
// ranked continuation, and as five pages of one continuation, with and without
// the retrieval "where" pattern that retrieve_where_test.go establishes, at one
// and sixteen concurrent workers.
//
// Workload: 12,500 / 25,000 / 49,000 synthetic SearchSource nodes belong to 356
// source groups. Each group has one Document, one Version and one Collection
// edge set, so the EXISTS pattern (node)<-[:HAS_SOURCE]-(v:Version)
// <-[:HAS_VERSION]-(:Document)-[:MEMBER_OF]->(c:Collection) can admit a node.
// Groups are split over four collections: collections 0 and 1 are listed in
// $ids and are not archived, collection 2 is listed and archived, collection 3
// is active but not listed. Exactly the 178 sources whose id is the first id of
// a collection 0 or 1 group pass the predicate; every other source is a
// non-current source of its group.
//
// Embeddings are 1024 dimensions and deterministic: a linear congruential
// generator fills every dimension from the source index, and the group's
// current source also has dimension 0 set to 1. The request supplies the
// controlled query embedding with dimension 0 set to 1, so the current sources
// are the nearest candidates and the filter examines the same population the
// search ranks. Content is about 1 KB per source so the BM25 branch carries the
// same population.
//
// The requests also set minRRFScore to 0, the public option that keeps a
// candidate found by one ranking only; the default 0.01 cuts single-ranking
// candidates past rank 40 and would shorten a page, which would make the
// returned-row counts depend on the score cutoff rather than on the population.
//
// Limits, disclosed: the fixture is deterministic and in-memory, and the
// request supplies its own embedding, so no embedder or external provider is
// reached. A concurrency-safe controlled reranker reuses voyage.PassThrough;
// rretrieve exercises real provider dispatch and memoization but no external
// network or model inference. Its measured time/call count is reported apart
// from initial execution and subsequent pulls. The executor's result cache and
// search service's
// response cache are off, so each iteration performs the public retrieval
// again instead of replaying a cached page.

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/voyage"
)

const (
	nor406EmbeddingDimensions = 1024
	nor406SourceGroups        = 356
	nor406Collections         = 4
	nor406ActiveCollections   = 2
	nor406Limit               = 50
	nor406PageSize            = 10
	nor406Pages               = 5
	nor406SourceIDPrefix      = "source-"

	nor406QueryText       = "retrieval screening corpus late fee clause"
	nor406ContentSentence = "The retrieval screening corpus records a late fee clause, a notice period, a renewal term, and a source group reference. "

	// nor406ExistsPredicate is the retrieval "where" of retrieve_where_test.go:
	// a source of a listed non-archived collection that is its version's
	// current source.
	nor406ExistsPredicate = `EXISTS { (node)<-[:HAS_SOURCE]-(v:Version)<-[:HAS_VERSION]-(:Document)-[:MEMBER_OF]->(c:Collection) WHERE c.id IN $ids AND NOT c.archived AND v.current_source_id = node.id }`
)

// nor406PopulationSizes is the population ladder. Each size is one benchmark
// level, so the standard benchmark regex selects sizes and cases; there is no
// environment knob.
var nor406PopulationSizes = []int{12500, 25000, 49000}

// BenchmarkSearchSourceRetrieval qualifies public retrieval at each corpus
// size. One disposable fixture serves the operation/filter/concurrency matrix;
// setup and index construction are outside every measured operation.
func BenchmarkSearchSourceRetrieval(b *testing.B) {
	for _, population := range nor406PopulationSizes {
		b.Run(nor406PopulationName(population), func(b *testing.B) {
			b.Setenv(search.EnvSearchResultCacheEnabled, "false")
			fixture := newNOR406RetrievalFixture(b, population)
			for _, operation := range []struct {
				name string
				run  func(context.Context, *StorageExecutor, *nor406RetrievalFixture, string, bool) error
			}{{"one-shot", nor406OneShot}, {"first-page", nor406FirstPage}, {"five-pages", nor406PullFivePages}} {
				b.Run(operation.name, func(b *testing.B) { nor406RunMatrix(b, fixture, operation.run) })
			}
		})
	}
}

// nor406RunMatrix runs the procedure, filter and concurrency levels for one
// population. The levels keep the matrix selectable by benchmark regex.
func nor406RunMatrix(b *testing.B, fixture *nor406RetrievalFixture, runOperation func(context.Context, *StorageExecutor, *nor406RetrievalFixture, string, bool) error) {
	for _, procedure := range []string{"retrieve", "rretrieve"} {
		call := "CALL db." + procedure + "($request)"
		for _, filter := range []struct {
			name     string
			filtered bool
		}{{"plain", false}, {"exists", true}} {
			for _, workers := range []struct {
				name  string
				count int
			}{{"concurrency1", 1}, {"concurrency16", 16}} {
				run := func(exec *StorageExecutor) error {
					return runOperation(b.Context(), exec, fixture, call, filter.filtered)
				}
				workerCount := workers.count
				b.Run(procedure+"/"+filter.name+"/"+workers.name, func(b *testing.B) {
					nor406RunWorkers(b, fixture, workerCount, run)
				})
			}
		}
	}
}

// nor406RunWorkers runs b.N operations with the requested concurrency, one
// executor per worker and no shared mutable request state.
func nor406RunWorkers(b *testing.B, fixture *nor406RetrievalFixture, workers int, run func(*StorageExecutor) error) {
	b.Helper()
	executors := make([]*StorageExecutor, workers)
	for index := range executors {
		executors[index] = fixture.newExecutor()
	}
	b.ReportAllocs()
	callsBefore, providerBefore := fixture.provider.calls.Load(), fixture.provider.nanos.Load()
	initialBefore, pullsBefore := fixture.initialNanos.Load(), fixture.pullNanos.Load()
	b.ResetTimer()
	defer func() {
		b.StopTimer()
		operations := float64(b.N)
		b.ReportMetric(float64(fixture.provider.calls.Load()-callsBefore)/operations, "provider-calls/op")
		b.ReportMetric(float64(fixture.provider.nanos.Load()-providerBefore)/operations, "provider-ns/op")
		b.ReportMetric(float64(fixture.initialNanos.Load()-initialBefore)/operations, "initial-ns/op")
		b.ReportMetric(float64(fixture.pullNanos.Load()-pullsBefore)/operations, "pulls-ns/op")
	}()
	if workers == 1 {
		for iteration := 0; iteration < b.N; iteration++ {
			if err := run(executors[0]); err != nil {
				b.Fatal(err)
			}
		}
		return
	}
	var group sync.WaitGroup
	perWorker, remainder := b.N/workers, b.N%workers
	for index, exec := range executors {
		iterations := perWorker
		if index < remainder {
			iterations++
		}
		group.Add(1)
		go func(exec *StorageExecutor, iterations int) {
			defer group.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				if err := run(exec); err != nil {
					b.Error(err)
					return
				}
			}
		}(exec, iterations)
	}
	group.Wait()
}

// nor406RetrievalFixture is the immutable graph, search service and request
// values one population needs. Every worker builds its own request maps, so
// the fixture is safe to read concurrently.
type nor406RetrievalFixture struct {
	store          *storage.NamespacedEngine
	service        *search.Service
	queryEmbedding []any
	collectionIDs  []any
	provider       controlledRetrievalReranker
	initialNanos   atomic.Int64
	pullNanos      atomic.Int64
}

func nor406PopulationName(population int) string {
	switch population {
	case 12500:
		return "12.5K"
	case 25000:
		return "25K"
	case 49000:
		return "49K"
	default:
		return strconv.Itoa(population)
	}
}

// newNOR406RetrievalFixture creates the graph, indexes it, and returns the
// shared values. The search response cache must already be off.
func newNOR406RetrievalFixture(b *testing.B, population int) *nor406RetrievalFixture {
	b.Helper()
	ctx := b.Context()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "nor406")

	nodes := make([]*storage.Node, 0, nor406Collections+2*nor406SourceGroups+population)
	edges := make([]*storage.Edge, 0, 2*nor406SourceGroups+population)

	for collection := 0; collection < nor406Collections; collection++ {
		id := "collection-" + strconv.Itoa(collection)
		nodes = append(nodes, &storage.Node{
			ID:         storage.NodeID(id),
			Labels:     []string{"Collection"},
			Properties: map[string]any{"id": id, "archived": collection == 2},
		})
	}

	for group := 0; group < nor406SourceGroups; group++ {
		groupID := strconv.Itoa(group)
		documentID := "document-" + groupID
		versionID := "version-" + groupID
		collectionID := "collection-" + strconv.Itoa(group%nor406Collections)
		currentSourceID := nor406SourceIDPrefix + groupID
		nodes = append(nodes,
			&storage.Node{ID: storage.NodeID(documentID), Labels: []string{"Document"}, Properties: map[string]any{"id": documentID}},
			&storage.Node{ID: storage.NodeID(versionID), Labels: []string{"Version"}, Properties: map[string]any{"id": versionID, "current_source_id": currentSourceID}},
		)
		edges = append(edges,
			&storage.Edge{ID: storage.EdgeID("member-of-" + groupID), StartNode: storage.NodeID(documentID), EndNode: storage.NodeID(collectionID), Type: "MEMBER_OF"},
			&storage.Edge{ID: storage.EdgeID("has-version-" + groupID), StartNode: storage.NodeID(documentID), EndNode: storage.NodeID(versionID), Type: "HAS_VERSION"},
		)
	}

	for index := 0; index < population; index++ {
		group := index % nor406SourceGroups
		id := nor406SourceIDPrefix + strconv.Itoa(index)
		current := index == group
		nodes = append(nodes, &storage.Node{
			ID:              storage.NodeID(id),
			Labels:          []string{"SearchSource"},
			Properties:      map[string]any{"id": id, "group": int64(group), "content": nor406SourceContent(index)},
			ChunkEmbeddings: [][]float32{nor406SourceEmbedding(index, current)},
		})
		edges = append(edges, &storage.Edge{
			ID:        storage.EdgeID("has-source-" + strconv.Itoa(index)),
			StartNode: storage.NodeID("version-" + strconv.Itoa(group)),
			EndNode:   storage.NodeID(id),
			Type:      "HAS_SOURCE",
		})
	}

	nor406BulkCreateNodes(b, store, nodes)
	nor406BulkCreateEdges(b, store, edges)

	service := search.NewService(store, nor406EmbeddingDimensions, nil)
	b.Cleanup(func() {
		if err := service.Close(); err != nil {
			b.Errorf("close search service: %v", err)
		}
	})
	if err := service.BuildIndexes(ctx); err != nil {
		b.Fatalf("build search indexes: %v", err)
	}

	fixture := &nor406RetrievalFixture{
		store:          store,
		service:        service,
		queryEmbedding: nor406QueryEmbedding(),
		collectionIDs:  []any{"collection-0", "collection-1", "collection-2"},
	}
	service.SetReranker(&fixture.provider)
	return fixture
}

// newExecutor builds one worker's public Cypher entry point with the executor
// result cache off, so each operation reaches retrieval.
func (f *nor406RetrievalFixture) newExecutor() *StorageExecutor {
	exec := NewStorageExecutor(f.store, QueryCachePolicy{MaxEntries: 0})
	exec.SetSearchService(f.service)
	return exec
}

// parameters binds the request and the statement's $ids parameter.
func (f *nor406RetrievalFixture) parameters(request map[string]any) map[string]any {
	return map[string]any{"request": request, "ids": f.collectionIDs}
}

func (f *nor406RetrievalFixture) oneShotRequest(filtered bool) map[string]any {
	request := map[string]any{
		"query":       nor406QueryText,
		"embedding":   f.queryEmbedding,
		"limit":       int64(nor406Limit),
		"minRRFScore": float64(0),
	}
	if filtered {
		request["where"] = nor406ExistsPredicate
	}
	return request
}

func (f *nor406RetrievalFixture) firstPageRequest(filtered bool) map[string]any {
	request := f.oneShotRequest(filtered)
	request["mode"] = string(search.SearchContinuationRanked)
	request["n"] = int64(nor406PageSize)
	return request
}

// nor406OneShot runs one terminal retrieval and checks its returned rows and ids.
func nor406OneShot(ctx context.Context, exec *StorageExecutor, fixture *nor406RetrievalFixture, call string, filtered bool) error {
	started := time.Now()
	result, err := exec.Execute(ctx, call, fixture.parameters(fixture.oneShotRequest(filtered)))
	fixture.initialNanos.Add(time.Since(started).Nanoseconds())
	if err != nil {
		return fmt.Errorf("one-shot retrieval: %w", err)
	}
	nodes, err := nor406ResultNodes(result)
	if err != nil {
		return err
	}
	if len(nodes) != nor406Limit {
		return fmt.Errorf("one-shot retrieval returned %d rows, want %d", len(nodes), nor406Limit)
	}
	return nor406CheckSourceIDs(nodes, filtered)
}

// nor406FirstPage starts a ranked continuation, checks the page, and discards
// the cursor it issued.
func nor406FirstPage(ctx context.Context, exec *StorageExecutor, fixture *nor406RetrievalFixture, call string, filtered bool) (err error) {
	pageCall := call + " YIELD page"
	started := time.Now()
	result, err := exec.Execute(ctx, pageCall, fixture.parameters(fixture.firstPageRequest(filtered)))
	fixture.initialNanos.Add(time.Since(started).Nanoseconds())
	if err != nil {
		return fmt.Errorf("first page: %w", err)
	}
	page, err := nor406ReadPage(result)
	if err != nil {
		return err
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
	if len(page.nodes) != nor406PageSize {
		return fmt.Errorf("first page returned %d results, want %d", len(page.nodes), nor406PageSize)
	}
	if page.returned != len(page.nodes) {
		return fmt.Errorf("first page reports returned %d for %d results", page.returned, len(page.nodes))
	}
	if !page.hasMore {
		return fmt.Errorf("first page reports no more rows for a %d-row population", fixture.population())
	}
	if qid == "" {
		return fmt.Errorf("first page reports more rows but issued no qid")
	}
	return nor406CheckSourceIDs(page.nodes, filtered)
}

// nor406PullFivePages starts one ranked continuation, consumes five pages of
// the same cursor, and discards it.
func nor406PullFivePages(ctx context.Context, exec *StorageExecutor, fixture *nor406RetrievalFixture, call string, filtered bool) (err error) {
	count := 0
	pageCall := call + " YIELD page"
	started := time.Now()
	result, err := exec.Execute(ctx, pageCall, fixture.parameters(fixture.firstPageRequest(filtered)))
	fixture.initialNanos.Add(time.Since(started).Nanoseconds())
	if err != nil {
		return fmt.Errorf("first page: %w", err)
	}
	page, err := nor406ReadPage(result)
	if err != nil {
		return err
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
	seen := make(map[storage.NodeID]struct{}, nor406Limit)
	for pulled := 0; pulled < nor406Pages; pulled++ {
		if len(page.nodes) != nor406PageSize {
			return fmt.Errorf("page %d returned %d results, want %d", pulled+1, len(page.nodes), nor406PageSize)
		}
		if page.returned != len(page.nodes) {
			return fmt.Errorf("page %d reports returned %d for %d results", pulled+1, page.returned, len(page.nodes))
		}
		if err := nor406CheckSourceIDs(page.nodes, filtered); err != nil {
			return fmt.Errorf("page %d: %w", pulled+1, err)
		}
		for _, node := range page.nodes {
			if _, duplicate := seen[node.ID]; duplicate {
				return fmt.Errorf("duplicate across continuation pages: %s", node.ID)
			}
			seen[node.ID] = struct{}{}
		}
		count += len(page.nodes)
		if pulled == nor406Pages-1 {
			break
		}
		if !page.hasMore || qid == "" {
			return fmt.Errorf("continuation ended after %d pages with %d rows", pulled+1, count)
		}
		pullStarted := time.Now()
		next, pullErr := exec.Execute(ctx, pageCall, map[string]any{
			"request": map[string]any{"qid": qid, "n": int64(nor406PageSize)},
		})
		fixture.pullNanos.Add(time.Since(pullStarted).Nanoseconds())
		if pullErr != nil {
			return fmt.Errorf("pull %d: %w", pulled+1, pullErr)
		}
		page, pullErr = nor406ReadPage(next)
		if pullErr != nil {
			return fmt.Errorf("pull %d: %w", pulled+1, pullErr)
		}
		qid = page.qid
	}
	if count != nor406Limit {
		return fmt.Errorf("five pages returned %d rows, want %d", count, nor406Limit)
	}
	return nil
}

// nor406Discard releases the cursor and requires the public discard result to
// say so.
func nor406Discard(ctx context.Context, exec *StorageExecutor, call string, qid string) error {
	result, err := exec.Execute(ctx, call, map[string]any{
		"request": map[string]any{"qid": qid, "discard": true},
	})
	if err != nil {
		return fmt.Errorf("discard cursor: %w", err)
	}
	page, err := nor406ReadPage(result)
	if err != nil {
		return fmt.Errorf("discard cursor: %w", err)
	}
	if !page.released {
		return fmt.Errorf("discard cursor: the stream was not released")
	}
	return nil
}

// nor406Page is the page map of retrievalColumns, read as the fields this
// benchmark asserts.
type nor406Page struct {
	qid      string
	hasMore  bool
	released bool
	returned int
	nodes    []*storage.Node
}

func nor406ReadPage(result *ExecuteResult) (*nor406Page, error) {
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
		return nil, fmt.Errorf("page result has %d rows", len(result.Rows))
	}
	values, ok := result.Rows[0][0].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("page is %T, want map[string]any", result.Rows[0][0])
	}
	page := &nor406Page{}
	page.qid, _ = values["qid"].(string)
	page.hasMore, _ = values["has_more"].(bool)
	page.released, _ = values["released"].(bool)
	returned, ok := values["returned"].(int64)
	if !ok {
		return nil, fmt.Errorf("page returned is %T, want int64", values["returned"])
	}
	page.returned = int(returned)
	results, ok := values["results"].([]any)
	if !ok {
		return nil, fmt.Errorf("page results are %T, want []any", values["results"])
	}
	for index, entry := range results {
		row, ok := entry.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("page result %d is %T, want map[string]any", index, entry)
		}
		node, ok := row["node"].(*storage.Node)
		if !ok {
			return nil, fmt.Errorf("page result %d node is %T, want *storage.Node", index, row["node"])
		}
		page.nodes = append(page.nodes, node)
	}
	return page, nil
}

func nor406ResultNodes(result *ExecuteResult) ([]*storage.Node, error) {
	nodes := make([]*storage.Node, 0, len(result.Rows))
	for index, row := range result.Rows {
		if len(row) == 0 {
			return nil, fmt.Errorf("row %d has no columns", index)
		}
		node, ok := row[0].(*storage.Node)
		if !ok {
			return nil, fmt.Errorf("row %d node is %T, want *storage.Node", index, row[0])
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

// nor406CheckSourceIDs requires unique SearchSource ids. With the filter, each
// id must also be its group's current source in a listed non-archived
// collection, which is exactly what nor406ExistsPredicate admits.
func nor406CheckSourceIDs(nodes []*storage.Node, filtered bool) error {
	seen := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		raw, ok := node.Properties["id"]
		if !ok {
			return fmt.Errorf("result node %s has no id property", node.ID)
		}
		id, ok := raw.(string)
		if !ok {
			return fmt.Errorf("result node %s id property is %T, want string", node.ID, raw)
		}
		if !strings.HasPrefix(id, nor406SourceIDPrefix) {
			return fmt.Errorf("result %q is not a SearchSource id", id)
		}
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("duplicate result %q", id)
		}
		seen[id] = struct{}{}
		if !filtered {
			continue
		}
		index, err := strconv.Atoi(strings.TrimPrefix(id, nor406SourceIDPrefix))
		if err != nil {
			return fmt.Errorf("result %q: %w", id, err)
		}
		group := index % nor406SourceGroups
		if index != group {
			return fmt.Errorf("result %q is not its group's current source", id)
		}
		if group%nor406Collections >= nor406ActiveCollections {
			return fmt.Errorf("result %q is outside the listed non-archived collections", id)
		}
	}
	return nil
}

// nor406SourceEmbedding is deterministic: every dimension comes from the
// source index, and only a group's current source points at dimension 0 of the
// request's query embedding.
func nor406SourceEmbedding(index int, current bool) []float32 {
	embedding := make([]float32, nor406EmbeddingDimensions)
	state := uint64(index+1) * 2654435761
	for dimension := range embedding {
		state = state*6364136223846793005 + 1442695040888963407
		embedding[dimension] = float32(int64(state>>33)%2001-1000) / 100000
	}
	if current {
		embedding[0] = 1
	}
	return embedding
}

func nor406QueryEmbedding() []any {
	embedding := make([]any, nor406EmbeddingDimensions)
	for dimension := range embedding {
		embedding[dimension] = float64(0)
	}
	embedding[0] = float64(1)
	return embedding
}

// nor406SourceContent is about 1 KB and shares the query's terms, so the BM25
// branch ranks the same population.
func nor406SourceContent(index int) string {
	return strings.Repeat(nor406ContentSentence, 8) + "Source record " + strconv.Itoa(index) + "."
}

func nor406BulkCreateNodes(b *testing.B, store *storage.NamespacedEngine, nodes []*storage.Node) {
	b.Helper()
	const chunk = 500
	for start := 0; start < len(nodes); start += chunk {
		end := min(start+chunk, len(nodes))
		if err := store.BulkCreateNodes(b.Context(), nodes[start:end]); err != nil {
			b.Fatalf("create nodes %d-%d: %v", start, end, err)
		}
	}
}

func nor406BulkCreateEdges(b *testing.B, store *storage.NamespacedEngine, edges []*storage.Edge) {
	b.Helper()
	const chunk = 2000
	for start := 0; start < len(edges); start += chunk {
		end := min(start+chunk, len(edges))
		if err := store.BulkCreateEdges(b.Context(), edges[start:end]); err != nil {
			b.Fatalf("create edges %d-%d: %v", start, end, err)
		}
	}
}

// population is reported by the first-page assertion so a short page names the
// population it was measured on.
func (f *nor406RetrievalFixture) population() int {
	return f.service.EmbeddingCount()
}

// controlledRetrievalReranker keeps qualification network-free and makes
// actual provider dispatch visible without duplicating result conversion.
type controlledRetrievalReranker struct {
	fixtureowner.AlwaysEnabled
	calls atomic.Int64
	nanos atomic.Int64
}

func (*controlledRetrievalReranker) Name() string                         { return "controlled-retrieval" }
func (*controlledRetrievalReranker) IsAvailable(ctx context.Context) bool { return ctx.Err() == nil }
func (r *controlledRetrievalReranker) Rerank(ctx context.Context, _ string, candidates []search.RerankCandidate) ([]search.RerankResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	started := time.Now()
	results := voyage.PassThrough(candidates)
	r.calls.Add(1)
	r.nanos.Add(time.Since(started).Nanoseconds())
	return results, nil
}

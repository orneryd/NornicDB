package cypher

// BenchmarkWhereFilteredSourceRetrieval preserves the published NOR-407 lab
// workload: disk-backed Badger, sequential writes of 12,500 nodes, 356 sources
// and memberships, 1,024-dimensional named vectors, the original seed-7 corpus
// and rotating distinct queries, default score cutoff, and an identity reranker.
// Each index arm owns a fresh corpus. Setup and index build precede timing;
// page validation and cursor cleanup are outside the timed region.
//
// The predicate independently admits exactly 356 sources. Every measured page
// checks membership, uniqueness, accounting and score order. Requesting 300 can
// return fewer results under the default cutoff; it does not assert 300 hits.
// Additional min-rrf-zero arms use the existing public option to require exactly
// n rows; they are separate from the unchanged default-cutoff lab shapes.
// No preliminary retrieval warms the measured query. The counter continues
// across page sizes within an arm and restarts for the other arm's corpus.
//
// Deliberate differences from the published harness are typed current APIs
// instead of reflection adapters, prompt cursor disposal instead of expiry, a
// shared controlled pass-through reranker, and exact assertions instead of a
// reduced row fingerprint. The corpus size is fixed to its published default.
// Distinct query texts avoid query-result hits; index construction can vary
// between processes, so this does not establish cross-process rank identity.
// This measures warm first pages only, not cold admission, later cursor pages,
// external providers, concurrency or the separate after-schema query shape.

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	whereCorpusDimensions = 1024
	whereCorpusTotalNodes = 12500
	whereCorpusSources    = 356

	// whereCorpusEmbeddings is every node the lab's corpus indexes as a vector:
	// all of them except the 356 documents, which carry none.
	whereCorpusEmbeddings = whereCorpusTotalNodes - whereCorpusSources

	whereCorpusTextWords = 1000
	whereCorpusSeed      = 7

	// whereCorpusOtherBody is repeated 170 times for each :Other node, as the
	// lab did.
	whereCorpusOtherBody = "payload stored on an unrelated node "

	// whereCorpusVocabulary is the 15-word vocabulary the lab drew source text
	// from.
	whereCorpusVocabulary = "company registration office record invoice payment contract tenant lease insurance claim policy letter statement balance account"

	// whereCorpusPredicate is the lab's where predicate: the candidate is
	// current, and its document belongs to the collection.
	whereCorpusPredicate = `coalesce(node.current, true) AND EXISTS { MATCH (:PDDocument {id: node.document_id})-[:MEMBER_OF]->(:PDCollection) }`

	// whereCorpusTypes is the lab's types list, and whereCorpusCall its
	// db.retrieve statement: the request as one map literal, the queried text as
	// the statement's only parameter, no YIELD (the page is the only row's only
	// column), and no minRRFScore, so the measured retrieval runs the default
	// cutoff the lab ran.
	whereCorpusTypes = "['PDSearchSource', 'PDSearchable']"
	whereCorpusCall  = "CALL db.retrieve({query: $q, n: %d%s, types: " + whereCorpusTypes + ", where: '" + whereCorpusPredicate + "'})"

	// whereCorpusDiscardCall releases the cursor a page issued. It is a second
	// statement because the retrieve above carries its request as literals.
	whereCorpusDiscardCall = "CALL db.retrieve({qid: $qid, discard: true})"
)

// whereCorpusPhrases is the lab's rotation: seven phrases, each suffixed with the
// per-call counter, so every measured iteration embeds a distinct query.
var whereCorpusPhrases = []string{"invoice payment", "tenant lease contract", "insurance claim", "company registration", "bank statement balance", "letter account", "policy office record"}

// whereCorpusEmbedder is the lab's stand-in embedder: a vector whose draws depend
// only on the query text's length, and one chunk per text. It keeps the
// chunk/embed/retrieve path in the measured region without a provider.
type whereCorpusEmbedder struct{ dims int }

func (e whereCorpusEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	r := rand.New(rand.NewSource(int64(len(text))))
	embedding := make([]float32, e.dims)
	for dimension := range embedding {
		embedding[dimension] = r.Float32()
	}
	return embedding, nil
}

func (e whereCorpusEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	return []string{text}, nil
}

// whereCorpusFixture is one corpus, its search service, the eligible source ids
// the generated graph fixes and the lab's per-call counter. It is read-only
// during the measured region apart from that counter, which only this fixture's
// single worker advances.
type whereCorpusFixture struct {
	executor *StorageExecutor
	provider controlledRetrievalReranker
	eligible map[string]struct{}
	calls    int
}

// BenchmarkWhereFilteredSourceRetrieval measures the lab's where-filtered
// db.retrieve first page over the 12,500-node source corpus, at the lab's two
// page sizes, with and without the PDDocument id property index. Every measured
// operation also checks the exact admissible population, the page's accounting
// and its ranked order.
func BenchmarkWhereFilteredSourceRetrieval(b *testing.B) {
	for _, arm := range []struct {
		name          string
		propertyIndex bool
	}{
		{"document-id-index-absent", false},
		{"document-id-index-present", true},
	} {
		b.Run(arm.name, func(b *testing.B) {
			fixture := newWhereCorpusFixture(b, arm.propertyIndex)
			for _, shape := range []struct {
				name     string
				n        int
				fullPage bool
			}{
				{"n3", 3, false},
				{"n300", 300, false},
				{"n3-min-rrf-zero", 3, true},
				{"n300-min-rrf-zero", 300, true},
			} {
				b.Run(shape.name, func(b *testing.B) { whereCorpusRun(b, fixture, shape.n, shape.fullPage) })
			}
		})
	}
}

// newWhereCorpusFixture builds the corpus, qualifies its eligible population and
// installs the search service, the identity reranker and the embedding stand-in.
// All of it happens outside every measured operation.
func newWhereCorpusFixture(b *testing.B, propertyIndex bool) *whereCorpusFixture {
	b.Helper()
	ctx := b.Context()

	// The lab's store is on disk, under the benchmark's temporary directory.
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](b, storage.ErrStorageClosed)(storage.NewBadgerEngine(b.TempDir()))
	if err != nil {
		b.Fatalf("open the lab's badger store: %v", err)
	}
	// Registered before the search service's cleanup below, so Cleanup (last in,
	// first out) closes the service first and the store it reads through last.
	b.Cleanup(func() {
		if err := engine.Close(); err != nil {
			b.Errorf("close the lab's badger store: %v", err)
		}
	})
	store := storage.NewNamespacedEngine(engine, "verify")
	executor := NewStorageExecutor(store)

	// One seeded stream, consumed in the lab's creation order, so the corpus
	// vectors and source texts are the values the lab measured.
	r := rand.New(rand.NewSource(whereCorpusSeed))
	vector := func() []float32 {
		embedding := make([]float32, whereCorpusDimensions)
		for dimension := range embedding {
			embedding[dimension] = r.Float32()
		}
		return embedding
	}
	words := strings.Fields(whereCorpusVocabulary)
	text := func(count int) string {
		parts := make([]string, count)
		for index := range parts {
			parts[index] = words[r.Intn(len(words))]
		}
		return strings.Join(parts, " ")
	}
	otherBody := strings.Repeat(whereCorpusOtherBody, 170)

	// One node per call, in the lab's order: the store then holds the lab's own
	// version, counter and change history for the corpus rather than a bulk
	// load's.
	create := func(node *storage.Node) {
		b.Helper()
		if _, err := store.CreateNode(ctx, node); err != nil {
			b.Fatalf("create node %s: %v", node.ID, err)
		}
	}
	for index := 0; index < whereCorpusTotalNodes-2*whereCorpusSources; index++ {
		create(&storage.Node{
			ID:              storage.NodeID("o" + strconv.Itoa(index)),
			Labels:          []string{"Other"},
			Properties:      map[string]any{"id": int64(index), "body": otherBody},
			NamedEmbeddings: map[string][]float32{"default": vector()},
		})
	}
	for index := 0; index < whereCorpusSources; index++ {
		sourceID := "s" + strconv.Itoa(index)
		documentID := "d" + strconv.Itoa(index)
		create(&storage.Node{
			ID:         storage.NodeID(documentID),
			Labels:     []string{"PDDocument"},
			Properties: map[string]any{"id": documentID, "current_version_id": "v" + documentID},
		})
		create(&storage.Node{
			ID:     storage.NodeID(sourceID),
			Labels: []string{"PDSearchSource", "PDSearchable"},
			Properties: map[string]any{
				"id":          sourceID,
				"document_id": documentID,
				"text":        text(whereCorpusTextWords),
				"current":     true,
			},
			NamedEmbeddings: map[string][]float32{"default": vector()},
		})
	}

	if _, err := executor.Execute(ctx, "CREATE (:PDCollection {id: 'c', archived: false}), (:PDSearchSettings {id: 'search', rerank_top_k: 20})", nil); err != nil {
		b.Fatalf("create the collection and settings nodes: %v", err)
	}
	if _, err := executor.Execute(ctx, "MATCH (d:PDDocument), (c:PDCollection) CREATE (d)-[:MEMBER_OF]->(c)", nil); err != nil {
		b.Fatalf("create the document memberships: %v", err)
	}
	if propertyIndex {
		if _, err := executor.Execute(ctx, "CREATE INDEX nor407_document_id IF NOT EXISTS FOR (d:PDDocument) ON (d.id)", nil); err != nil {
			b.Fatalf("create the document id property index: %v", err)
		}
	}

	// The eligible population is fixed by the generated graph, and the predicate
	// the measured request carries is the one that must select exactly it.
	fixture := &whereCorpusFixture{
		executor: executor,
		eligible: make(map[string]struct{}, whereCorpusSources),
	}
	for index := 0; index < whereCorpusSources; index++ {
		fixture.eligible["s"+strconv.Itoa(index)] = struct{}{}
	}
	admitted, err := whereCorpusAdmittedIDs(ctx, executor)
	if err != nil {
		b.Fatalf("qualify the where predicate: %v", err)
	}
	if len(admitted) != len(fixture.eligible) {
		b.Fatalf("the where predicate admits %d sources, want %d", len(admitted), len(fixture.eligible))
	}
	seen := make(map[string]struct{}, len(admitted))
	for _, id := range admitted {
		if _, eligible := fixture.eligible[id]; !eligible {
			b.Fatalf("the where predicate admits %q, which is not one of the %d generated sources", id, len(fixture.eligible))
		}
		if _, duplicate := seen[id]; duplicate {
			b.Fatalf("the independent predicate returned duplicate source %q", id)
		}
		seen[id] = struct{}{}
	}

	service := search.NewService(store, whereCorpusDimensions, nil)
	b.Cleanup(func() {
		if err := service.Close(); err != nil {
			b.Errorf("close search service: %v", err)
		}
	})
	if err := service.BuildIndexes(ctx); err != nil {
		b.Fatalf("build search indexes: %v", err)
	}
	if indexed := service.EmbeddingCount(); indexed != whereCorpusEmbeddings {
		b.Fatalf("the search index holds %d vectors, want the lab's %d", indexed, whereCorpusEmbeddings)
	}
	// The lab installed an identity reranker; the screening fixture's controlled
	// provider is that same voyage.PassThrough behavior with no network and no
	// model.
	service.SetReranker(&fixture.provider)
	executor.SetSearchService(service)
	executor.SetEmbedder(whereCorpusEmbedder{dims: whereCorpusDimensions})
	return fixture
}

// whereCorpusAdmittedIDs returns the ids the corpus predicate admits, through a
// plain MATCH over the same predicate text, so the expectation does not depend on
// the retrieval pipeline it qualifies.
func whereCorpusAdmittedIDs(ctx context.Context, executor *StorageExecutor) ([]string, error) {
	result, err := executor.Execute(ctx, "MATCH (node:PDSearchSource) WHERE "+whereCorpusPredicate+" RETURN node.id AS id", nil)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(result.Rows))
	for index, row := range result.Rows {
		if len(row) != 1 {
			return nil, fmt.Errorf("row %d has %d columns, want 1", index, len(row))
		}
		id, ok := row[0].(string)
		if !ok {
			return nil, fmt.Errorf("row %d id is %T, want string", index, row[0])
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// whereCorpusParameters is the lab's parameter map for one call: the next phrase
// of the seven, suffixed with the running counter, plus the request fields the
// lab always sent beside it. The counter belongs to the fixture and is never
// reset, so no query text repeats within one corpus.
func (f *whereCorpusFixture) whereCorpusParameters() map[string]any {
	call := f.calls
	f.calls++
	query := fmt.Sprintf("%s %d", whereCorpusPhrases[call%len(whereCorpusPhrases)], call)
	return map[string]any{
		"q": query, "query": query, "qid": nil,
		"page_size": int64(3), "min_similarity": 0.0, "rerank_min_score": 0.0,
		"history": false, "anchors": []any{},
	}
}

// whereCorpusRun measures one page size. Every iteration is checked against the
// generated population and the page's own accounting.
func whereCorpusRun(b *testing.B, fixture *whereCorpusFixture, want int, fullPage bool) {
	b.Helper()
	ctx := b.Context()
	options := ""
	if fullPage {
		options = ", minRRFScore: 0"
	}
	call := fmt.Sprintf(whereCorpusCall, want, options)
	var delivered int
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		parameters := fixture.whereCorpusParameters()
		result, err := fixture.executor.Execute(ctx, call, parameters)
		if err != nil {
			b.Fatalf("n=%d retrieval for %q: %v", want, parameters["q"], err)
		}
		b.StopTimer()
		page, err := whereCorpusReadPage(result)
		if err != nil {
			b.Fatalf("n=%d retrieval for %q: %v", want, parameters["q"], err)
		}
		if err := fixture.verify(page, want); err != nil {
			b.Fatalf("n=%d retrieval for %q, iteration %d: %v", want, parameters["q"], iteration+1, err)
		}
		if fullPage && len(page.ids) != want {
			b.Fatalf("zero-cutoff page returned %d results, want exactly %d", len(page.ids), want)
		}
		if page.qid != "" {
			if err := whereCorpusRelease(ctx, fixture.executor, page.qid); err != nil {
				b.Fatalf("n=%d retrieval for %q: %v", want, parameters["q"], err)
			}
		}
		delivered += len(page.ids)
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(delivered)/float64(b.N), "rows/op")
}

// whereCorpusPage is the page a ranked retrieval's first pull returns, read as
// the fields this fixture asserts. rrfScore is the fusion order key.
type whereCorpusPage struct {
	qid      string
	released bool
	returned int
	ids      []string
	rrfScore []float64
}

func whereCorpusReadPage(result *ExecuteResult) (*whereCorpusPage, error) {
	if result == nil {
		return nil, fmt.Errorf("retrieval returned no result")
	}
	if len(result.Rows) != 1 {
		return nil, fmt.Errorf("page result holds %d rows, want one row holding the page", len(result.Rows))
	}
	if len(result.Rows[0]) != 1 {
		return nil, fmt.Errorf("page row holds %d columns, want the one page column", len(result.Rows[0]))
	}
	values, ok := result.Rows[0][0].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("page is %T, want map[string]any", result.Rows[0][0])
	}
	page := &whereCorpusPage{}
	page.qid, _ = values["qid"].(string)
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
	page.ids = make([]string, 0, len(results))
	page.rrfScore = make([]float64, 0, len(results))
	for index, entry := range results {
		row, ok := entry.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("page result %d is %T, want map[string]any", index, entry)
		}
		node, ok := row["node"].(*storage.Node)
		if !ok {
			return nil, fmt.Errorf("page result %d node is %T, want *storage.Node", index, row["node"])
		}
		id, ok := node.Properties["id"].(string)
		if !ok {
			return nil, fmt.Errorf("page result %d id property is %T, want string", index, node.Properties["id"])
		}
		rrfScore, ok := row["rrf_score"].(float64)
		if !ok {
			return nil, fmt.Errorf("page result %d rrf_score is %T, want float64", index, row["rrf_score"])
		}
		page.ids = append(page.ids, id)
		page.rrfScore = append(page.rrfScore, rrfScore)
	}
	return page, nil
}

// verify checks what the generated corpus and the measured request fix: the
// page's accounting, that it is non-empty and holds at most n results, that every
// result is a unique eligible source, and the fusion order. It deliberately does
// not require n results, because the default minRRFScore cutoff the lab ran can
// shorten a page independently of the population.
func (f *whereCorpusFixture) verify(page *whereCorpusPage, want int) error {
	if page.returned != len(page.ids) {
		return fmt.Errorf("page reports returned %d for %d results", page.returned, len(page.ids))
	}
	if len(page.ids) == 0 {
		return fmt.Errorf("page returned no results of the %d eligible sources", len(f.eligible))
	}
	if len(page.ids) > want {
		return fmt.Errorf("page returned %d results for n=%d", len(page.ids), want)
	}
	seen := make(map[string]struct{}, len(page.ids))
	for index, id := range page.ids {
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("duplicate result %q", id)
		}
		seen[id] = struct{}{}
		if _, eligible := f.eligible[id]; !eligible {
			return fmt.Errorf("result %q is outside the %d eligible sources", id, len(f.eligible))
		}
		if index > 0 && page.rrfScore[index-1] < page.rrfScore[index] {
			return fmt.Errorf("result %d rrf_score %v is above result %d rrf_score %v", index, page.rrfScore[index], index-1, page.rrfScore[index-1])
		}
	}
	return nil
}

// whereCorpusRelease discards the cursor the page issued, so a measured run does
// not accumulate one live stream per iteration.
func whereCorpusRelease(ctx context.Context, executor *StorageExecutor, qid string) error {
	result, err := executor.Execute(ctx, whereCorpusDiscardCall, map[string]any{"qid": qid})
	if err != nil {
		return fmt.Errorf("discard continuation %q: %w", qid, err)
	}
	page, err := whereCorpusReadPage(result)
	if err != nil {
		return fmt.Errorf("discard continuation %q: %w", qid, err)
	}
	if !page.released {
		return fmt.Errorf("discard continuation %q: the stream was not released", qid)
	}
	return nil
}

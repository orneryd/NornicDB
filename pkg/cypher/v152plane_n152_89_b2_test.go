package cypher

// NOR-403 paired benchmark: the recorded equivalent statement shape on a
// generated corpus with the reported store size, the production schema and a
// missing-index negative control.
//
// Reproduction only: no production seam, no production change.
//
// Corpus (deterministic, generated):
//
//	346 PDDocument, 346 PDVersion, 4 PDCollection, 356 PDSource,
//	8 PDSummary and 47,940 heavy PDGraphNode rows: 49,000 nodes in total.
//
// Raw match shapes (the historical measurement points):
//
//	103 raw rows for "boiler" (103 sources)
//	258 raw rows for "invoice" (250 sources + 8 summaries)
//
// The summaries are in the fulltext index on purpose, so the summary branches
// of the multi-EXISTS predicate are evaluated on both match shapes.
//
// Schema variants:
//
//	indexed          PDDocument.id, PDCollection.id, PDVersion.id UNIQUE plus an
//	                 ONLINE PDVersion.document property index: the production set.
//	missing-index    the same corpus and fulltext index without those
//	                 constraints/indexes: the historical negative control, in
//	                 which the same statement is measurably more expensive.
//
// Each variant asserts the raw-row count, the eligible-row set (through the
// shared filter model), that the planner can use the PDVersion.document index
// where it exists, and that it claims no such seek in the control. Every
// measured sub-benchmark re-checks the projected rows, so a cost measurement
// cannot be reported for changed results. The executor result cache and the
// search-result cache are disabled, and the corpus is warm, so the bare and
// filtered reads of a pair measure the same retained corpus.
//
// Profiling: every measured execution runs under pprof labels
// ("phase"="nor403-fulltext-exists"), so the parent collects one CPU profile
// with -cpuprofile and attributes samples per parser, schema variant, match
// shape and mode. Run it on the bounded Linux host, for example:
//
//	go test -p 1 -run '^$' -bench '^BenchmarkNOR403FulltextExistsFilter$' \
//	  -benchtime 5x -cpuprofile nor403.cpu ./pkg/cypher
//
// No 16-way or concurrent stress variant is part of this benchmark.

import (
	"context"
	"fmt"
	"reflect"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/memorypressure"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	nor403BenchDocuments   = 346
	nor403BenchSources     = 356
	nor403BenchCollections = 4
	nor403BenchSummaries   = 8
	// nor403BenchTotalNodes is the reported store size: ~49k heavy nodes with
	// the 346 documents and 356 sources alongside them.
	nor403BenchTotalNodes = 49000
	// nor403BenchBoilerSources is the number of sources whose body carries
	// "boiler": the 103 raw-match shape.
	nor403BenchBoilerSources = 103
	// nor403BenchInvoiceSources plus the 8 summaries gives the 258 raw-match
	// shape.
	nor403BenchInvoiceSources = 250
	nor403BenchRawBoiler      = 103
	nor403BenchRawInvoice     = 258
	nor403BenchLimit          = 3
	// nor403BenchWideLimit is wider than any raw match shape, so a wide read
	// returns the complete population instead of a truncated page.
	nor403BenchWideLimit = 346
	// nor403BenchCorpusNodes is the document/source/summary/collection part of
	// the store; the heavy population fills the rest of nor403BenchTotalNodes.
	nor403BenchCorpusNodes = 2*nor403BenchDocuments + nor403BenchCollections + nor403BenchSources + nor403BenchSummaries
	nor403BenchHeavyNodes  = nor403BenchTotalNodes - nor403BenchCorpusNodes
)

// nor403BenchQuery is one reported raw-match shape.
type nor403BenchQuery struct {
	name, term string
	raw        int
}

// nor403BenchQueries are the two reported raw-match shapes.
var nor403BenchQueries = []nor403BenchQuery{
	{name: "raw103", term: "boiler", raw: nor403BenchRawBoiler},
	{name: "raw258", term: "invoice", raw: nor403BenchRawInvoice},
}

// nor403BenchFixture is the generated corpus, its recorded filter model and one
// public executor with the result cache disabled.
type nor403BenchFixture struct {
	*nor403Fixture
	exec *StorageExecutor
}

// nor403BenchSchema declares the production schema for the indexed variant and
// keeps only the fulltext index for the missing-index control, which must still
// be able to run the statement.
func nor403BenchSchema(indexed bool) []string {
	fulltext := "CREATE FULLTEXT INDEX " + nor403FulltextIndex + " FOR (n:PDSource|PDSummary) ON EACH [n.text]"
	if !indexed {
		return []string{fulltext}
	}
	return []string{
		"CREATE CONSTRAINT nor403_pddocument_id FOR (d:PDDocument) REQUIRE d.id IS UNIQUE",
		"CREATE CONSTRAINT nor403_pdcollection_id FOR (c:PDCollection) REQUIRE c.id IS UNIQUE",
		"CREATE CONSTRAINT nor403_pdversion_id FOR (v:PDVersion) REQUIRE v.id IS UNIQUE",
		"CREATE INDEX nor403_pdversion_document FOR (v:PDVersion) ON (v.document)",
		fulltext,
	}
}

// nor403BenchMemberships is the deterministic PDCollection membership of
// document i: archived-only, open, and open plus archived.
func nor403BenchMemberships(document int) []string {
	switch document % 3 {
	case 0:
		return []string{"c02"}
	case 1:
		return []string{"c01"}
	default:
		return []string{"c00", "c02"}
	}
}

// nor403BenchSourceText builds one source body. Exactly the first
// nor403BenchBoilerSources bodies carry "boiler" and the first
// nor403BenchInvoiceSources carry "invoice"; filler tokens vary the length
// without introducing either term.
func nor403BenchSourceText(source int) string {
	tokens := []string{"service", "notice", "record"}
	if source < nor403BenchBoilerSources {
		tokens = append(tokens, "boiler")
	}
	if source < nor403BenchInvoiceSources {
		tokens = append(tokens, "invoice")
	}
	filler := []string{"handling", "case", "reply", "thread", "filing", "revision", "digest"}
	for k := 0; k < source%5; k++ {
		tokens = append(tokens, filler[(source+k)%len(filler)])
	}
	return strings.Join(tokens, " ")
}

// nor403BenchSummaryText builds one summary body: every summary carries
// "invoice" and none carries "boiler".
func nor403BenchSummaryText(summary int) string {
	return fmt.Sprintf("summary digest invoice record%d", summary)
}

// nor403BenchSeed writes the generated corpus through the caller's bulk owner:
// the benchmark reuses the NOR-406 chunked bulk helper, and the fixture-shape
// test writes the small corpus directly.
type nor403BenchSeed func(store *storage.NamespacedEngine, nodes []*storage.Node, edges []*storage.Edge)

// newNOR403BenchFixture generates the corpus and seeds it through seed. Setup is
// outside every measured operation. heavyNodes controls only the unrelated heavy
// population, so the fixture-shape test can validate the same builder without
// allocating the full 49,000-node store.
func newNOR403BenchFixture(tb testing.TB, indexed bool, heavyNodes int, seed nor403BenchSeed) *nor403BenchFixture {
	tb.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(tb), "nor403bench")
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	require.Nil(tb, exec.cache, "the fixture must reach execution, not the result cache")
	for _, statement := range nor403BenchSchema(indexed) {
		_, err := exec.Execute(tb.Context(), statement, nil)
		require.NoError(tb, err, statement)
	}

	collections := make([]nor403Collection, 0, nor403BenchCollections)
	documents := make([]nor403Document, 0, nor403BenchDocuments)
	versions := make([]nor403Version, 0, nor403BenchDocuments)
	rows := make([]nor403Row, 0, nor403BenchSources+nor403BenchSummaries)
	nodes := make([]*storage.Node, 0, nor403BenchCorpusNodes+heavyNodes)
	edges := make([]*storage.Edge, 0, nor403BenchDocuments*2)

	for i := 0; i < nor403BenchCollections; i++ {
		id := fmt.Sprintf("c%02d", i)
		archived := i == 2
		collections = append(collections, nor403Collection{id: id, archived: nor403Bool(archived)})
		nodes = append(nodes, &storage.Node{
			ID:         storage.NodeID(id),
			Labels:     []string{"PDCollection"},
			Properties: map[string]any{"id": id, "archived": archived},
		})
	}

	for i := 0; i < nor403BenchDocuments; i++ {
		documentID := fmt.Sprintf("d%03d", i)
		versionID := fmt.Sprintf("v%03d", i)
		sourceID := fmt.Sprintf("s%03d", i)
		memberships := nor403BenchMemberships(i)
		documents = append(documents, nor403Document{id: documentID, currentVersionID: versionID, memberships: memberships})
		versions = append(versions, nor403Version{id: versionID, document: documentID, currentSourceID: sourceID})
		nodes = append(nodes,
			&storage.Node{
				ID:         storage.NodeID(documentID),
				Labels:     []string{"PDDocument"},
				Properties: map[string]any{"id": documentID, "current_version_id": versionID},
			},
			&storage.Node{
				ID:         storage.NodeID(versionID),
				Labels:     []string{"PDVersion"},
				Properties: map[string]any{"id": versionID, "document": documentID, "current_source_id": sourceID},
			})
		for _, collectionID := range memberships {
			edges = append(edges, &storage.Edge{
				ID:        storage.EdgeID("member-" + documentID + "-" + collectionID),
				StartNode: storage.NodeID(documentID),
				EndNode:   storage.NodeID(collectionID),
				Type:      "MEMBER_OF",
			})
		}
	}

	for j := 0; j < nor403BenchSources; j++ {
		id := fmt.Sprintf("s%03d", j)
		text := nor403BenchSourceText(j)
		properties := map[string]any{"id": id, "text": text}
		row := nor403Row{id: id, text: text}
		if j < nor403BenchDocuments {
			row.documentID = fmt.Sprintf("d%03d", j)
			properties["document_id"] = row.documentID
		}
		rows = append(rows, row)
		nodes = append(nodes, &storage.Node{ID: storage.NodeID(id), Labels: []string{"PDSource"}, Properties: properties})
	}

	for k := 0; k < nor403BenchSummaries; k++ {
		id := fmt.Sprintf("m%02d", k)
		row := nor403Row{id: id, summary: true, text: nor403BenchSummaryText(k)}
		switch k {
		case 0:
			row.current, row.ownerKind, row.ownerID, row.sourceIDs = nor403Bool(true), "collection", "c01", []string{"s003", "s005"}
		case 1:
			row.current, row.ownerKind, row.ownerID, row.sourceIDs = nor403Bool(true), "collection", "c02", []string{"s004"}
		case 2:
			row.current, row.ownerKind, row.ownerID, row.sourceIDs = nor403Bool(false), "collection", "c01", []string{"s004"}
		case 3:
			row.current, row.ownerKind, row.ownerID, row.sourceIDs = nor403Bool(true), "document", "d001", []string{"s001"}
		case 4:
			row.current, row.ownerKind, row.ownerID, row.sourceIDs = nor403Bool(true), "document", "d000", []string{"s000"}
		case 5:
			row.current, row.ownerKind, row.ownerID, row.sourceIDs = nor403Bool(true), "document", "d004", []string{"s003"}
		case 6:
			row.current, row.ownerKind, row.ownerID, row.sourceIDs = nor403Bool(true), "document", "d003", []string{"s003"}
		default:
			row.current, row.ownerKind, row.ownerID, row.sourceIDs = nor403Bool(true), "document", "d002", []string{"s002"}
		}
		properties := map[string]any{
			"id":         id,
			"text":       row.text,
			"current":    *row.current,
			"owner_kind": row.ownerKind,
			"owner_id":   row.ownerID,
			"source_ids": row.sourceIDs,
		}
		rows = append(rows, row)
		nodes = append(nodes, &storage.Node{ID: storage.NodeID(id), Labels: []string{"PDSummary"}, Properties: properties})
	}

	// The remaining rows are the heavy unrelated population the procedure's
	// acquisition streams past on every execution. Each body is distinct, so
	// the store holds the bodies instead of one shared string.
	require.GreaterOrEqual(tb, heavyNodes, 0)
	body := strings.Repeat("routine correspondence meeting attachment paragraph ", 16)
	for h := 0; h < heavyNodes; h++ {
		id := fmt.Sprintf("h%05d", h)
		nodes = append(nodes, &storage.Node{
			ID:         storage.NodeID(id),
			Labels:     []string{"PDGraphNode"},
			Properties: map[string]any{"id": id, "body": body + " case " + strconv.Itoa(h)},
		})
	}

	seed(store, nodes, edges)

	require.Equal(tb, nor403BenchCorpusNodes+heavyNodes, nor403BenchNodeCount(tb, exec),
		"the store holds the generated population")
	require.Len(tb, rows, nor403BenchSources+nor403BenchSummaries)
	require.Len(tb, documents, nor403BenchDocuments)
	require.Len(tb, versions, nor403BenchDocuments)

	byID := make(map[string]nor403Row, len(rows))
	for _, row := range rows {
		byID[row.id] = row
	}
	return &nor403BenchFixture{
		nor403Fixture: &nor403Fixture{
			store: store,
			rows:  rows,
			byID:  byID,
			truth: newNOR403Truth(collections, documents, versions),
		},
		exec: exec,
	}
}

// modelEligible is the shared filter model's decision for one generated row.
func (fixture *nor403BenchFixture) modelEligible(id string) bool {
	row, ok := fixture.byID[id]
	return ok && fixture.truth.sourceVisible(row)
}

// nor403BenchNodeCount is the one store-wide node count, read outside timing.
func nor403BenchNodeCount(tb testing.TB, exec *StorageExecutor) int {
	tb.Helper()
	result, err := exec.Execute(tb.Context(), "MATCH (n) RETURN count(n) AS n", nil)
	require.NoError(tb, err)
	require.Len(tb, result.Rows, 1)
	count, ok := result.Rows[0][0].(int64)
	require.True(tb, ok, "count(n) projects an integer, got %T", result.Rows[0][0])
	return int(count)
}

// verify checks the corpus, the schema variant and the raw/eligible populations
// for one match shape, outside every measured operation.
func (fixture *nor403BenchFixture) verify(tb testing.TB, indexed bool, query nor403BenchQuery) (raw, eligible []nor403ObservedRow) {
	tb.Helper()
	schema := fixture.store.GetSchema()
	_, ok := schema.GetFulltextIndex(nor403FulltextIndex)
	require.True(tb, ok, "the statement's fulltext index is declared")
	if indexed {
		require.Len(tb, schema.GetAllConstraints(), 3, "the production uniqueness constraints are declared")
	} else {
		require.Empty(tb, schema.GetAllConstraints(), "the missing-index control declares no constraints")
	}

	explain, err := fixture.exec.Execute(tb.Context(),
		"EXPLAIN MATCH (v:PDVersion {document: 'd001'}) RETURN v.id", nil)
	require.NoError(tb, err)
	plan, ok := explain.Metadata["plan"].(*ExecutionPlan)
	require.True(tb, ok, "EXPLAIN attaches an execution plan")
	require.NotNil(tb, plan.Root)
	operatorTypes := collectOperatorTypes(plan.Root)
	if indexed {
		require.Contains(tb, operatorTypes, "NodeIndexSeek",
			"the indexed variant's PDVersion.document index is planner-usable")
	} else {
		require.NotContains(tb, operatorTypes, "NodeIndexSeek",
			"the missing-index control has no index that could serve the lookup")
	}

	raw = nor403RunStatement(tb, fixture.exec, nor403RawStatement, query.term, nor403BenchWideLimit)
	require.Len(tb, raw, query.raw, "%s raw-match shape", query.name)
	eligible = nor403RankedRows(raw, nor403BenchWideLimit, fixture.modelEligible)
	require.GreaterOrEqual(tb, len(eligible), nor403BenchLimit,
		"%s keeps enough eligible rows for the LIMIT %d shape", query.name, nor403BenchLimit)
	return raw, eligible
}

// expectedFor is the paired expected row list for one measured mode: the raw
// shape for the bare read, the eligible population for the filtered reads,
// truncated by that mode's LIMIT.
func expectedFor(mode struct {
	name, statement string
	limit           int64
}, raw, eligible []nor403ObservedRow) []nor403ObservedRow {
	expected := eligible
	if mode.statement == nor403RawStatement {
		expected = raw
	}
	limit := int(mode.limit)
	if limit < len(expected) {
		expected = expected[:limit]
	}
	return expected
}

// BenchmarkNOR403FulltextExistsFilter measures the bare and filtered statement
// as a pair per raw-match shape, schema variant and parser, with the projected
// rows re-checked inside the measurement.
func BenchmarkNOR403FulltextExistsFilter(b *testing.B) {
	b.Setenv(search.EnvSearchResultCacheEnabled, "false")
	memorypressure.WithoutRuntimePressure(b)

	for _, parser := range relationshipEqualityParsers {
		b.Run(parser.name, func(b *testing.B) {
			b.Cleanup(parser.setup())
			for _, lookup := range []struct {
				name    string
				indexed bool
			}{
				{name: "indexed", indexed: true},
				{name: "missing-index", indexed: false},
			} {
				lookup := lookup
				b.Run(lookup.name, func(b *testing.B) {
					fixture := newNOR403BenchFixture(b, lookup.indexed, nor403BenchHeavyNodes,
						func(store *storage.NamespacedEngine, nodes []*storage.Node, edges []*storage.Edge) {
							nor406BulkCreateNodes(b, store, nodes)
							nor406BulkCreateEdges(b, store, edges)
						})
					for _, query := range nor403BenchQueries {
						query := query
						raw, eligible := fixture.verify(b, lookup.indexed, query)
						b.Logf("%s/%s/%s: %d raw rows, %d eligible rows",
							parser.name, lookup.name, query.name, len(raw), len(eligible))
						for _, mode := range []struct {
							name, statement string
							limit           int64
						}{
							{name: "bare-limit3", statement: nor403RawStatement, limit: nor403BenchLimit},
							{name: "filtered-limit3", statement: nor403EligibilityStatement, limit: nor403BenchLimit},
							{name: "filtered-all", statement: nor403EligibilityStatement, limit: nor403BenchWideLimit},
						} {
							mode := mode
							b.Run(query.name+"/"+mode.name, func(b *testing.B) {
								expected := expectedFor(mode, raw, eligible)
								labels := pprof.Labels(
									"phase", "nor403-fulltext-exists",
									"parser", parser.name,
									"lookup", lookup.name,
									"raw", query.name,
									"mode", mode.name)
								b.ReportAllocs()
								b.ResetTimer()
								for i := 0; i < b.N; i++ {
									pprof.Do(b.Context(), labels, func(ctx context.Context) {
										observed := nor403RunStatement(b, fixture.exec, mode.statement, query.term, mode.limit)
										if !reflect.DeepEqual(expected, observed) {
											b.Fatalf("%s/%s changed rows: got %v want %v",
												query.name, mode.name, nor403ObservedIDs(observed), nor403ObservedIDs(expected))
										}
									})
								}
							})
						}
					}
				})
			}
		})
	}
}

// TestNOR403BenchFixtureShape validates the benchmark fixture itself on the
// small corpora: the generated rows, both schema variants, the index-usage
// control and the 103/258 raw-match shapes. It runs without the heavy
// population, so the full 49,000-node store is only ever built by the benchmark
// on the bounded host.
func TestNOR403BenchFixtureShape(t *testing.T) {
	memorypressure.WithoutRuntimePressure(t)
	for _, lookup := range []struct {
		name    string
		indexed bool
	}{
		{name: "indexed", indexed: true},
		{name: "missing-index", indexed: false},
	} {
		lookup := lookup
		t.Run(lookup.name, func(t *testing.T) {
			fixture := newNOR403BenchFixture(t, lookup.indexed, 0,
				func(store *storage.NamespacedEngine, nodes []*storage.Node, edges []*storage.Edge) {
					require.NoError(t, store.BulkCreateNodes(t.Context(), nodes))
					require.NoError(t, store.BulkCreateEdges(t.Context(), edges))
				})
			require.Equal(t, nor403BenchCorpusNodes, nor403BenchNodeCount(t, fixture.exec))
			for _, query := range nor403BenchQueries {
				raw, eligible := fixture.verify(t, lookup.indexed, query)
				t.Logf("%s/%s: %d raw rows, %d eligible rows", lookup.name, query.name, len(raw), len(eligible))
				require.Equal(t, query.raw, len(raw))
			}
			// The heavy population is pure corpus weight and never a match.
			require.Equal(t, nor403BenchTotalNodes-nor403BenchCorpusNodes, nor403BenchHeavyNodes)
		})
	}
}

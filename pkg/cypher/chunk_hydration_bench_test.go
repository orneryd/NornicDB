package cypher

// Chunk-hydration qualification measurements for the reported read statements.
//
// These are benchmarks, not tests: a plain `go test` never runs them. Run them
// manually, for example:
//
//	go test ./pkg/cypher -run '^$' -bench 'BenchmarkChunkHydration' -benchmem
//
// The workload is generated, not the report's private 172K replay. The existing
// nor413 fixture (newNOR413Graph) builds documents whose current version names a
// source, each source owning chunksPerSource managed chunk vectors of 2048
// float32 values in the embedding sidecar, with one collection membership per
// document. The largest workload stores 512 MiB of source chunk vectors
// (512 documents x 128 chunks x 2048 values x 4 B = 512 MiB, 1 MiB per source).
// The control workload keeps the same 512 documents and therefore exactly the
// same returned rows with 32 MiB of stored vectors, so allocated bytes per
// operation can be compared at equal result size.
//
// Every workload runs on the fixture's native Badger engine with the query-result
// cache disabled (QueryCachePolicy{MaxEntries: 0}) and under both public parsers
// from relationshipEqualityParsers. Disabling the result cache forces execution:
// no iteration can be served cached result rows. It does not force a fresh parse
// or plan, because the executor's statement-text caches (the uppercase routing
// form and successful syntax validation) can stay warm for a statement text, so
// parse and validation work may not repeat per iteration.
//
// Timing: reported time and allocations cover the statement only. Fixture
// construction, index creation, BEGIN, ROLLBACK and the per-iteration row
// assertion all run with the timer stopped, so a StopTimer/StartTimer pair
// surrounds every assertion. Each sub-benchmark label names the one statement it
// measures, so a label never aggregates several statements. The timer switches
// and the untimed sections are still part of the run's wall time; only the
// reported ns/op and B/op exclude them. A pprof label marks the statement and
// its timer bookkeeping, so query attribution must inspect actual
// executor/storage frames. benchmarkStatementRead owns this measured read
// lifecycle.

import (
	"fmt"
	"runtime/pprof"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

// chunkHydrationWorkload is one generated stored-vector workload. Every workload
// has the reported read shape - one document, one current version naming one
// source, one source, one collection membership - so the same statements return
// the same rows in the same order for all of them; only the chunk vectors kept
// in the embedding sidecar differ.
type chunkHydrationWorkload struct {
	documents       int
	chunksPerSource int
}

// chunkHydrationWorkloads are the measured workloads. The largest one is the
// documented 512 MiB source payload; the 32 MiB workload repeats its documents
// and rows with 16 times fewer stored vectors.
func chunkHydrationWorkloads() []chunkHydrationWorkload {
	return []chunkHydrationWorkload{
		{documents: 64, chunksPerSource: 128},
		{documents: 512, chunksPerSource: 8},
		{documents: 512, chunksPerSource: 128},
	}
}

// label names the workload's size and its per-source stored vectors.
func (w chunkHydrationWorkload) label() string {
	return fmt.Sprintf("documents=%d/chunksPerSource=%d", w.documents, w.chunksPerSource)
}

// sourcePayloadMiB is the chunk-vector payload of one source.
func (w chunkHydrationWorkload) sourcePayloadMiB() float64 {
	return float64(w.chunksPerSource*nor413Dims*4) / (1 << 20)
}

// sourcesPayloadMiB is the chunk-vector payload of all the workload's sources.
func (w chunkHydrationWorkload) sourcesPayloadMiB() float64 {
	return w.sourcePayloadMiB() * float64(w.documents)
}

// chunkHydrationStatement is one measured statement with its complete expected
// result.
type chunkHydrationStatement struct {
	// name labels the statement and the transaction mode in the benchmark name.
	name string
	// query is the public statement text, executed unchanged.
	query string
	// explicit runs the statement inside BEGIN/ROLLBACK, both untimed.
	explicit bool
	// want is the complete expected result: every row and every column.
	want [][]interface{}
}

// chunkHydrationGraph builds one workload through the existing nor413 fixture
// and returns the executor the statements run through. The fixture already
// writes the chunk vectors, the collection edge and the property indexes, so
// this helper adds no storage or sidecar lifecycle of its own; it only checks
// that the stored payload is the one the workload label claims and disables the
// query-result cache.
func chunkHydrationGraph(b testing.TB, workload chunkHydrationWorkload) (*nor413Graph, *StorageExecutor) {
	b.Helper()
	g := newNOR413Graph(b, workload.documents, workload.chunksPerSource)
	// One chunk joins each document's own record, one the collection, and
	// chunksPerSource each source; every version stores none.
	stored := uint64(1+workload.documents*(1+workload.chunksPerSource)) * nor413Dims * 4
	require.Equal(b, stored, g.vectorBytes, "fixture stored chunk-vector payload")
	b.Logf("%s: %.4f MiB per source, %.0f MiB over %d sources, %.1f MiB stored in total",
		workload.label(), workload.sourcePayloadMiB(), workload.sourcesPayloadMiB(), workload.documents,
		float64(g.vectorBytes)/(1<<20))
	return g, NewStorageExecutor(g.store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
}

// chunkHydrationLabels names one measured statement's CPU profile samples. The
// workload label keeps each population's samples separate, and the statement
// name already carries the transaction mode. Labels include timer bookkeeping,
// so query attribution must inspect actual executor/storage frames.
func chunkHydrationLabels(parser, statement string, workload chunkHydrationWorkload) pprof.LabelSet {
	return pprof.Labels("phase", "chunk-hydration-query", "parser", parser,
		"statement", statement, "workload", workload.label())
}

// BenchmarkChunkHydrationCallChainReads measures the reported read chain with a
// read-only CALL subquery (nor413CallSubquery) and with the bundled READ
// procedure (nor413ReadProcedure), in auto-commit and inside an explicit
// transaction (BEGIN, statement, ROLLBACK). Both statements return one row per
// document - the source id, the source title and the CALL's own column - and
// every iteration asserts that complete expected result, so a statement that
// hydrates less by returning fewer or different rows fails instead of
// measuring faster.
func BenchmarkChunkHydrationCallChainReads(b *testing.B) {
	for _, workload := range chunkHydrationWorkloads() {
		b.Run(workload.label(), func(b *testing.B) {
			for _, parser := range relationshipEqualityParsers {
				b.Run(parser.name, func(b *testing.B) {
					b.Cleanup(parser.setup())
					g, exec := chunkHydrationGraph(b, workload)
					params := []map[string]interface{}{{"ids": g.ids}}
					collection := make([][]interface{}, len(g.ids))
					procedure := make([][]interface{}, len(g.ids))
					for i := range g.ids {
						source := fmt.Sprintf("src-%d", i)
						title := "source " + source
						collection[i] = []interface{}{source, title, []interface{}{"col-0"}}
						procedure[i] = []interface{}{source, title, true}
					}
					for _, statement := range []chunkHydrationStatement{
						{name: "autocommit-call-subquery", query: nor413CallSubquery, explicit: false, want: collection},
						{name: "explicit-call-subquery", query: nor413CallSubquery, explicit: true, want: collection},
						{name: "autocommit-read-procedure", query: nor413ReadProcedure, explicit: false, want: procedure},
						{name: "explicit-read-procedure", query: nor413ReadProcedure, explicit: true, want: procedure},
					} {
						b.Run(statement.name, func(b *testing.B) {
							benchmarkStatementRead(b, exec, statement.query, params, statement.explicit,
								chunkHydrationLabels(parser.name, statement.name, workload),
								func(result *ExecuteResult) {
									require.Equal(b, statement.want, result.Rows, "statement %q returned different rows", statement.query)
								})
						})
					}
				})
			}
		})
	}
}

const (
	// chunkHydrationFulltextIndex indexes the one searchable property the nor413
	// fixture gives a source. Index creation is setup, outside every timer.
	chunkHydrationFulltextIndex = "CREATE FULLTEXT INDEX chunk_hydration_source_title FOR (n:Source) ON EACH [n.title]"

	// chunkHydrationFulltextLimit is the procedure's own 25-result option.
	chunkHydrationFulltextLimit = 25

	// chunkHydrationFulltextQuery is the reported fulltext read: the query is a
	// parameter, the result count is the procedure's own option, and the
	// projection is metadata only (node.id, node.title), so no returned value
	// needs the node's stored chunk vectors.
	chunkHydrationFulltextQuery = "CALL db.index.fulltext.queryNodes('chunk_hydration_source_title', $query, {limit: 25}) YIELD node, score RETURN node.id AS id, node.title AS title"
)

// chunkHydrationFulltextRows is the complete expected result of
// chunkHydrationFulltextQuery. Each source title has two tokens and contains
// both alternated terms exactly once, so term frequency, document frequency,
// body length and average length are uniform: every source scores identically
// under either term, and the procedure orders equal scores by document id
// ascending. The expected rows are therefore the smallest source ids in byte
// order, each with its title - the identity and order of the returned rows, not
// only their count.
func chunkHydrationFulltextRows(g *nor413Graph, limit int) [][]interface{} {
	ids := make([]string, len(g.ids))
	for i := range ids {
		ids[i] = fmt.Sprintf("src-%d", i)
	}
	sort.Strings(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	rows := make([][]interface{}, len(ids))
	for i, id := range ids {
		rows[i] = []interface{}{id, "source " + id}
	}
	return rows
}

// BenchmarkChunkHydrationFulltextQueryNodes measures the fulltext read over the
// sources' stored vectors: db.index.fulltext.queryNodes with the query as a
// parameter, the procedure's 25-result option and a metadata-only projection.
// The read is qualified in auto-commit and inside an explicit transaction
// (BEGIN, statement, ROLLBACK), because the admitted lifecycle differs between
// them. The alternated queries are distinct term sets that match the same
// sources with the same scores, so a term-frequency or statistics shortcut
// cannot serve one of them from the other while the expected rows stay
// identical, and the query cache is off. The warm-up reads below acquire the
// retained corpus outside the measurement and their rows are asserted as well;
// they also rotate every term set before the first measured iteration.
func BenchmarkChunkHydrationFulltextQueryNodes(b *testing.B) {
	for _, workload := range chunkHydrationWorkloads() {
		b.Run(workload.label(), func(b *testing.B) {
			for _, parser := range relationshipEqualityParsers {
				b.Run(parser.name, func(b *testing.B) {
					b.Cleanup(parser.setup())
					g, exec := chunkHydrationGraph(b, workload)
					service := search.NewService(g.store, search.DefaultVectorDimensions, nil)
					b.Cleanup(func() { require.NoError(b, service.Close()) })
					exec.SetSearchService(service)
					_, err := exec.Execute(b.Context(), chunkHydrationFulltextIndex, nil)
					require.NoError(b, err)
					want := chunkHydrationFulltextRows(g, chunkHydrationFulltextLimit)
					require.Len(b, want, chunkHydrationFulltextLimit, "the workload exceeds the 25-result option")
					terms := []map[string]interface{}{{"query": "source"}, {"query": "src"}}
					for _, params := range terms {
						warm, err := exec.Execute(b.Context(), chunkHydrationFulltextQuery, params)
						require.NoError(b, err)
						require.Equal(b, want, warm.Rows, "warm-up fulltext read returned different rows")
					}
					for _, statement := range []chunkHydrationStatement{
						{name: "autocommit-fulltext-limit25", query: chunkHydrationFulltextQuery, explicit: false, want: want},
						{name: "explicit-fulltext-limit25", query: chunkHydrationFulltextQuery, explicit: true, want: want},
					} {
						b.Run(statement.name, func(b *testing.B) {
							benchmarkStatementRead(b, exec, statement.query, terms, statement.explicit,
								chunkHydrationLabels(parser.name, statement.name, workload),
								func(result *ExecuteResult) {
									require.Equal(b, statement.want, result.Rows, "statement %q returned different rows", statement.query)
								})
						})
					}
					allRows := chunkHydrationFulltextRows(g, len(g.ids))
					for _, unranked := range []struct {
						name, term  string
						skip, limit int
						want        [][]interface{}
					}{
						{"wildcard-page", "*", 5, 25, allRows[5:30]},
						{"presence-page", "title:*", 5, 25, allRows[5:30]},
						{"wildcard-unlimited", "*", 0, -1, allRows},
					} {
						for _, explicit := range []bool{false, true} {
							name := fmt.Sprintf("unranked-%s/explicit=%t", unranked.name, explicit)
							b.Run(name, func(b *testing.B) {
								const query = "CALL db.index.fulltext.queryNodes('chunk_hydration_source_title', $query, $options) YIELD node, score RETURN node.id AS id, node.title AS title"
								options := map[string]interface{}{"skip": int64(unranked.skip)}
								if unranked.limit >= 0 {
									options["limit"] = int64(unranked.limit)
								}
								params := []map[string]interface{}{{"query": unranked.term, "options": options}}
								benchmarkStatementRead(b, exec, query, params, explicit,
									chunkHydrationLabels(parser.name, name, workload),
									func(result *ExecuteResult) { require.Equal(b, unranked.want, result.Rows) })
							})
						}
					}
				})
			}
		})
	}
}

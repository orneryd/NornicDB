package cypher

// NOR-414 seed-shape A and B benchmarks: manual, optional qualification of the
// reported shapes A and B. Both shapes are qualified together, per parser, on
// the exact populations built by newTraversalLabShapeAFixture and
// newTraversalLabShapeBFixture. Nothing here runs under the unit-test suite.
//
// Every measured read is a public Execute call inside a fresh explicit
// transaction. Fixture construction, constraint installation, BEGIN and
// ROLLBACK stay outside the timer, so only the query's full result
// consumption is timed. Every iteration asserts the complete expected row
// population before the transaction is rolled back, ReportAllocs is on, and a
// pprof label marks the query and its timer bookkeeping; query attribution
// must inspect actual executor/storage frames. benchmarkStatementRead owns that
// measured read lifecycle for this file and the other native read benchmarks.
// The sub-benchmark names and the profile labels record the fixture history
// (sequential CreateNode/CreateEdge) and configuration (in-memory native Badger
// behind personal_documents).
//
// Run both shapes on both parsers, several reads per measurement:
//
//	go test ./pkg/cypher -run '^$' -bench '^BenchmarkTraversalLabSeedShape' -benchtime 3x -count 1 -timeout 120m
//
// The sub-benchmark identifier has five slash-separated elements (history,
// configuration, parser, statement, explicit), and each -bench element matches
// one of them, so a single read can be selected with per-element patterns, for
// example:
//
//	go test ./pkg/cypher -run '^$' -bench '^BenchmarkTraversalLabSeedShapeBKeyedLookups/history=.*/configuration=.*/parser=antlr/statement=content-document' -benchtime 3x
//
// The fixtures build 172,000 nodes and 290,000/290,001 relationships through
// sequential native writes, so expect minutes of setup per shape even before
// the measured reads, and expect the whole set to need a long -timeout. The
// forward control is the report's original composition (collect over a CASE
// expression, a comprehension index and any/all). Its complete result must
// pass unchanged; a rejected composition is a product failure, not a skip.

import (
	"context"
	"runtime/pprof"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkTraversalLabSeedShapeAControls times the reported shape A statements: the
// inverse traversal that starts at the one collection and the original forward
// control that starts at the matched documents.
func BenchmarkTraversalLabSeedShapeAControls(b *testing.B) {
	fixture := newTraversalLabShapeAFixture(b)
	benchmarkTraversalLabSeedStatements(b, fixture,
		traversalLabShapeAInverseStatement(),
		traversalLabShapeAForwardStatement(),
	)
}

// BenchmarkTraversalLabSeedShapeBKeyedLookups times the reported shape B statements:
// COLLECTION_DOCUMENTS and CONTENT_DOCUMENT, both selected by inline unique
// keys without a WHERE clause.
func BenchmarkTraversalLabSeedShapeBKeyedLookups(b *testing.B) {
	fixture := newTraversalLabShapeBFixture(b)
	benchmarkTraversalLabSeedStatements(b, fixture,
		traversalLabShapeBCollectionDocumentsStatement(),
		traversalLabShapeBContentDocumentStatement(),
	)
}

// benchmarkTraversalLabSeedStatements runs every reported statement of one shape on
// each statement parser through its own executor, so neither parser reuses the
// other's cached plan.
func benchmarkTraversalLabSeedStatements(b *testing.B, fixture *traversalLabSeedFixture, statements ...traversalLabStatement) {
	b.Helper()
	for _, parser := range relationshipEqualityParsers {
		b.Run("history=sequential-create-node-create-edge/configuration=badger-in-memory+personal_documents/parser="+parser.name, func(b *testing.B) {
			b.Cleanup(parser.setup())
			exec := fixture.executor()
			for _, statement := range statements {
				b.Run("statement="+statement.name+"/explicit=true", func(b *testing.B) {
					benchmarkTraversalLabSeedStatement(b, exec, parser.name, statement)
				})
			}
		})
	}
}

// benchmarkTraversalLabSeedStatement times one reported statement and asserts its
// complete expected row population on every measured read.
func benchmarkTraversalLabSeedStatement(b *testing.B, exec *StorageExecutor, parser string, statement traversalLabStatement) {
	b.Helper()
	benchmarkStatementRead(b, exec, statement.query, []map[string]interface{}{statement.params}, true,
		pprof.Labels("phase", "traversalLab-seed-shape-query", "shape", statement.shape,
			"statement", statement.name, "parser", parser,
			"history", "sequential-create-node-create-edge",
			"configuration", "badger-in-memory+personal_documents"),
		func(result *ExecuteResult) { requireTraversalLabSeedRows(b, result, statement.expected) })
}

// benchmarkStatementRead owns the measured public read and untimed transaction
// lifecycle for the native read benchmarks. It executes query once per
// iteration, rotating the parameter sets in order so every set reaches
// execution, and asserts the complete expected result with the timer stopped.
//
// An explicit read gets BEGIN and ROLLBACK around the measured statement, both
// untimed. The labels cover the timed execution and the assertion that follows
// it, so CPU labels include timer bookkeeping and query attribution must
// inspect actual executor/storage frames. The benchmark context is the request
// lifetime, so a canceled benchmark ends the reads. Callers construct the
// query, parameter sets, labels and assertion before calling, so ResetTimer
// excludes that construction.
func benchmarkStatementRead(b *testing.B, exec *StorageExecutor, query string, params []map[string]interface{}, explicit bool, labels pprof.LabelSet, assertResult func(*ExecuteResult)) {
	b.Helper()
	require.NotEmpty(b, params, "statement has at least one parameter set")
	ctx := b.Context()
	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if explicit {
			b.StopTimer()
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(b, err)
		}
		pprof.Do(ctx, labels, func(queryCtx context.Context) {
			b.StartTimer()
			result, err := exec.Execute(queryCtx, query, params[iteration%len(params)])
			b.StopTimer()
			require.NoError(b, err)
			assertResult(result)
		})
		if explicit {
			_, err := exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(b, err)
		}
	}
}

// requireTraversalLabSeedRows asserts that a read returned exactly one row per
// expected ID, each row matching the expected row with no duplicated or
// missing ID. It deliberately asserts no row order: the reported statements
// carry no ORDER BY.
func requireTraversalLabSeedRows(tb testing.TB, result *ExecuteResult, expected map[string][]interface{}) {
	tb.Helper()
	require.NotNil(tb, result)
	require.Len(tb, result.Rows, len(expected), "result row population")
	seen := make(map[string]struct{}, len(result.Rows))
	for _, row := range result.Rows {
		require.NotEmpty(tb, row, "result row")
		id, ok := row[0].(string)
		require.True(tb, ok, "result row ID column %v", row[0])
		want, known := expected[id]
		require.True(tb, known, "unexpected result row ID %q", id)
		require.Equal(tb, want, row, "result row %q", id)
		_, duplicate := seen[id]
		require.False(tb, duplicate, "duplicate result row ID %q", id)
		seen[id] = struct{}{}
	}
}

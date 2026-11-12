package cypher

// NOR-160 item 14 probe: the public typed one-hop step lists a start node's
// whole adjacency and discards the relationships of another type afterwards.
//
// The reported workload is a PD document population where each document owns
// one MEMBER_OF relationship and many PD_RELATION relationships. Its typed
// step is
//
//	MATCH (d:PDDocument {id: id}) OPTIONAL MATCH (d)-[:MEMBER_OF]->(c:PDCollection)
//
// The traversal needs each relationship only for its ID, type and endpoints, so
// it lists them through storage.EdgeHeaderReader. That interface has no typed
// form: the engine lists every relationship of the node, decoding one adjacency
// header per entry (BadgerEngine.edgeHeadersCommitted for the current view,
// BadgerTransaction.readCachedSnapshotAdjacentEdgesLocked headersOnly for a
// pinned snapshot), and the traversal discards the ones whose type is not in
// the pattern afterwards (TraversalContext.relTypes). A document with 48
// PD_RELATION relationships therefore pays for 49 headers on every typed step
// to use one.
//
// These probes assert the typed request's required result - the step reads only
// the relationships the pattern's type can match - so they FAIL against the
// current source and pass once the canonical adjacency owner answers a typed
// adjacent read. They observe the public Execute path only:
//
//   - listings: adjacencyReadSpy (relationship_id_pinned_edge_test.go) counts
//     every relationship the executor's storage lists for the traversal, so
//     the count is the number of adjacency headers the engine decoded for the
//     statement.
//   - allocations: testing.AllocsPerRun over the same statement with a small
//     and a large unrelated fanout. Decoding an unrelated type's adjacency
//     header allocates the decoded relationship and its type string, so today
//     the allocation count grows with the unrelated fanout alone. The same
//     measurement runs inside an explicit transaction, where the traversal
//     reads the pinned snapshot owner instead. No ns/walltime assertion is made.
//
// The probe executor's query result cache is disabled (as elsewhere in this
// package) so every measured execution reads adjacency instead of answering a
// previous execution's rows.
//
// BenchmarkNOR160TypedAdjacencyStep is the reusable fixture for the same
// statement: one committed population, one typed step per iteration, so a
// before/after comparison measures the typed read and nothing else.

import (
	"context"
	"fmt"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor160TypedAdjacencyStatement is the reported statement. Every document owns
// one MEMBER_OF relationship and unrelatedFanout PD_RELATION relationships; the
// traversal's relationship is anonymous and no path variable exposes it, so the
// step lists relationship headers.
const nor160TypedAdjacencyStatement = `UNWIND $ids AS id MATCH (d:PDDocument {id: id}) OPTIONAL MATCH (d)-[:MEMBER_OF]->(c:PDCollection) RETURN d.id AS id, c.archived AS archived`

// nor160TypedAdjacencyPopulation is one committed probe population.
type nor160TypedAdjacencyPopulation struct {
	// documents is the number of PDDocument nodes, each with its own
	// PDCollection reached by one MEMBER_OF relationship.
	documents int
	// unrelatedFanout is the number of PD_RELATION relationships pointing from
	// each document to its own PDOther nodes.
	unrelatedFanout int
	// documentsWithoutMembership is how many of the documents (the last ones)
	// own no MEMBER_OF relationship, so the OPTIONAL MATCH must still emit
	// their row with no collection.
	documentsWithoutMembership int
}

// memberships is the number of MEMBER_OF relationships the population holds:
// exactly the relationships the typed step must read.
func (p nor160TypedAdjacencyPopulation) memberships() int {
	return p.documents - p.documentsWithoutMembership
}

// relationships is the number of outgoing relationships the documents own.
func (p nor160TypedAdjacencyPopulation) relationships() int {
	return p.memberships() + p.documents*p.unrelatedFanout
}

// nor160TypedAdjacencyFixture is a committed population behind an
// adjacencyReadSpy, with an executor that reads through the spy.
type nor160TypedAdjacencyFixture struct {
	population nor160TypedAdjacencyPopulation
	spy        *adjacencyReadSpy
	exec       *StorageExecutor
	// writer reads the same committed store without the spy, for population
	// facts the probe must not count as traversal listings.
	writer *StorageExecutor
	params map[string]interface{}
}

// newNOR160TypedAdjacencyFixture commits the population through the same
// namespaced engine the read executor observes, so index and adjacency
// maintenance are the production ones.
func newNOR160TypedAdjacencyFixture(tb testing.TB, population nor160TypedAdjacencyPopulation) *nor160TypedAdjacencyFixture {
	tb.Helper()
	ctx := context.Background()
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](tb, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = base.Close() })
	namespaced := storage.NewNamespacedEngine(base, "nor160typed")
	writer := NewStorageExecutor(namespaced)
	spy := &adjacencyReadSpy{Engine: namespaced}
	exec := NewStorageExecutor(spy)
	// A cached result would answer the second execution of the same statement
	// with rows instead of a typed step.
	exec.cache = nil

	params := map[string]interface{}{}
	ids := make([]interface{}, 0, population.documents)
	for document := 1; document <= population.documents; document++ {
		ids = append(ids, int64(document))
		_, err := writer.Execute(ctx, "CREATE (:PDDocument {id: $id})", map[string]interface{}{"id": int64(document)})
		require.NoError(tb, err)
		if document <= population.memberships() {
			_, err = writer.Execute(ctx,
				"MATCH (d:PDDocument {id: $id}) CREATE (d)-[:MEMBER_OF]->(:PDCollection {archived: $archived})",
				map[string]interface{}{"id": int64(document), "archived": document%2 == 0})
			require.NoError(tb, err)
		}
		if population.unrelatedFanout > 0 {
			_, err = writer.Execute(ctx,
				"MATCH (d:PDDocument {id: $id}) UNWIND range(0, $fanout - 1) AS i CREATE (d)-[:PD_RELATION]->(:PDOther {n: i})",
				map[string]interface{}{"id": int64(document), "fanout": int64(population.unrelatedFanout)})
			require.NoError(tb, err)
		}
	}
	params["ids"] = ids

	fixture := &nor160TypedAdjacencyFixture{population: population, spy: spy, exec: exec, writer: writer, params: params}
	require.ElementsMatch(tb, fixture.expectedRows(), fixture.rows(tb, ctx), "the probe population must answer the reported statement")
	require.Equal(tb, fixture.expectedOutgoingTypes(), fixture.outgoingTypes(tb, ctx), "the probe population must hold the committed relationship types")
	return fixture
}

// rows executes the reported statement through the spy and returns its rows.
func (f *nor160TypedAdjacencyFixture) rows(tb testing.TB, ctx context.Context) [][]interface{} {
	tb.Helper()
	result, err := f.exec.Execute(ctx, nor160TypedAdjacencyStatement, f.params)
	require.NoError(tb, err)
	return result.Rows
}

// expectedRows is the statement's exact row semantics: one row per unwound
// document, the collection's archived flag where one MEMBER_OF relationship
// exists, and no collection (nil) where the document owns none.
func (f *nor160TypedAdjacencyFixture) expectedRows() [][]interface{} {
	out := make([][]interface{}, 0, f.population.documents)
	for document := 1; document <= f.population.documents; document++ {
		var archived interface{}
		if document <= f.population.memberships() {
			archived = document%2 == 0
		}
		out = append(out, []interface{}{int64(document), archived})
	}
	return out
}

// outgoingTypes counts every committed document relationship by type through an
// executor the probe does not measure, so the population's typed subset is a
// fact about the store rather than the probe's own listing count.
func (f *nor160TypedAdjacencyFixture) outgoingTypes(tb testing.TB, ctx context.Context) map[string]int64 {
	tb.Helper()
	result, err := f.writer.Execute(ctx, "MATCH (d:PDDocument)-[r]->() RETURN type(r) AS kind, count(r) AS held", nil)
	require.NoError(tb, err)
	held := make(map[string]int64, len(result.Rows))
	for _, row := range result.Rows {
		kind, ok := row[0].(string)
		require.True(tb, ok, "type(r) must be a string: %#v", row)
		count, ok := row[1].(int64)
		require.True(tb, ok, "count(r) must be an int64: %#v", row)
		held[kind] = count
	}
	return held
}

// expectedOutgoingTypes is the population's committed relationship-type
// population.
func (f *nor160TypedAdjacencyFixture) expectedOutgoingTypes() map[string]int64 {
	held := map[string]int64{}
	if unrelated := f.population.documents * f.population.unrelatedFanout; unrelated > 0 {
		held["PD_RELATION"] = int64(unrelated)
	}
	if memberships := f.population.memberships(); memberships > 0 {
		held["MEMBER_OF"] = int64(memberships)
	}
	return held
}

// typedStepListings executes the reported statement once and returns how many
// relationships the traversal listed and how many adjacency listings it made.
func (f *nor160TypedAdjacencyFixture) typedStepListings(tb testing.TB, ctx context.Context) (entryListings, listings int) {
	tb.Helper()
	f.spy.reset()
	require.ElementsMatch(tb, f.expectedRows(), f.rows(tb, ctx))
	return f.spy.entries, f.spy.calls
}

// TestNOR160TypedAdjacencyStepReadsOnlyMatchingType is the reproduction: the
// typed step must list the relationships its type can match, not the start
// node's whole adjacency. A document owning 48 PD_RELATION relationships and
// one MEMBER_OF relationship currently lists 49 headers for every document that
// has a membership, and 48 for one that has none.
func TestNOR160TypedAdjacencyStepReadsOnlyMatchingType(t *testing.T) {
	const unrelatedFanout = 48
	fixture := newNOR160TypedAdjacencyFixture(t, nor160TypedAdjacencyPopulation{
		documents:                  3,
		unrelatedFanout:            unrelatedFanout,
		documentsWithoutMembership: 1,
	})
	ctx := context.Background()

	entries, listings := fixture.typedStepListings(t, ctx)
	t.Logf("typed step over %d documents with %d unrelated relationships each: %d listing(s), %d relationship(s) listed",
		fixture.population.documents, unrelatedFanout, listings, entries)

	require.Equal(t, fixture.population.documents, listings,
		"each document's typed step lists its adjacency once")
	require.Equal(t, fixture.population.memberships(), entries,
		"the typed step must list only the %d MEMBER_OF relationship(s); it listed %d of the %d relationships the documents own",
		fixture.population.memberships(), entries, fixture.population.relationships())
}

// TestNOR160TypedAdjacencyStepAllocationIgnoresUnrelatedDegree measures the
// same statement over the same documents with a small and a large unrelated
// fanout. Decoding an unrelated relationship's adjacency header allocates it,
// so with an untyped listing the allocation count is a function of the
// unrelated fanout alone; with a typed listing it is not.
func TestNOR160TypedAdjacencyStepAllocationIgnoresUnrelatedDegree(t *testing.T) {
	const documents, lowFanout, highFanout = 2, 8, 64
	low := newNOR160TypedAdjacencyFixture(t, nor160TypedAdjacencyPopulation{documents: documents, unrelatedFanout: lowFanout})
	high := newNOR160TypedAdjacencyFixture(t, nor160TypedAdjacencyPopulation{documents: documents, unrelatedFanout: highFanout})
	ctx := context.Background()

	measure := func(fixture *nor160TypedAdjacencyFixture) float64 {
		return testing.AllocsPerRun(20, func() {
			fixture.spy.reset()
			rows, err := fixture.exec.Execute(ctx, nor160TypedAdjacencyStatement, fixture.params)
			if err != nil {
				t.Fatalf("statement: %v", err)
			}
			if len(rows.Rows) != documents {
				t.Fatalf("rows: %d, want %d", len(rows.Rows), documents)
			}
		})
	}
	lowAllocs, highAllocs := measure(low), measure(high)
	t.Logf("autocommit typed step allocations: %d documents, fanout %d = %v allocs/run, fanout %d = %v allocs/run (delta %v for %d extra unrelated relationships per document)",
		documents, lowFanout, lowAllocs, highFanout, highAllocs, highAllocs-lowAllocs, highFanout-lowFanout)

	// The unrelated fanout must not reach the typed step's cost. A relationship
	// the step reads still costs its own decoding; the bound leaves room for
	// that and for the statement's own per-row work, not for (highFanout -
	// lowFanout) * documents headers of another type.
	require.LessOrEqual(t, highAllocs-lowAllocs, float64(16),
		"allocation count grew by %v allocations for %d extra unrelated relationships per document (%v vs %v allocs/run): the typed step decodes the relationships it discards",
		highAllocs-lowAllocs, highFanout-lowFanout, highAllocs, lowAllocs)
}

// TestNOR160TypedAdjacencyStepTransactionAllocationIgnoresUnrelatedDegree is the
// same measurement inside an explicit transaction: the traversal then reads the
// transaction's pinned snapshot owner, which lists every relationship visible at
// the read version before the type filter runs. A fresh transaction per
// measurement keeps the transaction's own adjacency cache from answering a
// later execution.
//
// The snapshot owner confirms each relationship it lists against the
// relationship's MVCC head (snapshotHeaderLocked), so a bounded per-relationship
// cost remains admissible; the bound below is four allocations per unrelated
// relationship, well under the twelve the current listing spends on each.
func TestNOR160TypedAdjacencyStepTransactionAllocationIgnoresUnrelatedDegree(t *testing.T) {
	const documents, lowFanout, highFanout = 2, 8, 64
	low := newNOR160TypedAdjacencyFixture(t, nor160TypedAdjacencyPopulation{documents: documents, unrelatedFanout: lowFanout})
	high := newNOR160TypedAdjacencyFixture(t, nor160TypedAdjacencyPopulation{documents: documents, unrelatedFanout: highFanout})
	ctx := context.Background()

	measure := func(fixture *nor160TypedAdjacencyFixture) float64 {
		return testing.AllocsPerRun(20, func() {
			if _, err := fixture.exec.Execute(ctx, "BEGIN", nil); err != nil {
				t.Fatalf("begin: %v", err)
			}
			fixture.spy.reset()
			rows, err := fixture.exec.Execute(ctx, nor160TypedAdjacencyStatement, fixture.params)
			finished := len(rows.Rows) == documents
			if _, rollbackErr := fixture.exec.Execute(ctx, "ROLLBACK", nil); rollbackErr != nil {
				t.Fatalf("rollback: %v", rollbackErr)
			}
			if err != nil {
				t.Fatalf("statement: %v", err)
			}
			if !finished {
				t.Fatalf("rows: %d, want %d", len(rows.Rows), documents)
			}
		})
	}
	lowAllocs, highAllocs := measure(low), measure(high)
	extra := documents * (highFanout - lowFanout)
	t.Logf("explicit-transaction typed step allocations: %d documents, fanout %d = %v allocs/run, fanout %d = %v allocs/run (delta %v for %d extra unrelated relationships)",
		documents, lowFanout, lowAllocs, highFanout, highAllocs, highAllocs-lowAllocs, extra)

	require.LessOrEqual(t, highAllocs-lowAllocs, float64(4*extra),
		"snapshot typed step allocation count grew by %v allocations for %d unrelated relationships (%v allocs each; %v vs %v allocs/run): the snapshot owner decodes the relationships the pattern discards",
		highAllocs-lowAllocs, extra, (highAllocs-lowAllocs)/float64(extra), highAllocs, lowAllocs)
}

// BenchmarkNOR160TypedAdjacencyStep is the reusable fixture for the typed step:
// the same committed population, the same reported statement, one step per
// iteration. The membership-only shape is the same step over documents that own
// no unrelated relationship, so the difference between shapes is the cost of
// the relationships the pattern's type cannot match.
func BenchmarkNOR160TypedAdjacencyStep(b *testing.B) {
	for _, shape := range []struct {
		name       string
		population nor160TypedAdjacencyPopulation
	}{
		{name: "membership-only", population: nor160TypedAdjacencyPopulation{documents: 8}},
		{name: "unrelated-64", population: nor160TypedAdjacencyPopulation{documents: 8, unrelatedFanout: 64}},
		{name: "unrelated-400", population: nor160TypedAdjacencyPopulation{documents: 8, unrelatedFanout: 400}},
	} {
		b.Run(shape.name, func(b *testing.B) {
			for _, explicit := range []bool{false, true} {
				b.Run(fmt.Sprintf("explicit-%v", explicit), func(b *testing.B) {
					fixture := newNOR160TypedAdjacencyFixture(b, shape.population)
					ctx := context.Background()
					b.ReportAllocs()
					b.ResetTimer()
					for iteration := 0; iteration < b.N; iteration++ {
						if explicit {
							if _, err := fixture.exec.Execute(ctx, "BEGIN", nil); err != nil {
								b.Fatal(err)
							}
						}
						rows, err := fixture.exec.Execute(ctx, nor160TypedAdjacencyStatement, fixture.params)
						if err != nil {
							b.Fatal(err)
						}
						if len(rows.Rows) != shape.population.documents {
							b.Fatalf("rows: %d, want %d", len(rows.Rows), shape.population.documents)
						}
						if explicit {
							if _, err := fixture.exec.Execute(ctx, "ROLLBACK", nil); err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			}
		})
	}
}

func TestTypedAdjacencyPublicParserModes(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("transaction-%t", explicit), func(t *testing.T) {
				f := newNOR160TypedAdjacencyFixture(t, nor160TypedAdjacencyPopulation{documents: 3, unrelatedFanout: 8, documentsWithoutMembership: 1})
				ctx := t.Context()
				if explicit {
					_, err := f.exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer func() { _, err := f.exec.Execute(ctx, "ROLLBACK", nil); require.NoError(t, err) }()
				}
				require.ElementsMatch(t, f.expectedRows(), f.rows(t, ctx))
			})
		}
	})
}

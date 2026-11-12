package cypher

// NOR-414 residual: one MATCH operator reads its seed estimates once for its
// whole invocation and reuses them for that invocation's input rows. These
// cases are the controls around that reuse:
//
//   - the reused answers keep the exact request identity (counter family, name,
//     ordered type set) they answered, so two endpoint populations and two
//     positional counters never share one answer;
//   - nothing is carried from one statement to the next: each invocation reads
//     its own statistics, so a later statement's label or type set is read and
//     not reused;
//   - the materialized seeded owner (pipelineApplySeededTraversalMatch) creates
//     the memo itself when no streaming source supplies one, and still reads
//     its populations once for a write statement's input rows;
//   - a reused answer still observes cancellation,
//   - and a failed read is never remembered.
//
// Counts guide orientation only: every case below asserts the rows as well, so
// a reused estimate never becomes an empty proof or a count(*) answer.
// Fixtures and spies are the NOR-414 seed cost owners
// (traversal_seed_cost_test.go, traversal_seed_cost_typed_test.go).

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// traversalSeedReuseStatement is the reported lookup: 12 indexed documents,
// each reached by one input row, walking to one keyed collection.
const traversalSeedReuseStatement = "UNWIND $ids AS each WITH DISTINCT each MATCH (d:PDDocument {id: each})-[:MEMBER_OF]->(:PDCollection {id: 'col-0'}) RETURN d.id AS id"

// traversalSeedReuseWriteStatement adds a write after the MATCH, so the MATCH
// has no read-only tail: the statement runs it over materialized input rows
// through pipelineApplySeededTraversalMatch instead of the streaming source.
const traversalSeedReuseWriteStatement = "UNWIND $ids AS each WITH DISTINCT each MATCH (d:PDDocument {id: each})-[:MEMBER_OF]->(:PDCollection {id: 'col-0'}) SET d.seen = true RETURN d.id AS id"

// traversalSeedReusePopulation is the reported shape: one collection, 12
// documents indexed on id, every document also carrying a large unrelated
// outgoing adjacency.
func traversalSeedReusePopulation() traversalSeedCostPopulation {
	return traversalSeedCostPopulation{collections: 1, documents: 12, unrelatedFanout: 64}
}

// traversalSeedReuseRows is the one projected column of the reported lookup:
// each input row reads its own document id.
func traversalSeedReuseRows(ids []string) [][]interface{} {
	rows := make([][]interface{}, len(ids))
	for index, id := range ids {
		rows[index] = []interface{}{id}
	}
	return rows
}

// requireSeedRequestOnce requires exactly one read of want's counters: the
// invocation reused its answer for the remaining input rows without letting a
// different request's answer satisfy it.
func requireSeedRequestOnce(t *testing.T, requests []storage.CountStoreReadRequest, want storage.CountStoreReadRequest) {
	t.Helper()
	reads := 0
	for _, request := range requests {
		if reflect.DeepEqual(request, want) {
			reads++
		}
	}
	require.Equal(t, 1, reads, "count-store requests: %v", requests)
}

// TestTraversalSeedStatisticsAreReadAgainForTheNextMatch: the memo belongs to
// the MATCH invocation that created it, so the next statement reads its own
// statistics. Its changed type set is asked for, and the unchanged label
// populations are read again rather than reused from the earlier statement.
func TestTraversalSeedStatisticsAreReadAgainForTheNextMatch(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := newTraversalSeedCostFixture(t, traversalSeedReusePopulation())
		counter := &seedCountStoreSpy{Engine: fixture.spy}
		exec := NewStorageExecutor(counter, QueryCachePolicy{MaxEntries: 0})

		first, err := exec.Execute(t.Context(), traversalSeedReuseStatement, fixture.idParams())
		require.NoError(t, err)
		require.Equal(t, seedRowKeys(traversalSeedReuseRows(fixture.documents)), seedRowKeys(first.Rows))
		require.NotEmpty(t, counter.requests, "the first statement reads its own statistics")

		counter.requests = nil
		typed, err := exec.Execute(t.Context(),
			"UNWIND $ids AS each WITH DISTINCT each MATCH (d:PDDocument {id: each})-[:MEMBER_OF|ARCHIVED]->(:PDCollection {id: 'col-0'}) RETURN d.id AS id",
			fixture.idParams())
		require.NoError(t, err)
		require.Equal(t, seedRowKeys(traversalSeedReuseRows(fixture.documents)), seedRowKeys(typed.Rows))
		require.NotEmpty(t, counter.requests, "a later statement reads its own statistics")
		require.LessOrEqual(t, len(counter.requests), 4, "count-store requests: %v", counter.requests)
		requireSeedRequestOnce(t, counter.requests, storage.CountStoreReadRequest{
			Kind: storage.CountStoreNodesByLabel, Name: "PDDocument",
		})
		requireSeedRequestOnce(t, counter.requests, storage.CountStoreReadRequest{
			Kind: storage.CountStoreNodesByLabel, Name: "PDCollection",
		})
		requireSeedRequestOnce(t, counter.requests, storage.CountStoreReadRequest{
			Kind: storage.CountStoreRelationshipsByStartLabel, Name: "PDDocument", Types: []string{"MEMBER_OF", "ARCHIVED"},
		})
		requireSeedRequestOnce(t, counter.requests, storage.CountStoreReadRequest{
			Kind: storage.CountStoreRelationshipsByEndLabel, Name: "PDCollection", Types: []string{"MEMBER_OF", "ARCHIVED"},
		})
	})
}

// TestMaterializedSeededTraversalReusesItsOwnStatistics covers the second
// owner: a write tail leaves no read-only tail, so the MATCH runs over
// materialized input rows and pipelineApplySeededTraversalMatch creates the
// memo before its row loop. The 12 rows still read each population once, and
// the write itself stays per row.
func TestMaterializedSeededTraversalReusesItsOwnStatistics(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := newTraversalSeedCostFixture(t, traversalSeedReusePopulation())
		counter := &seedCountStoreSpy{Engine: fixture.spy}
		exec := NewStorageExecutor(counter, QueryCachePolicy{MaxEntries: 0})

		result, err := exec.Execute(t.Context(), traversalSeedReuseWriteStatement, fixture.idParams())
		require.NoError(t, err)
		require.Equal(t, seedRowKeys(traversalSeedReuseRows(fixture.documents)), seedRowKeys(result.Rows))
		require.LessOrEqual(t, len(counter.requests), 4,
			"the materialized owner reads the same populations once for all its input rows: %v", counter.requests)

		counter.requests = nil
		marked, err := exec.Execute(t.Context(),
			"MATCH (d:PDDocument) RETURN d.id AS id, d.seen AS seen", nil)
		require.NoError(t, err)
		written := make([][]interface{}, len(fixture.documents))
		for index, document := range fixture.documents {
			written[index] = []interface{}{document, true}
		}
		require.Equal(t, seedRowKeys(written), seedRowKeys(marked.Rows),
			"every row's write landed: the reused estimates never replaced the rows the write applied to")
	})
}

// seedFailingCountStoreSpy fails the first read of one counter name and
// forwards every later read, so a case sees whether a failed read was
// remembered.
type seedFailingCountStoreSpy struct {
	*seedCountStoreSpy
	failName string
	failures int
}

func (s *seedFailingCountStoreSpy) ReadCountStore(ctx context.Context, request storage.CountStoreReadRequest) (storage.CountStoreRead, error) {
	if request.Name == s.failName && s.failures == 0 {
		s.failures++
		s.seedCountStoreSpy.requests = append(s.seedCountStoreSpy.requests, request)
		return storage.CountStoreRead{}, errors.New("count store read failed")
	}
	return s.seedCountStoreSpy.ReadCountStore(ctx, request)
}

// TestTraversalSeedStatisticsObserveCancellationOnReusedAnswers: a reused
// answer is answered without a store read, but it still observes the
// cancellation the read it replaces would have observed, and an error is never
// remembered.
func TestTraversalSeedStatisticsObserveCancellationOnReusedAnswers(t *testing.T) {
	fixture := newTraversalSeedCostFixture(t, traversalSeedReusePopulation())
	counter := &seedCountStoreSpy{Engine: fixture.spy}
	statistics := &traversalSeedStatistics{}
	request := storage.CountStoreReadRequest{Kind: storage.CountStoreNodesByLabel, Name: "PDDocument"}

	population, known, err := statistics.read(t.Context(), counter, request)
	require.NoError(t, err)
	require.True(t, known, "the fixture's counters answer the label population")
	require.Equal(t, int64(len(fixture.documents)), population)
	require.Len(t, counter.requests, 1)

	// The same counters are answered from the memo, without another read.
	reused, known, err := statistics.read(t.Context(), counter, request)
	require.NoError(t, err)
	require.True(t, known)
	require.Equal(t, population, reused)
	require.Len(t, counter.requests, 1, "a reused answer must not read the counters again")

	// A reused answer still observes cancellation.
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = statistics.read(cancelled, counter, request)
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, counter.requests, 1, "cancellation is observed without reading the counters")

	// A different name and a different type set are different requests.
	collections, known, err := statistics.read(t.Context(), counter, storage.CountStoreReadRequest{
		Kind: storage.CountStoreNodesByLabel, Name: "PDCollection",
	})
	require.NoError(t, err)
	require.True(t, known)
	require.Equal(t, int64(1), collections)
	require.Len(t, counter.requests, 2)

	typed := storage.CountStoreReadRequest{Kind: storage.CountStoreRelationshipsByStartLabel, Name: "PDDocument", Types: []string{"MEMBER_OF"}}
	incidence, known, err := statistics.read(t.Context(), counter, typed)
	require.NoError(t, err)
	require.True(t, known)
	require.Equal(t, int64(len(fixture.documents)), incidence)
	require.Len(t, counter.requests, 3)

	// The untyped request is the typed request's neighbour, not its answer.
	_, _, err = statistics.read(t.Context(), counter, storage.CountStoreReadRequest{
		Kind: storage.CountStoreRelationshipsByStartLabel, Name: "PDDocument",
	})
	require.NoError(t, err)
	require.Len(t, counter.requests, 4, "an untyped request asks the counters again")

	// A failed read answers only its own caller.
	failing := &seedFailingCountStoreSpy{seedCountStoreSpy: counter, failName: "PDUnrelated"}
	retried := &traversalSeedStatistics{}
	failingRequest := storage.CountStoreReadRequest{Kind: storage.CountStoreNodesByLabel, Name: "PDUnrelated"}
	_, _, err = retried.read(t.Context(), failing, failingRequest)
	require.Error(t, err)
	_, _, err = retried.read(t.Context(), failing, failingRequest)
	require.NoError(t, err, "a failed read must not be remembered")
	require.Len(t, counter.requests, 6, "the retried read reached the counters")
}

// TestTraversalSeedStatisticsCancellationReachesTheStatement: cancellation is
// not masked by the estimates' reuse: a cancelled statement fails and returns
// no rows.
func TestTraversalSeedStatisticsCancellationReachesTheStatement(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := newTraversalSeedCostFixture(t, traversalSeedReusePopulation())
		counter := &seedCountStoreSpy{Engine: fixture.spy}
		exec := NewStorageExecutor(counter, QueryCachePolicy{MaxEntries: 0})

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		result, err := exec.Execute(ctx, traversalSeedReuseStatement, fixture.idParams())
		require.Error(t, err, "a cancelled statement must not succeed")
		require.Contains(t, err.Error(), context.Canceled.Error())
		if result != nil {
			require.Empty(t, result.Rows)
		}
	})
}

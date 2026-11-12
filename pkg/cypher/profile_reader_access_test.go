package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// These are the reader-side controls for the PROFILE access records: what a
// statement's canonical readers actually read, in the unit the plan operators
// are reported in (the entities and index entries a reader returned, and the
// read attempts that returned them). Each case runs the statement through the
// public entry point with the collector PROFILE's owner installs in the
// context, and once without a collector, so the statement's rows prove the
// observation changes nothing a statement returns.

// profileReaderExecutor returns an executor over five :Person nodes and two
// :KNOWS relationships, with one arity-1 index on Person.email. Two nodes share
// the indexed value, so one index read can return two entries.
func profileReaderExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	base := newTestMemoryEngine(t).BadgerEngine
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "profile_readers"), QueryCachePolicy{MaxEntries: 0})
	_, err := exec.Execute(context.Background(), `CREATE
		(a:Person {name: 'target', email: 'dup@x', score: 20}),
		(b:Person {name: 'second', email: 'dup@x', score: 30}),
		(c:Person {name: 'third', email: 'c@x', score: 40}),
		(d:Person {name: 'fourth', email: 'd@x', score: 50}),
		(e:Person {name: 'fifth', email: 'e@x', score: 60}),
		(a)-[:KNOWS]->(c), (b)-[:KNOWS]->(d)`, nil)
	require.NoError(t, err)
	// The index is created over the stored nodes, so its fill is the one the
	// schema owner performs for an existing label.
	_, err = exec.Execute(context.Background(), "CREATE INDEX profile_person_email FOR (n:Person) ON (n.email)", nil)
	require.NoError(t, err)
	return exec
}

// profileReaderAccesses executes query without a collector and then with one,
// and returns the observed rows and the collector. Both runs must agree: an
// observed statement is the same statement.
func profileReaderAccesses(t *testing.T, exec *StorageExecutor, query string) ([][]interface{}, *profileRuntimeCollector) {
	t.Helper()
	plain, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	collector := newProfileRuntimeCollector()
	observed, err := exec.Execute(withProfileRuntimeCollector(context.Background(), collector), query, nil)
	require.NoError(t, err)
	require.Equal(t, plain.Rows, observed.Rows, "an observed statement must return the same rows: %q", query)
	return observed.Rows, collector
}

// profileAccessesOfKind returns the collector's records for one reader kind.
func profileAccessesOfKind(collector *profileRuntimeCollector, kind string) []profileAccessObservation {
	var found []profileAccessObservation
	for _, observation := range collector.accessObservation() {
		if observation.Kind == kind {
			found = append(found, observation)
		}
	}
	return found
}

// A statement that reads no entity and no index records no access: the
// constant is the whole statement.
func TestProfileReaderAccessReportsNothingForAConstant(t *testing.T) {
	exec := profileReaderExecutor(t)
	rows, collector := profileReaderAccesses(t, exec, "RETURN 1 AS one")
	require.Equal(t, [][]interface{}{{int64(1)}}, rows)
	require.Empty(t, collector.accessObservation(), "a statement that reads nothing records no access")
}

// A label scan counts the entities it read, before the WHERE it admits them
// with: five entities were read, one row was admitted.
func TestProfileReaderAccessCountsScannedEntitiesBeforeAdmission(t *testing.T) {
	exec := profileReaderExecutor(t)
	rows, collector := profileReaderAccesses(t, exec, "MATCH (n:Person) WHERE n.score > 55 RETURN n.name AS name")
	require.Equal(t, [][]interface{}{{"fifth"}}, rows, "one of the five scanned entities is admitted")

	scans := profileAccessesOfKind(collector, profileKindNodeByLabelScan)
	require.Len(t, scans, 1)
	require.Equal(t, "Person", scans[0].Detail, "the scan names the label it read")
	require.EqualValues(t, 5, scans[0].Rows, "every entity the scan read is a row of the access record")
	require.EqualValues(t, 5, scans[0].Hits, "reading an entity is one hit")
}

// A property-index read counts the entries it returned and the one lookup that
// returned them; the nodes those entries name are point reads, counted by the
// point owner, not again by the index.
func TestProfileReaderAccessCountsIndexEntriesAndLookup(t *testing.T) {
	exec := profileReaderExecutor(t)

	t.Run("two entries", func(t *testing.T) {
		rows, collector := profileReaderAccesses(t, exec, "MATCH (n:Person) WHERE n.email = 'dup@x' RETURN n.name AS name")
		require.Len(t, rows, 2)

		seeks := profileAccessesOfKind(collector, profileKindNodeIndexSeek)
		require.Len(t, seeks, 1)
		require.Equal(t, "Person(email)", seeks[0].Detail, "the seek names the index it read")
		require.EqualValues(t, 2, seeks[0].Rows, "the index returned two entries")
		require.EqualValues(t, 3, seeks[0].Hits, "one lookup and its two returned entries")

		points := profileAccessesOfKind(collector, profileKindNodeByIdSeek)
		require.Len(t, points, 1)
		require.EqualValues(t, 2, points[0].Rows, "the point reader hydrated both entries")
		require.EqualValues(t, 2, points[0].Hits)
		require.Empty(t, profileAccessesOfKind(collector, profileKindNodeByLabelScan), "the seek seeds the read, so no label is scanned")
	})

	t.Run("a lookup that lists nothing still counted its lookup", func(t *testing.T) {
		rows, collector := profileReaderAccesses(t, exec, "MATCH (n:Person) WHERE n.email = 'missing@x' RETURN n.name AS name")
		require.Empty(t, rows)

		seeks := profileAccessesOfKind(collector, profileKindNodeIndexSeek)
		require.Len(t, seeks, 1)
		require.Equal(t, "Person(email)", seeks[0].Detail)
		require.Zero(t, seeks[0].Rows, "the index returned no entry")
		require.EqualValues(t, 1, seeks[0].Hits, "the lookup happened")
		require.Empty(t, profileAccessesOfKind(collector, profileKindNodeByIdSeek), "an empty posting hydrates nothing")
	})
}

// A typed expansion counts the relationships its adjacency read returned, in
// the pattern's direction.
func TestProfileReaderAccessCountsTypedTraversalEntries(t *testing.T) {
	exec := profileReaderExecutor(t)
	rows, collector := profileReaderAccesses(t, exec, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name AS name")
	require.Len(t, rows, 2, "the graph holds two :KNOWS relationships")

	expands := profileAccessesOfKind(collector, profileKindExpand)
	require.NotEmpty(t, expands, "the pattern's step reads adjacency")
	var expanded, hits int64
	for _, expand := range expands {
		require.True(t, strings.HasPrefix(expand.Detail, "outgoing"), "the record keeps the pattern's direction: %q", expand.Detail)
		expanded += expand.Rows
		hits += expand.Hits
	}
	require.EqualValues(t, 2, expanded, "each returned relationship is one entity read")
	require.EqualValues(t, 2, hits, "reading a relationship entry is one hit")
}

// A count the store's counters answer is one counter read and one count row;
// the row path it replaces reads no entity at all.
func TestProfileReaderAccessCountsOneCountStoreRead(t *testing.T) {
	ctx := context.Background()
	exec := profileReaderExecutor(t)

	t.Run("node label count", func(t *testing.T) {
		// Control: the counters must describe this count, or the statement
		// answers from its rows and is a different read.
		count, answered, err := exec.nodeCountFromStore(ctx, nodePatternInfo{variable: "n", labels: []string{"Person"}}, returnItem{expr: "count(n)"})
		require.NoError(t, err)
		require.True(t, answered.available(), "the store's counters must describe the label count")
		require.EqualValues(t, 5, count)

		rows, collector := profileReaderAccesses(t, exec, "MATCH (n:Person) RETURN count(n) AS c")
		require.Equal(t, [][]interface{}{{int64(5)}}, rows)

		counters := profileAccessesOfKind(collector, profileKindNodeCountFromCountStore)
		require.Len(t, counters, 1)
		require.Equal(t, "Person", counters[0].Detail)
		require.EqualValues(t, 1, counters[0].Rows, "the counter answers one count row")
		require.EqualValues(t, 1, counters[0].Hits, "one counter read answered it")
		require.Empty(t, profileAccessesOfKind(collector, profileKindNodeByLabelScan), "no entity was scanned")
	})

	t.Run("relationship type count", func(t *testing.T) {
		count, answered, err := exec.relationshipCountFromStore(ctx, exec.parseTraversalPattern(ctx, "()-[r:KNOWS]->()"), returnItem{expr: "count(r)"})
		require.NoError(t, err)
		require.True(t, answered.available(), "the store's counters must describe the type count")
		require.EqualValues(t, 2, count)

		rows, collector := profileReaderAccesses(t, exec, "MATCH ()-[r:KNOWS]->() RETURN count(r) AS c")
		require.Equal(t, [][]interface{}{{int64(2)}}, rows)

		counters := profileAccessesOfKind(collector, profileKindRelationshipCountFromCountStore)
		require.Len(t, counters, 1)
		require.Equal(t, "KNOWS", counters[0].Detail)
		require.EqualValues(t, 1, counters[0].Rows, "the counter answers one count row")
		require.EqualValues(t, 1, counters[0].Hits, "one counter read answered it")
		require.Empty(t, profileAccessesOfKind(collector, profileKindExpand), "no relationship was expanded")
	})
}

// profilePlanOperators selects access records structurally, optionally by scope.
func profilePlanOperators(op *PlanOperator, kind string, detail ...string) []*PlanOperator {
	if op == nil {
		return nil
	}
	var found []*PlanOperator
	if op.OperatorType == kind && (len(detail) == 0 || op.Description == detail[0]) {
		found = append(found, op)
	}
	for _, child := range op.Children {
		found = append(found, profilePlanOperators(child, kind, detail...)...)
	}
	return found
}

// profileEquivalentObservedStatement runs statement and then PROFILE statement on exec and
// holds both answers to the same columns and the same rows: a reviewed
// registration reports what it read without changing what the procedure returns.
func profileEquivalentObservedStatement(t *testing.T, exec *StorageExecutor, statement string) (*ExecuteResult, *ExecutionPlan) {
	t.Helper()
	plain, err := exec.Execute(t.Context(), statement, nil)
	require.NoError(t, err, statement)
	profiled, err := exec.Execute(t.Context(), "PROFILE "+statement, nil)
	require.NoError(t, err, statement)
	require.Equal(t, plain.Columns, profiled.Columns, statement)
	// These controls have no ORDER BY: preserve the exact row multiset.
	require.ElementsMatch(t, plain.Rows, profiled.Rows, statement)
	plan := executionPlanFromResult(t, profiled)
	require.Empty(t, plan.Unobserved, "%s: a reviewed registration reports its own reads", statement)
	require.True(t, plan.TotalDBHitsObserved, "%s: every read the procedure performed is reported", statement)
	return profiled, plan
}

package cypher

import (
	"context"
	"sync"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newProfileRuntimeTestExecutor returns an executor over a memory engine seeded
// with statements, and the context a PROFILE owner installs a collector in.
func newProfileRuntimeTestExecutor(t *testing.T, seed ...string) (*StorageExecutor, context.Context) {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "profile")
	exec := NewStorageExecutor(store)
	for _, statement := range seed {
		_, err := exec.Execute(context.Background(), statement, nil)
		require.NoError(t, err, "seed %s", statement)
	}
	return exec, context.Background()
}

// profileRuntimeClauseOf returns the one record of an executing clause kind.
func profileRuntimeClauseOf(t *testing.T, observation profileRuntimeObservation, kind pipelineClauseKind) profileRuntimeClauseObservation {
	t.Helper()
	var found []profileRuntimeClauseObservation
	for _, clause := range observation.Clauses {
		if clause.Kind == kind {
			found = append(found, clause)
		}
	}
	require.Len(t, found, 1, "executing records of clause kind %d in %+v", int(kind), observation.Clauses)
	return found[0]
}

// TestProfileRuntimeClauseContextWithoutCollectorKeepsTheContext is the ordinary
// statement's contract: no collector, no frame, no allocation.
func TestProfileRuntimeClauseContextWithoutCollectorKeepsTheContext(t *testing.T) {
	ctx := context.Background()
	clause := pipelineClause{kind: pipelineClauseMatch, text: "MATCH (n)"}

	require.Equal(t, ctx, profileRuntimeClauseContext(ctx, nil, 0, clause))
	if _, observed := profileRuntimeFrameFromContext(profileRuntimeClauseContext(ctx, nil, 0, clause)); observed {
		t.Fatal("a statement without a collector must not carry a clause frame")
	}
	require.Zero(t, testing.AllocsPerRun(100, func() {
		profileRuntimeClauseContext(ctx, nil, 0, clause)
		profileRuntimeClauseOutputRows(ctx, nil, 7)
		profileRuntimeRecordHits(ctx, 7)
		profileRuntimeFinishStatement(ctx, nil, false, newPipelineDispatchOutcome(nil, false, nil))
	}))

	// Without a collector the source the clause hands on is the source itself.
	var yielded int
	source := profileRuntimeClauseOutputSource(ctx, nil, pipelineRowsSource([]pipelineRow{{"n": 1}, {"n": 2}}))
	require.True(t, source(func(pipelineRow) bool { yielded++; return true }))
	require.Equal(t, 2, yielded)
}

// TestProfileRuntimeLazyOutputCountsTheRowsItActuallyYielded proves a lazy
// clause's rows are the rows its output source yielded, including for a consumer
// that stops early, and that the count is not the materialized row count of a
// helper that read the source.
func TestProfileRuntimeLazyOutputCountsTheRowsItActuallyYielded(t *testing.T) {
	collector := newProfileRuntimeCollector()
	ctx := withProfileRuntimeCollector(context.Background(), collector)
	clauseCtx := profileRuntimeClauseContext(ctx, collector, 0, pipelineClause{kind: pipelineClauseWith, text: "WITH 1 AS x"})

	source := profileRuntimeClauseOutputSource(clauseCtx, collector, func(yield func(pipelineRow) bool) bool {
		for index := 0; index < 5; index++ {
			if !yield(pipelineRow{"x": index}) {
				return false
			}
		}
		return true
	})
	var taken int
	require.False(t, source(func(pipelineRow) bool { taken++; return taken < 3 }))
	require.Equal(t, 3, taken)

	record := profileRuntimeClauseOf(t, collector.observation(), pipelineClauseWith)
	require.True(t, record.RowsObserved)
	require.Equal(t, int64(3), record.Rows)
	require.Equal(t, int64(1), record.Executions)
}

// TestProfileRuntimeRecordsKeyTheParentAndType proves a record carries the type
// it ran as and the record of the clause whose execution started its program,
// and that hits reported from its own context land on it.
func TestProfileRuntimeRecordsKeyTheParentAndType(t *testing.T) {
	collector := newProfileRuntimeCollector()
	ctx := withProfileRuntimeCollector(context.Background(), collector)
	require.Zero(t, profileRuntimeProgramParent(ctx), "a statement's own program has no parent")

	matchCtx := profileRuntimeClauseContext(ctx, collector, 0, pipelineClause{kind: pipelineClauseMatch, text: "MATCH (n)"})
	match := profileRuntimeClauseOf(t, collector.observation(), pipelineClauseMatch)
	require.Equal(t, match.ID, profileRuntimeProgramParent(matchCtx), "a program the clause starts reports the clause")
	profileRuntimeRecordHits(matchCtx, 4)
	profileRuntimeRecordHits(matchCtx, 6)

	// A program the CALL clause starts reports the CALL clause's record as the
	// parent of its own clauses.
	callCtx := profileRuntimeClauseContext(ctx, collector, 0, pipelineClause{kind: pipelineClauseCallSubquery, text: "CALL { RETURN 1 AS x }"})
	call := profileRuntimeClauseOf(t, collector.observation(), pipelineClauseCallSubquery)
	require.Equal(t, call.ID, profileRuntimeProgramParent(callCtx))
	innerCtx := profileRuntimeClauseContext(callCtx, collector, profileRuntimeProgramParent(callCtx), pipelineClause{kind: pipelineClauseReturn, text: "RETURN 1 AS x"})
	profileRuntimeRecordHits(innerCtx, 2)

	observation := collector.observation()
	require.Len(t, observation.Clauses, 3)
	observedMatch := observation.Clauses[0]
	require.Equal(t, int64(0), observedMatch.Parent)
	require.True(t, observedMatch.DBHitsObserved)
	require.Equal(t, int64(10), observedMatch.DBHits)
	require.Equal(t, "MATCH (n)", observedMatch.Source)
	inner := observation.Clauses[2]
	require.Equal(t, call.ID, inner.Parent)
	require.True(t, inner.DBHitsObserved)
	require.Equal(t, int64(2), inner.DBHits)
}

// TestProfileRuntimeFinishStatementRecordsTheFinalRowsOnce proves the statement's
// final row count has one writer, that a nested statement reports nothing of its
// own, and that a route the collector cannot see is reported as unobserved
// instead of as a zero count.
func TestProfileRuntimeFinishStatementRecordsTheFinalRowsOnce(t *testing.T) {
	collector := newProfileRuntimeCollector()
	ctx, statementCtx := context.Background(), withProfileRuntimeCollector(context.Background(), collector)

	nested := profileRuntimeClauseContext(statementCtx, collector, 0, pipelineClause{kind: pipelineClauseFilter, text: "FILTER x > 1"})
	profileRuntimeFinishStatement(nested, collector, true, newPipelineDispatchOutcome(&ExecuteResult{Rows: [][]interface{}{{1}}}, true, nil))
	require.False(t, collector.observation().FinalRowsObserved, "a clause's own statement is the clause's row value")

	profileRuntimeFinishStatement(statementCtx, collector, false, newPipelineDispatchOutcome(&ExecuteResult{Rows: [][]interface{}{{1}, {2}}}, true, nil))
	observation := collector.observation()
	require.True(t, observation.FinalRowsObserved)
	require.Equal(t, int64(2), observation.FinalRows)
	require.Len(t, observation.Unobserved, 1, "the route outside the clause row pipeline is reported")

	declined := newProfileRuntimeCollector()
	profileRuntimeFinishStatement(ctx, declined, false, newPipelineDispatchOutcome(nil, false, nil))
	require.False(t, declined.observation().FinalRowsObserved)
	require.Len(t, declined.observation().Unobserved, 1)
}

// TestProfileRuntimeObservesClauseRowsOfAStatement proves the clause row
// pipeline reports each clause's actual output rows and the statement's returned
// rows, with nothing left unobserved.
func TestProfileRuntimeObservesClauseRowsOfAStatement(t *testing.T) {
	exec, ctx := newProfileRuntimeTestExecutor(t,
		"CREATE (:Person {name: 'a'})",
		"CREATE (:Person {name: 'b'})",
		"CREATE (:Person {name: 'c'})",
	)
	collector := newProfileRuntimeCollector()
	outcome := exec.executePipeline(withProfileRuntimeCollector(ctx, collector), "MATCH (p:Person) WITH p.name AS name RETURN name")
	require.True(t, outcome.handled())
	require.NoError(t, outcome.err)
	require.Len(t, outcome.result.Rows, 3)

	observation := collector.observation()
	require.True(t, observation.FinalRowsObserved)
	require.Equal(t, int64(3), observation.FinalRows)
	require.Empty(t, observation.Unobserved)
	for _, kind := range []pipelineClauseKind{pipelineClauseMatch, pipelineClauseWith, pipelineClauseReturn} {
		clause := profileRuntimeClauseOf(t, observation, kind)
		require.True(t, clause.RowsObserved, "clause kind %d reported rows", int(kind))
		require.Equal(t, int64(3), clause.Rows, "clause kind %d", int(kind))
		require.Equal(t, int64(1), clause.Executions, "clause kind %d", int(kind))
	}
}

// TestProfileRuntimeRowOrderedWriteStretchAggregates proves a clause the pipeline
// runs once per row adds to the one record that names it, keeping its parent and
// source.
func TestProfileRuntimeRowOrderedWriteStretchAggregates(t *testing.T) {
	exec, ctx := newProfileRuntimeTestExecutor(t,
		"CREATE (:Person {name: 'a'})",
		"CREATE (:Person {name: 'b'})",
		"CREATE (:Person {name: 'c'})",
	)
	collector := newProfileRuntimeCollector()
	outcome := exec.executePipeline(withProfileRuntimeCollector(ctx, collector), "MATCH (p:Person) UNWIND [1, 2] AS k SET p.k = k")
	require.True(t, outcome.handled())
	require.NoError(t, outcome.err)

	observation := collector.observation()
	match := profileRuntimeClauseOf(t, observation, pipelineClauseMatch)
	unwind := profileRuntimeClauseOf(t, observation, pipelineClauseUnwind)
	set := profileRuntimeClauseOf(t, observation, pipelineClauseSet)
	require.Equal(t, int64(3), match.Rows)
	require.Equal(t, int64(6), unwind.Rows)
	// The UNWIND runs the SET once for each of its six rows: one record, six
	// executions, six output rows.
	require.Equal(t, "SET p.k = k", set.Source)
	require.Equal(t, int64(0), set.Parent)
	require.Equal(t, int64(6), set.Executions)
	require.True(t, set.RowsObserved)
	require.Equal(t, int64(6), set.Rows)
}

// TestProfileRuntimeClauseContextKeepsTheProgramsContext proves a clause runs
// over the context its program runs with, not over a context the collector
// built: what a statement deleted stays deleted for its later clauses.
func TestProfileRuntimeClauseContextKeepsTheProgramsContext(t *testing.T) {
	exec, ctx := newProfileRuntimeTestExecutor(t,
		"CREATE (:P {id: 4})",
		"CREATE (:DQ {id: 1, p: 5})-[:DR {w: 2}]->(:DQ {id: 2, p: 6})",
	)

	for _, statement := range []string{
		"MATCH (n:P) DELETE n RETURN n.id AS v",
		"MATCH (n:DQ {id: 1}) DETACH DELETE n WITH collect(n) AS ns UNWIND ns AS m RETURN m.p AS v",
	} {
		t.Run(statement, func(t *testing.T) {
			collector := newProfileRuntimeCollector()
			outcome := exec.executePipeline(withProfileRuntimeCollector(ctx, collector), statement)
			require.Error(t, outcome.err, "a deleted entity's property is not readable")
			code, _ := nornicerrors.Neo4jStatus(outcome.err)
			require.Equal(t, "Neo.ClientError.Statement.EntityNotFound", code)

			observation := collector.observation()
			require.NotEmpty(t, observation.Clauses, "the clauses that ran are still reported")
			require.False(t, observation.FinalRowsObserved, "a failed statement returns no rows")
			require.NotEmpty(t, observation.Unobserved, "a failed statement's rows are unobserved, not zero")
		})
	}
}

// TestProfileRuntimeNestedProgramKeepsItsParent proves the clauses a CALL
// subquery body runs are reported under the CALL clause that ran them, not under
// the statement.
func TestProfileRuntimeNestedProgramKeepsItsParent(t *testing.T) {
	exec, ctx := newProfileRuntimeTestExecutor(t,
		"CREATE (:N {id: 'a'})-[:R]->(:N {id: 'b'})",
	)
	collector := newProfileRuntimeCollector()
	outcome := exec.executePipeline(withProfileRuntimeCollector(ctx, collector),
		"MATCH (x:N {id: 'a'}) CALL (x) { MATCH (x)-[:R]->(m) RETURN m.id AS v } RETURN v")
	require.True(t, outcome.handled())
	require.NoError(t, outcome.err)
	require.Len(t, outcome.result.Rows, 1)

	observation := collector.observation()
	call := profileRuntimeClauseOf(t, observation, pipelineClauseCallSubquery)
	var nested []profileRuntimeClauseObservation
	for _, clause := range observation.Clauses {
		if clause.Parent == call.ID {
			nested = append(nested, clause)
		}
	}
	require.NotEmpty(t, nested, "the CALL body's clauses report the CALL as their parent")
	for _, clause := range nested {
		require.Equal(t, call.ID, clause.Parent)
		require.NotEqual(t, int64(0), clause.Parent)
	}
}

// TestProfileRuntimeCollectorIsConcurrencySafe proves records, counts and the
// statement's report survive concurrent clause execution.
func TestProfileRuntimeCollectorIsConcurrencySafe(t *testing.T) {
	collector := newProfileRuntimeCollector()
	ctx := withProfileRuntimeCollector(context.Background(), collector)
	clause := pipelineClause{kind: pipelineClauseMatch, text: "MATCH (n)"}

	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for round := 0; round < 25; round++ {
				clauseCtx := profileRuntimeClauseContext(ctx, collector, 0, clause)
				profileRuntimeRecordHits(clauseCtx, 1)
				profileRuntimeClauseOutputRows(clauseCtx, collector, 1)
				collector.noteUnobserved("one shared reason")
				_ = collector.observation()
			}
		}()
	}
	workers.Wait()

	observation := collector.observation()
	record := profileRuntimeClauseOf(t, observation, pipelineClauseMatch)
	require.Equal(t, int64(200), record.Executions)
	require.Equal(t, int64(200), record.Rows)
	require.Equal(t, int64(200), record.DBHits)
	require.Len(t, observation.Unobserved, 1, "one reason is reported once")
	require.Equal(t, "one shared reason", observation.Unobserved[0])
}

// TestProfileRuntimeClauseRunsRowsExcludesKindsThePipelineNeverRuns proves the
// collector never reports rows for a clause kind the row pipeline does not
// execute.
func TestProfileRuntimeClauseRunsRowsExcludesKindsThePipelineNeverRuns(t *testing.T) {
	require.False(t, profileRuntimeClauseRunsRows(pipelineClauseLoadCSV))
	require.False(t, profileRuntimeClauseRunsRows(pipelineClauseCommand))
	require.True(t, profileRuntimeClauseRunsRows(pipelineClauseMatch))
	require.True(t, profileRuntimeClauseRunsRows(pipelineClauseReturn))
}

// Identical source text at distinct clause positions is two operators. Repeated
// invocations of either retained clause still aggregate into its own record.
func TestProfileRuntimeRetainsDistinctClausePositions(t *testing.T) {
	exec, ctx := newProfileRuntimeTestExecutor(t, "CREATE (:Person {id:1}), (:Person {id:2})")
	collector := newProfileRuntimeCollector()
	outcome := exec.executePipeline(withProfileRuntimeCollector(ctx, collector), "MATCH (p:Person) WITH p WITH p RETURN p")
	require.NoError(t, outcome.err)
	require.True(t, outcome.handled())
	require.Len(t, outcome.result.Rows, 2)
	var withs []profileRuntimeClauseObservation
	for _, clause := range collector.observation().Clauses {
		if clause.Kind == pipelineClauseWith {
			withs = append(withs, clause)
		}
	}
	require.Len(t, withs, 2, "text identity must not merge two executed clause positions")
	require.NotEqual(t, withs[0].ID, withs[1].ID)
	for _, clause := range withs {
		require.Equal(t, int64(2), clause.Rows)
		require.Equal(t, int64(1), clause.Executions)
	}
}

func TestProfileLazySourceObservationRequiresConsumption(t *testing.T) {
	for _, consume := range []bool{false, true} {
		collector := newProfileRuntimeCollector()
		ctx := profileRuntimeClauseContext(withProfileRuntimeCollector(t.Context(), collector), collector, 0,
			pipelineClause{kind: pipelineClauseMatch, text: "MATCH (n:Missing)"})
		source := profileRuntimeClauseOutputSource(ctx, collector, pipelineRowsSource(nil))
		if consume {
			require.True(t, source(func(pipelineRow) bool { t.Fatal("empty source yielded a row"); return true }))
		}
		record := profileRuntimeClauseOf(t, collector.observation(), pipelineClauseMatch)
		require.Equal(t, consume, record.RowsObserved, "wrapping an unconsumed source is not execution; consuming an empty source observes zero")
		require.Zero(t, record.Rows)
	}
}

func TestProfileUnobservedPhysicalRouteDoesNotCertifyZeroDatabaseHits(t *testing.T) {
	collector := newProfileRuntimeCollector()
	ctx := withProfileRuntimeCollector(t.Context(), collector)
	result := &ExecuteResult{Rows: [][]interface{}{{1}}}
	profileRuntimeFinishStatement(ctx, collector, false, newPipelineDispatchOutcome(result, true, nil))
	plan := observedExecutionPlan("physical route", result, 0, collector)
	require.False(t, plan.TotalDBHitsObserved)
	require.NotEmpty(t, plan.Unobserved)
	require.True(t, plan.Root.RowsObserved)
	require.EqualValues(t, 1, plan.Root.ActualRows)
}

package cypher

// Public PROFILE controls for the write routes that do not run their clauses
// through the clause row pipeline: the fused CREATE plan (a statement whose
// leading CREATE stretch is run as one operator, followed by SET and RETURN)
// and the leading UNWIND batch routes.
//
// Every test here goes through StorageExecutor.Execute, the public entry point
// Bolt and HTTP use, on both configured parsers, and asserts only what the
// supported contract states:
//
//   - An operator reports the rows it actually handed on at its own boundary,
//     which is not the statement's final cardinality (a fused CREATE handing on
//     three rows and the aggregating RETURN that follows are two counts).
//   - A stretch the executor fuses is one operator reported on the first clause
//     of the stretch; the fused clauses have no record of their own, and the
//     operator ran once for the batch rather than once per row.
//   - An empty list is an observed zero, never a missing count, and a failed
//     statement returns its error without a plan that would have to invent
//     rows.
//   - The mutation requests these routes issue are their own records: the
//     fused route reports one native request per entity it staged, and a batch
//     the transaction stages in one call is one request rather than one per
//     entity. Nothing about the statement is left unobserved.

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// countProfileOperatorType counts the operators of operatorType in the plan.
func countProfileOperatorType(op *PlanOperator, operatorType string) int {
	count := 0
	for _, kind := range collectOperatorTypes(op) {
		if kind == operatorType {
			count++
		}
	}
	return count
}

// requireObservedRows asserts one operator reports rows and they are rows,
// never the statement's final cardinality reused as its own.
func requireObservedRows(t *testing.T, op *PlanOperator, rows int64, description string) *PlanOperator {
	t.Helper()
	require.NotNil(t, op, description)
	require.True(t, op.RowsObserved, "%s reports the rows it handed on", description)
	require.Equal(t, rows, op.ActualRows, "%s", description)
	return op
}

// TestProfileFusedCreateRouteReportsCreateSetAndReturnRows proves the fused
// CREATE plan reports each operator it ran at that operator's own boundary,
// while the ordinary statement keeps its result rows, its mutated entity and
// its counters.
func TestProfileFusedCreateRouteReportsCreateSetAndReturnRows(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "profile_fused_create"))

		ordinary, err := exec.Execute(t.Context(),
			`CREATE (n:Ordinary {name: 'b'}) SET n.done = true RETURN n.name AS name`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"b"}}, ordinary.Rows)
		require.NotNil(t, ordinary.Stats)
		require.Equal(t, 1, ordinary.Stats.NodesCreated)
		require.Equal(t, 2, ordinary.Stats.PropertiesSet)
		require.Nil(t, ordinary.Metadata["plan"], "an ordinary statement carries no plan")

		result, err := exec.Execute(t.Context(),
			`PROFILE CREATE (n:Fused {name: 'a'}) SET n.done = true RETURN n.name AS name`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"a"}}, result.Rows, "the profiled statement returns the same rows")
		require.NotNil(t, result.Stats)
		require.Equal(t, 1, result.Stats.NodesCreated)
		require.Equal(t, 2, result.Stats.PropertiesSet)

		plan := executionPlanFromResult(t, result)
		require.Equal(t, ModeProfile, plan.Mode)
		require.Equal(t, int64(1), plan.TotalRows)

		create := requireObservedRows(t, planOperatorOfType(plan.Root, "Create"), 1, "the fused CREATE operator")
		require.Equal(t, int64(1), create.Arguments["executions"])
		require.Contains(t, create.Description, "(n:Fused", "the operator keeps the clause source it ran")
		set := requireObservedRows(t, planOperatorOfType(plan.Root, "Set"), 1, "the SET operator")
		require.Equal(t, int64(1), set.Arguments["executions"])
		requireObservedRows(t, planOperatorOfType(plan.Root, "Projection"), 1, "the RETURN operator")

		// One operator per executed clause, in the order they ran, and the
		// node-create request attached under the clause that issued it.
		require.Equal(t, []string{"ProduceResults", "Projection", "Set", "Create", profileKindNodeCreateRequest},
			profilePlanOperatorChain(plan))

		// The fused route ran clauses, so the accesses it made are its own
		// records: the node it created and the property it set are the two
		// native requests it issued.
		require.Empty(t, plan.Unobserved)
		require.True(t, plan.TotalDBHitsObserved)
		created := requireRequestRecord(t, plan, profileKindNodeCreateRequest,
			"the node the fused CREATE staged")
		require.Equal(t, int64(1), created.DBHits, "one node is one native create request")
		require.Equal(t, int64(1), created.ActualRows, "the request was acknowledged")
		updated := requireRequestRecord(t, plan, profileKindNodeUpdateRequest,
			"the property the SET stored")
		require.Equal(t, int64(1), updated.DBHits, "one node edit is one native update request")
		require.Equal(t, int64(1), updated.ActualRows, "the request was acknowledged")
		require.Equal(t, int64(2), plan.TotalDBHits,
			"the create and the update are the only accesses the statement made")
	})
}

// TestProfileFusedCreateStretchIsOneOperator proves a stretch of CREATE clauses
// the route fuses is one operator: it reports the stretch's first source clause
// as its identity, never a second operator for the later clause, and it ran
// once for the batch.
func TestProfileFusedCreateStretchIsOneOperator(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "profile_fused_stretch"))

		result, err := exec.Execute(t.Context(),
			`PROFILE CREATE (a:First {v: 1}) CREATE (b:Second {v: 2}) RETURN a.v AS v`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		require.Equal(t, 2, result.Stats.NodesCreated, "both fused CREATE clauses wrote")

		plan := executionPlanFromResult(t, result)
		require.Equal(t, 1, countProfileOperatorType(plan.Root, "Create"),
			"one fused operator represents the CREATE stretch, not one per source clause")
		create := requireObservedRows(t, planOperatorOfType(plan.Root, "Create"), 1, "the fused CREATE operator")
		require.Equal(t, int64(1), create.Arguments["executions"])
		require.Contains(t, create.Description, "(a:First",
			"the fused operator keeps the stretch's first source clause as its identity")
		require.NotContains(t, create.Description, "(b:Second",
			"the later fused clause is not reported as its own operator")
	})
}

// TestProfileLeadingUnwindCreateBatchReportsFusedRows proves the leading-UNWIND
// route reports the UNWIND operator's own rows and the fused CREATE batch's
// rows, which differ from the aggregating RETURN's single row, and that the
// batch ran once rather than once per row.
func TestProfileLeadingUnwindCreateBatchReportsFusedRows(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "profile_unwind_create"))

		ordinary, err := exec.Execute(t.Context(),
			`UNWIND [1, 2, 3] AS x CREATE (n:Ordinary {v: x}) RETURN count(n) AS c`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(3)}}, ordinary.Rows)
		require.Equal(t, 3, ordinary.Stats.NodesCreated)

		result, err := exec.Execute(t.Context(),
			`PROFILE UNWIND [1, 2, 3] AS x CREATE (n:Batch {v: x}) RETURN count(n) AS c`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)
		require.Equal(t, 3, result.Stats.NodesCreated, "the profiled batch wrote the same entities")

		plan := executionPlanFromResult(t, result)
		require.Equal(t, int64(1), plan.TotalRows)

		require.Equal(t, 1, countProfileOperatorType(plan.Root, "Unwind"),
			"the leading UNWIND is reported once")
		unwind := requireObservedRows(t, planOperatorOfType(plan.Root, "Unwind"), 3, "the leading UNWIND operator")
		require.Equal(t, "UNWIND [1, 2, 3] AS x", unwind.Description,
			"the operator keeps the leading clause's own source")

		create := requireObservedRows(t, planOperatorOfType(plan.Root, "Create"), 3, "the fused CREATE operator")
		require.Equal(t, int64(1), create.Arguments["executions"],
			"the fused CREATE ran once for the batch, not once for each of the three rows")

		projection := requireObservedRows(t, planOperatorOfType(plan.Root, "Projection"), 1, "the RETURN operator")
		require.NotEqual(t, create.ActualRows, projection.ActualRows,
			"an operator's rows are its own boundary count, never the final result copied onto it")

		// The batch's three nodes are three native create requests: the node
		// create route stages one node per call, so the batch reports one
		// request per entity it wrote.
		require.Empty(t, plan.Unobserved)
		require.True(t, plan.TotalDBHitsObserved)
		requests := requireRequestRecord(t, plan, profileKindNodeCreateRequest,
			"the nodes the batch staged")
		require.Equal(t, int64(3), requests.DBHits, "one native create request per staged node")
		require.Equal(t, int64(3), requests.ActualRows, "each request was acknowledged")
		require.Equal(t, int64(3), plan.TotalDBHits,
			"the three create requests are the only accesses: the batch read no graph entity")
	})
}

// TestProfileLeadingUnwindPhysicalBatchReportsUnwindRows proves the batched
// physical route a bulk seeder emits reports the UNWIND operator's rows and the
// fused CREATE batch's rows at their own boundaries while it keeps the writes
// and the counters that route is responsible for.
func TestProfileLeadingUnwindPhysicalBatchReportsUnwindRows(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "profile_unwind_batch"))
		_, err := exec.Execute(t.Context(), `CREATE INDEX idx_doc_id FOR (n:Doc) ON (n.id)`, nil)
		require.NoError(t, err)
		for i := 0; i < 8; i++ {
			_, err = exec.Execute(t.Context(), `CREATE (:Doc {id: $id})`, map[string]interface{}{"id": int64(i)})
			require.NoError(t, err)
		}
		rows := make([]interface{}, 4)
		for i := range rows {
			rows[i] = map[string]interface{}{"offset": int64(i)}
		}

		result, err := exec.Execute(t.Context(), `PROFILE UNWIND $rows AS row
MATCH (a:Doc {id: row.offset})
MATCH (b:Doc {id: row.offset + 1})
CREATE (a)-[:NEXT]->(b)`, map[string]interface{}{"rows": rows})
		require.NoError(t, err)
		require.True(t, exec.LastHotPathTrace().UnwindMultiMatchCreateBatch,
			"this control must stay on the batched leading-UNWIND route")
		require.Equal(t, len(rows), result.Stats.RelationshipsCreated)

		plan := executionPlanFromResult(t, result)
		require.Equal(t, 1, countProfileOperatorType(plan.Root, "Unwind"),
			"the leading UNWIND is reported once for the whole batch")
		unwind := requireObservedRows(t, planOperatorOfType(plan.Root, "Unwind"), int64(len(rows)), "the leading UNWIND operator")
		require.Equal(t, "UNWIND $rows AS row", unwind.Description,
			"the operator keeps the leading clause's own source")
		requireObservedRows(t, planOperatorOfType(plan.Root, "Create"), int64(len(rows)), "the fused CREATE operator")

		// The batch reads through the same readers as any other statement and
		// creates its relationships through the native create API, so its
		// accesses are its own records.
		require.Empty(t, plan.Unobserved)
		require.True(t, plan.TotalDBHitsObserved)
		created := requireRequestRecord(t, plan, profileKindRelationshipCreateBatchRequest,
			"the relationships the batch staged")
		require.Equal(t, int64(1), created.DBHits,
			"the batch is one distinct native batch request, not one request per written relationship")
		require.Equal(t, int64(1), created.ActualRows, "the batch request was acknowledged")
		requireNoRequestRecord(t, plan, profileKindNodeCreateRequest,
			"the statement creates no node")
	})
}

// TestProfileLeadingUnwindEmptyListReportsObservedZero proves an empty list is
// an observed zero: the UNWIND and the CREATE batch that never ran report zero
// rows instead of an absent count, and the statement writes nothing.
func TestProfileLeadingUnwindEmptyListReportsObservedZero(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "profile_unwind_empty"))

		result, err := exec.Execute(t.Context(), `PROFILE UNWIND [] AS x CREATE (n:Nothing {v: x})`, nil)
		require.NoError(t, err)
		require.Empty(t, result.Rows, "a statement ending in a write returns no rows")
		require.NotNil(t, result.Stats)
		require.Zero(t, result.Stats.NodesCreated)

		plan := executionPlanFromResult(t, result)
		requireObservedRows(t, planOperatorOfType(plan.Root, "Unwind"), 0, "the empty leading UNWIND operator")
		requireObservedRows(t, planOperatorOfType(plan.Root, "Create"), 0, "the CREATE operator that handed on no rows")
	})
}

// TestProfileLeadingUnwindFailedStatementReturnsNoPlan proves a failed
// statement is its error alone: PROFILE reports no plan, so no operator can
// carry rows the run never produced.
func TestProfileLeadingUnwindFailedStatementReturnsNoPlan(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "profile_unwind_error"))

		result, err := exec.Execute(t.Context(), `PROFILE UNWIND $missing AS x CREATE (n:Failed {v: x})`, nil)
		require.Error(t, err)
		require.Nil(t, result, "a failed PROFILE returns the statement's error, never a plan of invented rows")
	})
}

// TestProfileFusedCreateRouteKeepsEqualSourcePositionsDistinct proves two SET
// clauses written the same way at different positions stay two operators: the
// route ran one per clause, and equal source text does not merge them.
func TestProfileFusedCreateRouteKeepsEqualSourcePositionsDistinct(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "profile_fused_positions"))

		result, err := exec.Execute(t.Context(),
			`PROFILE CREATE (n:Repeat {v: 1}) SET n.a = 1 SET n.a = 1 RETURN n.a AS v`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

		plan := executionPlanFromResult(t, result)
		require.Equal(t, 2, countProfileOperatorType(plan.Root, "Set"),
			"equal SET text at two positions is two executed operators")
		require.Equal(t, []string{"ProduceResults", "Projection", "Set", "Set", "Create", profileKindNodeCreateRequest},
			profilePlanOperatorChain(plan),
			"each SET position is its own operator above the fused CREATE, whose create request hangs below it")
	})
}

// profilePlanOperatorChain returns the plan's operator types from the root
// down its first-child chain, so a test can assert the topology the executor
// actually produced.
func profilePlanOperatorChain(plan *ExecutionPlan) []string {
	var chain []string
	for op := plan.Root; op != nil; {
		chain = append(chain, op.OperatorType)
		if len(op.Children) == 0 {
			break
		}
		op = op.Children[0]
	}
	return chain
}

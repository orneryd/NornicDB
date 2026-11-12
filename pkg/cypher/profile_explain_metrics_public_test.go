package cypher

// Public-path reproductions for PROFILE/EXPLAIN plan metrics.
//
// Every test here goes through StorageExecutor.Execute, the public entry point
// Bolt and HTTP use, and asserts only what the supported contract states:
//
//   - PROFILE reports the runs that actually happened. A statement that reads
//     no graph entity performed no storage read, so it must report zero db
//     hits; and a label scan reads every node of the label, so its row count is
//     the number of scanned nodes, not the final result count.
//   - EXPLAIN/PROFILE plan operators describe the plan the executor could run.
//     An index seek may only be claimed when the schema holds an index that is
//     applicable to the pattern's property predicate.
//
// These tests are expected to fail on the unmodified tree; they pin the
// behavior a fix must deliver, not the current implementation. Each claim is
// its own subtest so a fail-first run reports every claim separately.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// executionPlanFromResult returns the *ExecutionPlan PROFILE/EXPLAIN attached to
// the result metadata and asserts the plan tree exists.
func executionPlanFromResult(t *testing.T, result *ExecuteResult) *ExecutionPlan {
	t.Helper()
	require.NotNil(t, result)
	require.NotNil(t, result.Metadata)
	plan, ok := result.Metadata["plan"].(*ExecutionPlan)
	require.True(t, ok, "plan metadata must hold an *ExecutionPlan")
	require.NotNil(t, plan.Root, "plan must carry a root operator")
	return plan
}

// planOperatorOfType returns the first operator of operatorType in the plan
// tree, or nil when the plan does not contain one.
func planOperatorOfType(op *PlanOperator, operatorType string) *PlanOperator {
	if op == nil {
		return nil
	}
	if op.OperatorType == operatorType {
		return op
	}
	for _, child := range op.Children {
		if found := planOperatorOfType(child, operatorType); found != nil {
			return found
		}
	}
	return nil
}

// TestProfileReportsZeroDatabaseHitsForConstantProjection reproduces the
// fabricated-db-hit report: `PROFILE RETURN 1` reads no graph entity, yet the
// profile reports db hits derived from the operator's estimated rows.
func TestProfileReportsZeroDatabaseHitsForConstantProjection(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "profile_constant_hits")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// The store is empty on purpose: the statement under profile cannot read a
	// node, relationship, index or property, so any positive db-hit count is
	// fabricated.
	result, err := exec.Execute(ctx, `PROFILE RETURN 1`, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	plan := executionPlanFromResult(t, result)
	require.Equal(t, ModeProfile, plan.Mode)

	t.Run("row counts follow the executed statement", func(t *testing.T) {
		require.Len(t, result.Rows, 1)
		require.Equal(t, int64(1), plan.TotalRows, "the statement produces exactly one row")
		require.Equal(t, int64(1), plan.Root.ActualRows, "the root operator emits the one produced row")
	})

	t.Run("total db hits are zero without a storage read", func(t *testing.T) {
		require.Equal(t, int64(0), plan.TotalDBHits,
			"PROFILE RETURN 1 performs no storage read, so it must report 0 total db hits")
	})

	t.Run("operator db hits are zero without a storage read", func(t *testing.T) {
		require.Equal(t, int64(0), plan.Root.DBHits,
			"the only operator of PROFILE RETURN 1 reads no storage, so its db hits must be 0")
	})

	t.Run("rendered plan reports no db hits", func(t *testing.T) {
		// The rendered plan is the surface PROFILE consumers read; it must not
		// advertise hits the run did not perform.
		require.Contains(t, planStringFromResult(t, result), "Total DB Hits: 0")
	})
}

// TestProfileReportsPerOperatorRowCounts reproduces the copied-row-count
// report: PROFILE assigns the statement's final row count to every operator in
// the plan. A selective statement over a small graph proves the scan and the
// filter emitted different numbers of rows.
func TestProfileReportsPerOperatorRowCounts(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "profile_operator_rows")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Five :Person nodes; exactly one satisfies the WHERE clause. No index
	// exists, so the scan must read all five nodes before the filter reduces
	// them to one.
	for _, age := range []int{30, 31, 32, 33, 40} {
		_, err := exec.Execute(ctx, `CREATE (:Person {age: $age})`, map[string]interface{}{"age": age})
		require.NoError(t, err)
	}

	result, err := exec.Execute(ctx, `PROFILE MATCH (n:Person) WHERE n.age > 34 RETURN n`, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	plan := executionPlanFromResult(t, result)
	require.Equal(t, ModeProfile, plan.Mode)

	scan := planOperatorOfType(plan.Root, "NodeByLabelScan")
	require.NotNil(t, scan, "the plan must contain the label scan it executed")
	filter := planOperatorOfType(plan.Root, "Filter")
	require.NotNil(t, filter, "the plan must contain the WHERE filter it executed")

	t.Run("final row count", func(t *testing.T) {
		require.Len(t, result.Rows, 1)
		require.Equal(t, int64(1), plan.TotalRows, "the statement returns exactly one row")
		require.Equal(t, int64(1), plan.Root.ActualRows, "the root operator emits the returned row")
	})

	t.Run("label scan rows are the scanned nodes", func(t *testing.T) {
		require.Equal(t, int64(5), scan.ActualRows,
			"the label scan emits one row per :Person node (5), not the statement's final row count")
	})

	t.Run("filter rows are the rows that pass it", func(t *testing.T) {
		require.Equal(t, int64(1), filter.ActualRows,
			"the filter emits the single row that satisfies WHERE")
	})

	t.Run("storage reads cover the scanned nodes", func(t *testing.T) {
		// Guard against "fixing" the fabricated counts by zeroing them: reading
		// five nodes is at least five storage hits, wherever they are attributed.
		require.GreaterOrEqual(t, scan.DBHits, int64(5),
			"the label scan reads five nodes, so it performs at least five storage reads")
		require.GreaterOrEqual(t, plan.TotalDBHits, int64(5),
			"a five-node label scan must report at least one storage read per scanned node")
	})
}

// TestExplainDoesNotClaimIndexSeekWithoutIndex reproduces the text-derived plan
// report: the pattern's property map alone makes EXPLAIN claim an index seek
// although the schema has no index for the label.
func TestExplainDoesNotClaimIndexSeekWithoutIndex(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "explain_no_index")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// No CREATE INDEX anywhere: nothing can serve the property predicate.
	result, err := exec.Execute(ctx, `EXPLAIN MATCH (n:Person {name: 'Ada'}) RETURN n`, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	plan := executionPlanFromResult(t, result)
	require.NotContains(t, collectOperatorTypes(plan.Root), "NodeIndexSeek",
		"EXPLAIN must not claim an index seek when the schema holds no index for :Person")
	require.NotNil(t, planOperatorOfType(plan.Root, "NodeByLabelScan"),
		"without an applicable index the plan scans the label")
}

// TestExplainDoesNotClaimIndexSeekForUnindexedProperty reproduces the same
// defect with an index present but not applicable: :Person(name) is indexed,
// the pattern filters on :Person(age).
func TestExplainDoesNotClaimIndexSeekForUnindexedProperty(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "explain_misfit_index")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE INDEX idx_person_name FOR (n:Person) ON (n.name)`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `EXPLAIN MATCH (n:Person {age: 30}) RETURN n`, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	plan := executionPlanFromResult(t, result)
	require.NotContains(t, collectOperatorTypes(plan.Root), "NodeIndexSeek",
		"EXPLAIN must not claim an index seek for :Person(age) when only :Person(name) is indexed")
	require.NotNil(t, planOperatorOfType(plan.Root, "NodeByLabelScan"),
		"without an applicable index the plan scans the label")
}

// TestExplainClaimsIndexSeekForIndexedProperty is the positive control for the
// two tests above: with an index on the filtered property, the plan must report
// the seek. It passes today and must keep passing, so a fix cannot satisfy the
// negative cases by never reporting a seek.
func TestExplainClaimsIndexSeekForIndexedProperty(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "explain_applicable_index")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE INDEX idx_person_name FOR (n:Person) ON (n.name)`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `EXPLAIN MATCH (n:Person {name: 'Ada'}) RETURN n`, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	plan := executionPlanFromResult(t, result)
	seek := planOperatorOfType(plan.Root, "NodeIndexSeek")
	require.NotNil(t, seek, "an indexed property predicate must plan an index seek")
	require.Equal(t, "Person", seek.Arguments["label"])
	require.Equal(t, "available", seek.Arguments["indexStatus"],
		"the seek is only legitimate when the schema reports an available index")
}

// TestExplainClaimsIndexSeekForWhereEquality is the WHERE-clause positive
// control: the pattern carries no property map, but WHERE filters the indexed
// property, so the executor's canonical equality planner seeds from the index
// and EXPLAIN must report the seek.
func TestExplainClaimsIndexSeekForWhereEquality(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "explain_where_index")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE INDEX idx_person_name_where FOR (n:Person) ON (n.name)`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `EXPLAIN MATCH (n:Person) WHERE n.name = 'Ada' RETURN n`, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	plan := executionPlanFromResult(t, result)
	seek := planOperatorOfType(plan.Root, "NodeIndexSeek")
	require.NotNil(t, seek, "a WHERE equality on an indexed property must plan an index seek")
	require.Equal(t, "Person", seek.Arguments["label"])
	require.Equal(t, "available", seek.Arguments["indexStatus"])
}

// TestExplainDoesNotClaimIndexSeekForUnindexedWhereProperty is the WHERE
// counterpart of the unrelated-property case: :Person(name) is indexed, but
// the WHERE filters :Person(age).
func TestExplainDoesNotClaimIndexSeekForUnindexedWhereProperty(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "explain_where_misfit")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE INDEX idx_person_name_where_misfit FOR (n:Person) ON (n.name)`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `EXPLAIN MATCH (n:Person) WHERE n.age = 30 RETURN n`, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	plan := executionPlanFromResult(t, result)
	require.NotContains(t, collectOperatorTypes(plan.Root), "NodeIndexSeek",
		"EXPLAIN must not claim an index seek for :Person(age) when only :Person(name) is indexed")
	require.NotNil(t, planOperatorOfType(plan.Root, "NodeByLabelScan"),
		"without an applicable index the plan scans the label")
}

// TestExplainClaimsIndexSeekForCompositeKey is the composite-key positive
// control: the pattern's equality prefix is covered by a declared composite
// index, which the canonical inline-property planner reads as one lookup.
func TestExplainClaimsIndexSeekForCompositeKey(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "explain_composite_index")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE INDEX idx_person_name_pair FOR (n:Person) ON (n.firstName, n.lastName)`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `EXPLAIN MATCH (n:Person {firstName: 'Ada', lastName: 'Lovelace'}) RETURN n`, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	plan := executionPlanFromResult(t, result)
	seek := planOperatorOfType(plan.Root, "NodeIndexSeek")
	require.NotNil(t, seek, "a composite index covering the pattern's equality prefix must plan an index seek")
	require.Equal(t, "Person", seek.Arguments["label"])
	require.Equal(t, "available", seek.Arguments["indexStatus"])
}

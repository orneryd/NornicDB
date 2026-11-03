package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestUnwind_MatchedPerItemBoundExecution pins the UNWIND migration (§6.2):
// unwound rows travel as value bindings + parameters, never as query-text
// substitution, so hostile values (quotes, punctuation) and map property
// access resolve with their real Go values through the per-item MATCH path.
func TestUnwind_MatchedPerItemBoundExecution(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:Customer {customerID: 1})`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
UNWIND $rows AS row
MATCH (c:Customer {customerID: row.customerID})
CREATE (o:Order {orderID: row.orderID, note: row.notes})
RETURN o.orderID AS id, o.note AS note
ORDER BY id
`, map[string]interface{}{
		"rows": []interface{}{
			map[string]interface{}{"customerID": int64(1), "orderID": int64(9001), "notes": "O'Brien;2"},
			map[string]interface{}{"customerID": int64(1), "orderID": int64(9002), "notes": "a, b"},
		},
	})
	require.NoError(t, err)

	got, err := exec.Execute(ctx, `MATCH (o:Order) RETURN o.orderID, o.note ORDER BY o.orderID`, nil)
	require.NoError(t, err)
	require.Len(t, got.Rows, 2)
	require.EqualValues(t, 9001, got.Rows[0][0])
	require.Equal(t, "O'Brien;2", got.Rows[0][1])
	require.EqualValues(t, 9002, got.Rows[1][0])
	require.Equal(t, "a, b", got.Rows[1][1])
}

func TestUnwind_BoundExecutionCancellationBeforeWrites(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(context.Background(), "CREATE (:Customer {customerID: 1})", nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = exec.Execute(ctx, "UNWIND $rows AS row MATCH (c:Customer {customerID: row.customerID}) CREATE (o:Order {orderID: row.orderID}) RETURN o.orderID", map[string]interface{}{"rows": []interface{}{map[string]interface{}{"customerID": int64(1), "orderID": int64(9001)}}})
	require.ErrorIs(t, err, context.Canceled)
	_, err = exec.executeInternal(ctx, "CREATE (:Order {orderID: 9002})", nil)
	require.ErrorIs(t, err, context.Canceled)
	result, err := exec.Execute(context.Background(), "MATCH (o:Order) RETURN count(o) AS count", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}

func TestUnwind_MergeStopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &cancelOnFirstCreateEngine{Engine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel_unwind"), cancel: cancel}
	exec := NewStorageExecutor(store)
	_, err := exec.executeRequiredPipeline(ctx, "UNWIND [1, 2] AS row MERGE (n:CancelUnwind {value: row})")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, store.creates)
}

func TestUnwind_MergeChainStopsWithinRowAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &cancelOnFirstCreateEngine{Engine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel_chain"), cancel: cancel}
	exec := NewStorageExecutor(store)
	_, err := exec.executeRequiredPipeline(ctx, "UNWIND [1] AS row MERGE (a:CancelChainA {value: row}) MERGE (b:CancelChainB {value: row}) RETURN count(b) AS count")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, store.creates)
}

func TestUnwind_ProjectedMergeStopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &cancelOnFirstCreateEngine{Engine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel_projected_unwind"), cancel: cancel}
	exec := NewStorageExecutor(store)
	_, err := exec.executeRequiredPipeline(ctx, "UNWIND [1, 2] AS row MERGE (n:CancelUnwind {value: row}) RETURN n.value AS value")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, store.creates)
}

// pipelineSetCancelQuery evaluates the gate after the row's MATCH read, while
// the SET's value is computed: the loop's cancellation lands there.
const pipelineSetCancelQuery = "UNWIND [1, 2, 3] AS x CREATE (n:CancelSet {v: x}) WITH x MATCH (n:CancelSet {v: x}) SET n.v = " + statementGateFunction + "(x * 10)"

// pipelineCreateCancelQuery evaluates the gate while the UNWIND + CREATE batch
// plans each row's property map. The batch plans every row before it writes
// any, so this gate fires before the statement's first write.
const pipelineCreateCancelQuery = "UNWIND [1, 2, 3] AS x CREATE (n:CancelCreate {v: x, tick: " + statementGateFunction + "(x)})"

// pipelineCreateStagedRowQuery creates each row's node in its own CREATE after
// a WITH, so the gate on the second row runs after the first row's create is
// already staged in the statement's transaction.
const pipelineCreateStagedRowQuery = "UNWIND [1, 2, 3] AS x CREATE (n:CancelStagedA {v: x}) WITH x CREATE (n:CancelStagedB {v: x, tick: " + statementGateFunction + "(x)})"

// A canceled pipeline SET stops at the row whose value evaluation observed the
// cancellation and rolls the whole statement back, so no row of the statement's
// earlier CREATE survives either. The cancellation here arrives at the first
// row, before any of the statement's SET writes.
func TestPipeline_SetStopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel_pipeline_set")
	exec := NewStorageExecutor(store)
	held := newStatementGate(func(context.Context) error {
		cancel()
		return nil
	}).install(t)

	_, err := exec.Execute(ctx, pipelineSetCancelQuery, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(1), held.Calls(), "the pipeline evaluated no row after the canceled one")
	require.Equal(t, [][]interface{}{{int64(0)}}, countRows(t, exec, "CancelSet"), "the canceled statement rolled back the nodes its earlier CREATE staged")
}

// The original causal case for the SET family, preserved: the cancellation
// arrives at the second row, so the first row's update is already applied in
// the statement's transaction. The case reads the pending rows through that
// transaction — and reads committed storage through a separate executor —
// before it cancels, then requires that no later row ran and that the statement
// rolled the applied update back.
func TestPipeline_SetRollsBackTheStagedUpdateItCanceledAfter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel_pipeline_set_staged")
	exec := NewStorageExecutor(store)
	committed := NewStorageExecutor(store)
	var staged []*storage.Node
	held := newStatementGateAt(2, func(workCtx context.Context) error {
		nodes, err := stagedWriteBeforeCancel(workCtx, committed, "CancelSet", 3)
		if err != nil {
			return err
		}
		staged = nodes
		cancel()
		return nil
	}).install(t)

	_, err := exec.Execute(ctx, pipelineSetCancelQuery, nil)
	require.NoError(t, held.Err(), "the staged-state reads must reach the case before the cancellation")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(2), held.Calls(), "the pipeline evaluated no row after the row whose cancellation arrived")

	// The first row's update is the one write already applied: its stored value
	// is the updated one, and the rows after it still carry their CREATE value.
	require.Len(t, staged, 3)
	updated, unchanged := 0, 0
	for _, node := range staged {
		value, ok := node.Properties["v"].(int64)
		if !ok {
			continue
		}
		switch value {
		case 10:
			updated++
		case 2, 3:
			unchanged++
		}
	}
	require.Equal(t, 1, updated, "exactly the first row's update was applied before the cancellation")
	require.Equal(t, 2, unchanged, "the later rows' updates had not run yet")
	require.Equal(t, [][]interface{}{{int64(0)}}, countRows(t, exec, "CancelSet"), "the native transaction rolled the applied update back")
}

// The sensitivity control: without a cancellation every row is updated.
func TestPipeline_SetUpdatesEveryRowWithoutCancellation(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel_pipeline_set")
	exec := NewStorageExecutor(store)
	held := newStatementGate(nil).install(t)

	_, err := exec.Execute(context.Background(), pipelineSetCancelQuery, nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), held.Calls(), "every pipeline row is evaluated")

	result, err := exec.Execute(context.Background(), "MATCH (n:CancelSet) RETURN n.v ORDER BY n.v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(10)}, {int64(20)}, {int64(30)}}, result.Rows)
}

// A canceled UNWIND + CREATE batch reports the cancellation to the caller and
// writes nothing: the statement's atomic transaction owns the staged rows, so
// the client learns a definite outcome instead of an uncertain one. The batch
// plans its rows before it applies them, so the gate fires before the first
// write.
func TestPipeline_CreateStopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel_pipeline_create")
	exec := NewStorageExecutor(store)
	held := newStatementGate(func(context.Context) error {
		cancel()
		return nil
	}).install(t)

	_, err := exec.Execute(ctx, pipelineCreateCancelQuery, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(1), held.Calls(), "the batch planned no row after the canceled one")
	require.Equal(t, [][]interface{}{{int64(0)}}, countRows(t, exec, "CancelCreate"), "the canceled statement rolled its staged rows back")
}

// The original causal case for the CREATE family, preserved: the cancellation
// arrives at the second row, so the first row's create is already staged in the
// statement's transaction. The case reads that pending row through the
// statement's own transaction — and reads committed storage through a separate
// executor — before it cancels, then requires that no later row ran and that the
// statement rolled the staged row back.
func TestPipeline_CreateRollsBackTheStagedRowItCanceledAfter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel_pipeline_staged")
	exec := NewStorageExecutor(store)
	committed := NewStorageExecutor(store)
	var staged []*storage.Node
	held := newStatementGateAt(2, func(workCtx context.Context) error {
		nodes, err := stagedWriteBeforeCancel(workCtx, committed, "CancelStagedB", 1)
		if err != nil {
			return err
		}
		staged = nodes
		cancel()
		return nil
	}).install(t)

	_, err := exec.Execute(ctx, pipelineCreateStagedRowQuery, nil)
	require.NoError(t, held.Err(), "the staged-state reads must reach the case before the cancellation")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(2), held.Calls(), "the pipeline evaluated no row after the row whose cancellation arrived")

	// Exactly the first row's create of the gated clause was staged and
	// uncommitted when the cancellation arrived.
	require.Len(t, staged, 1)
	require.EqualValues(t, 1, staged[0].Properties["v"])

	for _, label := range []string{"CancelStagedA", "CancelStagedB"} {
		require.Equal(t, [][]interface{}{{int64(0)}}, countRows(t, exec, label), "the native transaction rolled back "+label)
	}
}

// The sensitivity control: without a cancellation every planned row commits.
func TestPipeline_CreateWritesEveryRowWithoutCancellation(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel_pipeline_create")
	exec := NewStorageExecutor(store)
	held := newStatementGate(nil).install(t)

	_, err := exec.Execute(context.Background(), pipelineCreateCancelQuery, nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), held.Calls(), "every planned row is evaluated")
	require.Equal(t, [][]interface{}{{int64(3)}}, countRows(t, exec, "CancelCreate"))
}

// TestPipeline_CreateCancelledBeforeApplyWritesNothing pins the pre-apply
// cancellation check: the batch is planned row by row, so a canceled
// context stops planning before any row is written.
func TestPipeline_CreateCancelledBeforeApplyWritesNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled before Execute
	store := &cancelOnFirstCreateEngine{Engine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel_pipeline_create"), cancel: func() {}}
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, "UNWIND [1, 2, 3] AS x CREATE (n:CancelCreate {v: x})", nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, store.creates)
}

// TestUnwind_MutationBoundExecutionHostileValues pins the UNWIND mutation
// migration (§6.2): MERGE/SET over unwound rows run against bound child
// contexts, so hostile values survive without query-text substitution and
// without the old WITH/UNWIND "{}"-collapse guards.
func TestUnwind_MutationBoundExecutionHostileValues(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (o:HostileRow {textKey: row.textKey})
ON CREATE SET o.note = row.note, o.weight = row.weight
`, map[string]interface{}{
		"rows": []interface{}{
			map[string]interface{}{"textKey": "k1", "note": "O'Brien;2", "weight": int64(3)},
			map[string]interface{}{"textKey": "k2", "note": "a, b", "weight": int64(5)},
		},
	})
	require.NoError(t, err)

	got, err := exec.Execute(ctx, `MATCH (o:HostileRow) RETURN o.textKey, o.note, o.weight ORDER BY o.textKey`, nil)
	require.NoError(t, err)
	require.Len(t, got.Rows, 2)
	require.Equal(t, "k1", got.Rows[0][0])
	require.Equal(t, "O'Brien;2", got.Rows[0][1])
	require.EqualValues(t, 3, got.Rows[0][2])
	require.Equal(t, "k2", got.Rows[1][0])
	require.Equal(t, "a, b", got.Rows[1][1])
	require.EqualValues(t, 5, got.Rows[1][2])
}

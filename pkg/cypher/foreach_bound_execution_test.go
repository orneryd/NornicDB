package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type cancelOnFirstCreateEngine struct {
	storage.Engine
	cancel  context.CancelFunc
	creates int
}

func TestForeachRejectsMalformedBodyBeforeRows(t *testing.T) {
	for _, populated := range []bool{false, true} {
		for _, body := range []string{"CREATE (w:W {id:i}) SET w.y = 1 garbage here", "CREATE (w:W {id:i}) SET w.y = "} {
			t.Run(fmt.Sprintf("populated=%v/body=%s", populated, body), func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
				if populated {
					_, err := exec.Execute(context.Background(), "CREATE (:T {id:1})", nil)
					require.NoError(t, err)
				}
				_, err := exec.Execute(context.Background(), "MATCH (t:T) FOREACH (i IN [1,2] | "+body+") RETURN t.id", nil)
				var classified interface{ BoltErrorCode() string }
				require.ErrorAs(t, err, &classified)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", classified.BoltErrorCode())
				result, err := exec.Execute(context.Background(), "MATCH (n:W) RETURN count(n)", nil)
				require.NoError(t, err)
				require.Equal(t, int64(0), result.Rows[0][0])
			})
		}
	}
}

func (engine *cancelOnFirstCreateEngine) CreateNode(ctx context.Context, node *storage.Node) (storage.NodeID, error) {
	id, err := engine.Engine.CreateNode(ctx, node)
	if err == nil {
		engine.creates++
		if engine.creates == 1 {
			engine.cancel()
		}
	}
	return id, err
}

// BulkCreateNodes counts the batched rows and cancels the context, so a
// UNWIND + CREATE batch (which plans all rows into one bulk call) observes
// the cancellation only after its atomic apply.
func (engine *cancelOnFirstCreateEngine) BulkCreateNodes(ctx context.Context, nodes []*storage.Node) error {
	engine.creates += len(nodes)
	engine.cancel()
	return engine.Engine.BulkCreateNodes(ctx, nodes)
}

func TestForeach_MergeStopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &cancelOnFirstCreateEngine{Engine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel_foreach"), cancel: cancel}
	exec := NewStorageExecutor(store)
	_, err := exec.executeRequiredPipeline(ctx, "FOREACH (x IN [1, 2] | MERGE (n:CancelMerge {value: x}))")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, store.creates)
}

// cancelForeachRowQuery creates one node per loop row. The gate sits in the
// row's property map, so the statement evaluates it while it builds that row's
// node: after the rows before it and before the rows after it.
const cancelForeachRowQuery = "FOREACH (row IN [1, 2, 3] | CREATE (n:CancelForeach {value: row, tick: " + statementGateFunction + "(row)}))"

// A canceled FOREACH CREATE statement stops at the row whose evaluation
// observed the cancellation, and — because the statement now owns its writes in
// one atomic transaction — leaves no partially created loop row behind. The
// cancellation here arrives at the first row, before any of the statement's
// writes.
func TestForeach_CreateStopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel_foreach_create")
	exec := NewStorageExecutor(store)
	held := newStatementGate(func(context.Context) error {
		cancel()
		return nil
	}).install(t)

	_, err := exec.Execute(ctx, cancelForeachRowQuery, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(1), held.Calls(), "the loop evaluated no row after the canceled one")
	require.Equal(t, [][]interface{}{{int64(0)}}, countRows(t, exec, "CancelForeach"), "the canceled statement rolled its loop rows back")
}

// The original causal case, preserved: the cancellation arrives at the second
// loop row, so the first row's create is already staged in the statement's
// transaction. The case reads that pending row through the statement's own
// transaction — and reads committed storage through a separate executor — before
// it cancels, then requires that no later row ran and that the statement rolled
// the staged row back in its native transaction.
func TestForeach_CreateRollsBackTheStagedRowItCanceledAfter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel_foreach_staged")
	exec := NewStorageExecutor(store)
	committed := NewStorageExecutor(store)
	var staged []*storage.Node
	held := newStatementGateAt(2, func(workCtx context.Context) error {
		nodes, err := stagedWriteBeforeCancel(workCtx, committed, "CancelForeach", 1)
		if err != nil {
			return err
		}
		staged = nodes
		cancel()
		return nil
	}).install(t)

	_, err := exec.Execute(ctx, cancelForeachRowQuery, nil)
	require.NoError(t, held.Err(), "the staged-state reads must reach the case before the cancellation")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(2), held.Calls(), "the loop evaluated no row after the row whose cancellation arrived")

	// Exactly the first row's create was staged and uncommitted when the
	// cancellation arrived.
	require.Len(t, staged, 1)
	require.EqualValues(t, 1, staged[0].Properties["value"])

	require.Equal(t, [][]interface{}{{int64(0)}}, countRows(t, exec, "CancelForeach"), "the native transaction rolled the staged row back")
}

// The sensitivity control: the same statement without a cancellation drives
// every loop row and commits them.
func TestForeach_CreateCommitsEveryRowWithoutCancellation(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel_foreach_create")
	exec := NewStorageExecutor(store)
	held := newStatementGate(nil).install(t)

	_, err := exec.Execute(context.Background(), cancelForeachRowQuery, nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), held.Calls(), "every loop row is evaluated")
	require.Equal(t, [][]interface{}{{int64(3)}}, countRows(t, exec, "CancelForeach"))
}

// TestForeach_BoundExecutionPinsNoSubstitution pins the FOREACH flip (§6.2):
// the loop variable travels as a value binding, so values that are hostile to
// query-text substitution (quotes, identifier-like content) and structured
// map items resolve with their real Go values.
func TestForeach_BoundExecutionPinsNoSubstitution(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Quote-heavy string values survive without text re-entry.
	_, err := exec.Execute(ctx, `FOREACH (x IN ["O'Brien", "a;b"] | CREATE (:QuoteSafe {v: x}))`, nil)
	require.NoError(t, err)
	got, err := exec.Execute(ctx, `MATCH (n:QuoteSafe) RETURN n.v ORDER BY n.v`, nil)
	require.NoError(t, err)
	require.Len(t, got.Rows, 2)
	require.Equal(t, "O'Brien", got.Rows[0][0])
	require.Equal(t, "a;b", got.Rows[1][0])

	// Structured map items resolve through dotted access.
	_, err = exec.Execute(ctx, `FOREACH (x IN [{name: 'ann', age: 3}] | CREATE (:MapItem {name: x.name, age: x.age}))`, nil)
	require.NoError(t, err)
	got, err = exec.Execute(ctx, `MATCH (n:MapItem) RETURN n.name, n.age`, nil)
	require.NoError(t, err)
	require.Len(t, got.Rows, 1)
	require.Equal(t, "ann", got.Rows[0][0])
	require.EqualValues(t, 3, got.Rows[0][1])

	// Typed integer loop values keep their numeric type.
	_, err = exec.Execute(ctx, `FOREACH (x IN [7] | CREATE (:Typed {v: x}))`, nil)
	require.NoError(t, err)
	got, err = exec.Execute(ctx, `MATCH (n:Typed) RETURN n.v`, nil)
	require.NoError(t, err)
	require.Len(t, got.Rows, 1)
	require.EqualValues(t, 7, got.Rows[0][0])
}

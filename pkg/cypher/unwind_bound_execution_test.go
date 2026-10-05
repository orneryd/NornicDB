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

type cancelOnFirstUpdateEngine struct {
	storage.Engine
	cancel  context.CancelFunc
	updates int
}

func (engine *cancelOnFirstUpdateEngine) UpdateNode(node *storage.Node) error {
	err := engine.Engine.UpdateNode(node)
	if err == nil {
		engine.updates++
		if engine.updates == 1 {
			engine.cancel()
		}
	}
	return err
}

func TestPipeline_SetStopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &cancelOnFirstUpdateEngine{Engine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel_pipeline_set"), cancel: cancel}
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, "UNWIND [1, 2, 3] AS x CREATE (n:CancelSet {v: x}) WITH x MATCH (n:CancelSet {v: x}) SET n.v = x * 10", nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, store.updates)
}

func TestPipeline_CreateStopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &cancelOnFirstCreateEngine{Engine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel_pipeline_create"), cancel: cancel}
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, "UNWIND [1, 2, 3] AS x CREATE (n:CancelCreate {v: x})", nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, store.creates)
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

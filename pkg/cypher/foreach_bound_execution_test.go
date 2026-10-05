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
				require.ErrorContains(t, err, "SyntaxError")
				result, err := exec.Execute(context.Background(), "MATCH (n:W) RETURN count(n)", nil)
				require.NoError(t, err)
				require.Equal(t, int64(0), result.Rows[0][0])
			})
		}
	}
}

func (engine *cancelOnFirstCreateEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	id, err := engine.Engine.CreateNode(node)
	if err == nil {
		engine.creates++
		if engine.creates == 1 {
			engine.cancel()
		}
	}
	return id, err
}

func TestForeach_MergeStopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &cancelOnFirstCreateEngine{Engine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel_foreach"), cancel: cancel}
	exec := NewStorageExecutor(store)
	_, err := exec.executeRequiredPipeline(ctx, "FOREACH (x IN [1, 2] | MERGE (n:CancelMerge {value: x}))")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, store.creates)
}

func TestForeach_CreateStopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &cancelOnFirstCreateEngine{Engine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "cancel_foreach_create"), cancel: cancel}
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, "FOREACH (row IN [1, 2, 3] | CREATE (n:CancelForeach {value: row}))", nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, store.creates)
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

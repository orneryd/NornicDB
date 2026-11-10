package cypher

// The two bounded controls for no-memo ResultStream row ownership.
//
// Control 1 is the metadata control: a cache-disabled memory-backed executor
// (no statement property-view memo) streams an ordered literal map whose root
// and nested key orders must be found in the returned execution result's
// MapKeyOrders by the identities of the maps the consumer actually received.
// The control passes the uncorrected source, because there the delivered maps
// are the maps whose order was recorded, and it fails a correction that copies
// rows without rebinding their map orders.
//
// Control 2 is the hook/admission control: a supported value implementing
// CallerCopy cancels the statement's context while it is copied. emit must run
// that copy hook, refuse the row with the canceled context, and leave no
// buffered or tail row behind. The uncorrected source does not copy no-memo
// rows, so it never runs the hook and admits the row.

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// cancelOnStreamValueCopy is a supported result value: storage.CopyValue asks a
// value implementing CallerCopy for its own copy, so the copy hook is the one
// place ownership of this value changes hands. The copy cancels the statement's
// context, which makes that hook's effect observable.
type cancelOnStreamValueCopy struct {
	cancel context.CancelFunc
	copies *atomic.Int32
}

func (v *cancelOnStreamValueCopy) CallerCopy() any {
	v.copies.Add(1)
	v.cancel()
	owned := *v
	return &owned
}

// TestPublicExecuteStreamedNoMemoMapOrdersFollowDeliveredRows is the metadata
// control: the streamed rows keep their values and their map key order
// metadata, keyed by the map identities the client received.
func TestPublicExecuteStreamedNoMemoMapOrdersFollowDeliveredRows(t *testing.T) {
	// A memory-backed store has no statement property-view memo, and the
	// explicitly disabled result cache keeps every read a fresh execution.
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor438"),
		QueryCachePolicy{MaxEntries: 0, TTL: 0})
	_, err := exec.Execute(context.Background(), "CREATE (n:Nor307Ownership {name: 'alpha', tags: ['a', 'b']})", nil)
	require.NoError(t, err)

	const query = "MATCH (n:Nor307Ownership) UNWIND [1,2,3,4,5,6] AS i RETURN {node:n,nested:{b:2,a:1}} AS value"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	run := executeStreamed(ctx, exec, query, 1)
	require.True(t, run.stream.streamed(), "the six-row read must stream: %v", run.errIfDone())
	rows, exhausted := run.stream.Next(-1)
	require.True(t, exhausted)
	<-run.stream.Done()
	require.NoError(t, run.err)

	require.NotNil(t, run.result)
	require.Equal(t, 6, len(rows), "an ordinary streamed read returns every row")

	identity := func(value map[string]interface{}) uintptr {
		return uintptr(reflect.ValueOf(value).UnsafePointer())
	}
	for index, row := range rows {
		require.Len(t, row, 1)
		value, ok := row[0].(map[string]interface{})
		require.True(t, ok, "row %d returns the projected map", index)

		node, ok := value["node"].(*storage.Node)
		require.True(t, ok, "row %d keeps its returned node", index)
		require.Equal(t, "alpha", node.Properties["name"], "row %d keeps the fresh node properties", index)
		require.Equal(t, []string{"a", "b"}, node.Properties["tags"], "row %d keeps the fresh node properties", index)
		require.Equal(t, []string{"Nor307Ownership"}, node.Labels)

		nested, ok := value["nested"].(map[string]interface{})
		require.True(t, ok, "row %d keeps its nested map", index)
		require.Equal(t, int64(2), nested["b"])
		require.Equal(t, int64(1), nested["a"])

		require.Equal(t, []string{"node", "nested"}, run.result.MapKeyOrders[identity(value)],
			"row %d's delivered map keeps its evaluated order", index)
		require.Equal(t, []string{"b", "a"}, run.result.MapKeyOrders[identity(nested)],
			"row %d's delivered nested map keeps its evaluated order", index)
	}
}

// TestResultStreamEmitCopiesRowAndStopsOnCanceledContext is the hook/admission
// control: the row's supported copy hook runs during emit, and the context it
// canceled stops the row before it enters either buffer.
func TestResultStreamEmitCopiesRowAndStopsOnCanceledContext(t *testing.T) {
	exec := nor307PublicExecutor(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hook := &cancelOnStreamValueCopy{cancel: cancel, copies: new(atomic.Int32)}
	stream := NewResultStream(1, nil)

	emitted := stream.emit(ctx, exec, []interface{}{hook})

	require.Equal(t, int32(1), hook.copies.Load(), "emit must run the row's copy hook exactly once")
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.False(t, emitted, "the canceled context must stop the row")
	require.Empty(t, stream.buffered, "a row the copy canceled its context for is not buffered")
	require.Empty(t, stream.tail, "a row the copy canceled its context for is not queued for a batch")
	require.False(t, stream.streamed())
}

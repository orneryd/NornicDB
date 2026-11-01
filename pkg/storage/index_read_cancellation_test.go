package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Cancel the real context on a deterministic poll, preserving the relationship
// between Err and Done rather than inventing an error from a live context.
type indexPollCancelContext struct {
	context.Context
	cancel    context.CancelFunc
	remaining atomic.Int64
}

func newIndexPollCancelContext(t *testing.T, polls int64) *indexPollCancelContext {
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	c := &indexPollCancelContext{Context: ctx, cancel: cancel}
	c.remaining.Store(polls)
	return c
}
func (c *indexPollCancelContext) Err() error {
	if c.remaining.Add(-1) == 0 {
		c.cancel()
	}
	return c.Context.Err()
}

// Done is evaluated when admission reaches its actual cancellable wait.
type indexWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *indexWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestIndexReadPreCanceledAndRetry(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("cancel_rank", "Item", []string{"rank"}))
	sm.PropertyIndexInsert("Item", "rank", "n", int64(1))
	idx, ok := sm.GetCompositeIndex("cancel_rank")
	require.True(t, ok)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for name, read := range map[string]func(context.Context) ([]NodeID, error){
		"full":   func(ctx context.Context) ([]NodeID, error) { return idx.LookupFull(ctx, int64(1)) },
		"prefix": func(ctx context.Context) ([]NodeID, error) { return idx.LookupPrefix(ctx, int64(1)) },
		"filter": func(ctx context.Context) ([]NodeID, error) {
			return idx.LookupWithFilter(ctx, func(NodeID) bool { return true }, int64(1))
		},
		"property": func(ctx context.Context) ([]NodeID, error) {
			return sm.PropertyIndexLookup(ctx, "Item", "rank", int64(1))
		},
		"any label": func(ctx context.Context) ([]NodeID, error) {
			return sm.PropertyIndexLookupAnyLabel(ctx, "rank", int64(1))
		},
		"top k": func(ctx context.Context) ([]NodeID, error) {
			return sm.PropertyIndexTopK(ctx, "Item", "rank", 1, false)
		},
		"all": func(ctx context.Context) ([]NodeID, error) {
			return sm.PropertyIndexAllNonNil(ctx, "Item", "rank", false)
		},
	} {
		t.Run(name, func(t *testing.T) {
			ids, err := read(ctx)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, ids)
			ids, err = read(t.Context())
			require.NoError(t, err)
			require.Equal(t, []NodeID{"n"}, ids)
		})
	}
	ids, available, err := sm.PropertyIndexRange(ctx, "Item", "rank", PropertyIndexBounds{})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, ids)
	_ = available // Availability is meaningful only after a successful read.
	visited := false
	_, err = sm.VisitPropertyIndexGroups(ctx, "Item", "rank", false, func([]NodeID) bool { visited = true; return true })
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, visited)
	_, err = sm.VisitPropertyIndexGroupsInRange(ctx, "Item", "rank", false, PropertyIndexBounds{}, func([]NodeID) bool { visited = true; return true })
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, visited)
}

func TestIndexReadCancellationDuringCopyFilterAndOrder(t *testing.T) {
	const count = 32768
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("copy_value", "Copy", []string{"value"}))
	require.NoError(t, sm.AddPropertyIndex("order_value", "Ordered", []string{"value"}))
	copyIndex, ok := sm.GetCompositeIndex("copy_value")
	require.True(t, ok)
	want := make([]NodeID, count)
	for i := range want {
		want[i] = NodeID(fmt.Sprintf("node:%05d", i))
		sm.PropertyIndexInsert("Copy", "value", want[i], int64(1))
		sm.PropertyIndexInsert("Ordered", "value", want[i], int64(count-i))
	}
	t.Run("copy", func(t *testing.T) {
		ctx := newIndexPollCancelContext(t, 16)
		ids, err := copyIndex.LookupFull(ctx, int64(1))
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, ids)
		require.ErrorIs(t, ctx.Context.Err(), context.Canceled)
		ids, err = copyIndex.LookupFull(t.Context(), int64(1))
		require.NoError(t, err)
		require.Equal(t, want, ids)
	})
	t.Run("filter", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		visited := 0
		ids, err := copyIndex.LookupWithFilter(ctx, func(NodeID) bool {
			visited++
			if visited == 256 {
				cancel()
			}
			return true
		}, int64(1))
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, ids)
		require.GreaterOrEqual(t, visited, 256)
		require.Less(t, visited, count)
		ids, err = copyIndex.LookupWithFilter(t.Context(), func(NodeID) bool { return true }, int64(1))
		require.NoError(t, err)
		require.Equal(t, want, ids)
	})
	t.Run("order cache retry", func(t *testing.T) {
		ctx := newIndexPollCancelContext(t, 16)
		ids, err := sm.PropertyIndexAllNonNil(ctx, "Ordered", "value", false)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, ids)
		require.ErrorIs(t, ctx.Context.Err(), context.Canceled)
		ids, err = sm.PropertyIndexAllNonNil(t.Context(), "Ordered", "value", false)
		require.NoError(t, err)
		require.Len(t, ids, count)
		for i, id := range ids {
			require.Equal(t, want[count-1-i], id)
		}
		again, err := sm.PropertyIndexAllNonNil(t.Context(), "Ordered", "value", false)
		require.NoError(t, err)
		require.Equal(t, ids, again)
	})
}

func TestIndexReadCanceledLockWaiter(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("wait_value", "Item", []string{"value"}))
	sm.PropertyIndexInsert("Item", "value", "node", int64(1))
	idx, ok := sm.GetCompositeIndex("wait_value")
	require.True(t, ok)
	for _, target := range []string{"schema", "index"} {
		t.Run(target, func(t *testing.T) {
			base, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctx := &indexWaitContext{Context: base, entered: make(chan struct{})}
			if target == "schema" {
				sm.mu.Lock()
			} else {
				idx.mu.Lock()
			}
			unlock := func() {
				if target == "schema" {
					sm.mu.Unlock()
				} else {
					idx.mu.Unlock()
				}
			}
			var once sync.Once
			defer once.Do(unlock)
			type result struct {
				ids []NodeID
				err error
			}
			done := make(chan result, 1)
			go func() { ids, err := sm.PropertyIndexLookup(ctx, "Item", "value", int64(1)); done <- result{ids, err} }()
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()
			select {
			case <-ctx.entered:
			case <-timer.C:
				cancel()
				once.Do(unlock)
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("lookup did not join after admission timeout")
				}
				t.Fatal("lookup did not reach held-lock wait")
			}
			cancel()
			select {
			case got := <-done:
				require.ErrorIs(t, got.err, context.Canceled)
				require.Nil(t, got.ids)
			case <-timer.C:
				once.Do(unlock)
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("lookup did not join after lock release")
				}
				t.Fatal("canceled waiter did not leave while writer retained lock")
			}
			once.Do(unlock)
			ids, err := sm.PropertyIndexLookup(t.Context(), "Item", "value", int64(1))
			require.NoError(t, err)
			require.Equal(t, []NodeID{"node"}, ids)
		})
	}
}

func TestPendingIndexCanceledPopulationAndRetry(t *testing.T) {
	const count = 32768
	nodes := make(map[NodeID]*Node, count)
	pending := newPendingNodeIndex()
	want := make([]NodeID, 0, count)
	for i := 0; i < count; i++ {
		id := NodeID(fmt.Sprintf("pending:%05d", i))
		node := &Node{ID: id, Labels: []string{"Item"}, Properties: map[string]interface{}{"value": int64(1)}}
		nodes[id] = node
		pending.add(node)
		want = append(want, id)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reads := 0
	err := pending.track(ctx, "Item", "value", func(id NodeID) *Node {
		reads++
		if reads == 256 {
			cancel()
		}
		return nodes[id]
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, reads, count)
	require.NoError(t, pending.track(t.Context(), "Item", "value", func(id NodeID) *Node { return nodes[id] }))
	valueKey, ok := indexValueKey(int64(1))
	require.True(t, ok)
	require.ElementsMatch(t, want, pending.byValue[pendingValueKey{label: "Item", property: "value", value: valueKey}])
	tx := &BadgerTransaction{pendingNodes: nodes}
	preCanceled, stop := context.WithCancel(t.Context())
	stop()
	privateIDs, privateErr := tx.pendingValueMatchesLocked(preCanceled, "Item", "value", valueKey)
	require.ErrorIs(t, privateErr, context.Canceled)
	require.Nil(t, privateIDs)
	poll := newIndexPollCancelContext(t, 16)
	ids, err := tx.MergePendingPropertyMatches(poll, []NodeID{"committed:kept"}, "Item", "value", int64(1))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, ids)
	ids, err = tx.MergePendingPropertyMatches(t.Context(), []NodeID{"committed:kept"}, "Item", "value", int64(1))
	require.NoError(t, err)
	require.ElementsMatch(t, append(want, "committed:kept"), ids)
	privateIDs, privateErr = tx.pendingValueMatchesLocked(t.Context(), "Item", "value", valueKey)
	require.NoError(t, privateErr)
	require.ElementsMatch(t, want, privateIDs)
}

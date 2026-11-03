package storage

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Group output cardinality is reported explicitly: the pre-fix buffered group
// benchmark omits pending rows, so it is diagnostic cost, not a correct baseline.
func BenchmarkOrderedIndexSnapshot(b *testing.B) {
	for _, pending := range []int{0, 256, 4096} {
		b.Run(fmt.Sprintf("pending=%d", pending), func(b *testing.B) {
			sm := NewSchemaManager()
			if err := sm.AddPropertyIndex("ordered", "Item", []string{"rank"}); err != nil {
				b.Fatal(err)
			}
			for i := range 1024 {
				if err := sm.PropertyIndexInsert("Item", "rank", NodeID(fmt.Sprintf("nornic:c%d", i)), int64(i%64)); err != nil {
					b.Fatal(err)
				}
			}
			w := &WriteBehindBuffer{active: newCommitBuffer()}
			for i := range pending {
				w.active.addNode(&Node{ID: NodeID(fmt.Sprintf("nornic:p%d", i)), Labels: []string{"Item"}, Properties: map[string]interface{}{"rank": int64(i % 64)}})
			}
			sm.attachPendingWrites(w, "nornic")
			ctx := b.Context()
			for _, kind := range []string{"groups", "all", "top5"} {
				b.Run(kind, func(b *testing.B) {
					b.ReportAllocs()
					count := 0
					b.ResetTimer()
					for range b.N {
						switch kind {
						case "groups":
							count = 0
							found, err := sm.VisitPropertyIndexGroups(ctx, "Item", "rank", false, func(ids []NodeID) bool { count += len(ids); return true })
							if err != nil || !found {
								b.Fatalf("found=%v err=%v", found, err)
							}
						case "all":
							ids, err := sm.PropertyIndexAllNonNil(ctx, "Item", "rank", false)
							if err != nil {
								b.Fatal(err)
							}
							count = len(ids)
							if count != 1024+pending {
								b.Fatalf("wrong population %d", count)
							}
						case "top5":
							ids, err := sm.PropertyIndexTopK(ctx, "Item", "rank", 5, false)
							if err != nil {
								b.Fatal(err)
							}
							count = len(ids)
							if count != 5 {
								b.Fatalf("wrong limit %d", count)
							}
						}
					}
					b.ReportMetric(float64(count), "ids/op")
				})
			}
		})
	}
}

func TestOrderedIndexSnapshotMergesPendingMutations(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("ordered", "Item", []string{"rank"}))
	nodes := map[NodeID]*Node{}
	for i := 1; i <= 7; i++ {
		rank := interface{}(int64(i))
		if i >= 6 {
			rank = float64(2)
		}
		n := &Node{ID: NodeID(fmt.Sprintf("test:c%d", i)), Labels: []string{"Item"}, Properties: map[string]interface{}{"rank": rank}}
		nodes[n.ID] = n
		require.NoError(t, sm.PropertyIndexInsert("Item", "rank", n.ID, rank))
	}
	w := &WriteBehindBuffer{active: newCommitBuffer()}
	sm.attachPendingWrites(w, "test")
	changed := func(id NodeID, labels []string, rank interface{}) Operation {
		return Operation{Type: OpUpdateNode, OldNode: nodes[id], Node: &Node{ID: id, Labels: labels, Properties: map[string]interface{}{"rank": rank}}}
	}
	w.AppendCommit(bufferedCommit{ops: []Operation{
		changed("test:c1", []string{"Item"}, int64(6)), changed("test:c2", []string{"Item"}, int64(0)),
		{Type: OpDeleteNode, NodeID: "test:c3", OldNode: nodes["test:c3"]}, changed("test:c4", []string{"Other"}, int64(4)), changed("test:c5", []string{"Item"}, nil),
		{Type: OpCreateNode, Node: &Node{ID: "test:p", Labels: []string{"Item"}, Properties: map[string]interface{}{"rank": int64(2)}}},
		{Type: OpCreateNode, Node: &Node{ID: "other:foreign", Labels: []string{"Item"}, Properties: map[string]interface{}{"rank": int64(-5)}}},
	}})
	for _, descending := range []bool{false, true} {
		t.Run(fmt.Sprint(descending), func(t *testing.T) {
			var groups [][]NodeID
			found, err := sm.VisitPropertyIndexGroups(t.Context(), "Item", "rank", descending, func(ids []NodeID) bool { groups = append(groups, ids); return true })
			require.NoError(t, err)
			require.True(t, found)
			want := [][]NodeID{{"test:c2"}, {"test:c6", "test:c7", "test:p"}, {"test:c1"}}
			if descending {
				slices.Reverse(want)
			}
			require.Equal(t, want, groups)
			var bounded [][]NodeID
			found, err = sm.VisitPropertyIndexGroupsInRange(t.Context(), "Item", "rank", descending, PropertyIndexBounds{HasLower: true, Lower: int64(1), LowerInclusive: true, HasUpper: true, Upper: int64(3), UpperInclusive: true}, func(ids []NodeID) bool { bounded = append(bounded, ids); return true })
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, [][]NodeID{{"test:c6", "test:c7", "test:p"}}, bounded)
			calls := 0
			found, err = sm.VisitPropertyIndexGroups(t.Context(), "Item", "rank", descending, func(ids []NodeID) bool { calls++; require.Equal(t, want[0], ids); return false })
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, 1, calls)
		})
	}
	all, err := sm.PropertyIndexAllNonNil(t.Context(), "Item", "rank", false)
	require.NoError(t, err)
	require.Equal(t, []NodeID{"test:c2", "test:c6", "test:c7", "test:p", "test:c1"}, all)
	top, err := sm.PropertyIndexTopK(t.Context(), "Item", "rank", 2, false)
	require.NoError(t, err)
	require.Equal(t, []NodeID{"test:c2", "test:c6"}, top)
}

func TestOrderedIndexSnapshotConservativeMixedBounds(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("ordered", "Item", []string{"rank"}))
	require.NoError(t, sm.PropertyIndexInsert("Item", "rank", "test:one", int64(1)))
	require.NoError(t, sm.PropertyIndexInsert("Item", "rank", "test:three", int64(3)))
	w := &WriteBehindBuffer{active: newCommitBuffer()}
	sm.attachPendingWrites(w, "test")
	w.AppendCommit(bufferedCommit{ops: []Operation{{Type: OpCreateNode, Node: &Node{ID: "test:string", Labels: []string{"Item"}, Properties: map[string]interface{}{"rank": "text"}}}}})
	bounds := PropertyIndexBounds{HasLower: true, Lower: int64(2), LowerInclusive: true}
	var ids []NodeID
	exists, err := sm.VisitPropertyIndexGroupsInRange(t.Context(), "Item", "rank", false, bounds, func(group []NodeID) bool { ids = append(ids, group...); return true })
	require.NoError(t, err)
	require.True(t, exists)
	slices.Sort(ids)
	require.Equal(t, []NodeID{"test:one", "test:string", "test:three"}, ids, "mixed kinds conservatively retain every group")
	exact, exists, err := sm.PropertyIndexRange(t.Context(), "Item", "rank", bounds)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, []NodeID{"test:three"}, exact, "ordered IDs still filter bounds exactly")
}

func TestOrderedIndexSnapshotCallbacksReleaseEveryLease(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("ordered", "Item", []string{"rank"}))
	require.NoError(t, sm.PropertyIndexInsert("Item", "rank", "test:one", int64(1)))
	w := &WriteBehindBuffer{active: newCommitBuffer()}
	sm.attachPendingWrites(w, "test")
	w.AppendCommit(bufferedCommit{ops: []Operation{{Type: OpCreateNode, Node: &Node{ID: "test:two", Labels: []string{"Item"}, Properties: map[string]interface{}{"rank": int64(2)}}}}})
	idx, exists, err := sm.GetPropertyIndex(t.Context(), "Item", "rank")
	require.NoError(t, err)
	require.True(t, exists)
	var groups [][]NodeID
	exists, err = sm.VisitPropertyIndexGroups(t.Context(), "Item", "rank", false, func(ids []NodeID) bool {
		for _, mu := range []*contextRWMutex{&sm.mu, &w.mu, &idx.mu} {
			acquired := mu.TryLock()
			if acquired {
				mu.Unlock()
			}
			require.True(t, acquired, "callback cannot retain schema, pending, or index reader")
		}
		groups = append(groups, ids)
		if len(groups) == 1 {
			w.AppendCommit(bufferedCommit{ops: []Operation{{Type: OpCreateNode, Node: &Node{ID: "test:three", Labels: []string{"Item"}, Properties: map[string]interface{}{"rank": int64(3)}}}}})
			all, err := sm.PropertyIndexAllNonNil(t.Context(), "Item", "rank", false)
			require.NoError(t, err)
			require.Equal(t, []NodeID{"test:one", "test:two", "test:three"}, all)
		}
		return true
	})
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, [][]NodeID{{"test:one"}, {"test:two"}}, groups, "membership was captured before callback mutation")
}

func TestOrderedIndexSnapshotCancellationAndRetry(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("ordered", "Item", []string{"rank"}))
	w := &WriteBehindBuffer{active: newCommitBuffer()}
	for i := range 2048 {
		w.active.addNode(&Node{ID: NodeID(fmt.Sprintf("test:n%04d", i)), Labels: []string{"Item"}, Properties: map[string]interface{}{"rank": int64(i % 64)}})
	}
	sm.attachPendingWrites(w, "test")
	for _, at := range []int64{1, 64, 1024} {
		t.Run(fmt.Sprint(at), func(t *testing.T) {
			ctx := newIndexPollCancelContext(t, at)
			calls := 0
			_, err := sm.VisitPropertyIndexGroups(ctx, "Item", "rank", false, func([]NodeID) bool { calls++; return true })
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, calls, "no callback may see a partially prepared snapshot")
			count := 0
			found, err := sm.VisitPropertyIndexGroups(t.Context(), "Item", "rank", false, func(ids []NodeID) bool { count += len(ids); return true })
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, 2048, count)
		})
	}
}

func TestOrderedIndexSnapshotEmptyViewKeepsSourceLease(t *testing.T) {
	for _, kind := range []string{"empty", "delete-only", "raw-order", "draining", "active"} {
		t.Run(kind, func(t *testing.T) {
			sm := NewSchemaManager()
			require.NoError(t, sm.AddPropertyIndex("ordered", "Item", []string{"rank"}))
			w := &WriteBehindBuffer{active: newCommitBuffer()}
			sm.attachPendingWrites(w, "test")
			node := &Node{ID: "test:n", Labels: []string{"Item"}, Properties: map[string]interface{}{"rank": int64(1)}}
			switch kind {
			case "delete-only":
				w.AppendCommit(bufferedCommit{ops: []Operation{{Type: OpDeleteNode, NodeID: node.ID, OldNode: node}}})
			case "raw-order":
				w.active.addNode(node)
			case "draining":
				w.AppendCommit(bufferedCommit{ops: []Operation{{Type: OpCreateNode, Node: node}}})
				if _, rotateErr := w.rotate(t.Context()); rotateErr != nil {
					t.Fatal(rotateErr)
				}
			case "active":
				w.AppendCommit(bufferedCommit{ops: []Operation{{Type: OpCreateNode, Node: node}}})
			}
			view, source, err := sm.beginPendingRead(t.Context())
			require.NoError(t, err)
			release := sync.OnceFunc(func() { endPendingRead(source) })
			defer release()
			require.Same(t, w, source, "empty membership does not abandon the actual source lease")
			acquired := w.mu.TryLock()
			if acquired {
				w.mu.Unlock()
			}
			require.False(t, acquired, "source read lease remains held")
			if kind == "empty" {
				require.Nil(t, view.index)
				require.Nil(t, view.owner)
			} else {
				require.Same(t, w.pendingIndex, view.index)
				require.Same(t, w, view.owner)
			}
			if kind == "delete-only" {
				require.False(t, view.keep(node.ID), "empty value membership is not absence of pending deletion")
			}
			release()
			acquired = w.mu.TryLock()
			if acquired {
				w.mu.Unlock()
			}
			require.True(t, acquired, "matching endPendingRead releases empty and nonempty sources")
		})
	}
}

func TestOrderedIndexSnapshotCanceledPendingWaiter(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("ordered", "Item", []string{"rank"}))
	require.NoError(t, sm.PropertyIndexInsert("Item", "rank", "test:n", int64(1)))
	w := &WriteBehindBuffer{active: newCommitBuffer()}
	sm.attachPendingWrites(w, "test")
	base, cancel := context.WithCancel(t.Context())
	ctx := &indexWaitContext{Context: base, entered: make(chan struct{})}
	w.mu.Lock()
	release := sync.OnceFunc(w.mu.Unlock)
	done := make(chan error, 1)
	realDone := make(chan struct{})
	var returned atomic.Int64
	calls := 0
	t.Cleanup(func() {
		cancel()
		release()
		select {
		case <-realDone:
		case <-time.After(5 * time.Second):
			t.Error("actual snapshot goroutine did not join")
		}
	})
	go func() {
		defer close(realDone)
		_, err := sm.VisitPropertyIndexGroups(ctx, "Item", "rank", false, func([]NodeID) bool { calls++; return true })
		returned.Store(time.Now().UnixNano())
		done <- err
	}()
	select {
	case <-ctx.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("did not reach actual pending-source admission wait")
	}
	canceledAt := time.Now()
	cancel()
	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("canceled snapshot did not return while pending writer retained lock")
	}
	require.ErrorIs(t, err, context.Canceled)
	require.LessOrEqual(t, time.Unix(0, returned.Load()).Sub(canceledAt), 100*time.Millisecond)
	select {
	case <-realDone:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot result was sent without actual goroutine completion")
	}
	require.Zero(t, calls)
	release()
	found, err := sm.VisitPropertyIndexGroups(t.Context(), "Item", "rank", false, func(ids []NodeID) bool { require.Equal(t, []NodeID{"test:n"}, ids); return true })
	require.NoError(t, err)
	require.True(t, found)
}

func TestAppendIndexNodeIDsOwnershipCancellationAndRetry(t *testing.T) {
	for _, input := range [][]NodeID{nil, {}} {
		got, err := appendIndexNodeIDs(t.Context(), nil, input)
		require.NoError(t, err)
		require.NotNil(t, got, "copy callers retain the nonnil empty-slice contract")
		require.Empty(t, got)
	}
	source := make([]NodeID, 1024)
	for i := range source {
		source[i] = NodeID(fmt.Sprint(i))
	}
	original := slices.Clone(source)
	copied, err := appendIndexNodeIDs(t.Context(), nil, source)
	require.NoError(t, err)
	require.Equal(t, source, copied)
	copied[0] = "changed-owned-copy"
	require.Equal(t, original, source, "returned copy cannot mutate an index bucket")
	out := make([]NodeID, 1, len(source)+1)
	out[0] = "prefix"
	appended, err := appendIndexNodeIDs(t.Context(), out, source)
	require.NoError(t, err)
	require.Same(t, &out[0], &appended[0], "preallocated output is reused across groups")
	require.Equal(t, append([]NodeID{"prefix"}, source...), appended)
	appended[1] = "changed-appended-copy"
	require.Equal(t, original, source)
	for _, at := range []int64{1, 3} {
		ctx := newIndexPollCancelContext(t, at)
		partial, err := appendIndexNodeIDs(ctx, nil, source)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, partial)
		require.Equal(t, original, source)
		retry, err := appendIndexNodeIDs(t.Context(), nil, source)
		require.NoError(t, err)
		require.Equal(t, original, retry)
	}
}

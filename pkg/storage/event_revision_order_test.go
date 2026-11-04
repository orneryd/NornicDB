// Author: #1683 lane (Claude), 2026-10-31 (NOR-148); ported to release/v1.4.4.
package storage

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// The retained full-text corpus rests on this order: a commit's publication, which
// moves the graph revision, runs only after its data is visible.
func TestCommitPublicationRunsAfterDataIsVisible(t *testing.T) {
	base := createTestBadgerEngine(t)
	key := nodeKey("order:one")
	published, visible := false, false
	err := base.withPublishingUpdate(context.Background(), func(txn *badger.Txn) error {
		return txn.Set(key, []byte{1})
	}, func(*commitCacheImages) {
		published = true
		visible = base.db.View(func(txn *badger.Txn) error {
			_, err := txn.Get(key)
			return err
		}) == nil
	}, nil, nil, nil)
	require.NoError(t, err)
	require.True(t, published)
	require.True(t, visible, "the revision would move before its data is visible")
}

// A revision naming n writes means those n writes are visible, on the public write
// path, with and without a listener (no listener: no queue entry).
func TestRevisionCoversVisibleWrites(t *testing.T) {
	for _, listener := range []bool{false, true} {
		t.Run(fmt.Sprintf("listener=%t", listener), func(t *testing.T) {
			base := createTestBadgerEngine(t)
			if listener {
				base.OnNodeCreated(func(*Node) {})
			}
			start, supported := base.GraphMutationVersion()
			require.True(t, supported)
			const writes = 2000
			var done atomic.Bool
			failed := make(chan error, 1)
			go func() {
				defer done.Store(true)
				for i := range writes {
					if _, err := base.CreateNode(context.Background(), &Node{ID: NodeID(fmt.Sprintf("order:n%d", i)), Labels: []string{"Order"}}); err != nil {
						failed <- err
						return
					}
				}
			}()
			samples := 0
			for !done.Load() {
				version, _ := base.GraphMutationVersion()
				if version == start {
					continue
				}
				samples++
				_, err := base.GetNode(context.Background(), NodeID(fmt.Sprintf("order:n%d", version-start-1)))
				require.NoError(t, err, "a revision named a write that is not visible")
			}
			select {
			case err := <-failed:
				t.Fatal(err)
			default:
			}
			t.Logf("samples: %d", samples)
		})
	}
}

// Refreshing by ID relies on revision and event count moving together only when
// the journal names every change. Each product write is measured on one view. A
// write that moved more events than revisions could cancel out one that moved
// fewer, so each native write must move both by the same amount.
func TestRevisionAndEventCountPerWrite(t *testing.T) {
	base := createTestBadgerEngine(t)
	view := NewNamespacedEngine(base, "delta")
	var events atomic.Uint64
	count := func() { events.Add(1) }
	require.NoError(t, SubscribeEvents(view, EventHandlers{
		NodeCreated: func(*Node) { count() }, NodeUpdated: func(*Node) { count() }, NodeDeleted: func(NodeID) { count() },
		EdgeCreated: func(*Edge) { count() }, EdgeUpdated: func(*Edge) { count() }, EdgeDeleted: func(EdgeID) { count() },
		NamespaceReset: func(string) { count() },
	}))
	node := func(id string) *Node {
		return &Node{ID: NodeID(id), Labels: []string{"Delta"}, Properties: map[string]any{"text": id}}
	}
	edge := func(id, from, to string) *Edge {
		return &Edge{ID: EdgeID(id), StartNode: NodeID(from), EndNode: NodeID(to), Type: "DELTA", Properties: map[string]any{"text": id}}
	}
	create := func(id string) error { _, err := view.CreateNode(context.Background(), node(id)); return err }
	steps := []struct {
		name string
		run  func() error
		want uint64
	}{
		{"create node", func() error { return create("a") }, 1},
		{"create second node", func() error { return create("b") }, 1},
		{"create third node", func() error { return create("c") }, 1},
		{"update node", func() error {
			current, err := view.GetNode(context.Background(), "a")
			if err != nil {
				return err
			}
			current.Properties["text"] = "changed"
			return view.UpdateNode(context.Background(), NodeUpdateRequest{After: current})
		}, 1},
		{"create edge", func() error { return view.CreateEdge(context.Background(), edge("e1", "a", "b")) }, 1},
		{"update edge", func() error {
			current, err := view.GetEdge(context.Background(), "e1")
			if err != nil {
				return err
			}
			current.Properties["text"] = "changed"
			return view.UpdateEdge(context.Background(), EdgeUpdateRequest{After: current})
		}, 1},
		{"delete edge", func() error { return view.DeleteEdge(context.Background(), "e1") }, 1},
		{"create edges for cascade", func() error {
			if err := view.CreateEdge(context.Background(), edge("e2", "a", "b")); err != nil {
				return err
			}
			return view.CreateEdge(context.Background(), edge("e3", "c", "a"))
		}, 2},
		{"delete node with two edges", func() error { return view.DeleteNode(context.Background(), "a") }, 3},
		{"bulk create nodes", func() error {
			return view.BulkCreateNodes(context.Background(), []*Node{node("x"), node("y"), node("z")})
		}, 3},
		{"bulk create edges", func() error {
			return view.BulkCreateEdges(context.Background(), []*Edge{edge("e4", "x", "y"), edge("e5", "y", "z")})
		}, 2},
		{"bulk delete edges", func() error { return view.BulkDeleteEdges(context.Background(), []EdgeID{"e4", "e5"}) }, 2},
		{"bulk delete nodes", func() error { return view.BulkDeleteNodes(context.Background(), []NodeID{"x", "y", "z"}) }, 3},
	}
	wait := func() {
		deadline := time.Now().Add(10 * time.Second)
		for !EventNotificationsSettled(view) {
			require.True(t, time.Now().Before(deadline), "events were not delivered")
			time.Sleep(time.Millisecond)
		}
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			wait()
			revision, supported := view.GraphMutationVersion()
			require.True(t, supported)
			before := events.Load()
			require.NoError(t, step.run())
			wait()
			moved, _ := view.GraphMutationVersion()
			require.Equal(t, step.want, moved-revision, "revision movement")
			require.Equal(t, step.want, events.Load()-before, "event movement")
		})
	}
	t.Run("transaction", func(t *testing.T) {
		wait()
		revision, _ := view.GraphMutationVersion()
		before := events.Load()
		tx, err := base.BeginTransaction(context.Background())
		require.NoError(t, err)
		_, err = tx.CreateNode(context.Background(), &Node{ID: "delta:t1", Labels: []string{"Delta"}})
		require.NoError(t, err)
		_, err = tx.CreateNode(context.Background(), &Node{ID: "delta:t2", Labels: []string{"Delta"}})
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		wait()
		moved, _ := view.GraphMutationVersion()
		require.Equal(t, uint64(2), moved-revision, "revision movement")
		require.Equal(t, uint64(2), events.Load()-before, "event movement")
	})
	t.Run("namespace reset moves both", func(t *testing.T) {
		wait()
		revision, _ := view.GraphMutationVersion()
		before := events.Load()
		_, _, err := base.DeleteByPrefix("delta:")
		require.NoError(t, err)
		wait()
		moved, _ := view.GraphMutationVersion()
		require.Greater(t, moved, revision)
		require.Greater(t, events.Load(), before)
	})
}

// The revision of a database named with ':' moves on an ordinary write.
func TestRevisionOwnerCoversColonNames(t *testing.T) {
	base := createTestBadgerEngine(t)
	view := NewNamespacedEngine(base, "a:b")
	before, supported := view.GraphMutationVersion()
	require.True(t, supported)
	_, err := view.CreateNode(context.Background(), &Node{ID: "one", Labels: []string{"T"}})
	require.NoError(t, err)
	after, _ := view.GraphMutationVersion()
	require.Greater(t, after, before)
}

// Rows are gone once the prefix drop has run, whichever cleanup step then fails, and
// a retry counts nothing to delete: the failed attempt itself must move the revision.
func TestPrefixDropFailureAfterDropMovesRevision(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered=%t", buffered), func(t *testing.T) {
			var engine *BadgerEngine
			if buffered {
				engine = newWriteBehindTestEngine(t, time.Hour)
			} else {
				engine = createTestBadgerEngine(t)
			}
			target := NewNamespacedEngine(engine, "test")
			_, err := target.CreateNode(context.Background(), &Node{ID: "prefix-victim", Labels: []string{"Doc"}})
			require.NoError(t, err)
			before, supported := engine.GraphMutationVersionInNamespace("test")
			require.True(t, supported)
			failure := errors.New("injected failure after the drop")
			engine.dropPrefixTailHook = func(string) error { return failure }
			_, _, err = engine.DeleteByPrefix("test:prefix-")
			engine.dropPrefixTailHook = nil
			require.ErrorIs(t, err, failure)
			exists, err := ReviewRawEntityExists(engine, StoredEntityNodeKind, "test:prefix-victim")
			require.NoError(t, err)
			require.False(t, exists, "the fixture must reach the failure after the rows are gone")
			after, supported := engine.GraphMutationVersionInNamespace("test")
			require.True(t, supported)
			require.Greater(t, after, before)
		})
	}
}

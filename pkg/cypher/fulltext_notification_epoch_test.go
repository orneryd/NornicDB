// Author: Codex implementation lane, 2026-10-26 (NOR-148).
package cypher

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An old notification arriving after recovery cannot stand in for a newer
// visible write whose notification is still queued, even when counts coincide.
func TestFulltextMaintainedPublicNotificationEpoch(t *testing.T) {
	for _, during := range []bool{false, true} {
		name := "warm_before_queue"
		if during {
			name = "first_population_during_queue"
		}
		t.Run(name, func(t *testing.T) { fulltextNotificationEpoch(t, during) })
	}
}

func fulltextNotificationEpoch(t *testing.T, startDuring bool) {
	exec, store := fulltextMaintainedFixture(t, 80)
	publisher := store.GetInnerEngine().(*storage.BadgerEngine)
	early, late := make(chan struct{}), make(chan struct{})
	releaseEarly, releaseLate := make(chan struct{}), make(chan struct{})
	var earlyOnce, lateOnce sync.Once
	unblockEarly := func() { earlyOnce.Do(func() { close(releaseEarly) }) }
	unblockLate := func() { lateOnce.Do(func() { close(releaseLate) }) }
	t.Cleanup(unblockLate)
	t.Cleanup(unblockEarly)
	publisher.OnNodeUpdated(func(node *storage.Node) {
		if strings.HasSuffix(string(node.ID), ":n000004") {
			close(early)
			<-releaseEarly
		}
	})
	ctx := context.Background()
	query := "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN node.id, score"
	registerLate := func(id string) {
		// This observer runs after the corpus owner's observer for this event.
		publisher.OnNodeUpdated(func(node *storage.Node) {
			if strings.HasSuffix(string(node.ID), ":"+id) {
				close(late)
				<-releaseLate
			}
		})
	}
	if !startDuring {
		_, err := exec.Execute(ctx, query, map[string]any{"query": "hotel"})
		require.NoError(t, err)
		registerLate("n000004")
	}
	barrier, err := store.GetNode("n000004")
	require.NoError(t, err)
	barrier.Properties["text"] = "oldernotification"
	finished := make(chan error, 1)
	go func() { finished <- store.UpdateNode(storage.NodeUpdateRequest{After: barrier}) }()
	wait := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatal("notification barrier did not arrive")
		}
	}
	wait(early)
	if startDuring {
		// This second event has not begun dispatch, so it will include the
		// corpus subscription installed by the first population below.
		older, err := store.GetNode("n000008")
		require.NoError(t, err)
		older.Properties["text"] = "olderqueuednotification"
		require.NoError(t, store.UpdateNode(storage.NodeUpdateRequest{After: older}))
	}
	_, err = exec.Execute(ctx, query, map[string]any{"query": "oldernotification"})
	require.NoError(t, err)
	if startDuring {
		registerLate("n000008")
	}
	unblockEarly()
	wait(late)
	newer, err := store.GetNode("n000040")
	require.NoError(t, err)
	newer.Properties["text"] = "newerunnotifiedwrite"
	require.NoError(t, store.UpdateNode(storage.NodeUpdateRequest{After: newer}))
	during, err := exec.Execute(ctx, query, map[string]any{"query": "newerunnotifiedwrite"})
	require.NoError(t, err)
	require.Len(t, during.Rows, 1, "an older notification cannot cover a newer graph revision")
	require.Equal(t, "n000040", during.Rows[0][0])
	unblockLate()
	require.NoError(t, <-finished)
	after, err := exec.Execute(ctx, query, map[string]any{"query": "newerunnotifiedwrite"})
	require.NoError(t, err)
	require.Equal(t, during.Rows, after.Rows)
}

func TestFulltextMaintainedPublicBufferedWrites(t *testing.T) {
	base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{InMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "buffered_fulltext")
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE FULLTEXT INDEX buffered_text FOR (n:Source) ON EACH [n.text]",
		"CREATE (:Source {id:'one', text:'hotel'}), (:Source {id:'two', text:'routine'})",
	} {
		_, err = exec.Execute(ctx, statement, nil)
		require.NoError(t, err)
	}
	query := "CALL db.index.fulltext.queryNodes('buffered_text', 'hotel') YIELD node, score RETURN node.id, score"
	initial, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, initial.Rows, 1)
	_, err = exec.Execute(ctx, "MATCH (n:Source {id:'two'}) SET n.text='hotel'", nil)
	require.NoError(t, err)
	updated, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, updated.Rows, 2)
	require.NoError(t, base.FlushWriteBehind(context.Background()))
	after, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, updated.Rows, after.Rows)
}

// Author: #1683 lane (Claude), 2026-10-30 (NOR-148).
package cypher

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// fulltextManagerCountingView is the query executor's view of a database that
// a DatabaseManager hands out. The test writes to the database itself, so the
// manager's write path stays exactly as in production; this view only counts
// snapshot scans and forwards the subscription contract to the database.
type fulltextManagerCountingView struct {
	storage.Engine
	scans atomic.Int64
}

func (v *fulltextManagerCountingView) GetInnerEngine() storage.Engine { return v.Engine }

func (v *fulltextManagerCountingView) OwnsSnapshotReader(issued storage.Engine) bool {
	return issued == v
}

func (v *fulltextManagerCountingView) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	inner, err := storage.OpenReadSnapshot(ctx, v.Engine)
	if err != nil {
		return nil, err
	}
	return &fulltextCountingSnapshot{QueryReadSnapshot: inner.(storage.QueryReadSnapshot), scans: &v.scans}, nil
}

func (v *fulltextManagerCountingView) SubscribeEvents(handlers storage.EventHandlers) error {
	return storage.SubscribeEvents(v.Engine, handlers)
}

func (v *fulltextManagerCountingView) EventNotificationsSettled() bool {
	return storage.EventNotificationsSettled(v.Engine)
}

// A database from a DatabaseManager reaches storage through the manager's own
// wrappers. One changed or deleted document must still refresh by ID there,
// exactly as on a bare namespaced engine, without repopulating the corpus.
func TestFulltextMaintainedPublicDatabaseManagerRefresh(t *testing.T) {
	base := storage.NewMemoryEngine().BadgerEngine
	// Cleanups run in reverse order: the manager closes before its engine.
	t.Cleanup(func() { _ = base.Close() })
	manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "fulltextdb"}))
	store, err := manager.GetStorage("fulltextdb")
	require.NoError(t, err)

	nodes := make([]*storage.Node, 80)
	for i := range nodes {
		text := strings.Repeat("routine correspondence ", 8)
		if i%8 == 0 {
			text += " hotel services"
		}
		id := fmt.Sprintf("n%06d", i)
		nodes[i] = &storage.Node{ID: storage.NodeID(id), Labels: []string{"Source"}, Properties: map[string]any{"id": id, "text": text}}
	}
	require.NoError(t, store.BulkCreateNodes(nodes))
	view := &fulltextManagerCountingView{Engine: store}
	exec := NewStorageExecutor(view, QueryCachePolicy{MaxEntries: 0})
	ctx := context.Background()
	_, err = exec.Execute(ctx, "CREATE FULLTEXT INDEX source_text FOR (n:Source) ON EACH [n.text]", nil)
	require.NoError(t, err)
	query := "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN node.id, score ORDER BY score DESC, node.id"

	first, err := exec.Execute(ctx, query, map[string]any{"query": "hotel"})
	require.NoError(t, err)
	require.Len(t, first.Rows, 10)
	scans := view.scans.Load()
	require.Positive(t, scans, "the first query populates the corpus")
	repeat, err := exec.Execute(ctx, query, map[string]any{"query": "hotel"})
	require.NoError(t, err)
	require.Equal(t, first.Rows, repeat.Rows)
	require.Equal(t, scans, view.scans.Load(), "a repeated query repopulated the corpus")

	node, err := store.GetNode("n000008")
	require.NoError(t, err)
	node.Properties["text"] = "newunique hotel services"
	require.NoError(t, store.UpdateNode(node))
	changed, err := exec.Execute(ctx, query, map[string]any{"query": "newunique"})
	require.NoError(t, err)
	require.Len(t, changed.Rows, 1)
	require.Equal(t, "n000008", changed.Rows[0][0])
	require.Equal(t, scans, view.scans.Load(), "one changed document must refresh without rereading the whole store")

	require.NoError(t, store.DeleteNode("n000008"))
	deleted, err := exec.Execute(ctx, query, map[string]any{"query": "newunique"})
	require.NoError(t, err)
	require.Empty(t, deleted.Rows)
	require.Equal(t, scans, view.scans.Load(), "deletion must retire its document without rereading the whole store")
}

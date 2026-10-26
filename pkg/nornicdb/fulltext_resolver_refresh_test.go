// Author: #1683 lane (Claude), 2026-10-30 (NOR-148).
package nornicdb

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// fulltextResolverView is the storage a query executor reads through. It
// counts corpus scans of its snapshots and forwards everything else to the
// database engine the manager handed out, so writes keep the real path.
type fulltextResolverView struct {
	storage.Engine
	scans atomic.Int64
}

func (v *fulltextResolverView) GetInnerEngine() storage.Engine { return v.Engine }

func (v *fulltextResolverView) OwnsSnapshotReader(issued storage.Engine) bool { return issued == v }

func (v *fulltextResolverView) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	inner, err := storage.OpenReadSnapshot(ctx, v.Engine)
	if err != nil {
		return nil, err
	}
	return &fulltextResolverSnapshot{QueryReadSnapshot: inner.(storage.QueryReadSnapshot), scans: &v.scans}, nil
}

func (v *fulltextResolverView) SubscribeEvents(handlers storage.EventHandlers) error {
	return storage.SubscribeEvents(v.Engine, handlers)
}

func (v *fulltextResolverView) EventNotificationsSettled() bool {
	return storage.EventNotificationsSettled(v.Engine)
}

// fulltextForwardingView also declares that it only forwards events, IDs and
// committed reads, as the production accounting and tracing wrappers do.
type fulltextForwardingView struct{ *fulltextResolverView }

func (v fulltextForwardingView) EventViewInner() storage.Engine { return v.Engine }

// The executor issues snapshots from this value, so it owns their reader.
func (v fulltextForwardingView) OwnsSnapshotReader(issued storage.Engine) bool {
	return issued == storage.Engine(v)
}

type fulltextResolverSnapshot struct {
	storage.QueryReadSnapshot
	scans *atomic.Int64
}

func (s *fulltextResolverSnapshot) IterateNodes(fn func(*storage.Node) bool) error {
	s.scans.Add(1)
	return s.QueryReadSnapshot.IterateNodes(fn)
}

func (s *fulltextResolverSnapshot) IterateVisibleNodes(ctx context.Context, fn func(*storage.Node) bool) error {
	s.scans.Add(1)
	return s.QueryReadSnapshot.IterateVisibleNodes(ctx, fn)
}

func (s *fulltextResolverSnapshot) StreamEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	s.scans.Add(1)
	return s.QueryReadSnapshot.StreamEdges(ctx, visit)
}

func (s *fulltextResolverSnapshot) IterateAllEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	s.scans.Add(1)
	return s.QueryReadSnapshot.IterateAllEdges(ctx, visit)
}

// openFulltextResolverDB opens a DB and builds its DatabaseManager exactly as
// the server does, over the DB's own base storage.
func openFulltextResolverDB(t *testing.T) (*DB, *multidb.DatabaseManager) {
	t.Helper()
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.AsyncWritesEnabled = false
	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	manager, err := multidb.NewDatabaseManager(db.LifetimeContext(), db.GetBaseStorageForManager(), multidb.NewConfigFromDefaultDatabase(db.defaultDatabaseName()))
	require.NoError(t, err)
	// Like the server, never close the manager: DatabaseManager.Close closes
	// the base engine the DB still owns, and DB.Close closes it.
	return db, manager
}

// resolverExecutor configures a query executor for one database as
// server_db.go does: the manager's engine for that database, the DB's search
// service resolver, and GetOrCreateSearchService returning the cached service.
func resolverExecutor(t *testing.T, db *DB, store storage.Engine, name string, proven bool) (*cypher.StorageExecutor, *fulltextResolverView) {
	t.Helper()
	view := &fulltextResolverView{Engine: store}
	var engine storage.Engine = view
	if proven {
		engine = fulltextForwardingView{view}
	}
	exec := cypher.NewStorageExecutor(engine, cypher.QueryCachePolicy{MaxEntries: 0})
	exec.SetSearchServiceResolver(name, db.SearchServiceResolver())
	_, err := db.GetOrCreateSearchService(name, engine)
	require.NoError(t, err)
	return exec, view
}

func waitEventsDelivered(t *testing.T, engine storage.Engine) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !storage.EventNotificationsSettled(engine) {
		require.True(t, time.Now().Before(deadline), "storage notifications were not delivered")
		time.Sleep(5 * time.Millisecond)
	}
}

func seedFulltextResolverCorpus(t *testing.T, store storage.Engine) {
	t.Helper()
	nodes := make([]*storage.Node, 40)
	for i := range nodes {
		text := "routine correspondence"
		if i%4 == 0 {
			text += " hotel services"
		}
		id := fmt.Sprintf("n%03d", i)
		nodes[i] = &storage.Node{ID: storage.NodeID(id), Labels: []string{"Source"}, Properties: map[string]any{"id": id, "text": text}}
	}
	require.NoError(t, store.BulkCreateNodes(nodes))
	for i := 0; i < 20; i++ {
		text := "routine link"
		if i%4 == 0 {
			text += " hotel services"
		}
		id := fmt.Sprintf("r%03d", i)
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(id), Type: "CITES", StartNode: storage.NodeID(fmt.Sprintf("n%03d", i)), EndNode: storage.NodeID(fmt.Sprintf("n%03d", i+20)), Properties: map[string]any{"id": id, "text": text}}))
	}
}

// fulltextResolverRefresh checks one entity kind: after the population query,
// an existing-ID update and a delete must change the rows without another
// corpus scan when the view is proven, and must re-read it when it is not.
func fulltextResolverRefresh(t *testing.T, exec *cypher.StorageExecutor, view *fulltextResolverView, store storage.Engine, relationships, proven bool) {
	t.Helper()
	ctx := context.Background()
	query := "CALL db.index.fulltext.queryNodes('source_text', $q) YIELD node, score RETURN node.id ORDER BY score DESC, node.id"
	if relationships {
		query = "CALL db.index.fulltext.queryRelationships('cites_text', $q) YIELD relationship, score RETURN relationship.id ORDER BY score DESC, relationship.id"
	}
	run := func(q string) [][]any {
		t.Helper()
		waitEventsDelivered(t, store)
		result, err := exec.Execute(ctx, query, map[string]any{"q": q})
		require.NoError(t, err)
		return result.Rows
	}
	first := run("hotel")
	require.NotEmpty(t, first)
	scans := view.scans.Load()
	require.Positive(t, scans, "the first query populates the corpus")
	require.Equal(t, first, run("hotel"))
	require.Equal(t, scans, view.scans.Load(), "a repeated query re-read the corpus")

	if relationships {
		edge, err := store.GetEdge("r004")
		require.NoError(t, err)
		edge.Properties["text"] = "newunique hotel"
		require.NoError(t, store.UpdateEdge(edge))
	} else {
		node, err := store.GetNode("n004")
		require.NoError(t, err)
		node.Properties["text"] = "newunique hotel"
		require.NoError(t, store.UpdateNode(node))
	}
	changed := run("newunique")
	require.Len(t, changed, 1)
	if proven {
		require.Equal(t, scans, view.scans.Load(), "an existing-ID update must refresh by ID, without a corpus scan")
	} else {
		require.Greater(t, view.scans.Load(), scans, "an unproven view must re-read the corpus after a write")
		scans = view.scans.Load()
	}

	if relationships {
		require.NoError(t, store.DeleteEdge("r004"))
	} else {
		// n004 ends relationship r004; detach first so the node delete stands alone.
		require.NoError(t, store.DeleteEdge("r004"))
		waitEventsDelivered(t, store)
		require.Len(t, run("newunique"), 1)
		if proven {
			require.Equal(t, scans, view.scans.Load(), "a relationship delete must not re-read the node corpus")
		} else {
			scans = view.scans.Load()
		}
		require.NoError(t, store.DeleteNode("n004"))
	}
	require.Empty(t, run("newunique"))
	if proven {
		require.Equal(t, scans, view.scans.Load(), "a delete must retire its document without a corpus scan")
	} else {
		require.Greater(t, view.scans.Load(), scans, "an unproven view must re-read the corpus after a delete")
	}
}

// A server pairs a database's cached search service, built from one storage
// view, with query executors reading through the DatabaseManager's view of the
// same database. Incremental refresh must hold across that pairing for the
// default database (service created when the DB opens) and for a named
// database (service created by its first write event), for nodes and
// relationships. An unproven view keeps the full re-read.
func TestFulltextMaintainedResolverPathRefresh(t *testing.T) {
	for _, tc := range []struct {
		name          string
		named, proven bool
		relationships bool
	}{
		{"default_database/nodes", false, true, false},
		{"default_database/relationships", false, true, true},
		{"named_database/nodes", true, true, false},
		{"named_database/relationships", true, true, true},
		{"unproven_view/nodes", false, false, false},
		{"unproven_view/relationships", false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, manager := openFulltextResolverDB(t)
			name := db.defaultDatabaseName()
			if tc.named {
				name = "fulltextnamed"
				require.NoError(t, manager.CreateDatabase(context.Background(), multidb.DatabaseCreateRequest{Name: name}))
			}
			store, err := manager.GetStorage(name)
			require.NoError(t, err)
			seedFulltextResolverCorpus(t, store)
			settleQueuedRefreshes()
			waitEventsDelivered(t, store)
			// The cached service exists before the query executor asks for one: the
			// DB built it from its own view (at Open, or from the write events).
			db.searchServicesMu.RLock()
			entry := db.searchServices[name]
			db.searchServicesMu.RUnlock()
			require.NotNil(t, entry, "the DB must have created the database's search service from its own view")
			require.NotNil(t, entry.svc)

			exec, view := resolverExecutor(t, db, store, name, tc.proven)
			index := "CREATE FULLTEXT INDEX source_text FOR (n:Source) ON EACH [n.text]"
			if tc.relationships {
				index = "CREATE FULLTEXT INDEX cites_text FOR ()-[r:CITES]-() ON EACH [r.text]"
			}
			_, err = exec.Execute(context.Background(), index, nil)
			require.NoError(t, err)
			fulltextResolverRefresh(t, exec, view, store, tc.relationships, tc.proven)
		})
	}
}

// Another database's writes are never applied to this database's corpus.
func TestFulltextMaintainedResolverNamespaceSeparation(t *testing.T) {
	db, manager := openFulltextResolverDB(t)
	ctx := context.Background()
	require.NoError(t, manager.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: "fulltextother"}))
	name := db.defaultDatabaseName()
	store, err := manager.GetStorage(name)
	require.NoError(t, err)
	other, err := manager.GetStorage("fulltextother")
	require.NoError(t, err)
	seedFulltextResolverCorpus(t, store)
	seedFulltextResolverCorpus(t, other)
	settleQueuedRefreshes()
	exec, _ := resolverExecutor(t, db, store, name, true)
	_, err = exec.Execute(ctx, "CREATE FULLTEXT INDEX source_text FOR (n:Source) ON EACH [n.text]", nil)
	require.NoError(t, err)
	query := "CALL db.index.fulltext.queryNodes('source_text', $q) YIELD node RETURN node.id"
	waitEventsDelivered(t, store)
	_, err = exec.Execute(ctx, query, map[string]any{"q": "hotel"})
	require.NoError(t, err)
	node, err := other.GetNode("n008")
	require.NoError(t, err)
	node.Properties["text"] = "otheronly"
	require.NoError(t, other.UpdateNode(node))
	waitEventsDelivered(t, store)
	rows, err := exec.Execute(ctx, query, map[string]any{"q": "otheronly"})
	require.NoError(t, err)
	require.Empty(t, rows.Rows, "another database's write leaked into this database's corpus")
	rows, err = exec.Execute(ctx, query, map[string]any{"q": "hotel"})
	require.NoError(t, err)
	require.Len(t, rows.Rows, 10, "this database's corpus changed after another database's write")
}

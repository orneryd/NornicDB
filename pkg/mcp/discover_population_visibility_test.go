package mcp

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// heldTraversalEngine holds every node traversal until released, the point a
// search population reaches after it has replaced the node sources.
type heldTraversalEngine struct {
	*storage.MemoryEngine
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
	released sync.Once
}

func (e *heldTraversalEngine) IterateNodes(fn func(*storage.Node) bool) error {
	e.once.Do(func() { close(e.entered) })
	<-e.release
	nodes, err := e.MemoryEngine.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if !fn(node) {
			break
		}
	}
	return nil
}

func (e *heldTraversalEngine) releaseAll() { e.released.Do(func() { close(e.release) }) }

// A write to a persisted search service that has never been built starts its
// population in the background; while that population is held, MCP discover
// already finds the write (F-I86 decision B).
func TestDiscoverFindsWriteWhilePopulationRuns(t *testing.T) {
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	db, err := nornicdb.Open(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	server := NewServer(db, DefaultServerConfig())
	dbName := server.DefaultDatabaseName()

	engine := &heldTraversalEngine{MemoryEngine: storage.NewMemoryEngine(), entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(engine.releaseAll)
	db.ResetSearchService(dbName)
	svc, err := db.GetOrCreateSearchService(dbName, engine)
	require.NoError(t, err)
	dir := t.TempDir()
	svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
	svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
	svc.SetPersistenceEnabled(true)

	node := &storage.Node{ID: "nornic:written", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "zephyrine notes"}}
	_, err = engine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(node))
	select {
	case <-engine.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the write did not start the population")
	}
	require.False(t, svc.IsReady(), "the population is held")

	out, err := server.handleDiscover(context.Background(), map[string]interface{}{"query": "zephyrine"})
	require.NoError(t, err)
	result, ok := out.(DiscoverResult)
	require.True(t, ok)
	require.Equal(t, 1, result.Total, "discover must find the write while its population runs")
	require.Contains(t, result.Results[0].ID, "written")
	engine.releaseAll()
}

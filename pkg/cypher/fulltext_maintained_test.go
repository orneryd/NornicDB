// Author: Codex implementation lane, 2026-10-26 (NOR-148).
package cypher

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Count the actual storage population calls, independently of elapsed time.
// The public query must stop reconstructing the corpus on every invocation.
type fulltextCountingStore struct {
	*storage.NamespacedEngine
	scans    atomic.Int64
	nodeDocs atomic.Int64
}

func (s *fulltextCountingStore) StreamNodes(ctx context.Context, visit func(*storage.Node) error) error {
	s.scans.Add(1)
	return s.NamespacedEngine.StreamNodes(ctx, func(node *storage.Node) error { s.nodeDocs.Add(1); return visit(node) })
}

func (s *fulltextCountingStore) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	s.scans.Add(1)
	return s.NamespacedEngine.StreamNodesWithOptions(ctx, opts, func(node *storage.Node) error { s.nodeDocs.Add(1); return visit(node) })
}

func (s *fulltextCountingStore) StreamEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	s.scans.Add(1)
	return s.NamespacedEngine.StreamEdges(ctx, visit)
}

// GetInnerEngine returns the counted view, preserving its namespace boundary.
func (s *fulltextCountingStore) GetInnerEngine() storage.Engine { return s.NamespacedEngine }

// PreservesEventView declares that counting changes no event, ID or committed read.
func (s *fulltextCountingStore) PreservesEventView() {}

// fulltextFixtureBodies selects generated body diversity without changing the
// public query shape, eligible population or expected result counts.
type fulltextFixtureBodies uint8

const fulltextDistinctBodies fulltextFixtureBodies = 1

func fulltextMaintainedFixture(tb testing.TB, count int, bodies ...fulltextFixtureBodies) (*StorageExecutor, *fulltextCountingStore) {
	tb.Helper()
	// Use native Badger in memory, with the same capture/publication owner as disk.
	return fulltextMaintainedFixtureOn(tb, fixtureowner.Track(tb, storage.NewMemoryEngine(), storage.ErrStorageClosed).BadgerEngine, count, bodies...)
}

func fulltextMaintainedFixtureOn(tb testing.TB, base *storage.BadgerEngine, count int, bodies ...fulltextFixtureBodies) (*StorageExecutor, *fulltextCountingStore) {
	tb.Helper()
	store := &fulltextCountingStore{NamespacedEngine: storage.NewNamespacedEngine(base, "fulltext_fixture")}
	tb.Cleanup(func() { require.NoError(tb, base.Close()) })
	nodes := make([]*storage.Node, count)
	for i := range nodes {
		label, text := "Source", strings.Repeat("routine correspondence meeting attachment ", 48)
		if i%4 != 0 {
			label = "Unrelated"
		}
		if i%40 == 0 {
			text += " hotel services torrey pines la jolla"
		}
		if len(bodies) > 0 && bodies[0] == fulltextDistinctBodies {
			text += fmt.Sprintf(" distinct-body-%06d", i)
		}
		nodes[i] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%06d", i)), Labels: []string{label}, Properties: map[string]any{
			"id": fmt.Sprintf("n%06d", i), "text": text, "title": "Correspondence", "current": i%80 == 0,
		}}
	}
	err := store.BulkCreateNodes(tb.Context(), nodes)
	require.NoError(tb, err)
	// Every measured invocation must reach the procedure, including repeated
	// identical parameters; query-result caching would hide corpus rebuilds.
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	tb.Cleanup(func() {
		if svc := exec.search(); svc != nil {
			require.NoError(tb, svc.Close())
		}
	})
	_, err = exec.Execute(context.Background(), "CREATE FULLTEXT INDEX source_text FOR (n:Source) ON EACH [n.text, n.title]", nil)
	require.NoError(tb, err)
	return exec, store
}

func TestFulltextMaintainedPublicNoRebuild(t *testing.T) {
	exec, store := fulltextMaintainedFixture(t, 4000)
	ctx := context.Background()
	params := map[string]any{"query": "hotel torrey pines services", "limit": int64(10)}
	queries := []string{
		"CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN node.id, score ORDER BY score DESC, node.id LIMIT $limit",
		"CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN count(node) AS n",
		"CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score WHERE coalesce(node.current, true) RETURN node.id, score ORDER BY score DESC, node.id LIMIT $limit",
	}
	for _, query := range queries {
		start := time.Now()
		first, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.NotEmpty(t, first.Rows)
		t.Logf("first call %s: %s", query, time.Since(start))
		scans := store.scans.Load()
		for call := 0; call < 3; call++ {
			start = time.Now()
			got, err := exec.Execute(ctx, query, params)
			require.NoError(t, err)
			require.Equal(t, first.Rows, got.Rows)
			t.Logf("repeat %d: %s", call, time.Since(start))
		}
		require.Equal(t, scans, store.scans.Load(), "a repeated public search rebuilt the corpus from storage")
	}
	node, err := store.GetNode(t.Context(), "n000040")
	require.NoError(t, err)
	node.Properties["text"] = "newunique hotel services"
	require.NoError(t, store.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: node}))
	scans := store.scans.Load()
	changed, err := exec.Execute(ctx, queries[0], map[string]any{"query": "newunique", "limit": int64(10)})
	require.NoError(t, err)
	require.Len(t, changed.Rows, 1)
	require.Equal(t, scans, store.scans.Load(), "one changed document must refresh without rereading the whole store")
	require.NoError(t, store.DeleteNode(t.Context(), "n000040"))
	deleted, err := exec.Execute(ctx, queries[0], map[string]any{"query": "newunique", "limit": int64(10)})
	require.NoError(t, err)
	require.Empty(t, deleted.Rows)
	require.Equal(t, scans, store.scans.Load(), "deletion must retire its document without rereading the whole store")
}

func (s *fulltextCountingStore) take() (scans, docs int) {
	return int(s.scans.Swap(0)), int(s.nodeDocs.Swap(0))
}

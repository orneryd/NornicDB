// Author: Codex implementation lane, 2026-10-26 (NOR-148).
package cypher

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Count the actual storage population calls, independently of elapsed time.
// The public query must stop reconstructing the corpus on every invocation.
type fulltextCountingStore struct {
	*storage.NamespacedEngine
	scans atomic.Int64
}

func (s *fulltextCountingStore) AllNodes() ([]*storage.Node, error) {
	s.scans.Add(1)
	return s.NamespacedEngine.AllNodes()
}

func fulltextMaintainedFixture(tb testing.TB, count int) (*StorageExecutor, *fulltextCountingStore) {
	tb.Helper()
	// Use the physical Badger owner (in memory). The MemoryEngine wrapper's
	// missing canonical unwrapping is tracked separately as NOR-289.
	return fulltextMaintainedFixtureOn(tb, storage.NewMemoryEngine().BadgerEngine, count)
}

func fulltextMaintainedFixtureOn(tb testing.TB, base *storage.BadgerEngine, count int) (*StorageExecutor, *fulltextCountingStore) {
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
		nodes[i] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%06d", i)), Labels: []string{label}, Properties: map[string]any{
			"id": fmt.Sprintf("n%06d", i), "text": text, "title": "Correspondence", "current": i%80 == 0,
		}}
	}
	err := store.BulkCreateNodes(nodes)
	require.NoError(tb, err)
	// Every measured invocation must reach the procedure, including repeated
	// identical parameters; query-result caching would hide corpus rebuilds.
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
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
	node, err := store.GetNode("n000040")
	require.NoError(t, err)
	node.Properties["text"] = "newunique hotel services"
	require.NoError(t, store.UpdateNode(storage.NodeUpdateRequest{After: node}))
	scans := store.scans.Load()
	changed, err := exec.Execute(ctx, queries[0], map[string]any{"query": "newunique", "limit": int64(10)})
	require.NoError(t, err)
	require.Len(t, changed.Rows, 1)
	require.Equal(t, scans, store.scans.Load(), "one changed document must refresh without rereading the whole store")
	require.NoError(t, store.DeleteNode("n000040"))
	deleted, err := exec.Execute(ctx, queries[0], map[string]any{"query": "newunique", "limit": int64(10)})
	require.NoError(t, err)
	require.Empty(t, deleted.Rows)
	require.Equal(t, scans, store.scans.Load(), "deletion must retire its document without rereading the whole store")
}

// The carrier deliberately uses only release APIs so both A/B arms execute
// identical setup and queries, and profiles can be collected on the old head.
func BenchmarkNOR148FulltextPublic(b *testing.B) {
	exec, _ := fulltextMaintainedFixture(b, 6000)
	params := map[string]any{"query": "hotel torrey pines services", "limit": int64(10)}
	for name, query := range map[string]string{
		"top10":    "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN node.id, score ORDER BY score DESC, node.id LIMIT $limit",
		"count":    "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN count(node) AS n",
		"eligible": "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score WHERE coalesce(node.current, true) RETURN node.id, score ORDER BY score DESC, node.id LIMIT $limit",
	} {
		b.Run(name, func(b *testing.B) {
			_, err := exec.Execute(context.Background(), query, params)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := exec.Execute(context.Background(), query, params)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

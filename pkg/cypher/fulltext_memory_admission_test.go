// Author: #1683 implementation lane (Codex), 2026-11-01.
package cypher

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testutil/memorypressure"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Application admission is exercised before search: copies must already have
// been released when another workload's forecast is rejected. The sampler is
// real; this checks release ordering without prescribing GC/recovery timing.
func TestFulltextRetainedMemoryApplicationPublic(t *testing.T) {
	memorypressure.WithoutRuntimePressure(t)
	warmExecutor, store := fulltextMaintainedFixture(t, 160)
	query := fulltextMemoryQueries()[0]
	// Sample the application budget after one-time parser/procedure setup.
	// Otherwise cold initialization itself can consume the pressure fixture's
	// headroom and introduce a release before the reservation being tested.
	warm, err := warmExecutor.Execute(t.Context(), query, nil)
	require.NoError(t, err)
	require.NoError(t, warmExecutor.search().Close())
	policy := memorypressure.New(t)
	options := search.DefaultServiceOptions("fulltext_fixture")
	options.MemoryPolicy = policy
	service := search.NewService(store, search.DefaultVectorDimensions, &options)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	logs := &fulltextMemoryLogs{}
	service.SetLogger(slog.New(fixtureowner.RawLogHandler(logs.Handle)))
	executor := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	executor.SetSearchServiceResolver("fulltext_fixture", func(string) *search.Service { return service })
	require.NoError(t, policy.Refresh())
	expected, err := executor.Execute(t.Context(), query, nil)
	require.NoError(t, err)
	require.Equal(t, warm.Rows, expected.Rows)
	require.Len(t, service.FulltextRetention(), 1)
	require.True(t, service.FulltextRetention()[0].Retained)
	_, err = policy.Reserve(1 << 30)
	require.ErrorIs(t, err, memory.ErrLimitExceeded)
	require.Len(t, logs.matching("Full-text retained copies released:"), 1, "admission rejection must not precede cache release/warning")
	require.True(t, service.FulltextRetention()[0].StorageReads)
	before := store.scans.Load()
	got, err := executor.Execute(t.Context(), query, nil)
	require.NoError(t, err)
	require.Equal(t, expected.Rows, got.Rows)
	require.Greater(t, store.scans.Load(), before)
	require.NoError(t, policy.Refresh())
	_, err = executor.Execute(t.Context(), query, nil)
	require.NoError(t, err)
	require.Len(t, logs.matching("Full-text retained copies refilled:"), 1)
	require.NoError(t, service.Close())
	require.Zero(t, service.IndexSizeBytes("bm25"), "service retirement must release the same metric/ownership state")
}

type fulltextHydrationProbe struct {
	*fulltextCountingStore
	captures, nodeReads, edgeReads atomic.Int64
	before                         func(string, bool)
}

func (p *fulltextHydrationProbe) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	p.captures.Add(1)
	view, err := p.NamespacedEngine.OpenReadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return &fulltextHydrationSnapshot{EntityReadSnapshot: view.(storage.EntityReadSnapshot), probe: p}, nil
}

type fulltextHydrationSnapshot struct {
	storage.EntityReadSnapshot
	probe *fulltextHydrationProbe
}

func (v *fulltextHydrationSnapshot) ReadNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	v.probe.nodeReads.Add(1)
	if v.probe.before != nil {
		v.probe.before(string(id), false)
	}
	return v.EntityReadSnapshot.ReadNode(ctx, id)
}

func (v *fulltextHydrationSnapshot) ReadEdge(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	v.probe.edgeReads.Add(1)
	if v.probe.before != nil {
		v.probe.before(string(id), true)
	}
	return v.EntityReadSnapshot.ReadEdge(ctx, id)
}

// This observes the public adapter's actual captured point reads. Each hot
// request hydrates only its emitted row; a write after capture cannot combine
// new output properties with old scoring fields, and an old read cannot poison
// the canonical current-body cache. The next request must see the committed edit.
func TestFulltextRetainedRowsHydrateOnlyEmittedPublic(t *testing.T) {
	_, counted := fulltextMaintainedFixture(t, 160)
	probe := &fulltextHydrationProbe{fulltextCountingStore: counted}
	service := search.NewService(probe, search.DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	executor := NewStorageExecutor(probe, QueryCachePolicy{MaxEntries: 0})
	executor.SetSearchServiceResolver("fulltext_fixture", func(string) *search.Service { return service })
	_, err := executor.Execute(t.Context(), "CREATE FULLTEXT INDEX hydration_edges FOR ()-[r:LINK]-() ON EACH [r.text]", nil)
	require.NoError(t, err)
	for _, id := range []storage.EdgeID{"a", "b", "c"} {
		require.NoError(t, counted.CreateEdge(t.Context(), &storage.Edge{ID: id, StartNode: "n000000", EndNode: "n000040", Type: "LINK", Properties: map[string]any{"id": string(id), "text": "hotel"}}))
	}
	for _, relationships := range []bool{false, true} {
		query := "CALL db.index.fulltext.queryNodes('source_text','hotel',{limit:1}) YIELD node,score RETURN node.id,node.text,score"
		if relationships {
			query = "CALL db.index.fulltext.queryRelationships('hydration_edges','hotel',{limit:1}) YIELD relationship,score RETURN relationship.id,relationship.text,score"
		}
		cold, err := executor.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		require.Len(t, cold.Rows, 1)
		probe.captures.Store(0)
		probe.nodeReads.Store(0)
		probe.edgeReads.Store(0)
		scans := counted.scans.Load()
		selected := ""
		probe.before = func(id string, edge bool) {
			probe.before = nil
			selected = id
			require.Equal(t, relationships, edge)
			if edge {
				row, err := counted.GetEdge(t.Context(), storage.EdgeID(id))
				require.NoError(t, err)
				row.Properties["text"] = "later"
				require.NoError(t, counted.UpdateEdge(t.Context(), storage.EdgeUpdateRequest{After: row}))
			} else {
				row, err := counted.GetNode(t.Context(), storage.NodeID(id))
				require.NoError(t, err)
				row.Properties["text"] = "later"
				require.NoError(t, counted.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: row}))
			}
		}
		hot, err := executor.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		require.NotEmpty(t, selected, "the public path must reach the captured point reader")
		require.Equal(t, cold.Rows, hot.Rows, "output and score bits must belong to the admitted view")
		require.Equal(t, int64(1), probe.captures.Load())
		require.Equal(t, int64(1), probe.nodeReads.Load()+probe.edgeReads.Load(), "only one emitted row may be hydrated")
		require.Equal(t, scans, counted.scans.Load(), "hot hydration must not scan the corpus")
		if relationships {
			row, err := counted.GetEdge(t.Context(), storage.EdgeID(selected))
			require.NoError(t, err)
			require.Equal(t, "later", row.Properties["text"])
		} else {
			row, err := counted.GetNode(t.Context(), storage.NodeID(selected))
			require.NoError(t, err)
			require.Equal(t, "later", row.Properties["text"])
		}
		next, err := executor.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		require.Len(t, next.Rows, 1)
		require.NotEqual(t, cold.Rows[0][0], next.Rows[0][0], "the next request must retire the edited document")
		fresh := NewStorageExecutor(probe, QueryCachePolicy{MaxEntries: 0})
		fresh.SetSearchServiceResolver("fulltext_fixture", func(string) *search.Service { return nil })
		rebuilt, err := fresh.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		require.Equal(t, rebuilt.Rows, next.Rows, "the updated retained view must equal its original acquisition/rebuild")
	}
}

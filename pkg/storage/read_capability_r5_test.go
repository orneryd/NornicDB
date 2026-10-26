package storage_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type r5UnknownRead interface{ UnscopedFutureRead() (*storage.Node, error) }
type r5ExtendedNamespace struct{ *storage.NamespacedEngine }

func (*r5ExtendedNamespace) UnscopedFutureRead() (*storage.Node, error) {
	return &storage.Node{ID: "other:secret"}, nil
}

type r5LifecycleMetadata struct {
	*storage.MemoryEngine
	status map[string]any
}

func (r *r5LifecycleMetadata) LifecycleStatus() map[string]any         { return r.status }
func (*r5LifecycleMetadata) ReadCapabilityAvailable(reflect.Type) bool { return false }

func TestReadCapabilityR5NamespaceLifecycleMetadata(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	inner := &r5LifecycleMetadata{MemoryEngine: base, status: map[string]any{
		"enabled": true, "pressure_band": "normal", "paused": true,
		"top_debt_keys": []storage.MVCCLifecycleDebtKey{{Namespace: "foreign", LogicalKey: "secret"}},
		"per_namespace": map[string]map[string]int64{"tenant": {"compaction_debt_keys": 1}, "foreign": {"compaction_debt_keys": 9}},
		"readers":       []storage.SnapshotReaderInfo{{Namespace: "tenant", ReaderID: "own"}, {Namespace: "foreign", ReaderID: "secret"}},
	}}
	view := storage.NewNamespacedEngine(storage.NewTracedEngine(storage.NewWALEngine(inner, nil)), "tenant")
	reader, ok := storage.ViewMetadataForEngine[storage.MVCCLifecycleEngine](view)
	require.True(t, ok, "data unavailability must not hide scoped metadata")
	status := reader.LifecycleStatus()
	require.Equal(t, true, status["enabled"])
	require.Equal(t, "normal", status["pressure_band"])
	require.Equal(t, true, status["paused"])
	require.Equal(t, map[string]map[string]int64{"tenant": {"compaction_debt_keys": 1}}, status["per_namespace"])
	require.Equal(t, []storage.SnapshotReaderInfo{{Namespace: "tenant", ReaderID: "own"}}, status["readers"])
	require.Equal(t, int64(1), status["mvcc_active_snapshot_readers"])
	require.NotContains(t, status, "top_debt_keys")
	require.NotContains(t, inner.status, "namespace", "metadata must not mutate the provider's map")
}

type r5ExtendedComposite struct{ *storage.CompositeEngine }

func (*r5ExtendedComposite) UnscopedFutureRead() (*storage.Node, error) {
	return &storage.Node{ID: "other:secret"}, nil
}

type r5ForeignProjection struct{ storage.Engine }

func (*r5ForeignProjection) GetNodeProjected(storage.NodeID, []string) (*storage.Node, error) {
	return &storage.Node{ID: "other:secret"}, nil
}

func TestReadCapabilityR5ClosedBoundary(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	tenant := storage.NewNamespacedEngine(base, "tenant")
	composite := storage.NewCompositeEngine(map[string]storage.Engine{"tenant": tenant}, map[string]string{"tenant": "tenant"}, map[string]string{"tenant": "read"})
	for _, view := range []storage.Engine{&r5ExtendedNamespace{tenant}, &r5ExtendedComposite{composite}} {
		require.False(t, storage.HasReadCapability[r5UnknownRead](view))
		_, ok := storage.ViewMetadataForEngine[r5UnknownRead](view)
		require.False(t, ok, "metadata discovery cannot open an unowned read")
		_, err := storage.ReadWithCapability(view, func(reader r5UnknownRead) (*storage.Node, error) {
			t.Fatal("unscoped method reached")
			return reader.UnscopedFutureRead()
		}, nil)
		require.ErrorIs(t, err, storage.ErrNotImplemented)
	}
	require.Equal(t, []string{"tenant"}, tenant.ListNamespaces())
	require.Nil(t, tenant.GetSchemaForNamespace("other"))
	_, err := tenant.NodeCountByLabelInNamespace("other", "Fact")
	require.ErrorIs(t, err, storage.ErrInvalidID)
	foreign := storage.NewNamespacedEngine(&r5ForeignProjection{base}, "tenant")
	node, err := foreign.GetNodeProjected("n", []string{})
	require.NoError(t, err)
	require.Nil(t, node, "a foreign result must be rejected before translation")
}

type r5RefusingMatcher struct {
	storage.Engine
	after bool
	calls int
}

func (r *r5RefusingMatcher) MatchEdgesBetween(a, b storage.NodeID, kind string, props []string, match func(*storage.Edge) bool) ([]*storage.Edge, error) {
	r.calls++
	if r.after {
		match(&storage.Edge{ID: storage.EdgeID(strings.TrimSuffix(string(a), "a") + "e"), StartNode: a, EndNode: b, Type: kind})
	}
	return nil, storage.ErrNotImplemented
}

func TestReadCapabilityR5MatcherRefusal(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_predicate", true: "after_predicate"}[after], func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			r := &r5RefusingMatcher{Engine: base, after: after}
			tenant := storage.NewNamespacedEngine(r, "tenant")
			for _, id := range []storage.NodeID{"a", "b"} {
				_, err := tenant.CreateNode(&storage.Node{ID: id})
				require.NoError(t, err)
			}
			require.NoError(t, tenant.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK"}))
			view := storage.NewTracedEngine(storage.NewWALEngine(tenant, nil))
			calls := 0
			edges, err := storage.MatchEdgesBetween(view, "a", "b", "LINK", nil, func(*storage.Edge) bool { calls++; return true })
			require.Equal(t, 1, r.calls)
			require.Equal(t, 1, calls, "a predicate must never be replayed")
			if after {
				require.ErrorIs(t, err, storage.ErrNotImplemented)
			} else {
				require.NoError(t, err)
				require.Len(t, edges, 1)
			}
		})
	}
}

func TestReadCapabilityR5NamespaceFailures(t *testing.T) {
	base, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	for _, ns := range []string{"tenant", "other"} {
		id := storage.NodeID(ns + ":n")
		_, err = base.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
		require.NoError(t, base.UpdateNodeEmbeddingSidecar(&storage.Node{ID: id, EmbedMeta: map[string]any{"embedding_failed": true, "embedding_error": ns}}))
	}
	for _, inner := range []storage.Engine{storage.NewTracedEngine(storage.NewWALEngine(base, nil)), storage.NewWALEngine(storage.NewTracedEngine(base), nil)} {
		tenant := storage.NewNamespacedEngine(inner, "tenant")
		visits := 0
		count, err := tenant.StreamParkedEmbeddingFailures(context.Background(), func(id storage.NodeID, meta map[string]any) error {
			visits++
			require.Equal(t, storage.NodeID("n"), id)
			require.Equal(t, "tenant", meta["embedding_error"])
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, count)
		require.Equal(t, 1, visits)
	}
}

func TestReadCapabilityR5WALVisitorRefusal(t *testing.T) {
	base, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	for _, id := range []storage.NodeID{"tenant:a", "tenant:b"} {
		_, err = base.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
		require.NoError(t, base.UpdateNodeEmbeddingSidecar(&storage.Node{ID: id, EmbedMeta: map[string]any{"embedding_failed": true}}))
	}
	wal := storage.NewWALEngine(base, nil)
	for _, stop := range []int{1, 2} {
		visits := 0
		count, err := wal.StreamParkedEmbeddingFailures(context.Background(), func(storage.NodeID, map[string]any) error {
			visits++
			if visits == stop {
				return storage.ErrNotImplemented
			}
			return nil
		})
		require.ErrorIs(t, err, storage.ErrNotImplemented)
		require.Equal(t, stop, count)
		require.Equal(t, stop, visits)
	}
}

type r5RefusingFailures struct{ storage.Engine }

func (r5RefusingFailures) StreamParkedEmbeddingFailures(context.Context, func(storage.NodeID, map[string]any) error) (int, error) {
	return 0, storage.ErrNotImplemented
}

func TestReadCapabilityR5WALProviderRefusal(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	wal := storage.NewWALEngine(r5RefusingFailures{base}, nil)
	count, err := wal.StreamParkedEmbeddingFailures(context.Background(), func(storage.NodeID, map[string]any) error {
		t.Fatal("refused provider must not deliver")
		return nil
	})
	require.ErrorIs(t, err, storage.ErrNotImplemented)
	require.Zero(t, count)
}

func TestReadCapabilityR5CompositeMaintenanceIdentity(t *testing.T) {
	base, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	base.SetEmbeddingsEnabled(true)
	views, names, modes := map[string]storage.Engine{}, map[string]string{}, map[string]string{}
	for _, name := range []string{"one", "two", "foreign"} {
		view := storage.NewNamespacedEngine(base, name)
		_, err := view.CreateNode(&storage.Node{ID: "pending", Properties: map[string]any{"text": "pending"}})
		require.NoError(t, err)
		_, err = view.CreateNode(&storage.Node{ID: "failed", EmbedMeta: map[string]any{"embedding_failed": true}})
		require.NoError(t, err)
		require.NoError(t, view.UpdateNodeEmbeddingSidecar(&storage.Node{ID: "failed", EmbedMeta: map[string]any{"embedding_failed": true, "embedding_error": name}}))
		if name != "foreign" {
			views[name], names[name], modes[name] = storage.NewNamespacedEngine(storage.NewTracedEngine(storage.NewWALEngine(base, nil)), name), name, "read"
		}
	}
	composite := storage.NewCompositeEngine(views, names, modes)
	t.Run("failures", func(t *testing.T) {
		var ids []storage.NodeID
		count, err := storage.VisitEmbeddingFailures(composite, context.Background(), nil, func(id storage.NodeID, meta map[string]any) error {
			kind, database, local, qualified := storage.ParseElementID(string(id))
			require.True(t, qualified, "ID-only delivery must retain its constituent")
			require.Equal(t, "4", kind)
			require.Equal(t, "failed", local)
			require.Equal(t, meta["embedding_error"], database)
			ids = append(ids, id)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, count)
		require.ElementsMatch(t, []storage.NodeID{"4:one:failed", "4:two:failed"}, ids)
	})
	t.Run("pending", func(t *testing.T) {
		require.Equal(t, 2, storage.CountPendingEmbeddingIDs(composite, nil))
		count := storage.CountPendingEmbeddingIDs(composite, func(id []byte) bool { return string(id) != "4:one:pending" })
		require.Equal(t, 1, count, "excluding one constituent's ID cannot exclude or retain another by accident")
	})
	for _, stop := range []int{1, 2} {
		visits := 0
		count, err := storage.VisitEmbeddingFailures(composite, context.Background(), nil, func(storage.NodeID, map[string]any) error {
			visits++
			if visits == stop {
				return storage.ErrNotImplemented
			}
			return nil
		})
		require.ErrorIs(t, err, storage.ErrNotImplemented)
		require.Equal(t, stop, visits)
		require.Equal(t, visits, count, "held constituent callbacks are not delivered rows")
	}
}

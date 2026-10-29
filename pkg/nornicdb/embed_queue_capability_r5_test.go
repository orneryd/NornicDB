package nornicdb_test

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type r5FailureRefusal struct {
	storage.Engine
	calls int
}

func (r *r5FailureRefusal) StreamParkedEmbeddingFailures(context.Context, func(storage.NodeID, map[string]any) error) (int, error) {
	r.calls++
	return 0, storage.ErrNotImplemented
}
func TestEmbedWorkerR5FailureList(t *testing.T) {
	for _, refusal := range []bool{false, true} {
		t.Run(map[bool]string{false: "scoped_sidecar", true: "ordinary_on_refusal"}[refusal], func(t *testing.T) {
			base, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			for _, ns := range []string{"tenant", "other"} {
				id := storage.NodeID(ns + ":n")
				_, err = base.CreateNode(&storage.Node{ID: id, EmbedMeta: map[string]any{"embedding_failed": true, "embedding_error": ns}})
				require.NoError(t, err)
				require.NoError(t, base.UpdateNodeEmbeddingSidecar(&storage.Node{ID: id, EmbedMeta: map[string]any{"embedding_failed": true, "embedding_error": ns}}))
			}
			var inner storage.Engine = base
			r := &r5FailureRefusal{Engine: base}
			if refusal {
				inner = r
			}
			view := storage.NewNamespacedEngine(storage.NewTracedEngine(storage.NewWALEngine(inner, nil)), "tenant")
			cfg := nornicdb.DefaultEmbedWorkerConfig()
			cfg.DeferWorkerStart = true
			worker := nornicdb.NewEmbedWorker(nil, view, cfg)
			t.Cleanup(worker.Close)
			failures, err := worker.ParkedEmbeddingFailures(context.Background(), 0)
			require.NoError(t, err)
			require.Len(t, failures, 1)
			require.Equal(t, storage.NodeID("n"), failures[0].NodeID)
			require.Equal(t, "tenant", failures[0].Error)
			if refusal {
				require.Positive(t, r.calls)
			}
		})
	}
}

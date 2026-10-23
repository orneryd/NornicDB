package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type pendingStreamEngine struct{ Engine }

// The public drain counter must use the same eligibility and exclusion rules
// through durable indexes, generic streams, namespaces, WAL and composites.
func TestEmbeddingPendingCounterPublicWrappers(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			var base Engine
			if backend == "badger" {
				disk, err := NewBadgerEngine(t.TempDir())
				require.NoError(t, err)
				disk.SetEmbeddingsEnabled(true)
				base = disk
			} else {
				memory := newTestMemoryEngine(t)
				memory.SetEmbeddingsEnabled(true)
				// Hide the optional counter capability to exercise the public
				// stream fallback with the same real storage eligibility rules.
				base = &pendingStreamEngine{Engine: memory}
			}
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			one := NewNamespacedEngine(base, "one")
			two := NewNamespacedEngine(base, "two")
			for _, store := range []*NamespacedEngine{one, two} {
				_, err := store.CreateNode(&Node{ID: "a", Properties: map[string]any{"text": "pending"}})
				require.NoError(t, err)
			}
			_, err := one.CreateNode(&Node{ID: "b", Properties: map[string]any{"text": "pending"}})
			require.NoError(t, err)
			_, err = one.CreateNode(&Node{ID: "embedded", ChunkEmbeddings: [][]float32{{1, 0}}, Properties: map[string]any{"text": "done"}})
			require.NoError(t, err)
			_, err = one.CreateNode(&Node{ID: "terminal", EmbedMeta: map[string]any{"embedding_failed": true}, Properties: map[string]any{"text": "parked"}})
			require.NoError(t, err)
			wal := NewNamespacedEngine(NewWALEngine(base, nil), "one")
			composite := NewCompositeEngine(map[string]Engine{"one": one, "two": two},
				map[string]string{"one": "one", "two": "two"},
				map[string]string{"one": "read", "two": "read"})
			views := []struct {
				name  string
				store Engine
				count int
			}{{"namespace", one, 2}, {"wal", wal, 2}, {"composite", composite, 3}}
			for _, view := range views {
				t.Run(view.name, func(t *testing.T) {
					require.Equal(t, view.count, CountPendingEmbeddingIDs(view.store, nil))
					require.Equal(t, 1, CountPendingEmbeddingIDs(view.store, func(id []byte) bool { return string(id) != "a" }),
						"IDs owned by claims or DLQ must not also count in MAIN")
				})
			}
			require.NoError(t, one.DeleteNode("a"))
			require.NoError(t, one.DeleteNode("b"))
			require.NoError(t, two.DeleteNode("a"))
			for _, view := range views {
				require.Zero(t, CountPendingEmbeddingIDs(view.store, nil), view.name+": deletion must drain every representation")
			}
		})
	}
}

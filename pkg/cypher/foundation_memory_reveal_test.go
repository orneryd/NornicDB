// Author: Lane #1644 (Codex). NOR-289: physical wrapper identity must let
// the existing reveal scope owner reach native Badger, without exposing reads.
package cypher_test

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFoundationPublicMemoryReveal(t *testing.T) {
	for _, name := range []string{"namespace-memory", "wal-traced-namespace-memory", "namespace-traced-wal-memory"} {
		t.Run(name, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			defer base.Close()
			tenant := storage.NewNamespacedEngine(base, "tenant")
			_, err := tenant.CreateNode(&storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"name": "hidden"}})
			require.NoError(t, err)
			require.NoError(t, base.SetManualSuppression(storage.StoredEntityNodeKind, "tenant:a", true))
			var issued storage.Engine = tenant
			switch name {
			case "wal-traced-namespace-memory":
				issued = storage.NewWALEngine(storage.NewTracedEngine(tenant), nil)
			case "namespace-traced-wal-memory":
				issued = storage.NewNamespacedEngine(storage.NewTracedEngine(storage.NewWALEngine(base, nil)), "tenant")
			}
			executor := cypher.NewStorageExecutor(issued)
			ctx := context.Background()
			for _, explicit := range []bool{false, true} {
				if explicit {
					_, err = executor.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				ordinary, err := executor.Execute(ctx, "MATCH (n:Doc) RETURN n.name", nil)
				require.NoError(t, err)
				require.Empty(t, ordinary.Rows)
				revealed, err := executor.Execute(ctx, "MATCH (n:Doc) RETURN reveal(n).name", nil)
				require.NoError(t, err)
				require.Equal(t, [][]any{{"hidden"}}, revealed.Rows)
				ordinary, err = executor.Execute(ctx, "MATCH (n:Doc) RETURN n.name", nil)
				require.NoError(t, err)
				require.Empty(t, ordinary.Rows, "reveal lease must not leak to the next statement")
				if explicit {
					_, err = executor.Execute(ctx, "ROLLBACK", nil)
					require.NoError(t, err)
				}
			}
			require.Same(t, base.BadgerEngine, storage.UnwrapEngine(issued))
		})
	}
}

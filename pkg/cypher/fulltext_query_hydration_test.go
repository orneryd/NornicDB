package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

// A fulltext query reads text and node metadata, not stored vector chunks.
// The existing poisoned fixture proves the native full read cannot decode them.
func TestFulltextQueryLeavesStoredVectorsUnread(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		g := newPoisonedQueryScanGraph(t)
		snapshot, err := storage.OpenEntityReadSnapshot(t.Context(), g.store, false)
		require.NoError(t, err)
		reader, ok := snapshot.(storage.QueryNodeReader)
		require.True(t, ok)
		node, err := reader.GetQueryNode(t.Context(), "doc-0")
		require.NoError(t, err)
		require.Equal(t, "doc 0", node.Properties["title"])
		require.Empty(t, node.ChunkEmbeddings)
		_, err = snapshot.ReadNode(t.Context(), "doc-0")
		require.ErrorContains(t, err, "embedding chunk 0")
		snapshot.Close()
		service := search.NewService(g.store, search.DefaultVectorDimensions, nil)
		t.Cleanup(func() { service.Close() })
		g.exec.SetSearchService(service)
		_, err = g.exec.Execute(t.Context(), "CREATE FULLTEXT INDEX doc_text FOR (n:Doc) ON EACH [n.title]", nil)
		require.NoError(t, err)
		for _, explicit := range []bool{false, true} {
			t.Run(map[bool]string{false: "autocommit", true: "transaction"}[explicit], func(t *testing.T) {
				if explicit {
					_, err := g.exec.Execute(t.Context(), "BEGIN", nil)
					require.NoError(t, err)
					defer g.exec.Execute(t.Context(), "ROLLBACK", nil)
				}
				for _, term := range []string{"*", "doc", "doc"} {
					result, err := g.exec.Execute(t.Context(), "CALL db.index.fulltext.queryNodes('doc_text', $term) YIELD node, score RETURN node.title", map[string]interface{}{"term": term})
					require.NoError(t, err)
					require.Len(t, result.Rows, 4)
				}
			})
		}

	})
}

// Native Go procedures retain their full-image input contract even when the
// query only needs metadata for fulltext matching and ranking.
func TestFulltextResultNativeProcedureReceivesStoredVectors(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const name = "nativeFulltext.vectorComponent"
		cleanupAdmissionProcedure(t, name)
		require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: name, Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 1, Returns: []ProcedureColumn{{Name: "value", Type: "FLOAT"}}}, func(_ context.Context, _ *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
			return &ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{nor413VectorComponent(args[0])}}}, nil
		}))
		graph := newLargeEmbeddingGraph(t, 4)
		service := search.NewService(graph.store, search.DefaultVectorDimensions, nil)
		t.Cleanup(func() { require.NoError(t, service.Close()) })
		graph.exec.SetSearchService(service)
		_, err := graph.exec.Execute(t.Context(), "CREATE FULLTEXT INDEX native_fulltext FOR (n:Doc) ON EACH [n.title]", nil)
		require.NoError(t, err)
		got, err := graph.exec.Execute(t.Context(), "CALL db.index.fulltext.queryNodes('native_fulltext','doc') YIELD node CALL nativeFulltext.vectorComponent(node) YIELD value RETURN node.title,value ORDER BY node.title", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"doc 0", float64(0)}, {"doc 1", float64(0)}, {"doc 2", float64(0)}, {"doc 3", float64(1)}}, got.Rows)
	})
}

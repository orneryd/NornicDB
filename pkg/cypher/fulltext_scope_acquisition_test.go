package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type fulltextScopedStream struct {
	*storage.NamespacedEngine
	labels    []string
	delivered int
}

func (s *fulltextScopedStream) StreamNodesWithOptions(ctx context.Context, options storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	s.labels = append(s.labels, options.Label)
	return s.NamespacedEngine.StreamNodesWithOptions(ctx, options, func(node *storage.Node) error {
		s.delivered++
		return visit(node)
	})
}

func TestFulltextPopulationUsesDeclaredLabels(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		inner := storage.NewNamespacedEngine(newTestMemoryEngine(t), "scoped")
		for i := range 128 {
			labels := []string{"Other"}
			if i < 4 {
				labels = []string{"Doc"}
			}
			if i >= 4 && i < 8 {
				labels = []string{"Note"}
			}
			if i == 0 {
				labels = append(labels, "Note")
			}
			_, err := inner.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("node-%03d", i)), Labels: labels, Properties: map[string]any{"text": "hello world"}})
			require.NoError(t, err)
		}
		for _, definition := range []struct {
			name, pattern string
			count         int
		}{
			{"docs", "n:Doc", 4}, {"notes_and_docs", "n:Doc|Note", 8},
		} {
			t.Run(definition.name, func(t *testing.T) {
				boundary := &fulltextScopedStream{NamespacedEngine: inner}
				exec := NewStorageExecutor(boundary, QueryCachePolicy{MaxEntries: 0, TTL: 0})
				service := search.NewService(boundary, search.DefaultVectorDimensions, nil)
				t.Cleanup(func() { require.NoError(t, service.Close()) })
				exec.SetSearchService(service)
				_, err := exec.Execute(t.Context(), "CREATE FULLTEXT INDEX "+definition.name+" FOR ("+definition.pattern+") ON EACH [n.text]", nil)
				require.NoError(t, err)
				response, err := exec.Execute(t.Context(), "CALL db.index.fulltext.queryNodes('"+definition.name+"','hello') YIELD node,score RETURN node.text,score", nil)
				require.NoError(t, err)
				require.Len(t, response.Rows, definition.count)
				require.NotEmpty(t, boundary.labels, "public population must reach the observed native reader")
				require.NotContains(t, boundary.labels, "", "declared label population must not scan the whole graph")
				require.LessOrEqual(t, boundary.delivered, definition.count+1, "a multi-label entity is acquired once per label, never the unrelated graph")
			})
		}
	})
}

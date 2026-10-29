package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type fixedQueryEmbedder struct{}

func (fixedQueryEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{1, 0}, nil
}

func (fixedQueryEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	return []string{text}, nil
}

// db.retrieve / db.rretrieve rows and page.results carry each hit's
// matches (#1060): the matched chunk and the query term occurrences, each
// with its property and [start, end) in code points, so substring() cuts
// the passage out of the node.
func TestRetrieveReturnsMatches(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	exec.SetEmbedder(fixedQueryEmbedder{})
	properties := map[string]interface{}{"text": "Rent is due monthly. Notices go to Müller."}
	node := &storage.Node{ID: "doc", Labels: []string{"Document"}, Properties: properties}
	source := embeddingutil.BuildEmbedText(properties, nil, &embeddingutil.EmbedTextOptions{})
	embeddingutil.ApplyManagedEmbedding(node, [][]float32{{0, 1}, {1, 0}}, "test", 2, time.Now())
	embeddingutil.RecordChunkSpans(node, source, []string{"text: Rent is due monthly. ", "Notices go to Müller."})
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	service := search.NewService(store, 2, nil)
	require.NoError(t, service.BuildIndexes(ctx))
	exec.SetSearchService(service)

	want := []interface{}{"chunk 1: Notices go to Müller.", "term müller: Müller"}
	for _, procedure := range []string{"db.retrieve", "db.rretrieve"} {
		result, err := exec.Execute(ctx, "CALL "+procedure+"({query: 'müller', limit: 5}) YIELD node, matches "+
			"RETURN [m IN matches | m.kind + ' ' + coalesce(toString(m.chunk), m.term) + ': ' + substring(node[m.property], m.start, m.end - m.start)] AS passages", nil)
		require.NoError(t, err, procedure)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, procedure)
	}

	// matches: false skips them.
	skipped, err := exec.Execute(ctx, "CALL db.retrieve({query: 'müller', limit: 5, matches: false}) YIELD matches RETURN matches", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{}}}, skipped.Rows)

	result, err := exec.Execute(ctx, "CALL db.retrieve({query: 'müller', limit: 5, n: 1}) YIELD page "+
		"RETURN [r IN page.results | [m IN r.matches | m.kind + ' ' + coalesce(toString(m.chunk), m.term) + ': ' + substring(r.node[m.property], m.start, m.end - m.start)]] AS passages", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{want}}}, result.Rows)
}

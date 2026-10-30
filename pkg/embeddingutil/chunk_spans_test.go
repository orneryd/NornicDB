package embeddingutil

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestBuildEmbedTextIsBuildText(t *testing.T) {
	for _, opts := range []*EmbedTextOptions{nil, {IncludeLabels: false}, {Include: []string{"text"}}, {Exclude: []string{"pages"}, IncludeLabels: true}} {
		for _, properties := range []map[string]any{
			{},
			{"text": "body"},
			{"text": "a\nb", "title": "T", "pages": []any{int64(1), "x"}, "n": int64(3), "ok": true, "none": nil, "m": map[string]any{"k": "v"}},
		} {
			for _, labels := range [][]string{nil, {"Doc"}, {"A", "B"}} {
				require.Equal(t, BuildText(properties, labels, opts), BuildEmbedText(properties, labels, opts).Text)
			}
		}
	}
}

// A chunk's spans are the parts of string property values it covers, in
// code points, trimmed; labels, keys and non-string values have none, and
// a chunk that runs across two properties has a span in each (#1060).
func TestChunkSpans(t *testing.T) {
	properties := map[string]any{"search_text": "Müller GmbH invoice", "text": "Straße 1.\n\nRent is due.", "pages": []any{int64(1), int64(2)}}
	source := BuildEmbedText(properties, []string{"Doc"}, nil)
	require.Equal(t, "labels: Doc\npages: 1, 2\nsearch_text: Müller GmbH invoice\ntext: Straße 1.\n\nRent is due.", source.Text)
	// As Voyage's contextualized auto-chunking returns them: verbatim, tiling.
	chunks := []string{"labels: Doc\npages: 1, 2\nsearch_text: Müller GmbH ", "invoice\ntext: Straße 1.\n\n", "Rent is due.", "not in the text"}
	require.Equal(t, []ChunkSpan{
		{Chunk: 0, Property: "search_text", Start: 0, End: 11},
		{Chunk: 1, Property: "search_text", Start: 12, End: 19},
		{Chunk: 1, Property: "text", Start: 0, End: 9},
		{Chunk: 2, Property: "text", Start: 11, End: 23},
	}, source.ChunkSpans(chunks))
	require.Empty(t, source.ChunkSpans([]string{"labels: Doc\npages: 1, 2"}))
}

// RecordChunkSpans stores the spans in EmbedMeta, and StoredChunkSpans reads
// them back from the stored (msgpack) form; a node whose chunks don't match
// its vectors one to one keeps none, not an earlier embedding's.
func TestRecordChunkSpans(t *testing.T) {
	properties := map[string]any{"text": "first part. second part."}
	source := BuildEmbedText(properties, nil, &EmbedTextOptions{})
	chunks := []string{"text: first part. ", "second part."}
	node := &storage.Node{ChunkEmbeddings: [][]float32{{1}, {2}}, EmbedMeta: map[string]any{}}
	RecordChunkSpans(node, source, chunks)
	want := []ChunkSpan{{Chunk: 0, Property: "text", Start: 0, End: 11}, {Chunk: 1, Property: "text", Start: 12, End: 24}}
	spans, ok := StoredChunkSpans(node.EmbedMeta)
	require.True(t, ok)
	require.Equal(t, want, spans)

	encoded, err := msgpack.Marshal(node.EmbedMeta)
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, msgpack.Unmarshal(encoded, &decoded))
	spans, ok = StoredChunkSpans(decoded)
	require.True(t, ok)
	require.Equal(t, want, spans)

	single := &storage.Node{ChunkEmbeddings: [][]float32{{1}}, EmbedMeta: map[string]any{chunkSpansKey: []any{}}}
	RecordChunkSpans(single, source, chunks)
	_, ok = StoredChunkSpans(single.EmbedMeta)
	require.False(t, ok)
	require.Equal(t, -1, ChunkCount(single.EmbedMeta))
	require.Equal(t, 2, ChunkCount(map[string]any{"chunk_count": uint8(2)}))
}

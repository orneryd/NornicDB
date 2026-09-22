package search

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRerankCandidateContentPrependsTitleAndExpandsChunkNeighbours(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	chunks := []string{"chunk zero", "chunk one", "chunk two matched", "chunk three", "chunk four"}
	node := &storage.Node{
		ID:         "nornic:doc",
		Labels:     []string{"Doc"},
		Properties: map[string]interface{}{"title": "  The Title  ", "text": strings.Join(chunks, " ")},
		EmbedMeta:  map[string]interface{}{"chunk_texts": chunks},
	}
	result := rrfResult{ID: "nornic:doc", MatchID: "nornic:doc-chunk-2", VectorRank: 1}

	content := svc.rerankCandidateContent(node, result, "matched", 4096)
	require.Equal(t, "The Title\nchunk zero chunk one chunk two matched chunk three chunk four", content)

	// Header and body share one budget; the matched chunk is kept whole first.
	content = svc.rerankCandidateContent(node, result, "matched", len("The Title")+1+len("chunk two matched")+1)
	require.Equal(t, "The Title\nchunk two matched", content)

	// The window grows in document order: the next chunk first, then the previous one.
	content = svc.rerankCandidateContent(node, result, "matched", len("The Title")+1+len("chunk one chunk two matched chunk three"))
	require.Equal(t, "The Title\nchunk one chunk two matched chunk three", content)

	// A neighbour that does not fit whole is added as a bounded prefix, then expansion stops.
	content = svc.rerankCandidateContent(node, result, "matched", len("The Title")+1+len("chunk two matched chunk th"))
	require.Equal(t, "The Title\nchunk two matched chunk th", content)

	// No identifying properties: body only, as before.
	delete(node.Properties, "title")
	content = svc.rerankCandidateContent(node, result, "matched", 4096)
	require.Equal(t, "chunk zero chunk one chunk two matched chunk three chunk four", content)
}

func TestRerankCandidateContentHeaderPropertiesAreConfigurableAndBounded(t *testing.T) {
	t.Setenv(EnvSearchRerankContextProperties, " name , missing,title ")
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	longName := strings.Repeat("н", 300)
	node := &storage.Node{
		ID:         "nornic:doc",
		Properties: map[string]interface{}{"name": longName, "title": "T"},
		EmbedMeta:  map[string]interface{}{"chunk_texts": []any{"only chunk"}},
	}
	content := svc.rerankCandidateContent(node, rrfResult{ID: "nornic:doc", MatchID: "nornic:doc"}, "q", 4096)
	header, body, found := strings.Cut(content, "\n")
	require.True(t, found)
	require.Equal(t, "only chunk", body)
	require.True(t, strings.HasSuffix(header, " | T"))
	require.LessOrEqual(t, utf8.RuneCountInString(strings.TrimSuffix(header, " | T")), rerankContextPropertyMaxChars)
	require.Equal(t, rerankContextPropertyMaxChars, utf8.RuneCountInString(strings.TrimSuffix(header, " | T")), "a 300-character name is cut to 256 characters, not bytes")
	require.True(t, utf8.ValidString(header))
}

func TestExpandedChunkWindowDoesNotRepeatOverlap(t *testing.T) {
	// Chunker overlap: each chunk repeats the tail of the previous one.
	chunks := []string{"a b c d", "c d e f", "e f g h", "g h i j"}
	require.Equal(t, "a b c d e f g h i j", expandedChunkWindow(chunks, 2, "g", 4096))
	require.Equal(t, "a b c d e f g h i j", expandedChunkWindow(chunks, 0, "a", 4096))
	require.Equal(t, "a b c d e f g h i j", expandedChunkWindow(chunks, 3, "j", 4096))
}

func TestExpandedChunkWindowKeepsQueryWindowForOversizedChunk(t *testing.T) {
	chunk := strings.Repeat("предисловие ", 40) + "alpha tango" + strings.Repeat(" заключение", 40)
	window := expandedChunkWindow([]string{"before", chunk, "after"}, 1, "alpha tango", 96)
	require.LessOrEqual(t, utf8.RuneCountInString(window), 96)
	require.Contains(t, window, "alpha")
	require.True(t, utf8.ValidString(window))
}

func TestExpandedChunkWindowNeverExceedsBudgetWithMultibyteText(t *testing.T) {
	chunks := []string{strings.Repeat("я", 50), strings.Repeat("ю", 50), strings.Repeat("э", 50)}
	for budget := 1; budget < 400; budget += 7 {
		window := expandedChunkWindow(chunks, 1, "", budget)
		require.LessOrEqual(t, utf8.RuneCountInString(window), budget, "budget %d", budget)
		require.True(t, utf8.ValidString(window), "budget %d", budget)
	}
}

func TestRerankBudgetCountsCharactersNotBytes(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	for _, script := range []struct {
		name   string
		chunks []string
	}{
		{"latin", []string{strings.Repeat("a", 100), strings.Repeat("b", 100), strings.Repeat("c", 100)}},
		{"cyrillic", []string{strings.Repeat("я", 100), strings.Repeat("ю", 100), strings.Repeat("э", 100)}},
		{"cjk", []string{strings.Repeat("字", 100), strings.Repeat("文", 100), strings.Repeat("本", 100)}},
	} {
		node := &storage.Node{ID: "nornic:doc", EmbedMeta: map[string]interface{}{"chunk_texts": script.chunks}}
		content := svc.rerankCandidateContent(node, rrfResult{ID: "nornic:doc", MatchID: "nornic:doc-chunk-1"}, "", 250)
		// 100 (matched) + 1 + 100 (next) + 1 + 48 (bounded previous suffix) = 250 characters in every script.
		require.Equal(t, 250, utf8.RuneCountInString(content), script.name)
		bytesPerChar := len(script.chunks[0]) / 100
		require.Equal(t, 248*bytesPerChar+2, len(content), script.name+": 248 letters plus two ASCII spaces; byte length scales with the script")
	}
	// A lexical-only window is a character window too.
	text := strings.Repeat("字 ", 300) + "alpha" + strings.Repeat(" 字", 300)
	window := boundedWindow(text, strings.Index(text, "alpha"), 61)
	require.LessOrEqual(t, utf8.RuneCountInString(window), 61)
	require.Greater(t, utf8.RuneCountInString(window), 40)
	require.Contains(t, window, "alpha")
	require.Equal(t, "字字字", boundedSuffix("字字字字字", 3))
	require.Equal(t, "字字", boundedPrefix("字字字字字", 2))
}

func TestRerankDefaultBudgetIs2048Chars(t *testing.T) {
	require.Equal(t, 2048, defaultRerankMaxDocumentChars)
	require.Equal(t, 2048, effectiveRerankMaxChars(&SearchOptions{}))
}

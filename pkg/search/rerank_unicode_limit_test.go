package search

import (
	"context"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNOR401RerankProviderTextCutsPreserveUTF8 is the focused N152-88
// regression matrix for the approved provider input contract: both rerankers
// cut model input at character boundaries, so their configured character
// allowance yields the same amount of text for Latin, Cyrillic, CJK and mixed
// content and never hands a split rune to the model. The exported rerankers are
// exercised through their existing scorer/generator DI seams, and the ASCII row
// preserves the previous byte-index behavior for plain ASCII text.
func TestNOR401RerankProviderTextCutsPreserveUTF8(t *testing.T) {
	for _, tc := range []struct {
		name, query, document string
		max                   int
	}{
		{"ascii", "query123", "123456789", 5},
		{"cyrillic", "запрос", "число", 5},
		{"cjk", "查询问题测试", "汉字数字测试", 5},
		{"mixed", "A汉字B5Z", "A汉字B5Z", 5},
		{"one-character", "éé", "éé", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wantDoc := string([]rune(tc.document)[:tc.max])
			wantQuery := string([]rune(tc.query)[:tc.max])
			t.Run("local", func(t *testing.T) {
				scorer := &captureScorer{score: 0.5}
				reranker := NewLocalReranker(scorer, &LocalRerankerConfig{
					Enabled: true, Timeout: time.Second, MaxCandidates: 4, MaxDocChars: tc.max,
				}, testRerankAdmission())
				results, err := reranker.Rerank(t.Context(), tc.query, []RerankCandidate{{ID: "doc", Content: tc.document, Score: 0.5}})
				require.NoError(t, err)
				require.Len(t, results, 1)
				require.Len(t, scorer.docs, 1)
				assert.True(t, utf8.ValidString(scorer.docs[0]), "model input must be valid UTF-8")
				assert.Equal(t, wantDoc, scorer.docs[0], "document allowance counts characters")
				require.Equal(t, tc.query, scorer.queries[0], "local query has no configured truncation")
			})
			t.Run("llm", func(t *testing.T) {
				var prompt string
				reranker := NewLLMReranker(&LLMRerankerConfig{
					Enabled: true, Timeout: time.Second, MaxCandidates: 4, MaxDocChars: tc.max, MaxQueryChars: tc.max,
				}, func(_ context.Context, actual string) (string, error) {
					prompt = actual
					return `{"ranked":[{"index":0,"score":0.9}]}`, nil
				})
				results, err := reranker.Rerank(t.Context(), tc.query, []RerankCandidate{{ID: "doc", Content: tc.document, Score: 0.5}})
				require.NoError(t, err)
				require.Len(t, results, 1)
				assert.True(t, utf8.ValidString(prompt), "model prompt must be valid UTF-8")
				assert.Contains(t, prompt, "QUERY:\n"+wantQuery+"\n\n", "query allowance counts characters")
				assert.Contains(t, prompt, wantDoc+"\n---\n", "document allowance counts characters")
			})
		})
	}
}

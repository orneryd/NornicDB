package search

import (
	"context"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestV152RCCRerankByteSliceCause compares what the production rerankers pass
// to the model with a test-side copy of the byte slice at local_rerank.go:144
// and llm_rerank.go:107/:171, and with a rune-count corrected copy. The
// production text equals the byte copy in every case; the corrected copy alone
// gives the character allowance; the ASCII control is equal in all three.
func TestV152RCCRerankByteSliceCause(t *testing.T) {
	byteCut := func(s string, n int) string {
		s = strings.TrimSpace(s)
		if len(s) > n {
			return s[:n]
		}
		return s
	}
	runeCut := func(s string, n int) string {
		r := []rune(strings.TrimSpace(s))
		if len(r) > n {
			return string(r[:n])
		}
		return string(r)
	}
	for _, tc := range []struct {
		name, query, doc string
		max              int
		trigger          bool
	}{
		{"ascii_control", "query123", "123456789", 5, false},
		{"cjk_trigger", "查询问题测试", "汉字数字测试", 5, true},
		{"cyrillic_trigger", "запрос", "число!", 5, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := runeCut(tc.doc, tc.max)
			wantQuery := runeCut(tc.query, tc.max)
			scorer := &captureScorer{score: 0.5}
			local := NewLocalReranker(scorer, &LocalRerankerConfig{Enabled: true, Timeout: time.Second, MaxCandidates: 4, MaxDocChars: tc.max}, testRerankAdmission())
			_, err := local.Rerank(t.Context(), tc.query, []RerankCandidate{{ID: "doc", Content: tc.doc, Score: 0.5}})
			require.NoError(t, err)
			require.Len(t, scorer.docs, 1)
			actual := scorer.docs[0]
			var prompt string
			llm := NewLLMReranker(&LLMRerankerConfig{Enabled: true, Timeout: time.Second, MaxCandidates: 4, MaxDocChars: tc.max, MaxQueryChars: tc.max},
				func(_ context.Context, p string) (string, error) { prompt = p; return `{"ranked":[{"index":0,"score":0.9}]}`, nil })
			_, err = llm.Rerank(t.Context(), tc.query, []RerankCandidate{{ID: "doc", Content: tc.doc, Score: 0.5}})
			require.NoError(t, err)
			t.Logf("RCC_RERANK owner=pkg/search/local_rerank.go:143-144 doc_actual=%q byte_copy=%q rune_copy=%q valid=%v", actual, byteCut(tc.doc, tc.max), want, utf8.ValidString(actual))
			t.Logf("RCC_RERANK owner=pkg/search/llm_rerank.go:106-107,171 prompt_has_byte_query=%v prompt_has_byte_doc=%v prompt_has_rune_query=%v prompt_has_rune_doc=%v valid=%v",
				strings.Contains(prompt, "QUERY:\n"+byteCut(tc.query, tc.max)+"\n\n"), strings.Contains(prompt, byteCut(tc.doc, tc.max)+"\n---\n"),
				strings.Contains(prompt, "QUERY:\n"+wantQuery+"\n\n"), strings.Contains(prompt, want+"\n---\n"), utf8.ValidString(prompt))
			require.Equal(t, byteCut(tc.doc, tc.max), actual, "local reranker text is exactly the byte slice")
			require.Contains(t, prompt, "QUERY:\n"+byteCut(tc.query, tc.max)+"\n\n", "LLM query is exactly the byte slice")
			require.Contains(t, prompt, byteCut(tc.doc, tc.max)+"\n---\n", "LLM document is exactly the byte slice")
			require.True(t, utf8.ValidString(want), "corrected copy is valid UTF-8")
			if !tc.trigger {
				require.Equal(t, want, actual)
				return
			}
			assert.Equal(t, want, actual, "allowance counts characters")
			assert.True(t, utf8.ValidString(actual), "model input must be valid UTF-8")
		})
	}
}

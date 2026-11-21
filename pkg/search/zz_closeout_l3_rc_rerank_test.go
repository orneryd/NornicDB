package search

// Closeout lane L3, root-cause proof for P-N152-88 (Plane N152-88 / NOR-401).
// Mechanism: the local reranker (local_rerank.go:143-145) and the LLM reranker
// (llm_rerank.go:106-107 query, :170-171 candidate) compare len(text) (bytes)
// with an allowance documented in characters and cut text[:allowance] (a byte
// offset). Proof:
//  (a) diagnostic: for every failing input, the text the scorer / prompt
//      receives is exactly the byte slice strings.TrimSpace(input)[:allowance];
//  (b) bypass: the real Rerank path with the allowance converted to the byte
//      length of the first <allowance> characters delivers exactly those
//      characters, valid UTF-8; so the unit of the cut is the only wrong step.
// Test-only.

import (
	"context"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func closeoutL3RCByteCut(input string, n int) string {
	s := strings.TrimSpace(input)
	if len(s) > n {
		s = s[:n]
	}
	return s
}

// closeoutL3RCLocal runs the real local reranker and returns what the scorer saw.
func closeoutL3RCLocal(t *testing.T, input string, allowance int) string {
	t.Helper()
	scorer := &closeoutL3CaptureScorer{}
	cfg := &LocalRerankerConfig{Enabled: true, Timeout: 10 * time.Second, MaxCandidates: 10, MaxDocChars: allowance}
	r := NewLocalReranker(scorer, cfg, closeoutL3Admit)
	if _, err := r.Rerank(context.Background(), "q", []RerankCandidate{{ID: "n1", Content: input, Score: 1}}); err != nil {
		t.Fatalf("PRECONDITION: local Rerank: %v", err)
	}
	if len(scorer.docs) != 1 {
		t.Fatalf("PRECONDITION: scorer saw %d documents, want 1", len(scorer.docs))
	}
	return scorer.docs[0]
}

// closeoutL3RCLLM runs the real LLM reranker and returns the candidate and
// query text placed in the prompt.
func closeoutL3RCLLM(t *testing.T, input string, allowance int) (doc, query string) {
	t.Helper()
	var prompt string
	llm := func(_ context.Context, p string) (string, error) {
		prompt = p
		return `{"ranked":[{"index":0,"score":0.5}]}`, nil
	}
	cfg := &LLMRerankerConfig{Enabled: true, Timeout: 10 * time.Second, MaxCandidates: 10, MaxDocChars: allowance, MaxQueryChars: allowance}
	if _, err := NewLLMReranker(cfg, llm).Rerank(context.Background(), input, []RerankCandidate{{ID: "n1", Content: input, Score: 1}}); err != nil {
		t.Fatalf("PRECONDITION: LLM Rerank: %v", err)
	}
	return closeoutL3PromptDoc(t, prompt), closeoutL3PromptQuery(t, prompt)
}

func TestCloseoutL3_RC_N152_88_SeamReceivesByteSlice(t *testing.T) {
	_, cases := closeoutL3Cases()
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			local := closeoutL3RCLocal(t, c.input, c.allowance)
			doc, query := closeoutL3RCLLM(t, c.input, c.allowance)
			cut := closeoutL3RCByteCut(c.input, c.allowance)
			t.Logf("CLOSEOUT_L3_RC_RERANK diag %s allowance=%d inputBytes=%d inputRunes=%d byteCut=%q local=%q llmDoc=%q llmQuery=%q",
				c.name, c.allowance, len(c.input), utf8.RuneCountInString(c.input), cut, local, doc, query)
			for label, got := range map[string]string{"local": local, "llm/doc": doc, "llm/query": query} {
				if got != cut {
					t.Errorf("MECHANISM: %s input %q is not the byte cut %q", label, got, cut)
				}
			}
			if utf8.ValidString(cut) && cut == closeoutL3Prefix(c.input, c.allowance) {
				t.Errorf("PRECONDITION: case %s does not separate bytes from characters", c.name)
			}
		})
	}
}

func TestCloseoutL3_RC_N152_88_ByteAllowanceBypass(t *testing.T) {
	_, cases := closeoutL3Cases()
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			want := closeoutL3Prefix(c.input, c.allowance)
			byteAllowance := len(want)
			local := closeoutL3RCLocal(t, c.input, byteAllowance)
			doc, query := closeoutL3RCLLM(t, c.input, byteAllowance)
			t.Logf("CLOSEOUT_L3_RC_RERANK bypass %s chars=%d bytes=%d local=%q llmDoc=%q llmQuery=%q want=%q",
				c.name, c.allowance, byteAllowance, local, doc, query, want)
			for label, got := range map[string]string{"local": local, "llm/doc": doc, "llm/query": query} {
				if got != want || !utf8.ValidString(got) {
					t.Errorf("BYPASS: %s with a byte allowance of %d got %q, want %q", label, byteAllowance, got, want)
				}
			}
		})
	}
}

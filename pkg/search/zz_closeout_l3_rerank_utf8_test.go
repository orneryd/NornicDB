package search

// Closeout lane L3, item P-N152-88 (Plane N152-88 / NOR-401): the local and
// LLM rerankers cut candidate text (and the LLM query) by byte offset while
// their allowances are documented as characters. Expected: the text the model
// receives is valid UTF-8 and holds min(allowance, rune count) characters.
// The ASCII control must pass on any build.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

type closeoutL3CaptureScorer struct {
	mu   sync.Mutex
	docs []string
}

func (s *closeoutL3CaptureScorer) Score(_ context.Context, _ string, document string) (float32, error) {
	s.mu.Lock()
	s.docs = append(s.docs, document)
	s.mu.Unlock()
	return 0.5, nil
}

func closeoutL3Admit(ctx context.Context) (context.Context, func(), error) {
	return ctx, func() {}, nil
}

type closeoutL3Case struct {
	name      string
	input     string
	allowance int
}

func closeoutL3Prefix(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		r = r[:n]
	}
	return string(r)
}

func closeoutL3Cases() (controls, cases []closeoutL3Case) {
	controls = []closeoutL3Case{
		{name: "ascii_5", input: "abcdefghij", allowance: 5},
		{name: "ascii_1", input: "ee", allowance: 1},
	}
	cases = []closeoutL3Case{
		{name: "cyrillic_5", input: "привет мир", allowance: 5},
		{name: "cjk_5", input: "日本語の検索結果", allowance: 5},
		{name: "mixed_5", input: "aпbр日c", allowance: 5},
		{name: "e_acute_1", input: "éé", allowance: 1},
	}
	return controls, cases
}

func closeoutL3Check(t *testing.T, label, got, input string, allowance int) {
	t.Helper()
	want := closeoutL3Prefix(input, allowance)
	t.Logf("CLOSEOUT_L3_RERANK %s input=%q allowance=%d got=%q (bytes=%d runes=%d validUTF8=%v) want=%q",
		label, input, allowance, got, len(got), utf8.RuneCountInString(got), utf8.ValidString(got), want)
	if !utf8.ValidString(got) {
		t.Errorf("%s: model input is not valid UTF-8: %q", label, got)
	}
	if got != want {
		t.Errorf("%s: model input %q, want first %d characters %q", label, got, allowance, want)
	}
}

func closeoutL3RunLocal(t *testing.T, c closeoutL3Case) {
	scorer := &closeoutL3CaptureScorer{}
	cfg := &LocalRerankerConfig{Enabled: true, Timeout: 10 * time.Second, MaxCandidates: 10, MaxDocChars: c.allowance}
	r := NewLocalReranker(scorer, cfg, closeoutL3Admit)
	_, err := r.Rerank(context.Background(), "q", []RerankCandidate{{ID: "n1", Content: c.input, Score: 1}})
	if err != nil {
		t.Fatalf("Rerank: %v", err)
	}
	if len(scorer.docs) != 1 {
		t.Fatalf("scorer saw %d documents, want 1", len(scorer.docs))
	}
	closeoutL3Check(t, "local/doc/"+c.name, scorer.docs[0], c.input, c.allowance)
}

// closeoutL3PromptDoc extracts the single candidate body from the prompt
// built by LLMReranker.buildPrompt: "0) id=n1 bi_score=...\n<doc>\n---\n".
func closeoutL3PromptDoc(t *testing.T, prompt string) string {
	t.Helper()
	marker := "0) id=n1 bi_score="
	i := strings.Index(prompt, marker)
	if i < 0 {
		t.Fatalf("candidate header not found in prompt: %q", prompt)
	}
	rest := prompt[i:]
	nl := strings.IndexByte(rest, '\n')
	if nl < 0 {
		t.Fatalf("malformed prompt: %q", prompt)
	}
	rest = rest[nl+1:]
	end := strings.Index(rest, "\n---\n")
	if end < 0 {
		t.Fatalf("candidate terminator not found: %q", prompt)
	}
	return rest[:end]
}

func closeoutL3PromptQuery(t *testing.T, prompt string) string {
	t.Helper()
	start := strings.Index(prompt, "QUERY:\n")
	end := strings.Index(prompt, "\n\nCANDIDATES:\n")
	if start < 0 || end < 0 || end < start {
		t.Fatalf("query section not found: %q", prompt)
	}
	return prompt[start+len("QUERY:\n") : end]
}

func closeoutL3RunLLM(t *testing.T, c closeoutL3Case) {
	var prompt string
	llm := func(_ context.Context, p string) (string, error) {
		prompt = p
		return `{"ranked":[{"index":0,"score":0.5}]}`, nil
	}
	cfg := &LLMRerankerConfig{Enabled: true, Timeout: 10 * time.Second, MaxCandidates: 10, MaxDocChars: c.allowance, MaxQueryChars: c.allowance}
	r := NewLLMReranker(cfg, llm)
	_, err := r.Rerank(context.Background(), c.input, []RerankCandidate{{ID: "n1", Content: c.input, Score: 1}})
	if err != nil {
		t.Fatalf("Rerank: %v", err)
	}
	closeoutL3Check(t, "llm/doc/"+c.name, closeoutL3PromptDoc(t, prompt), c.input, c.allowance)
	closeoutL3Check(t, "llm/query/"+c.name, closeoutL3PromptQuery(t, prompt), c.input, c.allowance)
}

func TestCloseoutL3_N152_88_Control(t *testing.T) {
	controls, _ := closeoutL3Cases()
	for _, c := range controls {
		c := c
		t.Run("local/"+c.name, func(t *testing.T) { closeoutL3RunLocal(t, c) })
		t.Run("llm/"+c.name, func(t *testing.T) { closeoutL3RunLLM(t, c) })
	}
}

func TestCloseoutL3_N152_88_LocalRerankerCharacterAllowance(t *testing.T) {
	_, cases := closeoutL3Cases()
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) { closeoutL3RunLocal(t, c) })
	}
}

func TestCloseoutL3_N152_88_LLMRerankerCharacterAllowance(t *testing.T) {
	_, cases := closeoutL3Cases()
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) { closeoutL3RunLLM(t, c) })
	}
}

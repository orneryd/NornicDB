package search

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/voyage"
)

func TestReviewerEnvelopeBoundary(t *testing.T) {
	observations := 0
	for _, provider := range reviewerBoundaryProviders() {
		for _, testCase := range reviewerEnvelopeCases(provider) {
			t.Run(provider.name+"/"+testCase.name, func(t *testing.T) {
				t.Run("explicit", func(t *testing.T) {
					assertReviewerEnvelope(t, provider, testCase, false)
				})
				t.Run("node_search", func(t *testing.T) {
					assertReviewerEnvelope(t, provider, testCase, true)
				})
			})
			observations += 2
		}
	}
	if observations != 142 {
		t.Fatalf("envelope public-path observation population changed: got %d want 142", observations)
	}
}

func TestReviewerEveryLLMResponseUsesEnvelopeGrammar(t *testing.T) {
	provider := reviewerBoundaryProvider{name: "llm"}
	observations := 0
	for _, body := range []string{"2 0", "2 0 null", "2 0 true", "2 0]", "2 0,", `2 0 "tail"`, "2 0 1e"} {
		t.Run(fmt.Sprintf("%q", body), func(t *testing.T) {
			testCase := reviewerEnvelopeCase{name: body, body: body, valid: false}
			t.Run("explicit", func(t *testing.T) {
				assertReviewerEnvelope(t, provider, testCase, false)
			})
			t.Run("node_search", func(t *testing.T) {
				assertReviewerEnvelope(t, provider, testCase, true)
			})
		})
		observations += 2
	}
	if observations != 14 {
		t.Fatalf("LLM non-envelope public-path observation population changed: got %d want 14", observations)
	}
}

type reviewerBoundaryProvider struct {
	name             string
	value            string
	duplicate        string
	escapedDuplicate string
}

type reviewerEnvelopeCase struct {
	name  string
	body  string
	valid bool
}

func reviewerBoundaryProviders() []reviewerBoundaryProvider {
	return []reviewerBoundaryProvider{
		{
			name:             "llm",
			value:            `{"order":[2,0],"scores":[0.9,0.8]}`,
			duplicate:        `{"order":[0,2],"order":[2,0],"scores":[0.9,0.8]}`,
			escapedDuplicate: `{"order":[0,2],"ord\u0065r":[2,0],"scores":[0.9,0.8]}`,
		},
		{
			name:             "cross_encoder",
			value:            `{"results":[{"index":2,"relevance_score":0.9},{"index":0,"relevance_score":0.8}]}`,
			duplicate:        `{"results":[{"index":0,"relevance_score":0.8}],"results":[{"index":2,"relevance_score":0.9},{"index":0,"relevance_score":0.8}]}`,
			escapedDuplicate: `{"results":[{"index":0,"relevance_score":0.8}],"res\u0075lts":[{"index":2,"relevance_score":0.9},{"index":0,"relevance_score":0.8}]}`,
		},
		{
			name:             "voyage",
			value:            `{"data":[{"index":2,"relevance_score":0.9},{"index":0,"relevance_score":0.8}]}`,
			duplicate:        `{"data":[{"index":0,"relevance_score":0.8}],"data":[{"index":2,"relevance_score":0.9},{"index":0,"relevance_score":0.8}]}`,
			escapedDuplicate: `{"data":[{"index":0,"relevance_score":0.8}],"da\u0074a":[{"index":2,"relevance_score":0.9},{"index":0,"relevance_score":0.8}]}`,
		},
	}
}

func reviewerEnvelopeCases(provider reviewerBoundaryProvider) []reviewerEnvelopeCase {
	fence := func(value string) string { return "```json\n" + value + "\n```" }
	withReason := strings.TrimSuffix(provider.value, "}") + `,"reason":"nested ` + "```json ```" + `"}`
	cases := []reviewerEnvelopeCase{
		{"one_fence_prose", fence(provider.value) + "\nCandidates ranked.", true},
		{"identical_fences", fence(provider.value) + "\n" + fence(provider.value), false},
		{"object", provider.value, true},
		{"nested_backticks", fence(withReason), true},
		{"duplicate_key", provider.duplicate, false},
		{"duplicate_escaped_key", provider.escapedDuplicate, false},
		{"empty_fence", fence(""), false},
		{"whitespace", " \n\t" + provider.value + "\r\n ", true},
		{"BOM_prefix", "\ufeff" + provider.value, true},
		{"BOM_suffix", provider.value + "\ufeff", false},
		{"bare_second_null", provider.value + " null", false},
		{"bare_second_true", provider.value + " true", false},
		{"bare_second_number", provider.value + " 42", false},
		{"bare_second_string", provider.value + ` "ignored"`, false},
		{"unmatched_close", provider.value + "}", false},
		{"trailing_comma", provider.value + ",", false},
		{"fence_second_scalar", fence(provider.value) + "\n```json\nnull\n```", false},
		{"fence_second_empty", fence(provider.value) + "\n" + fence(""), false},
		{"fence_truncated_scalar", fence(provider.value) + "\n```json\ntru", false},
		{"fence_truncated_string", fence(provider.value) + ` "`, false},
		{"fence_truncated_number", fence(provider.value) + ` -`, false},
		{"fence_truncated_exponent", fence(provider.value) + ` 1e`, false},
		{"fence_trailing_comma", fence(provider.value) + ` ,`, false},
	}
	if provider.name == "llm" {
		cases = append(cases,
			reviewerEnvelopeCase{"array", "[2,0]", true},
			reviewerEnvelopeCase{"array_unmatched_close", "[2,0]]", false},
		)
	}
	return cases
}

// assertReviewerEnvelope owns the shared service lifecycle and exercises each
// public contract: explicit candidates reject invalid envelopes, while node
// search returns its original ranking and reports the rerank failure.
func assertReviewerEnvelope(t *testing.T, provider reviewerBoundaryProvider, testCase reviewerEnvelopeCase, nodeSearch bool) {
	t.Helper()
	var engine storage.Engine
	dimensions := DefaultVectorDimensions
	if nodeSearch {
		engine = storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "nornic")
		dimensions = 2
	}
	service := NewService(engine, dimensions, nil)
	t.Cleanup(func() { _ = service.Close() })
	options := DefaultSearchOptions()
	options.RerankTopK = 3
	if !nodeSearch {
		service.SetReranker(newReviewerBoundaryReranker(t, provider.name, testCase.body))
		rows, err := service.RerankCandidates(context.Background(), "q", []RerankCandidate{
			{ID: "a", Content: "alpha", Score: 0.03},
			{ID: "b", Content: "beta", Score: 0.02},
			{ID: "c", Content: "gamma", Score: 0.01},
		}, options)
		if testCase.valid {
			if err != nil {
				t.Fatal(err)
			}
			assertReviewerResultIDs(t, rows, []string{"c", "a", "b"})
		} else if err == nil || rows != nil {
			t.Fatalf("malformed/ambiguous response returned partial success: rows=%+v err=%v", rows, err)
		}
		return
	}
	fixtures := []struct {
		content   string
		embedding []float32
	}{
		{content: "query query query alpha", embedding: []float32{1, 0}},
		{content: "query query beta", embedding: []float32{0.9, 0.1}},
		{content: "query gamma", embedding: []float32{0.8, 0.2}},
	}
	for index, fixture := range fixtures {
		node := &storage.Node{
			ID: storage.NodeID(string(rune('a' + index))), Labels: []string{"Doc"},
			Properties: map[string]any{"content": fixture.content}, ChunkEmbeddings: [][]float32{fixture.embedding},
		}
		if _, err := engine.CreateNode(node); err != nil {
			t.Fatal(err)
		}
		if err := service.IndexNode(node); err != nil {
			t.Fatal(err)
		}
	}
	options.Limit = 3
	options.MinRRFScore = 0
	baseline, err := service.Search(context.Background(), "query", []float32{1, 0}, options)
	if err != nil {
		t.Fatal(err)
	}
	if len(baseline.Results) != 3 {
		t.Fatalf("baseline candidate population = %d want 3", len(baseline.Results))
	}
	baselineIDs := reviewerSearchResultIDs(baseline.Results)
	service.SetReranker(newReviewerBoundaryReranker(t, provider.name, testCase.body))
	options.RerankEnabled = true
	response, err := service.Search(context.Background(), "query", []float32{1, 0}, options)
	if err != nil {
		t.Fatal(err)
	}
	if testCase.valid {
		if !response.rerankApplied || response.FallbackTriggered || !strings.HasSuffix(response.SearchMethod, "+rerank") {
			t.Fatalf("valid response did not report rerank: %+v", response)
		}
		want := []string{baselineIDs[2], baselineIDs[0], baselineIDs[1]}
		assertReviewerSearchResultIDs(t, response.Results, want)
		return
	}
	if response.rerankApplied || !response.FallbackTriggered || response.FallbackReason != SearchFallbackRerankFailed || strings.HasSuffix(response.SearchMethod, "+rerank") {
		t.Fatalf("invalid response did not report fallback: %+v", response)
	}
	assertReviewerSearchResultIDs(t, response.Results, baselineIDs)
}

func newReviewerBoundaryReranker(t *testing.T, provider, body string) Reranker {
	t.Helper()
	if provider == "llm" {
		return NewLLMReranker(&LLMRerankerConfig{
			Enabled: true, Timeout: time.Second, MaxCandidates: 3,
		}, func(context.Context, string) (string, error) { return body, nil })
	}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(response, body)
	}))
	t.Cleanup(server.Close)
	if provider == "cross_encoder" {
		return NewCrossEncoder(&CrossEncoderConfig{Enabled: true, APIURL: server.URL, TopK: 3, Timeout: time.Second})
	}
	reranker, err := voyage.NewReranker(&voyage.RerankerConfig{
		Enabled: true, APIURL: server.URL, APIKey: "review-fixture", TopK: 3, Timeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return reranker
}

func reviewerSearchResultIDs(results []SearchResult) []string {
	ids := make([]string, len(results))
	for index := range results {
		ids[index] = results[index].ID
	}
	return ids
}

func assertReviewerResultIDs(t *testing.T, results []RerankResult, want []string) {
	t.Helper()
	if len(results) != len(want) {
		t.Fatalf("result count = %d want %d: %+v", len(results), len(want), results)
	}
	for index := range want {
		if results[index].ID != want[index] {
			t.Fatalf("result %d = %q want %q: %+v", index, results[index].ID, want[index], results)
		}
	}
}

func assertReviewerSearchResultIDs(t *testing.T, results []SearchResult, want []string) {
	t.Helper()
	got := reviewerSearchResultIDs(results)
	if len(got) != len(want) {
		t.Fatalf("result count = %d want %d: %v", len(got), len(want), got)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("result %d = %q want %q: got=%v want=%v", index, got[index], want[index], got, want)
		}
	}
}

func TestReviewerAliasedFieldsBoundary(t *testing.T) {
	observations := 0
	for _, testCase := range []struct {
		provider string
		name     string
		body     string
	}{
		{"llm", "ranking_fields", `{"order":[-1],"ORDER":[2,0],"scores":[0.9,0.8]}`},
		{"llm", "index_fields", `{"ranked":[{"index":-1,"INDEX":2,"score":0.9},{"index":0,"score":0.8}]}`},
		{"llm", "score_fields", `{"ranked":[{"index":2,"score":0.1,"SCORE":0.9},{"index":0,"score":0.8}]}`},
		{"cross_encoder", "ranking_fields", `{"results":[{"index":-1,"relevance_score":0.1}],"RESULTS":[{"index":2,"relevance_score":0.9},{"index":0,"relevance_score":0.8}]}`},
		{"cross_encoder", "index_fields", `{"results":[{"index":-1,"INDEX":2,"relevance_score":0.9},{"index":0,"relevance_score":0.8}]}`},
		{"voyage", "ranking_fields", `{"data":[{"index":-1,"relevance_score":0.1}],"DATA":[{"index":2,"relevance_score":0.9},{"index":0,"relevance_score":0.8}]}`},
		{"voyage", "index_fields", `{"data":[{"index":-1,"INDEX":2,"relevance_score":0.9},{"index":0,"relevance_score":0.8}]}`},
	} {
		t.Run(testCase.provider+"/"+testCase.name, func(t *testing.T) {
			var reranker Reranker
			if testCase.provider == "llm" {
				reranker = NewLLMReranker(&LLMRerankerConfig{
					Enabled: true, Timeout: time.Second, MaxCandidates: 3,
				}, func(context.Context, string) (string, error) { return testCase.body, nil })
			} else {
				server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
					_, _ = fmt.Fprint(response, testCase.body)
				}))
				t.Cleanup(server.Close)
				if testCase.provider == "cross_encoder" {
					reranker = NewCrossEncoder(&CrossEncoderConfig{Enabled: true, APIURL: server.URL, TopK: 3, Timeout: time.Second})
				} else {
					var err error
					reranker, err = voyage.NewReranker(&voyage.RerankerConfig{
						Enabled: true, APIURL: server.URL, APIKey: "review-fixture", TopK: 3, Timeout: time.Second,
					})
					if err != nil {
						t.Fatal(err)
					}
				}
			}

			engine := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "nornic")
			service := NewService(engine, 2, nil)
			t.Cleanup(func() { _ = service.Close() })
			service.SetReranker(reranker)
			options := DefaultSearchOptions()
			options.Limit = 3
			options.RerankTopK = 3
			options.RerankEnabled = true
			options.MinRRFScore = 0
			rows, err := service.RerankCandidates(context.Background(), "query", []RerankCandidate{
				{ID: "a", Content: "alpha", Score: 0.03},
				{ID: "b", Content: "beta", Score: 0.02},
				{ID: "c", Content: "gamma", Score: 0.01},
			}, options)
			if err == nil || rows != nil {
				t.Errorf("conflicting fields were accepted: rows=%+v err=%v", rows, err)
			}
			for index, word := range []string{"alpha", "beta", "gamma"} {
				node := &storage.Node{
					ID: storage.NodeID(string(rune('a' + index))), Labels: []string{"Doc"},
					Properties: map[string]any{"content": word + " query"}, ChunkEmbeddings: [][]float32{{1, 0}},
				}
				if _, err := engine.CreateNode(node); err != nil {
					t.Fatal(err)
				}
				if err := service.IndexNode(node); err != nil {
					t.Fatal(err)
				}
			}
			response, err := service.Search(context.Background(), "query", []float32{1, 0}, options)
			if err != nil {
				t.Fatal(err)
			}
			if response.rerankApplied || !response.FallbackTriggered || response.FallbackReason != SearchFallbackRerankFailed || strings.HasSuffix(response.SearchMethod, "+rerank") {
				t.Errorf("ambiguous output reported applied: %+v", response)
			}
		})
		observations += 2
	}
	if observations != 14 {
		t.Fatalf("aliased-field public-path observation population changed: got %d want 14", observations)
	}
}

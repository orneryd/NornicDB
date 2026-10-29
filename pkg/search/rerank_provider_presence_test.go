package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/voyage"
)

type providerShapeScorer struct{ shape string }

func (s providerShapeScorer) Score(_ context.Context, _, document string) (float32, error) {
	switch s.shape {
	case "absent", "null", "wrong_type":
		return 0, errors.New("provider value unavailable")
	case "nan_or_inf":
		return float32(math.NaN()), nil
	case "negative":
		if document == "alpha" {
			return -0.9, nil
		}
	case "valid_zero_score":
		if document == "alpha" {
			return 0, nil
		}
	case "valid_zero_index":
		if document == "alpha" {
			return 0.8, nil
		}
	}
	if document == "gamma" {
		return 0.9, nil
	}
	return 0.3, nil
}

type providerRepresentation struct {
	name         string
	providerType string
}

type providerShapeCase struct {
	shape          string
	wantApplied    bool
	wantIDs        []string
	publicRejected bool
}

type providerListedPackage struct {
	ImportPath string
	Dir        string
	GoFiles    []string
	CgoFiles   []string
	ImportMap  map[string]string
}

type providerSourceImporter struct {
	fset   *token.FileSet
	listed map[string]*providerListedPackage
	loaded map[string]*types.Package
}

func (i *providerSourceImporter) Import(path string) (*types.Package, error) {
	if path == "unsafe" {
		return types.Unsafe, nil
	}
	if loaded := i.loaded[path]; loaded != nil {
		return loaded, nil
	}
	listed := i.listed[path]
	if listed == nil {
		return nil, fmt.Errorf("%s is absent from go list -deps", path)
	}
	files, err := parseProviderPackageFiles(i.fset, listed)
	if err != nil {
		return nil, err
	}
	configuration := types.Config{IgnoreFuncBodies: true, FakeImportC: true, Importer: i.importerFor(listed)}
	loaded, err := configuration.Check(path, i.fset, files, nil)
	if loaded != nil {
		i.loaded[path] = loaded
	}
	return loaded, err
}

func (i *providerSourceImporter) importerFor(listed *providerListedPackage) types.Importer {
	return providerImportFunc(func(path string) (*types.Package, error) {
		if mapped, ok := listed.ImportMap[path]; ok {
			path = mapped
		}
		return i.Import(path)
	})
}

type providerImportFunc func(path string) (*types.Package, error)

func (f providerImportFunc) Import(path string) (*types.Package, error) { return f(path) }

func parseProviderPackageFiles(fset *token.FileSet, listed *providerListedPackage) ([]*ast.File, error) {
	files := make([]*ast.File, 0, len(listed.GoFiles)+len(listed.CgoFiles))
	for _, name := range append(append([]string{}, listed.GoFiles...), listed.CgoFiles...) {
		file, err := parser.ParseFile(fset, filepath.Join(listed.Dir, name), nil, 0)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	return files, nil
}

// TestRerankProviderOutputMatrix is the single canonical case table for the provider-representation x malformed-shape population of the rerank contract. Every discovered production reranker type must appear both in the executable representation population and in this table, every representation must carry the full shape set its provider can produce, and each shape declares the outcome its owner enforces:
//
// Concretely:
//
//   - the internal runner must apply accepted shapes in the documented order with no duplicate results, and must return the original order, log a rerank failure and report SearchFallbackRerankFailed for every shape it rejects;
//   - the public RerankCandidates owner must reject the structured-ambiguity shapes with an error and no results, must surface the canonical rerank failure for every shape the internal runner rejects, and otherwise returns the same order the internal runner produced.
//
// Sequencing is derived per shape from the shape token plus the internal
// verdict, so one case cannot disagree with itself across the two owners.
func TestRerankProviderOutputMatrix(t *testing.T) {
	providerTypes := discoverProductionRerankerTypes(t)
	representations := executableProviderRepresentations()
	if len(representations) != 9 {
		t.Fatalf("provider representation population changed: got %d want 9", len(representations))
	}
	executableTypes := make(map[string]int)
	for _, representation := range representations {
		executableTypes[representation.providerType]++
	}
	if strings.Join(providerTypes, "\n") != strings.Join(sortedProviderTypeKeys(executableTypes), "\n") {
		t.Fatalf("type-derived reranker population has no exact executable malformed-input mapping\n--- go/types ---\n%s\n--- executable ---\n%s", strings.Join(providerTypes, "\n"), strings.Join(sortedProviderTypeKeys(executableTypes), "\n"))
	}
	candidates := []RerankCandidate{
		{ID: "a", Content: "alpha", Score: 0.03},
		{ID: "b", Content: "beta", Score: 0.02},
		{ID: "c", Content: "gamma", Score: 0.01},
	}
	invalidOutputRejections := make(map[string]int)
	// structuredPublicRejections counts only the representation/shape pairs the
	// public owner refuses although the internal runner can still fall back.
	structuredPublicRejections := make(map[string]int)
	publicRejections := make(map[string]int)
	executedShapeObservations := 0
	for _, representation := range representations {
		cases := executableProviderShapeCases(representation.name)
		if len(cases) == 0 {
			t.Fatalf("provider representation %s has no executable shape cases", representation.name)
		}
		for _, testCase := range cases {
			// A provider failure is itself a public rejection: explicit callers
			// receive the canonical rerank error, never a fallback order.
			if !testCase.wantApplied {
				invalidOutputRejections[representation.providerType]++
			}
			if testCase.publicRejected && !testCase.wantApplied {
				publicRejections[representation.providerType]++
				structuredPublicRejections[representation.name+"/"+testCase.shape]++
			}
			executedShapeObservations++
			wantIDs := testCase.wantIDs
			if !testCase.wantApplied {
				wantIDs = []string{"a", "b", "c"}
			}

			t.Run(representation.name+"/"+testCase.shape+"/internal_runner", func(t *testing.T) {
				reranker := makeProviderShapeReranker(t, representation, testCase.shape)
				service := NewService(newNamespacedEngine(t), DefaultVectorDimensions, nil)
				t.Cleanup(func() { _ = service.Close() })
				var logs bytes.Buffer
				service.SetLogger(slog.New(slog.NewTextHandler(&logs, nil)))
				outcome := service.runRerank(context.Background(), reranker, "query", candidates, rerankRunOptions{providerLimit: 3})
				if testCase.wantApplied {
					if outcome.Method != rerankMethodApplied || outcome.FallbackTriggered {
						t.Fatalf("valid provider response rejected: method=%q fallback=%v reason=%q", outcome.Method, outcome.FallbackTriggered, outcome.FallbackReason)
					}
				} else {
					if outcome.Method != rerankMethodOriginal || !outcome.FallbackTriggered || outcome.FallbackReason != SearchFallbackRerankFailed {
						t.Fatalf("invalid provider response accepted: method=%q fallback=%v reason=%q", outcome.Method, outcome.FallbackTriggered, outcome.FallbackReason)
					}
					if !strings.Contains(logs.String(), "Reranking failed") {
						t.Fatalf("invalid provider response was not logged: %q", logs.String())
					}
				}
				if len(outcome.Results) != len(wantIDs) {
					t.Fatalf("complete result length = %d want %d", len(outcome.Results), len(wantIDs))
				}
				seen := make(map[string]bool, len(outcome.Results))
				for index, result := range outcome.Results {
					if result.ID != wantIDs[index] {
						t.Fatalf("result %d = %q want %q", index, result.ID, wantIDs[index])
					}
					if seen[result.ID] {
						t.Fatalf("duplicate result %q", result.ID)
					}
					seen[result.ID] = true
				}
			})

			t.Run(representation.name+"/"+testCase.shape+"/public_candidates", func(t *testing.T) {
				reranker := makeProviderShapeReranker(t, representation, testCase.shape)
				service := NewService(newNamespacedEngine(t), DefaultVectorDimensions, nil)
				t.Cleanup(func() { _ = service.Close() })
				service.SetReranker(reranker)
				options := DefaultSearchOptions()
				options.RerankTopK = len(candidates)
				results, err := service.RerankCandidates(context.Background(), "query", candidates, options)
				if !testCase.wantApplied {
					// Both owners read one rerankOutcome: a provider failure is
					// reported as the canonical rerank error, never as a silent
					// fallback order for an explicit caller.
					if err == nil {
						t.Fatalf("failed provider output reported success: results=%+v", results)
					}
					if results != nil {
						t.Fatalf("failed explicit rerank returned results: %+v", results)
					}
					return
				}
				if !testCase.publicRejected {
					if err != nil {
						t.Fatalf("accepted provider output reported failure: %v", err)
					}
				}
				// The public owner returns the internal owner's order: the
				// provider's for applied shapes, the original for fallbacks.
				if len(results) != len(wantIDs) {
					t.Fatalf("public result length = %d want %d", len(results), len(wantIDs))
				}
				for index, result := range results {
					if result.ID != wantIDs[index] {
						t.Fatalf("public result %d = %q want %q", index, result.ID, wantIDs[index])
					}
				}
			})
		}
	}
	if strings.Join(providerTypes, "\n") != strings.Join(sortedProviderTypeKeys(publicRejections), "\n") {
		t.Fatalf("type-derived reranker population has no exact public invalid-output case\n--- go/types ---\n%s\n--- executable ---\n%s", strings.Join(providerTypes, "\n"), strings.Join(sortedProviderTypeKeys(publicRejections), "\n"))
	}
	invalidOutputObservations := 0
	for _, count := range invalidOutputRejections {
		invalidOutputObservations += count
	}
	publicObservations := 0
	for _, count := range publicRejections {
		publicObservations += count
	}
	if invalidOutputObservations != 83 {
		t.Fatalf("public invalid-output population changed: got %d want 83", invalidOutputObservations)
	}
	ambiguousPublicObservations := 0
	for _, count := range structuredPublicRejections {
		ambiguousPublicObservations += count
	}
	if ambiguousPublicObservations != 29 {
		t.Fatalf("public ambiguous-output population changed: got %d want 29 from %d rejections", ambiguousPublicObservations, publicObservations)
	}
	if executedShapeObservations != 108 {
		t.Fatalf("provider representation/shape executable population changed: got %d want 108", executedShapeObservations)
	}
	t.Logf("executed %d discovered provider types, %d representations, %d internal runner shape cases, %d public invalid-output cases", len(providerTypes), len(representations), executedShapeObservations, publicObservations)
}

func executableProviderShapeCases(name string) []providerShapeCase {
	failures := func(shapes ...string) []providerShapeCase {
		cases := make([]providerShapeCase, len(shapes))
		for index, shape := range shapes {
			cases[index] = providerShapeCase{shape: shape}
			switch shape {
			case "bare_second_object", "truncated_structured_suffix", "two_json_fences", "fence_then_second_object", "conflicting_representations", "duplicate_json_field":
				cases[index].publicRejected = true
			}
		}
		return cases
	}
	success := func(shape string, ids ...string) providerShapeCase {
		return providerShapeCase{shape: shape, wantApplied: true, wantIDs: ids}
	}
	indexed := func() []providerShapeCase {
		return append(failures("absent", "null", "wrong_type", "nan_or_inf", "negative", "out_of_range", "duplicate"),
			success("valid_zero_index", "a", "c", "b"), success("valid_zero_score", "c", "a", "b"))
	}
	structuredSuffixFailures := failures("bare_second_object", "truncated_structured_suffix")
	llmEnvelopeFailures := failures("two_json_fences", "fence_then_second_object")
	switch name {
	case "cross_encoder.results", "voyage.data":
		cases := append(append(indexed(), structuredSuffixFailures...), failures("conflicting_representations")...)
		return append(cases, failures("duplicate_json_field")...)
	case "cross_encoder.rankings", "voyage.results":
		return append(indexed(), structuredSuffixFailures...)
	case "llm.ranked":
		cases := append(indexed(), success("fenced_json", "c", "a", "b"), success("trailing_prose", "c", "a", "b"))
		cases = append(cases, structuredSuffixFailures...)
		cases = append(cases, llmEnvelopeFailures...)
		cases = append(cases, failures("conflicting_representations")...)
		return append(cases, failures("duplicate_json_field")...)
	case "llm.order":
		cases := append(indexed(), success("fenced_json", "c", "a", "b"), success("trailing_prose", "c", "a", "b"))
		cases = append(cases, structuredSuffixFailures...)
		return append(cases, llmEnvelopeFailures...)
	case "llm.array":
		cases := append(failures("absent", "null", "wrong_type", "nan_or_inf", "negative", "out_of_range", "duplicate"),
			success("valid_zero_index", "a", "c", "b"), success("fenced_json", "c", "a", "b"), success("trailing_prose", "c", "a", "b"))
		cases = append(cases, structuredSuffixFailures...)
		return append(cases, llmEnvelopeFailures...)
	case "cross_encoder.scores":
		cases := append(failures("absent", "null", "wrong_type", "nan_or_inf", "out_of_range"),
			success("negative", "b", "c"), success("valid_zero_index", "a", "b", "c"), success("valid_zero_score", "c", "b", "a"))
		return append(cases, structuredSuffixFailures...)
	case "local.scorer":
		return []providerShapeCase{
			{shape: "nan_or_inf", publicRejected: true}, success("negative", "c", "b", "a"),
			success("valid_zero_index", "c", "a", "b"), success("valid_zero_score", "c", "b", "a"),
		}
	default:
		return nil
	}
}

func executableProviderRepresentations() []providerRepresentation {
	const searchPackage = "github.com/orneryd/nornicdb/pkg/search."
	const voyagePackage = "github.com/orneryd/nornicdb/pkg/voyage."
	return []providerRepresentation{
		{name: "cross_encoder.results", providerType: searchPackage + "CrossEncoder"},
		{name: "cross_encoder.rankings", providerType: searchPackage + "CrossEncoder"},
		{name: "cross_encoder.scores", providerType: searchPackage + "CrossEncoder"},
		{name: "llm.ranked", providerType: searchPackage + "LLMReranker"},
		{name: "llm.order", providerType: searchPackage + "LLMReranker"},
		{name: "llm.array", providerType: searchPackage + "LLMReranker"},
		{name: "local.scorer", providerType: searchPackage + "LocalReranker"},
		{name: "voyage.data", providerType: voyagePackage + "Reranker"},
		{name: "voyage.results", providerType: voyagePackage + "Reranker"},
	}
}
func sortedProviderTypeKeys(values map[string]int) []string {
	keys := make([]string, 0, len(values))
	for key, count := range values {
		if count > 0 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func discoverProductionRerankerTypes(t *testing.T) []string {
	t.Helper()
	moduleRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "list", "-deps", "-json=ImportPath,Dir,GoFiles,CgoFiles,ImportMap", "./...")
	command.Dir = moduleRoot
	listing, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	imp := &providerSourceImporter{fset: token.NewFileSet(), listed: map[string]*providerListedPackage{}, loaded: map[string]*types.Package{}}
	decoder := json.NewDecoder(bytes.NewReader(listing))
	for decoder.More() {
		listed := &providerListedPackage{}
		if err := decoder.Decode(listed); err != nil {
			t.Fatal(err)
		}
		imp.listed[listed.ImportPath] = listed
	}
	searchPackage, err := imp.Import("github.com/orneryd/nornicdb/pkg/search")
	if err != nil {
		t.Fatal(err)
	}
	interfaceObject := searchPackage.Scope().Lookup("Reranker")
	if interfaceObject == nil {
		t.Fatal("search.Reranker interface not found")
	}
	contract, ok := interfaceObject.Type().Underlying().(*types.Interface)
	if !ok {
		t.Fatal("search.Reranker is not an interface")
	}
	contract.Complete()
	modulePaths := make([]string, 0)
	for path, listed := range imp.listed {
		if strings.HasPrefix(path, "github.com/orneryd/nornicdb/") && len(listed.GoFiles)+len(listed.CgoFiles) > 0 {
			modulePaths = append(modulePaths, path)
		}
	}
	sort.Strings(modulePaths)
	var discovered []string
	for _, path := range modulePaths {
		typedPackage, err := imp.Import(path)
		if err != nil {
			t.Fatalf("type-check %s: %v", path, err)
		}
		scope := typedPackage.Scope()
		for _, name := range scope.Names() {
			object := scope.Lookup(name)
			named, ok := object.Type().(*types.Named)
			if !ok {
				continue
			}
			if _, isInterface := named.Underlying().(*types.Interface); isInterface {
				continue
			}
			if types.Implements(named, contract) || types.Implements(types.NewPointer(named), contract) {
				discovered = append(discovered, path+"."+name)
			}
		}
	}
	sort.Strings(discovered)
	return discovered
}

func makeProviderShapeReranker(t *testing.T, representation providerRepresentation, shape string) Reranker {
	t.Helper()
	if representation.name == "local.scorer" {
		cfg := DefaultLocalRerankerConfig()
		return NewLocalReranker(providerShapeScorer{shape: shape}, cfg)
	}
	body := providerShapeBody(representation, shape)
	if strings.HasPrefix(representation.name, "llm.") {
		return NewLLMReranker(&LLMRerankerConfig{Enabled: true, Timeout: time.Second, MaxCandidates: 3}, func(context.Context, string) (string, error) {
			return body, nil
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		fmt.Fprint(response, body)
	}))
	t.Cleanup(server.Close)
	if strings.HasPrefix(representation.name, "voyage.") {
		reranker, err := voyage.NewReranker(&voyage.RerankerConfig{Enabled: true, APIURL: server.URL, APIKey: "test", Timeout: time.Second, TopK: 3})
		if err != nil {
			t.Fatal(err)
		}
		return reranker
	}
	return NewCrossEncoder(&CrossEncoderConfig{Enabled: true, APIURL: server.URL, Timeout: time.Second, TopK: 3})
}

func providerShapeBody(representation providerRepresentation, shape string) string {
	name := representation.name
	if shape == "bare_second_object" || shape == "truncated_structured_suffix" || shape == "two_json_fences" || shape == "fence_then_second_object" {
		primary := providerShapeBody(representation, "valid_zero_index")
		switch shape {
		case "bare_second_object":
			return primary + " " + primary
		case "truncated_structured_suffix":
			return primary + ` {"order":[`
		case "two_json_fences":
			return "```json\n" + primary + "\n```\n```json\n{\"order\":[2,0]}\n```"
		case "fence_then_second_object":
			return "```json\n" + primary + "\n```\n" + `{"order":[2,0]}`
		}
	}
	if shape == "conflicting_representations" {
		switch name {
		case "cross_encoder.results":
			return `{"results":[{"index":2,"relevance_score":0.9}],"rankings":[{"index":0,"score":0.8}]}`
		case "llm.ranked":
			return `{"ranked":[{"index":2,"score":0.9}],"order":[0,2],"scores":[0.9,0.8]}`
		case "voyage.data":
			return `{"data":[{"index":2,"relevance_score":0.9}],"results":[{"index":0,"relevance_score":0.8}]}`
		}
	}
	if shape == "duplicate_json_field" {
		switch name {
		case "cross_encoder.results":
			return `{"results":[{"index":2,"relevance_score":0.9}],"results":[{"index":0,"relevance_score":0.8}]}`
		case "llm.ranked":
			return `{"ranked":[{"index":2,"score":0.9}],"ranked":[{"index":0,"score":0.8}]}`
		case "voyage.data":
			return `{"data":[{"index":2,"relevance_score":0.9}],"data":[{"index":0,"relevance_score":0.8}]}`
		}
	}
	if strings.HasPrefix(name, "llm.") && (shape == "fenced_json" || shape == "trailing_prose") {
		structured := map[string]string{
			"llm.ranked": `{"ranked":[{"index":2,"score":0.9},{"index":0,"score":0.8}]}`,
			"llm.order":  `{"order":[2,0],"scores":[0.9,0.8]}`,
			"llm.array":  `[2,0]`,
		}[name]
		if shape == "fenced_json" {
			return "```json\n" + structured + "\n```"
		}
		return "```json\n" + structured + "\n``` candidates ranked"
	}
	if name == "llm.array" {
		values := map[string]string{"absent": `[]`, "null": `[null]`, "wrong_type": `["x"]`, "nan_or_inf": `[NaN]`, "negative": `[-1]`, "out_of_range": `[99]`, "duplicate": `[0,0]`, "valid_zero_index": `[0,2]`}
		return values[shape]
	}
	if name == "llm.order" {
		values := map[string]string{"absent": `{"order":[]}`, "null": `{"order":[null]}`, "wrong_type": `{"order":["x"]}`, "nan_or_inf": `{"order":[2],"scores":[NaN]}`, "negative": `{"order":[-1]}`, "out_of_range": `{"order":[99]}`, "duplicate": `{"order":[0,0]}`, "valid_zero_index": `{"order":[0,2]}`, "valid_zero_score": `{"order":[2,0],"scores":[0.9,0]}`}
		return values[shape]
	}
	if name == "cross_encoder.scores" {
		values := map[string]string{"absent": `{"scores":[]}`, "null": `{"scores":[null]}`, "wrong_type": `{"scores":["x"]}`, "nan_or_inf": `{"scores":[Infinity]}`, "negative": `{"scores":[-0.9,0.5,0.1]}`, "out_of_range": `{"scores":[0.9,0.7,0.5,0.3]}`, "valid_zero_index": `{"scores":[0.9,0.5,0.1]}`, "valid_zero_score": `{"scores":[0,0.5,0.9]}`}
		return values[shape]
	}
	container, scoreField := "results", "relevance_score"
	switch name {
	case "cross_encoder.rankings":
		container, scoreField = "rankings", "score"
	case "voyage.data":
		container = "data"
	case "voyage.results":
		container = "results"
	case "llm.ranked":
		container, scoreField = "ranked", "score"
	}
	values := map[string]string{
		"absent":           fmt.Sprintf(`{"%s":[{}]}`, container),
		"null":             fmt.Sprintf(`{"%s":[{"index":null,"%s":null}]}`, container, scoreField),
		"wrong_type":       fmt.Sprintf(`{"%s":[{"index":"0","%s":"high"}]}`, container, scoreField),
		"nan_or_inf":       fmt.Sprintf(`{"%s":[{"index":0,"%s":NaN}]}`, container, scoreField),
		"negative":         fmt.Sprintf(`{"%s":[{"index":-1,"%s":0.9}]}`, container, scoreField),
		"out_of_range":     fmt.Sprintf(`{"%s":[{"index":99,"%s":0.9}]}`, container, scoreField),
		"duplicate":        fmt.Sprintf(`{"%s":[{"index":0,"%s":0.9},{"index":0,"%s":0.1}]}`, container, scoreField, scoreField),
		"valid_zero_index": fmt.Sprintf(`{"%s":[{"index":0,"%s":0.9},{"index":2,"%s":0.1}]}`, container, scoreField, scoreField),
		"valid_zero_score": fmt.Sprintf(`{"%s":[{"index":2,"%s":0.9},{"index":0,"%s":0}]}`, container, scoreField, scoreField),
	}
	return values[shape]
}

func TestRerankOutcomeRetainsExplicitProviderFilters(t *testing.T) {
	candidates := []RerankCandidate{
		{ID: "a", Content: "alpha", Score: 0.3},
		{ID: "b", Content: "beta", Score: 0.2},
		{ID: "c", Content: "gamma", Score: 0.1},
	}
	for _, testCase := range []struct {
		name       string
		raw        []RerankResult
		wantIDs    []string
		excludedID []string
	}{
		{
			name: "all filtered",
			raw: []RerankResult{
				{ID: "a", OriginalRank: 1, CrossScore: 0.3, FinalScore: 0.3, Filtered: true},
				{ID: "b", OriginalRank: 2, CrossScore: 0.2, FinalScore: 0.2, Filtered: true},
				{ID: "c", OriginalRank: 3, CrossScore: 0.1, FinalScore: 0.1, Filtered: true},
			},
			excludedID: []string{"a", "b", "c"},
		},
		{
			name: "filtered row plus flat survivors",
			raw: []RerankResult{
				{ID: "a", OriginalRank: 1, CrossScore: 0.1, FinalScore: 0.1, Filtered: true},
				{ID: "b", OriginalRank: 2, CrossScore: 0.8, FinalScore: 0.8},
				{ID: "c", OriginalRank: 3, CrossScore: 0.8, FinalScore: 0.8},
			},
			wantIDs:    []string{"b", "c"},
			excludedID: []string{"a"},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			outcome := completererankOutcome(candidates, testCase.raw, len(candidates), rerankRunOptions{resultLimit: len(candidates)})
			if outcome.Method != rerankMethodApplied || outcome.FallbackTriggered || outcome.FallbackReason != SearchFallbackNone {
				t.Fatalf("filter decision lost: method=%q fallback=%v reason=%q", outcome.Method, outcome.FallbackTriggered, outcome.FallbackReason)
			}
			if len(outcome.Results) != len(testCase.wantIDs) {
				t.Fatalf("result count = %d want %d", len(outcome.Results), len(testCase.wantIDs))
			}
			for index, id := range testCase.wantIDs {
				if outcome.Results[index].ID != id {
					t.Fatalf("result %d = %q want %q", index, outcome.Results[index].ID, id)
				}
			}
			if strings.Join(outcome.excludedIDs, ",") != strings.Join(testCase.excludedID, ",") {
				t.Fatalf("excluded IDs = %v want %v", outcome.excludedIDs, testCase.excludedID)
			}
		})
	}
}

func TestLLMRerankerRejectsMalformedStructuredOutputPublicOutcome(t *testing.T) {
	cases := map[string]string{
		"truncated_ranked_object":     `{"ranked":[{"index":2`,
		"embedded_truncated_object":   `ranking follows: {"order":[2`,
		"missing_closing_brackets":    `{"order":[2`,
		"cut_off_string":              `{"ranked":[{"index":2,"score":"cut`,
		"trailing_comma":              `{"order":[2,`,
		"mixed_null_structured_array": `[null,2]`,
	}
	candidates := []RerankCandidate{
		{ID: "a", Content: "alpha", Score: 0.03},
		{ID: "b", Content: "beta", Score: 0.02},
		{ID: "c", Content: "gamma", Score: 0.01},
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			service := NewService(newNamespacedEngine(t), DefaultVectorDimensions, nil)
			t.Cleanup(func() { _ = service.Close() })
			reranker := NewLLMReranker(&LLMRerankerConfig{
				Enabled:       true,
				Timeout:       time.Second,
				MaxCandidates: 3,
			}, func(context.Context, string) (string, error) {
				return body, nil
			})
			outcome := service.runRerank(context.Background(), reranker, "query", candidates, rerankRunOptions{providerLimit: 3})
			if outcome.Method != rerankMethodOriginal || !outcome.FallbackTriggered || outcome.FallbackReason != SearchFallbackRerankFailed {
				t.Fatalf("malformed structured response accepted: method=%q fallback=%v reason=%q results=%v", outcome.Method, outcome.FallbackTriggered, outcome.FallbackReason, outcome.Results)
			}
			if len(outcome.Results) != len(candidates) {
				t.Fatalf("fallback result count changed: got %d want %d", len(outcome.Results), len(candidates))
			}
			for index, result := range outcome.Results {
				if result.ID != candidates[index].ID {
					t.Fatalf("fallback order changed at %d: got %q want %q", index, result.ID, candidates[index].ID)
				}
			}
		})
	}
}

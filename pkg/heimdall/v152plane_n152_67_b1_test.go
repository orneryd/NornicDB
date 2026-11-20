package heimdall

// NOR281 public-route autocomplete reproduction.
//
// The historical probe receipt (fc79 path
// sig/handoff/r73/1486-layer-r5/HEIMDALL-AUTOCOMPLETE-SOURCE-BOUNDARY.md) is not
// present in this checkout, so these cases are reconstructed witnesses, not an
// exact replay of that run. Each case drives the public HTTP surface
// (Handler.ServeHTTP, POST /api/bifrost/autocomplete) with the real
// heimdall_autocomplete_suggest schema action registered and returning success
// with an empty suggestion, so the handler reaches the mock generator. The
// generator output is the only variable, which makes each assertion deterministic.
//
// The observed contract under test: the handler returns the model's completed
// Cypher statement, with markdown framing removed and the statement text itself
// preserved exactly. Interior quoted-identifier whitespace and escaped/internal
// backticks are part of the statement, not framing.
//
// Acceptance of the witness statements by pkg/cypher is deliberately out of
// scope here (the parent owns that check); this file asserts only the boundary
// between generated text and the suggestion returned to the client.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const nor281AutocompleteQuery = "RETURN 1 AS"

// nor281InstallAutocompleteSubsystem installs a fresh plugin/action subsystem
// that owns heimdall_autocomplete_suggest, then restores the previous global
// manager during cleanup so no other test observes this registration. Tests
// using it must not run in parallel.
func nor281InstallAutocompleteSubsystem(t *testing.T) {
	t.Helper()

	installed := &SubsystemManager{
		plugins:    make(map[string]*LoadedHeimdallPlugin),
		actions:    make(map[string]ActionFunc),
		orderDirty: true,
	}
	installed.SetContext(SubsystemHost{Config: DefaultConfig(), Bifrost: &NoOpBifrost{}})

	globalManagerMu.Lock()
	prior := globalManager
	globalManager = installed
	globalManagerMu.Unlock()

	t.Cleanup(func() {
		_ = installed.StopAll()
		globalManagerMu.Lock()
		globalManager = prior
		globalManagerMu.Unlock()
	})

	// The action supplies schema context and no suggestion, which is the
	// documented handoff to the model completion path.
	RegisterBuiltinAction(ActionFunc{
		Name:        "heimdall_autocomplete_suggest",
		Description: "NOR281 autocomplete schema action",
		Category:    "testing",
		Handler: func(ActionContext) (*ActionResult, error) {
			return &ActionResult{
				Success: true,
				Data: map[string]interface{}{
					"schema": map[string]interface{}{
						"labels":     []interface{}{"Person"},
						"properties": []interface{}{"name"},
						"relTypes":   []interface{}{"KNOWS"},
					},
					"suggestion": "",
				},
			}, nil
		},
	})
}

// nor281AutocompleteHandler builds the public handler around a generator whose
// completion and error are controlled by the case.
func nor281AutocompleteHandler(t *testing.T, completion string, generateErr error) *Handler {
	t.Helper()

	generator := NewMockGenerator("/test/model.gguf")
	generator.generateFunc = func(context.Context, string, GenerateParams) (string, error) {
		return completion, generateErr
	}
	manager := newTestManager(generator)
	return testHandler(manager, manager.config)
}

func nor281PostAutocomplete(t *testing.T, handler *Handler, body string) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/bifrost/autocomplete", bytes.NewBufferString(body))
	handler.ServeHTTP(recorder, request)
	return recorder
}

func nor281Suggestion(t *testing.T, recorder *httptest.ResponseRecorder) string {
	t.Helper()

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var payload map[string]interface{}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &payload))
	require.Contains(t, payload, "schema")

	suggestion, ok := payload["suggestion"]
	require.True(t, ok, "response must carry a suggestion field")
	require.NotNil(t, suggestion)
	text, ok := suggestion.(string)
	require.True(t, ok, "suggestion must be a string")
	return text
}

// TestNOR281AutocompletePreservesExactCypher is the reproduction matrix for the
// suggestion-cleaning boundary: a valid generated statement must come back
// unchanged, and markdown framing must be removed without editing the statement.
func TestNOR281AutocompletePreservesExactCypher(t *testing.T) {
	nor281InstallAutocompleteSubsystem(t)

	cases := []struct {
		name        string
		query       string
		completion  string
		generateErr error
		want        string
	}{
		{
			name:       "plain return alias",
			query:      nor281AutocompleteQuery,
			completion: "RETURN 1 AS one",
			want:       "RETURN 1 AS one",
		},
		{
			name:       "quoted plain alias",
			query:      nor281AutocompleteQuery,
			completion: "RETURN 1 AS `a b`",
			want:       "RETURN 1 AS `a b`",
		},
		{
			name:       "quoted alias with double internal space",
			query:      nor281AutocompleteQuery,
			completion: "RETURN 1 AS `a  b`",
			want:       "RETURN 1 AS `a  b`",
		},
		{
			name:       "quoted alias ending in escaped backtick",
			query:      nor281AutocompleteQuery,
			completion: "RETURN 1 AS `a```",
			want:       "RETURN 1 AS `a```",
		},
		{
			name:       "quoted alias with escaped internal backtick",
			query:      nor281AutocompleteQuery,
			completion: "RETURN 1 AS `a``b`",
			want:       "RETURN 1 AS `a``b`",
		},
		{
			name:       "plain fence then newline",
			query:      nor281AutocompleteQuery,
			completion: "```\nRETURN 1 AS `a b`\n```",
			want:       "RETURN 1 AS `a b`",
		},
		{
			name:       "cypher fence then newline",
			query:      nor281AutocompleteQuery,
			completion: "```cypher\nRETURN 1 AS `a b`\n```",
			want:       "RETURN 1 AS `a b`",
		},
		{
			name:       "plain fence adjacent to closing alias",
			query:      nor281AutocompleteQuery,
			completion: "```\nRETURN 1 AS `a b````",
			want:       "RETURN 1 AS `a b`",
		},
		{
			name:       "cypher fence adjacent to closing alias",
			query:      nor281AutocompleteQuery,
			completion: "```cypher\nRETURN 1 AS `a b````",
			want:       "RETURN 1 AS `a b`",
		},
		{
			name:       "cypher fence adjacent to escaped backtick alias",
			query:      nor281AutocompleteQuery,
			completion: "```cypher\nRETURN 1 AS `a``````",
			want:       "RETURN 1 AS `a```",
		},
		{
			name:       "ordinary completion",
			query:      "MATCH (n:Person",
			completion: "MATCH (n:Person) RETURN n LIMIT 25",
			want:       "MATCH (n:Person) RETURN n LIMIT 25",
		},
		{
			name:       "empty generation",
			query:      nor281AutocompleteQuery,
			completion: "",
			want:       "",
		},
		{
			name:       "malformed generation without a cypher keyword",
			query:      nor281AutocompleteQuery,
			completion: "I am not sure which query you mean.",
			want:       "",
		},
		{
			name:        "generation error",
			query:       nor281AutocompleteQuery,
			completion:  "",
			generateErr: errors.New("mock decode failure"),
			want:        "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := nor281AutocompleteHandler(t, tc.completion, tc.generateErr)

			recorder := nor281PostAutocomplete(t, handler, `{"query":`+nor281QuoteJSON(t, tc.query)+`}`)

			assert.Equal(t, tc.want, nor281Suggestion(t, recorder))
		})
	}
}

// TestNOR281AutocompleteRequestBaseline pins the pre-existing request handling
// for this route. No new invalid-input behavior is introduced here.
func TestNOR281AutocompleteRequestBaseline(t *testing.T) {
	nor281InstallAutocompleteSubsystem(t)

	generator := NewMockGenerator("/test/model.gguf")
	generator.generateFunc = func(context.Context, string, GenerateParams) (string, error) {
		return "RETURN 1 AS one", nil
	}
	manager := newTestManager(generator)
	handler := testHandler(manager, manager.config)

	t.Run("empty query is rejected", func(t *testing.T) {
		recorder := nor281PostAutocomplete(t, handler, `{"query":""}`)
		assert.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	})

	t.Run("invalid json is rejected", func(t *testing.T) {
		recorder := nor281PostAutocomplete(t, handler, `{"query":`)
		assert.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	})

	t.Run("get is not allowed", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/bifrost/autocomplete", nil))
		assert.Equal(t, http.StatusMethodNotAllowed, recorder.Code, recorder.Body.String())
	})

	assert.Equal(t, int64(0), generator.GetGenerateCount(), "rejected requests must not reach the model")
}

// nor281QuoteJSON encodes a Go string as a JSON string literal.
func nor281QuoteJSON(t *testing.T, value string) string {
	t.Helper()

	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	return string(encoded)
}

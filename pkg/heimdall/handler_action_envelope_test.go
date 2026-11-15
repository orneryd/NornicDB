package heimdall

import (
	"bytes"
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bracedActionEnvelopeParams is one model-emitted parameter object whose strings
// contain the characters a brace counter mistakes for structure: a closing
// brace, an opening brace, an escaped quote and Windows-style backslashes. Its
// numbers cover integers above 2^53 (positive and negative), a 19-digit integer,
// FLOAT forms and those forms nested in arrays and maps.
const bracedActionEnvelopeParams = `{"note":"closing } then opening { brace","quote":"say \"hello\" to }","path":"C:\\tmp\\ledger.json","nested":{"inner":{"value":1,"list":[[2,3],{"deep":4}]}},"max":9007199254740993,"min":-9007199254740993,"ledger":1234567890123456789,"ratio":0.125,"exponent":1.5e3}`

// bracedActionEnvelope frames those parameters as the envelope an SLM emits for
// the named action.
func bracedActionEnvelope(action string) string {
	return `{"action":"` + action + `","params":` + bracedActionEnvelopeParams + `}`
}

// assertBracedEnvelopeParams asserts the envelope parameters reached their
// consumer unchanged: the brace-bearing strings stay literal, the integers keep
// the exact values the model emitted (no float64 rounding) and the nested
// containers keep their shape.
func assertBracedEnvelopeParams(t *testing.T, params map[string]interface{}) {
	t.Helper()
	require.NotNil(t, params, "the action envelope was not parsed into parameters")
	assert.Equal(t, "closing } then opening { brace", params["note"])
	assert.Equal(t, `say "hello" to }`, params["quote"])
	assert.Equal(t, `C:\tmp\ledger.json`, params["path"])
	assert.Equal(t, int64(9007199254740993), params["max"])
	assert.Equal(t, int64(-9007199254740993), params["min"])
	assert.Equal(t, int64(1234567890123456789), params["ledger"])
	assert.Equal(t, 0.125, params["ratio"])
	assert.Equal(t, 1500.0, params["exponent"])

	nested, isMap := params["nested"].(map[string]interface{})
	require.True(t, isMap, "nested must stay a JSON object, got %T", params["nested"])
	inner, isMap := nested["inner"].(map[string]interface{})
	require.True(t, isMap, "nested.inner must stay a JSON object, got %T", nested["inner"])
	assert.Equal(t, int64(1), inner["value"])
	assert.Equal(t, []interface{}{[]interface{}{int64(2), int64(3)}, map[string]interface{}{"deep": int64(4)}}, inner["list"])
}

// TestParseActionEnvelopeReadsJSONValueGrammar pins the envelope contract to JSON
// value framing instead of brace counting. Each case below is a shape a naive
// brace scan mis-frames: a quoted brace ends or opens nesting, an escape hides a
// brace, prose surrounds the object, or the object is malformed and must not be
// accepted.
func TestParseActionEnvelopeReadsJSONValueGrammar(t *testing.T) {
	tests := []struct {
		name       string
		response   string
		wantAction string
		check      func(t *testing.T, action *ParsedAction)
	}{
		{
			name:       "quoted closing brace does not end the object",
			response:   `{"action":"record_envelope","params":{"note":"closing } in text"}}`,
			wantAction: "record_envelope",
			check: func(t *testing.T, action *ParsedAction) {
				assert.Equal(t, "closing } in text", action.Params["note"])
			},
		},
		{
			name:       "quoted opening brace does not open nesting",
			response:   `{"action":"record_envelope","params":{"note":"opening { in text"}}`,
			wantAction: "record_envelope",
			check: func(t *testing.T, action *ParsedAction) {
				assert.Equal(t, "opening { in text", action.Params["note"])
			},
		},
		{
			name:       "escaped quotes and backslashes stay literal",
			response:   `{"action":"record_envelope","params":{"note":"quote \" and brace } and backslash \\","path":"C:\\tmp\\ledger.json"}}`,
			wantAction: "record_envelope",
			check: func(t *testing.T, action *ParsedAction) {
				assert.Equal(t, `quote " and brace } and backslash \`, action.Params["note"])
				assert.Equal(t, `C:\tmp\ledger.json`, action.Params["path"])
			},
		},
		{
			name:       "nested objects and arrays stay structured",
			response:   `{"action":"record_envelope","params":{"outer":{"inner":{"value":1,"list":[[2,3],{"deep":4}]}}}}`,
			wantAction: "record_envelope",
			check: func(t *testing.T, action *ParsedAction) {
				outer, isMap := action.Params["outer"].(map[string]interface{})
				require.True(t, isMap, "outer must stay a JSON object, got %T", action.Params["outer"])
				inner, isMap := outer["inner"].(map[string]interface{})
				require.True(t, isMap, "outer.inner must stay a JSON object, got %T", outer["inner"])
				assert.Equal(t, int64(1), inner["value"])
				assert.Equal(t, []interface{}{[]interface{}{int64(2), int64(3)}, map[string]interface{}{"deep": int64(4)}}, inner["list"])
			},
		},
		{
			name:       "suffix prose after the envelope",
			response:   `{"action":"record_envelope","params":{"value":1}} and that is the whole call.`,
			wantAction: "record_envelope",
			check: func(t *testing.T, action *ParsedAction) {
				assert.Equal(t, int64(1), action.Params["value"])
			},
		},
		{
			name:       "prose before the envelope",
			response:   "Sure, running it now:\n{\"action\":\"record_envelope\",\"params\":{\"value\":1}}\n",
			wantAction: "record_envelope",
			check: func(t *testing.T, action *ParsedAction) {
				assert.Equal(t, int64(1), action.Params["value"])
			},
		},
		{
			name:       "surrounding whitespace and absent params",
			response:   " \n\t{\"action\":\"record_envelope\"}\r\n ",
			wantAction: "record_envelope",
			check: func(t *testing.T, action *ParsedAction) {
				require.NotNil(t, action.Params, "an envelope without params must still carry a non-nil params map")
				assert.Empty(t, action.Params)
			},
		},
		{
			name:     "blank action is rejected",
			response: `{"action":"   ","params":{"value":1}}`,
		},
		{
			name:     "absent action is rejected",
			response: `{"params":{"value":1}}`,
		},
		{
			name:     "truncated envelope is rejected",
			response: `{"action":"record_envelope","params":{`,
		},
		{
			name:     "truncated envelope after a quoted brace is rejected",
			response: `{"action":"record_envelope","params":{"note":"}"}`,
		},
		{
			name:     "malformed json is rejected",
			response: `{not valid json}`,
		},
		{
			name:     "response without a json object is rejected",
			response: "no json content here",
		},
		{
			name:     "empty response is rejected",
			response: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			action := parseActionEnvelope(tt.response)
			if tt.wantAction == "" {
				assert.Nil(t, action, "a response without a usable action envelope must not parse")
				return
			}
			require.NotNil(t, action, "the action envelope must parse")
			assert.Equal(t, tt.wantAction, action.Action)
			if tt.check != nil {
				tt.check(t, action)
			}
		})
	}
}

// TestParseActionEnvelopePreservesExactNumbers pins the numeric range the action
// envelope must carry into tool parameters: int64 bounds and integers above 2^53
// stay exact, out-of-int64 integers keep rejecting the envelope.
func TestParseActionEnvelopePreservesExactNumbers(t *testing.T) {
	action := parseActionEnvelope(`{"action":"record_envelope","params":{"max":9223372036854775807,"min":-9223372036854775808,"above":9007199254740993,"nested":[9007199254740993,{"deep":-9007199254740993}]}}`)
	require.NotNil(t, action)
	assert.Equal(t, int64(9223372036854775807), action.Params["max"])
	assert.Equal(t, int64(-9223372036854775808), action.Params["min"])
	assert.Equal(t, int64(9007199254740993), action.Params["above"])
	assert.Equal(t, []interface{}{int64(9007199254740993), map[string]interface{}{"deep": int64(-9007199254740993)}}, action.Params["nested"])

	for _, outOfRange := range []string{"9223372036854775808", "-9223372036854775809"} {
		assert.Nil(t, parseActionEnvelope(`{"action":"record_envelope","params":{"v":`+outOfRange+`}}`), "integer %s is outside int64", outOfRange)
	}
}

// TestLiveHeimdallInvokerSendPromptDispatchesParsedEnvelope proves the invoker's
// public prompt path reaches the canonical envelope owner. The model output
// carries braces inside strings, so only JSON value framing recovers the
// parameters, and the invoker must invoke the parsed action rather than return
// the raw model text.
func TestLiveHeimdallInvokerSendPromptDispatchesParsedEnvelope(t *testing.T) {
	previous := globalManager
	globalManager = nil
	t.Cleanup(func() { globalManager = previous })

	RegisterBuiltinAction(ActionFunc{
		Name:        "envelope_capture",
		Description: "Returns the parameters parsed from the model response",
		Category:    "testing",
		Handler: func(ctx ActionContext) (*ActionResult, error) {
			return &ActionResult{Success: true, Message: "captured", Data: map[string]interface{}{"params": ctx.Params}}, nil
		},
	})

	mockGen := NewMockGenerator("/test/model.gguf")
	mockGen.generateFunc = func(ctx context.Context, prompt string, params GenerateParams) (string, error) {
		return bracedActionEnvelope("envelope_capture"), nil
	}
	invoker := NewLiveHeimdallInvoker(GetSubsystemManager(), mockGen, NewMockBifrost(), &mockDBRouter{}, &mockMetricsReader{})

	result, err := invoker.SendPrompt(context.Background(), "record this")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Success, "the parsed action result must be returned, got %q", result.Message)
	assert.Equal(t, "captured", result.Message, "SendPrompt must invoke the parsed action instead of returning the raw model text")

	params, isMap := result.Data["params"].(map[string]interface{})
	require.True(t, isMap, "the action was not invoked with parameters: %#v", result.Data)
	assertBracedEnvelopeParams(t, params)
}

// TestHeimdallPromptDispatchParsesBracedActionEnvelope drives the public Bifrost
// chat endpoint through the prompt-based loop and proves the dispatched tool
// arguments come from the canonical envelope owner.
func TestHeimdallPromptDispatchParsesBracedActionEnvelope(t *testing.T) {
	previous := globalManager
	globalManager = nil
	t.Cleanup(func() { globalManager = previous })

	RegisterBuiltinAction(ActionFunc{
		Name:        "record_envelope",
		Description: "Records the exact tool arguments without executing anything",
		Category:    "testing",
		Handler: func(ctx ActionContext) (*ActionResult, error) {
			return &ActionResult{Success: true}, nil
		},
	})

	var mu sync.Mutex
	rounds := 0
	mockGen := NewMockGenerator("/test/model.gguf")
	mockGen.generateFunc = func(ctx context.Context, prompt string, params GenerateParams) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		rounds++
		if rounds == 1 {
			return bracedActionEnvelope("record_envelope"), nil
		}
		return "done", nil
	}
	manager := newTestManager(mockGen)
	require.False(t, manager.SupportsTools(), "the prompt-based loop must own this path")

	handler := testHandler(manager, manager.config)
	runner := &toolIntegerRecordingRunner{names: []string{"record_envelope"}}
	handler.SetInMemoryToolRunner(runner)

	body, err := json.Marshal(map[string]interface{}{
		"model":    "test-model",
		"messages": []map[string]interface{}{{"role": "user", "content": "record this"}},
		"stream":   false,
	})
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body)))

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	assertBracedEnvelopeParams(t, runner.argumentObject())
}

// TestActionEnvelopeOwnerIsSingleSource keeps the retired duplicates retired: the
// package owns action-envelope parsing once, with no forwarding wrapper or
// test-only brace scanner left in production source.
func TestActionEnvelopeOwnerIsSingleSource(t *testing.T) {
	retired := map[string]bool{
		"tryParseActionResponse": true,
		"parsedActionCmd":        true,
		"extractFirstJSONObject": true,
	}
	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)
	scanned := 0
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		source, err := os.ReadFile(path)
		require.NoError(t, err)
		file, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
		require.NoError(t, err)
		scanned++
		ast.Inspect(file, func(node ast.Node) bool {
			if identifier, ok := node.(*ast.Ident); ok && retired[identifier.Name] {
				t.Errorf("%s: retired action-envelope parser %q must not be reintroduced", path, identifier.Name)
			}
			return true
		})
	}
	require.NotZero(t, scanned, "no production source was scanned")
}

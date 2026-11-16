package heimdall

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// toolIntegerArgumentsJSON is one provider/SLM tool-call argument object covering
// the JSON number forms an assistant can emit: integers above 2^53 (positive and
// negative), a 19-digit integer, fractional and exponent FLOATs, and those forms
// nested inside mixed arrays and maps.
const toolIntegerArgumentsJSON = `{"content":"x","metadata":{"external_id":9007199254740993,"ledger":1234567890123456789,"negative_id":-9007199254740993,"ratio":0.125,"exponent":1.5e3,"mixed":[9007199254740993,{"deep":-42,"nested":[1,2.5]}]}}`

// toolIntegerRecordingRunner records the arguments the agentic loop dispatches to
// an in-process (MCP-style) tool, so a test can assert exactly what the tool got.
type toolIntegerRecordingRunner struct {
	mu     sync.Mutex
	names  []string
	called []map[string]interface{}
}

func (r *toolIntegerRecordingRunner) ToolDefinitions() []MCPTool {
	definitions := make([]MCPTool, 0, len(r.names))
	for _, name := range r.names {
		definitions = append(definitions, MCPTool{Name: name, Description: "records tool arguments", InputSchema: DefaultActionInputSchema})
	}
	return definitions
}

func (r *toolIntegerRecordingRunner) ToolNames() []string { return r.names }

func (r *toolIntegerRecordingRunner) CallTool(_ context.Context, _ string, args map[string]interface{}, _ string) (interface{}, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.called = append(r.called, args)
	return map[string]interface{}{"stored": true}, nil
}

func (r *toolIntegerRecordingRunner) argumentObject() map[string]interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.called) == 0 {
		return nil
	}
	return r.called[0]
}

// assertExactToolArguments asserts the dispatched argument object still carries
// the exact integers, negatives, FLOATs and nested containers the model emitted.
func assertExactToolArguments(t *testing.T, args map[string]interface{}) {
	t.Helper()
	require.NotNil(t, args, "the tool was not dispatched with arguments")
	assert.Equal(t, "x", args["content"])

	metadata, isMap := args["metadata"].(map[string]interface{})
	require.True(t, isMap, "metadata must stay a JSON object, got %T", args["metadata"])
	assert.Equal(t, int64(9007199254740993), metadata["external_id"])
	assert.Equal(t, int64(1234567890123456789), metadata["ledger"])
	assert.Equal(t, int64(-9007199254740993), metadata["negative_id"])
	assert.Equal(t, 0.125, metadata["ratio"])
	assert.Equal(t, 1500.0, metadata["exponent"])

	mixed, isList := metadata["mixed"].([]interface{})
	require.True(t, isList, "mixed must stay a JSON array, got %T", metadata["mixed"])
	require.Len(t, mixed, 2)
	assert.Equal(t, int64(9007199254740993), mixed[0])
	nested, isMap := mixed[1].(map[string]interface{})
	require.True(t, isMap, "mixed[1] must stay a JSON object, got %T", mixed[1])
	assert.Equal(t, int64(-42), nested["deep"])
	assert.Equal(t, []interface{}{int64(1), 2.5}, nested["nested"])
}

// TestHeimdallToolArgumentsPreserveExactIntegers drives the public Bifrost chat
// endpoint with a provider that emits native tool calls and proves the in-process
// tool receives the JSON numbers the model emitted, without float64 rounding.
func TestHeimdallToolArgumentsPreserveExactIntegers(t *testing.T) {
	var rounds int
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("provider request used the wrong path: %s", r.URL.Path)
		}
		var request map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("provider request body: %v", err)
			return
		}
		rounds++
		if rounds == 1 {
			message := map[string]interface{}{
				"content": "",
				"tool_calls": []map[string]interface{}{{
					"id":   "call_1",
					"type": "function",
					"function": map[string]interface{}{
						"name":      "store",
						"arguments": toolIntegerArgumentsJSON,
					},
				}},
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"choices": []map[string]interface{}{{"message": message, "finish_reason": "tool_calls"}},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"choices": []map[string]interface{}{{"message": map[string]interface{}{"content": "done"}, "finish_reason": "stop"}},
		})
	}))
	t.Cleanup(provider.Close)

	manager, err := NewManager(Config{Enabled: true, Provider: "openai", APIURL: provider.URL, APIKey: "probe-key", Model: "test"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.True(t, manager.SupportsTools(), "the openai provider must dispatch native tool calls")

	handler := NewHandler(manager, manager.config, &mockDBRouter{}, &mockMetricsReader{})
	require.NotNil(t, handler)
	runner := &toolIntegerRecordingRunner{names: []string{"store"}}
	handler.SetInMemoryToolRunner(runner)

	body, err := json.Marshal(map[string]interface{}{
		"model":    "test",
		"messages": []map[string]interface{}{{"role": "user", "content": "store this"}},
		"stream":   false,
	})
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body)))

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, 2, rounds, "the agentic loop must call the provider once per round")
	assertExactToolArguments(t, runner.argumentObject())
}

// TestHeimdallPromptToolArgumentsPreserveExactIntegers drives the same public
// endpoint through the prompt-based loop a local provider uses, where the action
// envelope from the SLM response supplies the tool parameters.
func TestHeimdallPromptToolArgumentsPreserveExactIntegers(t *testing.T) {
	previous := globalManager
	globalManager = nil
	t.Cleanup(func() { globalManager = previous })

	RegisterBuiltinAction(ActionFunc{
		Name:        "record_integers",
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
			return `{"action":"record_integers","params":` + toolIntegerArgumentsJSON + `}`, nil
		}
		return "done", nil
	}
	manager := newTestManager(mockGen)
	require.False(t, manager.SupportsTools(), "the prompt-based loop must own this path")

	handler := testHandler(manager, manager.config)
	runner := &toolIntegerRecordingRunner{names: []string{"record_integers"}}
	handler.SetInMemoryToolRunner(runner)

	body, err := json.Marshal(map[string]interface{}{
		"model":    "test-model",
		"messages": []map[string]interface{}{{"role": "user", "content": "store this"}},
		"stream":   false,
	})
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body)))

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	assertExactToolArguments(t, runner.argumentObject())
}

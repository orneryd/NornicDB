package heimdall

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Exercise both historical consumers, including both HTTP response transports.
// The recording action never evaluates the strings as a query or contacts a provider.
func nor202ObserveConsumer(t *testing.T, mode, response string) ([]map[string]interface{}, string) {
	t.Helper()
	resetNOR202Subsystem(t)
	if mode == "invoker" {
		var mu sync.Mutex
		var calls []map[string]interface{}
		RegisterBuiltinAction(ActionFunc{Name: "foo", Category: "testing", Handler: func(ctx ActionContext) (*ActionResult, error) {
			mu.Lock()
			calls = append(calls, ctx.Params)
			mu.Unlock()
			return &ActionResult{Success: true, Message: "captured"}, nil
		}})
		generator := NewMockGenerator("/test/model.gguf")
		generator.generateFunc = func(context.Context, string, GenerateParams) (string, error) { return response, nil }
		invoker := NewLiveHeimdallInvoker(GetSubsystemManager(), generator, NewMockBifrost(), &mockDBRouter{}, &mockMetricsReader{})
		result, err := invoker.SendPrompt(t.Context(), "record only")
		require.NoError(t, err)
		require.NotNil(t, result)
		require.True(t, result.Success)
		require.EqualValues(t, 1, generator.GetGenerateCount())
		mu.Lock()
		defer mu.Unlock()
		return append([]map[string]interface{}(nil), calls...), result.Message
	}
	registerNOR202NoOpAction("foo")
	runner := &nor202RecordedRunner{names: []string{"foo"}}
	script := &nor202Script{responses: []string{response}}
	handler := newNOR202PromptHandler(t, script, runner)
	if mode == "stream" {
		// The legacy streaming route uses GenerateStream, not the nonstream
		// agentic loop's Generate. Feed the exact response in two chunks so
		// aggregation and parsing are exercised at the actual transport owner.
		generator := handler.manager.generator.(*MockGenerator)
		generator.streamFunc = func(ctx context.Context, _ string, _ GenerateParams, callback func(string) error) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			middle := len(response) / 2
			if err := callback(response[:middle]); err != nil {
				return err
			}
			return callback(response[middle:])
		}
	}
	body, err := json.Marshal(map[string]interface{}{
		"model": "test-model", "stream": mode == "stream",
		"messages": []map[string]interface{}{{"role": "user", "content": "record only"}},
	})
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body)))
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	if mode != "stream" {
		var answer ChatResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &answer))
		require.Len(t, answer.Choices, 1)
		return nor202RecordedToolCalls(runner), answer.Choices[0].Message.Content
	}
	require.Contains(t, recorder.Header().Get("Content-Type"), "text/event-stream")
	var content strings.Builder
	done := false
	for _, line := range strings.Split(recorder.Body.String(), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			done = true
			continue
		}
		var chunk ChatResponse
		require.NoError(t, json.Unmarshal([]byte(data), &chunk))
		for _, choice := range chunk.Choices {
			if choice.Delta != nil {
				content.WriteString(choice.Delta.Content)
			}
		}
	}
	require.True(t, done, "the actual SSE transport must complete")
	require.EqualValues(t, 1, handler.manager.generator.(*MockGenerator).GetStreamCount())
	require.Equal(t, 0, script.roundCount(), "legacy SSE uses its stream owner, not the nonstream generation loop")
	return nor202RecordedToolCalls(runner), content.String()
}

func TestNOR202CompleteBracedStringAndFramingConsumers(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]interface{}
		prefix string
		suffix string
	}{
		{name: "exact reported envelope", params: map[string]interface{}{"query": "RETURN '}'"}},
		{name: "unmatched opening brace in value", params: map[string]interface{}{"query": "RETURN '{'"}},
		{name: "multiple closing braces in value", params: map[string]interface{}{"query": "RETURN '}}}'"}},
		{name: "escaped quotes and backslashes", params: map[string]interface{}{"query": `say "hi" at C:\tmp\{x} then }`}},
		{name: "nested maps arrays and string keys", params: map[string]interface{}{"nested": map[string]interface{}{"key}": "value{"}, "array": []interface{}{"}", "{"}}},
		{name: "ordinary values", params: map[string]interface{}{"query": "RETURN 1", "enabled": true}},
		{name: "leading and trailing prose", params: map[string]interface{}{"query": "RETURN '}'"}, prefix: "Here is the action:\n", suffix: "\nThat is the full action."},
		{name: "markdown JSON framing", params: map[string]interface{}{"query": "RETURN '{'"}, prefix: "```json\n", suffix: "\n```"},
	}
	for _, mode := range []string{"http", "stream", "invoker"} {
		t.Run(mode, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					encoded, err := json.Marshal(map[string]interface{}{"action": "foo", "params": tc.params})
					require.NoError(t, err)
					if tc.name == "exact reported envelope" {
						require.Equal(t, nor202ReportedResponse, string(encoded))
					}
					calls, answer := nor202ObserveConsumer(t, mode, tc.prefix+string(encoded)+tc.suffix)
					require.Equal(t, []map[string]interface{}{tc.params}, calls)
					if mode == "invoker" {
						require.Equal(t, "captured", answer)
					} else if mode == "stream" {
						require.Contains(t, answer, "Action completed successfully")
					} else {
						require.Equal(t, "done", strings.TrimSpace(answer))
					}
				})
			}
		})
	}
}

func TestNOR202MalformedAndPlainResponsesNeverSubstituteActions(t *testing.T) {
	for _, mode := range []string{"http", "stream", "invoker"} {
		t.Run(mode, func(t *testing.T) {
			for _, tc := range []struct{ name, response string }{
				{"truncated string", `{"action":"foo","params":{"query":"RETURN '}`},
				{"truncated envelope", `{"action":"foo","params":{"query":"RETURN '}'"}`},
				{"invalid JSON escape", `{"action":"foo","params":{"query":"bad\q}"}}`},
				{"wrong params shape", `{"action":"foo","params":["}"]}`},
				{"no action", `{"params":{"query":"}"}}`},
				{"malformed first envelope followed by another action", `{"action":"broken","params":} {"action":"foo","params":{"query":"RETURN '}'"}}`},
				{"plain text", "A plain answer."},
			} {
				t.Run(tc.name, func(t *testing.T) {
					calls, answer := nor202ObserveConsumer(t, mode, tc.response)
					require.Empty(t, calls)
					require.Equal(t, strings.TrimSpace(tc.response), strings.TrimSpace(answer))
				})
			}
		})
	}
}

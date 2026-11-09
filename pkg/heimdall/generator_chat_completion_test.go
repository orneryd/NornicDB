package heimdall

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProviderCompletionToolRoundWireContract(t *testing.T) {
	for _, provider := range []struct {
		name string
		new  func(Config) (Generator, error)
	}{
		{"ollama", newOllamaGenerator}, {"openai", newOpenAIGenerator},
		{"vllm", newOpenAIGenerator}, {"litellm", newOpenAIGenerator}, {"orca", newOpenAIGenerator},
	} {
		t.Run(provider.name, func(t *testing.T) {
			requests := make(chan map[string]any, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request map[string]any
				err := json.NewDecoder(r.Body).Decode(&request)
				if err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				requests <- request
				message := map[string]any{"content": "done", "tool_calls": []any{
					map[string]any{"id": "next-1", "type": "function", "function": map[string]any{"name": "inspect", "arguments": `{"n":1}`}},
					map[string]any{"id": "next-2", "type": "function", "function": map[string]any{"name": "inspect", "arguments": `{"n":2}`}},
				}}
				if provider.name == "ollama" {
					_ = json.NewEncoder(w).Encode(map[string]any{"message": message})
				} else {
					_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": message}}})
				}
			}))
			t.Cleanup(server.Close)
			generator, err := provider.new(Config{Provider: provider.name, APIURL: server.URL, APIKey: "synthetic", Model: "wire-test"})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, generator.Close()) })
			messages := []ToolRoundMessage{
				{Role: "assistant", Content: "checking", ToolCalls: []ParsedToolCall{{Id: "prior", Name: "inspect", Arguments: `{"n":0}`}}},
				{Role: "tool", Content: "prior result", ToolCallID: "prior"},
			}
			tools := []MCPTool{{Name: "inspect", Description: "Inspect data", InputSchema: json.RawMessage(`{"type":"object"}`)}}
			content, calls, err := generator.(GeneratorWithTools).GenerateWithTools(context.Background(), messages, tools, GenerateParams{MaxTokens: 17, Temperature: 0.5})
			require.NoError(t, err)
			received := <-requests
			require.Equal(t, "done", content)
			require.Equal(t, []ParsedToolCall{{Id: "next-1", Name: "inspect", Arguments: `{"n":1}`}, {Id: "next-2", Name: "inspect", Arguments: `{"n":2}`}}, calls)
			require.Equal(t, []any{
				map[string]any{"role": "assistant", "content": "checking", "tool_calls": []any{map[string]any{"id": "prior", "type": "function", "function": map[string]any{"name": "inspect", "arguments": `{"n":0}`}}}},
				map[string]any{"role": "tool", "content": "prior result", "tool_call_id": "prior"},
			}, received["messages"])
			require.Equal(t, []any{map[string]any{"type": "function", "function": map[string]any{"name": "inspect", "description": "Inspect data", "parameters": map[string]any{"type": "object"}}}}, received["tools"])
			require.Equal(t, float64(0.5), received["temperature"])
			tokenKey := "max_tokens"
			if provider.name == "ollama" {
				tokenKey = "num_predict"
			}
			require.Equal(t, float64(17), received[tokenKey])
			require.Equal(t, "prior", messages[0].ToolCalls[0].Id)
			require.Equal(t, "prior result", messages[1].Content)
			require.Equal(t, json.RawMessage(`{"type":"object"}`), tools[0].InputSchema)
		})
	}
}

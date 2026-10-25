package heimdall

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestR73ReviewHeimdallAPIURLQueryCredentialsReachProviders(t *testing.T) {
	const rawQuery = "api_key=first%2Bvalue&region=eu&api_key=second%20value"
	tests := []struct {
		provider     string
		basePath     string
		expectedPath string
	}{
		{provider: "ollama", basePath: "/proxy", expectedPath: "/proxy/api/chat"},
		{provider: "openai", basePath: "/proxy", expectedPath: "/proxy/v1/chat/completions"},
		{provider: "orca", basePath: "/proxy/v1", expectedPath: "/proxy/v1/chat/completions"},
		{provider: "vllm", basePath: "/proxy", expectedPath: "/proxy/v1/chat/completions"},
		{provider: "litellm", basePath: "/proxy", expectedPath: "/proxy/v1/chat/completions"},
	}
	for _, test := range tests {
		t.Run(test.provider, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.EscapedPath() != test.expectedPath {
					t.Error("provider request used the wrong path")
				}
				if r.URL.RawQuery != rawQuery {
					t.Error("provider request did not preserve the ordered query")
				}
				if test.provider == "ollama" {
					_ = json.NewEncoder(w).Encode(map[string]any{"message": map[string]any{"content": "ok"}, "done": true})
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"choices": []map[string]any{{"message": map[string]any{"content": "ok"}, "finish_reason": "stop"}},
				})
			}))
			t.Cleanup(server.Close)

			manager, err := NewManager(Config{
				Enabled:  true,
				Provider: test.provider,
				APIURL:   server.URL + test.basePath + "?" + rawQuery,
				APIKey:   "header-value",
				Model:    "route-test",
			})
			if err != nil {
				t.Fatal("provider construction failed")
			}
			t.Cleanup(func() { _ = manager.Close() })
			if _, err := manager.Generate(context.Background(), "probe", GenerateParams{}); err != nil {
				t.Fatal("provider request failed")
			}
		})
	}
}

// All modes must reach the same request owner without changing authentication,
// generation options or their distinct response protocols.
func TestHeimdallChatRequestModes(t *testing.T) {
	for _, provider := range []string{"ollama", "openai"} {
		for _, mode := range []string{"plain", "tools", "stream"} {
			for _, failed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/failure=%t", provider, mode, failed), func(t *testing.T) {
					const credential = "synthetic-provider-mode-credential"
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" || r.URL.RawQuery != "api_key="+credential {
							t.Error("request method, content type or URL credential changed")
						}
						path := "/proxy/api/chat"
						if provider == "openai" {
							path = "/proxy/v1/chat/completions"
							if r.Header.Get("Authorization") != "Bearer "+credential {
								t.Error("runtime authentication changed")
							}
						}
						if r.URL.Path != path {
							t.Error("request route changed")
						}
						var request map[string]any
						if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
							t.Error(err)
							return
						}
						if request["model"] != "mode-test" || request["temperature"] != float64(0.25) || request["messages"].([]any)[0].(map[string]any)["content"] != "probe" {
							t.Error("model, temperature or message changed")
						}
						tokenKey := "max_tokens"
						if provider == "ollama" {
							tokenKey = "num_predict"
						}
						if request[tokenKey] != float64(1024) || (request["stream"] == true) != (mode == "stream") {
							t.Error("default token budget or streaming mode changed")
						}
						_, hasTools := request["tools"]
						if hasTools != (mode == "tools") {
							t.Error("tool definitions changed")
						}
						if failed {
							w.WriteHeader(http.StatusBadGateway)
							_, _ = fmt.Fprint(w, "upstream rejected "+credential)
							return
						}
						if mode == "stream" {
							if provider == "ollama" {
								_, _ = fmt.Fprintln(w, `{"message":{"content":"first"}}`)
								_, _ = fmt.Fprintln(w, `{"message":{"content":"second"},"done":true}`)
							} else {
								_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"second\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n")
							}
							return
						}
						message := map[string]any{"content": "complete", "tool_calls": []map[string]any{{"id": "call-1", "type": "function", "function": map[string]any{"name": "inspect", "arguments": `{}`}}}}
						if provider == "ollama" {
							_ = json.NewEncoder(w).Encode(map[string]any{"message": message, "done": true})
						} else {
							_ = json.NewEncoder(w).Encode(map[string]any{"choices": []map[string]any{{"message": message}}})
						}
					}))
					t.Cleanup(server.Close)
					var generator Generator
					cfg := Config{APIURL: server.URL + "/proxy?api_key=" + credential, APIKey: credential, Model: "mode-test"}
					var err error
					if provider == "ollama" {
						generator, err = newOllamaGenerator(cfg)
					} else {
						generator, err = newOpenAIGenerator(cfg)
					}
					if err != nil {
						t.Fatal(err)
					}
					params := GenerateParams{Temperature: 0.25}
					var content string
					var calls []ParsedToolCall
					switch mode {
					case "plain":
						content, err = generator.Generate(context.Background(), "probe", params)
					case "tools":
						content, calls, err = generator.(GeneratorWithTools).GenerateWithTools(context.Background(), []ToolRoundMessage{{Role: "user", Content: "probe"}}, []MCPTool{{Name: "inspect", InputSchema: json.RawMessage(`{"type":"object"}`)}}, params)
					case "stream":
						var tokens []string
						err = generator.GenerateStream(context.Background(), "probe", params, func(token string) error { tokens = append(tokens, token); return nil })
						content = strings.Join(tokens, "")
						if !failed && !reflect.DeepEqual(tokens, []string{"first", "second"}) {
							t.Error("stream chunk protocol changed")
						}
					}
					if failed {
						if err == nil || strings.Contains(err.Error(), credential) || !strings.Contains(err.Error(), "502") {
							t.Fatal("status failure or credential redaction changed")
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					want := "complete"
					if mode == "stream" {
						want = "firstsecond"
					}
					if content != want || mode == "tools" && !reflect.DeepEqual(calls, []ParsedToolCall{{Id: "call-1", Name: "inspect", Arguments: `{}`}}) {
						t.Fatal("response content or tool calls changed")
					}
				})
			}
		}
	}
}

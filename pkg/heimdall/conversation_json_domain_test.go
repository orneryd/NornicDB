package heimdall

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"github.com/stretchr/testify/require"
)

// The conversation transports now write through the property-number domain
// owner. These controls keep the previous encoding/json bytes as the contract
// for the typed SSE/HTTP envelopes and for the strings persisted on
// AssistantMessage.attachment_ids/tool_calls, so the migration is a change of
// owner only, not of storage or wire format.
func TestConversationEnvelopeBytesMatchStandardJSON(t *testing.T) {
	for _, tc := range []struct {
		name   string
		value  interface{}
		golden string
	}{
		{
			name:   "http_error_body",
			value:  map[string]string{"message": `model said "no" <stop> & done`},
			golden: `{"message":"model said \"no\" \u003cstop\u003e \u0026 done"}`,
		},
		{
			name: "non_streaming_completion",
			value: ChatResponse{ID: "req-1", Object: "chat.completion", Model: "test-model", Created: math.MaxInt64,
				Choices: []ChatChoice{{Index: 0, Message: &ChatMessage{Role: "assistant", Content: "a \"quote\" <tag> \u2028 end"}, FinishReason: "stop"}}},
			golden: `{"id":"req-1","object":"chat.completion","model":"test-model","created":9223372036854775807,"choices":[{"index":0,"message":{"role":"assistant","content":"a \"quote\" \u003ctag\u003e \u2028 end"},"finish_reason":"stop"}]}`,
		},
		{
			name: "streaming_content_chunk",
			value: ChatResponse{ID: "chunk-1", Object: "chat.completion.chunk", Model: "test-model", Created: 7,
				Choices: []ChatChoice{{Index: 0, Delta: &ChatMessage{Role: "assistant", Content: "hi"}, FinishReason: "stop"}}},
			golden: `{"id":"chunk-1","object":"chat.completion.chunk","model":"test-model","created":7,"choices":[{"index":0,"delta":{"role":"assistant","content":"hi"},"finish_reason":"stop"}]}`,
		},
		{
			name: "streaming_error_chunk",
			value: ChatResponse{ID: "chunk-2", Object: "chat.completion.chunk", Model: "test-model", Created: 8,
				Choices: []ChatChoice{{Index: 0, Delta: &ChatMessage{Content: "Error: boom"}}}},
			golden: `{"id":"chunk-2","object":"chat.completion.chunk","model":"test-model","created":8,"choices":[{"index":0,"delta":{"role":"","content":"Error: boom"}}]}`,
		},
		{
			name:   "usage_event",
			value:  map[string]interface{}{"input_tokens_estimated": 12, "context_window": 8192, "reserved_output_tokens": 512, "estimated": true},
			golden: `{"context_window":8192,"estimated":true,"input_tokens_estimated":12,"reserved_output_tokens":512}`,
		},
		{
			name:   "reasoning_event",
			value:  map[string]string{"text": "thinking <x>"},
			golden: `{"text":"thinking \u003cx\u003e"}`,
		},
		{
			name:   "integer_and_unsigned_property_values",
			value:  map[string]interface{}{"big_int": int64(math.MaxInt64), "big_uint": uint64(math.MaxUint64), "raw": json.RawMessage(`{"a": [1,2]}`)},
			golden: `{"big_int":9223372036854775807,"big_uint":18446744073709551615,"raw":{"a":[1,2]}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			standard, err := json.Marshal(tc.value)
			require.NoError(t, err)
			require.Equal(t, tc.golden, string(standard), "the recorded golden must match the previous owner")
			domainBytes, err := valueconvert.PropertyJSON.Marshal(tc.value)
			require.NoError(t, err)
			require.Equal(t, string(standard), string(domainBytes))

			var oldBody, newBody bytes.Buffer
			require.NoError(t, json.NewEncoder(&oldBody).Encode(tc.value))
			require.NoError(t, valueconvert.PropertyJSON.NewEncoder(&newBody).Encode(tc.value))
			require.Equal(t, oldBody.String(), newBody.String())
			require.Equal(t, tc.golden+"\n", newBody.String())
		})
	}
}

// publishTool writes the inspected tool as a typed SSE event. The event body is
// the same document encoding/json produced before the migration, including the
// raw JSON fragments carried by ConversationTool.Input/Result.
func TestPublishedConversationToolEventMatchesStandardJSON(t *testing.T) {
	tool := ConversationTool{
		ID:         "call-1",
		Name:       "query <n>",
		Status:     "completed",
		Input:      json.RawMessage(`{"cypher":"MATCH (n) RETURN n","limit":1}`),
		Result:     json.RawMessage(`{"rows":[{"n":1}]}`),
		DurationMS: math.MaxInt64,
	}
	const golden = `{"id":"call-1","name":"query \u003cn\u003e","status":"completed","input":{"cypher":"MATCH (n) RETURN n","limit":1},"result":{"rows":[{"n":1}]},"duration_ms":9223372036854775807}`
	standard, err := json.Marshal(tool)
	require.NoError(t, err)
	require.Equal(t, golden, string(standard))

	recorder := httptest.NewRecorder()
	lifecycle := &requestLifecycle{StreamWriter: recorder, StreamFlusher: recorder, tools: []ConversationTool{tool}}
	lifecycle.publishTool(0)
	require.Equal(t, "event: nornic.tool\ndata: "+golden+"\n\n", recorder.Body.String())
}

// Tool transcripts retain their opaque standard-JSON bytes, including whole-FLOAT
// spelling. The persisted document is not reinterpreted as graph property JSON.
func TestSafeToolValueKeepsOpaqueStandardJSONSpelling(t *testing.T) {
	require.Equal(t, json.RawMessage(`1`), safeToolValue(float64(1)))
	require.Equal(t, json.RawMessage(`2`), safeToolValue(float64(2)))
	require.Equal(t, json.RawMessage(`{"big_int":9223372036854775807,"big_uint":18446744073709551615,"v":1}`),
		safeToolValue(map[string]interface{}{"v": float64(1), "big_int": int64(math.MaxInt64), "big_uint": uint64(math.MaxUint64)}))
	require.Equal(t, json.RawMessage(`"a \u003cb\u003e"`), safeToolValue("a <b>"))
	require.Equal(t, json.RawMessage(`null`), safeToolValue(func() {}))
}

// The two strings persisted on a private conversation message must keep the
// exact bytes encoding/json produced, and must read back through both owners
// with the same result: no stored document needs rewriting.
func TestPersistedConversationStringsMatchStandardJSON(t *testing.T) {
	tools := []ConversationTool{{
		ID:         "call-1",
		Name:       "query <n>",
		Status:     "completed",
		Input:      json.RawMessage(`{"cypher":"MATCH (n) RETURN n","limit":1}`),
		Result:     json.RawMessage(`{"rows":[{"n":1}]}`),
		DurationMS: math.MaxInt64,
	}}
	const toolsGolden = `[{"id":"call-1","name":"query \u003cn\u003e","status":"completed","input":{"cypher":"MATCH (n) RETURN n","limit":1},"result":{"rows":[{"n":1}]},"duration_ms":9223372036854775807}]`
	standardTools, err := json.Marshal(tools)
	require.NoError(t, err)
	require.Equal(t, toolsGolden, string(standardTools))
	domainTools, err := valueconvert.PropertyJSON.Marshal(tools)
	require.NoError(t, err)
	require.Equal(t, string(standardTools), string(domainTools))

	var oldTools, newTools []ConversationTool
	require.NoError(t, json.Unmarshal(standardTools, &oldTools))
	require.NoError(t, valueconvert.PropertyJSON.Unmarshal(standardTools, &newTools))
	require.Equal(t, oldTools, newTools)
	reencoded, err := valueconvert.PropertyJSON.Marshal(newTools)
	require.NoError(t, err)
	require.Equal(t, string(standardTools), string(reencoded), "a persisted tool document must round-trip unchanged")

	ids := []string{"attach-1", "attach-<2>"}
	const idsGolden = `["attach-1","attach-\u003c2\u003e"]`
	standardIDs, err := json.Marshal(ids)
	require.NoError(t, err)
	require.Equal(t, idsGolden, string(standardIDs))
	domainIDs, err := valueconvert.PropertyJSON.Marshal(ids)
	require.NoError(t, err)
	require.Equal(t, string(standardIDs), string(domainIDs))

	var oldIDs, newIDs []string
	require.NoError(t, json.Unmarshal(standardIDs, &oldIDs))
	require.NoError(t, valueconvert.PropertyJSON.Unmarshal(standardIDs, &newIDs))
	require.Equal(t, oldIDs, newIDs)
	require.Equal(t, ids, newIDs)

	// A message that was stored without attachments keeps its "null" document.
	standardNil, err := json.Marshal([]string(nil))
	require.NoError(t, err)
	domainNil, err := valueconvert.PropertyJSON.Marshal([]string(nil))
	require.NoError(t, err)
	require.Equal(t, "null", string(standardNil))
	require.Equal(t, string(standardNil), string(domainNil))

	// Both owners reject a corrupted attachment document identically.
	var oldBroken, newBroken []string
	oldErr := json.Unmarshal([]byte(`attach-1`), &oldBroken)
	newErr := valueconvert.PropertyJSON.Unmarshal([]byte(`attach-1`), &newBroken)
	require.Error(t, oldErr)
	require.Error(t, newErr)
	require.Equal(t, oldErr.Error(), newErr.Error())
}

// A chat request is an external Cypher-parameter domain read. Declared float32
// request fields keep their current valid values and their current rejection of
// out-of-range literals; the handler's 400 path is unchanged.
func TestFloatRequestFieldsKeepCurrentContract(t *testing.T) {
	for _, body := range []string{
		`{"model":"m","messages":[{"role":"user","content":"hi"}],"temperature":0.7,"top_p":0.9}`,
		`{"model":"m","messages":[],"temperature":0,"top_p":1}`,
		`{"model":"m","messages":[],"temperature":1.25,"stream":true}`,
		`{"model":"m","messages":[],"temperature":-0.5,"top_p":0.25,"max_tokens":64}`,
	} {
		var standard, domain ChatRequest
		require.NoError(t, json.NewDecoder(strings.NewReader(body)).Decode(&standard))
		require.NoError(t, valueconvert.CypherInputJSON.NewDecoder(strings.NewReader(body)).Decode(&domain))
		require.Equal(t, standard, domain)
	}
	var temperature ChatRequest
	require.NoError(t, valueconvert.CypherInputJSON.NewDecoder(strings.NewReader(`{"model":"m","messages":[],"temperature":0.7}`)).Decode(&temperature))
	require.Equal(t, float32(0.7), temperature.Temperature)

	// No new acceptance: a value outside the float32 range is still refused.
	const outOfRange = `{"model":"m","messages":[],"temperature":3.5e38}`
	var standard, domain ChatRequest
	require.Error(t, json.NewDecoder(strings.NewReader(outOfRange)).Decode(&standard))
	require.Error(t, valueconvert.CypherInputJSON.NewDecoder(strings.NewReader(outOfRange)).Decode(&domain))
}

// A cancelled or truncated request body produces the same error as before, so
// the conversation and chat error responses keep their current contracts.
func TestRequestBodyDecodeCancelErrors(t *testing.T) {
	var standard, domain ChatRequest
	cancelledErr := json.NewDecoder(iotest.ErrReader(context.Canceled)).Decode(&standard)
	domainErr := valueconvert.CypherInputJSON.NewDecoder(iotest.ErrReader(context.Canceled)).Decode(&domain)
	require.Error(t, cancelledErr)
	require.Error(t, domainErr)
	require.True(t, errors.Is(cancelledErr, context.Canceled))
	require.True(t, errors.Is(domainErr, context.Canceled))

	var standardTruncated, domainTruncated ChatRequest
	truncatedErr := json.NewDecoder(strings.NewReader(`{"model":"m","messages":[`)).Decode(&standardTruncated)
	domainTruncatedErr := valueconvert.CypherInputJSON.NewDecoder(strings.NewReader(`{"model":"m","messages":[`)).Decode(&domainTruncated)
	require.Error(t, truncatedErr)
	require.Error(t, domainTruncatedErr)
	require.Equal(t, truncatedErr.Error(), domainTruncatedErr.Error())
}

// The public chat endpoint writes these envelopes and events. Every emitted
// document must still be the byte image encoding/json produces for the value
// the handler built.
func TestChatCompletionsEnvelopesMatchStandardJSON(t *testing.T) {
	const reply = "reply with <tag> & \"quotes\" \u2028 end"
	t.Run("non_streaming", func(t *testing.T) {
		generator := NewMockGenerator("/test/model.gguf")
		generator.generateFunc = func(ctx context.Context, prompt string, params GenerateParams) (string, error) {
			return reply, nil
		}
		manager := newTestManager(generator)
		handler := testHandler(manager, manager.config)
		recorder := httptest.NewRecorder()
		handler.handleChatCompletions(recorder, httptest.NewRequest("POST", "/api/bifrost/chat/completions",
			strings.NewReader(`{"model":"ignored","messages":[{"role":"user","content":"hello"}]}`)))
		require.Equal(t, 200, recorder.Code, recorder.Body.String())
		var response ChatResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
		standard, err := json.Marshal(response)
		require.NoError(t, err)
		require.Equal(t, string(standard)+"\n", recorder.Body.String())
	})

	t.Run("streaming_with_tools", func(t *testing.T) {
		documents := standardSSEDocuments(t, streamingToolChatBody(t, func(ctx context.Context, prompt string, params GenerateParams) (string, error) {
			return reply, nil
		}), 2)
		require.Equal(t, "chat.completion.chunk", documents[0].Object)
		require.Equal(t, "stop", documents[1].Choices[0].FinishReason)
	})

	t.Run("streaming_with_tools_error", func(t *testing.T) {
		documents := standardSSEDocuments(t, streamingToolChatBody(t, func(ctx context.Context, prompt string, params GenerateParams) (string, error) {
			return "", errors.New("provider <down> & out")
		}), 2)
		require.Equal(t, "Error: provider <down> & out", documents[0].Choices[0].Delta.Content)
		require.Equal(t, "stop", documents[1].Choices[0].FinishReason)
	})
}

// streamingToolChatBody drives the public chat endpoint through the tool-capable
// streaming adapter and returns the SSE body it produced.
func streamingToolChatBody(t *testing.T, generate func(context.Context, string, GenerateParams) (string, error)) string {
	t.Helper()
	generator := &toolStreamingGenerator{MockGenerator: NewMockGenerator("/test/model.gguf")}
	generator.generateFunc = generate
	manager := newTestManager(generator)
	require.True(t, manager.SupportsTools(), "the tool-capable adapter owns this test")
	handler := testHandler(manager, manager.config)
	recorder := httptest.NewRecorder()
	handler.handleChatCompletions(recorder, httptest.NewRequest("POST", "/api/bifrost/chat/completions",
		strings.NewReader(`{"model":"ignored","stream":true,"messages":[{"role":"user","content":"hello"}]}`)))
	require.Equal(t, 200, recorder.Code, recorder.Body.String())
	body := recorder.Body.String()
	require.True(t, strings.HasSuffix(body, "data: [DONE]\n\n"), body)
	return body
}

// standardSSEDocuments checks every emitted chunk against encoding/json and
// returns the decoded chunks.
func standardSSEDocuments(t *testing.T, body string, expected int) []ChatResponse {
	t.Helper()
	documents := make([]ChatResponse, 0, expected)
	for _, frame := range strings.Split(strings.TrimSuffix(body, "data: [DONE]\n\n"), "\n\n") {
		if frame == "" {
			continue
		}
		payload, ok := strings.CutPrefix(frame, "data: ")
		require.True(t, ok, frame)
		var chunk ChatResponse
		require.NoError(t, json.Unmarshal([]byte(payload), &chunk))
		standard, err := json.Marshal(chunk)
		require.NoError(t, err)
		require.Equal(t, string(standard), payload)
		documents = append(documents, chunk)
	}
	require.Len(t, documents, expected)
	return documents
}

type toolStreamingGenerator struct {
	*MockGenerator
}

func (g *toolStreamingGenerator) GenerateWithTools(ctx context.Context, messages []ToolRoundMessage, tools []MCPTool, params GenerateParams) (string, []ParsedToolCall, error) {
	prompt := ""
	if len(messages) > 0 {
		prompt = messages[len(messages)-1].Content
	}
	content, err := g.Generate(ctx, prompt, params)
	return content, nil, err
}

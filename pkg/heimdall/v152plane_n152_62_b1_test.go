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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nor202ReportedResponse is the exact action envelope from the NOR-202 report.
// The Cypher parameter ends with a quoted closing brace, which a brace-counting
// extractor mistakes for the end of the object, so the envelope is either
// rejected or truncated before the parameter value is complete.
const nor202ReportedResponse = `{"action":"foo","params":{"query":"RETURN '}'"}}`

// nor202QuotedBraceParamsJSON is one model-emitted parameter object whose strings
// contain both braces, an escaped quote and Windows-style backslashes, nested in
// an inner map. It names the shapes a brace counter or a naive unquote mis-frames.
const nor202QuotedBraceParamsJSON = `{"query":"MATCH (n) RETURN n // } {","nested":{"where":"x { y }","path":"C:\\tmp\\ledger.json"},"note":"say \"hi\" and }"}`

// nor202RecordedRunner is the no-op recording action consumer: it records the
// exact argument object the prompt-based loop dispatches, and never executes a
// query or reaches a provider.
type nor202RecordedRunner = toolIntegerRecordingRunner

// nor202Script is the deterministic generator script behind the prompt-based
// loop. It returns the supplied model responses in order, then "done", and
// counts the rounds the loop actually ran.
type nor202Script struct {
	mu        sync.Mutex
	responses []string
	rounds    int
}

func (s *nor202Script) generate(_ context.Context, _ string, _ GenerateParams) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rounds++
	if len(s.responses) == 0 {
		return "done", nil
	}
	response := s.responses[0]
	s.responses = s.responses[1:]
	return response, nil
}

func (s *nor202Script) roundCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rounds
}

// resetNOR202Subsystem isolates one test from the process-wide action registry.
// Registered actions live in the global subsystem manager, so each test installs
// a fresh manager and restores the previous one on cleanup. Tests using this
// helper must not run in parallel.
func resetNOR202Subsystem(t *testing.T) {
	t.Helper()
	globalManagerMu.Lock()
	previous := globalManager
	globalManager = nil
	globalManagerMu.Unlock()
	t.Cleanup(func() {
		globalManagerMu.Lock()
		installed := globalManager
		globalManagerMu.Unlock()
		if installed != nil && installed != previous {
			if err := installed.StopAll(); err != nil {
				t.Errorf("stop test-owned subsystem: %v", err)
			}
		}
		globalManagerMu.Lock()
		globalManager = previous
		globalManagerMu.Unlock()
	})
}

// registerNOR202NoOpAction registers one controlled recording action. It only
// reports success: the tests never execute a query or contact a provider.
func registerNOR202NoOpAction(name string) {
	RegisterBuiltinAction(ActionFunc{
		Name:        name,
		Description: "Records nothing and executes nothing",
		Category:    "testing",
		Handler: func(ctx ActionContext) (*ActionResult, error) {
			return &ActionResult{Success: true}, nil
		},
	})
}

// nor202RecordedToolCalls returns the argument objects the in-memory tool runner
// received, so a test can assert the exact decoded dispatch values.
func nor202RecordedToolCalls(runner *nor202RecordedRunner) []map[string]interface{} {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return append([]map[string]interface{}(nil), runner.called...)
}

// newNOR202PromptHandler builds the public Bifrost handler behind a prompt-based
// provider (no native tool support), which is the provider shape that owns the
// action-envelope loop.
func newNOR202PromptHandler(t *testing.T, script *nor202Script, runner *nor202RecordedRunner) *Handler {
	t.Helper()
	mockGen := NewMockGenerator("/test/model.gguf")
	mockGen.generateFunc = script.generate
	manager := newTestManager(mockGen)
	require.False(t, manager.SupportsTools(), "the prompt-based loop must own this path")
	handler := testHandler(manager, manager.config)
	handler.SetInMemoryToolRunner(runner)
	return handler
}

// postNOR202Chat drives the public Bifrost chat endpoint and returns the decoded
// OpenAI-compatible response.
func postNOR202Chat(t *testing.T, handler *Handler) ChatResponse {
	t.Helper()
	body, err := json.Marshal(map[string]interface{}{
		"model":    "test-model",
		"messages": []map[string]interface{}{{"role": "user", "content": "run the query"}},
		"stream":   false,
	})
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body)))
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var response ChatResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response), recorder.Body.String())
	require.Len(t, response.Choices, 1, "the chat response must carry exactly one choice")
	require.NotNil(t, response.Choices[0].Message, "the chat response must carry an assistant message")
	return response
}

// TestParseActionEnvelopeReportedQuotedBraceParameter pins the reported envelope
// itself: the quoted closing brace inside the Cypher parameter must stay part of
// the parameter value, so the whole envelope decodes with the exact text.
func TestParseActionEnvelopeReportedQuotedBraceParameter(t *testing.T) {
	action := parseActionEnvelope(nor202ReportedResponse)
	require.NotNil(t, action, "the reported envelope must parse")
	assert.Equal(t, "foo", action.Action)
	assert.Equal(t, map[string]interface{}{"query": "RETURN '}'"}, action.Params)
}

// TestHeimdallPromptLoopDispatchesReportedEnvelope drives the public Bifrost chat
// endpoint through the prompt-based loop with the exact reported envelope and
// proves the action dispatch carries the complete decoded parameter.
func TestHeimdallPromptLoopDispatchesReportedEnvelope(t *testing.T) {
	resetNOR202Subsystem(t)
	registerNOR202NoOpAction("foo")

	script := &nor202Script{responses: []string{nor202ReportedResponse}}
	runner := &nor202RecordedRunner{names: []string{"foo"}}
	handler := newNOR202PromptHandler(t, script, runner)

	response := postNOR202Chat(t, handler)

	assert.Equal(t, "done\n", response.Choices[0].Message.Content)
	assert.Equal(t, []map[string]interface{}{{"query": "RETURN '}'"}}, nor202RecordedToolCalls(runner),
		"the dispatched action arguments must carry the quoted brace inside the parameter value")
	assert.Equal(t, 2, script.roundCount(), "the loop must run the action round and the answer round")
}

// TestHeimdallPromptLoopDispatchesExactBracedParameterValues proves the public
// prompt-loop dispatch decodes string braces, nested maps, escaped quotes and
// backslashes exactly, with no reformatting or truncation.
func TestHeimdallPromptLoopDispatchesExactBracedParameterValues(t *testing.T) {
	resetNOR202Subsystem(t)
	registerNOR202NoOpAction("nor202_record_values")

	script := &nor202Script{responses: []string{`{"action":"nor202_record_values","params":` + nor202QuotedBraceParamsJSON + `}`}}
	runner := &nor202RecordedRunner{names: []string{"nor202_record_values"}}
	handler := newNOR202PromptHandler(t, script, runner)

	postNOR202Chat(t, handler)

	assert.Equal(t, []map[string]interface{}{{
		"query": `MATCH (n) RETURN n // } {`,
		"nested": map[string]interface{}{
			"where": "x { y }",
			"path":  `C:\tmp\ledger.json`,
		},
		"note": `say "hi" and }`,
	}}, nor202RecordedToolCalls(runner))
}

// TestHeimdallPromptLoopDispatchesOrdinaryActionParams proves an ordinary
// registered action with plain parameters still dispatches unchanged.
func TestHeimdallPromptLoopDispatchesOrdinaryActionParams(t *testing.T) {
	resetNOR202Subsystem(t)
	registerNOR202NoOpAction("nor202_plain_action")

	script := &nor202Script{responses: []string{`{"action":"nor202_plain_action","params":{"limit":5,"label":"Person","enabled":true}}`}}
	runner := &nor202RecordedRunner{names: []string{"nor202_plain_action"}}
	handler := newNOR202PromptHandler(t, script, runner)

	postNOR202Chat(t, handler)

	assert.Equal(t, []map[string]interface{}{{
		"limit":   int64(5),
		"label":   "Person",
		"enabled": true,
	}}, nor202RecordedToolCalls(runner))
}

// TestHeimdallPromptLoopPassesPlainTextWithoutDispatching proves a model answer
// without an action envelope stays an ordinary chat answer: no registered action
// runs and the text reaches the client unchanged.
func TestHeimdallPromptLoopPassesPlainTextWithoutDispatching(t *testing.T) {
	resetNOR202Subsystem(t)
	registerNOR202NoOpAction("nor202_record_query")

	const plain = "The graph currently holds 12 nodes."
	script := &nor202Script{responses: []string{plain}}
	runner := &nor202RecordedRunner{names: []string{"nor202_record_query"}}
	handler := newNOR202PromptHandler(t, script, runner)

	response := postNOR202Chat(t, handler)

	assert.Equal(t, plain+"\n", response.Choices[0].Message.Content)
	assert.Empty(t, nor202RecordedToolCalls(runner), "plain text must not dispatch a registered action")
	assert.Equal(t, 1, script.roundCount())
}

// TestHeimdallPromptLoopDoesNotDispatchMalformedEnvelopes proves malformed or
// truncated envelopes keep the existing rejection: the raw model text returns to
// the client, no registered action runs, and no other action is substituted.
func TestHeimdallPromptLoopDoesNotDispatchMalformedEnvelopes(t *testing.T) {
	tests := []struct {
		name     string
		response string
	}{
		{
			name:     "truncated before the parameter string closes",
			response: `{"action":"nor202_record_query","params":{"query":"RETURN '}`,
		},
		{
			name:     "truncated before the envelope closes",
			response: `{"action":"nor202_record_query","params":{"query":"RETURN '}'}`,
		},
		{
			name:     "malformed parameter value",
			response: `{"action":"nor202_record_query","params":{"query":}}`,
		},
		{
			name:     "envelope without an action",
			response: `{"params":{"query":"RETURN '}'"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetNOR202Subsystem(t)
			registerNOR202NoOpAction("nor202_record_query")

			script := &nor202Script{responses: []string{tt.response}}
			runner := &nor202RecordedRunner{names: []string{"nor202_record_query"}}
			handler := newNOR202PromptHandler(t, script, runner)

			response := postNOR202Chat(t, handler)

			assert.Equal(t, strings.TrimSpace(tt.response)+"\n", response.Choices[0].Message.Content,
				"the rejected envelope must return the raw model text")
			assert.Empty(t, nor202RecordedToolCalls(runner), "a malformed envelope must not dispatch an action")
			assert.Equal(t, 1, script.roundCount())
		})
	}
}

// TestHeimdallPromptLoopReportsUnknownActionWithoutSubstitution proves an
// envelope that parses but names an unregistered action is reported, and never
// turns into a dispatch of a different registered action.
func TestHeimdallPromptLoopReportsUnknownActionWithoutSubstitution(t *testing.T) {
	resetNOR202Subsystem(t)
	registerNOR202NoOpAction("nor202_record_query")

	script := &nor202Script{responses: []string{`{"action":"nor202_missing_action","params":{"query":"RETURN '}'"}}`}}
	runner := &nor202RecordedRunner{names: []string{"nor202_record_query"}}
	handler := newNOR202PromptHandler(t, script, runner)

	response := postNOR202Chat(t, handler)

	assert.Contains(t, response.Choices[0].Message.Content, "nor202_missing_action",
		"the response must report the action the model requested")
	assert.NotContains(t, response.Choices[0].Message.Content, `{"action"`,
		"the response must not echo a raw action envelope back as chat text")
	assert.Empty(t, nor202RecordedToolCalls(runner), "an unknown action must not dispatch a registered action")
	assert.Equal(t, 1, script.roundCount())
}

// TestHeimdallPromptLoopDispatchesFirstEnvelopeOnly proves a second envelope in
// the same model turn is ignored: the first envelope decides the action and no
// other registered action runs.
func TestHeimdallPromptLoopDispatchesFirstEnvelopeOnly(t *testing.T) {
	resetNOR202Subsystem(t)
	registerNOR202NoOpAction("nor202_record_query")
	registerNOR202NoOpAction("nor202_other_action")

	response := `{"action":"nor202_record_query","params":{"query":"RETURN '}'"}}` + "\nAlso consider " +
		`{"action":"nor202_other_action","params":{"query":"RETURN '{'"}}`
	script := &nor202Script{responses: []string{response}}
	runner := &nor202RecordedRunner{names: []string{"nor202_record_query", "nor202_other_action"}}
	handler := newNOR202PromptHandler(t, script, runner)

	postNOR202Chat(t, handler)

	assert.Equal(t, []map[string]interface{}{{"query": "RETURN '}'"}}, nor202RecordedToolCalls(runner),
		"only the first envelope may dispatch an action")
}

// TestLiveHeimdallInvokerSendPromptDispatchesReportedEnvelope covers the second
// existing action-response path. LiveHeimdallInvoker.SendPrompt is the
// plugin-facing consumer: pkg/server injects the invoker into each plugin's
// HeimdallInvoker (pkg/server/server.go, pkg/server/heimdall_plugins.go), so a
// plugin reaches it through ctx.Heimdall.SendPrompt, and SendPromptAsync wraps
// it. It has no HTTP route. Before 479eaa373 this method called
// tryParseActionResponse, the second raw brace-counting extractor in NOR-202.
func TestLiveHeimdallInvokerSendPromptDispatchesReportedEnvelope(t *testing.T) {
	resetNOR202Subsystem(t)

	var mu sync.Mutex
	var captured []map[string]interface{}
	RegisterBuiltinAction(ActionFunc{
		Name:        "nor202_invoker_capture",
		Description: "Captures the exact action parameters without executing anything",
		Category:    "testing",
		Handler: func(ctx ActionContext) (*ActionResult, error) {
			mu.Lock()
			defer mu.Unlock()
			captured = append(captured, ctx.Params)
			return &ActionResult{Success: true, Message: "captured"}, nil
		},
	})

	mockGen := NewMockGenerator("/test/model.gguf")
	mockGen.generateFunc = func(_ context.Context, _ string, _ GenerateParams) (string, error) {
		return `{"action":"nor202_invoker_capture","params":{"query":"RETURN '}'"}}`, nil
	}
	invoker := NewLiveHeimdallInvoker(GetSubsystemManager(), mockGen, NewMockBifrost(), &mockDBRouter{}, &mockMetricsReader{})

	result, err := invoker.SendPrompt(context.Background(), "run the query")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Success, "the parsed action must run: %q", result.Message)
	assert.Equal(t, "captured", result.Message, "SendPrompt must invoke the parsed action instead of returning the raw model text")

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []map[string]interface{}{{"query": "RETURN '}'"}}, captured,
		"the invoked action must receive the quoted brace inside the parameter value")
}

// TestLiveHeimdallInvokerSendPromptReturnsRawTextWithoutDispatching proves the
// same consumer keeps its existing rejection for malformed envelopes and plain
// answers: the raw model text is returned and no registered action runs.
func TestLiveHeimdallInvokerSendPromptReturnsRawTextWithoutDispatching(t *testing.T) {
	tests := []struct {
		name     string
		response string
	}{
		{
			name:     "truncated before the envelope closes",
			response: `{"action":"nor202_invoker_capture","params":{"query":"RETURN '}'}`,
		},
		{
			name:     "malformed parameter value",
			response: `{"action":"nor202_invoker_capture","params":{"query":}}`,
		},
		{
			name:     "plain answer",
			response: "The graph currently holds 12 nodes.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetNOR202Subsystem(t)

			var mu sync.Mutex
			invocations := 0
			RegisterBuiltinAction(ActionFunc{
				Name:        "nor202_invoker_capture",
				Description: "Captures the exact action parameters without executing anything",
				Category:    "testing",
				Handler: func(ctx ActionContext) (*ActionResult, error) {
					mu.Lock()
					defer mu.Unlock()
					invocations++
					return &ActionResult{Success: true}, nil
				},
			})

			mockGen := NewMockGenerator("/test/model.gguf")
			mockGen.generateFunc = func(_ context.Context, _ string, _ GenerateParams) (string, error) {
				return tt.response, nil
			}
			invoker := NewLiveHeimdallInvoker(GetSubsystemManager(), mockGen, NewMockBifrost(), &mockDBRouter{}, &mockMetricsReader{})

			result, err := invoker.SendPrompt(context.Background(), "run the query")
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.True(t, result.Success)
			assert.Equal(t, tt.response, result.Message, "the rejected response must return the raw model text")

			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, 0, invocations, "a rejected response must not invoke an action")
		})
	}
}

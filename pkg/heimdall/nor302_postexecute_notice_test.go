package heimdall

// NOR-302 reproduction coverage for the post-execute notification path used by
// late session-memory persistence failures.
//
// The ticket requires the existing post-execute notification path to deliver a
// separate memory-not-saved notice, for both model-response parsers, without
// silently losing the asynchronous outcome.
//
// Both public streaming parsers must flush the action reply while the
// post-execute hook runs, then report its outcome before closing the stream.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	nor302ProbePluginName  = "nor302probe"
	nor302ProbeAction      = "heimdall_" + nor302ProbePluginName + "_memory_write"
	nor302ProbeActionReply = "NOR-302 probe action reply"
	nor302ProbeFailure     = "nor302 injected session memory write failure"
)

var errNOR302ProbeMemory = errors.New(nor302ProbeFailure)

type nor302MemoryFailingRouter struct{ mockDBRouter }

func (nor302MemoryFailingRouter) Query(context.Context, string, string, map[string]interface{}) ([]map[string]interface{}, error) {
	return nil, errNOR302ProbeMemory
}

// The channels let the client read the action result while the hook is still
// running, and then release the hook so it can report the database outcome.
type nor302ProbePlugin struct {
	*MockHeimdallPlugin
	gate     *testfixture.PauseGate
	finished chan struct{}
}

func (p *nor302ProbePlugin) Actions() map[string]ActionFunc {
	return map[string]ActionFunc{
		"memory_write": {
			Description: "Record the probe action result",
			Category:    "coding",
			Handler: func(ActionContext) (*ActionResult, error) {
				return &ActionResult{Success: true, Message: nor302ProbeActionReply}, nil
			},
		},
	}
}

// PostExecute performs the late session-memory write. Its outcome is reported
// through the existing post-execute notification queue.
func (p *nor302ProbePlugin) PostExecute(ctx *PostExecuteContext) {
	if !p.gate.Pause(p.gate.Armed, p.gate.Entered) {
		panic("NOR-302 probe hook called more than once")
	}

	database := p.ctx.Database
	_, failure := database.Query(ctx.Context, database.DefaultDatabaseName(),
		`MERGE (m:HeimdallSessionMemory {scope: 'coding_session'}) SET m.summary = 'recent coding session facts'`, nil)
	if failure != nil {
		ctx.NotifyError("Coding Memory", "Session memory was not saved: "+failure.Error())
	} else {
		ctx.NotifySuccess("Coding Memory", "Session memory saved")
	}
	close(p.finished)
}

func (p *nor302ProbePlugin) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-p.gate.Entered:
	case <-time.After(10 * time.Second):
		t.Fatal("post-execute hook did not start")
	}
}

func (p *nor302ProbePlugin) waitFinished(t *testing.T) {
	t.Helper()
	select {
	case <-p.finished:
	case <-time.After(10 * time.Second):
		t.Errorf("post-execute hook did not finish after release")
	}
}

// newNOR302Probe registers the probe plugin for one parser run and owns the
// hook's async work: the cleanup always releases and joins it.
func newNOR302Probe(t *testing.T) *nor302ProbePlugin {
	t.Helper()
	ResetSubsystemManagerForTests()
	t.Cleanup(ResetSubsystemManagerForTests)

	plugin := &nor302ProbePlugin{
		MockHeimdallPlugin: NewMockPlugin(nor302ProbePluginName),
		gate:               testfixture.NewPauseGate(),
		finished:           make(chan struct{}),
	}
	plugin.gate.Armed.Store(true)
	GetSubsystemManager().SetHostContext(SubsystemHost{
		Config:   DefaultConfig(),
		Database: &nor302MemoryFailingRouter{},
		Bifrost:  &NoOpBifrost{},
	})
	require.NoError(t, GetSubsystemManager().RegisterPlugin(plugin, "", true))
	t.Cleanup(func() {
		plugin.gate.ReleaseWrites()
		plugin.waitFinished(t)
	})
	return plugin
}

type nor302ChatStream struct {
	*http.Response
	cancel context.CancelFunc
	done   <-chan struct{}
}

func serveNOR302Chat(t *testing.T, handler *Handler, model string, conversation bool) nor302ChatStream {
	t.Helper()
	request := ChatRequest{
		Model:    model,
		Stream:   true,
		Messages: []ChatMessage{{Role: "user", Content: "remember the probe"}},
	}
	if conversation {
		request.ConversationID, request.MessageID = "test-thread", "test-message"
	}
	body, err := json.Marshal(request)
	require.NoError(t, err)
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/api/bifrost/chat/completions", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = response.Body.Close() })
	return nor302ChatStream{Response: response, cancel: cancel, done: done}
}

// serveNOR302PromptParser drives the JSON action-envelope parser used by a
// provider without native tool calls.
func serveNOR302PromptParser(t *testing.T) nor302ChatStream {
	t.Helper()
	generator := NewMockGenerator("/test/model.gguf")
	generator.streamFunc = func(_ context.Context, _ string, _ GenerateParams, callback func(string) error) error {
		return callback(`{"action": "` + nor302ProbeAction + `", "params": {}}`)
	}
	manager := newTestManager(generator)
	require.False(t, manager.SupportsTools(), "the prompt parser serves providers without native tool calls")
	return serveNOR302Chat(t, NewHandler(manager, manager.config, &mockDBRouter{}, &mockMetricsReader{}), "test-model", false)
}

func serveNOR302Conversation(t *testing.T) nor302ChatStream {
	t.Helper()
	turn := &turnTestPlugin{MockHeimdallPlugin: NewMockPlugin("nor302-turn")}
	require.NoError(t, GetSubsystemManager().RegisterPlugin(turn, "", true))
	generator := NewMockGenerator("/test/model.gguf")
	var rounds int
	generator.streamFunc = func(_ context.Context, _ string, _ GenerateParams, callback func(string) error) error {
		rounds++
		if rounds == 1 {
			return callback(`{"action": "` + nor302ProbeAction + `", "params": {}}`)
		}
		return callback("probe conversation reply")
	}
	manager := newTestManager(generator)
	return serveNOR302Chat(t, NewHandler(manager, manager.config, &mockDBRouter{}, &mockMetricsReader{}), "test-model", true)
}

// serveNOR302NativeToolParser drives the native tool-call parser.
func serveNOR302NativeToolParser(t *testing.T) nor302ChatStream {
	t.Helper()
	var rounds int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&rounds, 1) == 1 {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"choices": []map[string]interface{}{{
					"message": map[string]interface{}{
						"content": "",
						"tool_calls": []map[string]interface{}{{
							"id":       "call_1",
							"type":     "function",
							"function": map[string]interface{}{"name": nor302ProbeAction, "arguments": "{}"},
						}},
					},
					"finish_reason": "tool_calls",
				}},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"choices": []map[string]interface{}{{
				"message":       map[string]interface{}{"content": "probe answer"},
				"finish_reason": "stop",
			}},
		})
	}))
	t.Cleanup(provider.Close)

	manager, err := NewManager(Config{Enabled: true, Provider: "openai", APIURL: provider.URL, APIKey: "probe-key", Model: "test"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.True(t, manager.SupportsTools(), "the native parser serves tool-capable providers")
	return serveNOR302Chat(t, NewHandler(manager, manager.config, &mockDBRouter{}, &mockMetricsReader{}), "test", false)
}

// TestNOR302PostExecuteFailureNoticeReachesBothParsers asserts acceptance
// criterion 2 for both model-response parsers: the original successful action
// outcome is unchanged and the separate memory-not-saved notice is delivered.
func TestNOR302PostExecuteFailureNoticeReachesBothParsers(t *testing.T) {
	cases := []struct {
		name      string
		serve     func(*testing.T) nor302ChatStream
		wantReply string
	}{
		{name: "json action envelope parser", serve: serveNOR302PromptParser, wantReply: nor302ProbeActionReply},
		{name: "native tool-call parser", serve: serveNOR302NativeToolParser, wantReply: "probe answer"},
		{name: "stored conversation", serve: serveNOR302Conversation, wantReply: "probe conversation reply"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plugin := newNOR302Probe(t)
			response := tc.serve(t)
			require.Equal(t, http.StatusOK, response.StatusCode)
			plugin.waitStarted(t)

			reader := bufio.NewScanner(response.Body)
			var events []string
			var reply strings.Builder
			for reader.Scan() {
				events = append(events, reader.Text())
				payload, ok := strings.CutPrefix(reader.Text(), "data: ")
				if !ok || payload == "[DONE]" {
					continue
				}
				var chunk ChatResponse
				require.NoError(t, json.Unmarshal([]byte(payload), &chunk))
				for _, choice := range chunk.Choices {
					if choice.Delta != nil && choice.Delta.Role != "heimdall" {
						reply.WriteString(choice.Delta.Content)
					}
				}
				if strings.Contains(reply.String(), tc.wantReply) {
					break
				}
			}
			require.NoError(t, reader.Err(), "received stream lines: %q", events)
			require.Contains(t, reply.String(), tc.wantReply, "the action must reply while the hook is still blocked")

			plugin.gate.ReleaseWrites()
			for reader.Scan() {
				events = append(events, reader.Text())
			}
			require.NoError(t, reader.Err())
			body := strings.Join(events, "\n")
			notice := strings.Index(body, "Session memory was not saved")
			done := strings.Index(body, "data: [DONE]")
			assert.GreaterOrEqual(t, notice, 0, body)
			assert.Greater(t, done, notice, body)
			assert.Contains(t, body, nor302ProbeFailure)
			plugin.waitFinished(t)
		})
	}
}

func TestNOR302DisconnectStopsWaitingForPostExecute(t *testing.T) {
	plugin := newNOR302Probe(t)
	stream := serveNOR302PromptParser(t)
	plugin.waitStarted(t)

	reader := bufio.NewScanner(stream.Body)
	for reader.Scan() {
		if strings.Contains(reader.Text(), nor302ProbeActionReply) {
			break
		}
	}
	require.NoError(t, reader.Err())
	require.Contains(t, reader.Text(), nor302ProbeActionReply)

	stream.cancel()
	select {
	case <-stream.done:
	case <-time.After(time.Second * 5):
		t.Fatal("closing the client did not release the response handler")
	}
	plugin.gate.ReleaseWrites()
	plugin.waitFinished(t)
}

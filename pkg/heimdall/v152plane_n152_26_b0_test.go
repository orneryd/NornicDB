package heimdall

// NOR-302 reproduction coverage for the post-execute notification path used by
// late session-memory persistence failures.
//
// The ticket requires the existing post-execute notification path to deliver a
// separate memory-not-saved notice, for both model-response parsers, without
// silently losing the asynchronous outcome.
//
// CallPostExecuteHooks enqueues the hook on a worker pool and returns; the
// streaming callers then drain the notification queue without joining that
// work. These tests gate the probe hook (a deterministic stand-in for a hook
// that performs a database write) on an owned channel, drive both public
// streaming parsers, and assert the failure notice reaches the reply. No sleep
// is used as synchronization.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// nor302MemoryFailingRouter is the public DatabaseRouter the probe hook writes
// through. Every session-memory write fails, as it would against a refusing or
// unhealthy database.
type nor302MemoryFailingRouter struct {
	mu      sync.Mutex
	queries []string
}

func (r *nor302MemoryFailingRouter) DefaultDatabaseName() string { return "default" }

func (r *nor302MemoryFailingRouter) ResolveDatabase(nameOrAlias string) (string, error) {
	if nameOrAlias == "" {
		return "default", nil
	}
	return nameOrAlias, nil
}

func (r *nor302MemoryFailingRouter) ListDatabases() []string { return []string{"default"} }

func (r *nor302MemoryFailingRouter) Query(_ context.Context, _ string, cypher string, _ map[string]interface{}) ([]map[string]interface{}, error) {
	r.mu.Lock()
	r.queries = append(r.queries, cypher)
	r.mu.Unlock()
	if strings.Contains(cypher, "HeimdallSessionMemory") {
		return nil, errNOR302ProbeMemory
	}
	return nil, nil
}

func (r *nor302MemoryFailingRouter) Stats(context.Context, string) (DatabaseStats, error) {
	return DatabaseStats{}, nil
}

func (r *nor302MemoryFailingRouter) Discover(context.Context, string, string, []string, int, int) (*DiscoverResult, error) {
	return &DiscoverResult{}, nil
}

// nor302ProbePlugin persists session memory in PostExecute, as the reported
// coding plugin does, and queues the failure notice the ticket requires. The
// hook is gated so a test can observe the handler's notification drain without
// racing the worker pool.
type nor302ProbePlugin struct {
	router *nor302MemoryFailingRouter

	started  chan struct{}
	release  chan struct{}
	finished chan struct{}
	startOne sync.Once
	relOne   sync.Once

	mu       sync.Mutex
	database DatabaseRouter
	status   SubsystemStatus
}

func newNOR302ProbePlugin() *nor302ProbePlugin {
	return &nor302ProbePlugin{
		router:   &nor302MemoryFailingRouter{},
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		finished: make(chan struct{}),
		status:   StatusUninitialized,
	}
}

func (p *nor302ProbePlugin) Name() string        { return nor302ProbePluginName }
func (p *nor302ProbePlugin) Version() string     { return "1.0.0" }
func (p *nor302ProbePlugin) Type() string        { return PluginTypeHeimdall }
func (p *nor302ProbePlugin) Description() string { return "NOR-302 post-execute notification probe" }

func (p *nor302ProbePlugin) Initialize(ctx SubsystemContext) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.database = ctx.Database
	p.status = StatusReady
	return nil
}

func (p *nor302ProbePlugin) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.status = StatusRunning
	return nil
}

func (p *nor302ProbePlugin) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.status = StatusStopped
	return nil
}

func (p *nor302ProbePlugin) Shutdown() error { return nil }

func (p *nor302ProbePlugin) Status() SubsystemStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.status
}

func (p *nor302ProbePlugin) Health() SubsystemHealth {
	return SubsystemHealth{Status: p.Status(), Healthy: true, LastCheck: time.Now()}
}

func (p *nor302ProbePlugin) Metrics() map[string]interface{} { return map[string]interface{}{} }

func (p *nor302ProbePlugin) Config() map[string]interface{} {
	return map[string]interface{}{"summary_scope": "coding_session"}
}

func (p *nor302ProbePlugin) Configure(map[string]interface{}) error { return nil }

func (p *nor302ProbePlugin) ConfigSchema() map[string]interface{} {
	return map[string]interface{}{"type": "object"}
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

func (p *nor302ProbePlugin) Summary() string { return "NOR-302 post-execute notification probe" }

func (p *nor302ProbePlugin) RecentEvents(int) []SubsystemEvent { return nil }

// PostExecute performs the late session-memory write. Its outcome is reported
// through the existing post-execute notification queue.
func (p *nor302ProbePlugin) PostExecute(ctx *PostExecuteContext) {
	p.startOne.Do(func() { close(p.started) })
	<-p.release

	p.mu.Lock()
	database := p.database
	p.mu.Unlock()

	var failure error
	if database == nil {
		failure = errors.New("no database router")
	} else {
		_, failure = database.Query(ctx.Context, database.DefaultDatabaseName(), `MERGE (m:HeimdallSessionMemory {plugin: $plugin, scope: $scope})
			SET m.summary = $summary, m.updated_at = $updated_at
			RETURN m.summary AS summary`, map[string]interface{}{
			"plugin":     p.Name(),
			"scope":      "coding_session",
			"summary":    "recent coding session facts",
			"updated_at": time.Now().UTC().Format(time.RFC3339Nano),
		})
	}
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
	case <-p.started:
	case <-time.After(10 * time.Second):
		t.Fatal("post-execute hook did not start")
	}
}

func (p *nor302ProbePlugin) releaseHook() {
	p.relOne.Do(func() { close(p.release) })
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

	plugin := newNOR302ProbePlugin()
	GetSubsystemManager().SetHostContext(SubsystemHost{
		Config:   DefaultConfig(),
		Database: plugin.router,
		Bifrost:  &NoOpBifrost{},
	})
	require.NoError(t, GetSubsystemManager().RegisterPlugin(plugin, "", true))
	t.Cleanup(func() {
		plugin.releaseHook()
		plugin.waitFinished(t)
	})
	return plugin
}

func serveNOR302Chat(t *testing.T, handler *Handler, model string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(ChatRequest{
		Model:    model,
		Stream:   true,
		Messages: []ChatMessage{{Role: "user", Content: "remember the probe"}},
	})
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", bytes.NewReader(body)))
	return recorder
}

// serveNOR302PromptParser drives the JSON action-envelope parser used by a
// provider without native tool calls.
func serveNOR302PromptParser(t *testing.T) *httptest.ResponseRecorder {
	t.Helper()
	generator := NewMockGenerator("/test/model.gguf")
	generator.streamFunc = func(_ context.Context, _ string, _ GenerateParams, callback func(string) error) error {
		return callback(`{"action": "` + nor302ProbeAction + `", "params": {}}`)
	}
	manager := newTestManager(generator)
	require.False(t, manager.SupportsTools(), "the prompt parser serves providers without native tool calls")
	return serveNOR302Chat(t, NewHandler(manager, manager.config, &mockDBRouter{}, &mockMetricsReader{}), "test-model")
}

// serveNOR302NativeToolParser drives the native tool-call parser.
func serveNOR302NativeToolParser(t *testing.T) *httptest.ResponseRecorder {
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
	return serveNOR302Chat(t, NewHandler(manager, manager.config, &mockDBRouter{}, &mockMetricsReader{}), "test")
}

// TestNOR302PostExecuteFailureNoticeReachesBothParsers asserts acceptance
// criterion 2 for both model-response parsers: the original successful action
// outcome is unchanged and the separate memory-not-saved notice is delivered.
func TestNOR302PostExecuteFailureNoticeReachesBothParsers(t *testing.T) {
	cases := []struct {
		name      string
		serve     func(*testing.T) *httptest.ResponseRecorder
		wantReply string
	}{
		{name: "json action envelope parser", serve: serveNOR302PromptParser, wantReply: nor302ProbeActionReply},
		{name: "native tool-call parser", serve: serveNOR302NativeToolParser, wantReply: "probe answer"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plugin := newNOR302Probe(t)
			recorder := tc.serve(t)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			plugin.waitStarted(t)

			body := recorder.Body.String()
			// The action already replied; its successful outcome is unchanged.
			assert.Contains(t, body, tc.wantReply,
				"the original successful action outcome must remain unchanged")
			// The hook's own outcome must reach the same reply.
			if assert.Contains(t, body, "Session memory was not saved",
				"AC-2: the post-execute notification path must deliver the separate memory-not-saved failure notice") {
				assert.Contains(t, body, nor302ProbeFailure,
					"the delivered notice must carry the actual persistence failure, not only a generic line")
			}

			plugin.releaseHook()
			plugin.waitFinished(t)
		})
	}
}

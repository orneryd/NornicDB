package heimdall

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type coverageMinimalPlugin struct {
	name string
}

func newCoverageMinimalPlugin(name string) *coverageMinimalPlugin {
	return &coverageMinimalPlugin{name: name}
}

func (p *coverageMinimalPlugin) Name() string        { return p.name }
func (p *coverageMinimalPlugin) Version() string     { return "1.0.0" }
func (p *coverageMinimalPlugin) Type() string        { return PluginTypeHeimdall }
func (p *coverageMinimalPlugin) Description() string { return "coverage test plugin" }
func (p *coverageMinimalPlugin) Initialize(ctx SubsystemContext) error {
	return nil
}
func (p *coverageMinimalPlugin) Start() error            { return nil }
func (p *coverageMinimalPlugin) Stop() error             { return nil }
func (p *coverageMinimalPlugin) Shutdown() error         { return nil }
func (p *coverageMinimalPlugin) Status() SubsystemStatus { return StatusReady }
func (p *coverageMinimalPlugin) Health() SubsystemHealth {
	return SubsystemHealth{Status: StatusReady, Healthy: true}
}
func (p *coverageMinimalPlugin) Metrics() map[string]interface{} {
	return map[string]interface{}{"name": p.name}
}
func (p *coverageMinimalPlugin) Config() map[string]interface{} {
	return map[string]interface{}{"name": p.name}
}
func (p *coverageMinimalPlugin) Configure(settings map[string]interface{}) error {
	return nil
}
func (p *coverageMinimalPlugin) ConfigSchema() map[string]interface{} {
	return map[string]interface{}{"type": "object"}
}
func (p *coverageMinimalPlugin) Actions() map[string]ActionFunc { return map[string]ActionFunc{} }
func (p *coverageMinimalPlugin) Summary() string                { return "coverage summary" }
func (p *coverageMinimalPlugin) RecentEvents(limit int) []SubsystemEvent {
	return []SubsystemEvent{{Type: "info", Message: "recent"}}
}

type coverageReflectFallbackPlugin struct{}

func (p *coverageReflectFallbackPlugin) Name() string        { return "fallback" }
func (p *coverageReflectFallbackPlugin) Version() string     { return "0.0.1" }
func (p *coverageReflectFallbackPlugin) Type() string        { return PluginTypeHeimdall }
func (p *coverageReflectFallbackPlugin) Description() string { return "fallback plugin" }
func (p *coverageReflectFallbackPlugin) Initialize(ctx SubsystemContext) error {
	return errors.New("init failed")
}
func (p *coverageReflectFallbackPlugin) Start() error    { return errors.New("start failed") }
func (p *coverageReflectFallbackPlugin) Stop() error     { return errors.New("stop failed") }
func (p *coverageReflectFallbackPlugin) Shutdown() error { return errors.New("shutdown failed") }
func (p *coverageReflectFallbackPlugin) Status() string  { return "not-a-status" }
func (p *coverageReflectFallbackPlugin) Health() string  { return "not-a-health" }
func (p *coverageReflectFallbackPlugin) Metrics() string { return "not-a-map" }
func (p *coverageReflectFallbackPlugin) Config() string  { return "not-a-config" }
func (p *coverageReflectFallbackPlugin) Configure(settings map[string]interface{}) error {
	return errors.New("configure failed")
}
func (p *coverageReflectFallbackPlugin) ConfigSchema() string { return "not-a-schema" }
func (p *coverageReflectFallbackPlugin) Actions() string      { return "not-actions" }
func (p *coverageReflectFallbackPlugin) Summary() string      { return "fallback summary" }
func (p *coverageReflectFallbackPlugin) RecentEvents(limit int) string {
	return "not-events"
}

type coverageLifecyclePlugin struct {
	*coverageMinimalPlugin
	prePromptFn   func(*PromptContext) error
	preExecuteFn  func(*PreExecuteContext, func(PreExecuteResult))
	postExecuteFn func(*PostExecuteContext)
	synthesisFn   func(*SynthesisContext, func(string))
}

func (p *coverageLifecyclePlugin) PrePrompt(ctx *PromptContext) error {
	if p.prePromptFn != nil {
		return p.prePromptFn(ctx)
	}
	return nil
}

func (p *coverageLifecyclePlugin) PreExecute(ctx *PreExecuteContext, done func(PreExecuteResult)) {
	if p.preExecuteFn != nil {
		p.preExecuteFn(ctx, done)
		return
	}
	done(PreExecuteResult{Continue: true})
}

func (p *coverageLifecyclePlugin) PostExecute(ctx *PostExecuteContext) {
	if p.postExecuteFn != nil {
		p.postExecuteFn(ctx)
	}
}

func (p *coverageLifecyclePlugin) Synthesize(ctx *SynthesisContext, done func(string)) {
	if p.synthesisFn != nil {
		p.synthesisFn(ctx, done)
		return
	}
	done("")
}

type coverageEventPlugin struct {
	*coverageMinimalPlugin
	events chan *DatabaseEvent
}

func (p *coverageEventPlugin) OnDatabaseEvent(event *DatabaseEvent) {
	select {
	case p.events <- event:
	default:
	}
}

type coverageQueryDB struct {
	lastQuery  string
	lastParams map[string]interface{}
	queryRows  []map[string]interface{}
	queryErr   error
	nodeCount  int64
	edgeCount  int64
}

func (d *coverageQueryDB) Query(ctx context.Context, cypher string, params map[string]interface{}) ([]map[string]interface{}, error) {
	d.lastQuery = cypher
	d.lastParams = params
	if d.queryErr != nil {
		return nil, d.queryErr
	}
	return d.queryRows, nil
}

func (d *coverageQueryDB) Stats() interface{} { return "stats" }
func (d *coverageQueryDB) NodeCount() (int64, error) {
	return d.nodeCount, nil
}
func (d *coverageQueryDB) EdgeCount() (int64, error) {
	return d.edgeCount, nil
}

type coverageSearcher struct {
	searchResults []*SemanticSearchResult
	searchErr     error
	edges         map[string][]*GraphEdge
	nodes         map[string]*NodeData
}

type coverageEmbedder struct {
	chunks   []string
	chunkErr error
	embeds   map[string][]float32
	embedErr error
}

func (e *coverageEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	if e.embedErr != nil {
		return nil, e.embedErr
	}
	if e.embeds != nil {
		return e.embeds[text], nil
	}
	return []float32{1, 2, 3}, nil
}

func (e *coverageEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	if e.chunkErr != nil {
		return nil, e.chunkErr
	}
	if e.chunks != nil {
		return e.chunks, nil
	}
	return []string{text}, nil
}

func (s *coverageSearcher) HybridSearch(ctx context.Context, query string, queryEmbedding []float32, labels []string, limit int) ([]*SemanticSearchResult, error) {
	return s.searchResults, nil
}

func (s *coverageSearcher) Search(ctx context.Context, query string, labels []string, limit int) ([]*SemanticSearchResult, error) {
	return s.searchResults, s.searchErr
}

func (s *coverageSearcher) Neighbors(ctx context.Context, nodeID string) ([]string, error) {
	return nil, nil
}

func (s *coverageSearcher) GetEdgesForNode(ctx context.Context, nodeID string) ([]*GraphEdge, error) {
	return s.edges[nodeID], nil
}

func (s *coverageSearcher) GetNode(ctx context.Context, nodeID string) (*NodeData, error) {
	node := s.nodes[nodeID]
	if node == nil {
		return nil, errors.New("not found")
	}
	return node, nil
}

type coverageToolGenerator struct {
	*MockGenerator
	content   string
	toolCalls []ParsedToolCall
	err       error
	called    bool
}

type coverageGPUGenerator struct {
	*MockGenerator
}

func (g *coverageGPUGenerator) UsingGPU() bool { return true }
func (g *coverageGPUGenerator) GPULayers() int { return 4 }

type coverageFailingResponseWriter struct{}

func (w *coverageFailingResponseWriter) Header() http.Header { return http.Header{} }
func (w *coverageFailingResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}
func (w *coverageFailingResponseWriter) WriteHeader(statusCode int) {}
func (w *coverageFailingResponseWriter) Flush()                     {}

func (g *coverageToolGenerator) GenerateWithTools(ctx context.Context, messages []ToolRoundMessage, tools []MCPTool, params GenerateParams) (string, []ParsedToolCall, error) {
	g.called = true
	return g.content, g.toolCalls, g.err
}

type coverageSequenceToolGenerator struct {
	*MockGenerator
	mu     sync.Mutex
	rounds []coverageToolRound
	index  int
}

type coverageToolRound struct {
	content   string
	toolCalls []ParsedToolCall
	err       error
}

func (g *coverageSequenceToolGenerator) GenerateWithTools(ctx context.Context, messages []ToolRoundMessage, tools []MCPTool, params GenerateParams) (string, []ParsedToolCall, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.index >= len(g.rounds) {
		return "", nil, nil
	}
	round := g.rounds[g.index]
	g.index++
	return round.content, round.toolCalls, round.err
}

type coverageInMemoryRunner struct {
	names  []string
	raw    interface{}
	err    error
	called bool
}

func (r *coverageInMemoryRunner) ToolDefinitions() []MCPTool {
	return []MCPTool{{Name: r.names[0], Description: "coverage tool", InputSchema: DefaultActionInputSchema}}
}

func (r *coverageInMemoryRunner) ToolNames() []string { return r.names }

func (r *coverageInMemoryRunner) CallTool(ctx context.Context, name string, args map[string]interface{}, dbName string) (interface{}, error) {
	r.called = true
	return r.raw, r.err
}

func setupHeimdallCoverageGlobals(t *testing.T) {
	t.Helper()

	globalManager = &SubsystemManager{
		plugins:    make(map[string]*LoadedHeimdallPlugin),
		actions:    make(map[string]ActionFunc),
		orderDirty: true,
	}
	globalManager.SetContext(SubsystemContext{
		Config:  DefaultConfig(),
		Bifrost: &NoOpBifrost{},
	})

	globalPostExecutePool = &postExecuteWorkerPool{}
	globalEventDispatcher = &dbEventDispatcher{
		events:       make(chan *DatabaseEvent, 1000),
		done:         make(chan struct{}),
		pluginQueues: make(map[string]*pluginEventQueue),
	}
}

func buildCoverageHeimdallPlugin(t *testing.T, source string) string {
	t.Helper()

	wd, err := os.Getwd()
	require.NoError(t, err)
	repoRoot := filepath.Clean(filepath.Join(wd, "../.."))
	pluginDir, err := os.MkdirTemp(repoRoot, "heimdall-plugin-*")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(pluginDir)
	})

	srcPath := filepath.Join(pluginDir, "main.go")
	soPath := filepath.Join(pluginDir, "plugin.so")
	require.NoError(t, os.WriteFile(srcPath, []byte(source), 0o600))

	cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", soPath, srcPath)
	cmd.Dir = repoRoot
	output, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "failed to build plugin: %s", string(output))
	return soPath
}

func notificationCount(b *MockBifrost) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.notifications)
}

func TestHeimdallCoverage_ReflectPluginAndInvokers(t *testing.T) {
	setupHeimdallCoverageGlobals(t)

	mockPlugin := NewMockPlugin("reflective")
	adapter := &reflectHeimdallPlugin{val: reflect.ValueOf(mockPlugin)}
	ctx := SubsystemContext{Config: DefaultConfig(), Bifrost: &NoOpBifrost{}}

	assert.Equal(t, "reflective", adapter.Name())
	assert.Equal(t, "1.0.0", adapter.Version())
	assert.Equal(t, PluginTypeHeimdall, adapter.Type())
	assert.Equal(t, "Mock plugin for testing", adapter.Description())
	require.NoError(t, adapter.Initialize(ctx))
	require.NoError(t, adapter.Start())
	require.NoError(t, adapter.Configure(map[string]interface{}{"mode": "test"}))
	assert.Equal(t, StatusRunning, adapter.Status())
	assert.True(t, adapter.Health().Healthy)
	assert.Equal(t, 42, adapter.Metrics()["mock_metric"])
	assert.Equal(t, "test", adapter.Config()["mode"])
	assert.Equal(t, "object", adapter.ConfigSchema()["type"])
	assert.Contains(t, adapter.Actions(), "test_action")
	assert.Equal(t, "Mock plugin summary", adapter.Summary())
	assert.Len(t, adapter.RecentEvents(5), 0)
	require.NoError(t, adapter.PrePrompt(&PromptContext{RequestID: "req-1"}))
	done := make(chan PreExecuteResult, 1)
	adapter.PreExecute(&PreExecuteContext{RequestID: "req-1"}, func(r PreExecuteResult) { done <- r })
	assert.True(t, (<-done).Continue)
	adapter.PostExecute(&PostExecuteContext{RequestID: "req-1"})
	require.NoError(t, adapter.Stop())
	require.NoError(t, adapter.Shutdown())

	noHooks := &reflectHeimdallPlugin{val: reflect.ValueOf(newCoverageMinimalPlugin("minimal"))}
	require.NoError(t, noHooks.PrePrompt(&PromptContext{}))
	done = make(chan PreExecuteResult, 1)
	noHooks.PreExecute(&PreExecuteContext{}, func(r PreExecuteResult) { done <- r })
	assert.True(t, (<-done).Continue)
	noHooks.PostExecute(&PostExecuteContext{})

	fallback := &reflectHeimdallPlugin{val: reflect.ValueOf(&coverageReflectFallbackPlugin{})}
	require.ErrorContains(t, fallback.Initialize(ctx), "init failed")
	require.ErrorContains(t, fallback.Start(), "start failed")
	require.ErrorContains(t, fallback.Stop(), "stop failed")
	require.ErrorContains(t, fallback.Shutdown(), "shutdown failed")
	require.ErrorContains(t, fallback.Configure(map[string]interface{}{}), "configure failed")
	assert.Equal(t, StatusError, fallback.Status())
	assert.False(t, fallback.Health().Healthy)
	assert.Nil(t, fallback.Metrics())
	assert.Nil(t, fallback.Config())
	assert.Nil(t, fallback.ConfigSchema())
	assert.Nil(t, fallback.Actions())
	assert.Nil(t, fallback.RecentEvents(1))

	noOp := &NoOpHeimdallInvoker{}
	result, err := noOp.InvokeAction(context.Background(), "anything", nil)
	require.NoError(t, err)
	assert.False(t, result.Success)
	result, err = noOp.SendPrompt(context.Background(), "hello")
	require.NoError(t, err)
	assert.False(t, result.Success)
	result, err = noOp.SendRawPrompt(context.Background(), "hello")
	require.NoError(t, err)
	assert.False(t, result.Success)
	noOp.InvokeActionAsync("ignored", nil)
	noOp.SendPromptAsync("ignored")

	RegisterBuiltinAction(ActionFunc{
		Name:        "heimdall_cov_echo",
		Description: "Coverage echo",
		Category:    "testing",
		Handler: func(ctx ActionContext) (*ActionResult, error) {
			return &ActionResult{
				Success: true,
				Message: fmt.Sprintf("echo:%s", ctx.UserMessage),
				Data:    map[string]interface{}{"params": ctx.Params},
			}, nil
		},
	})

	bifrost := NewMockBifrost()
	invoker := NewLiveHeimdallInvoker(GetSubsystemManager(), nil, bifrost, &mockDBRouter{}, &mockMetricsReader{})
	require.NotNil(t, invoker)

	result, err = invoker.InvokeAction(context.Background(), "heimdall_cov_echo", map[string]interface{}{"k": "v"})
	require.NoError(t, err)
	assert.True(t, result.Success)
	result, err = invoker.SendPrompt(context.Background(), "hello")
	assert.Equal(t, "SLM not available", mustActionResultMessage(t, result, err))
	result, err = invoker.SendRawPrompt(context.Background(), "hello")
	assert.Equal(t, "SLM not available", mustActionResultMessage(t, result, err))

	mockGen := NewMockGenerator("/tmp/model.gguf")
	mockGen.generateFunc = func(ctx context.Context, prompt string, params GenerateParams) (string, error) {
		if strings.Contains(prompt, "direct raw") {
			return "raw-response", nil
		}
		return `{"action":"heimdall_cov_echo","params":{"mode":"json"}}`, nil
	}
	invoker.generator = mockGen

	result, err = invoker.SendPrompt(context.Background(), "hello world")
	require.NoError(t, err)
	assert.True(t, result.Success)
	assert.Contains(t, result.Message, "echo:")

	result, err = invoker.SendRawPrompt(context.Background(), "direct raw")
	require.NoError(t, err)
	assert.Equal(t, "raw-response", result.Message)

	invoker.InvokeActionAsync("heimdall_cov_echo", map[string]interface{}{"mode": "async"})
	require.Eventually(t, func() bool {
		return notificationCount(bifrost) >= 1
	}, time.Second, 10*time.Millisecond)

	invoker.SendPromptAsync("hello async")
	require.Eventually(t, func() bool {
		return notificationCount(bifrost) >= 2
	}, time.Second, 10*time.Millisecond)

	invoker.InvokeActionAsync("heimdall_missing_action", nil)
	require.Eventually(t, func() bool {
		return notificationCount(bifrost) >= 3
	}, time.Second, 10*time.Millisecond)
}

func TestHeimdallCoverage_HooksDispatcherAndActionInvoker(t *testing.T) {
	setupHeimdallCoverageGlobals(t)

	postExecuteCalled := make(chan string, 1)
	eventCalls := make(chan *DatabaseEvent, 4)

	alpha := &coverageLifecyclePlugin{
		coverageMinimalPlugin: newCoverageMinimalPlugin("alpha"),
		prePromptFn: func(ctx *PromptContext) error {
			ctx.AdditionalInstructions += "alpha"
			return errors.New("warn alpha")
		},
		preExecuteFn: func(ctx *PreExecuteContext, done func(PreExecuteResult)) {
			done(PreExecuteResult{
				Continue:       true,
				ModifiedParams: map[string]interface{}{"stage": "alpha"},
			})
		},
		postExecuteFn: func(ctx *PostExecuteContext) {
			postExecuteCalled <- ctx.Action
		},
		synthesisFn: func(ctx *SynthesisContext, done func(string)) {
			done("")
		},
	}
	beta := &coverageLifecyclePlugin{
		coverageMinimalPlugin: newCoverageMinimalPlugin("beta"),
		prePromptFn: func(ctx *PromptContext) error {
			ctx.Cancel("blocked", "beta")
			return nil
		},
		preExecuteFn: func(ctx *PreExecuteContext, done func(PreExecuteResult)) {
			ctx.Cancel("cancelled by beta", "beta")
			done(PreExecuteResult{Continue: true})
		},
		synthesisFn: func(ctx *SynthesisContext, done func(string)) {
			done("custom synthesis")
		},
	}
	eventer := &coverageEventPlugin{
		coverageMinimalPlugin: newCoverageMinimalPlugin("eventer"),
		events:                eventCalls,
	}

	StartEventDispatcher()
	defer StopEventDispatcher()

	manager := GetSubsystemManager()
	require.NoError(t, manager.RegisterPlugin(alpha, "", true))
	require.NoError(t, manager.RegisterPlugin(beta, "", true))
	require.NoError(t, manager.RegisterPlugin(eventer, "", true))

	promptCtx := &PromptContext{
		RequestID:   "req-hooks",
		RequestTime: time.Now(),
		PluginData:  make(map[string]interface{}),
	}
	CallPrePromptHooks(promptCtx)
	assert.True(t, promptCtx.Cancelled())
	assert.Equal(t, "blocked", promptCtx.CancelReason())
	assert.Equal(t, "alpha", promptCtx.AdditionalInstructions)

	preExecCtx := &PreExecuteContext{
		RequestID:   "req-hooks",
		RequestTime: time.Now(),
		Action:      "heimdall_cov_echo",
		Params:      map[string]interface{}{"before": "value"},
	}
	preResult := CallPreExecuteHooks(preExecCtx)
	assert.False(t, preResult.Continue)
	assert.Equal(t, "cancelled by beta", preResult.AbortMessage)
	assert.Equal(t, "alpha", preExecCtx.Params["stage"])

	postCtx := &PostExecuteContext{RequestID: "req-hooks", Action: "act"}
	CallPostExecuteHooks(postCtx)
	select {
	case got := <-postExecuteCalled:
		assert.Equal(t, "act", got)
	case <-time.After(time.Second):
		t.Fatal("expected post execute hook to run")
	}

	synth := CallSynthesisHooks(&SynthesisContext{RequestID: "req-hooks"})
	assert.Equal(t, "custom synthesis", synth)

	RegisterBuiltinAction(ActionFunc{
		Name:        "heimdall_cov_invoke",
		Description: "Invoke coverage action",
		Category:    "testing",
		Handler: func(ctx ActionContext) (*ActionResult, error) {
			return &ActionResult{
				Success: true,
				Message: ctx.UserMessage,
			}, nil
		},
	})

	invoker := NewActionInvoker(&mockDBRouter{}, &mockMetricsReader{})
	result, err := invoker.Invoke(context.Background(), ParsedAction{
		Action: "heimdall_cov_invoke",
		Params: map[string]interface{}{"x": 1},
	}, "from user")
	require.NoError(t, err)
	assert.Equal(t, "from user", result.Message)
	assert.Contains(t, result.Data, "duration_ms")

	EmitDatabaseEvent(&DatabaseEvent{Type: EventDatabaseStarted})
	EmitNodeEvent(EventNodeCreated, "n1", []string{"Person"}, map[string]interface{}{"name": "Ada"})
	EmitRelationshipEvent(EventRelationshipCreated, "r1", "KNOWS", "n1", "n2", nil)
	EmitQueryEvent(EventQueryFailed, "MATCH (n)", map[string]interface{}{"limit": 1}, 10*time.Millisecond, 0, errors.New("boom"))

	var received []*DatabaseEvent
	require.Eventually(t, func() bool {
		for len(received) < 4 {
			select {
			case evt := <-eventCalls:
				received = append(received, evt)
			default:
				return false
			}
		}
		return true
	}, time.Second, 10*time.Millisecond)

	assert.NotZero(t, received[0].Timestamp)
	assert.Equal(t, EventQueryFailed, received[3].Type)
	assert.Equal(t, "boom", received[3].Error)

	StopEventDispatcher()
	EmitDatabaseEvent(&DatabaseEvent{Type: EventDatabaseShutdown})
}

func TestHeimdallCoverage_QueryExecutorAndLoggerHelpers(t *testing.T) {
	db := &coverageQueryDB{
		queryRows: []map[string]interface{}{{"count": int64(42)}},
		nodeCount: 7,
		edgeCount: 3,
	}
	searcher := &coverageSearcher{
		searchResults: []*SemanticSearchResult{
			{
				ID:     "root",
				Labels: []string{"Person"},
				Properties: map[string]interface{}{
					"title":   "Ada",
					"content": strings.Repeat("x", 250),
				},
				Score: 0.98,
			},
		},
		edges: map[string][]*GraphEdge{
			"root": {
				{ID: "e1", Type: "KNOWS", SourceID: "root", TargetID: "friend"},
			},
		},
		nodes: map[string]*NodeData{
			"friend": {
				ID:     "friend",
				Labels: []string{"Person"},
				Properties: map[string]interface{}{
					"title": "Grace",
				},
			},
		},
	}

	exec := NewQueryExecutor(db, time.Second)
	rows, err := exec.Query(context.Background(), "MATCH (n) RETURN n", map[string]interface{}{"limit": 1})
	require.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, "MATCH (n) RETURN n", db.lastQuery)

	stats := exec.Stats()
	assert.Equal(t, int64(7), stats.NodeCount)
	assert.Equal(t, int64(3), stats.RelationshipCount)

	noSearchExec := NewQueryExecutor(db, time.Second)
	_, err = noSearchExec.Discover(context.Background(), "query", nil, 5, 1)
	require.ErrorContains(t, err, "semantic search not available")

	searchExec := NewQueryExecutorWithSearch(db, searcher, nil, time.Second)
	discover, err := searchExec.Discover(context.Background(), "query", []string{"Person"}, 5, 2)
	require.NoError(t, err)
	assert.Equal(t, "keyword", discover.Method)
	require.Len(t, discover.Results, 1)
	assert.Equal(t, "Person", discover.Results[0].Type)
	assert.Len(t, discover.Results[0].Related, 1)
	assert.True(t, strings.HasSuffix(discover.Results[0].ContentPreview, "..."))

	assert.Nil(t, searchExec.getRelatedNodes(context.Background(), "root", 0))

	logger := NewDefaultLogger("coverage")
	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")
	logger.Error("error")
}

func TestHeimdallCoverage_VLLMGeneratorBranches(t *testing.T) {
	_, err := newVLLMGenerator(Config{})
	require.ErrorContains(t, err, "vllm provider requires")

	gen, err := newVLLMGenerator(Config{Model: "mistral"})
	require.NoError(t, err)
	oag := gen.(*openAIGenerator)
	assert.Equal(t, defaultVLLMBaseURL, oag.baseURL)
	assert.Equal(t, "EMPTY", oag.apiKey)
	assert.Equal(t, "mistral", oag.model)
	assert.Equal(t, "openai:mistral", gen.ModelPath())

	gen, err = newVLLMGenerator(Config{Model: "mixtral", APIURL: "http://vllm.local:9000/", APIKey: "sk-test"})
	require.NoError(t, err)
	oag = gen.(*openAIGenerator)
	assert.Equal(t, "http://vllm.local:9000", oag.baseURL)
	assert.Equal(t, "sk-test", oag.apiKey)
	assert.Equal(t, "mixtral", oag.model)
}

func TestHeimdallCoverage_OrderPluginsCycleAndSelfLoop(t *testing.T) {
	selfLoop := NewOrderedMockPlugin("solo", 5, []string{"solo"}, []string{"solo"})
	ordered := orderPlugins(map[string]*LoadedHeimdallPlugin{
		"solo": {Plugin: selfLoop, Builtin: true},
	})
	require.Len(t, ordered, 1)
	assert.Equal(t, "solo", ordered[0].Plugin.Name())

	alpha := NewOrderedMockPlugin("alpha", 10, []string{"beta"}, nil)
	beta := NewOrderedMockPlugin("beta", 5, []string{"alpha"}, nil)
	ordered = orderPlugins(map[string]*LoadedHeimdallPlugin{
		"alpha": {Plugin: alpha, Builtin: true},
		"beta":  {Plugin: beta, Builtin: true},
	})
	require.Len(t, ordered, 2)
	assert.Equal(t, "alpha", ordered[0].Plugin.Name())
	assert.Equal(t, "beta", ordered[1].Plugin.Name())

	assert.Nil(t, orderPlugins(nil))
}

func TestHeimdallCoverage_ModelsEndpointDefaults(t *testing.T) {
	manager := newTestManager(NewMockGenerator("/tmp/model.gguf"))
	handler := NewHandler(manager, Config{Enabled: true}, &mockDBRouter{}, &mockMetricsReader{})
	require.NotNil(t, handler)
	assert.Equal(t, "nornicdb-heimdall", handler.announcedModel())

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var payload map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&payload))
	data := payload["data"].([]interface{})
	require.Len(t, data, 1)
	model := data[0].(map[string]interface{})
	assert.Equal(t, "nornicdb-heimdall", model["id"])

	req = httptest.NewRequest(http.MethodPost, "/v1/models", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHeimdallCoverage_PromptToolBudgetBranches(t *testing.T) {
	ctx := &PromptContext{
		ActionPrompt: strings.Repeat("x", MaxSystemPromptTokens()*8),
		UserMessage:  "test",
	}
	prompt := ctx.BuildFinalPromptForTools()
	assert.Contains(t, prompt, "ACTIONS:")
	assert.NotContains(t, prompt, "RESPONSE RULES:")

	toolCtx := &PromptContext{ExternalTools: []MCPTool{
		{Name: "", Description: "skip me"},
		{Name: "real_tool"},
		{Name: "schema_tool", Description: "schema desc", InputSchema: json.RawMessage(`{"type":"object"}`)},
	}}
	toolsPrompt := toolCtx.externalToolsPrompt()
	assert.NotContains(t, toolsPrompt, "skip me")
	assert.Contains(t, toolsPrompt, "real_tool: No description provided")
	assert.Contains(t, toolsPrompt, "schema_tool: schema desc")
	assert.Contains(t, toolsPrompt, `inputSchema: {"type":"object"}`)

	originalBudget := tokenBudget
	t.Cleanup(func() { tokenBudget = originalBudget })
	budgetCtx := &PromptContext{
		ActionPrompt: "act",
		UserMessage:  strings.Repeat("u", 800),
	}
	systemTokens := budgetCtx.EstimatedSystemTokens()
	tokenBudget = TokenBudget{
		MaxContext: systemTokens + 10,
		MaxSystem:  systemTokens + 100,
		MaxUser:    MaxUserMessageTokens(),
	}
	err := budgetCtx.ValidateTokenBudget()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "total prompt too large")
}

func TestHeimdallCoverage_LocalGeneratorFallbackAndManagerReset(t *testing.T) {
	ResetSubsystemManagerForTests()
	first := GetSubsystemManager()
	RegisterBuiltinAction(ActionFunc{Name: "heimdall_reset_probe", Description: "probe", Handler: func(ctx ActionContext) (*ActionResult, error) {
		return &ActionResult{Success: true}, nil
	}})
	ResetSubsystemManagerForTests()
	second := GetSubsystemManager()
	assert.NotSame(t, first, second)
	_, ok := GetHeimdallAction("heimdall_reset_probe")
	assert.False(t, ok)

	tmpDir := t.TempDir()
	modelPath := filepath.Join(tmpDir, "custom.gguf")
	require.NoError(t, os.WriteFile(modelPath, []byte("model"), 0o600))

	var calls []int
	previous := SetGeneratorLoader(func(modelPath string, gpuLayers, contextSize, batchSize int) (Generator, error) {
		calls = append(calls, gpuLayers)
		assert.Equal(t, 8192, contextSize)
		assert.Equal(t, 2048, batchSize)
		if gpuLayers != 0 {
			return nil, errors.New("gpu unavailable")
		}
		return NewMockGenerator(modelPath), nil
	})
	t.Cleanup(func() { SetGeneratorLoader(previous) })

	gen, resolvedPath, err := loadLocalGenerator(Config{Model: "custom", ModelsDir: tmpDir, GPULayers: 7})
	require.NoError(t, err)
	assert.Equal(t, modelPath, resolvedPath)
	assert.Equal(t, []int{7, 0}, calls)
	assert.Equal(t, modelPath, gen.ModelPath())

	_, _, err = loadLocalGenerator(Config{Model: "missing", ModelsDir: tmpDir})
	require.ErrorContains(t, err, "Heimdall model not found")
}

func TestHeimdallCoverage_PostExecuteAndEventQueueBranches(t *testing.T) {
	postCalled := make(chan string, 1)
	hook := &coverageLifecyclePlugin{
		coverageMinimalPlugin: newCoverageMinimalPlugin("post-hook"),
		postExecuteFn: func(ctx *PostExecuteContext) {
			postCalled <- ctx.Action
		},
	}
	pool := &postExecuteWorkerPool{workers: 1}
	pool.enqueue(postExecuteJob{pluginName: "post-hook", hook: hook, ctx: &PostExecuteContext{Action: "act"}})
	require.Eventually(t, func() bool {
		select {
		case got := <-postCalled:
			return got == "act"
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	close(pool.jobs)

	dropPool := &postExecuteWorkerPool{jobs: make(chan postExecuteJob)}
	require.NotPanics(t, func() {
		dropPool.enqueue(postExecuteJob{pluginName: "drop", hook: hook, ctx: &PostExecuteContext{Action: "drop"}})
	})

	dispatcher := &dbEventDispatcher{pluginQueues: make(map[string]*pluginEventQueue)}
	dispatcher.ensurePluginQueueForPlugin(nil)
	dispatcher.ensurePluginQueueForPlugin(&LoadedHeimdallPlugin{Plugin: newCoverageMinimalPlugin("plain")})
	require.Empty(t, dispatcher.pluginQueues)

	eventCalls := make(chan *DatabaseEvent, 2)
	eventer := &coverageEventPlugin{coverageMinimalPlugin: newCoverageMinimalPlugin("eventer-extra"), events: eventCalls}
	dispatcher.ensurePluginQueueForPlugin(&LoadedHeimdallPlugin{Plugin: eventer})
	dispatcher.ensurePluginQueueForPlugin(&LoadedHeimdallPlugin{Plugin: eventer})
	require.Len(t, dispatcher.pluginQueues, 1)
	dispatcher.enqueueEvent("eventer-extra", eventer, &DatabaseEvent{Type: EventNodeUpdated})
	require.Eventually(t, func() bool {
		select {
		case evt := <-eventCalls:
			return evt.Type == EventNodeUpdated
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	dispatcher.mu.Lock()
	for _, queue := range dispatcher.pluginQueues {
		close(queue.ch)
	}
	dispatcher.mu.Unlock()
}

func TestHeimdallCoverage_BifrostConfirmationNoClients(t *testing.T) {
	b := NewBifrost(Config{BifrostEnabled: true})
	confirmed, err := b.RequestConfirmation("dangerous action")
	require.NoError(t, err)
	assert.False(t, confirmed)
	assert.False(t, b.IsConnected())
	assert.Zero(t, b.ConnectionCount())
}

func TestHeimdallCoverage_BifrostBroadcastClientBranches(t *testing.T) {
	b := NewBifrost(Config{BifrostEnabled: true})
	recorder := httptest.NewRecorder()
	b.RegisterClient("client-1", recorder, recorder)
	require.True(t, b.IsConnected())
	require.Equal(t, 1, b.ConnectionCount())

	require.NoError(t, b.SendMessage("hello"))
	require.Contains(t, recorder.Body.String(), `"type":"message"`)
	require.Contains(t, recorder.Body.String(), `"content":"hello"`)

	confirmed, err := b.RequestConfirmation("drop index")
	require.NoError(t, err)
	require.False(t, confirmed)
	require.Contains(t, recorder.Body.String(), `"type":"confirmation_request"`)

	stats := b.Stats()
	require.Equal(t, true, stats["enabled"])
	require.Equal(t, 1, stats["connection_count"])

	err = b.broadcast(BifrostMessage{Type: "bad", Data: map[string]interface{}{"bad": make(chan int)}})
	require.ErrorContains(t, err, "failed to marshal message")

	b.RegisterClient("client-2", &coverageFailingResponseWriter{}, &coverageFailingResponseWriter{})
	b.UnregisterClient("client-1")
	err = b.Broadcast("will fail")
	require.ErrorContains(t, err, "write failed")
}

func TestHeimdallCoverage_HandlerAutocompleteAndHelpers(t *testing.T) {
	setupHeimdallCoverageGlobals(t)

	mockGen := NewMockGenerator("/tmp/model.gguf")
	mockGen.generateFunc = func(ctx context.Context, prompt string, params GenerateParams) (string, error) {
		return "```cypher\nMATCH (n:Person)\nRETURN n\nLIMIT 25\n```\nThe user is asking for help", nil
	}
	manager := newTestManager(mockGen)
	handler := testHandler(manager, manager.config)
	handler.SetInMemoryToolRunner(nil)
	assert.Nil(t, handler.inMemoryRunner)

	RegisterBuiltinAction(ActionFunc{
		Name:        "heimdall_autocomplete_suggest",
		Description: "Autocomplete coverage action",
		Category:    "testing",
		Handler: func(ctx ActionContext) (*ActionResult, error) {
			return &ActionResult{
				Success: true,
				Data: map[string]interface{}{
					"schema": map[string]interface{}{
						"labels":     []interface{}{"Person"},
						"properties": []interface{}{"name", "email"},
						"relTypes":   []interface{}{"KNOWS"},
					},
					"suggestion": "",
				},
			}, nil
		},
	})

	body := bytes.NewBufferString(`{"query":"MATCH (n:Person"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/bifrost/autocomplete", body)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var payload map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	assert.Equal(t, "MATCH (n:Person)", payload["suggestion"])
	require.Contains(t, payload, "schema")

	req = httptest.NewRequest(http.MethodPost, "/api/bifrost/autocomplete", bytes.NewBufferString(`{"query":""}`))
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Result().StatusCode)

	req = httptest.NewRequest(http.MethodGet, "/api/bifrost/autocomplete", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusMethodNotAllowed, w.Result().StatusCode)

	handler.bifrost = NewBifrost(Config{BifrostEnabled: true})
	w = httptest.NewRecorder()
	handler.sendCancellationResponse(w, "req-1", "pre-execute", "plugin-a", "policy")
	assert.Equal(t, http.StatusOK, w.Result().StatusCode)

	streamRec := httptest.NewRecorder()
	lifecycle := &requestLifecycle{
		requestID:     "req-stream",
		StreamWriter:  streamRec,
		StreamFlusher: streamRec,
		StreamModel:   "test-model",
	}
	handler.sendStreamNotifications(lifecycle, []QueuedNotification{
		{Type: "info", Title: "Info", Message: "hello"},
		{Type: "progress", Title: "Progress", Message: "working"},
	})
	assert.Contains(t, streamRec.Body.String(), "[Heimdall]: ℹ️ Info: hello")
	assert.Contains(t, streamRec.Body.String(), "[Heimdall]: 🔄 Progress: working")

	assert.Equal(t, "No results available.", handler.defaultFormatResponse(nil))
	assert.Equal(t, "Action failed: nope", handler.defaultFormatResponse(&ActionResult{Success: false, Message: "nope"}))
	assert.Equal(t, "plain", handler.defaultFormatResponse(&ActionResult{Success: true, Message: "plain"}))
	assert.Contains(t, handler.defaultFormatResponse(&ActionResult{
		Success: true,
		Message: "structured",
		Data:    map[string]interface{}{"ok": true},
	}), "```json")
}

func TestHeimdallCoverage_SchedulerToolHelpers(t *testing.T) {
	base := NewMockGenerator("/tmp/tools.gguf")
	toolGen := &coverageToolGenerator{
		MockGenerator: base,
		content:       "tool-response",
		toolCalls: []ParsedToolCall{
			{Id: "call-1", Name: "tool.one", Arguments: `{"value":1}`},
		},
	}

	manager := newTestManager(toolGen)
	assert.True(t, manager.SupportsTools())
	assert.Equal(t, "/test/model.gguf", manager.ModelPath())

	content, toolCalls, err := manager.GenerateWithTools(context.Background(), []ToolRoundMessage{
		{Role: "user", Content: "hello"},
	}, []MCPTool{{Name: "tool.one"}}, DefaultGenerateParams())
	require.NoError(t, err)
	assert.True(t, toolGen.called)
	assert.Equal(t, "tool-response", content)
	assert.Len(t, toolCalls, 1)

	manager.closed = true
	_, _, err = manager.GenerateWithTools(context.Background(), nil, nil, DefaultGenerateParams())
	require.ErrorContains(t, err, "closed")

	noGenManager := &Manager{}
	_, _, err = noGenManager.GenerateWithTools(context.Background(), nil, nil, DefaultGenerateParams())
	require.ErrorContains(t, err, "no generator loaded")

	plainManager := newTestManager(NewMockGenerator("/tmp/plain.gguf"))
	_, _, err = plainManager.GenerateWithTools(context.Background(), nil, nil, DefaultGenerateParams())
	require.ErrorContains(t, err, "does not support tools")
}

func TestHeimdallCoverage_ToolLoopAndProviderGenerators(t *testing.T) {
	t.Run("handler tool loop and streaming", func(t *testing.T) {
		setupHeimdallCoverageGlobals(t)

		RegisterBuiltinAction(ActionFunc{
			Name:        "heimdall_cov_tool",
			Description: "Coverage tool action",
			Category:    "testing",
			Handler: func(ctx ActionContext) (*ActionResult, error) {
				return &ActionResult{
					Success: true,
					Message: "tool action ran",
					Data:    map[string]interface{}{"params": ctx.Params},
				}, nil
			},
		})

		seqGen := &coverageSequenceToolGenerator{
			MockGenerator: NewMockGenerator("/tmp/tool-seq.gguf"),
			rounds: []coverageToolRound{
				{
					toolCalls: []ParsedToolCall{
						{Name: "heimdall_cov_tool", Arguments: `{"value":1}`},
					},
				},
				{
					content: "final streamed answer",
				},
			},
		}
		manager := newTestManager(seqGen)
		handler := testHandler(manager, manager.config)
		lifecycle := &requestLifecycle{
			promptCtx: &PromptContext{
				RequestID:   "req-tools",
				RequestTime: time.Now(),
				UserMessage: "run a tool",
				PluginData:  map[string]interface{}{},
			},
			requestID: "req-tools",
			database:  &mockDBRouter{},
			metrics:   &mockMetricsReader{},
		}

		rec := httptest.NewRecorder()
		handler.handleStreamingWithTools(rec, context.Background(), "prompt", DefaultGenerateParams(), "test-model", lifecycle)
		body := rec.Body.String()
		assert.Contains(t, body, "final streamed answer")
		assert.Contains(t, body, "[DONE]")

		runner := &coverageInMemoryRunner{
			names: []string{"memory.store"},
			raw:   map[string]interface{}{"stored": true},
		}
		handler.inMemoryRunner = runner
		inMemoryGen := &coverageSequenceToolGenerator{
			MockGenerator: NewMockGenerator("/tmp/in-memory.gguf"),
			rounds: []coverageToolRound{
				{
					toolCalls: []ParsedToolCall{
						{Id: "tool-1", Name: "memory.store", Arguments: `{"key":"v"}`},
					},
				},
				{
					content: "memory final answer",
				},
			},
		}
		handler.manager = newTestManager(inMemoryGen)
		lifecycle.requestID = "req-memory"
		lifecycle.promptCtx.RequestID = "req-memory"
		lifecycle.promptCtx.RequestTime = time.Now()
		final, err := handler.runAgenticLoopWithTools(context.Background(), lifecycle, "system", "remember this", runner.ToolDefinitions(), DefaultGenerateParams())
		require.NoError(t, err)
		assert.True(t, runner.called)
		assert.Equal(t, "memory final answer", final)

		errorGen := &coverageSequenceToolGenerator{
			MockGenerator: NewMockGenerator("/tmp/error.gguf"),
			rounds: []coverageToolRound{
				{
					toolCalls: []ParsedToolCall{
						{Id: "tool-2", Name: "heimdall_missing_tool", Arguments: `{}`},
					},
				},
				{
					content: "after missing tool",
				},
			},
		}
		handler.manager = newTestManager(errorGen)
		final, err = handler.runAgenticLoopWithTools(context.Background(), lifecycle, "system", "bad tool", []MCPTool{{Name: "heimdall_missing_tool"}}, DefaultGenerateParams())
		require.NoError(t, err)
		assert.Equal(t, "after missing tool", final)

		fallbackGen := &coverageSequenceToolGenerator{
			MockGenerator: NewMockGenerator("/tmp/fallback.gguf"),
		}
		handler.manager = newTestManager(fallbackGen)
		final, err = handler.runAgenticLoopWithTools(context.Background(), lifecycle, "system", "no output", []MCPTool{{Name: "heimdall_cov_tool"}}, DefaultGenerateParams())
		require.NoError(t, err)
		assert.Contains(t, final, "completed the available actions")

		errOnlyGen := &coverageToolGenerator{
			MockGenerator: NewMockGenerator("/tmp/err-only.gguf"),
			err:           errors.New("tool round failed"),
		}
		handler.manager = newTestManager(errOnlyGen)
		rec = httptest.NewRecorder()
		handler.handleStreamingWithTools(rec, context.Background(), "prompt", DefaultGenerateParams(), "test-model", lifecycle)
		assert.Contains(t, rec.Body.String(), "Error: tool round failed")
		assert.Contains(t, rec.Body.String(), "[DONE]")

		assert.True(t, sliceContains([]string{"a", "b"}, "b"))
		assert.False(t, sliceContains([]string{"a", "b"}, "c"))
		logToolResult("req", "tool", time.Millisecond, nil)
		logToolResult("req", "tool", time.Millisecond, errors.New("boom"))
	})

	t.Run("openai tool and stream", func(t *testing.T) {
		var capturedReq openAIChatRequest
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/v1/chat/completions", r.URL.Path)
			require.NoError(t, json.NewDecoder(r.Body).Decode(&capturedReq))
			req := capturedReq
			if req.Stream {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello \"}}]}\n\n")
				_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"world\"},\"finish_reason\":\"stop\"}]}\n\n")
				_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
				return
			}
			require.NotEmpty(t, req.Tools)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"choices": []map[string]interface{}{
					{
						"message": map[string]interface{}{
							"content": "openai content",
							"tool_calls": []map[string]interface{}{
								{
									"id":   "call-1",
									"type": "function",
									"function": map[string]interface{}{
										"name":      "tool.one",
										"arguments": "{\"x\":1}",
									},
								},
							},
						},
					},
				},
			})
		}))
		defer server.Close()

		gen, err := newOpenAIGenerator(Config{
			APIURL: server.URL,
			APIKey: "test-key",
			Model:  "gpt-4o-mini",
		})
		require.NoError(t, err)
		openaiGen := gen.(*openAIGenerator)

		content, toolCalls, err := openaiGen.GenerateWithTools(context.Background(), []ToolRoundMessage{
			{Role: "user", Content: strings.Repeat("x", 32)},
		}, []MCPTool{{Name: "tool.one", Description: "desc", InputSchema: DefaultActionInputSchema}}, DefaultGenerateParams())
		require.NoError(t, err)
		assert.Equal(t, "openai content", content)
		assert.Len(t, toolCalls, 1)

		_, _, err = openaiGen.GenerateWithTools(context.Background(), []ToolRoundMessage{
			{Role: "system", Content: "system"},
			{Role: "user", Content: "first prompt"},
			{Role: "assistant", Content: "", ToolCalls: []ParsedToolCall{{Id: "call-1", Name: "tool.one", Arguments: "{\"x\":1}"}}},
			{Role: "tool", ToolCallID: "call-1", Content: "tool result"},
		}, []MCPTool{{Name: "tool.one", Description: "desc", InputSchema: DefaultActionInputSchema}}, DefaultGenerateParams())
		require.NoError(t, err)
		require.Len(t, capturedReq.Messages, 4)
		assert.Equal(t, "assistant", capturedReq.Messages[2].Role)
		assert.Equal(t, "", capturedReq.Messages[2].Content)
		require.Len(t, capturedReq.Messages[2].ToolCalls, 1)
		assert.Equal(t, "call-1", capturedReq.Messages[2].ToolCalls[0].Id)

		var streamed strings.Builder
		err = openaiGen.GenerateStream(context.Background(), "prompt", DefaultGenerateParams(), func(token string) error {
			streamed.WriteString(token)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, "hello world", streamed.String())

		errServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, `{"error":"context_length_exceeded"}`, http.StatusBadRequest)
		}))
		defer errServer.Close()
		gen, err = newOpenAIGenerator(Config{
			APIURL: errServer.URL,
			APIKey: "test-key",
			Model:  "gpt-4o-mini",
		})
		require.NoError(t, err)
		_, _, err = gen.(*openAIGenerator).GenerateWithTools(context.Background(), []ToolRoundMessage{{Role: "user", Content: "too long"}}, []MCPTool{{Name: "tool.one"}}, DefaultGenerateParams())
		require.ErrorContains(t, err, "context limit exceeded")

		defaultGen, err := newOpenAIGenerator(Config{
			APIKey: "test-key",
			Model:  "local-model.gguf",
		})
		require.NoError(t, err)
		assert.Equal(t, "openai:"+defaultOpenAIModel, defaultGen.ModelPath())

		generateErrServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "bad request", http.StatusBadRequest)
		}))
		defer generateErrServer.Close()
		gen, err = newOpenAIGenerator(Config{
			APIURL: generateErrServer.URL,
			APIKey: "test-key",
			Model:  "gpt-4o-mini",
		})
		require.NoError(t, err)
		_, err = gen.(*openAIGenerator).Generate(context.Background(), "prompt", DefaultGenerateParams())
		require.ErrorContains(t, err, "openai returned 400")

		noChoicesServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = fmt.Fprint(w, `{"choices":[]}`)
		}))
		defer noChoicesServer.Close()
		gen, err = newOpenAIGenerator(Config{
			APIURL: noChoicesServer.URL,
			APIKey: "test-key",
			Model:  "gpt-4o-mini",
		})
		require.NoError(t, err)
		_, err = gen.(*openAIGenerator).Generate(context.Background(), "prompt", DefaultGenerateParams())
		require.ErrorContains(t, err, "no choices")

		badJSONServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = fmt.Fprint(w, `{"choices":[`)
		}))
		defer badJSONServer.Close()
		gen, err = newOpenAIGenerator(Config{
			APIURL: badJSONServer.URL,
			APIKey: "test-key",
			Model:  "gpt-4o-mini",
		})
		require.NoError(t, err)
		_, _, err = gen.(*openAIGenerator).GenerateWithTools(context.Background(), []ToolRoundMessage{{Role: "user", Content: "prompt"}}, []MCPTool{{Name: "tool.one"}}, DefaultGenerateParams())
		require.ErrorContains(t, err, "openai decode")

		callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
			_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
		}))
		defer callbackServer.Close()
		gen, err = newOpenAIGenerator(Config{
			APIURL: callbackServer.URL,
			APIKey: "test-key",
			Model:  "gpt-4o-mini",
		})
		require.NoError(t, err)
		err = gen.(*openAIGenerator).GenerateStream(context.Background(), "prompt", DefaultGenerateParams(), func(token string) error {
			return errors.New("callback failed")
		})
		require.ErrorContains(t, err, "callback failed")
	})

	t.Run("ollama tool and stream", func(t *testing.T) {
		var capturedReq ollamaChatRequest
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/api/chat", r.URL.Path)
			require.NoError(t, json.NewDecoder(r.Body).Decode(&capturedReq))
			req := capturedReq
			if req.Stream {
				w.Header().Set("Content-Type", "application/x-ndjson")
				_, _ = fmt.Fprintln(w, `{"message":{"content":"hello "},"done":false}`)
				_, _ = fmt.Fprintln(w, `{"message":{"content":"ollama"},"done":true}`)
				return
			}
			require.NotEmpty(t, req.Tools)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"message": map[string]interface{}{
					"content": "ollama content",
					"tool_calls": []map[string]interface{}{
						{
							"id":   "call-9",
							"type": "function",
							"function": map[string]interface{}{
								"name":      "tool.two",
								"arguments": "{\"y\":2}",
							},
						},
					},
				},
				"done": true,
			})
		}))
		defer server.Close()

		gen, err := newOllamaGenerator(Config{
			APIURL: server.URL,
			Model:  "llama3.2",
		})
		require.NoError(t, err)
		ollamaGen := gen.(*ollamaGenerator)

		content, toolCalls, err := ollamaGen.GenerateWithTools(context.Background(), []ToolRoundMessage{
			{Role: "user", Content: "hello"},
		}, []MCPTool{{Name: "tool.two", Description: "desc", InputSchema: DefaultActionInputSchema}}, DefaultGenerateParams())
		require.NoError(t, err)
		assert.Equal(t, "ollama content", content)
		assert.Len(t, toolCalls, 1)

		_, _, err = ollamaGen.GenerateWithTools(context.Background(), []ToolRoundMessage{
			{Role: "system", Content: "system"},
			{Role: "user", Content: "first prompt"},
			{Role: "assistant", Content: "", ToolCalls: []ParsedToolCall{{Id: "call-2", Name: "tool.two", Arguments: "{\"y\":2}"}}},
			{Role: "tool", ToolCallID: "call-2", Content: "tool result"},
		}, []MCPTool{{Name: "tool.two", Description: "desc", InputSchema: DefaultActionInputSchema}}, DefaultGenerateParams())
		require.NoError(t, err)
		require.Len(t, capturedReq.Messages, 4)
		assert.Equal(t, "assistant", capturedReq.Messages[2].Role)
		assert.Equal(t, "", capturedReq.Messages[2].Content)
		require.Len(t, capturedReq.Messages[2].ToolCalls, 1)
		assert.Equal(t, "call-2", capturedReq.Messages[2].ToolCalls[0].Id)

		var streamed strings.Builder
		err = ollamaGen.GenerateStream(context.Background(), "prompt", DefaultGenerateParams(), func(token string) error {
			streamed.WriteString(token)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, "hello ollama", streamed.String())

		defaultGen, err := newOllamaGenerator(Config{})
		require.NoError(t, err)
		assert.Equal(t, "ollama:llama3.2", defaultGen.ModelPath())

		errServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "failure", http.StatusInternalServerError)
		}))
		defer errServer.Close()
		gen, err = newOllamaGenerator(Config{APIURL: errServer.URL, Model: "llama3.2"})
		require.NoError(t, err)
		_, err = gen.(*ollamaGenerator).Generate(context.Background(), "prompt", DefaultGenerateParams())
		require.ErrorContains(t, err, "ollama returned 500")

		badJSONServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = fmt.Fprint(w, `{"message":`)
		}))
		defer badJSONServer.Close()
		gen, err = newOllamaGenerator(Config{APIURL: badJSONServer.URL, Model: "llama3.2"})
		require.NoError(t, err)
		_, _, err = gen.(*ollamaGenerator).GenerateWithTools(context.Background(), []ToolRoundMessage{{Role: "user", Content: "prompt"}}, []MCPTool{{Name: "tool.two"}}, DefaultGenerateParams())
		require.ErrorContains(t, err, "ollama decode")

		callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = fmt.Fprintln(w, `{"message":{"content":"hello"},"done":false}`)
		}))
		defer callbackServer.Close()
		gen, err = newOllamaGenerator(Config{APIURL: callbackServer.URL, Model: "llama3.2"})
		require.NoError(t, err)
		err = gen.(*ollamaGenerator).GenerateStream(context.Background(), "prompt", DefaultGenerateParams(), func(token string) error {
			return errors.New("callback failed")
		})
		require.ErrorContains(t, err, "callback failed")
	})
}

func TestHeimdallCoverage_PromptStreamingAndPluginLoading(t *testing.T) {
	t.Run("prompt based loop and non-streaming response", func(t *testing.T) {
		setupHeimdallCoverageGlobals(t)

		RegisterBuiltinAction(ActionFunc{
			Name:        "heimdall_cov_prompt",
			Description: "Coverage prompt action",
			Category:    "testing",
			Handler: func(ctx ActionContext) (*ActionResult, error) {
				return &ActionResult{
					Success: true,
					Message: "prompt action ran",
					Data:    map[string]interface{}{"params": ctx.Params},
				}, nil
			},
		})

		mockGen := NewMockGenerator("/tmp/prompt.gguf")
		var calls int
		mockGen.generateFunc = func(ctx context.Context, prompt string, params GenerateParams) (string, error) {
			calls++
			switch calls {
			case 1:
				return `{"action":"heimdall_cov_prompt","params":{"id":1}}`, nil
			default:
				return "final prompt answer", nil
			}
		}

		manager := newTestManager(mockGen)
		handler := testHandler(manager, manager.config)
		lifecycle := &requestLifecycle{
			promptCtx: &PromptContext{
				RequestID:   "req-prompt",
				RequestTime: time.Now(),
				UserMessage: "do prompt action",
				PluginData:  map[string]interface{}{},
			},
			requestID: "req-prompt",
			database:  &mockDBRouter{},
			metrics:   &mockMetricsReader{},
		}

		final, err := handler.runAgenticLoopPromptBased(context.Background(), lifecycle, "system", "do prompt action", nil, DefaultGenerateParams())
		require.NoError(t, err)
		assert.Equal(t, "final prompt answer", final)

		rec := httptest.NewRecorder()
		handler.handleNonStreamingResponse(rec, context.Background(), "prompt", DefaultGenerateParams(), "test-model", lifecycle)
		assert.Equal(t, http.StatusOK, rec.Result().StatusCode)
		assert.Contains(t, rec.Body.String(), "final prompt answer")

		manager.generator = &ErrorGenerator{generateErr: errors.New("generate failed")}
		rec = httptest.NewRecorder()
		handler.handleNonStreamingResponse(rec, context.Background(), "prompt", DefaultGenerateParams(), "test-model", lifecycle)
		assert.Equal(t, http.StatusInternalServerError, rec.Result().StatusCode)
	})

	t.Run("streaming response action execution", func(t *testing.T) {
		setupHeimdallCoverageGlobals(t)

		RegisterBuiltinAction(ActionFunc{
			Name:        "heimdall_cov_stream",
			Description: "Coverage stream action",
			Category:    "testing",
			Handler: func(ctx ActionContext) (*ActionResult, error) {
				return &ActionResult{
					Success: true,
					Message: "stream action completed",
					Data:    map[string]interface{}{"ok": true},
				}, nil
			},
		})

		mockGen := NewMockGenerator("/tmp/stream.gguf")
		mockGen.streamFunc = func(ctx context.Context, prompt string, params GenerateParams, callback func(string) error) error {
			for _, token := range []string{`{"action":"heimdall_cov_stream","params":{"id":1}}`} {
				if err := callback(token); err != nil {
					return err
				}
			}
			return nil
		}

		manager := newTestManager(mockGen)
		handler := testHandler(manager, manager.config)
		lifecycle := &requestLifecycle{
			promptCtx: &PromptContext{
				RequestID:   "req-streaming",
				RequestTime: time.Now(),
				UserMessage: "do stream action",
				PluginData:  map[string]interface{}{},
			},
			requestID: "req-streaming",
			database:  &mockDBRouter{},
			metrics:   &mockMetricsReader{},
		}

		rec := httptest.NewRecorder()
		handler.handleStreamingResponse(rec, context.Background(), "prompt", DefaultGenerateParams(), "test-model", lifecycle)
		body := rec.Body.String()
		assert.Contains(t, body, "stream action completed")
		assert.Contains(t, body, "[DONE]")

		mockGen.streamFunc = func(ctx context.Context, prompt string, params GenerateParams, callback func(string) error) error {
			return errors.New("stream failed")
		}
		rec = httptest.NewRecorder()
		handler.handleStreamingResponse(rec, context.Background(), "prompt", DefaultGenerateParams(), "test-model", lifecycle)
		assert.Contains(t, rec.Body.String(), `"error": "stream failed"`)
	})

	t.Run("plugin loading errors", func(t *testing.T) {
		setupHeimdallCoverageGlobals(t)

		ctx := SubsystemContext{Config: DefaultConfig(), Bifrost: &NoOpBifrost{}}
		require.NoError(t, LoadHeimdallPluginsFromDir("", ctx))

		tmpDir := t.TempDir()
		require.NoError(t, LoadHeimdallPluginsFromDir(tmpDir, ctx))

		notDir := tmpDir + "/file.txt"
		require.NoError(t, os.WriteFile(notDir, []byte("not a dir"), 0o600))
		require.ErrorContains(t, LoadHeimdallPluginsFromDir(notDir, ctx), "not a directory")

		badPluginDir := t.TempDir()
		badPluginPath := badPluginDir + "/bad.so"
		require.NoError(t, os.WriteFile(badPluginPath, []byte("not a plugin"), 0o600))
		require.NoError(t, LoadHeimdallPluginsFromDir(badPluginDir, ctx))

		_, err := loadHeimdallPluginFromFile(badPluginPath)
		require.ErrorContains(t, err, "open:")

		decorated := decoratePluginOpenError(errors.New("plugin.Open(\"watcher\"): plugin was built with a different version of package github.com/orneryd/nornicdb/pkg/heimdall"))
		require.ErrorContains(t, decorated, "different version of package github.com/orneryd/nornicdb/pkg/heimdall")
		require.ErrorContains(t, decorated, "same Go toolchain")
		require.ErrorContains(t, decorated, runtime.Version())
	})

	t.Run("plugin loading success", func(t *testing.T) {
		setupHeimdallCoverageGlobals(t)

		interfacePlugin := buildCoverageHeimdallPlugin(t, `package main

import "github.com/orneryd/nornicdb/pkg/heimdall"

type pluginImpl struct{}

func (p *pluginImpl) Name() string { return "iface_plugin" }
func (p *pluginImpl) Version() string { return "1.0.0" }
func (p *pluginImpl) Type() string { return heimdall.PluginTypeHeimdall }
func (p *pluginImpl) Description() string { return "iface plugin" }
func (p *pluginImpl) Initialize(ctx heimdall.SubsystemContext) error { return nil }
func (p *pluginImpl) Start() error { return nil }
func (p *pluginImpl) Stop() error { return nil }
func (p *pluginImpl) Shutdown() error { return nil }
func (p *pluginImpl) Status() heimdall.SubsystemStatus { return heimdall.StatusReady }
func (p *pluginImpl) Health() heimdall.SubsystemHealth { return heimdall.SubsystemHealth{Status: heimdall.StatusReady, Healthy: true} }
func (p *pluginImpl) Metrics() map[string]interface{} { return map[string]interface{}{"ok": true} }
func (p *pluginImpl) Config() map[string]interface{} { return map[string]interface{}{} }
func (p *pluginImpl) Configure(settings map[string]interface{}) error { return nil }
func (p *pluginImpl) ConfigSchema() map[string]interface{} { return map[string]interface{}{"type": "object"} }
func (p *pluginImpl) Actions() map[string]heimdall.ActionFunc {
	return map[string]heimdall.ActionFunc{
		"echo": {
			Name: "echo",
			Description: "echo",
			Category: "testing",
			Handler: func(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
				return &heimdall.ActionResult{Success: true, Message: "echo"}, nil
			},
		},
	}
}
func (p *pluginImpl) Summary() string { return "iface summary" }
func (p *pluginImpl) RecentEvents(limit int) []heimdall.SubsystemEvent { return nil }

var Plugin heimdall.HeimdallPlugin = &pluginImpl{}
`)

		reflectPlugin := buildCoverageHeimdallPlugin(t, `package main

import "github.com/orneryd/nornicdb/pkg/heimdall"

type pluginImpl struct{}

func (p *pluginImpl) Name() string { return "reflect_plugin" }
func (p *pluginImpl) Version() string { return "1.0.1" }
func (p *pluginImpl) Type() string { return heimdall.PluginTypeHeimdall }
func (p *pluginImpl) Description() string { return "reflect plugin" }
func (p *pluginImpl) Initialize(ctx heimdall.SubsystemContext) error { return nil }
func (p *pluginImpl) Start() error { return nil }
func (p *pluginImpl) Stop() error { return nil }
func (p *pluginImpl) Shutdown() error { return nil }
func (p *pluginImpl) Status() heimdall.SubsystemStatus { return heimdall.StatusReady }
func (p *pluginImpl) Health() heimdall.SubsystemHealth { return heimdall.SubsystemHealth{Status: heimdall.StatusReady, Healthy: true} }
func (p *pluginImpl) Metrics() map[string]interface{} { return map[string]interface{}{"ok": true} }
func (p *pluginImpl) Config() map[string]interface{} { return map[string]interface{}{} }
func (p *pluginImpl) Configure(settings map[string]interface{}) error { return nil }
func (p *pluginImpl) ConfigSchema() map[string]interface{} { return map[string]interface{}{"type": "object"} }
func (p *pluginImpl) Actions() map[string]heimdall.ActionFunc { return map[string]heimdall.ActionFunc{} }
func (p *pluginImpl) Summary() string { return "reflect summary" }
func (p *pluginImpl) RecentEvents(limit int) []heimdall.SubsystemEvent { return nil }

var Plugin = &pluginImpl{}
`)

		loaded, err := loadHeimdallPluginFromFile(interfacePlugin)
		if err != nil && strings.Contains(err.Error(), "different version of package github.com/orneryd/nornicdb/pkg/heimdall") {
			t.Skip("plugin success path unavailable under instrumented test binary")
		}
		require.NoError(t, err)
		assert.Equal(t, "iface_plugin", loaded.Name())

		loaded, err = loadHeimdallPluginFromFile(reflectPlugin)
		require.NoError(t, err)
		assert.Equal(t, "reflect_plugin", loaded.Name())

		pluginsDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(pluginsDir, "README.txt"), []byte("ignore me"), 0o600))
		interfaceBytes, err := os.ReadFile(interfacePlugin)
		require.NoError(t, err)
		reflectBytes, err := os.ReadFile(reflectPlugin)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(pluginsDir, "iface.so"), interfaceBytes, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(pluginsDir, "reflect.so"), reflectBytes, 0o700))

		ctx := SubsystemContext{Config: DefaultConfig(), Bifrost: &NoOpBifrost{}}
		require.NoError(t, LoadHeimdallPluginsFromDir(pluginsDir, ctx))
		assert.True(t, HeimdallPluginsInitialized())
		assert.NotEmpty(t, ListHeimdallPlugins())
		_, ok := GetHeimdallAction("heimdall_iface_plugin_echo")
		assert.True(t, ok)
	})

	t.Run("prompt loop branches and streaming branches", func(t *testing.T) {
		setupHeimdallCoverageGlobals(t)
		defer setupHeimdallCoverageGlobals(t)

		RegisterBuiltinAction(ActionFunc{
			Name:        "heimdall_cov_maybe_fail",
			Description: "Coverage prompt failure action",
			Category:    "testing",
			Handler: func(ctx ActionContext) (*ActionResult, error) {
				return nil, errors.New("tool exploded")
			},
		})
		RegisterBuiltinAction(ActionFunc{
			Name:        "heimdall_cov_known",
			Description: "Known action",
			Category:    "testing",
			Handler: func(ctx ActionContext) (*ActionResult, error) {
				return &ActionResult{Success: true, Message: "known action"}, nil
			},
		})
		RegisterBuiltinAction(ActionFunc{
			Name:        "memory.store",
			Description: "Memory tool placeholder",
			Category:    "testing",
			Handler: func(ctx ActionContext) (*ActionResult, error) {
				return &ActionResult{Success: true, Message: "memory placeholder"}, nil
			},
		})

		mockGen := NewMockGenerator("/tmp/prompt-branches.gguf")
		call := 0
		mockGen.generateFunc = func(ctx context.Context, prompt string, params GenerateParams) (string, error) {
			call++
			switch call {
			case 1:
				return `{"action":"heimdall_cov_maybe_fail","params":{"id":1}}`, nil
			case 2:
				return "answer after failure", nil
			case 3:
				return `{"action":"heimdall_unknown","params":{}}`, nil
			case 4:
				return `{"action":"memory.store","params":{"note":"remember"}}`, nil
			default:
				return "memory answer", nil
			}
		}

		manager := newTestManager(mockGen)
		handler := testHandler(manager, manager.config)
		lifecycle := &requestLifecycle{
			promptCtx: &PromptContext{
				RequestID:   "req-branches",
				RequestTime: time.Now(),
				UserMessage: "run branchy prompts",
				PluginData:  map[string]interface{}{},
			},
			requestID: "req-branches",
			database:  &mockDBRouter{},
			metrics:   &mockMetricsReader{},
		}

		final, err := handler.runAgenticLoopPromptBased(context.Background(), lifecycle, "system", "please fail then answer", nil, DefaultGenerateParams())
		require.NoError(t, err)
		assert.Equal(t, "answer after failure", final)

		final, err = handler.runAgenticLoopPromptBased(context.Background(), lifecycle, "system", "unknown action", nil, DefaultGenerateParams())
		require.NoError(t, err)
		assert.Contains(t, final, "don't know how to perform the action")

		runner := &coverageInMemoryRunner{
			names: []string{"memory.store"},
			raw:   map[string]interface{}{"saved": true},
		}
		handler.inMemoryRunner = runner
		final, err = handler.runAgenticLoopPromptBased(context.Background(), lifecycle, "system", "use memory", nil, DefaultGenerateParams())
		require.NoError(t, err)
		assert.Equal(t, "memory answer", final)
		assert.True(t, runner.called)

		capturedPrompt := ""
		mockGen.generateFunc = func(ctx context.Context, prompt string, params GenerateParams) (string, error) {
			capturedPrompt = prompt
			return "external tool answer", nil
		}
		lifecycle.promptCtx.ExternalTools = []MCPTool{{
			Name:        "mcp__continue__searchRepo",
			Description: "Search repository context provided by Continue",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"query":{"type":"string"}}}`),
		}}
		final, err = handler.runAgenticLoopPromptBased(context.Background(), lifecycle, "system", "use continue tool", lifecycle.promptCtx.ExternalTools, DefaultGenerateParams())
		require.NoError(t, err)
		assert.Equal(t, "external tool answer", final)
		assert.Contains(t, capturedPrompt, "TOOLS AVAILABLE TO YOU:")
		assert.Contains(t, capturedPrompt, "mcp__continue__searchRepo")
		assert.Contains(t, capturedPrompt, "Search repository context provided by Continue")

		setupHeimdallCoverageGlobals(t)
		abortPlugin := &coverageLifecyclePlugin{
			coverageMinimalPlugin: newCoverageMinimalPlugin("aborter"),
			preExecuteFn: func(ctx *PreExecuteContext, done func(PreExecuteResult)) {
				ctx.NotifyWarning("Blocked", "policy")
				done(PreExecuteResult{Continue: false, AbortMessage: "blocked by hook"})
			},
		}
		require.NoError(t, GetSubsystemManager().RegisterPlugin(abortPlugin, "", true))
		RegisterBuiltinAction(ActionFunc{
			Name:        "heimdall_cov_known",
			Description: "Known action",
			Category:    "testing",
			Handler: func(ctx ActionContext) (*ActionResult, error) {
				return &ActionResult{Success: true, Message: "should not run"}, nil
			},
		})

		streamGen := NewMockGenerator("/tmp/stream-branches.gguf")
		streamGen.streamFunc = func(ctx context.Context, prompt string, params GenerateParams, callback func(string) error) error {
			switch prompt {
			case "plain":
				if err := callback("hello "); err != nil {
					return err
				}
				return callback("world")
			case "unknown":
				return callback(`{"action":"heimdall_missing_action","params":{}}`)
			default:
				return callback(`{"action":"heimdall_cov_known","params":{"id":1}}`)
			}
		}

		handler = testHandler(newTestManager(streamGen), Config{Enabled: true, Model: "test-model", BifrostEnabled: true})
		lifecycle = &requestLifecycle{
			promptCtx: &PromptContext{
				RequestID:   "req-stream-branches",
				RequestTime: time.Now(),
				UserMessage: "stream branches",
				PluginData:  map[string]interface{}{},
			},
			requestID: "req-stream-branches",
			database:  &mockDBRouter{},
			metrics:   &mockMetricsReader{},
		}

		rec := httptest.NewRecorder()
		handler.handleStreamingResponse(rec, context.Background(), "plain", DefaultGenerateParams(), "test-model", lifecycle)
		assert.Contains(t, rec.Body.String(), "hello ")
		assert.Contains(t, rec.Body.String(), "world")
		assert.Contains(t, rec.Body.String(), "[DONE]")

		rec = httptest.NewRecorder()
		handler.handleStreamingResponse(rec, context.Background(), "unknown", DefaultGenerateParams(), "test-model", lifecycle)
		assert.Contains(t, rec.Body.String(), "don't know how to perform the action")
		assert.Contains(t, rec.Body.String(), "[DONE]")

		rec = httptest.NewRecorder()
		handler.handleStreamingResponse(rec, context.Background(), "known", DefaultGenerateParams(), "test-model", lifecycle)
		assert.Contains(t, rec.Body.String(), "[Heimdall]: ⚠️ Blocked: policy")
		assert.Contains(t, rec.Body.String(), "blocked by hook")
		assert.Contains(t, rec.Body.String(), "[DONE]")

		notifyHandler := testHandler(newTestManager(NewMockGenerator("/tmp/notify.gguf")), Config{Enabled: true, Model: "test-model"})
		notifyRec := httptest.NewRecorder()
		notifyLifecycle := &requestLifecycle{
			requestID:     "req-notifs",
			StreamWriter:  notifyRec,
			StreamFlusher: notifyRec,
			StreamModel:   "test-model",
		}
		notifyHandler.sendStreamNotifications(notifyLifecycle, []QueuedNotification{
			{Type: "error", Title: "Error", Message: "bad"},
			{Type: "warning", Title: "Warn", Message: "careful"},
			{Type: "success", Title: "Success", Message: "done"},
		})
		assert.Contains(t, notifyRec.Body.String(), "❌ Error: bad")
		assert.Contains(t, notifyRec.Body.String(), "⚠️ Warn: careful")
		assert.Contains(t, notifyRec.Body.String(), "✅ Success: done")

		assert.True(t, looksLikeLocalModel("model.gguf"))
		assert.False(t, looksLikeLocalModel("gpt-4o-mini"))
		assert.Equal(t, "Label", getLabelType([]string{"Label"}))
		assert.Equal(t, "", getLabelType(nil))
		assert.Equal(t, "", getStringProp(nil, "k"))
		assert.Equal(t, "value", getStringProp(map[string]interface{}{"k": "value"}, "k"))
		assert.Equal(t, "", getStringProp(map[string]interface{}{"k": 42}, "k"))
		assert.NotEmpty(t, truncateContentToTokenEstimate(strings.Repeat("z", 5000), 100))
	})
}

func TestHeimdallCoverage_StreamingResponseExtraBranches(t *testing.T) {
	t.Run("cancelled pre-execute stream", func(t *testing.T) {
		setupHeimdallCoverageGlobals(t)
		defer setupHeimdallCoverageGlobals(t)

		cancelPlugin := &coverageLifecyclePlugin{
			coverageMinimalPlugin: newCoverageMinimalPlugin("canceler"),
			preExecuteFn: func(ctx *PreExecuteContext, done func(PreExecuteResult)) {
				ctx.Cancel("policy denied", "canceler")
				done(PreExecuteResult{Continue: true})
			},
		}
		require.NoError(t, GetSubsystemManager().RegisterPlugin(cancelPlugin, "", true))
		RegisterBuiltinAction(ActionFunc{
			Name:        "heimdall_cov_cancel",
			Description: "cancel action",
			Category:    "testing",
			Handler: func(ctx ActionContext) (*ActionResult, error) {
				return &ActionResult{Success: true, Message: "should not run"}, nil
			},
		})

		mockGen := NewMockGenerator("/tmp/cancel-stream.gguf")
		mockGen.streamFunc = func(ctx context.Context, prompt string, params GenerateParams, callback func(string) error) error {
			return callback(`{"action":"heimdall_cov_cancel","params":{}}`)
		}

		handler := testHandler(newTestManager(mockGen), Config{Enabled: true, Model: "test-model", BifrostEnabled: true})
		lifecycle := &requestLifecycle{
			promptCtx: &PromptContext{
				RequestID:   "req-cancel-stream",
				RequestTime: time.Now(),
				UserMessage: "cancel this",
				PluginData:  map[string]interface{}{},
			},
			requestID: "req-cancel-stream",
			database:  &mockDBRouter{},
			metrics:   &mockMetricsReader{},
		}

		rec := httptest.NewRecorder()
		handler.handleStreamingResponse(rec, context.Background(), "cancel", DefaultGenerateParams(), "test-model", lifecycle)
		assert.Contains(t, rec.Body.String(), "Request cancelled by canceler: policy denied")
		assert.Contains(t, rec.Body.String(), "[DONE]")
	})

	t.Run("in-memory streaming with synthesis and notifications", func(t *testing.T) {
		setupHeimdallCoverageGlobals(t)
		defer setupHeimdallCoverageGlobals(t)

		hookPlugin := &coverageLifecyclePlugin{
			coverageMinimalPlugin: newCoverageMinimalPlugin("synthesizer"),
			postExecuteFn: func(ctx *PostExecuteContext) {
				ctx.NotifySuccess("Stored", "memory saved")
			},
			synthesisFn: func(ctx *SynthesisContext, done func(string)) {
				done("custom synthesized reply")
			},
		}
		require.NoError(t, GetSubsystemManager().RegisterPlugin(hookPlugin, "", true))
		RegisterBuiltinAction(ActionFunc{
			Name:        "memory.store",
			Description: "memory tool",
			Category:    "testing",
			Handler: func(ctx ActionContext) (*ActionResult, error) {
				return &ActionResult{Success: true, Message: "memory builtin"}, nil
			},
		})

		mockGen := NewMockGenerator("/tmp/memory-stream.gguf")
		mockGen.streamFunc = func(ctx context.Context, prompt string, params GenerateParams, callback func(string) error) error {
			return callback(`{"action":"memory.store","params":{"k":"v"}}`)
		}

		handler := testHandler(newTestManager(mockGen), Config{Enabled: true, Model: "test-model", BifrostEnabled: true})
		handler.inMemoryRunner = &coverageInMemoryRunner{
			names: []string{"memory.store"},
			raw:   map[string]interface{}{"saved": true},
		}
		lifecycle := &requestLifecycle{
			promptCtx: &PromptContext{
				RequestID:   "req-memory-stream",
				RequestTime: time.Now(),
				UserMessage: "save memory",
				PluginData:  map[string]interface{}{},
			},
			requestID: "req-memory-stream",
			database:  &mockDBRouter{},
			metrics:   &mockMetricsReader{},
		}

		rec := httptest.NewRecorder()
		handler.handleStreamingResponse(rec, context.Background(), "memory", DefaultGenerateParams(), "test-model", lifecycle)
		body := rec.Body.String()
		assert.Contains(t, body, "custom synthesized reply")
		assert.Contains(t, body, "[DONE]")
	})
}

func mustActionResultMessage(t *testing.T, result *ActionResult, err error) string {
	t.Helper()
	require.NoError(t, err)
	require.NotNil(t, result)
	return result.Message
}

func TestCoverageExtra_NoOpAsyncAndDefaultLogger(t *testing.T) {
	noop := &NoOpHeimdallInvoker{}
	noop.InvokeActionAsync("ignored", map[string]interface{}{"k": "v"})
	noop.SendPromptAsync("ignored")

	res, err := noop.InvokeAction(context.Background(), "ignored", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.False(t, res.Success)

	logger := NewDefaultLogger("cov")
	logger.Debug("debug", "k", "v")
	logger.Info("info")
	logger.Warn("warn")
	logger.Error("error")
}

func TestHeimdallCoverage_HandlerToolAndPromptHelpers(t *testing.T) {
	sanitized := sanitizeAssistantResponse(" answer <|im_start|>assistant leak")
	require.Equal(t, "answer\n", sanitized)
	require.Empty(t, sanitizeAssistantResponse("   "))
	require.Equal(t, "answer\n", sanitizeAssistantResponse("answer<|start_of_turn|>model"))

	require.True(t, looksLikeActionEnvelopePrefix(`{"action":"heimdall_status","params":{}}`))
	require.True(t, looksLikeActionEnvelopePrefix(`prefix {"action":"heimdall_status"`))
	require.False(t, looksLikeActionEnvelopePrefix(""))
	require.False(t, looksLikeActionEnvelopePrefix("plain text"))

	schema := json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}}}`)
	converted := chatRequestToolsToMCPTools([]ChatToolDefinition{
		{Type: "function", Function: ChatToolDefinitionFunction{Name: "lookup", Description: " Lookup ", Parameters: schema}},
		{Type: "", Function: ChatToolDefinitionFunction{Name: "fallback"}},
		{Type: "custom", Function: ChatToolDefinitionFunction{Name: "skip"}},
		{Type: "function", Function: ChatToolDefinitionFunction{Name: "   "}},
	})
	require.Len(t, converted, 2)
	require.Equal(t, "lookup", converted[0].Name)
	require.Equal(t, "Lookup", converted[0].Description)
	require.Equal(t, schema, converted[0].InputSchema)
	require.Equal(t, "fallback", converted[1].Name)
	require.JSONEq(t, string(DefaultActionInputSchema), string(converted[1].InputSchema))
	require.Nil(t, chatRequestToolsToMCPTools(nil))

	merged := mergeMCPTools(
		[]MCPTool{{Name: " a ", Description: "first"}, {Name: "", Description: "skip"}},
		[]MCPTool{{Name: "a", Description: "duplicate"}, {Name: "b", Description: "second"}},
	)
	require.Len(t, merged, 2)
	require.Equal(t, " a ", merged[0].Name)
	require.Equal(t, "b", merged[1].Name)

	handler := &Handler{}
	require.Equal(t, "No results available.", handler.defaultFormatResponse(nil))
	require.Equal(t, "Action failed: nope", handler.defaultFormatResponse(&ActionResult{Success: false, Message: "nope"}))
	require.Equal(t, "ok", handler.defaultFormatResponse(&ActionResult{Success: true, Message: "ok"}))
	formatted := handler.defaultFormatResponse(&ActionResult{Success: true, Message: "ok", Data: map[string]interface{}{"count": 2}})
	require.Contains(t, formatted, "```json")
	require.Contains(t, formatted, `"count": 2`)

	prompt := &PromptContext{
		ActionPrompt: "- heimdall_status: status",
		UserMessage:  "What is status?",
		ExternalTools: []MCPTool{
			{Name: "external_lookup", Description: " Lookup external ", InputSchema: schema},
			{Name: "   ", Description: "skip"},
			{Name: "external_empty"},
		},
	}
	require.Contains(t, prompt.externalToolsPrompt(), "external_lookup: Lookup external")
	require.Contains(t, prompt.externalToolsPrompt(), "external_empty: No description provided")
	fullPrompt := prompt.buildFullPromptForTools()
	require.Contains(t, fullPrompt, "AVAILABLE ACTIONS")
	require.Contains(t, fullPrompt, "EXTERNAL CLIENT TOOLS")
}

func TestHeimdallCoverage_OpenAITrimAndTruncateHelpers(t *testing.T) {
	short := []ToolRoundMessage{{Role: "system", Content: "s"}, {Role: "user", Content: "u"}}
	require.Equal(t, short, trimMessagesForContext(short, 1))

	longTool := strings.Repeat("tool-result ", openAIMaxTokensPerToolResult*2)
	messages := []ToolRoundMessage{
		{Role: "system", Content: "system"},
		{Role: "user", Content: "user"},
		{Role: "assistant", Content: strings.Repeat("old assistant ", 200)},
		{Role: "tool", Content: longTool},
		{Role: "assistant", Content: "recent"},
	}
	trimmed := trimMessagesForContext(messages, 128)
	require.Equal(t, "system", trimmed[0].Content)
	require.Equal(t, "user", trimmed[1].Content)
	require.NotContains(t, fmt.Sprint(trimmed), "old assistant")

	toolOnly := []ToolRoundMessage{
		{Role: "system", Content: "system"},
		{Role: "user", Content: "user"},
		{Role: "tool", Content: longTool},
	}
	trimmedToolOnly := trimMessagesForContext(toolOnly, 1)
	require.Len(t, trimmedToolOnly, 3)
	require.Contains(t, trimmedToolOnly[2].Content, "[Truncated for context limit.]")

	underLimit := "small"
	require.Equal(t, underLimit, truncateForOpenAI(underLimit))
	overLimit := strings.Repeat("a", openAIMaxContentPerMessage+128)
	truncated := truncateForOpenAI(overLimit)
	require.LessOrEqual(t, len(truncated), openAIMaxContentPerMessage)
	require.Contains(t, truncated, "[Content truncated for API limit;")
}

func TestHeimdallCoverage_QueryExecutorDiscoverBranches(t *testing.T) {
	executor := NewQueryExecutorWithSearch(&coverageQueryDB{}, nil, nil, time.Second)
	_, err := executor.Discover(context.Background(), "graph", nil, 5, 1)
	require.ErrorContains(t, err, "semantic search not available")

	searcher := &coverageSearcher{
		searchResults: []*SemanticSearchResult{{
			ID:     "doc-1",
			Labels: []string{"Document"},
			Properties: map[string]interface{}{
				"title":   "Doc One",
				"content": strings.Repeat("content ", 40),
			},
			Score: 0.7,
		}},
		edges: map[string][]*GraphEdge{
			"doc-1": {{SourceID: "doc-1", TargetID: "doc-2", Type: "LINKS"}},
			"doc-2": {{SourceID: "doc-3", TargetID: "doc-2", Type: "MENTIONS"}},
		},
		nodes: map[string]*NodeData{
			"doc-2": {ID: "doc-2", Labels: []string{"File"}, Properties: map[string]interface{}{"title": "Doc Two"}},
			"doc-3": {ID: "doc-3", Labels: []string{"Person"}, Properties: map[string]interface{}{"title": "Doc Three"}},
		},
	}

	executor = NewQueryExecutorWithSearch(&coverageQueryDB{nodeCount: 2, edgeCount: 1}, searcher, nil, time.Second)
	stats := executor.Stats()
	require.Equal(t, int64(2), stats.NodeCount)
	require.Equal(t, int64(1), stats.RelationshipCount)
	result, err := executor.Discover(context.Background(), "graph", []string{"Document"}, 3, 2)
	require.NoError(t, err)
	require.Equal(t, "keyword", result.Method)
	require.Equal(t, 1, result.Total)
	require.Equal(t, "Document", result.Results[0].Type)
	require.Equal(t, "Doc One", result.Results[0].Title)
	require.Contains(t, result.Results[0].ContentPreview, "...")
	require.Len(t, result.Results[0].Related, 2)
	require.Equal(t, "outgoing", result.Results[0].Related[0].Direction)
	require.Equal(t, "incoming", result.Results[0].Related[1].Direction)

	chunkErrExecutor := NewQueryExecutorWithSearch(&coverageQueryDB{}, searcher, &coverageEmbedder{chunkErr: errors.New("chunk failed")}, time.Second)
	_, err = chunkErrExecutor.Discover(context.Background(), "graph", nil, 3, 1)
	require.ErrorContains(t, err, "chunk failed")

	vectorExecutor := NewQueryExecutorWithSearch(&coverageQueryDB{}, searcher, &coverageEmbedder{
		chunks: []string{"chunk one", "chunk two"},
		embeds: map[string][]float32{"chunk one": {1}, "chunk two": {2}},
	}, time.Second)
	vectorResult, err := vectorExecutor.Discover(context.Background(), "graph", nil, 1, 1)
	require.NoError(t, err)
	require.Equal(t, "vector", vectorResult.Method)
	require.Equal(t, 1, vectorResult.Total)
	require.Equal(t, 0.7, vectorResult.Results[0].Similarity)

	fallbackExecutor := NewQueryExecutorWithSearch(&coverageQueryDB{}, searcher, &coverageEmbedder{embedErr: errors.New("embed failed")}, time.Second)
	fallbackResult, err := fallbackExecutor.Discover(context.Background(), "graph", nil, 3, 0)
	require.NoError(t, err)
	require.Equal(t, "keyword", fallbackResult.Method)

	errorSearcher := &coverageSearcher{searchErr: errors.New("search failed")}
	errorExecutor := NewQueryExecutorWithSearch(&coverageQueryDB{}, errorSearcher, nil, time.Second)
	_, err = errorExecutor.Discover(context.Background(), "graph", nil, 3, 0)
	require.ErrorContains(t, err, "search failed")
}

func TestHeimdallCoverage_NewManagerProviderBranches(t *testing.T) {
	disabled, err := NewManager(Config{Enabled: false})
	require.NoError(t, err)
	require.Nil(t, disabled)

	providerName := "coverage-provider"
	oldFactory, hadFactory := heimdallProviderFactories[providerName]
	RegisterHeimdallProvider(providerName, func(cfg Config) (Generator, error) {
		return &coverageGPUGenerator{MockGenerator: NewMockGenerator("provider:model")}, nil
	})
	t.Cleanup(func() {
		if hadFactory {
			heimdallProviderFactories[providerName] = oldFactory
		} else {
			delete(heimdallProviderFactories, providerName)
		}
	})

	manager, err := NewManager(Config{Enabled: true, Provider: "  COVERAGE-PROVIDER  ", MaxTokens: 128})
	require.NoError(t, err)
	require.NotNil(t, manager)
	require.Equal(t, "provider:model", manager.ModelPath())
	require.True(t, manager.lastUsed.After(time.Time{}))

	errorProvider := "coverage-provider-error"
	oldErrorFactory, hadErrorFactory := heimdallProviderFactories[errorProvider]
	RegisterHeimdallProvider(errorProvider, func(cfg Config) (Generator, error) {
		return nil, errors.New("provider failed")
	})
	t.Cleanup(func() {
		if hadErrorFactory {
			heimdallProviderFactories[errorProvider] = oldErrorFactory
		} else {
			delete(heimdallProviderFactories, errorProvider)
		}
	})
	_, err = NewManager(Config{Enabled: true, Provider: errorProvider})
	require.ErrorContains(t, err, "provider failed")

	tmpDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "unknown.gguf"), []byte("model"), 0o600))
	previous := SetGeneratorLoader(func(modelPath string, gpuLayers, contextSize, batchSize int) (Generator, error) {
		return NewMockGenerator(modelPath), nil
	})
	t.Cleanup(func() { SetGeneratorLoader(previous) })
	manager, err = NewManager(Config{Enabled: true, Provider: "unknown-provider", Model: "unknown", ModelsDir: tmpDir})
	require.NoError(t, err)
	require.Equal(t, filepath.Join(tmpDir, "unknown.gguf"), manager.ModelPath())
}

func TestHeimdallCoverage_LoadLocalGeneratorDefaultDiscovery(t *testing.T) {
	tmpDir := t.TempDir()
	modelsDir := filepath.Join(tmpDir, "models")
	require.NoError(t, os.MkdirAll(modelsDir, 0o755))
	modelPath := filepath.Join(modelsDir, "qwen3-0.6b-instruct.gguf")
	require.NoError(t, os.WriteFile(modelPath, []byte("model"), 0o600))
	t.Chdir(tmpDir)

	var capturedModelPath string
	var capturedGPULayers int
	var capturedContextSize int
	var capturedBatchSize int
	previous := SetGeneratorLoader(func(modelPath string, gpuLayers, contextSize, batchSize int) (Generator, error) {
		capturedModelPath = modelPath
		capturedGPULayers = gpuLayers
		capturedContextSize = contextSize
		capturedBatchSize = batchSize
		return &coverageGPUGenerator{MockGenerator: NewMockGenerator(modelPath)}, nil
	})
	t.Cleanup(func() { SetGeneratorLoader(previous) })

	gen, resolvedPath, err := loadLocalGenerator(Config{GPULayers: 5, ContextSize: 4096, BatchSize: 1024})
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(resolvedPath, "qwen3-0.6b-instruct.gguf"))
	require.Equal(t, resolvedPath, capturedModelPath)
	require.Equal(t, 5, capturedGPULayers)
	require.Equal(t, 4096, capturedContextSize)
	require.Equal(t, 1024, capturedBatchSize)
	require.True(t, gen.(*coverageGPUGenerator).UsingGPU())
}

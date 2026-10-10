package heimdall

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// PromptContext tests
// ============================================================================

func TestPromptContext_Cancel(t *testing.T) {
	ctx := &PromptContext{}

	assert.False(t, ctx.Cancelled())
	assert.Empty(t, ctx.CancelReason())
	assert.Empty(t, ctx.CancelledBy())

	ctx.Cancel("quota exceeded", "rate-limiter-plugin")

	assert.True(t, ctx.Cancelled())
	assert.Equal(t, "quota exceeded", ctx.CancelReason())
	assert.Equal(t, "rate-limiter-plugin", ctx.CancelledBy())
}

func TestPromptContext_Notifications(t *testing.T) {
	ctx := &PromptContext{}

	ctx.NotifyInfo("Info Title", "Info message")
	ctx.NotifyWarning("Warn Title", "Warn message")
	ctx.NotifyError("Err Title", "Err message")
	ctx.NotifyProgress("Prog Title", "Prog message")

	notifications := ctx.DrainNotifications()
	require.Len(t, notifications, 4)
	assert.Equal(t, "info", notifications[0].Type)
	assert.Equal(t, "Info Title", notifications[0].Title)
	assert.Equal(t, "warning", notifications[1].Type)
	assert.Equal(t, "error", notifications[2].Type)
	assert.Equal(t, "progress", notifications[3].Type)

	// Second drain should be empty
	empty := ctx.DrainNotifications()
	assert.Empty(t, empty)
}

func TestPromptContext_Notify_Direct(t *testing.T) {
	ctx := &PromptContext{}
	ctx.Notify("success", "Done", "All good")

	notifications := ctx.DrainNotifications()
	require.Len(t, notifications, 1)
	assert.Equal(t, "success", notifications[0].Type)
	assert.Equal(t, "Done", notifications[0].Title)
	assert.Equal(t, "All good", notifications[0].Message)
}

func TestPromptContext_SendMessage_NilBifrost(t *testing.T) {
	ctx := &PromptContext{}
	// Should not panic with nil bifrost
	ctx.SendMessage("test message")
	ctx.Broadcast("broadcast message")
}

func TestPromptContext_SetBifrost_SendMessage(t *testing.T) {
	ctx := &PromptContext{}
	bifrost := NewMockBifrost()
	ctx.SetBifrost(bifrost)

	ctx.SendMessage("hello via bifrost")
	time.Sleep(5 * time.Millisecond) // goroutine
	bifrost.mu.Lock()
	msgs := bifrost.messages
	bifrost.mu.Unlock()
	assert.NotEmpty(t, msgs)
}

func TestPromptContext_SetBifrost_Broadcast(t *testing.T) {
	ctx := &PromptContext{}
	bifrost := NewMockBifrost()
	ctx.SetBifrost(bifrost)

	ctx.Broadcast("system announcement")
	time.Sleep(5 * time.Millisecond) // goroutine
	bifrost.mu.Lock()
	broadcasts := bifrost.broadcasts
	bifrost.mu.Unlock()
	assert.NotEmpty(t, broadcasts)
}

func TestPromptContext_BuildFinalPrompt_WithContext(t *testing.T) {
	ctx := &PromptContext{
		ActionPrompt:           "available actions: query, status",
		UserMessage:            "show me all nodes",
		AdditionalInstructions: "The user has admin access.",
		Examples: []PromptExample{
			{UserSays: "count nodes", ActionJSON: `{"action":"query","params":{"cypher":"MATCH (n) RETURN count(n)"}}`},
		},
	}

	prompt := ctx.BuildFinalPrompt()
	assert.NotEmpty(t, prompt)
	assert.Contains(t, prompt, "Heimdall")
	assert.Contains(t, prompt, "query, status")
	assert.Contains(t, prompt, "admin access")
	assert.Contains(t, prompt, "count nodes")
}

func TestPromptContext_BuildFinalPromptForTools_WithContext(t *testing.T) {
	ctx := &PromptContext{
		ActionPrompt:           "actions: store, recall",
		UserMessage:            "store this memory",
		AdditionalInstructions: "Context from plugin.",
		Examples: []PromptExample{
			{UserSays: "remember X", ActionJSON: `{"action":"store","params":{}}`},
		},
	}

	prompt := ctx.BuildFinalPromptForTools()
	assert.NotEmpty(t, prompt)
	assert.Contains(t, prompt, "Heimdall")
	assert.Contains(t, prompt, "store, recall")
	assert.Contains(t, prompt, "Context from plugin")
}

func TestPromptContext_BuildFinalPrompt_FallsBackToMinimalWhenHuge(t *testing.T) {
	// ActionPrompt so large it exceeds system token budget → triggers minimal prompt
	huge := strings.Repeat("x", MaxSystemPromptTokens()*8) // way over limit
	ctx := &PromptContext{
		ActionPrompt: huge,
		UserMessage:  "test",
	}

	prompt := ctx.BuildFinalPrompt()
	assert.NotEmpty(t, prompt)
	// Should use minimal prompt
	assert.Contains(t, prompt, "Heimdall")
}

func TestPromptContext_EstimatedSystemTokens(t *testing.T) {
	ctx := &PromptContext{
		ActionPrompt: "some actions",
		UserMessage:  "hello",
	}
	tokens := ctx.EstimatedSystemTokens()
	assert.Greater(t, tokens, 0)
}

func TestPromptContext_ValidateTokenBudget_OK(t *testing.T) {
	ctx := &PromptContext{
		ActionPrompt: "short action prompt",
		UserMessage:  "short user message",
	}
	err := ctx.ValidateTokenBudget()
	assert.NoError(t, err)
}

func TestPromptContext_ValidateTokenBudget_SystemTooLarge(t *testing.T) {
	// System prompt that's too large
	huge := strings.Repeat("x", MaxSystemPromptTokens()*8) // forces over budget
	ctx := &PromptContext{
		ActionPrompt: huge,
		UserMessage:  "ok",
	}
	err := ctx.ValidateTokenBudget()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "system prompt too large")
}

func TestPromptContext_ValidateTokenBudget_TotalTooLarge(t *testing.T) {
	// Set custom small budget for this test
	original := tokenBudget
	defer func() { tokenBudget = original }()

	tokenBudget = TokenBudget{
		MaxContext: 50,
		MaxSystem:  40,
		MaxUser:    10,
	}

	ctx := &PromptContext{
		ActionPrompt: "actions: x",
		UserMessage:  strings.Repeat("u", MaxContextTokens()*8),
	}
	err := ctx.ValidateTokenBudget()
	assert.Error(t, err)
}

func TestPromptContext_GetBudgetInfo(t *testing.T) {
	ctx := &PromptContext{
		ActionPrompt: "actions",
		UserMessage:  "hello",
	}
	info := ctx.GetBudgetInfo()
	assert.Greater(t, info.MaxSystem, 0)
	assert.Greater(t, info.MaxUser, 0)
	assert.Greater(t, info.MaxTotal, 0)
	assert.GreaterOrEqual(t, info.TotalTokens, info.SystemTokens+info.UserTokens-1)
}

// ============================================================================
// PreExecuteContext tests
// ============================================================================

func TestPreExecuteContext_Cancel(t *testing.T) {
	ctx := &PreExecuteContext{}

	assert.False(t, ctx.Cancelled())
	assert.Empty(t, ctx.CancelReason())
	assert.Empty(t, ctx.CancelledBy())

	ctx.Cancel("not allowed", "security-hook")

	assert.True(t, ctx.Cancelled())
	assert.Equal(t, "not allowed", ctx.CancelReason())
	assert.Equal(t, "security-hook", ctx.CancelledBy())
}

func TestPreExecuteContext_SetBifrost(t *testing.T) {
	ctx := &PreExecuteContext{}
	bifrost := &NoOpBifrost{}
	ctx.SetBifrost(bifrost)
	// Just ensuring no panic
}

func TestPreExecuteContext_Notifications(t *testing.T) {
	ctx := &PreExecuteContext{}

	ctx.NotifyInfo("Info", "Info msg")
	ctx.NotifyWarning("Warn", "Warn msg")
	ctx.NotifyError("Err", "Err msg")
	ctx.NotifyProgress("Progress", "Progress msg")

	notifications := ctx.DrainNotifications()
	require.Len(t, notifications, 4)
	assert.Equal(t, "info", notifications[0].Type)
	assert.Equal(t, "warning", notifications[1].Type)
	assert.Equal(t, "error", notifications[2].Type)
	assert.Equal(t, "progress", notifications[3].Type)

	empty := ctx.DrainNotifications()
	assert.Empty(t, empty)
}

func TestPreExecuteContext_Notify_Direct(t *testing.T) {
	ctx := &PreExecuteContext{}
	ctx.Notify("custom", "Custom Title", "Custom msg")

	notifications := ctx.DrainNotifications()
	require.Len(t, notifications, 1)
	assert.Equal(t, "custom", notifications[0].Type)
}

// ============================================================================
// PostExecuteContext tests
// ============================================================================

func TestPostExecuteContext_Notifications(t *testing.T) {
	ctx := &PostExecuteContext{}

	ctx.NotifyInfo("Info", "Info msg")
	ctx.NotifyWarning("Warn", "Warn msg")
	ctx.NotifyError("Err", "Err msg")
	ctx.NotifySuccess("Success", "Success msg")

	notifications := ctx.DrainNotifications()
	require.Len(t, notifications, 4)
	assert.Equal(t, "info", notifications[0].Type)
	assert.Equal(t, "warning", notifications[1].Type)
	assert.Equal(t, "error", notifications[2].Type)
	assert.Equal(t, "success", notifications[3].Type)

	empty := ctx.DrainNotifications()
	assert.Empty(t, empty)
}

func TestPostExecuteContext_Notify_Direct(t *testing.T) {
	ctx := &PostExecuteContext{}
	ctx.Notify("progress", "Step", "Doing step 1")

	notifications := ctx.DrainNotifications()
	require.Len(t, notifications, 1)
	assert.Equal(t, "progress", notifications[0].Type)
	assert.Equal(t, "Step", notifications[0].Title)
	assert.Equal(t, "Doing step 1", notifications[0].Message)
}

// ============================================================================
// Token budget tests
// ============================================================================

func TestSetTokenBudget_AppliesValues(t *testing.T) {
	original := tokenBudget
	defer func() { tokenBudget = original }()

	flags := &MockFeatureFlags{
		maxContextTokens: 4096,
		maxSystemTokens:  3000,
		maxUserTokens:    1000,
	}

	SetTokenBudget(flags)

	assert.Equal(t, 4096, MaxContextTokens())
	assert.Equal(t, 3000, MaxSystemPromptTokens())
	assert.Equal(t, 1000, MaxUserMessageTokens())
	budget := GetTokenBudget()
	assert.Equal(t, 4096, budget.MaxContext)
}

func TestSetTokenBudget_ZeroValuesSkipped(t *testing.T) {
	original := tokenBudget
	defer func() { tokenBudget = original }()

	// Set known values first
	tokenBudget = TokenBudget{MaxContext: 9999, MaxSystem: 8888, MaxUser: 7777}

	// Zero values should not overwrite
	flags := &MockFeatureFlags{
		maxContextTokens: 0,
		maxSystemTokens:  0,
		maxUserTokens:    0,
	}
	SetTokenBudget(flags)

	assert.Equal(t, 9999, MaxContextTokens())
	assert.Equal(t, 8888, MaxSystemPromptTokens())
	assert.Equal(t, 7777, MaxUserMessageTokens())
}

func TestGetTokenBudget_DefaultValues(t *testing.T) {
	original := tokenBudget
	defer func() { tokenBudget = original }()

	tokenBudget = TokenBudget{
		MaxContext: DefaultMaxContextTokens,
		MaxSystem:  DefaultMaxSystemPromptTokens,
		MaxUser:    DefaultMaxUserMessageTokens,
	}

	assert.Equal(t, DefaultMaxContextTokens, MaxContextTokens())
	assert.Equal(t, DefaultMaxSystemPromptTokens, MaxSystemPromptTokens())
	assert.Equal(t, DefaultMaxUserMessageTokens, MaxUserMessageTokens())
}

// ============================================================================
// EstimateTokens / EstimateToolRoundMessagesTokens
// ============================================================================

func TestEstimateTokens(t *testing.T) {
	// Empty string → 0
	assert.Equal(t, 0, EstimateTokens(""))

	// ~4 chars per token: 400 chars → 100 tokens
	text400 := strings.Repeat("a", 400)
	assert.Equal(t, 100, EstimateTokens(text400))

	// Larger text
	text800 := strings.Repeat("b", 800)
	assert.Equal(t, 200, EstimateTokens(text800))
}

func TestEstimateToolRoundMessagesTokens_Empty(t *testing.T) {
	assert.Equal(t, 0, EstimateToolRoundMessagesTokens(nil))
	assert.Equal(t, 0, EstimateToolRoundMessagesTokens([]ToolRoundMessage{}))
}

func TestEstimateToolRoundMessagesTokens_WithToolCalls(t *testing.T) {
	msgs := []ToolRoundMessage{
		{Role: "user", Content: strings.Repeat("a", 400)},
		{
			Role:    "assistant",
			Content: "calling tool",
			ToolCalls: []ParsedToolCall{
				{Id: "call-1", Name: "test_tool", Arguments: `{"key":"val"}`},
			},
		},
	}
	tokens := EstimateToolRoundMessagesTokens(msgs)
	// user: 100 tokens, assistant: 3 tokens + id/name/args overhead + 50 overhead
	assert.Greater(t, tokens, 100)
}

func TestEstimateToolRoundMessagesTokens_MultipleToolCalls(t *testing.T) {
	msgs := []ToolRoundMessage{
		{
			Role:    "assistant",
			Content: "",
			ToolCalls: []ParsedToolCall{
				{Id: "c1", Name: "tool_a", Arguments: `{"x":1}`},
				{Id: "c2", Name: "tool_b", Arguments: `{"y":2}`},
			},
		},
	}
	tokens := EstimateToolRoundMessagesTokens(msgs)
	assert.Greater(t, tokens, 0)
}

// ============================================================================
// DatabaseEvent type classification
// ============================================================================

func TestDatabaseEvent_IsNodeEvent(t *testing.T) {
	nodeTypes := []DatabaseEventType{
		EventNodeCreated, EventNodeUpdated, EventNodeDeleted, EventNodeRead,
	}
	for _, eventType := range nodeTypes {
		e := &DatabaseEvent{Type: eventType}
		assert.True(t, e.IsNodeEvent(), "expected IsNodeEvent for %s", eventType)
		assert.False(t, e.IsRelationshipEvent())
		assert.False(t, e.IsQueryEvent())
		assert.False(t, e.IsTransactionEvent())
	}
}

func TestDatabaseEvent_IsRelationshipEvent(t *testing.T) {
	relTypes := []DatabaseEventType{
		EventRelationshipCreated, EventRelationshipUpdated, EventRelationshipDeleted,
	}
	for _, eventType := range relTypes {
		e := &DatabaseEvent{Type: eventType}
		assert.False(t, e.IsNodeEvent())
		assert.True(t, e.IsRelationshipEvent(), "expected IsRelationshipEvent for %s", eventType)
		assert.False(t, e.IsQueryEvent())
		assert.False(t, e.IsTransactionEvent())
	}
}

func TestDatabaseEvent_IsQueryEvent(t *testing.T) {
	queryTypes := []DatabaseEventType{EventQueryExecuted, EventQueryFailed}
	for _, eventType := range queryTypes {
		e := &DatabaseEvent{Type: eventType}
		assert.False(t, e.IsNodeEvent())
		assert.False(t, e.IsRelationshipEvent())
		assert.True(t, e.IsQueryEvent(), "expected IsQueryEvent for %s", eventType)
		assert.False(t, e.IsTransactionEvent())
	}
}

func TestDatabaseEvent_IsTransactionEvent(t *testing.T) {
	txnTypes := []DatabaseEventType{EventTransactionCommit, EventTransactionRollback}
	for _, eventType := range txnTypes {
		e := &DatabaseEvent{Type: eventType}
		assert.False(t, e.IsNodeEvent())
		assert.False(t, e.IsRelationshipEvent())
		assert.False(t, e.IsQueryEvent())
		assert.True(t, e.IsTransactionEvent(), "expected IsTransactionEvent for %s", eventType)
	}
}

func TestDatabaseEvent_SystemEvents_NotClassified(t *testing.T) {
	systemTypes := []DatabaseEventType{
		EventDatabaseStarted, EventDatabaseShutdown,
		EventBackupStarted, EventBackupCompleted,
		EventIndexCreated, EventIndexDropped,
	}
	for _, eventType := range systemTypes {
		e := &DatabaseEvent{Type: eventType}
		assert.False(t, e.IsNodeEvent(), "unexpected IsNodeEvent for %s", eventType)
		assert.False(t, e.IsRelationshipEvent(), "unexpected IsRelationshipEvent for %s", eventType)
		assert.False(t, e.IsQueryEvent(), "unexpected IsQueryEvent for %s", eventType)
		assert.False(t, e.IsTransactionEvent(), "unexpected IsTransactionEvent for %s", eventType)
	}
}

// ============================================================================
// NoOpHeimdallInvoker
// ============================================================================

func TestNoOpHeimdallInvoker(t *testing.T) {
	invoker := &NoOpHeimdallInvoker{}

	result, err := invoker.InvokeAction(context.Background(), "any_action", map[string]interface{}{"key": "val"})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Success)
	assert.Contains(t, result.Message, "not available")

	result, err = invoker.SendPrompt(context.Background(), "do something")
	require.NoError(t, err)
	assert.False(t, result.Success)

	result, err = invoker.SendRawPrompt(context.Background(), "just answer this")
	require.NoError(t, err)
	assert.False(t, result.Success)

	// These are fire-and-forget, just ensure no panic
	invoker.InvokeActionAsync("test_action", nil)
	invoker.SendPromptAsync("test prompt")
}

// ============================================================================
// tryParseActionResponse
// ============================================================================

func TestTryParseActionResponse_ValidAction(t *testing.T) {
	resp := `{"action":"heimdall_health_check","params":{"verbose":true}}`
	action := tryParseActionResponse(resp)
	require.NotNil(t, action)
	assert.Equal(t, "heimdall_health_check", action.Action)
	assert.Equal(t, true, action.Params["verbose"])
}

func TestTryParseActionResponse_NoActionField(t *testing.T) {
	resp := `{"key":"value","other":123}`
	action := tryParseActionResponse(resp)
	assert.Nil(t, action)
}

func TestTryParseActionResponse_NoJSON(t *testing.T) {
	action := tryParseActionResponse("no json content here")
	assert.Nil(t, action)
}

func TestTryParseActionResponse_PrefixThenJSON(t *testing.T) {
	resp := `Okay, I'll run: {"action":"query","params":{"cypher":"MATCH (n) RETURN n"}}`
	action := tryParseActionResponse(resp)
	require.NotNil(t, action)
	assert.Equal(t, "query", action.Action)
}

func TestTryParseActionResponse_UnclosedJSON(t *testing.T) {
	resp := `{"action":"test","params":{`
	action := tryParseActionResponse(resp)
	assert.Nil(t, action)
}

func TestTryParseActionResponse_EmptyString(t *testing.T) {
	action := tryParseActionResponse("")
	assert.Nil(t, action)
}

func TestTryParseActionResponse_EmptyAction(t *testing.T) {
	resp := `{"action":"","params":{}}`
	action := tryParseActionResponse(resp)
	assert.Nil(t, action)
}

func TestTryParseActionResponse_InvalidJSON(t *testing.T) {
	resp := `{not valid json}`
	action := tryParseActionResponse(resp)
	assert.Nil(t, action)
}

// ============================================================================
// FormatActionResultForModel
// ============================================================================

func TestFormatActionResultForModel_Nil(t *testing.T) {
	s := FormatActionResultForModel(nil)
	assert.Contains(t, s, "no result")
	assert.Contains(t, s, "false")
}

func TestFormatActionResultForModel_Success(t *testing.T) {
	result := &ActionResult{
		Success: true,
		Message: "operation completed",
		Data:    map[string]interface{}{"count": 42},
	}
	s := FormatActionResultForModel(result)
	assert.Contains(t, s, "true")
	assert.Contains(t, s, "operation completed")
	assert.Contains(t, s, "42")
}

func TestFormatActionResultForModel_Failure(t *testing.T) {
	result := &ActionResult{
		Success: false,
		Message: "error occurred",
	}
	s := FormatActionResultForModel(result)
	assert.Contains(t, s, "false")
	assert.Contains(t, s, "error occurred")
}

// ============================================================================
// FormatInMemoryToolResult
// ============================================================================

func TestFormatInMemoryToolResult_Error(t *testing.T) {
	s := FormatInMemoryToolResult(nil, errors.New("something failed"))
	assert.Contains(t, s, "false")
	assert.Contains(t, s, "something failed")
}

func TestFormatInMemoryToolResult_Success(t *testing.T) {
	payload := map[string]interface{}{"nodes": 3, "edges": 7}
	s := FormatInMemoryToolResult(payload, nil)
	assert.Contains(t, s, "nodes")
	assert.Contains(t, s, "3")
	assert.Contains(t, s, "edges")
}

func TestFormatInMemoryToolResult_NilSuccess(t *testing.T) {
	s := FormatInMemoryToolResult(nil, nil)
	assert.NotEmpty(t, s)
}

// ============================================================================
// MetricsCollector
// ============================================================================

func TestNewMetricsCollector_Collect(t *testing.T) {
	collector := NewMetricsCollector(nil, nil)
	require.NotNil(t, collector)

	metrics := collector.Collect()
	require.NotNil(t, metrics)
	assert.Greater(t, int(metrics.Runtime.GoroutineCount), 0)
	assert.False(t, metrics.CollectedAt.IsZero())
}

func TestMetricsCollector_Collect_Cache(t *testing.T) {
	collector := NewMetricsCollector(nil, nil)

	first := collector.Collect()
	second := collector.Collect()

	// Should return cached result (same CollectedAt)
	assert.Equal(t, first.CollectedAt, second.CollectedAt)
}

func TestMetricsCollector_Runtime(t *testing.T) {
	collector := NewMetricsCollector(nil, nil)
	rt := collector.Runtime()
	assert.Greater(t, int(rt.GoroutineCount), 0)
}

type lifecycleMetricsSourceStub struct{}

func (l *lifecycleMetricsSourceStub) Stats() interface{}                      { return nil }
func (l *lifecycleMetricsSourceStub) NodeCount() (int64, error)               { return 0, nil }
func (l *lifecycleMetricsSourceStub) EdgeCount() (int64, error)               { return 0, nil }
func (l *lifecycleMetricsSourceStub) EmbedQueueStats() interface{}            { return nil }
func (l *lifecycleMetricsSourceStub) GetWAL() WALStats                        { return nil }
func (l *lifecycleMetricsSourceStub) GetSchemaManager() SchemaManagerStats    { return nil }
func (l *lifecycleMetricsSourceStub) GetQueryCache() QueryCacheStats          { return nil }
func (l *lifecycleMetricsSourceStub) GetGPUManager() GPUManagerStats          { return nil }
func (l *lifecycleMetricsSourceStub) EncryptionStats() map[string]interface{} { return nil }
func (l *lifecycleMetricsSourceStub) LifecycleStatus() (map[string]interface{}, error) {
	return map[string]interface{}{"enabled": true, "pressure_band": "normal"}, nil
}

type lifecycleMetricsErrorStub struct{ lifecycleMetricsSourceStub }

func (l *lifecycleMetricsErrorStub) LifecycleStatus() (map[string]interface{}, error) {
	return nil, errors.New("status failed")
}

func TestMetricsCollector_Collect_MVCCLifecycleStatus(t *testing.T) {
	collector := NewMetricsCollector(&lifecycleMetricsSourceStub{}, nil)
	collector.cacheTTL = 0
	metrics := collector.Collect()
	require.NotNil(t, metrics.Database.MVCCLifecycle)
	assert.Equal(t, true, metrics.Database.MVCCLifecycle["enabled"])
	assert.Equal(t, "normal", metrics.Database.MVCCLifecycle["pressure_band"])
}

func TestMetricsCollector_Collect_MVCCLifecycleStatusError(t *testing.T) {
	collector := NewMetricsCollector(&lifecycleMetricsErrorStub{}, nil)
	collector.cacheTTL = 0
	metrics := collector.Collect()
	assert.Nil(t, metrics.Database.MVCCLifecycle)
}

// ============================================================================
// RealMetricsReader
// ============================================================================

func TestRealMetricsReader_Runtime(t *testing.T) {
	reader := &RealMetricsReader{}
	m := reader.Runtime()
	assert.Greater(t, int(m.GoroutineCount), 0)
	// NumGC can be 0 in short-lived tests, just check it's a uint
	_ = m.NumGC
	_ = m.MemoryAllocMB
}

// ============================================================================
// DefaultLogger
// ============================================================================

func TestNewDefaultLogger(t *testing.T) {
	logger := NewDefaultLogger("test-prefix")
	require.NotNil(t, logger)

	// All log levels should be no-ops (no panics)
	logger.Debug("debug msg", "k", "v")
	logger.Info("info msg")
	logger.Warn("warn msg", "x", 1)
	logger.Error("error msg")
}

// ============================================================================
// SubsystemManager - AllHealth and AllSummaries
// ============================================================================

func TestSubsystemManager_AllHealth(t *testing.T) {
	manager := &SubsystemManager{
		plugins: make(map[string]*LoadedHeimdallPlugin),
		actions: make(map[string]ActionFunc),
	}
	ctx := SubsystemContext{Config: DefaultConfig(), Bifrost: &NoOpBifrost{}}
	manager.SetContext(ctx)

	p1 := NewMockPlugin("svc_a")
	p2 := NewMockPlugin("svc_b")
	require.NoError(t, manager.RegisterPlugin(p1, "", true))
	require.NoError(t, manager.RegisterPlugin(p2, "", true))
	require.NoError(t, manager.StartAll())

	health := manager.AllHealth()
	assert.Len(t, health, 2)
	assert.Contains(t, health, "svc_a")
	assert.Contains(t, health, "svc_b")
	assert.True(t, health["svc_a"].Healthy)
}

func TestSubsystemManager_AllSummaries(t *testing.T) {
	manager := &SubsystemManager{
		plugins: make(map[string]*LoadedHeimdallPlugin),
		actions: make(map[string]ActionFunc),
	}
	ctx := SubsystemContext{Config: DefaultConfig(), Bifrost: &NoOpBifrost{}}
	manager.SetContext(ctx)

	plugin := NewMockPlugin("summarized_svc")
	require.NoError(t, manager.RegisterPlugin(plugin, "", true))

	summaries := manager.AllSummaries()
	assert.Contains(t, summaries, "summarized_svc")
	assert.Equal(t, "Mock plugin summary", summaries["summarized_svc"])
}

func TestSubsystemManager_DuplicatePlugin(t *testing.T) {
	manager := &SubsystemManager{
		plugins: make(map[string]*LoadedHeimdallPlugin),
		actions: make(map[string]ActionFunc),
	}
	ctx := SubsystemContext{Config: DefaultConfig(), Bifrost: &NoOpBifrost{}}
	manager.SetContext(ctx)

	p1 := NewMockPlugin("dup_plugin")
	p2 := NewMockPlugin("dup_plugin")
	require.NoError(t, manager.RegisterPlugin(p1, "", true))

	err := manager.RegisterPlugin(p2, "", true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already registered")
}

// ============================================================================
// rbac_context helpers
// ============================================================================

func TestPrincipalRolesFromContext_Empty(t *testing.T) {
	roles := PrincipalRolesFromContext(context.Background())
	assert.Nil(t, roles)
}

func TestDatabaseAccessModeFromContext_Empty(t *testing.T) {
	mode := DatabaseAccessModeFromContext(context.Background())
	assert.Nil(t, mode)
}

func TestResolvedAccessResolverFromContext_Empty(t *testing.T) {
	fn := ResolvedAccessResolverFromContext(context.Background())
	assert.Nil(t, fn)
}

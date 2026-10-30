package heimdall

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockHeimdallPlugin implements HeimdallPlugin for testing
type MockHeimdallPlugin struct {
	mu          sync.RWMutex
	name        string
	version     string
	description string
	status      SubsystemStatus
	initialized bool
	started     bool
	stopped     bool
	shutdown    bool
	ctx         SubsystemContext
	configMap   map[string]interface{}
	events      []SubsystemEvent
}

func NewMockPlugin(name string) *MockHeimdallPlugin {
	return &MockHeimdallPlugin{
		name:        name,
		version:     "1.0.0",
		description: "Mock plugin for testing",
		status:      StatusUninitialized,
		configMap:   make(map[string]interface{}),
		events:      make([]SubsystemEvent, 0),
	}
}

func (m *MockHeimdallPlugin) Name() string        { return m.name }
func (m *MockHeimdallPlugin) Version() string     { return m.version }
func (m *MockHeimdallPlugin) Type() string        { return PluginTypeHeimdall }
func (m *MockHeimdallPlugin) Description() string { return m.description }

func (m *MockHeimdallPlugin) Initialize(ctx SubsystemContext) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ctx = ctx
	m.initialized = true
	m.status = StatusReady
	return nil
}

func (m *MockHeimdallPlugin) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.started = true
	m.status = StatusRunning
	return nil
}

func (m *MockHeimdallPlugin) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopped = true
	m.status = StatusStopped
	return nil
}

func (m *MockHeimdallPlugin) Shutdown() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shutdown = true
	m.status = StatusUninitialized
	return nil
}

func (m *MockHeimdallPlugin) Status() SubsystemStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status
}

func (m *MockHeimdallPlugin) Health() SubsystemHealth {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return SubsystemHealth{
		Status:    m.status,
		Healthy:   m.status == StatusRunning,
		Message:   "Mock health",
		LastCheck: time.Now(),
	}
}

func (m *MockHeimdallPlugin) Metrics() map[string]interface{} {
	return map[string]interface{}{
		"mock_metric": 42,
	}
}

func (m *MockHeimdallPlugin) Config() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string]interface{})
	for k, v := range m.configMap {
		result[k] = v
	}
	return result
}

func (m *MockHeimdallPlugin) Configure(settings map[string]interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range settings {
		m.configMap[k] = v
	}
	return nil
}

func (m *MockHeimdallPlugin) ConfigSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
	}
}

func (m *MockHeimdallPlugin) Actions() map[string]ActionFunc {
	return map[string]ActionFunc{
		"test_action": {
			Name:        "test_action",
			Description: "A test action",
			Category:    "testing",
			Handler: func(ctx ActionContext) (*ActionResult, error) {
				return &ActionResult{
					Success: true,
					Message: "Test action executed",
					Data: map[string]interface{}{
						"user_message": ctx.UserMessage,
					},
				}, nil
			},
		},
		"bifrost_action": {
			Name:        "bifrost_action",
			Description: "Tests Bifrost bridge",
			Category:    "testing",
			Handler: func(ctx ActionContext) (*ActionResult, error) {
				if ctx.Bifrost != nil {
					ctx.Bifrost.SendMessage("Test message via Bifrost")
				}
				return &ActionResult{
					Success: true,
					Message: "Bifrost action executed",
				}, nil
			},
		},
	}
}

func (m *MockHeimdallPlugin) Summary() string {
	return "Mock plugin summary"
}

func (m *MockHeimdallPlugin) RecentEvents(limit int) []SubsystemEvent {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if limit > len(m.events) {
		limit = len(m.events)
	}
	return m.events[:limit]
}

func (m *MockHeimdallPlugin) PrePrompt(ctx *PromptContext) error {
	// No-op for mock
	return nil
}

func (m *MockHeimdallPlugin) PreExecute(ctx *PreExecuteContext, done func(PreExecuteResult)) {
	// Default: continue with execution
	done(PreExecuteResult{Continue: true})
}

func (m *MockHeimdallPlugin) PostExecute(ctx *PostExecuteContext) {
	// No-op for mock
}

// MockBifrost implements BifrostBridge for testing
type MockBifrost struct {
	mu            sync.Mutex
	messages      []string
	notifications []struct {
		Type, Title, Message string
	}
	broadcasts    []string
	confirmations bool
	connected     bool
	connCount     int
}

func NewMockBifrost() *MockBifrost {
	return &MockBifrost{
		messages:      make([]string, 0),
		broadcasts:    make([]string, 0),
		confirmations: true,
		connected:     true,
		connCount:     1,
	}
}

func (b *MockBifrost) SendMessage(msg string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.messages = append(b.messages, msg)
	return nil
}

func (b *MockBifrost) SendNotification(notifType, title, message string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.notifications = append(b.notifications, struct{ Type, Title, Message string }{notifType, title, message})
	return nil
}

func (b *MockBifrost) Broadcast(msg string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.broadcasts = append(b.broadcasts, msg)
	return nil
}

func (b *MockBifrost) RequestConfirmation(action string) (bool, error) {
	return b.confirmations, nil
}

func (b *MockBifrost) IsConnected() bool {
	return b.connected
}

func (b *MockBifrost) ConnectionCount() int {
	return b.connCount
}

// Tests

func TestPluginTypeHeimdall(t *testing.T) {
	assert.Equal(t, "heimdall", PluginTypeHeimdall)
}

func TestSubsystemManager_Singleton(t *testing.T) {
	// Reset global manager for test
	globalManager = nil

	m1 := GetSubsystemManager()
	m2 := GetSubsystemManager()

	assert.Same(t, m1, m2, "GetSubsystemManager should return same instance")
}

func TestSubsystemManager_RegisterPlugin(t *testing.T) {
	// Create fresh manager for test
	manager := &SubsystemManager{
		plugins: make(map[string]*LoadedHeimdallPlugin),
		actions: make(map[string]ActionFunc),
	}

	ctx := SubsystemContext{
		Config:  DefaultConfig(),
		Bifrost: &NoOpBifrost{},
	}
	manager.SetContext(ctx)

	plugin := NewMockPlugin("test_plugin")

	err := manager.RegisterPlugin(plugin, "", true)
	require.NoError(t, err)

	// Verify plugin is registered
	registered, ok := manager.GetPlugin("test_plugin")
	assert.True(t, ok)
	assert.Equal(t, "test_plugin", registered.Name())

	// Verify plugin was initialized
	assert.True(t, plugin.initialized)
	assert.Equal(t, StatusReady, plugin.Status())
}

func TestSubsystemManager_RegisterPlugin_WrongType(t *testing.T) {
	manager := &SubsystemManager{
		plugins: make(map[string]*LoadedHeimdallPlugin),
		actions: make(map[string]ActionFunc),
	}

	ctx := SubsystemContext{Config: DefaultConfig()}
	manager.SetContext(ctx)

	// Plugin that returns wrong type
	plugin := &wrongTypePlugin{}

	err := manager.RegisterPlugin(plugin, "", true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected")
}

// wrongTypePlugin returns wrong type
type wrongTypePlugin struct {
	MockHeimdallPlugin
}

func (w *wrongTypePlugin) Type() string { return "wrong_type" }

func TestSubsystemManager_RegisterActions(t *testing.T) {
	manager := &SubsystemManager{
		plugins: make(map[string]*LoadedHeimdallPlugin),
		actions: make(map[string]ActionFunc),
	}

	ctx := SubsystemContext{
		Config:  DefaultConfig(),
		Bifrost: &NoOpBifrost{},
	}
	manager.SetContext(ctx)

	plugin := NewMockPlugin("test_plugin")
	err := manager.RegisterPlugin(plugin, "", true)
	require.NoError(t, err)

	// Verify actions are registered with full names
	action, ok := manager.GetAction("heimdall_test_plugin_test_action")
	assert.True(t, ok)
	assert.Equal(t, "A test action", action.Description)
}

func TestSubsystemManager_StartAll(t *testing.T) {
	manager := &SubsystemManager{
		plugins: make(map[string]*LoadedHeimdallPlugin),
		actions: make(map[string]ActionFunc),
	}

	ctx := SubsystemContext{
		Config:  DefaultConfig(),
		Bifrost: &NoOpBifrost{},
	}
	manager.SetContext(ctx)

	plugin := NewMockPlugin("test_plugin")
	err := manager.RegisterPlugin(plugin, "", true)
	require.NoError(t, err)

	err = manager.StartAll()
	require.NoError(t, err)

	assert.True(t, plugin.started)
	assert.Equal(t, StatusRunning, plugin.Status())
}

func TestSubsystemManager_StopAll(t *testing.T) {
	manager := &SubsystemManager{
		plugins: make(map[string]*LoadedHeimdallPlugin),
		actions: make(map[string]ActionFunc),
	}

	ctx := SubsystemContext{
		Config:  DefaultConfig(),
		Bifrost: &NoOpBifrost{},
	}
	manager.SetContext(ctx)

	plugin := NewMockPlugin("test_plugin")
	_ = manager.RegisterPlugin(plugin, "", true)
	_ = manager.StartAll()

	err := manager.StopAll()
	require.NoError(t, err)

	assert.True(t, plugin.stopped)
	assert.Equal(t, StatusStopped, plugin.Status())
}

func TestSubsystemManager_ShutdownAll(t *testing.T) {
	manager := &SubsystemManager{
		plugins: make(map[string]*LoadedHeimdallPlugin),
		actions: make(map[string]ActionFunc),
	}

	ctx := SubsystemContext{
		Config:  DefaultConfig(),
		Bifrost: &NoOpBifrost{},
	}
	manager.SetContext(ctx)

	plugin := NewMockPlugin("test_plugin")
	_ = manager.RegisterPlugin(plugin, "", true)
	_ = manager.StartAll()

	err := manager.ShutdownAll()
	require.NoError(t, err)

	assert.True(t, plugin.shutdown)
	assert.Equal(t, StatusUninitialized, plugin.Status())
}

func TestNoOpBifrost(t *testing.T) {
	bifrost := &NoOpBifrost{}

	// All operations should succeed silently
	assert.NoError(t, bifrost.SendMessage("test"))
	assert.NoError(t, bifrost.SendNotification("info", "title", "msg"))
	assert.NoError(t, bifrost.Broadcast("broadcast"))

	confirmed, err := bifrost.RequestConfirmation("action")
	assert.NoError(t, err)
	assert.False(t, confirmed)

	assert.False(t, bifrost.IsConnected())
	assert.Equal(t, 0, bifrost.ConnectionCount())
}

func TestMockBifrost(t *testing.T) {
	bifrost := NewMockBifrost()

	// Test SendMessage
	err := bifrost.SendMessage("Hello")
	require.NoError(t, err)
	assert.Len(t, bifrost.messages, 1)
	assert.Equal(t, "Hello", bifrost.messages[0])

	// Test SendNotification
	err = bifrost.SendNotification("warning", "Alert", "Something happened")
	require.NoError(t, err)
	assert.Len(t, bifrost.notifications, 1)
	assert.Equal(t, "warning", bifrost.notifications[0].Type)

	// Test Broadcast
	err = bifrost.Broadcast("System message")
	require.NoError(t, err)
	assert.Len(t, bifrost.broadcasts, 1)

	// Test RequestConfirmation
	confirmed, err := bifrost.RequestConfirmation("delete")
	require.NoError(t, err)
	assert.True(t, confirmed)

	// Test connection status
	assert.True(t, bifrost.IsConnected())
	assert.Equal(t, 1, bifrost.ConnectionCount())
}

func TestActionContext_WithBifrost(t *testing.T) {
	bifrost := NewMockBifrost()

	ctx := ActionContext{
		Context:     context.Background(),
		UserMessage: "Test message",
		Params:      map[string]interface{}{"key": "value"},
		Bifrost:     bifrost,
	}

	// Simulate action using Bifrost
	ctx.Bifrost.SendMessage("Action started")
	ctx.Bifrost.SendNotification("info", "Progress", "50% complete")
	ctx.Bifrost.Broadcast("Action finished")

	assert.Len(t, bifrost.messages, 1)
	assert.Len(t, bifrost.notifications, 1)
	assert.Len(t, bifrost.broadcasts, 1)
}

func TestSubsystemStatus_Constants(t *testing.T) {
	statuses := []SubsystemStatus{
		StatusUninitialized,
		StatusInitializing,
		StatusReady,
		StatusRunning,
		StatusStopping,
		StatusStopped,
		StatusError,
	}

	// All should be distinct strings
	seen := make(map[SubsystemStatus]bool)
	for _, s := range statuses {
		assert.False(t, seen[s], "Duplicate status: %s", s)
		seen[s] = true
	}
}

func TestSubsystemHealth(t *testing.T) {
	health := SubsystemHealth{
		Status:    StatusRunning,
		Healthy:   true,
		Message:   "All systems operational",
		LastCheck: time.Now(),
		Details: map[string]interface{}{
			"uptime": 3600,
		},
	}

	assert.True(t, health.Healthy)
	assert.Equal(t, StatusRunning, health.Status)
	assert.Equal(t, int(3600), health.Details["uptime"])
}

func TestSubsystemEvent(t *testing.T) {
	event := SubsystemEvent{
		Time:    time.Now(),
		Type:    "info",
		Message: "Something happened",
		Data: map[string]interface{}{
			"count": 42,
		},
	}

	assert.Equal(t, "info", event.Type)
	assert.Equal(t, 42, event.Data["count"])
}

func TestActionFunc(t *testing.T) {
	action := ActionFunc{
		Name:        "heimdall_test_action",
		Description: "Test action",
		Category:    "testing",
		Handler: func(ctx ActionContext) (*ActionResult, error) {
			return &ActionResult{
				Success: true,
				Message: "Done",
			}, nil
		},
	}

	assert.Equal(t, "heimdall_test_action", action.Name)
	assert.Equal(t, "testing", action.Category)

	// Execute the handler
	result, err := action.Handler(ActionContext{})
	require.NoError(t, err)
	assert.True(t, result.Success)
}

func TestActionResult(t *testing.T) {
	result := ActionResult{
		Success: true,
		Message: "Operation completed",
		Data: map[string]interface{}{
			"items_processed": 100,
		},
	}

	assert.True(t, result.Success)
	assert.Equal(t, 100, result.Data["items_processed"])
}

func TestListHeimdallPlugins(t *testing.T) {
	// Reset global manager
	globalManager = nil

	manager := GetSubsystemManager()
	ctx := SubsystemContext{
		Config:  DefaultConfig(),
		Bifrost: &NoOpBifrost{},
	}
	manager.SetContext(ctx)

	plugin1 := NewMockPlugin("plugin1")
	plugin2 := NewMockPlugin("plugin2")

	_ = manager.RegisterPlugin(plugin1, "", true)
	_ = manager.RegisterPlugin(plugin2, "/path/to/plugin2.so", false)

	plugins := ListHeimdallPlugins()
	assert.Len(t, plugins, 2)

	// Find by name
	names := make(map[string]bool)
	for _, p := range plugins {
		names[p.Plugin.Name()] = true
	}
	assert.True(t, names["plugin1"])
	assert.True(t, names["plugin2"])
}

type OrderedMockPlugin struct {
	*MockHeimdallPlugin
	priority int
	before   []string
	after    []string
}

func NewOrderedMockPlugin(name string, priority int, before, after []string) *OrderedMockPlugin {
	return &OrderedMockPlugin{
		MockHeimdallPlugin: NewMockPlugin(name),
		priority:           priority,
		before:             before,
		after:              after,
	}
}

func (o *OrderedMockPlugin) Priority() int    { return o.priority }
func (o *OrderedMockPlugin) Before() []string { return o.before }
func (o *OrderedMockPlugin) After() []string  { return o.after }

func TestListHeimdallPlugins_Ordering(t *testing.T) {
	globalManager = nil

	manager := GetSubsystemManager()
	ctx := SubsystemContext{
		Config:  DefaultConfig(),
		Bifrost: &NoOpBifrost{},
	}
	manager.SetContext(ctx)

	pluginA := NewOrderedMockPlugin("alpha", 0, nil, nil)
	pluginB := NewOrderedMockPlugin("beta", 10, nil, nil)
	pluginC := NewOrderedMockPlugin("gamma", 5, nil, []string{"beta"})

	require.NoError(t, manager.RegisterPlugin(pluginA, "", true))
	require.NoError(t, manager.RegisterPlugin(pluginB, "", true))
	require.NoError(t, manager.RegisterPlugin(pluginC, "", true))

	plugins := ListHeimdallPlugins()
	require.Len(t, plugins, 3)
	assert.Equal(t, "beta", plugins[0].Plugin.Name())
	assert.Equal(t, "gamma", plugins[1].Plugin.Name())
	assert.Equal(t, "alpha", plugins[2].Plugin.Name())
}

func TestListHeimdallActions(t *testing.T) {
	// Reset global manager
	globalManager = nil

	manager := GetSubsystemManager()
	ctx := SubsystemContext{
		Config:  DefaultConfig(),
		Bifrost: &NoOpBifrost{},
	}
	manager.SetContext(ctx)

	plugin := NewMockPlugin("test")
	_ = manager.RegisterPlugin(plugin, "", true)

	actions := ListHeimdallActions()

	// Should have actions from the mock plugin
	found := false
	for _, name := range actions {
		if name == "heimdall_test_test_action" {
			found = true
			break
		}
	}
	assert.True(t, found, "Should find heimdall_test_test_action in actions list")
}

func TestActionsAsMCPTools(t *testing.T) {
	globalManager = nil
	manager := GetSubsystemManager()
	ctx := SubsystemContext{Config: DefaultConfig(), Bifrost: &NoOpBifrost{}}
	manager.SetContext(ctx)
	plugin := NewMockPlugin("test")
	_ = manager.RegisterPlugin(plugin, "", true)

	tools := ActionsAsMCPTools()
	require.NotEmpty(t, tools, "plugin actions should produce MCP tools")

	for _, tool := range tools {
		assert.NotEmpty(t, tool.Name, "MCP tool must have name")
		assert.NotEmpty(t, tool.Description, "MCP tool must have description")
		assert.NotEmpty(t, tool.InputSchema, "MCP tool must have inputSchema (default or set)")
		var schema map[string]interface{}
		err := json.Unmarshal(tool.InputSchema, &schema)
		assert.NoError(t, err, "InputSchema for %s must be valid JSON", tool.Name)
		assert.Equal(t, "object", schema["type"], "inputSchema type should be object")
	}
}

func TestGetHeimdallAction(t *testing.T) {
	// Reset global manager
	globalManager = nil

	manager := GetSubsystemManager()
	ctx := SubsystemContext{
		Config:  DefaultConfig(),
		Bifrost: &NoOpBifrost{},
	}
	manager.SetContext(ctx)

	plugin := NewMockPlugin("test")
	_ = manager.RegisterPlugin(plugin, "", true)

	action, ok := GetHeimdallAction("heimdall_test_test_action")
	assert.True(t, ok)
	assert.Equal(t, "A test action", action.Description)

	_, ok = GetHeimdallAction("heimdall_nonexistent_action")
	assert.False(t, ok)
}

func TestHeimdallPluginsInitialized(t *testing.T) {
	// Create a fresh manager for this test (don't use global)
	manager := &SubsystemManager{
		plugins: make(map[string]*LoadedHeimdallPlugin),
		actions: make(map[string]ActionFunc),
	}

	// Not initialized yet
	assert.False(t, manager.initialized)

	ctx := SubsystemContext{
		Config:  DefaultConfig(),
		Bifrost: &NoOpBifrost{},
	}
	manager.SetContext(ctx)

	plugin := NewMockPlugin("test_init")
	_ = manager.RegisterPlugin(plugin, "", true)

	// After registering a plugin, should be initialized
	assert.True(t, manager.initialized)
}

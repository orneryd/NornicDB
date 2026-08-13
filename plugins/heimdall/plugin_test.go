package heimdall

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helper to create ActionContext
func newActionCtx(params map[string]interface{}) heimdall.ActionContext {
	return heimdall.ActionContext{
		Context: context.Background(),
		Params:  params,
		Bifrost: &heimdall.NoOpBifrost{},
	}
}

type memoryDBRouter struct {
	summary string
	facts   []string
	seen    map[string]struct{}
}

func (m *memoryDBRouter) DefaultDatabaseName() string { return "default" }

func (m *memoryDBRouter) ResolveDatabase(nameOrAlias string) (string, error) {
	if nameOrAlias == "" {
		return "default", nil
	}
	return nameOrAlias, nil
}

func (m *memoryDBRouter) ListDatabases() []string { return []string{"default"} }

func (m *memoryDBRouter) Query(ctx context.Context, database string, cypher string, params map[string]interface{}) ([]map[string]interface{}, error) {
	_ = ctx
	_ = database
	switch {
	case strings.Contains(cypher, "SET m.summary"):
		if summary, ok := params["summary"].(string); ok {
			m.summary = summary
		}
		return []map[string]interface{}{{"summary": m.summary}}, nil
	case strings.Contains(cypher, "MERGE (f:HeimdallSessionFact"):
		if m.seen == nil {
			m.seen = make(map[string]struct{})
		}
		if fact, ok := params["fact"].(string); ok {
			if _, exists := m.seen[fact]; !exists {
				m.seen[fact] = struct{}{}
				m.facts = append(m.facts, fact)
			}
		}
		return []map[string]interface{}{{"fact": params["fact"]}}, nil
	case strings.Contains(cypher, "RETURN m.summary AS summary"):
		if m.summary == "" {
			return nil, nil
		}
		return []map[string]interface{}{{"summary": m.summary}}, nil
	case strings.Contains(cypher, "RETURN f.fact AS fact"):
		rows := make([]map[string]interface{}, 0, len(m.facts))
		for i := len(m.facts) - 1; i >= 0; i-- {
			rows = append(rows, map[string]interface{}{"fact": m.facts[i]})
		}
		return rows, nil
	case strings.Contains(cypher, "DETACH DELETE f"):
		return nil, nil
	case strings.Contains(cypher, "RETURN 1 AS one"):
		return []map[string]interface{}{{"one": int64(1)}}, nil
	default:
		return nil, fmt.Errorf("unexpected query: %s", cypher)
	}
}

func (m *memoryDBRouter) Stats(database string) (heimdall.DatabaseStats, error) {
	_ = database
	return heimdall.DatabaseStats{}, nil
}

func (m *memoryDBRouter) Discover(ctx context.Context, database string, query string, nodeTypes []string, limit int, depth int) (*heimdall.DiscoverResult, error) {
	_ = ctx
	_ = database
	_ = query
	_ = nodeTypes
	_ = limit
	_ = depth
	return &heimdall.DiscoverResult{}, nil
}

// TestWatcherPlugin_Interface verifies plugin implements HeimdallPlugin
func TestWatcherPlugin_Interface(t *testing.T) {
	var _ heimdall.HeimdallPlugin = &WatcherPlugin{}
}

func TestWatcherPlugin_PreExecuteContinuesAndCountsRequest(t *testing.T) {
	p := &WatcherPlugin{}
	resultCh := make(chan heimdall.PreExecuteResult, 1)
	ctx := &heimdall.PreExecuteContext{
		Context:   context.Background(),
		RequestID: "request-1",
		Action:    "heimdall_watcher_status",
		Params:    map[string]interface{}{},
	}

	p.PreExecute(ctx, func(result heimdall.PreExecuteResult) {
		resultCh <- result
	})

	select {
	case result := <-resultCh:
		assert.True(t, result.Continue)
	case <-time.After(time.Second):
		t.Fatal("PreExecute did not invoke its callback")
	}
	assert.Equal(t, int64(1), p.Metrics()["requests"])
}

// TestWatcherPlugin_Identity tests identity methods
func TestWatcherPlugin_Identity(t *testing.T) {
	p := &WatcherPlugin{}

	assert.Equal(t, "watcher", p.Name())
	assert.Equal(t, "1.0.0", p.Version())
	assert.Equal(t, heimdall.PluginTypeHeimdall, p.Type())
	assert.Contains(t, p.Description(), "coding agent")
}

// TestWatcherPlugin_Lifecycle tests the full lifecycle
func TestWatcherPlugin_Lifecycle(t *testing.T) {
	p := &WatcherPlugin{}

	// Initialize
	ctx := heimdall.SubsystemContext{
		Config: heimdall.Config{
			Model:       "test-model",
			MaxTokens:   512,
			Temperature: 0.1,
		},
	}
	err := p.Initialize(ctx)
	require.NoError(t, err)
	assert.Equal(t, heimdall.StatusReady, p.Status())

	// Start
	err = p.Start()
	require.NoError(t, err)
	assert.Equal(t, heimdall.StatusRunning, p.Status())

	// Check health
	health := p.Health()
	assert.True(t, health.Healthy)
	assert.Equal(t, heimdall.StatusRunning, health.Status)

	// Stop
	err = p.Stop()
	require.NoError(t, err)
	assert.Equal(t, heimdall.StatusStopped, p.Status())

	// Shutdown
	err = p.Shutdown()
	require.NoError(t, err)
	assert.Equal(t, heimdall.StatusUninitialized, p.Status())
}

// TestWatcherPlugin_Actions tests that all actions are registered (GRAPH-RAG + system).
func TestWatcherPlugin_Actions(t *testing.T) {
	p := &WatcherPlugin{}
	actions := p.Actions()

	expectedActions := []string{
		"hello",                // Greeting/readiness check
		"help",                 // List available actions
		"status",               // Get status
		"repo_map",             // Repository structure summary
		"autocomplete_suggest", // Cypher schema suggestions
		"search",               // Semantic search (GRAPH-RAG)
		"query",                // Read-only Cypher
		"db_stats",             // Database statistics
	}

	for _, name := range expectedActions {
		t.Run(name, func(t *testing.T) {
			action, ok := actions[name]
			assert.True(t, ok, "Action %s should be registered", name)
			assert.NotEmpty(t, action.Description)
			assert.NotEmpty(t, action.Category)
			assert.NotNil(t, action.Handler)
		})
	}
}

// TestWatcherPlugin_HelpAction tests the help action (lists action catalog)
func TestWatcherPlugin_HelpAction(t *testing.T) {
	p := &WatcherPlugin{}

	ctx := heimdall.SubsystemContext{
		Config: heimdall.Config{
			Model:       "test-model",
			MaxTokens:   512,
			Temperature: 0.1,
		},
	}
	require.NoError(t, p.Initialize(ctx))
	require.NoError(t, p.Start())

	actionCtx := newActionCtx(map[string]interface{}{})

	result, err := p.actionHelp(actionCtx)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.True(t, result.Success)
	assert.Contains(t, result.Message, "Available Heimdall coding-agent actions")
	assert.NotNil(t, result.Data)
	_, hasCatalog := result.Data["catalog"]
	assert.True(t, hasCatalog, "result.Data should have catalog key")
}

// TestWatcherPlugin_StatusAction tests the status action
func TestWatcherPlugin_StatusAction(t *testing.T) {
	p := &WatcherPlugin{}

	ctx := heimdall.SubsystemContext{
		Config: heimdall.Config{
			Model:       "qwen3-0.6b",
			MaxTokens:   512,
			Temperature: 0.1,
		},
	}
	require.NoError(t, p.Initialize(ctx))
	require.NoError(t, p.Start())

	actionCtx := newActionCtx(map[string]interface{}{})

	result, err := p.actionStatus(actionCtx)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.True(t, result.Success)
	assert.Contains(t, result.Message, "Coding plugin status")

	codingPluginData, ok := result.Data["coding_plugin"].(map[string]interface{})
	require.True(t, ok, "result.Data should have coding_plugin key")
	assert.Equal(t, heimdall.StatusRunning, codingPluginData["status"])
	assert.NotNil(t, codingPluginData["config"])
	assert.NotNil(t, codingPluginData["summary"])
}

// TestWatcherPlugin_MetricsAction tests the metrics action
func TestWatcherPlugin_MetricsAction(t *testing.T) {
	p := &WatcherPlugin{}

	ctx := heimdall.SubsystemContext{
		Config: heimdall.Config{
			Model: "test-model",
		},
	}
	require.NoError(t, p.Initialize(ctx))
	require.NoError(t, p.Start())

	// Make some requests to generate metrics
	for i := 0; i < 5; i++ {
		actionCtx := newActionCtx(map[string]interface{}{})
		_, _ = p.actionHello(actionCtx)
	}

	actionCtx := newActionCtx(map[string]interface{}{})

	result, err := p.actionDBStats(actionCtx)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.True(t, result.Success)
	assert.Contains(t, result.Message, "CONTEXT STRATEGY")

	contextStrategy, ok := result.Data["context_strategy"].(map[string]interface{})
	require.True(t, ok, "result.Data should have context_strategy key")
	assert.Equal(t, "summarize_and_rag", contextStrategy["history_strategy"])
}

// TestWatcherPlugin_EventsAction tests the events action
func TestWatcherPlugin_EventsAction(t *testing.T) {
	p := &WatcherPlugin{}

	ctx := heimdall.SubsystemContext{
		Config: heimdall.Config{
			Model: "test-model",
		},
	}
	require.NoError(t, p.Initialize(ctx))
	require.NoError(t, p.Start())

	// Generate some events via hello action
	for i := 0; i < 3; i++ {
		actionCtx := newActionCtx(map[string]interface{}{})
		_, _ = p.actionHello(actionCtx)
	}

	events := p.RecentEvents(10)
	assert.NotEmpty(t, events)
}

// TestWatcherPlugin_ConfigureAction tests the set_config action
func TestWatcherPlugin_ConfigureAction(t *testing.T) {
	p := &WatcherPlugin{}

	ctx := heimdall.SubsystemContext{
		Config: heimdall.Config{
			Model:       "test-model",
			MaxTokens:   512,
			Temperature: 0.1,
		},
	}
	require.NoError(t, p.Initialize(ctx))
	require.NoError(t, p.Start())

	t.Run("valid config update", func(t *testing.T) {
		err := p.Configure(map[string]interface{}{
			"max_tokens": 1024,
		})
		require.NoError(t, err)

		// Verify config was updated
		config := p.Config()
		assert.Equal(t, 1024, config["max_tokens"])
	})

	t.Run("invalid config value", func(t *testing.T) {
		err := p.Configure(map[string]interface{}{
			"max_tokens": 10000, // Too high
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid max_tokens")
	})
}

// TestWatcherPlugin_BroadcastAction tests the broadcast action
func TestWatcherPlugin_BroadcastAction(t *testing.T) {
	p := &WatcherPlugin{}

	ctx := heimdall.SubsystemContext{
		Config: heimdall.Config{
			Model: "test-model",
		},
	}
	require.NoError(t, p.Initialize(ctx))
	require.NoError(t, p.Start())

	t.Run("with message", func(t *testing.T) {
		actionCtx := newActionCtx(map[string]interface{}{
			"limit": 5,
		})

		result, err := p.actionRepoMap(actionCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Contains(t, result.Message, "database not available")
	})

	t.Run("missing message", func(t *testing.T) {
		actionCtx := newActionCtx(map[string]interface{}{
			"cypher": strings.Repeat("a", 10001),
		})

		result, err := p.actionQuery(actionCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Contains(t, result.Message, "query too long")
	})
}

func TestWatcherPlugin_QueryReadOnlyGuard(t *testing.T) {
	p := &WatcherPlugin{}

	ctx := heimdall.SubsystemContext{
		Config: heimdall.Config{Model: "test-model"},
	}
	require.NoError(t, p.Initialize(ctx))
	require.NoError(t, p.Start())

	blocked, err := p.actionQuery(newActionCtx(map[string]interface{}{
		"cypher": "CREATE (:Thing {id: 1})",
	}))
	require.NoError(t, err)
	require.False(t, blocked.Success)
	require.Contains(t, blocked.Message, "read-only Cypher")

	allowedDB := &memoryDBRouter{}
	p.Initialize(heimdall.SubsystemContext{Config: heimdall.Config{Model: "test-model"}, Database: allowedDB})
	require.NoError(t, p.Start())

	allowed, err := p.actionQuery(heimdall.ActionContext{
		Context: context.Background(),
		Params: map[string]interface{}{
			"cypher": "RETURN 1 AS one",
		},
		Bifrost:  &heimdall.NoOpBifrost{},
		Database: allowedDB,
	})
	require.NoError(t, err)
	require.True(t, allowed.Success)
	require.Equal(t, 1, len(allowed.Data["rows"].([]map[string]interface{})))
}

// TestWatcherPlugin_NotifyAction tests the notify action
func TestWatcherPlugin_NotifyAction(t *testing.T) {
	p := &WatcherPlugin{}

	ctx := heimdall.SubsystemContext{
		Config: heimdall.Config{
			Model: "test-model",
		},
	}
	require.NoError(t, p.Initialize(ctx))
	require.NoError(t, p.Start())

	t.Run("full notification", func(t *testing.T) {
		actionCtx := newActionCtx(map[string]interface{}{
			"query": "plugin loading",
		})

		result, err := p.actionDiscover(actionCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Contains(t, result.Message, "database not available")
	})

	t.Run("missing message", func(t *testing.T) {
		actionCtx := newActionCtx(map[string]interface{}{})

		result, err := p.actionDiscover(actionCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Contains(t, result.Message, "query parameter required")
	})
}

// TestWatcherPlugin_Concurrency tests thread safety
func TestWatcherPlugin_Concurrency(t *testing.T) {
	p := &WatcherPlugin{}

	ctx := heimdall.SubsystemContext{
		Config: heimdall.Config{
			Model: "test-model",
		},
	}
	require.NoError(t, p.Initialize(ctx))
	require.NoError(t, p.Start())

	// Run concurrent requests
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				actionCtx := newActionCtx(map[string]interface{}{})
				_, _ = p.actionHello(actionCtx)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify metrics are consistent
	metrics := p.Metrics()
	assert.GreaterOrEqual(t, metrics["requests"].(int64), int64(1000))
	assert.LessOrEqual(t, len(p.RecentEvents(1000)), 100)
}

func TestWatcherPlugin_EventBufferConcurrentAccess(t *testing.T) {
	p := &WatcherPlugin{}

	ctx := heimdall.SubsystemContext{Config: heimdall.Config{Model: "test-model"}}
	require.NoError(t, p.Initialize(ctx))
	require.NoError(t, p.Start())

	stop := make(chan struct{})
	go func() {
		for i := 0; i < 200; i++ {
			_, _ = p.actionHello(newActionCtx(map[string]interface{}{}))
		}
		close(stop)
	}()

	for {
		select {
		case <-stop:
			return
		default:
			_ = p.RecentEvents(25)
		}
	}
}

func TestWatcherPlugin_CaptureSessionMemory(t *testing.T) {
	p := &WatcherPlugin{}

	ctx := heimdall.SubsystemContext{
		Config: heimdall.Config{
			Model:       "test-model",
			MaxTokens:   512,
			Temperature: 0.1,
		},
	}
	require.NoError(t, p.Initialize(ctx))
	require.NoError(t, p.Start())

	result := &heimdall.ActionResult{
		Success: true,
		Message: "Repository graph map: 20 nodes, 10 relationships, 3 label groups",
		Data: map[string]interface{}{
			"database":           "default",
			"node_count":         20,
			"relationship_count": 10,
		},
	}

	p.captureSessionMemory(context.Background(), "heimdall_watcher_repo_map", map[string]interface{}{}, result)

	assert.NotEmpty(t, p.summary)
	assert.NotEmpty(t, p.facts)
	assert.Contains(t, p.summary, "Recent coding session facts")
	assert.Contains(t, strings.Join(p.facts, "\n"), "Repository node count observed: 20")
	assert.Contains(t, strings.Join(p.facts, "\n"), "Database in focus: default")
}

func TestWatcherPlugin_BuildSessionMemoryContext(t *testing.T) {
	p := &WatcherPlugin{}
	p.summary = "Recent coding session facts:\n- Updated plugin summary"
	p.facts = []string{
		"Query investigated: plugin loading",
		"Successful action: heimdall_watcher_search",
		"Database in focus: default",
	}

	contextBlock := p.buildSessionMemoryContext("show plugin loading flow")
	assert.Contains(t, contextBlock, "SUMMARIZED CODING SESSION MEMORY")
	assert.Contains(t, contextBlock, "Updated plugin summary")
	assert.Contains(t, contextBlock, "plugin loading")
}

func TestWatcherPlugin_GraphBackedSessionMemory(t *testing.T) {
	db := &memoryDBRouter{}
	p := &WatcherPlugin{}

	ctx := heimdall.SubsystemContext{
		Config: heimdall.Config{
			Model:       "test-model",
			MaxTokens:   512,
			Temperature: 0.1,
		},
		Database: db,
	}
	require.NoError(t, p.Initialize(ctx))
	require.NoError(t, p.Start())

	result := &heimdall.ActionResult{
		Success: true,
		Message: "Repository graph map: 7 nodes, 5 relationships, 2 label groups",
		Data: map[string]interface{}{
			"database":           "default",
			"node_count":         7,
			"relationship_count": 5,
		},
	}

	p.captureSessionMemory(context.Background(), "heimdall_watcher_repo_map", map[string]interface{}{"query": "plugin graph"}, result)
	assert.NotEmpty(t, db.summary)
	assert.NotEmpty(t, db.facts)
	firstFactCount := len(db.facts)
	p.captureSessionMemory(context.Background(), "heimdall_watcher_repo_map", map[string]interface{}{"query": "plugin graph"}, result)
	assert.Equal(t, firstFactCount, len(db.facts))

	p.summary = ""
	p.facts = nil
	p.refreshSessionMemoryFromGraph(context.Background(), "plugin graph", db, "coding_session")

	assert.NotEmpty(t, p.summary)
	assert.NotEmpty(t, p.facts)
	assert.Contains(t, p.summary, "Recent coding session facts")
	assert.Contains(t, strings.Join(p.facts, "\n"), "plugin graph")
}

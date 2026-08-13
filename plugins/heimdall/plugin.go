// Package heimdall provides the Heimdall SLM Management plugin.
//
// Heimdall is the all-seeing guardian of the SLM subsystem, named after the
// Norse god who watches over Bifröst. Like its namesake, Heimdall coordinates
// repository awareness and controls access to the
// cognitive capabilities of NornicDB.
//
// # Plugin Type
//
// This is a Heimdall plugin, which means it provides agentic actions and
// prompt-shaping capabilities that the coding model can use.
//
// # Actions Provided
//
//   - heimdall_watcher_help - List available coding-agent tools
//   - heimdall_watcher_status - Get coding-agent status and current capabilities
//   - heimdall_watcher_repo_map - Summarize repository structure from the graph
//   - heimdall_watcher_search - Search the graph for relevant code/domain context
//   - heimdall_watcher_query - Run graph-backed Cypher for targeted investigation
//
// # Example Usage
//
// User: "Help me understand this repository before making a change"
// Heimdall maps to: heimdall_watcher_repo_map
// Result: Returns repository-focused graph context for the coding model
//
// # Building as Plugin
//
// To build as a standalone .so plugin:
//
//	go build -buildmode=plugin -o heimdall.so ./plugins/heimdall
//
// # Built-in Registration
//
// This plugin is also registered as a built-in plugin, so no .so file is needed.
package heimdall

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/heimdall"
)

// Plugin is the exported plugin variable.
// For .so plugins, export as: var Plugin heimdall.HeimdallPlugin = &WatcherPlugin{}
var Plugin heimdall.HeimdallPlugin = &WatcherPlugin{}

// WatcherPlugin implements heimdall.HeimdallPlugin for SLM management.
// The Watcher is Heimdall's built-in coding agent coordinator.
//
// This plugin also demonstrates autonomous action invocation:
// - Shapes prompts with repository-aware context
// - Exposes graph-backed coding actions to the model
// - Uses HeimdallInvoker to synthesize action results into coding guidance
// - Maintains durable summarized conversation state for long context windows
type WatcherPlugin struct {
	mu       sync.RWMutex
	eventMu  sync.RWMutex
	ctx      heimdall.SubsystemContext
	status   heimdall.SubsystemStatus
	events   []heimdall.SubsystemEvent
	config   map[string]interface{}
	summary  string
	facts    []string
	started  time.Time
	requests int64
	errors   int64
}

// === Identity Methods ===

func (p *WatcherPlugin) Name() string {
	return "watcher"
}

func (p *WatcherPlugin) Version() string {
	return "1.0.0"
}

func (p *WatcherPlugin) Type() string {
	return heimdall.PluginTypeHeimdall // Must return "heimdall"
}

func (p *WatcherPlugin) Description() string {
	return "Heimdall coding agent - repository-aware orchestration for graph-backed software development"
}

// === Lifecycle Methods ===

func (p *WatcherPlugin) Initialize(ctx heimdall.SubsystemContext) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.ctx = ctx
	p.status = heimdall.StatusReady
	p.events = make([]heimdall.SubsystemEvent, 0, 100)
	p.summary = ""
	p.facts = make([]string, 0, 32)
	p.config = map[string]interface{}{
		"max_tokens":       ctx.Config.MaxTokens,
		"temperature":      ctx.Config.Temperature,
		"model":            ctx.Config.Model,
		"history_strategy": "summarize_and_rag",
		"summary_scope":    "coding_session",
		"response_style":   "implementation-first",
	}

	p.addEvent("info", "\nHeimdall coding agent initialized\n", nil)
	return nil
}

func (p *WatcherPlugin) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.status = heimdall.StatusRunning
	p.started = time.Now()
	p.addEvent("info", "\nHeimdall coding agent active\n", nil)
	return nil
}

func (p *WatcherPlugin) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.status = heimdall.StatusStopped
	p.addEvent("info", "\nHeimdall coding agent paused\n", nil)
	return nil
}

func (p *WatcherPlugin) Shutdown() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.status = heimdall.StatusUninitialized
	p.addEvent("info", "\nHeimdall coding agent shutdown\n", nil)
	return nil
}

// === State Methods ===

func (p *WatcherPlugin) Status() heimdall.SubsystemStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.status
}

func (p *WatcherPlugin) Health() heimdall.SubsystemHealth {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return heimdall.SubsystemHealth{
		Status:    p.status,
		Healthy:   p.status == heimdall.StatusRunning || p.status == heimdall.StatusReady,
		Message:   fmt.Sprintf("Heimdall coding plugin is %s", p.status),
		LastCheck: time.Now(),
		Details: map[string]interface{}{
			"uptime_seconds":   time.Since(p.started).Seconds(),
			"requests":         p.requests,
			"errors":           p.errors,
			"history_strategy": p.config["history_strategy"],
			"stored_facts":     len(p.facts),
		},
	}
}

func (p *WatcherPlugin) Metrics() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return map[string]interface{}{
		"status":           string(p.status),
		"uptime_seconds":   time.Since(p.started).Seconds(),
		"requests":         p.requests,
		"errors":           p.errors,
		"error_rate":       float64(p.errors) / float64(max(p.requests, 1)),
		"memory_mb":        memStats.Alloc / 1024 / 1024,
		"goroutines":       runtime.NumGoroutine(),
		"history_strategy": p.config["history_strategy"],
		"stored_facts":     len(p.facts),
	}
}

// === Configuration Methods ===

func (p *WatcherPlugin) Config() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Return copy
	result := make(map[string]interface{})
	for k, v := range p.config {
		result[k] = v
	}
	return result
}

func (p *WatcherPlugin) Configure(settings map[string]interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Validate and apply settings
	for key, value := range settings {
		switch key {
		case "max_tokens":
			if v, ok := value.(int); ok && v > 0 && v <= 4096 {
				p.config[key] = v
			} else {
				return fmt.Errorf("invalid max_tokens: must be 1-4096")
			}
		case "temperature":
			if v, ok := value.(float64); ok && v >= 0 && v <= 2 {
				p.config[key] = v
			} else {
				return fmt.Errorf("invalid temperature: must be 0-2")
			}
		case "history_strategy", "summary_scope", "response_style":
			if v, ok := value.(string); ok && strings.TrimSpace(v) != "" {
				p.config[key] = strings.TrimSpace(v)
			} else {
				return fmt.Errorf("invalid %s: must be a non-empty string", key)
			}
		default:
			return fmt.Errorf("unknown config key: %s", key)
		}
	}

	p.addEvent("info", "Heimdall configuration updated", settings)
	return nil
}

func (p *WatcherPlugin) ConfigSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"max_tokens": map[string]interface{}{
				"type":        "integer",
				"description": "Maximum tokens to generate",
				"minimum":     1,
				"maximum":     4096,
				"default":     512,
			},
			"temperature": map[string]interface{}{
				"type":        "number",
				"description": "Generation temperature (0=deterministic, 2=creative)",
				"minimum":     0,
				"maximum":     2,
				"default":     0.1,
			},
			"history_strategy": map[string]interface{}{
				"type":        "string",
				"description": "How the coding plugin should persist and recover chat context",
				"default":     "summarize_and_rag",
			},
			"summary_scope": map[string]interface{}{
				"type":        "string",
				"description": "Scope used when summarizing prior coding turns",
				"default":     "coding_session",
			},
			"response_style": map[string]interface{}{
				"type":        "string",
				"description": "Preferred interaction style for coding assistance",
				"default":     "implementation-first",
			},
		},
	}
}

// === Actions ===

// autocompleteSuggestInputSchema is JSON Schema for the autocomplete_suggest action (query param).
var autocompleteSuggestInputSchema = json.RawMessage([]byte(`{"type":"object","properties":{"query":{"type":"string","description":"Partial Cypher query to complete"}},"required":["query"]}`))

// discoverInputSchema is JSON Schema for the discover action (semantic search in the graph).
var discoverInputSchema = json.RawMessage([]byte(`{"type":"object","properties":{"query":{"type":"string","description":"Natural language or keyword search query"},"limit":{"type":"integer","description":"Max results (default 10)"},"depth":{"type":"integer","description":"Traversal depth for relationships from matched nodes (default 1)"}},"required":["query"]}`))

// queryInputSchema is JSON Schema for the query action (read-only Cypher).
var queryInputSchema = json.RawMessage([]byte(`{"type":"object","properties":{"cypher":{"type":"string","description":"Cypher query to execute"},"params":{"type":"object","description":"Optional query parameters"},"database":{"type":"string","description":"Logical database name (optional)"}},"required":["cypher"]}`))

// repoMapInputSchema is JSON Schema for the repo_map action.
var repoMapInputSchema = json.RawMessage([]byte(`{"type":"object","properties":{"database":{"type":"string","description":"Logical database name (optional)"},"limit":{"type":"integer","description":"Max number of files or modules to include (default 25)"}},"additionalProperties":false}`))

func (p *WatcherPlugin) Actions() map[string]heimdall.ActionFunc {
	return map[string]heimdall.ActionFunc{
		"hello": {
			Description: "Respond with a simple greeting to confirm Heimdall is available. Use for greetings, smoke tests, or a lightweight readiness check.",
			Category:    "system",
			Handler:     p.actionHello,
		},
		"help": {
			Description: "List all available Heimdall coding-agent actions",
			Category:    "system",
			Handler:     p.actionHelp,
		},
		"status": {
			Description: "Get coding-plugin status and active context-management settings",
			Category:    "coding",
			Handler:     p.actionStatus,
		},
		"repo_map": {
			Description: "Summarize repository structure from the graph database. Use before planning a code change or when mapping modules, labels, and connected entities.",
			Category:    "coding",
			InputSchema: repoMapInputSchema,
			Handler:     p.actionRepoMap,
		},
		"autocomplete_suggest": {
			Description: "Generate Cypher autocomplete suggestions for repository and graph exploration",
			Category:    "coding",
			InputSchema: autocompleteSuggestInputSchema,
			Handler:     p.actionAutocompleteSuggest,
		},
		"search": {
			Description: "Semantic search in the repository graph. Use for concepts, features, symbols, files, or implementation areas relevant to a coding task. Params: query (required), limit (optional), depth (optional).",
			Category:    "coding",
			InputSchema: discoverInputSchema,
			Handler:     p.actionDiscover,
		},
		"query": {
			Description: "Execute a read-only Cypher query for repository investigation. Use when explicit graph inspection is needed for code understanding. Params: cypher (required), params (optional), database (optional).",
			Category:    "coding",
			InputSchema: queryInputSchema,
			Handler:     p.actionQuery,
		},
		"db_stats": {
			Description: "Get graph statistics that help the coding plugin understand repository coverage: nodes, edges, labels, clustering, embeddings, and feature flags",
			Category:    "coding",
			Handler:     p.actionDBStats,
		},
	}
}

// Action Handlers

// actionHelp returns the action catalog from the package (all registered actions by category).
func (p *WatcherPlugin) actionHelp(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	catalog := heimdall.ActionCatalog()
	p.addEvent("info", "Help: listed actions", nil)
	return &heimdall.ActionResult{
		Success: true,
		Message: "Available Heimdall coding-agent actions by category",
		Data:    map[string]interface{}{"catalog": catalog},
	}, nil
}

func (p *WatcherPlugin) actionRepoMap(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	if ctx.Database == nil {
		return &heimdall.ActionResult{Success: false, Message: "database not available"}, nil
	}

	limit := 25
	if l, ok := ctx.Params["limit"].(float64); ok && l > 0 && l <= 100 {
		limit = int(l)
	}
	dbName := getDatabaseParam(ctx.Params)
	if dbName == "" {
		dbName = ctx.Database.DefaultDatabaseName()
	}

	stats, err := ctx.Database.Stats(dbName)
	if err != nil {
		return &heimdall.ActionResult{
			Success: false,
			Message: fmt.Sprintf("failed to load repository graph stats: %v", err),
		}, nil
	}

	rows, err := ctx.Database.Query(ctx.Context, dbName, `
		MATCH (n)
		WITH labels(n) AS nodeLabels, n
		UNWIND nodeLabels AS label
		RETURN label, count(*) AS count
		ORDER BY count DESC, label ASC
		LIMIT $limit
	`, map[string]interface{}{"limit": limit})
	if err != nil {
		return &heimdall.ActionResult{
			Success: false,
			Message: fmt.Sprintf("failed to build repository map: %v", err),
		}, nil
	}

	labels := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		labels = append(labels, row)
	}

	message := fmt.Sprintf("Repository graph map: %d nodes, %d relationships, %d label groups",
		stats.NodeCount, stats.RelationshipCount, len(labels))
	p.addEvent("info", "Repository graph map generated", map[string]interface{}{"database": dbName, "limit": limit})
	p.mu.RLock()
	config := p.configSnapshotLocked()
	p.mu.RUnlock()

	relTypeRows, err := ctx.Database.Query(ctx.Context, dbName, `
		MATCH ()-[r]->()
		RETURN type(r) AS relationship_type, count(*) AS count
		ORDER BY count DESC, relationship_type ASC
		LIMIT $limit
	`, map[string]interface{}{"limit": limit})
	if err != nil {
		relTypeRows = nil
	}
	relTypes := make([]map[string]interface{}, 0, len(relTypeRows))
	for _, row := range relTypeRows {
		relTypes = append(relTypes, row)
	}

	return &heimdall.ActionResult{
		Success: true,
		Message: message,
		Data: map[string]interface{}{
			"database":               dbName,
			"node_count":             stats.NodeCount,
			"relationship_count":     stats.RelationshipCount,
			"label_counts":           stats.LabelCounts,
			"top_label_groups":       labels,
			"top_relationship_types": relTypes,
			"history_strategy":       config["history_strategy"],
			"response_style":         config["response_style"],
		},
	}, nil
}

// actionAutocompleteSuggest returns database schema (labels, relationship types, properties) for Cypher autocomplete.
func (p *WatcherPlugin) actionAutocompleteSuggest(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	query, _ := ctx.Params["query"].(string)
	if query == "" {
		return &heimdall.ActionResult{
			Success: false,
			Message: "query parameter required",
		}, nil
	}

	var labels, properties, relTypes []string
	var suggestions []string
	if ctx.Database != nil {
		dbName, _ := ctx.Params["database"].(string)
		if dbName == "" {
			dbName, _ = ctx.Params["db"].(string)
		}
		labelResults, err := ctx.Database.Query(ctx.Context, dbName, "CALL db.labels() YIELD label RETURN label", nil)
		if err == nil {
			for _, row := range labelResults {
				if label, ok := row["label"].(string); ok {
					labels = append(labels, label)
				}
			}
		}
		relResults, err := ctx.Database.Query(ctx.Context, dbName, "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType", nil)
		if err == nil {
			for _, row := range relResults {
				if relType, ok := row["relationshipType"].(string); ok {
					relTypes = append(relTypes, relType)
				}
			}
		}
		propResults, err := ctx.Database.Query(ctx.Context, dbName, `
			MATCH (n) WITH n, keys(n) as props UNWIND props as prop RETURN DISTINCT prop LIMIT 50
		`, nil)
		if err == nil {
			for _, row := range propResults {
				if prop, ok := row["prop"].(string); ok {
					properties = append(properties, prop)
				}
			}
		}
		if len(labels) > 0 {
			suggestions = append(suggestions, fmt.Sprintf("MATCH (n:%s) RETURN n LIMIT 25", labels[0]))
		}
		if len(relTypes) > 0 {
			suggestions = append(suggestions, fmt.Sprintf("MATCH ()-[r:%s]->() RETURN r LIMIT 25", relTypes[0]))
		}
		if len(properties) > 0 {
			suggestions = append(suggestions, fmt.Sprintf("MATCH (n) WHERE exists(n.%s) RETURN n LIMIT 25", properties[0]))
		}
	}
	suggestion := ""
	if len(suggestions) > 0 {
		suggestion = suggestions[0]
	}
	schemaInfo := map[string]interface{}{
		"labels":     labels,
		"properties": properties,
		"relTypes":   relTypes,
	}
	return &heimdall.ActionResult{
		Success: true,
		Message: "Autocomplete suggestions",
		Data: map[string]interface{}{
			"query":       query,
			"schema":      schemaInfo,
			"suggestions": suggestions,
			"suggestion":  suggestion,
		},
	}, nil
}

// actionDiscover runs semantic search on the graph and returns formatted results for the LLM.
func (p *WatcherPlugin) actionDiscover(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	query, _ := ctx.Params["query"].(string)
	if query == "" {
		return &heimdall.ActionResult{
			Success: false,
			Message: "query parameter required",
		}, nil
	}
	limit := 10
	if l, ok := ctx.Params["limit"].(float64); ok && l > 0 && l <= 50 {
		limit = int(l)
	}
	depth := 1 // relationship hop from matched nodes (1 = direct neighbors)
	if d, ok := ctx.Params["depth"].(float64); ok && d >= 0 && d <= 5 {
		depth = int(d)
	}
	dbName := getDatabaseParam(ctx.Params)
	if dbName == "" && ctx.Database != nil {
		dbName = ctx.Database.DefaultDatabaseName()
	}

	if ctx.Database == nil {
		return &heimdall.ActionResult{Success: false, Message: "database not available"}, nil
	}

	discoverCtx, cancel := context.WithTimeout(ctx.Context, 15*time.Second)
	defer cancel()
	result, err := ctx.Database.Discover(discoverCtx, dbName, query, nil, limit, depth)
	if err != nil {
		log.Printf("[Watcher] discover failed: %v", err)
		return &heimdall.ActionResult{
			Success: false,
			Message: fmt.Sprintf("Search failed: %v", err),
		}, nil
	}
	formatted := formatDiscoverResult(result)
	// Put formatted context in Message so the agentic loop sends readable context to the LLM
	msg := formatted
	if len(msg) > 8000 {
		msg = msg[:8000] + "\n... (truncated)"
	}
	if msg == "" {
		msg = fmt.Sprintf("Found %d result(s) (%s search); no formatted summary.", result.Total, result.Method)
	}
	p.addEvent("info", "Discover: semantic search executed", map[string]interface{}{
		"query": query, "total": result.Total, "method": result.Method,
	})
	return &heimdall.ActionResult{
		Success: true,
		Message: msg,
		Data:    map[string]interface{}{"results": result.Results, "total": result.Total},
	}, nil
}

// actionQuery executes a read-only Cypher query and returns rows for the LLM.
func (p *WatcherPlugin) actionQuery(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	cypher, _ := ctx.Params["cypher"].(string)
	if cypher == "" {
		return &heimdall.ActionResult{
			Success: false,
			Message: "cypher parameter required",
		}, nil
	}
	if len(cypher) > 10000 {
		return &heimdall.ActionResult{
			Success: false,
			Message: "query too long (max 10000 characters)",
		}, nil
	}
	if err := validateReadOnlyCypher(cypher); err != nil {
		return &heimdall.ActionResult{
			Success: false,
			Message: err.Error(),
		}, nil
	}
	dbName := getDatabaseParam(ctx.Params)
	if dbName == "" && ctx.Database != nil {
		dbName = ctx.Database.DefaultDatabaseName()
	}
	var params map[string]interface{}
	if p, ok := ctx.Params["params"].(map[string]interface{}); ok {
		params = p
	}

	if ctx.Database == nil {
		return &heimdall.ActionResult{Success: false, Message: "database not available"}, nil
	}

	rows, err := ctx.Database.Query(ctx.Context, dbName, cypher, params)
	if err != nil {
		log.Printf("[Watcher] query failed (database=%s query_len=%d param_count=%d)", dbName, len(cypher), len(params))
		return &heimdall.ActionResult{
			Success: false,
			Message: fmt.Sprintf("Query failed: %v", err),
		}, nil
	}
	p.addEvent("info", "Query: Cypher executed", map[string]interface{}{
		"database": dbName, "rows": len(rows),
	})
	return &heimdall.ActionResult{
		Success: true,
		Message: fmt.Sprintf("Query returned %d row(s)", len(rows)),
		Data:    map[string]interface{}{"rows": rows},
	}, nil
}

// actionHello is a simple test action to verify Heimdall is working.
// Prompt examples that should trigger this:
//   - "say hello"
//   - "test the system"
//   - "hello world"
//   - "run a test action"
func (p *WatcherPlugin) actionHello(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	name := "World"
	if n, ok := ctx.Params["name"].(string); ok && n != "" {
		name = n
	}

	greeting := fmt.Sprintf("Hello, %s! 👋 Heimdall is operational and ready to serve.", name)
	p.addEvent("info", greeting, nil)

	return &heimdall.ActionResult{
		Success: true,
		Message: greeting,
	}, nil
}

func (p *WatcherPlugin) actionStatus(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()
	config := p.Config()
	statusValue := p.Status()
	p.mu.RLock()
	facts := append([]string(nil), p.facts...)
	p.mu.RUnlock()

	status := map[string]interface{}{
		"coding_plugin": map[string]interface{}{
			"status":  statusValue,
			"config":  config,
			"summary": p.Summary(),
			"facts":   facts,
		},
	}

	if ctx.Database != nil {
		dbName := getDatabaseParam(ctx.Params)
		dbStats, err := ctx.Database.Stats(dbName)
		if err == nil {
			status["database"] = map[string]interface{}{
				"database":      coalesceString(dbName, ctx.Database.DefaultDatabaseName()),
				"nodes":         dbStats.NodeCount,
				"relationships": dbStats.RelationshipCount,
				"labels":        dbStats.LabelCounts,
			}
		} else {
			status["database"] = map[string]interface{}{
				"database": coalesceString(dbName, ctx.Database.DefaultDatabaseName()),
				"error":    err.Error(),
			}
		}
	}

	return &heimdall.ActionResult{
		Success: true,
		Message: fmt.Sprintf("Coding plugin status: %s, strategy=%v, scope=%v",
			statusValue, config["history_strategy"], config["summary_scope"]),
		Data: status,
	}, nil
}

// actionDBStats returns coding-focused repository graph statistics.
func (p *WatcherPlugin) actionDBStats(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()
	p.mu.RLock()
	config := p.configSnapshotLocked()
	p.mu.RUnlock()

	stats := map[string]interface{}{}
	var msgBuilder strings.Builder

	if ctx.Database != nil {
		dbName := getDatabaseParam(ctx.Params)
		dbStats, err := ctx.Database.Stats(dbName)
		if err != nil {
			stats["database"] = map[string]interface{}{
				"database": coalesceString(dbName, ctx.Database.DefaultDatabaseName()),
				"error":    err.Error(),
			}
			msgBuilder.WriteString(fmt.Sprintf("DATABASE: error reading stats for %s: %v\n\n",
				coalesceString(dbName, ctx.Database.DefaultDatabaseName()), err))
		} else {
			stats["database"] = map[string]interface{}{
				"database":      coalesceString(dbName, ctx.Database.DefaultDatabaseName()),
				"nodes":         dbStats.NodeCount,
				"relationships": dbStats.RelationshipCount,
				"labels":        dbStats.LabelCounts,
			}
			msgBuilder.WriteString(fmt.Sprintf("REPOSITORY GRAPH (%s): %d nodes, %d relationships\n\n",
				coalesceString(dbName, ctx.Database.DefaultDatabaseName()),
				dbStats.NodeCount, dbStats.RelationshipCount))

			if dbStats.ClusterStats != nil {
				stats["clustering"] = map[string]interface{}{
					"embedding_count":    dbStats.ClusterStats.EmbeddingCount,
					"num_clusters":       dbStats.ClusterStats.NumClusters,
					"is_clustered":       dbStats.ClusterStats.IsClustered,
					"avg_cluster_size":   dbStats.ClusterStats.AvgClusterSize,
					"cluster_iterations": dbStats.ClusterStats.Iterations,
				}
				msgBuilder.WriteString("RETRIEVAL COVERAGE:\n")
				msgBuilder.WriteString(fmt.Sprintf("  • Embeddings: %d\n", dbStats.ClusterStats.EmbeddingCount))
				msgBuilder.WriteString(fmt.Sprintf("  • Clusters: %d\n", dbStats.ClusterStats.NumClusters))
				if dbStats.ClusterStats.IsClustered {
					msgBuilder.WriteString(fmt.Sprintf("  • Clustered: Yes (avg size: %.1f, iterations: %d)\n",
						dbStats.ClusterStats.AvgClusterSize, dbStats.ClusterStats.Iterations))
				} else {
					msgBuilder.WriteString("  • Clustered: No\n")
				}
				msgBuilder.WriteString("\n")
			}
		}
	}

	stats["context_strategy"] = map[string]interface{}{
		"history_strategy": config["history_strategy"],
		"summary_scope":    config["summary_scope"],
		"response_style":   config["response_style"],
	}
	msgBuilder.WriteString("CONTEXT STRATEGY:\n")
	msgBuilder.WriteString(fmt.Sprintf("  • History Strategy: %v\n", config["history_strategy"]))
	msgBuilder.WriteString(fmt.Sprintf("  • Summary Scope: %v\n", config["summary_scope"]))
	msgBuilder.WriteString(fmt.Sprintf("  • Response Style: %v\n", config["response_style"]))

	return &heimdall.ActionResult{
		Success: true,
		Message: msgBuilder.String(),
		Data:    stats,
	}, nil
}

func (p *WatcherPlugin) Summary() string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return fmt.Sprintf("Heimdall coding plugin: Status=%s, Model=%s, HistoryStrategy=%v, SummaryScope=%v, Uptime=%.0fs, Requests=%d, Errors=%d",
		p.status,
		p.config["model"],
		p.config["history_strategy"],
		p.config["summary_scope"],
		time.Since(p.started).Seconds(),
		p.requests,
		p.errors,
	)
}

func (p *WatcherPlugin) RecentEvents(limit int) []heimdall.SubsystemEvent {
	p.eventMu.RLock()
	defer p.eventMu.RUnlock()

	if limit <= 0 || limit > len(p.events) {
		limit = len(p.events)
	}

	start := len(p.events) - limit
	if start < 0 {
		start = 0
	}

	result := make([]heimdall.SubsystemEvent, limit)
	copy(result, p.events[start:])
	return result
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func getDatabaseParam(params map[string]interface{}) string {
	if params == nil {
		return ""
	}
	if v, ok := params["database"].(string); ok {
		return strings.TrimSpace(v)
	}
	if v, ok := params["db"].(string); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

func coalesceString(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func (p *WatcherPlugin) PrePrompt(ctx *heimdall.PromptContext) error {
	msgPreview := ctx.UserMessage
	if len(msgPreview) > 50 {
		msgPreview = msgPreview[:50] + "..."
	}
	log.Printf("[HeimdallCodingAgent] PrePrompt: request=%s user_msg_len=%d", ctx.RequestID, len(msgPreview))

	p.mu.RLock()
	config := p.configSnapshotLocked()
	db := p.ctx.Database
	p.mu.RUnlock()

	ctx.NotifyProgress("Repository Graph", "Searching repository knowledge graph...")
	ragContext := p.performGraphRAG(ctx, db)
	if ragContext != "" {
		log.Printf("[HeimdallCodingAgent] PrePrompt: request=%s user_msg_len=%d", ctx.RequestID, len(msgPreview))
		ctx.NotifyInfo("Repository Graph", "Found relevant repository context")
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	if ctx.PluginData == nil {
		ctx.PluginData = make(map[string]interface{})
	}
	ctx.PluginData["coding_agent_preprompt_time"] = time.Now()
	ctx.PluginData["coding_agent_goroutines"] = runtime.NumGoroutine()
	ctx.PluginData["coding_agent_memory_mb"] = m.Alloc / 1024 / 1024
	ctx.PluginData["coding_agent_history_strategy"] = config["history_strategy"]
	ctx.PluginData["coding_agent_summary_scope"] = config["summary_scope"]
	p.refreshSessionMemoryFromGraph(ctx.Context, ctx.UserMessage, db, fmt.Sprint(config["summary_scope"]))
	p.mu.RLock()
	ctx.PluginData["coding_agent_session_summary"] = p.summary
	ctx.PluginData["coding_agent_session_facts"] = append([]string(nil), p.facts...)
	p.mu.RUnlock()

	retrievedMemory := p.buildSessionMemoryContext(ctx.UserMessage)
	if retrievedMemory != "" {
		ctx.AdditionalInstructions += retrievedMemory
		ctx.NotifyInfo("Coding Memory", "Recovered summarized session context")
	}

	ctx.AdditionalInstructions += "\n\nADDITIONAL CODING AGENT INSTRUCTIONS:\n"
	ctx.AdditionalInstructions += "- You are operating as Heimdall's built-in coding agent for NornicDB.\n"
	ctx.AdditionalInstructions += "- Prefer repository-aware reasoning before answering implementation questions.\n"
	ctx.AdditionalInstructions += "- Do not skip repository retrieval for short meta-questions; map the codebase whenever that improves correctness.\n"
	ctx.AdditionalInstructions += "- Use heimdall_watcher_repo_map first when you need to understand project structure or likely integration points.\n"
	ctx.AdditionalInstructions += "- Use heimdall_watcher_search for semantic lookup of features, files, symbols, files, and concepts.\n"
	ctx.AdditionalInstructions += "- Use heimdall_watcher_query only for targeted graph inspection when explicit Cypher is warranted.\n"
	ctx.AdditionalInstructions += "- When the conversation becomes long, summarize the coding work so far, preserve the summary, and preserve key implementation facts so they can be re-retrieved via Graph-RAG.\n"
	ctx.AdditionalInstructions += "- Summarize after major milestones, when tool-call history becomes noisy, before context pressure causes loss, and before switching tasks or subsystems.\n"
	ctx.AdditionalInstructions += "- The stored summary should capture user goals, decisions, touched files, open questions, and next steps. Key facts should be stored as atomic retrievable items.\n"
	ctx.AdditionalInstructions += "- If memory or in-process tools are available, use them to persist summarized chat history and key facts; then use retrieval to rebuild context instead of relying on raw transcript alone.\n"
	ctx.AdditionalInstructions += "- When the graph context is sufficient, answer directly with implementation guidance.\n"

	ctx.Examples = append(ctx.Examples,
		heimdall.PromptExample{
			UserSays:   "hello",
			ActionJSON: `{"action": "heimdall_watcher_hello", "params": {}}`,
		},
		heimdall.PromptExample{
			UserSays:   "map this repository before we change anything",
			ActionJSON: `{"action": "heimdall_watcher_repo_map", "params": {}}`,
		},
		heimdall.PromptExample{
			UserSays:   "show me the project structure",
			ActionJSON: `{"action": "heimdall_watcher_repo_map", "params": {}}`,
		},
		heimdall.PromptExample{
			UserSays:   "find the code related to plugin loading",
			ActionJSON: `{"action": "heimdall_watcher_search", "params": {"query": "plugin loading"}}`,
		},
		heimdall.PromptExample{
			UserSays:   "find the implementation for the heimdall plugin system",
			ActionJSON: `{"action": "heimdall_watcher_search", "params": {"query": "heimdall plugin system implementation"}}`,
		},
		heimdall.PromptExample{
			UserSays:   "show graph coverage for this repository",
			ActionJSON: `{"action": "heimdall_watcher_db_stats", "params": {}}`,
		},
		heimdall.PromptExample{
			UserSays:   "run a graph query to inspect plugin nodes",
			ActionJSON: `{"action": "heimdall_watcher_query", "params": {"cypher": "MATCH (n) WHERE any(label IN labels(n) WHERE toLower(label) CONTAINS 'plugin') RETURN labels(n) AS labels, count(*) AS count LIMIT 25"}}`,
		},
	)

	p.addEvent("info", "PrePrompt hook executed for coding agent", map[string]interface{}{
		"request_id":   ctx.RequestID,
		"user_msg":     ctx.UserMessage[:min(50, len(ctx.UserMessage))],
		"has_history":  len(ctx.Messages) > 0,
		"rag_enriched": ragContext != "",
	})

	return nil
}

func (p *WatcherPlugin) performGraphRAG(ctx *heimdall.PromptContext, db heimdall.DatabaseRouter) string {
	if db == nil || len(ctx.UserMessage) < 5 {
		return ""
	}

	result, err := db.Discover(
		ctx.Context,
		"",
		ctx.UserMessage,
		nil,
		5,
		1,
	)
	if err != nil {
		log.Printf("[HeimdallCodingAgent] Graph-RAG search failed")
		return ""
	}

	if result.Total == 0 {
		return ""
	}
	return formatDiscoverResult(result)
}

var internalDiscoverPropertyKeys = map[string]bool{
	"embedding":            true,
	"embedding_model":      true,
	"embedding_dimensions": true,
	"has_embedding":        true,
	"embedded_at":          true,
	"chunk_count":          true,
	"orderLevelLookupKey":  true,
	"fetchedAt":            true,
	"id":                   true,
	"created_at":           true,
	"updated_at":           true,
}

func formatDiscoverResult(result *heimdall.DiscoverResult) string {
	if result == nil || len(result.Results) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("\n\n=== REPOSITORY KNOWLEDGE FROM GRAPH DATABASE ===\n")
	sb.WriteString("The following repository information was found in the knowledge graph and may help answer the coding request:\n\n")

	for i, r := range result.Results {
		title := r.Title
		if title == "" {
			title = r.ID
		}
		sb.WriteString(fmt.Sprintf("### %d. [%s] %s\n", i+1, r.Type, title))

		if r.ContentPreview != "" {
			sb.WriteString(fmt.Sprintf("Content: %s\n", r.ContentPreview))
		}

		if len(r.Properties) > 0 {
			sb.WriteString("Properties:\n")
			for k, v := range r.Properties {
				if internalDiscoverPropertyKeys[k] {
					continue
				}
				valStr := fmt.Sprint(v)
				if len(valStr) > 300 {
					valStr = valStr[:300] + "..."
				}
				sb.WriteString(fmt.Sprintf("  - %s: %s\n", k, valStr))
			}
		}

		if len(r.Related) > 0 {
			sb.WriteString("Related:\n")
			shown := 0
			for _, rel := range r.Related {
				if shown >= 3 {
					sb.WriteString(fmt.Sprintf("  ... and %d more related items\n", len(r.Related)-3))
					break
				}
				relTitle := rel.Title
				if relTitle == "" {
					relTitle = rel.ID
				}
				direction := "→"
				if rel.Direction == "incoming" {
					direction = "←"
				}
				sb.WriteString(fmt.Sprintf("  %s [%s] %s (via %s)\n", direction, rel.Type, relTitle, rel.Relationship))
				shown++
			}
		}
		sb.WriteString("\n")
	}

	sb.WriteString("=== END REPOSITORY KNOWLEDGE ===\n\n")
	sb.WriteString("The above lists repository graph nodes (with Properties) and their Related nodes (relationship links). Use this information to answer the user with coding-oriented guidance. Do not call another action when this knowledge already answers the request.\n")

	return sb.String()
}

func (p *WatcherPlugin) PreExecute(ctx *heimdall.PreExecuteContext, done func(heimdall.PreExecuteResult)) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	log.Printf("[HeimdallCodingAgent] PreExecute: request=%s action=%s param_count=%d", ctx.RequestID, ctx.Action, len(ctx.Params))
	ctx.NotifyInfo("\nHeimdall Coding Agent", fmt.Sprintf("Executing action: %s\n", ctx.Action))

	p.addEvent("info", fmt.Sprintf("PreExecute: %s", ctx.Action), map[string]interface{}{
		"request_id": ctx.RequestID,
		"action":     ctx.Action,
		"params":     ctx.Params,
	})

	go func() {
		if ctx.Action == "heimdall_watcher_query" {
			if cypher, ok := ctx.Params["cypher"].(string); ok {
				if len(cypher) > 10000 {
					ctx.NotifyWarning("Cypher Validation", "Query too long, aborting")
					done(heimdall.PreExecuteResult{Continue: false, AbortMessage: "Query too long (max 10000 chars)"})
					return
				}
				ctx.NotifyProgress("Cypher Analysis", "Validating repository graph query...")
			}
		}
		done(heimdall.PreExecuteResult{Continue: true})
	}()
}

func (p *WatcherPlugin) PostExecute(ctx *heimdall.PostExecuteContext) {
	log.Printf("[HeimdallCodingAgent] PostExecute: request=%s action=%s duration=%v", ctx.RequestID, ctx.Action, ctx.Duration)

	if ctx.WasCancelled && ctx.CancellationInfo != nil {
		p.addEvent("warning", fmt.Sprintf("Request was cancelled: %s", ctx.CancellationInfo.Reason), map[string]interface{}{
			"request_id":   ctx.RequestID,
			"cancelled_by": ctx.CancellationInfo.CancelledBy,
			"phase":        ctx.CancellationInfo.Phase,
		})
		return
	}

	executionTime := float64(ctx.Duration.Microseconds()) / 1000
	p.addEvent("info", fmt.Sprintf("PostExecute: %s (%.2fms)", ctx.Action, executionTime), map[string]interface{}{
		"request_id": ctx.RequestID,
		"action":     ctx.Action,
		"duration":   ctx.Duration.String(),
		"success":    ctx.Result != nil && ctx.Result.Success,
	})

	if ctx.Result != nil && !ctx.Result.Success {
		p.mu.Lock()
		p.errors++
		p.mu.Unlock()
	}
	p.captureSessionMemory(ctx.Context, ctx.Action, ctx.Params, ctx.Result)

	if ctx.Result != nil && ctx.Result.Success {
		ctx.NotifySuccess("\nHeimdall Coding Agent", fmt.Sprintf("Action completed in %.2fms\n", executionTime))
	} else if ctx.Result != nil {
		ctx.NotifyError("\nHeimdall Coding Agent", fmt.Sprintf("Action failed: %s\n", ctx.Result.Message))
	}
}

func (p *WatcherPlugin) Synthesize(ctx *heimdall.SynthesisContext, done func(response string)) {
	if ctx.Result == nil || !ctx.Result.Success {
		done("")
		return
	}

	if len(ctx.Result.Data) == 0 {
		done(ctx.Result.Message)
		return
	}

	p.mu.RLock()
	heimdallInvoker := p.ctx.Heimdall
	p.mu.RUnlock()
	if heimdallInvoker == nil {
		log.Printf("[HeimdallCodingAgent] Heimdall invoker not available, using pre-formatted message")
		if ctx.Result.Message != "" {
			done(ctx.Result.Message)
			return
		}
		done("")
		return
	}

	synthesisPrompt := p.buildSynthesisPrompt(ctx.UserQuestion, ctx.Result.Message, ctx.Result.Data)
	log.Printf("[HeimdallCodingAgent] Generating LLM synthesis for request question_len=%d", len(ctx.UserQuestion))

	result, err := heimdallInvoker.SendRawPrompt(ctx.Context, synthesisPrompt)
	if err != nil {
		log.Printf("[HeimdallCodingAgent] LLM synthesis failed: %v, falling back to formatted message", err)
		if ctx.Result.Message != "" {
			done(ctx.Result.Message)
			return
		}
		done("")
		return
	}

	if result == nil || !result.Success || result.Message == "" {
		log.Printf("[HeimdallCodingAgent] LLM returned empty/unsuccessful result, using formatted message")
		if ctx.Result.Message != "" {
			done(ctx.Result.Message)
			return
		}
		done("")
		return
	}

	response := strings.TrimSpace(result.Message)
	if strings.HasPrefix(response, "{") || strings.HasPrefix(response, "[") {
		log.Printf("[HeimdallCodingAgent] LLM returned JSON instead of prose, using formatted message")
		if ctx.Result.Message != "" {
			done(ctx.Result.Message)
			return
		}
		done("")
		return
	}

	if len(response) < 50 && ctx.Result.Message != "" && len(ctx.Result.Message) > len(response)*2 {
		log.Printf("[HeimdallCodingAgent] LLM response too short (%d chars), using formatted message (%d chars)", len(response), len(ctx.Result.Message))
		done(ctx.Result.Message)
		return
	}

	log.Printf("[HeimdallCodingAgent] LLM synthesis successful (%d chars)", len(response))
	done(response)
}

func (p *WatcherPlugin) buildSynthesisPrompt(userQuestion, actionMessage string, data map[string]interface{}) string {
	dataJSON, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		dataJSON = []byte(fmt.Sprintf("%v", data))
	}

	var prompt strings.Builder
	prompt.WriteString("You are Heimdall, the coding assistant for NornicDB. Answer the user's question using the repository and graph-backed data provided.\n\n")
	prompt.WriteString(fmt.Sprintf("USER QUESTION: %s\n\n", userQuestion))
	if actionMessage != "" {
		prompt.WriteString("FORMATTED DATA:\n")
		prompt.WriteString(actionMessage)
		prompt.WriteString("\n\n")
	}
	prompt.WriteString("RAW DATA (JSON):\n")
	prompt.WriteString(string(dataJSON))
	prompt.WriteString("\n\n")
	prompt.WriteString("INSTRUCTIONS:\n")
	prompt.WriteString("1. Answer the user's coding question directly and completely.\n")
	prompt.WriteString("2. Preserve important implementation details, decisions, and file-level insights.\n")
	prompt.WriteString("3. Prefer concise technical prose over generic assistant language.\n")
	prompt.WriteString("4. Do NOT output JSON.\n\n")
	prompt.WriteString("YOUR RESPONSE:")
	return prompt.String()
}

func (p *WatcherPlugin) addEvent(eventType, message string, data map[string]interface{}) {
	p.eventMu.Lock()
	defer p.eventMu.Unlock()

	event := heimdall.SubsystemEvent{
		Time:    time.Now(),
		Type:    eventType,
		Message: message,
		Data:    data,
	}

	p.events = append(p.events, event)
	if len(p.events) > 100 {
		p.events = p.events[len(p.events)-100:]
	}
}

func (p *WatcherPlugin) buildSessionMemoryContext(userMessage string) string {
	if strings.TrimSpace(p.summary) == "" && len(p.facts) == 0 {
		return ""
	}

	matchedFacts := p.selectRelevantFacts(userMessage, 5)
	if strings.TrimSpace(p.summary) == "" && len(matchedFacts) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("\n\n=== SUMMARIZED CODING SESSION MEMORY ===\n")
	if strings.TrimSpace(p.summary) != "" {
		sb.WriteString("Session Summary:\n")
		sb.WriteString(p.summary)
		sb.WriteString("\n")
	}
	if len(matchedFacts) > 0 {
		sb.WriteString("Relevant Stored Facts:\n")
		for _, fact := range matchedFacts {
			sb.WriteString("- ")
			sb.WriteString(fact)
			sb.WriteString("\n")
		}
	}
	sb.WriteString("Use this stored session memory to maintain continuity across long coding conversations.\n")
	return sb.String()
}

func (p *WatcherPlugin) selectRelevantFacts(userMessage string, limit int) []string {
	if limit <= 0 || len(p.facts) == 0 {
		return nil
	}
	queryTerms := tokenizeForMemory(userMessage)
	if len(queryTerms) == 0 {
		if len(p.facts) < limit {
			limit = len(p.facts)
		}
		return append([]string(nil), p.facts[:limit]...)
	}

	type scoredFact struct {
		fact  string
		score int
	}
	scored := make([]scoredFact, 0, len(p.facts))
	for _, fact := range p.facts {
		score := 0
		factLower := strings.ToLower(fact)
		for _, term := range queryTerms {
			if strings.Contains(factLower, term) {
				score++
			}
		}
		if score > 0 {
			scored = append(scored, scoredFact{fact: fact, score: score})
		}
	}

	if len(scored) == 0 {
		if len(p.facts) < limit {
			limit = len(p.facts)
		}
		start := len(p.facts) - limit
		if start < 0 {
			start = 0
		}
		return append([]string(nil), p.facts[start:]...)
	}

	for i := 0; i < len(scored)-1; i++ {
		for j := i + 1; j < len(scored); j++ {
			if scored[j].score > scored[i].score {
				scored[i], scored[j] = scored[j], scored[i]
			}
		}
	}
	if len(scored) < limit {
		limit = len(scored)
	}
	result := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		result = append(result, scored[i].fact)
	}
	return result
}

func (p *WatcherPlugin) captureSessionMemory(ctx context.Context, action string, params map[string]interface{}, result *heimdall.ActionResult) {
	if result == nil {
		return
	}

	newFacts := make([]string, 0, 10)
	if result.Success {
		newFacts = append(newFacts, fmt.Sprintf("Successful action: %s", action))
	} else {
		newFacts = append(newFacts, fmt.Sprintf("Failed action: %s (%s)", action, strings.TrimSpace(result.Message)))
	}

	for _, fact := range extractFactsFromAction(action, params, result) {
		newFacts = append(newFacts, fact)
	}

	for _, fact := range newFacts {
		p.appendFactLocked(fact)
	}

	p.rebuildSummaryLocked()
	p.mu.RLock()
	summary := p.summary
	config := p.configSnapshotLocked()
	db := p.ctx.Database
	p.mu.RUnlock()
	p.persistSessionMemoryToGraph(ctx, db, p.Name(), fmt.Sprint(config["summary_scope"]), summary, fmt.Sprint(config["history_strategy"]), fmt.Sprint(config["model"]), action, newFacts)
}

func (p *WatcherPlugin) appendFactLocked(fact string) {
	fact = strings.TrimSpace(fact)
	if fact == "" {
		return
	}
	for _, existing := range p.facts {
		if existing == fact {
			return
		}
	}
	p.facts = append(p.facts, fact)
	if len(p.facts) > 50 {
		p.facts = p.facts[len(p.facts)-50:]
	}
}

func (p *WatcherPlugin) rebuildSummaryLocked() {
	if len(p.facts) == 0 {
		p.summary = ""
		return
	}
	start := len(p.facts) - 8
	if start < 0 {
		start = 0
	}
	var sb strings.Builder
	sb.WriteString("Recent coding session facts:\n")
	for _, fact := range p.facts[start:] {
		if strings.TrimSpace(fact) == "" {
			continue
		}
		sb.WriteString("- ")
		sb.WriteString(fact)
		sb.WriteString("\n")
	}
	p.summary = strings.TrimSpace(sb.String())
}

func extractFactsFromAction(action string, params map[string]interface{}, result *heimdall.ActionResult) []string {
	facts := make([]string, 0, 8)
	if query, ok := params["query"].(string); ok && strings.TrimSpace(query) != "" {
		facts = append(facts, fmt.Sprintf("Query investigated: %s", strings.TrimSpace(query)))
	}
	if cypher, ok := params["cypher"].(string); ok && strings.TrimSpace(cypher) != "" {
		facts = append(facts, fmt.Sprintf("Cypher inspected: %s", truncateForFact(strings.TrimSpace(cypher), 160)))
	}
	if result != nil && strings.TrimSpace(result.Message) != "" {
		facts = append(facts, fmt.Sprintf("%s result: %s", action, truncateForFact(strings.TrimSpace(result.Message), 200)))
	}
	if result != nil && result.Data != nil {
		if db, ok := result.Data["database"].(string); ok && strings.TrimSpace(db) != "" {
			facts = append(facts, fmt.Sprintf("Database in focus: %s", strings.TrimSpace(db)))
		}
		if nodeCount, ok := result.Data["node_count"]; ok {
			facts = append(facts, fmt.Sprintf("Repository node count observed: %v", nodeCount))
		}
		if relationshipCount, ok := result.Data["relationship_count"]; ok {
			facts = append(facts, fmt.Sprintf("Repository relationship count observed: %v", relationshipCount))
		}
		if total, ok := result.Data["total"]; ok {
			facts = append(facts, fmt.Sprintf("Discover results returned: %v", total))
		}
	}
	return facts
}

func truncateForFact(value string, maxLen int) string {
	if maxLen <= 0 || len(value) <= maxLen {
		return value
	}
	return value[:maxLen] + "..."
}

func tokenizeForMemory(value string) []string {
	value = strings.ToLower(value)
	replacer := strings.NewReplacer(",", " ", ".", " ", ":", " ", ";", " ", "(", " ", ")", " ", "{", " ", "}", " ", "[", " ", "]", " ", "\n", " ", "\t", " ")
	value = replacer.Replace(value)
	parts := strings.Fields(value)
	terms := make([]string, 0, len(parts))
	for _, part := range parts {
		if len(part) < 3 {
			continue
		}
		terms = append(terms, part)
	}
	return terms
}

func (p *WatcherPlugin) persistSessionMemoryToGraph(ctx context.Context, db heimdall.DatabaseRouter, pluginName, scope, summary, historyStrategy, model, action string, facts []string) {
	if db == nil {
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	dbName := db.DefaultDatabaseName()

	_, err := db.Query(ctx, dbName, `
		MERGE (m:HeimdallSessionMemory {plugin: $plugin, scope: $scope})
		SET m.summary = $summary,
		    m.updated_at = $updated_at,
		    m.history_strategy = $history_strategy,
		    m.model = $model
		RETURN m.summary AS summary
	`, map[string]interface{}{
		"plugin":           pluginName,
		"scope":            scope,
		"summary":          summary,
		"updated_at":       now,
		"history_strategy": historyStrategy,
		"model":            model,
	})
	if err != nil {
		log.Printf("[HeimdallCodingAgent] failed to persist session summary")
		return
	}

	for _, fact := range facts {
		fact = strings.TrimSpace(fact)
		if fact == "" {
			continue
		}
		_, err := db.Query(ctx, dbName, `
			MERGE (m:HeimdallSessionMemory {plugin: $plugin, scope: $scope})
			MERGE (f:HeimdallSessionFact {plugin: $plugin, scope: $scope, fact: $fact, action: $action})
			ON CREATE SET f.first_seen_at = $created_at,
				f.created_at = $created_at,
				f.hit_count = 1
			SET f.last_seen_at = $created_at,
				f.hit_count = coalesce(f.hit_count, 0) + 1
			MERGE (m)-[:HAS_FACT]->(f)
			RETURN f.fact AS fact
		`, map[string]interface{}{
			"plugin":     pluginName,
			"scope":      scope,
			"fact":       fact,
			"action":     action,
			"created_at": now,
		})
		if err != nil {
			log.Printf("[HeimdallCodingAgent] failed to persist session fact")
		}
	}

	_, err = db.Query(ctx, dbName, `
		MATCH (:HeimdallSessionMemory {plugin: $plugin, scope: $scope})-[:HAS_FACT]->(f:HeimdallSessionFact {plugin: $plugin, scope: $scope})
		WITH f
		ORDER BY coalesce(f.last_seen_at, f.created_at) DESC, f.fact ASC
		SKIP 100
		DETACH DELETE f
	`, map[string]interface{}{
		"plugin": pluginName,
		"scope":  scope,
	})
	if err != nil {
		log.Printf("[HeimdallCodingAgent] failed to prune session facts")
	}
}

func (p *WatcherPlugin) refreshSessionMemoryFromGraph(ctx context.Context, userMessage string, db heimdall.DatabaseRouter, scope string) {
	if db == nil {
		return
	}
	dbName := db.DefaultDatabaseName()

	summaryRows, err := db.Query(ctx, dbName, `
		MATCH (m:HeimdallSessionMemory {plugin: $plugin, scope: $scope})
		RETURN m.summary AS summary
		LIMIT 1
	`, map[string]interface{}{
		"plugin": p.Name(),
		"scope":  scope,
	})
	loadedSummary := ""
	if err == nil && len(summaryRows) > 0 {
		if summary, ok := summaryRows[0]["summary"].(string); ok && strings.TrimSpace(summary) != "" {
			loadedSummary = strings.TrimSpace(summary)
		}
	}

	factRows, err := db.Query(ctx, dbName, `
		MATCH (:HeimdallSessionMemory {plugin: $plugin, scope: $scope})-[:HAS_FACT]->(f:HeimdallSessionFact {plugin: $plugin, scope: $scope})
		RETURN f.fact AS fact
		ORDER BY f.created_at DESC
		LIMIT 50
	`, map[string]interface{}{
		"plugin": p.Name(),
		"scope":  scope,
	})
	if err != nil {
		return
	}

	loadedFacts := make([]string, 0, len(factRows))
	for _, row := range factRows {
		if fact, ok := row["fact"].(string); ok && strings.TrimSpace(fact) != "" {
			loadedFacts = append(loadedFacts, strings.TrimSpace(fact))
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if loadedSummary != "" {
		p.summary = loadedSummary
	}
	if len(loadedFacts) > 0 {
		p.facts = dedupeFacts(loadedFacts)
		selected := p.selectRelevantFacts(userMessage, 10)
		if len(selected) > 0 {
			p.facts = selected
		}
	}
	if strings.TrimSpace(p.summary) == "" && len(p.facts) > 0 {
		p.rebuildSummaryLocked()
	}
}

func (p *WatcherPlugin) configSnapshotLocked() map[string]interface{} {
	result := make(map[string]interface{}, len(p.config))
	for k, v := range p.config {
		result[k] = v
	}
	return result
}

var readOnlyCypherPattern = regexp.MustCompile(`(?i)\b(CREATE|MERGE|SET|DELETE|REMOVE|FOREACH|DETACH\s+DELETE|LOAD\s+CSV|DROP|ALTER|GRANT|DENY|REVOKE)\b`)

func validateReadOnlyCypher(cypher string) error {
	cleaned := stripCypherComments(cypher)
	if readOnlyCypherPattern.MatchString(cleaned) {
		return fmt.Errorf("query contains write operations; heimdall_watcher_query only allows read-only Cypher")
	}
	return nil
}

func stripCypherComments(query string) string {
	lines := strings.Split(query, "\n")
	for i, line := range lines {
		if idx := strings.Index(line, "//"); idx >= 0 {
			line = line[:idx]
		}
		lines[i] = line
	}
	return strings.Join(lines, "\n")
}

func dedupeFacts(facts []string) []string {
	seen := make(map[string]struct{}, len(facts))
	result := make([]string, 0, len(facts))
	for _, fact := range facts {
		fact = strings.TrimSpace(fact)
		if fact == "" {
			continue
		}
		if _, ok := seen[fact]; ok {
			continue
		}
		seen[fact] = struct{}{}
		result = append(result, fact)
	}
	return result
}

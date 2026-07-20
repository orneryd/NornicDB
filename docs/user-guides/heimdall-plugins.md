# Heimdall Plugin Development Guide

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Quick Start](#quick-start)
4. [Plugin Interface](#plugin-interface)
5. [Action Handlers](#action-handlers)
6. [Building and Loading Plugins](#building-and-loading-plugins)
7. [Testing Plugins](#testing-plugins)
8. [Example: Complete Plugin](#example-complete-plugin)
9. [Ordering and Determinism](#ordering-and-determinism)
10. [Optional Lifecycle Hooks](#optional-lifecycle-hooks)
11. [Best Practices](#best-practices)
12. [Troubleshooting](#troubleshooting)

---

## Overview

Heimdall is the cognitive guardian of NornicDB - a subsystem that enables AI-powered database management through an embedded Small Language Model (SLM). Named after the Norse god who guards Bifröst with his all-seeing eye, Heimdall watches over NornicDB's cognitive capabilities.

**Heimdall Plugins** are a DISTINCT plugin type from regular NornicDB plugins (like APOC). While regular plugins provide Cypher functions, Heimdall plugins provide **subsystem management actions** that the SLM can invoke based on natural language user requests.

### How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│  User: "Check for graph anomalies"                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Bifrost (Chat Interface)                                       │
│  └─ Sends message to Heimdall SLM                               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Heimdall SLM (qwen3-0.6b)                                    │
│  ├─ Receives system prompt with available actions               │
│  ├─ Interprets user intent                                      │
│  └─ Responds: {"action": "heimdall.anomaly.detect", "params": {}}│
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Action Invoker                                                 │
│  ├─ Parses JSON action command                                  │
│  ├─ Looks up handler in SubsystemManager                        │
│  └─ Executes: heimdall.anomaly.detect handler                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Anomaly Plugin (Your Plugin!)                                  │
│  └─ Executes detection logic, returns ActionResult              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  User sees: "Found 3 anomalies: [details...]"                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Architecture

### Plugin Types

| Plugin Type    | Interface                 | Purpose                | Example                   |
| -------------- | ------------------------- | ---------------------- | ------------------------- |
| Regular (APOC) | `nornicdb.Plugin`         | Cypher functions       | `apoc.coll.sum()`         |
| **Heimdall**   | `heimdall.HeimdallPlugin` | SLM-managed subsystems | `heimdall.anomaly.detect` |

### Key Components

1. **SubsystemManager**: Global registry for all Heimdall plugins and actions
2. **HeimdallPlugin Interface**: Contract all plugins must implement
3. **ActionFunc**: Individual action definitions with handlers
4. **ActionContext**: Context passed to handlers (database, metrics, Bifrost)
5. **ActionResult**: Standardized return format for actions
6. **BifrostBridge**: Communication channel to connected UI clients

---

## Quick Start

### 1. Define Your Plugin

```go
package myplugin

import (
    "github.com/orneryd/nornicdb/pkg/heimdall"
)

// MyPlugin implements heimdall.HeimdallPlugin
type MyPlugin struct {
    // Your plugin state
}

// Export as HeimdallPlugin - REQUIRED for .so plugins
var Plugin heimdall.HeimdallPlugin = &MyPlugin{}
```

### 2. Implement Required Methods

```go
// Identity
func (p *MyPlugin) Name() string        { return "myplugin" }
func (p *MyPlugin) Version() string     { return "1.0.0" }
func (p *MyPlugin) Type() string        { return "heimdall" } // MUST be "heimdall"
func (p *MyPlugin) Description() string { return "My custom subsystem" }
```

### 3. Define Actions

```go
func (p *MyPlugin) Actions() map[string]heimdall.ActionFunc {
    return map[string]heimdall.ActionFunc{
        "analyze": {
            Description: "Analyze something in the graph",
            Category:    "analysis",
            Handler:     p.handleAnalyze,
        },
    }
}

func (p *MyPlugin) handleAnalyze(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
    // Your logic here
    return &heimdall.ActionResult{
        Success: true,
        Message: "Analysis complete",
        Data:    map[string]interface{}{"findings": []string{"item1", "item2"}},
    }, nil
}
```

#### MCP tool format

Heimdall actions are aligned with the [Model Context Protocol (MCP)](https://modelcontextprotocol.io) tool format:

- **Name** – Full action name (e.g. `heimdall_watcher_query`)
- **Description** – Plain-text description for the model
- **InputSchema** – Optional JSON Schema for parameters (same as MCP `inputSchema`)

When you define an action, you can set `InputSchema` so that MCP clients and the assistant know the expected parameters:

```go
"query": {
    Description: "Run a Cypher query (prefer read-only for user-triggered actions)",
    Category:    "query",
    InputSchema: json.RawMessage([]byte(`{"type":"object","properties":{"cypher":{"type":"string","description":"Cypher query"}},"required":["cypher"]}`)),
    Handler:     p.handleQuery,
},
```

Use `heimdall.ActionsAsMCPTools()` to export all registered actions in MCP tool list format (e.g. for merging with NornicDB’s MCP server or external clients). Invocation matches MCP: action name plus `params` (MCP `arguments`).

### 4. Build and Deploy

```bash
# Build as shared library
go build -buildmode=plugin -o myplugin.so ./plugins/myplugin

# Place in plugins directory
cp myplugin.so $NORNICDB_HEIMDALL_PLUGINS_DIR/

# Or register as built-in (see below)
```

---

## Plugin Interface

Every Heimdall plugin must implement the `HeimdallPlugin` interface:

```go
type HeimdallPlugin interface {
    // === Identity ===
    Name() string        // Plugin/subsystem identifier (e.g., "anomaly")
    Version() string     // Semver version (e.g., "1.0.0")
    Type() string        // MUST return "heimdall"
    Description() string // Human-readable description

    // === Lifecycle ===
    Initialize(ctx SubsystemContext) error  // Called on load
    Start() error                           // Begin background operations
    Stop() error                            // Pause background operations
    Shutdown() error                        // Final cleanup

    // === State & Health ===
    Status() SubsystemStatus                // Current status
    Health() SubsystemHealth                // Detailed health
    Metrics() map[string]interface{}        // Subsystem metrics

    // === Configuration ===
    Config() map[string]interface{}                    // Current config
    Configure(settings map[string]interface{}) error   // Update config
    ConfigSchema() map[string]interface{}              // JSON schema for validation

    // === Actions ===
    Actions() map[string]ActionFunc         // All available actions

    // === Data Access ===
    Summary() string                        // Text summary for SLM context
    RecentEvents(limit int) []SubsystemEvent // Recent events
}
```

### SubsystemContext

Provided during initialization:

```go
type SubsystemContext struct {
    Config   Config          // Heimdall configuration
    Database DatabaseRouter  // Multi-database graph access ("" = default database)
    Metrics  MetricsReader   // Runtime metrics
    Logger   SubsystemLogger // Logging interface
    Bifrost  BifrostBridge   // Communication to UI clients
    Heimdall HeimdallInvoker // Optional: invoke the SLM from plugins
}
```

### SubsystemStatus Values

```go
const (
    StatusUninitialized SubsystemStatus = "uninitialized"
    StatusInitializing  SubsystemStatus = "initializing"
    StatusReady         SubsystemStatus = "ready"
    StatusRunning       SubsystemStatus = "running"
    StatusStopping      SubsystemStatus = "stopping"
    StatusStopped       SubsystemStatus = "stopped"
    StatusError         SubsystemStatus = "error"
)
```

---

## Action Handlers

Actions are the heart of Heimdall plugins - they define what the SLM can do.

### ActionFunc Structure

```go
type ActionFunc struct {
    Name        string                                         // Auto-set: heimdall.{plugin}.{action}
    Handler     func(ctx ActionContext) (*ActionResult, error) // Your handler
    Description string                                         // Shown to SLM/users
    Category    string                                         // Grouping (monitoring, analysis, etc.)
}
```

### ActionContext

Passed to every handler:

```go
type ActionContext struct {
    context.Context                        // Standard Go context

    UserMessage string                     // Original user request
    Params      map[string]interface{}     // Extracted parameters

    Database    DatabaseRouter             // Query/search by database ("" = default database)
    Metrics     MetricsReader              // Get runtime metrics
    Bifrost     BifrostBridge              // Communicate with UI

    // Per-database RBAC (set when Bifrost is behind auth)
    PrincipalRoles     []string             // Authenticated principal's role names
    DatabaseAccessMode auth.DatabaseAccessMode  // CanAccessDatabase(dbName) before running Cypher
    ResolvedAccess     func(dbName string) auth.ResolvedAccess  // Per-DB read/write for mutations
}
```

When the server mounts Bifrost behind authentication, it attaches **PrincipalRoles**, **DatabaseAccessMode**, and **ResolvedAccess** to the request context; these are then available on `ActionContext` (and `PreExecuteContext`). Plugins that run Cypher or mutations against a specific database should enforce per-database access: call `ctx.DatabaseAccessMode.CanAccessDatabase(dbName)` before querying that database, and for mutations use `ctx.ResolvedAccess(dbName).Write`. See [Per-Database RBAC & Lockout Recovery](../security/per-database-rbac.md).

#### Example: Validating per-database RBAC for a chat completion

The following is a **commented-out example** of a PreExecute hook that validates the caller’s per-database access before an action runs (e.g. before the SLM’s chosen action is executed in a chat completion). If the principal cannot access the target database or cannot write when the action is a mutation, the hook cancels execution and returns a clear message.

```go
// PreExecute validates per-database RBAC before the action runs (chat completion flow).
// func (p *MyPlugin) PreExecute(ctx *heimdall.PreExecuteContext, done func(heimdall.PreExecuteResult)) {
//     // 1. Determine target database from action params (e.g. "database" or "db" key)
//     dbName := ""
//     if d, ok := ctx.Params["database"].(string); ok && d != "" {
//         dbName = d
//     }
//     if dbName == "" {
//         dbName = ctx.Database.DefaultDatabaseName()
//     }
//
//     // 2. Enforce database access: principal must be allowed to access this DB
//     if ctx.DatabaseAccessMode != nil && !ctx.DatabaseAccessMode.CanAccessDatabase(dbName) {
//         ctx.Cancel("Access to database '"+dbName+"' is not allowed for your role.", "plugin:rbac")
//         done(heimdall.PreExecuteResult{Continue: false, AbortMessage: "Permission denied: you cannot access database '" + dbName + "'."})
//         return
//     }
//
//     // 3. If this action performs mutations, require write permission for that DB
//     if isMutationAction(ctx.Action) && ctx.ResolvedAccess != nil {
//         ra := ctx.ResolvedAccess(dbName)
//         if !ra.Write {
//             ctx.Cancel("Write on database '"+dbName+"' is not allowed for your role.", "plugin:rbac")
//             done(heimdall.PreExecuteResult{Continue: false, AbortMessage: "Permission denied: write on database '" + dbName + "' is not allowed."})
//             return
//         }
//     }
//
//     done(heimdall.PreExecuteResult{Continue: true})
// }
//
// func isMutationAction(action string) bool {
//     // Example: list of action names that perform CREATE/DELETE/SET/MERGE, etc.
//     switch action {
//     case "heimdall_myplugin_create", "heimdall_myplugin_delete":
//         return true
//     default:
//         return false
//     }
// }
```

### ActionResult

Standard response format:

```go
type ActionResult struct {
    Success bool                   `json:"success"`
    Message string                 `json:"message"`
    Data    map[string]interface{} `json:"data,omitempty"`
}
```

### Example Handler

```go
func (p *MyPlugin) handleDetect(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
    // 1. Parse parameters
    threshold := 0.8
    if t, ok := ctx.Params["threshold"].(float64); ok {
        threshold = t
    }

    // 2. Query the database
    results, err := ctx.Database.Query(ctx, "", `
        MATCH (n)
        WHERE n.score > $threshold
        RETURN n.id, n.score
    `, map[string]interface{}{"threshold": threshold})
    if err != nil {
        return nil, fmt.Errorf("query failed: %w", err)
    }

    // 3. Send progress via Bifrost (optional)
    if ctx.Bifrost.IsConnected() {
        ctx.Bifrost.SendNotification("info", "Scan Progress", "Found potential anomalies...")
    }

    // 4. Return result
    return &heimdall.ActionResult{
        Success: true,
        Message: fmt.Sprintf("Found %d items above threshold %.2f", len(results), threshold),
        Data: map[string]interface{}{
            "count":     len(results),
            "threshold": threshold,
            "items":     results,
        },
    }, nil
}
```

---

## Building and Loading Plugins

### Method 1: External .so Plugin

```bash
# Build
cd plugins/myplugin
go build -buildmode=plugin -o myplugin.so .

# Remote-provider-only Heimdall build (no local GGUF/localllm dependency)
go build -tags nolocalllm -buildmode=plugin -o myplugin.so .

# Deploy
export NORNICDB_HEIMDALL_PLUGINS_DIR=/path/to/plugins
cp myplugin.so $NORNICDB_HEIMDALL_PLUGINS_DIR/
```

**Requirements for .so plugins:**

- Must export `var Plugin heimdall.HeimdallPlugin = &YourType{}`
- Built with same Go version as NornicDB
- Same CGO settings (important for llama.cpp)
- Same build tags as the NornicDB binary

If your host binary uses Heimdall only through remote providers like Ollama or OpenAI, you can build both the host binary and Heimdall plugins with `-tags nolocalllm` to avoid linking the local GGUF generator.

For the built-in watcher example:

```bash
make plugin-heimdall-watcher-remote
# equivalent to: make plugin-heimdall-watcher NOLOCALLLM=1
```

Homebrew note: Go plugins must match the exact host-binary build context. A Homebrew-installed `nornicdb` binary and a locally built plugin often will not load together unless the host binary was built with the same Go toolchain, CGO setting, architecture, and build tags.

### Method 2: Built-in Plugin (Recommended)

Register in your plugin's init():

```go
package myplugin

import "github.com/orneryd/nornicdb/pkg/heimdall"

func init() {
    manager := heimdall.GetSubsystemManager()
    manager.RegisterPlugin(&MyPlugin{}, "", true) // path="", builtin=true
}
```

Then import in cmd/nornicdb/main.go:

```go
import _ "github.com/orneryd/nornicdb/plugins/myplugin"
```

---

## Testing Plugins

### Unit Testing

```go
func TestMyPlugin_Actions(t *testing.T) {
    plugin := &MyPlugin{}

    // Test initialization
    ctx := heimdall.SubsystemContext{
        Config:  heimdall.DefaultConfig(),
        Bifrost: &heimdall.NoOpBifrost{},
    }
    err := plugin.Initialize(ctx)
    require.NoError(t, err)

    // Test action
    actions := plugin.Actions()
    action, ok := actions["analyze"]
    require.True(t, ok)

    actCtx := heimdall.ActionContext{
        Context:     context.Background(),
        UserMessage: "analyze the graph",
        Params:      map[string]interface{}{"threshold": 0.5},
        Bifrost:     &heimdall.NoOpBifrost{},
    }

    result, err := action.Handler(actCtx)
    require.NoError(t, err)
    assert.True(t, result.Success)
}
```

### Integration Testing via Chat

```bash
# Start NornicDB with Heimdall enabled
NORNICDB_HEIMDALL_ENABLED=true ./nornicdb

# Note: Heimdall plugins are skipped unless Heimdall is enabled and initialized.
# Environment variables override config file values, so double-check your container env.

# Open Bifrost chat UI and type:
# "run my analyze action"
# "analyze the graph with threshold 0.5"
```

---

## Example: Complete Plugin

Here's a complete anomaly detection plugin:

```go
// plugins/anomaly/plugin.go
package anomaly

import (
    "fmt"
    "sync"
    "time"

    "github.com/orneryd/nornicdb/pkg/heimdall"
)

var Plugin heimdall.HeimdallPlugin = &AnomalyPlugin{}

type AnomalyPlugin struct {
    mu       sync.RWMutex
    ctx      heimdall.SubsystemContext
    status   heimdall.SubsystemStatus
    events   []heimdall.SubsystemEvent
    lastScan time.Time
    scanCount int64
}

// === Identity ===

func (p *AnomalyPlugin) Name() string        { return "anomaly" }
func (p *AnomalyPlugin) Version() string     { return "1.0.0" }
func (p *AnomalyPlugin) Type() string        { return "heimdall" }
func (p *AnomalyPlugin) Description() string { return "Graph anomaly detection subsystem" }

// === Lifecycle ===

func (p *AnomalyPlugin) Initialize(ctx heimdall.SubsystemContext) error {
    p.mu.Lock()
    defer p.mu.Unlock()

    p.ctx = ctx
    p.status = heimdall.StatusReady
    p.events = make([]heimdall.SubsystemEvent, 0, 100)
    p.addEvent("info", "Anomaly detector initialized")
    return nil
}

func (p *AnomalyPlugin) Start() error {
    p.mu.Lock()
    defer p.mu.Unlock()
    p.status = heimdall.StatusRunning
    return nil
}

func (p *AnomalyPlugin) Stop() error {
    p.mu.Lock()
    defer p.mu.Unlock()
    p.status = heimdall.StatusStopped
    return nil
}

func (p *AnomalyPlugin) Shutdown() error {
    return p.Stop()
}

// === State & Health ===

func (p *AnomalyPlugin) Status() heimdall.SubsystemStatus {
    p.mu.RLock()
    defer p.mu.RUnlock()
    return p.status
}

func (p *AnomalyPlugin) Health() heimdall.SubsystemHealth {
    p.mu.RLock()
    defer p.mu.RUnlock()
    return heimdall.SubsystemHealth{
        Status:    p.status,
        Healthy:   p.status == heimdall.StatusRunning,
        LastCheck: time.Now(),
    }
}

func (p *AnomalyPlugin) Metrics() map[string]interface{} {
    p.mu.RLock()
    defer p.mu.RUnlock()
    return map[string]interface{}{
        "scan_count": p.scanCount,
        "last_scan":  p.lastScan,
    }
}

// === Configuration ===

func (p *AnomalyPlugin) Config() map[string]interface{} {
    return map[string]interface{}{"threshold": 0.8}
}

func (p *AnomalyPlugin) Configure(settings map[string]interface{}) error {
    return nil // Accept all settings
}

func (p *AnomalyPlugin) ConfigSchema() map[string]interface{} {
    return map[string]interface{}{
        "type": "object",
        "properties": map[string]interface{}{
            "threshold": map[string]interface{}{"type": "number"},
        },
    }
}

// === Actions ===

func (p *AnomalyPlugin) Actions() map[string]heimdall.ActionFunc {
    return map[string]heimdall.ActionFunc{
        "detect": {
            Description: "Detect anomalies in the graph structure",
            Category:    "analysis",
            Handler:     p.actionDetect,
        },
        "scan": {
            Description: "Full graph anomaly scan (params: depth, threshold)",
            Category:    "analysis",
            Handler:     p.actionScan,
        },
    }
}

func (p *AnomalyPlugin) actionDetect(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
    p.mu.Lock()
    p.scanCount++
    p.lastScan = time.Now()
    p.mu.Unlock()

    // Example: Find nodes with unusually high edge counts
    results, err := ctx.Database.Query(ctx, "", `
        MATCH (n)
        WITH n, size((n)--()) as edgeCount
        WHERE edgeCount > 100
        RETURN n.id as id, edgeCount
        ORDER BY edgeCount DESC
        LIMIT 10
    `, nil)
    if err != nil {
        return nil, err
    }

    return &heimdall.ActionResult{
        Success: true,
        Message: fmt.Sprintf("Found %d potential anomalies (nodes with >100 edges)", len(results)),
        Data: map[string]interface{}{
            "anomalies": results,
        },
    }, nil
}

func (p *AnomalyPlugin) actionScan(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
    // Parse params
    depth := 3
    if d, ok := ctx.Params["depth"].(float64); ok {
        depth = int(d)
    }

    threshold := 0.8
    if t, ok := ctx.Params["threshold"].(float64); ok {
        threshold = t
    }

    // Notify user of progress
    if ctx.Bifrost.IsConnected() {
        ctx.Bifrost.SendNotification("info", "Scan Started",
            fmt.Sprintf("Scanning with depth=%d, threshold=%.2f", depth, threshold))
    }

    // Your anomaly detection logic here...

    return &heimdall.ActionResult{
        Success: true,
        Message: "Full scan complete",
        Data: map[string]interface{}{
            "depth":     depth,
            "threshold": threshold,
            "findings":  []string{},
        },
    }, nil
}

// === Data Access ===

func (p *AnomalyPlugin) Summary() string {
    return fmt.Sprintf("Anomaly detector: %d scans performed", p.scanCount)
}

func (p *AnomalyPlugin) RecentEvents(limit int) []heimdall.SubsystemEvent {
    p.mu.RLock()
    defer p.mu.RUnlock()
    if limit > len(p.events) {
        limit = len(p.events)
    }
    return p.events[len(p.events)-limit:]
}

func (p *AnomalyPlugin) addEvent(eventType, message string) {
    p.events = append(p.events, heimdall.SubsystemEvent{
        Time:    time.Now(),
        Type:    eventType,
        Message: message,
    })
}
```

---

## Ordering and Determinism

Heimdall executes hooks in a deterministic order. Plugins can opt into ordering
control by implementing `PluginOrdering`.

```go
type PluginOrdering interface {
    Priority() int      // Higher runs earlier
    Before() []string   // Plugins that must run after this one
    After() []string    // Plugins that must run before this one
}
```

If Heimdall detects a cycle, it falls back to `priority desc, name asc` and logs a warning.

### Execution Model

- PrePrompt and PreExecute: ordered and synchronous.
- PostExecute: asynchronous via a bounded worker pool.
- DatabaseEvent: per-plugin bounded queues with backpressure.

## Optional Lifecycle Hooks

Plugins can implement optional interfaces to hook into the request lifecycle. These are **opt-in** - plugins only implement what they need.

### Available Hooks

| Interface           | When Called             | Purpose                                |
| ------------------- | ----------------------- | -------------------------------------- |
| `PrePromptHook`     | Before SLM request      | Modify prompts, add context, validate  |
| `PreExecuteHook`    | Before action execution | Validate params, fetch data, authorize |
| `PostExecuteHook`   | After action execution  | Logging, metrics, cleanup              |
| `DatabaseEventHook` | On database operations  | Audit, monitoring, triggers            |

### PrePromptHook (Modify Prompts)

```go
// Implement this interface to modify prompts before SLM processing
type PrePromptHook interface {
    PrePrompt(ctx *PromptContext) error
}

// Example implementation
func (p *MyPlugin) PrePrompt(ctx *heimdall.PromptContext) error {
    // Add context to the prompt
    ctx.AdditionalInstructions += "\nUser is querying the analytics database."

    // Send notification (appears before AI response)
    ctx.NotifyInfo("Context", "Added analytics context")

    // Optionally cancel the request
    if !p.isAuthorized(ctx.UserMessage) {
        ctx.Cancel("Unauthorized request", "PrePrompt:myplugin")
    }
    return nil
}
```

### PreExecuteHook (Before Action Execution)

```go
// Implement this interface to validate/modify action parameters
type PreExecuteHook interface {
    PreExecute(ctx *PreExecuteContext, done func(PreExecuteResult))
}

// Example: Validate parameters asynchronously
func (p *MyPlugin) PreExecute(ctx *heimdall.PreExecuteContext, done func(heimdall.PreExecuteResult)) {
    // Async validation - call done() when finished
    go func() {
        // Fetch additional data
        data, err := p.fetchContext(ctx.Action.Params)
        if err != nil {
            done(heimdall.PreExecuteResult{Error: err})
            return
        }

        // Modify parameters
        ctx.Action.Params["extra_data"] = data
        done(heimdall.PreExecuteResult{})
    }()
}
```

### PostExecuteHook (After Action Execution)

```go
// Implement this interface for post-action processing
type PostExecuteHook interface {
    PostExecute(ctx *PostExecuteContext)
}

// Example: Log all action results
func (p *MyPlugin) PostExecute(ctx *heimdall.PostExecuteContext) {
    p.logger.Info("Action completed",
        "action", ctx.Action.Action,
        "success", ctx.Result.Success,
        "duration", ctx.Duration,
    )
}
```

PostExecute hooks run asynchronously via a bounded worker pool. If the queue is full,
the job is dropped and logged.

### DatabaseEventHook (React to DB Operations)

```go
// Implement this interface to react to database events
type DatabaseEventHook interface {
	OnDatabaseEvent(event *DatabaseEvent)
}

// Event types: node.created, node.updated, node.deleted, node.read,
// relationship.created, relationship.updated, relationship.deleted,
// query.executed, query.failed, index.created, index.dropped,
// transaction.commit, transaction.rollback,
// database.started, database.shutdown, backup.started, backup.completed

// Example: Audit node deletions
func (p *MyPlugin) OnDatabaseEvent(event *heimdall.DatabaseEvent) {
    if event.Type == heimdall.EventNodeDeleted {
        p.auditLog.Record(event.NodeID, event.UserID, event.Timestamp)
    }
}
```

Database events are delivered through per-plugin bounded queues. Under sustained
load, events may be dropped to preserve system health.

### Autonomous Action Invocation (HeimdallInvoker)

Plugins can autonomously trigger SLM actions based on accumulated events.
The `SubsystemContext` provides a `Heimdall` invoker for this purpose.

```go
// HeimdallInvoker interface methods:
type HeimdallInvoker interface {
    // Directly invoke a registered action
    InvokeAction(ctx context.Context, action string, params map[string]interface{}) (*ActionResult, error)

    // Send a natural language prompt to the SLM (action-routing context)
    SendPrompt(ctx context.Context, prompt string) (*ActionResult, error)

    // Send a prompt directly to the LLM without action routing
    SendRawPrompt(ctx context.Context, prompt string) (*ActionResult, error)

    // Async versions (fire-and-forget, results via Bifrost)
    InvokeActionAsync(action string, params map[string]interface{})
    SendPromptAsync(prompt string)
}
```

**Example: Autonomous Anomaly Detection Based on Event Accumulation**

```go
type SecurityPlugin struct {
    ctx             heimdall.SubsystemContext
    failedQueries   int64
    lastReset       time.Time
}

func (p *SecurityPlugin) OnDatabaseEvent(event *heimdall.DatabaseEvent) {
    // Track query failures
    if event.Type == heimdall.EventQueryFailed {
        // Reset counter every 5 minutes
        if time.Since(p.lastReset) > 5*time.Minute {
            p.failedQueries = 0
            p.lastReset = time.Now()
        }
        p.failedQueries++

        // AUTONOMOUS ACTION: After 5 failures, trigger analysis
        if p.failedQueries >= 5 && p.ctx.Heimdall != nil {
            // Option 1: Directly invoke an action
            p.ctx.Heimdall.InvokeActionAsync("heimdall.anomaly.detect", map[string]interface{}{
                "trigger": "autonomous",
                "reason":  "query_failures",
            })

            // Option 2: Send natural language prompt to SLM
            p.ctx.Heimdall.SendPromptAsync(
                "Multiple query failures detected. Analyze for potential issues.")

            p.failedQueries = 0
        }
    }
}
```

**Use Cases for Autonomous Actions:**

1. **Security Monitoring**: Track failed auth attempts → trigger security analysis
2. **Performance Alerts**: Monitor slow queries → trigger optimization suggestions
3. **Anomaly Detection**: Detect unusual patterns → trigger investigation
4. **Resource Management**: Monitor memory usage → trigger cleanup recommendations
5. **Compliance Auditing**: Track sensitive operations → trigger audit reports

For a full guide on wiring database event triggers to the model and running remediation (e.g. Cypher) from plugins or actions, see [Database event triggers and automatic remediation](heimdall-event-triggers-remediation.md). For how the agentic loop works and how PrePrompt/PreExecute/PostExecute fit in, see [Heimdall agentic loop](heimdall-agentic-loop.md).

### Notification Methods

Within lifecycle hooks, use `PromptContext` notification methods:

```go
// All notifications are queued and sent inline with the streaming response
ctx.NotifyInfo("Title", "Message")      // ℹ️ Info
ctx.NotifyWarning("Title", "Message")   // ⚠️ Warning
ctx.NotifyError("Title", "Message")     // ❌ Error
ctx.NotifyProgress("Title", "Message")  // 🔄 Progress
ctx.Notify("success", "Title", "Msg")   // ✅ Custom type
```

### Cancellation

Any hook can cancel the request:

```go
func (p *MyPlugin) PrePrompt(ctx *heimdall.PromptContext) error {
    if reason := p.shouldCancel(ctx.UserMessage); reason != "" {
        ctx.Cancel(reason, "PrePrompt:myplugin")
        // Request stops here, user sees cancellation message
    }
    return nil
}
```

### Full Lifecycle Plugin Example

```go
// Implement all hooks (convenience interface)
var _ heimdall.FullLifecycleHook = (*MyPlugin)(nil)

type MyPlugin struct {
    // ... your fields ...
}

func (p *MyPlugin) PrePrompt(ctx *heimdall.PromptContext) error {
    ctx.NotifyInfo("Processing", "Preparing your request...")
    return nil
}

func (p *MyPlugin) PreExecute(ctx *heimdall.PreExecuteContext, done func(heimdall.PreExecuteResult)) {
    // Synchronous validation
    done(heimdall.PreExecuteResult{})
}

func (p *MyPlugin) PostExecute(ctx *heimdall.PostExecuteContext) {
    log.Printf("Action %s completed in %v", ctx.Action.Action, ctx.Duration)
}

func (p *MyPlugin) OnDatabaseEvent(event *heimdall.DatabaseEvent) {
    // React to database operations
}
```

---

## Best Practices

### 1. Action Naming

- Use lowercase, descriptive names: `detect`, `scan`, `configure`
- Full action name format: `heimdall.{plugin}.{action}`
- Example: `heimdall.anomaly.detect`

### 2. Parameter Extraction

```go
// Always provide defaults and type-check
threshold := 0.5 // default
if t, ok := ctx.Params["threshold"].(float64); ok {
    threshold = t
}
```

### 3. Error Handling

```go
// Return errors, don't panic
if err != nil {
    return &heimdall.ActionResult{
        Success: false,
        Message: fmt.Sprintf("Failed: %v", err),
    }, nil // Return nil error if you handled it
}
```

### 4. Ordering and Hooks

- Only implement `PluginOrdering` if you truly need deterministic ordering.
- Avoid hard dependencies unless required; keep hooks independent when possible.
- Assume PostExecute and DatabaseEvent delivery can be dropped under load.

### 5. Progress Updates (Inline Notifications)

```go
// Notifications are sent inline with the streaming response
// They appear in proper order - before/after the content they relate to
// Use PromptContext.Notify() in lifecycle hooks:
func (p *MyPlugin) PrePrompt(ctx *heimdall.PromptContext) error {
    ctx.NotifyInfo("Processing", "Analyzing your request...")
    return nil
}

// In action handlers, use the ActionContext:
if ctx.Bifrost.IsConnected() {
    ctx.Bifrost.SendNotification("info", "Progress", "50% complete...")
}
```

**Note:** Notifications from lifecycle hooks (PrePrompt) are queued and sent at the start of the streaming response, ensuring proper ordering with chat content.

### 5. Thread Safety

```go
// Protect shared state
p.mu.Lock()
p.counter++
p.mu.Unlock()
```

### 6. Action Descriptions

Write clear descriptions - they're shown to both the SLM and users:

```go
"detect": {
    Description: "Detect graph anomalies - finds nodes with unusual connectivity patterns",
    Category:    "analysis",
    Handler:     p.actionDetect,
}
```

---

## Troubleshooting

### Plugin Not Loading

1. Check plugin type returns "heimdall":

   ```go
   func (p *MyPlugin) Type() string { return "heimdall" }
   ```

2. Verify export variable exists:

   ```go
   var Plugin heimdall.HeimdallPlugin = &MyPlugin{}
   ```

3. Check build mode:
   ```bash
   go build -buildmode=plugin -o myplugin.so .
   ```

### Action Not Triggering

1. Verify action is registered:

   ```go
   // Check in Bifrost chat:
   /help
   // Should list: heimdall.myplugin.myaction
   ```

2. Check action description matches user intent - the SLM uses descriptions to map requests

3. Try explicit action name:
   ```
   User: "execute heimdall.myplugin.detect"
   ```

### Database Access Fails

1. Verify the target database exists (or pass `""` to use the default database)
2. If your action is intended to be read-only, validate and reject write clauses (CREATE/MERGE/DELETE/SET, etc.)
3. Check context isn't cancelled
4. Verify database routing in `Health()`

### Bifrost Communication Fails

1. Check `ctx.Bifrost.IsConnected()` before sending in action handlers
2. Use `NoOpBifrost` in tests to avoid panics
3. For lifecycle hooks, notifications are queued - they won't fail but may not display if the request is cancelled

### Notifications Out of Order

Notifications from lifecycle hooks are **queued and sent inline** with the streaming response:

- PrePrompt notifications appear **before** the AI response
- They're sent as special SSE chunks with `role: "heimdall"`
- No separate EventSource subscription is needed in the UI

If you see ordering issues:

1. Ensure you're using `ctx.NotifyInfo()` etc. in PrePrompt hooks
2. Action handler notifications via `ctx.Bifrost.SendNotification()` may arrive later

---

## Reference

### Available Categories

- `monitoring` - Status, health, metrics
- `analysis` - Detection, scanning, diagnostics
- `configuration` - Config get/set
- `optimization` - Query/storage tuning
- `curation` - Memory/data management
- `system` - Core system operations
- `test` - Test/debug actions

### Example Prompts → Actions

| User Says           | Maps To                    |
| ------------------- | -------------------------- |
| "check the status"  | `heimdall_watcher_status`  |
| "detect anomalies"  | `heimdall.anomaly.detect`  |
| "say hello"         | `heimdall_watcher_hello`   |
| "what's the health" | `heimdall_watcher_health`  |
| "show me metrics"   | `heimdall_watcher_metrics` |

---

## See Also

- [Heimdall Architecture](../architecture/cognitive-slm-architecture.md)
- [Bifrost UI Guide](./heimdall-ai-assistant.md)
- [Example Plugin: Watcher](https://github.com/orneryd/nornicdb/blob/main/plugins/heimdall/plugin.go)

---

**Version:** 1.1.0  
**Last Updated:** 2024-12-03  
**Maintainer:** NornicDB Team

### Changelog

- **1.1.0**: Added optional lifecycle hooks (PrePromptHook, PreExecuteHook, PostExecuteHook, DatabaseEventHook), inline notification system for proper ordering

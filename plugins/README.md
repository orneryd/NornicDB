# NornicDB Heimdall Plugins

This directory contains Heimdall plugins that extend NornicDB's cognitive database capabilities.

Heimdall is named after the all-seeing Norse god who guards Bifröst. Like its namesake, Heimdall watches over NornicDB's cognitive subsystems, providing SLM (Small Language Model) management and plugin architecture.

## Plugin Types

NornicDB supports two types of plugins:

| Plugin Type | Interface | Prefix | Purpose |
|-------------|-----------|--------|---------|
| **Regular Plugins** | `PluginFunction` | `apoc.*` | Provide Cypher functions |
| **Heimdall Plugins** | `HeimdallPlugin` | `heimdall.*` | Provide subsystem management for the SLM |

This directory contains **Heimdall Plugins**.

## HeimdallPlugin Interface

Heimdall plugins must implement the `heimdall.HeimdallPlugin` interface:

```go
type HeimdallPlugin interface {
    // Identity
    Name() string        // Plugin identifier (e.g., "watcher", "anomaly")
    Version() string     // Semver version
    Type() string        // Must return "heimdall"
    Description() string // Human-readable description

    // Lifecycle
    Initialize(ctx SubsystemContext) error
    Start() error
    Stop() error
    Shutdown() error

    // State & Health
    Status() SubsystemStatus
    Health() SubsystemHealth
    Metrics() map[string]interface{}

    // Configuration
    Config() map[string]interface{}
    Configure(settings map[string]interface{}) error
    ConfigSchema() map[string]interface{}

    // Actions
    Actions() map[string]ActionFunc

    // Data Access (for SLM reasoning)
    Summary() string
    RecentEvents(limit int) []SubsystemEvent
}
```

## Built-in Plugins

### watcher (formerly slm-server)

**Location:** `plugins/heimdall/`

The Watcher plugin is Heimdall's core guardian - it provides actions to monitor and control the SLM subsystem itself.

**Actions:**
- `heimdall_watcher_status` - Get current status and statistics
- `heimdall_watcher_health` - Check health
- `heimdall_watcher_config` - Get configuration
- `heimdall_watcher_set_config` - Update configuration
- `heimdall_watcher_metrics` - Get detailed metrics
- `heimdall_watcher_events` - Get recent events

## Creating a Custom Heimdall Plugin

### 1. Create Plugin Structure

```
plugins/
└── my-plugin/
    └── plugin.go
```

### 2. Implement HeimdallPlugin Interface

```go
package myplugin

import "github.com/orneryd/nornicdb/pkg/heimdall"

// Export as HeimdallPlugin type
var Plugin heimdall.HeimdallPlugin = &MyPlugin{}

type MyPlugin struct {
    // ... your fields
}

func (p *MyPlugin) Name() string    { return "myplugin" }
func (p *MyPlugin) Version() string { return "1.0.0" }
func (p *MyPlugin) Type() string    { return heimdall.PluginTypeHeimdall }
// ... implement all other methods
```

### 3. Define Actions

```go
func (p *MyPlugin) Actions() map[string]heimdall.ActionFunc {
    return map[string]heimdall.ActionFunc{
        "analyze": {
            Description: "Analyze something",
            Category:    "analysis",
            Handler:     p.handleAnalyze,
        },
    }
}

func (p *MyPlugin) handleAnalyze(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
    // Access user message: ctx.UserMessage
    // Access parameters: ctx.Params
    // Access database: ctx.Database.Query(...)
    // Access metrics: ctx.Metrics.Runtime()
    // Communicate via Bifrost: ctx.Bifrost
    
    return &heimdall.ActionResult{
        Success: true,
        Message: "Analysis complete",
        Data: map[string]interface{}{
            "result": "...",
        },
    }, nil
}
```

### 4. Using the Bifrost Bridge

Plugins have access to **Bifrost** - the rainbow bridge for communicating with connected clients.

```go
// In your action handler:
func (p *MyPlugin) handleAnalyze(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
    // Send progress updates via Bifrost
    ctx.Bifrost.SendMessage("🔍 Starting analysis...")
    
    // Long running operation
    result := doAnalysis()
    
    // Send notification based on result
    if result.HasIssues {
        ctx.Bifrost.SendNotification("warning", "Analysis Complete", 
            fmt.Sprintf("Found %d issues", result.IssueCount))
    }
    
    // Broadcast to all connected clients
    ctx.Bifrost.Broadcast("📢 Analysis complete for all nodes")
    
    // Request user confirmation for destructive actions
    if confirmed, _ := ctx.Bifrost.RequestConfirmation("Delete orphan nodes?"); confirmed {
        deleteOrphanNodes()
    }
    
    return &heimdall.ActionResult{Success: true, Message: "Done"}, nil
}
```

**BifrostBridge Interface:**

```go
type BifrostBridge interface {
    // SendMessage sends a message to connected clients
    SendMessage(msg string) error
    
    // SendNotification sends a typed notification (info, warning, error, success)
    SendNotification(notifType, title, message string) error
    
    // Broadcast sends a message to ALL connected clients
    Broadcast(msg string) error
    
    // RequestConfirmation asks user for confirmation before proceeding
    RequestConfirmation(action string) (bool, error)
    
    // IsConnected returns true if any clients are connected
    IsConnected() bool
    
    // ConnectionCount returns number of active connections
    ConnectionCount() int
}
```

### 5. Build as .so Plugin (Optional)

Build the host and plugins together with the repository-pinned Go 1.26.9
toolchain and matching build tags/race settings. Rebuild both after a toolchain
upgrade. On macOS 27, Go 1.26.9 (or 1.27.2 for the 1.27 series) includes the
[linker fix](https://github.com/golang/go/issues/81889) required to load plugins
with debug information; earlier affected patches can fail with
`chained fixups, seg_count does not match number of segments`.

```bash
go build -buildmode=plugin -o my-plugin.so ./plugins/my-plugin
```

Place the `.so` file in `NORNICDB_HEIMDALL_PLUGINS_DIR` (default: `/data/heimdall-plugins`).

### 6. Or Register as Built-in

In your initialization code:

```go
import "github.com/orneryd/nornicdb/plugins/myplugin"

manager := heimdall.GetSubsystemManager()
manager.RegisterPlugin(myplugin.Plugin, "", true) // built-in = true
```

## How Heimdall Uses Plugins

1. **User sends message via chat**: "Check the system health"

2. **SLM interprets intent** and maps to action: `heimdall_watcher_health`

3. **Action is executed** via the plugin:
   ```
   plugin.Actions()["health"].Handler(ctx)
   ```

4. **Result returned** to user via chat:
   ```json
   {
     "success": true,
     "message": "Heimdall reports: SLM is running",
     "data": { "health": { ... } }
   }
   ```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NORNICDB_HEIMDALL_ENABLED` | `false` | Enable Heimdall subsystem (Bifrost auto-enables with it) |
| `NORNICDB_HEIMDALL_MODEL` | `qwen3-0.6b-instruct` | Model name (without .gguf extension) |
| `NORNICDB_HEIMDALL_GPU_LAYERS` | `-1` | GPU layer offload (-1=auto, 0=CPU only) |
| `NORNICDB_HEIMDALL_MAX_TOKENS` | `512` | Maximum tokens for generation |
| `NORNICDB_HEIMDALL_TEMPERATURE` | `0.1` | Temperature (lower = more deterministic) |
| `NORNICDB_HEIMDALL_ANOMALY_DETECTION` | `true` | Enable graph anomaly detection |
| `NORNICDB_HEIMDALL_RUNTIME_DIAGNOSIS` | `true` | Enable runtime diagnosis |
| `NORNICDB_HEIMDALL_MEMORY_CURATION` | `false` | Enable memory curation (experimental) |
| `NORNICDB_HEIMDALL_PLUGINS_DIR` | `/data/heimdall-plugins` | Directory to load .so plugins from |
| `NORNICDB_MODELS_DIR` | `/data/models` | Shared directory for all GGUF models |

### BYOM (Bring Your Own Model)

You can use any GGUF model by:
1. Placing the `.gguf` file in `NORNICDB_MODELS_DIR`
2. Setting `NORNICDB_HEIMDALL_MODEL` to the filename (without `.gguf`)

```bash
# Example: Use a different model
export NORNICDB_MODELS_DIR=/data/models
export NORNICDB_HEIMDALL_MODEL=phi-3.5-mini-instruct
# File: /data/models/phi-3.5-mini-instruct.gguf
```

## Plugin Categories

Actions are organized by category for the SLM to understand:

- **monitoring** - Status, health, metrics (Heimdall's vigilant watch)
- **configuration** - Get/set configuration
- **optimization** - Query optimization
- **curation** - Memory curation and cleanup
- **analysis** - Data analysis
- **system** - System-level actions

## Security Considerations

1. **Multi-database routing**: Plugins receive `DatabaseRouter` and should route each operation to the correct logical database (`""` = default database)
2. **No arbitrary code execution**: Actions are predefined and registered
3. **Configuration validation**: `ConfigSchema()` defines valid config
4. **Event logging**: All actions logged via `RecentEvents()`

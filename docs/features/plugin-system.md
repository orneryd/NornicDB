# Plugin System

NornicDB features a **unified, auto-detecting plugin system** that supports two plugin types: **Function Plugins** (extend Cypher) and **Heimdall Plugins** (extend the SLM subsystem). Plugins are dynamically loaded at runtime from `.so` files with zero configuration required.

## Quick Start

### Using Plugins

```bash
# Run with both plugin types
NORNICDB_PLUGINS_DIR=apoc/built-plugins \
NORNICDB_HEIMDALL_ENABLED=true \
NORNICDB_HEIMDALL_PLUGINS_DIR=plugins/heimdall/built-plugins \
./bin/nornicdb serve
```

**Output:**
```
╔══════════════════════════════════════════════════════════════╗
║ Loading Plugins                                              ║
╠══════════════════════════════════════════════════════════════╣
║ ✓ [FUNC] apoc            v1.0.0       983 functions          ║
║ ✓ [HEIM] watcher         v1.0.0        11 actions            ║
╠══════════════════════════════════════════════════════════════╣
║ Loaded: 2 plugins (1 function, 1 heimdall)                  ║
║         983 Cypher functions available                        ║
║         11 Heimdall actions available                         ║
╚══════════════════════════════════════════════════════════════╝
```

### Building Plugins

```bash
# Build all plugins
make plugins

# Build individual plugins
make plugin-apoc                    # Function plugin (APOC)
make plugin-heimdall-watcher        # Heimdall plugin
make plugin-heimdall-watcher-remote # Heimdall plugin for remote-provider-only Heimdall

# Clean built plugins
make plugins-clean
```

## Plugin Types

| Type | Purpose | Example | Configuration |
|------|---------|---------|---------------|
| **Function** | Extend Cypher with custom functions | APOC (983 functions) | `NORNICDB_PLUGINS_DIR` |
| **Heimdall** | Extend SLM with subsystem management | Watcher (11 actions) | `NORNICDB_HEIMDALL_PLUGINS_DIR` |

### Function Plugins

Provide custom Cypher functions callable from queries:

```cypher
// Collection functions
RETURN apoc.coll.sum([1, 2, 3, 4, 5]) AS total

// Text processing
RETURN apoc.text.join(['Hello', 'World'], ' ') AS greeting

// Custom plugin functions
RETURN myplugin.analyze(node) AS result
```

### Heimdall Plugins

Provide SLM-invokable actions for coding, repository understanding, Graph-RAG, and context management:

```
User: "map this repository before we make a change"
SLM → Invokes: heimdall_watcher_repo_map
Result: Repository graph structure, labels, and likely integration points
```

## Configuration

### Environment Variables

```bash
# Function plugin directory (APOC-style)
NORNICDB_PLUGINS_DIR=/opt/nornicdb/plugins

# Heimdall plugin directory (coding-agent plugins)
NORNICDB_HEIMDALL_PLUGINS_DIR=/opt/nornicdb/heimdall-plugins

# Enable Heimdall (required for Heimdall plugins; env overrides config file)
NORNICDB_HEIMDALL_ENABLED=true
NORNICDB_MODELS_DIR=/opt/nornicdb/models
```

**Important:** Heimdall plugins require an initialized Heimdall subsystem context (Bifrost, DB reader, invoker). If Heimdall is disabled, Heimdall plugins are **skipped and not started** — this prevents background goroutines from running when `heimdall.enabled: false`.

### Docker Example

```yaml
version: '3.8'
services:
  nornicdb:
    image: nornicdb/nornicdb:latest
    environment:
      - NORNICDB_PLUGINS_DIR=/plugins/functions
      - NORNICDB_HEIMDALL_ENABLED=true
      - NORNICDB_HEIMDALL_PLUGINS_DIR=/plugins/heimdall
      - NORNICDB_MODELS_DIR=/models
    volumes:
      - ./custom-plugins:/plugins/functions
      - ./heimdall-plugins:/plugins/heimdall
      - ./models:/models
      - ./data:/var/lib/nornicdb
```

## How It Works

### Auto-Detection

The plugin loader automatically detects plugin type by calling the `Type()` method:

```go
plugin.Type() → "function"   // Loads as function plugin
plugin.Type() → "heimdall"   // Loads as Heimdall plugin
plugin.Type() → "apoc"       // Alias for function plugin
plugin.Type() → ""           // Defaults to function plugin
```

No manual configuration or type declaration needed - plugins self-identify.

### Loading Flow

```
1. Scan directory for *.so files
   ↓
2. For each plugin:
   - plugin.Open("plugin.so")
   - Lookup "Plugin" symbol
   - Call Plugin.Type()
   ↓
3. Auto-route based on type:
   ┌──────────────────┬─────────────────┐
   │ type="function"  │ type="heimdall" │
   ├──────────────────┼─────────────────┤
   │ Extract functions│ Register with   │
   │ Register with    │ SubsystemMgr    │
   │ Cypher executor  │ Start plugin    │
   └──────────────────┴─────────────────┘
```

## Creating Plugins

### Function Plugin

Provides custom Cypher functions.

**Minimal Example:**

```go
// my-plugin/plugin.go
package main

import "github.com/orneryd/nornicdb/pkg/cypher"

type MyPlugin struct{}

func (p *MyPlugin) Name() string    { return "myplugin" }
func (p *MyPlugin) Version() string { return "1.0.0" }
func (p *MyPlugin) Type() string    { return "function" }

func (p *MyPlugin) Functions() map[string]cypher.PluginFunction {
    return map[string]cypher.PluginFunction{
        "myplugin.greet": {
            Handler: func(args ...interface{}) (interface{}, error) {
                if len(args) == 0 {
                    return "Hello!", nil
                }
                return "Hello, " + args[0].(string) + "!", nil
            },
            Description: "Returns a greeting",
        },
    }
}

// Export as Plugin
var Plugin = &MyPlugin{}
```

**Build:**

```bash
go build -buildmode=plugin -o myplugin.so ./my-plugin
```

**Use:**

```cypher
RETURN myplugin.greet("World") AS greeting
// Returns: "Hello, World!"
```

### Heimdall Plugin

Provides SLM-invokable coding actions.

**Minimal Example:**

```go
// my-subsystem/plugin.go
package main

import "github.com/orneryd/nornicdb/pkg/heimdall"

type MySubsystem struct {
    // plugin state
}

func (p *MySubsystem) Name() string        { return "mysubsystem" }
func (p *MySubsystem) Version() string     { return "1.0.0" }
func (p *MySubsystem) Type() string        { return "heimdall" }
func (p *MySubsystem) Description() string { return "Custom subsystem" }

// Lifecycle
func (p *MySubsystem) Initialize(ctx heimdall.SubsystemContext) error { return nil }
func (p *MySubsystem) Start() error   { return nil }
func (p *MySubsystem) Stop() error    { return nil }
func (p *MySubsystem) Shutdown() error { return nil }

// State
func (p *MySubsystem) Status() heimdall.SubsystemStatus {
    return heimdall.StatusRunning
}
func (p *MySubsystem) Health() heimdall.SubsystemHealth {
    return heimdall.SubsystemHealth{Status: heimdall.StatusRunning, Healthy: true}
}
func (p *MySubsystem) Metrics() map[string]interface{} { return nil }
func (p *MySubsystem) Config() map[string]interface{}  { return nil }
func (p *MySubsystem) Configure(settings map[string]interface{}) error { return nil }
func (p *MySubsystem) ConfigSchema() map[string]interface{} { return nil }
func (p *MySubsystem) Summary() string { return "Running" }
func (p *MySubsystem) RecentEvents(limit int) []heimdall.SubsystemEvent { return nil }

// Actions (invoked by SLM)
func (p *MySubsystem) Actions() map[string]heimdall.ActionFunc {
    return map[string]heimdall.ActionFunc{
        "analyze": {
            Description: "Analyze repository state for a coding task",
            Category:    "coding",
            Handler:     p.analyze,
        },
    }
}

func (p *MySubsystem) analyze(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
    return &heimdall.ActionResult{
        Success: true,
        Message: "Repository context analyzed",
    }, nil
}

// Export as HeimdallPlugin
var Plugin heimdall.HeimdallPlugin = &MySubsystem{}
```

**Build:**

```bash
go build -buildmode=plugin -o mysubsystem.so ./my-subsystem
```

**Use:**

```
User: "analyze this repository area"
SLM → heimdall.mysubsystem.analyze
Result: "Repository context analyzed"
```

## Built-In Plugins

### APOC Plugin

**Type:** Function  
**Functions:** 983  
**Location:** `apoc/built-plugins/apoc.so`

Provides Neo4j-compatible APOC functions:
- Collection operations (`apoc.coll.*`)
- Text processing (`apoc.text.*`)
- Math functions (`apoc.math.*`)
- Date/time (`apoc.date.*`, `apoc.temporal.*`)
- Graph algorithms (`apoc.algo.*`)
- [Complete list →](apoc-functions.md)

### Watcher Plugin

**Type:** Heimdall  
**Actions:** 6  
**Location:** `plugins/heimdall/built-plugins/watcher.so`

Provides coding-agent orchestration:
- Repository mapping (`heimdall_watcher_repo_map`)
- Graph-RAG discovery (`heimdall_watcher_discover`)
- Targeted Cypher inspection (`heimdall_watcher_query`)
- Graph coverage summary (`heimdall_watcher_db_stats`)
- Coding-agent prompt shaping and synthesis
- Long-context summary persistence strategy for re-retrieval

## Platform Support

| Platform | Function Plugins | Heimdall Plugins | Notes |
|----------|-----------------|------------------|-------|
| **Linux (amd64)** | ✅ | ✅ | Full support |
| **Linux (arm64)** | ✅ | ✅ | Full support |
| **macOS (arm64)** | ✅ | ✅ | Apple Silicon |
| **macOS (amd64)** | ✅ | ✅ | Intel Macs |
| **Windows** | ❌ | ❌ | Go plugin limitation - use static linking |

**Windows Users:** Compile plugins directly into the binary instead of using `.so` files.

## Best Practices

### 1. Plugin Interface

Always implement the `Type()` method explicitly:

```go
func (p *MyPlugin) Type() string { return "function" }
// or
func (p *MyPlugin) Type() string { return "heimdall" }
```

### 2. Error Handling

Return descriptive errors:

```go
func myHandler(args ...interface{}) (interface{}, error) {
    if len(args) == 0 {
        return nil, fmt.Errorf("myplugin.func: missing required argument")
    }
    // ...
}
```

### 3. Naming Conventions

**Function plugins:** Use `namespace.function` format:
```
myplugin.calculate
myplugin.transform
```

**Heimdall plugins:** Actions are auto-prefixed:
```
heimdall.mysubsystem.action
```

### 4. Documentation

Document each function/action:

```go
return map[string]cypher.PluginFunction{
    "myplugin.analyze": {
        Handler:     analyzeFunc,
        Description: "Analyzes data and returns insights",
        Category:    "analysis",
    },
}
```

### 5. Testing

Test plugins both standalone and via the loader:

```go
func TestMyPlugin(t *testing.T) {
    // Test plugin directly
    p := &MyPlugin{}
    funcs := p.Functions()
    result, err := funcs["myplugin.greet"].Handler("World")
    assert.NoError(t, err)
    assert.Equal(t, "Hello, World!", result)
}
```

## Troubleshooting

### Plugin Not Loading

**Symptom:** Plugin file exists but doesn't appear in loading output

**Causes:**
1. Missing `Type()` method
2. Incorrect export name (must be `Plugin`)
3. Plugin not compiled with `-buildmode=plugin`
4. Wrong directory (check `NORNICDB_PLUGINS_DIR` or `NORNICDB_HEIMDALL_PLUGINS_DIR`)

**Solution:**

```bash
# Verify plugin exports "Plugin" symbol
nm -gU myplugin.so | grep Plugin

# Rebuild with correct flags
go build -buildmode=plugin -o myplugin.so .
```

### Type Detection Fails

**Symptom:** Plugin loads as wrong type

**Solution:** Check `Type()` method returns correct string:
- `"function"`, `"apoc"`, or `""` → Function plugin
- `"heimdall"` → Heimdall plugin

### Functions Not Callable

**Symptom:** `ERROR: Unknown function: myplugin.func`

**Causes:**
1. Function plugin not loaded
2. Function name mismatch
3. Plugin directory not set

**Solution:**

```bash
# Check plugin loaded
# Look for: ✓ [FUNC] myplugin in startup output

# Verify environment variable
echo $NORNICDB_PLUGINS_DIR

# List available functions
MATCH (n) RETURN keys(n) LIMIT 1
// Check error message for available functions
```

### Heimdall Actions Not Available

**Symptom:** SLM can't invoke action

**Causes:**
1. Heimdall not enabled (`NORNICDB_HEIMDALL_ENABLED=false`)
2. Heimdall plugin not loaded
3. Model not initialized

**Solution:**

```bash
# Enable Heimdall
NORNICDB_HEIMDALL_ENABLED=true \
NORNICDB_HEIMDALL_PLUGINS_DIR=plugins/heimdall/built-plugins \
NORNICDB_MODELS_DIR=models \
./bin/nornicdb serve

# Check Heimdall status
curl http://localhost:7474/api/bifrost/status
```

## Security Considerations

### Plugin Permissions

Plugins run with **full NornicDB permissions**:
- Database read/write access
- Filesystem access
- Network access
- System calls

**Best Practices:**
1. Only load plugins from trusted sources
2. Review plugin code before deployment
3. Use file permissions to restrict plugin directory:

```bash
chmod 755 /opt/nornicdb/plugins
chown root:nornicdb /opt/nornicdb/plugins
```

### Sandboxing (Future)

Plugin sandboxing is on the roadmap:
- Resource limits (CPU, memory)
- Permission restrictions
- Network isolation
- Audit logging

## Performance

### Function Plugin Overhead

Function plugins have **minimal overhead**:
- Direct function pointer calls
- No serialization
- No IPC

**Benchmark:** ~50ns per plugin function call (vs ~30ns for built-in functions)

### Heimdall Plugin Overhead

Heimdall plugins have **low overhead**:
- Direct method calls
- Asynchronous execution
- Event-driven architecture

**Typical latency:** <10ms for most actions

## Examples

### Collection Plugin

```go
type CollectionPlugin struct{}

func (p *CollectionPlugin) Type() string { return "function" }
func (p *CollectionPlugin) Name() string { return "collections" }

func (p *CollectionPlugin) Functions() map[string]cypher.PluginFunction {
    return map[string]cypher.PluginFunction{
        "collections.unique": {
            Handler: func(args ...interface{}) (interface{}, error) {
                list := args[0].([]interface{})
                seen := make(map[interface{}]bool)
                result := []interface{}{}
                for _, item := range list {
                    if !seen[item] {
                        seen[item] = true
                        result = append(result, item)
                    }
                }
                return result, nil
            },
        },
    }
}
```

### Monitoring Plugin

```go
type MonitoringPlugin struct {
    metrics map[string]float64
}

func (p *MonitoringPlugin) Type() string { return "heimdall" }
func (p *MonitoringPlugin) Name() string { return "monitor" }

func (p *MonitoringPlugin) Actions() map[string]heimdall.ActionFunc {
    return map[string]heimdall.ActionFunc{
        "cpu": {
            Handler: p.getCPU,
            Description: "Get current CPU usage",
        },
    }
}

func (p *MonitoringPlugin) getCPU(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
    // Collect CPU metrics
    usage := p.metrics["cpu"]
    return &heimdall.ActionResult{
        Success: true,
        Message: fmt.Sprintf("CPU: %.1f%%", usage),
        Data:    map[string]interface{}{"usage": usage},
    }, nil
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    NornicDB Core                            │
├─────────────────────────────────────────────────────────────┤
│  Unified Plugin Loader (pkg/nornicdb/plugins.go)           │
│  ├─ Auto-detect type via Type() method                     │
│  ├─ Load function plugins → Cypher executor                │
│  └─ Load Heimdall plugins → SubsystemManager               │
├─────────────────────────────────────────────────────────────┤
│  Function Plugins          │  Heimdall Plugins              │
│  ├─ APOC (983 funcs)      │  ├─ Watcher (11 actions)      │
│  └─ Custom plugins         │  └─ Custom subsystems         │
└─────────────────────────────────────────────────────────────┘
```

## Next Steps

- **[APOC Functions](apoc-functions.md)** - Complete function reference
- **[Heimdall](auto-tlp-heimdall.md)** - SLM subsystem management
- **[Development Guide](../development/README.md)** - Plugin development details
- **[API Reference](../api-reference/README.md)** - Function documentation

---

**Create plugins** → **[Development Guide](../development/README.md)**

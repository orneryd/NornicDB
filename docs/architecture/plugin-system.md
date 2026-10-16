# Plugin System Architecture

NornicDB ships a unified plugin loader that auto-detects and routes plugins to the right subsystem at startup. Two plugin types are supported:

| Type | Interface | Subsystem | Example |
|---|---|---|---|
| **Function** | `apoc.Plugin` (in `apoc/`) | Cypher executor — registers functions callable from queries | APOC (`apoc.coll.sum`, `apoc.text.join`, a 983-name library catalog; 865 scalar-exported names after the 118 procedure-only entries are excluded; callability follows the checked typed dispatch and native adapters, not a fixed count — see the [APOC procedure capability audit](../features/apoc-procedure-capabilities.md); many are [NornicDB extensions](../features/apoc-functions.md#nornicdb-extensions)) |
| **Heimdall** | `heimdall.HeimdallPlugin` (in `pkg/heimdall/`) | SLM subsystem — registers actions invokable by the AI assistant | Watcher plugin (`heimdall_watcher_query`, `heimdall_watcher_status`, …) |

Plugins are dynamic Go shared objects (`.so` files on Linux/macOS) loaded at startup from configured directories. Each plugin's `Type()` method tells the loader which subsystem to route it to, so neither type requires manual registration in the host binary.

## Loader

The loader lives in `pkg/nornicdb/plugins.go` for function plugins and `pkg/heimdall/plugin.go` for Heimdall plugins. Both follow the same pattern:

1. Scan the plugin directory for `*.so` files.
2. `plugin.Open()` each file and look up the exported `Plugin` symbol.
3. Call `Type()` on the symbol.
4. Route to the function-plugin path or the Heimdall-plugin path.

Function plugins register their functions with the Cypher executor through the APOC adapter; Heimdall plugins register their actions with the `SubsystemManager` and start their lifecycle hooks.

## Configuration

| Variable | Purpose |
|---|---|
| `NORNICDB_PLUGINS_DIR` | Directory scanned for function plugins. Default: `apoc/built-plugins`. |
| `NORNICDB_PLUGINS_ENABLED` | Master switch for the loader. |
| `NORNICDB_HEIMDALL_PLUGINS_DIR` | Directory scanned for Heimdall plugins. |
| `NORNICDB_HEIMDALL_ENABLED` | Heimdall plugins only start when this is `true` (the loader skips them otherwise to avoid background goroutines). |

There is no separate Cypher procedure for plugin lifecycle (`dbms.plugins.list/load/unload` is **not** implemented). Plugins are loaded once at startup; restart the server to add or remove plugins.

## Where the consumer-facing docs live

This architecture file is intentionally small. For day-to-day usage:

- [Plugin System (User Guide)](../user-guides/plugin-system.md) — APOC plugin usage in Cypher.
- [Plugin System (Feature Reference)](../features/plugin-system.md) — full operator-facing plugin reference, including build/install steps and the auto-detecting loader.
- [APOC Functions](../features/apoc-functions.md) — function catalog by category.
- [Heimdall Plugins](../user-guides/heimdall-plugins.md) — writing Heimdall plugins (action handlers, lifecycle hooks, autonomous invocation).

## Platform support

Go plugins are supported on Linux (amd64, arm64), macOS (arm64, amd64), and **not supported on Windows** (Go's `plugin` package is Linux/macOS only). For Windows deployments, plugins must be compiled into the main binary as built-ins.

## Security posture

- Plugins run in the same process and with the same OS permissions as `nornicdb serve`. They are not sandboxed.
- Only load plugins from trusted sources.
- Plugin code has full system access (storage engine, network, filesystem).
- Use Docker volume mounts and explicit `NORNICDB_PLUGINS_DIR` paths for isolation.
- Heimdall plugins additionally see the active session's `PrincipalRoles`, `DatabaseAccessMode`, and `ResolvedAccess` so they can enforce per-database RBAC inside their own action handlers — see [Heimdall Plugins](../user-guides/heimdall-plugins.md#actioncontext) for the contract.

## Related Documentation

- [Plugin System (Feature Reference)](../features/plugin-system.md)
- [Plugin System (User Guide)](../user-guides/plugin-system.md)
- [APOC Functions](../features/apoc-functions.md)
- [Heimdall Plugins](../user-guides/heimdall-plugins.md)
- [Cognitive SLM Architecture](cognitive-slm-architecture.md) — Heimdall subsystem context

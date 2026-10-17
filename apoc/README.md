# APOC Functions for NornicDB

NornicDB includes **850+ APOC functions** compatible with Neo4j's APOC library.

## Documentation

📚 **[Full Documentation →](../docs/features/apoc-functions.md)**

## Quick Reference

### Configuration

```bash
# Environment variables (Docker/K8s friendly)
NORNICDB_PLUGINS_DIR=/plugins       # Custom plugin directory
NORNICDB_APOC_ALGO_ENABLED=false         # Disable expensive algorithms
NORNICDB_APOC_CREATE_ENABLED=false       # Disable write operations
```

### Available Categories

| Category | Functions | Description |
|----------|-----------|-------------|
| `apoc.coll.*` | 60+ | Collection operations |
| `apoc.text.*` | 50+ | Text processing |
| `apoc.math.*` | 50+ | Math operations |
| `apoc.algo.*` | 15+ | Graph algorithms |
| `apoc.create.*` | 25+ | Dynamic creation |
| `apoc.atomic.*` | 20+ | Atomic operations |
| `apoc.bitwise.*` | 15+ | Bitwise operations |
| `apoc.cypher.*` | 20+ | Dynamic Cypher |
| `apoc.export.*` | 15+ | Export data |
| `apoc.import.*` | 15+ | Import data |
| `apoc.hashing.*` | 20+ | Hashing functions |
| `apoc.load.*` | 30+ | Data loading |
| `apoc.lock.*` | 15+ | Locking mechanisms |
| `apoc.log.*` | 25+ | Logging functions |
| `apoc.merge.*` | 20+ | Merge operations |
| `apoc.meta.*` | 30+ | Metadata functions |
| `apoc.nodes.*` | 30+ | Batch node operations |
| `apoc.paths.*` | 25+ | Advanced paths |
| `apoc.periodic.*` | 10+ | Periodic execution |
| `apoc.refactor.*` | 25+ | Graph refactoring |
| `apoc.schema.*` | 25+ | Schema management |
| `apoc.scoring.*` | 25+ | Scoring/ranking |
| `apoc.search.*` | 30+ | Full-text search |
| `apoc.spatial.*` | 25+ | Geographic functions |
| `apoc.stats.*` | 30+ | Statistics |
| `apoc.temporal.*` | 40+ | Date/time operations |
| `apoc.trigger.*` | 9 | Triggers: native procedures (docs/features/apoc-triggers.md) |
| `apoc.warmup.*` | 15+ | Database warmup |
| `apoc.xml.*` | 25+ | XML processing |
| ...and 15+ more categories | | See full docs |

### Custom Plugins

Drop `.so` files into `NORNICDB_PLUGINS_DIR` - they're auto-loaded on startup.

```go
// Your plugin must export:
var Plugin YourPlugin

type YourPlugin struct{}
func (p YourPlugin) Name() string { return "custom" }
func (p YourPlugin) Version() string { return "1.0.0" }
func (p YourPlugin) Functions() map[string]apoc.PluginFunction { ... }
```

## Package Structure

```
apoc/
├── apoc.go          # Main entry point, function registration
├── config.go        # Configuration (env vars, YAML)
├── plugins.go       # Plugin loading (.so files)
├── storage/         # Storage interface
├── registry/        # Function registry
├── plugin/          # Plugin system
│
├── Core Functions (45+ packages):
├── agg/             # Aggregation functions
├── algo/            # Graph algorithms
├── atomic/          # Atomic operations
├── bitwise/         # Bitwise operations
├── coll/            # Collection functions
├── convert/         # Type conversions
├── create/          # Dynamic creation
├── cypher/          # Dynamic Cypher
├── date/            # Date/time functions
├── diff/            # Diff operations
├── export/          # Export data
├── graph/           # Virtual graphs
├── hashing/         # Hashing functions
├── imports/         # Import data
├── json/            # JSON operations
├── label/           # Label operations
├── load/            # Data loading
├── lock/            # Locking mechanisms
├── log/             # Logging functions
├── map/             # Map operations
├── math/            # Math operations
├── merge/           # Merge operations
├── meta/            # Metadata functions
├── neighbors/       # Neighbor traversal
├── node/            # Node operations
├── nodes/           # Batch node operations
├── number/          # Number formatting
├── path/            # Path finding
├── paths/           # Advanced paths
├── periodic/        # Periodic execution
├── refactor/        # Graph refactoring
├── rel/             # Relationship operations
├── schema/          # Schema management
├── scoring/         # Scoring/ranking
├── search/          # Full-text search
├── spatial/         # Geographic functions
├── stats/           # Statistics
├── temporal/        # Advanced date/time
├── text/            # Text processing
├── trigger/         # Trigger management
├── util/            # Utility functions
├── warmup/          # Database warmup
├── xml/             # XML processing
│
├── plugins/         # Built-in plugins
│   ├── coll_plugin.go
│   └── text_plugin.go
└── examples/        # Plugin examples
```

## See Also

- [Feature Flags](../docs/features/feature-flags.md) - Runtime configuration
- [Cypher Reference](../docs/api-reference/) - Query language
- [Performance Guide](../docs/performance/) - Optimization tips

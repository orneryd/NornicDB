# NornicDB Plugin System

NornicDB features a powerful plugin system that extends Cypher with APOC (Awesome Procedures On Cypher) functions. The APOC plugin provides **964 functions** covering collections, text processing, math, graph algorithms, data import/export, and more.

## Overview

The plugin system provides:

- **Dynamic Plugin Loading**: Plugins are `.so` files loaded automatically at startup
- **APOC Plugin**: Pre-built plugin with 964 functions included in all Docker images
- **Custom Plugins**: Create your own plugins following the same interface
- **Configuration Control**: Enable/disable via environment variables

## Quick Start

### Using APOC Functions

All Docker images include the APOC plugin pre-loaded:

```cypher
// Collection functions
RETURN apoc.coll.sum([1, 2, 3, 4, 5])           // 15
RETURN apoc.coll.avg([1, 2, 3, 4, 5])           // 3.0
RETURN apoc.coll.flatten([[1,2], [3,4]])        // [1,2,3,4]

// Text functions
RETURN apoc.text.capitalize('hello world')      // "Hello world"
RETURN apoc.text.camelCase('hello_world')       // "helloWorld"
RETURN apoc.text.levenshteinDistance('cat', 'bat')  // 1

// Math functions
RETURN apoc.math.sqrt(16)                       // 4.0
RETURN apoc.math.sigmoid(0)                     // 0.5

// Scoring functions
RETURN apoc.scoring.cosine([1,0], [0,1])        // 0.0
RETURN apoc.scoring.jaccard([1,2,3], [2,3,4])   // 0.5

// Spatial functions
RETURN apoc.spatial.haversineDistance(40.7128, -74.0060, 34.0522, -118.2437)

// Create functions
RETURN apoc.create.uuid()                       // "550e8400-e29b-41d4-..."
```

## Configuration

### Environment Variables

```bash
# Directory containing .so plugin files (default: /app/plugins in Docker)
NORNICDB_PLUGINS_DIR=/app/plugins

# Enable/disable plugin system
NORNICDB_PLUGINS_ENABLED=true
```

### Docker Compose

```yaml
services:
  nornicdb:
    image: timothyswt/nornicdb-arm64-metal:latest
    environment:
      - NORNICDB_PLUGINS_DIR=/app/plugins
    volumes:
      - ./custom-plugins:/app/plugins/custom  # Add custom plugins
```

## Available Function Categories

The APOC plugin includes **964 functions** across these categories:

| Category | Count | Description |
|----------|-------|-------------|
| `apoc.coll` | 46 | Collection operations (sum, avg, sort, flatten, union, etc.) |
| `apoc.text` | 41 | Text manipulation (capitalize, replace, distance, regex, etc.) |
| `apoc.math` | 45 | Mathematical operations (sqrt, trig, statistics, etc.) |
| `apoc.convert` | 24 | Type conversion (toFloat, toJson, toList, etc.) |
| `apoc.date` | 15 | Date/time handling (format, parse, add, etc.) |
| `apoc.json` | 18 | JSON operations (parse, stringify, path, merge, etc.) |
| `apoc.util` | 38 | Utilities (md5, sha256, uuid, compress, etc.) |
| `apoc.agg` | 15 | Aggregation (median, percentile, stdev, histogram, etc.) |
| `apoc.algo` | 9 | Graph algorithms (pageRank, dijkstra, etc.) |
| `apoc.map` | 25 | Map operations (merge, flatten, groupBy, etc.) |
| `apoc.create` | 19 | Node/relationship creation (node, relationship, uuid, etc.) |
| `apoc.export` | 14 | Data export (json, csv, cypher, graphML, etc.) |
| `apoc.import` | 19 | Data import (json, csv, xml, etc.) |
| `apoc.load` | 29 | Data loading (json, csv, jdbc, s3, kafka, etc.) |
| `apoc.log` | 25 | Logging (info, debug, warn, metrics, etc.) |
| `apoc.node` | 34 | Node operations (degree, labels, properties, etc.) |
| `apoc.nodes` | 22 | Multi-node operations (group, partition, connected, etc.) |
| `apoc.rel` | 29 | Relationship operations (type, properties, weight, etc.) |
| `apoc.refactor` | 20 | Graph refactoring (mergeNodes, renameLabel, etc.) |
| `apoc.schema` | 30 | Schema operations (indexes, constraints, etc.) |
| `apoc.meta` | 38 | Metadata (schema, stats, types, etc.) |
| `apoc.neighbors` | 6 | Neighbor traversal (atHop, bfs, dfs, etc.) |
| `apoc.path` | 9 | Path operations (expand, slice, etc.) |
| `apoc.paths` | 21 | Path finding (shortest, all, kShortest, etc.) |
| `apoc.periodic` | 10 | Periodic operations (iterate, commit, etc.) |
| `apoc.search` | 29 | Search operations (fulltext, fuzzy, regex, etc.) |
| `apoc.scoring` | 22 | Similarity scoring (cosine, jaccard, bm25, etc.) |
| `apoc.spatial` | 19 | Spatial operations (distance, bearing, geohash, etc.) |
| `apoc.stats` | 23 | Statistics (mean, median, correlation, etc.) |
| `apoc.temporal` | 26 | Temporal operations (format, parse, duration, etc.) |
| `apoc.trigger` | 24 | Trigger management (add, remove, enable, etc.) |
| `apoc.warmup` | 15 | Cache warmup (run, nodes, relationships, etc.) |
| `apoc.xml` | 23 | XML operations (parse, query, transform, etc.) |
| `apoc.label` | 27 | Label operations (add, remove, exists, etc.) |
| `apoc.lock` | 19 | Locking (nodes, relationships, etc.) |
| `apoc.merge` | 17 | Merge operations (node, relationship, etc.) |
| `apoc.hashing` | 18 | Hashing (md5, sha256, murmur, etc.) |
| `apoc.graph` | 15 | Graph operations (fromData, validate, etc.) |
| `apoc.diff` | 9 | Diff operations (nodes, maps, etc.) |
| `apoc.cypher` | 16 | Cypher utilities (run, parallel, etc.) |
| `apoc.bitwise` | 15 | Bitwise operations (and, or, xor, etc.) |
| `apoc.atomic` | 9 | Atomic operations (add, update, etc.) |
| `apoc.number` | 38 | Number formatting (format, parse, toHex, etc.) |

## Creating Custom Plugins

### Plugin Interface

Create a Go file that exports a `Plugin` variable implementing the interface:

```go
// my_plugin.go
package main

import "github.com/orneryd/nornicdb/apoc"

// Plugin is the exported symbol NornicDB loads
var Plugin = MyPlugin{}

type MyPlugin struct{}

func (p MyPlugin) Name() string    { return "myplugin" }
func (p MyPlugin) Version() string { return "1.0.0" }

func (p MyPlugin) Functions() map[string]apoc.FunctionInfo {
    return map[string]apoc.FunctionInfo{
        "apoc.myplugin.hello": {
            Handler:     Hello,
            Category:    "myplugin",
            Description: "Returns a greeting",
            Examples:    []string{"apoc.myplugin.hello('World') => 'Hello, World!'"},
        },
        "apoc.myplugin.double": {
            Handler:     Double,
            Category:    "myplugin",
            Description: "Doubles a number",
            Examples:    []string{"apoc.myplugin.double(21) => 42"},
        },
    }
}

func Hello(name string) string {
    return "Hello, " + name + "!"
}

func Double(n float64) float64 {
    return n * 2
}
```

### Building Your Plugin

```bash
# Build the plugin (must use same Go version as NornicDB)
go build -buildmode=plugin -o my-plugin.so my_plugin.go

# Copy to plugins directory
cp my-plugin.so /path/to/nornicdb/plugins/
```

### Using Your Plugin

After restarting NornicDB, your functions are available:

```cypher
RETURN apoc.myplugin.hello('World')   // "Hello, World!"
RETURN apoc.myplugin.double(21)       // 42
```

## Startup Logs

On startup, NornicDB logs loaded plugins:

```
🔌 Loading Plugins from /app/plugins...
  ✓ apoc.so: 964 functions loaded (v1.0.0)
  ✓ my-plugin.so: 2 functions loaded (v1.0.0)
📦 Total: 966 plugin functions available
```

## Building the APOC Plugin

From the NornicDB source:

```bash
# Build using Makefile
make plugins

# Or manually
cd apoc/plugin-src/apoc
go build -buildmode=plugin -o ../../../apoc/built-plugins/apoc.so apoc_plugin.go
```

## Docker Image Plugin Locations

All official Docker images include the pre-built APOC plugin:

| Image | Plugins Location | Functions |
|-------|------------------|-----------|
| `nornicdb-arm64-metal` | `/app/plugins/apoc.so` | 964 |
| `nornicdb-amd64-cuda` | `/app/plugins/apoc.so` | 964 |
| `nornicdb-amd64-cpu` | `/app/plugins/apoc.so` | 964 |

## Troubleshooting

### Plugin Not Loading

1. Check plugin file exists and is readable:
   ```bash
   ls -la /app/plugins/
   ```

2. Verify Go version compatibility (plugins must be built with same Go version):
   ```bash
   go version
   ```

3. Check logs for errors:
   ```bash
   docker logs nornicdb 2>&1 | grep -i plugin
   ```

### Function Not Found

1. Check if function exists:
   ```cypher
   CALL apoc.help('coll.sum')
   ```

2. Verify plugin loaded at startup (check logs)

### Invalid ELF Header

This error means the plugin was built for a different architecture:
- Rebuild the plugin for the target architecture (arm64 vs amd64)
- Ensure you're using `go build -buildmode=plugin` with matching platform

## Platform Support

| Platform | Plugin Support | Notes |
|----------|---------------|-------|
| Linux (amd64) | ✅ Full | Native Go plugin support |
| Linux (arm64) | ✅ Full | Native Go plugin support |
| macOS (arm64) | ✅ Full | Native Go plugin support |
| macOS (amd64) | ✅ Full | Native Go plugin support |
| Windows | ❌ None | Go plugins not supported |

For Windows, build NornicDB with APOC functions compiled in (not as plugin).

## Performance

- Plugins load once at startup (~100ms for APOC)
- Function calls have minimal overhead (direct function pointer)
- No difference in performance between built-in and plugin functions

## Security Considerations

- Plugins run with the same permissions as NornicDB
- Only load plugins from trusted sources
- Plugin code has full system access
- Use Docker volume mounts for isolation
- Review plugin source code before deployment

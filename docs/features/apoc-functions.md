# APOC Functions

NornicDB includes **850+ APOC functions** fully compatible with Neo4j's APOC library. All core functions are compiled into the binary for maximum performance, with optional plugin support for custom extensions.

## Quick Start

### Using APOC in Cypher

```cypher
// Collection functions
MATCH (n:Person) 
RETURN apoc.coll.sum(n.scores) AS total

// Text processing
RETURN apoc.text.join(['Hello', 'World'], ' ') AS greeting

// Graph algorithms
MATCH (n:Person)
RETURN apoc.algo.pageRank(n) AS rank
```

### Configuration via Environment Variables

```bash
# Docker example - disable expensive algorithms, enable custom plugins
docker run \
  -e NORNICDB_APOC_ALGO_ENABLED=false \
  -e NORNICDB_APOC_PLUGINS_DIR=/plugins \
  -v ./my-plugins:/plugins \
  nornicdb/nornicdb
```

## Available Function Categories

| Category | Functions | Description |
|----------|-----------|-------------|
| `apoc.coll.*` | 60+ | Collection operations (sum, avg, sort, filter, etc.) |
| `apoc.text.*` | 50+ | Text processing (join, split, regex, Levenshtein, etc.) |
| `apoc.math.*` | 50+ | Math operations (trig, stats, number theory) |
| `apoc.convert.*` | 30+ | Type conversions (toInteger, toJson, etc.) |
| `apoc.map.*` | 35+ | Map operations (merge, keys, values, flatten) |
| `apoc.date.*` | 20+ | Date/time functions (parse, format, add) |
| `apoc.temporal.*` | 40+ | Advanced date/time (timezone, duration, age) |
| `apoc.json.*` | 25+ | JSON operations (path, validate, merge) |
| `apoc.util.*` | 50+ | Utilities (MD5, SHA, UUID, compress) |
| `apoc.agg.*` | 20+ | Aggregations (median, percentile, histogram) |
| `apoc.node.*` | 40+ | Node operations (degree, labels, neighbors) |
| `apoc.nodes.*` | 30+ | Batch node operations (link, group, filter) |
| `apoc.rel.*` | 30+ | Relationship operations (properties, clone) |
| `apoc.path.*` | 15+ | Path finding (shortestPath, allPaths) |
| `apoc.paths.*` | 25+ | Advanced paths (k-shortest, disjoint, cycles) |
| `apoc.neighbors.*` | 10+ | Neighbor traversal (BFS, DFS, atHop) |
| `apoc.algo.*` | 15+ | Graph algorithms (PageRank, centrality) |
| `apoc.create.*` | 25+ | Dynamic creation (virtual nodes, clone) |
| `apoc.atomic.*` | 20+ | Atomic operations (add, subtract, locks) |
| `apoc.bitwise.*` | 15+ | Bitwise operations (and, or, xor, shift) |
| `apoc.cypher.*` | 20+ | Dynamic Cypher (run, parallel, parse) |
| `apoc.diff.*` | 10+ | Diff operations (nodes, relationships, maps) |
| `apoc.export.*` | 15+ | Export data (JSON, CSV, Cypher, GraphML) |
| `apoc.import.*` | 15+ | Import data (JSON, CSV, GraphML, batch) |
| `apoc.graph.*` | 15+ | Virtual graphs (from, merge, validate) |
| `apoc.hashing.*` | 20+ | Hashing (MD5, SHA*, MurmurHash, xxHash) |
| `apoc.label.*` | 15+ | Label operations (add, remove, merge) |
| `apoc.load.*` | 30+ | Data loading (JSON, CSV, XML, JDBC, S3) |
| `apoc.lock.*` | 15+ | Locking (nodes, relationships, deadlock) |
| `apoc.log.*` | 25+ | Logging (info, debug, metrics, audit) |
| `apoc.merge.*` | 20+ | Merge operations (nodes, rels, properties) |
| `apoc.meta.*` | 30+ | Metadata (schema, stats, constraints) |
| `apoc.number.*` | 40+ | Number formatting (roman, hex, base conversion) |
| `apoc.periodic.*` | 10+ | Periodic execution (iterate, schedule) |
| `apoc.refactor.*` | 25+ | Graph refactoring (merge, clone, normalize) |
| `apoc.schema.*` | 25+ | Schema management (indexes, constraints) |
| `apoc.scoring.*` | 25+ | Scoring/ranking (cosine, jaccard, TF-IDF) |
| `apoc.search.*` | 30+ | Full-text search (fuzzy, regex, autocomplete) |
| `apoc.spatial.*` | 25+ | Geographic functions (distance, bearing) |
| `apoc.stats.*` | 30+ | Statistics (mean, median, correlation) |
| `apoc.trigger.*` | 20+ | Trigger management (onCreate, onUpdate) |
| `apoc.warmup.*` | 15+ | Database warmup (cache, indexes) |
| `apoc.xml.*` | 25+ | XML processing (parse, query, transform) |

## Configuration

### Environment Variables

All APOC settings can be configured via environment variables (Docker/K8s friendly):

```bash
# Plugin directory for custom .so plugins
NORNICDB_APOC_PLUGINS_DIR=/opt/nornicdb/plugins

# Enable/disable function categories
NORNICDB_APOC_COLL_ENABLED=true      # Collection functions
NORNICDB_APOC_TEXT_ENABLED=true      # Text processing
NORNICDB_APOC_MATH_ENABLED=true      # Math operations
NORNICDB_APOC_ALGO_ENABLED=false     # Graph algorithms (disable if expensive)
NORNICDB_APOC_CREATE_ENABLED=false   # Dynamic creation (disable for read-only)

# Security settings
NORNICDB_APOC_SECURITY_ALLOW_FILE_ACCESS=false
NORNICDB_APOC_SECURITY_MAX_COLLECTION_SIZE=100000
```

### YAML Configuration

Alternatively, use a configuration file:

```yaml
# /etc/nornicdb/apoc.yaml

# Plugin directory for custom .so plugins
plugins_dir: /opt/nornicdb/plugins

# Enable/disable categories
categories:
  coll: true
  text: true
  math: true
  algo: false      # Disable expensive algorithms
  create: false    # Disable write operations

# Fine-grained function control (overrides categories)
functions:
  "apoc.export.*": false    # Disable all export
  "apoc.import.*": false    # Disable all import
  "apoc.algo.pageRank": true # Re-enable specific algorithm

# Security
security:
  allow_dynamic_creation: false
  allow_file_access: false
  max_collection_size: 10000
```

### Docker Compose Example

```yaml
version: '3.8'
services:
  nornicdb:
    image: nornicdb/nornicdb:latest
    environment:
      - NORNICDB_APOC_ALGO_ENABLED=false
      - NORNICDB_APOC_CREATE_ENABLED=false
      - NORNICDB_APOC_PLUGINS_DIR=/plugins
    volumes:
      - ./custom-plugins:/plugins
      - ./data:/var/lib/nornicdb
    ports:
      - "7687:7687"
```

## Custom Plugins

NornicDB supports loading custom functions from Go plugin files (`.so`).

### Plugin Interface

Custom plugins must implement this interface:

```go
type PluginInterface interface {
    Name() string                           // Plugin name (e.g., "ml")
    Version() string                        // Version (e.g., "1.0.0")
    Functions() map[string]PluginFunction   // Function definitions
}

type PluginFunction struct {
    Handler     interface{}   // The function implementation
    Description string        // Documentation
    Examples    []string      // Usage examples
}
```

### Creating a Custom Plugin

**Step 1: Create the plugin**

```go
// plugin_ml.go
package main

import (
    "math"
    "github.com/orneryd/nornicdb/apoc"
)

// Plugin must be exported
var Plugin MLPlugin

type MLPlugin struct{}

func (p MLPlugin) Name() string    { return "ml" }
func (p MLPlugin) Version() string { return "1.0.0" }

func (p MLPlugin) Functions() map[string]apoc.PluginFunction {
    return map[string]apoc.PluginFunction{
        "sigmoid": {
            Handler:     Sigmoid,
            Description: "Sigmoid activation function",
            Examples:    []string{"apoc.ml.sigmoid(0) => 0.5"},
        },
        "relu": {
            Handler:     ReLU,
            Description: "ReLU activation function",
            Examples:    []string{"apoc.ml.relu(-5) => 0"},
        },
    }
}

func Sigmoid(x float64) float64 {
    return 1.0 / (1.0 + math.Exp(-x))
}

func ReLU(x float64) float64 {
    if x < 0 {
        return 0
    }
    return x
}
```

**Step 2: Build as plugin**

```bash
go build -buildmode=plugin -o apoc-ml.so plugin_ml.go
```

**Step 3: Deploy**

```bash
# Copy to plugins directory
cp apoc-ml.so /opt/nornicdb/plugins/

# Or mount in Docker
docker run -v ./apoc-ml.so:/plugins/apoc-ml.so \
           -e NORNICDB_APOC_PLUGINS_DIR=/plugins \
           nornicdb/nornicdb
```

**Step 4: Use in Cypher**

```cypher
RETURN apoc.ml.sigmoid(0.5) AS activation
// Returns: 0.6224593312018546
```

### Auto-Detection

When NornicDB starts with `NORNICDB_APOC_PLUGINS_DIR` set, it:

1. Scans the directory for `*.so` files
2. Validates each file has a `Plugin` export
3. Checks the export implements `PluginInterface`
4. Registers valid plugin functions as `apoc.<plugin-name>.<function>`
5. Logs warnings for invalid plugins (doesn't prevent startup)

```
plugins/
├── apoc-ml.so       ✅ Loaded → apoc.ml.sigmoid, apoc.ml.relu
├── apoc-kafka.so    ✅ Loaded → apoc.kafka.produce, apoc.kafka.consume
├── random-lib.so    ⚠️ Skipped (no Plugin export)
└── broken.so        ⚠️ Skipped (invalid interface)
```

## Function Reference

### Collection Functions (`apoc.coll.*`)

```cypher
// Aggregation
apoc.coll.sum([1,2,3])         // → 6
apoc.coll.avg([1,2,3])         // → 2.0
apoc.coll.min([3,1,2])         // → 1
apoc.coll.max([3,1,2])         // → 3

// Transformation
apoc.coll.sort([3,1,2])        // → [1,2,3]
apoc.coll.reverse([1,2,3])     // → [3,2,1]
apoc.coll.flatten([[1,2],[3]]) // → [1,2,3]

// Set operations
apoc.coll.union([1,2], [2,3])       // → [1,2,3]
apoc.coll.intersection([1,2], [2,3]) // → [2]
apoc.coll.subtract([1,2,3], [2])    // → [1,3]

// Filtering
apoc.coll.contains([1,2,3], 2)      // → true
apoc.coll.duplicates([1,2,2,3])     // → [2]
apoc.coll.frequencies([1,2,2,3])    // → {1:1, 2:2, 3:1}
```

### Text Functions (`apoc.text.*`)

```cypher
// Basic
apoc.text.join(['a','b'], '-')     // → "a-b"
apoc.text.split('a-b', '-')        // → ["a", "b"]
apoc.text.replace('hello', 'l', 'L') // → "heLLo"

// Case conversion
apoc.text.capitalize('hello')      // → "Hello"
apoc.text.camelCase('hello_world') // → "helloWorld"
apoc.text.snakeCase('helloWorld')  // → "hello_world"

// String similarity (full algorithms, not placeholders)
apoc.text.distance('hello', 'helo')          // → 1 (Levenshtein)
apoc.text.jaroWinklerDistance('hello', 'helo') // → 0.96
apoc.text.hammingDistance('hello', 'hallo')  // → 1

// Phonetic
apoc.text.phonetic('Robert')       // → "R163" (Soundex)
```

### Math Functions (`apoc.math.*`)

```cypher
// Basic
apoc.math.round(3.7)    // → 4
apoc.math.ceil(3.2)     // → 4
apoc.math.floor(3.8)    // → 3
apoc.math.abs(-5)       // → 5

// Statistics
apoc.math.mean([1,2,3,4,5])       // → 3.0
apoc.math.median([1,2,3,4,5])     // → 3.0
apoc.math.stdDev([1,2,3,4,5])     // → 1.414...
apoc.math.percentile([1,2,3,4,5], 0.5) // → 3.0

// Number theory
apoc.math.gcd(12, 8)    // → 4
apoc.math.lcm(12, 8)    // → 24
apoc.math.factorial(5)  // → 120
apoc.math.isPrime(17)   // → true
```

### Graph Algorithm Functions (`apoc.algo.*`)

```cypher
// Centrality
MATCH (n:Person)
RETURN n.name, apoc.algo.pageRank(n) AS rank
ORDER BY rank DESC

MATCH (n:Person)
RETURN n.name, apoc.algo.betweennessCentrality(n) AS centrality

// Pathfinding
MATCH (start:Person {name:'Alice'}), (end:Person {name:'Bob'})
RETURN apoc.algo.dijkstra(start, end, 'KNOWS', 'weight')

// Community detection
MATCH (n:Person)
RETURN apoc.algo.community(n) AS communityId
```

### Atomic Operations (`apoc.atomic.*`)

```cypher
// Atomic updates
MATCH (n:Counter {id: 'visits'})
CALL apoc.atomic.add(n, 'count', 1)
RETURN n.count

// Atomic list operations
MATCH (n:User {id: 123})
CALL apoc.atomic.insert(n, 'tags', 0, 'featured')

// Compare and swap
MATCH (n:Lock {resource: 'db'})
CALL apoc.atomic.compareAndSwap(n, 'owner', null, 'process-123')
```

### Bitwise Operations (`apoc.bitwise.*`)

```cypher
// Basic operations
RETURN apoc.bitwise.op(12, '&', 10) AS result  // → 8
RETURN apoc.bitwise.and(12, 10, 8) AS result   // → 8
RETURN apoc.bitwise.or(4, 2, 1) AS result      // → 7

// Bit manipulation
RETURN apoc.bitwise.setBit(0, 3) AS result     // → 8
RETURN apoc.bitwise.testBit(8, 3) AS result    // → true
RETURN apoc.bitwise.countBits(15) AS result    // → 4
```

### Dynamic Cypher (`apoc.cypher.*`)

```cypher
// Run dynamic queries
CALL apoc.cypher.run('MATCH (n:Person) WHERE n.age > $age RETURN n', {age: 30})
YIELD value
RETURN value.n

// Run multiple queries
CALL apoc.cypher.runMany('
  CREATE (n:Person {name: $name});
  MATCH (n:Person) RETURN count(n);
', {name: 'Alice'})

// Parallel execution
CALL apoc.cypher.parallel(['query1', 'query2'], {})
```

### Diff Operations (`apoc.diff.*`)

```cypher
// Compare nodes
MATCH (n1:Person {id: 1}), (n2:Person {id: 2})
RETURN apoc.diff.nodes(n1, n2) AS differences

// Compare maps
RETURN apoc.diff.maps(
  {name: 'Alice', age: 30},
  {name: 'Alice', age: 31}
) AS changes
// → {changed: {age: {old: 30, new: 31}}}
```

### Export Functions (`apoc.export.*`)

```cypher
// Export to JSON
MATCH (n:Person)
CALL apoc.export.json.query('MATCH (n:Person) RETURN n', '/tmp/people.json', {})

// Export to CSV
MATCH (n:Person)
CALL apoc.export.csv.all('/tmp/graph.csv', {})

// Export to Cypher
CALL apoc.export.cypher.all('/tmp/backup.cypher', {format: 'cypher-shell'})
```

### Import Functions (`apoc.import.*`)

```cypher
// Import JSON
CALL apoc.import.json('/data/people.json')
YIELD node
RETURN node

// Import CSV
CALL apoc.import.csv('/data/data.csv', {delimiter: ',', header: true})

// Batch import
CALL apoc.import.batch([
  {labels: ['Person'], props: {name: 'Alice'}},
  {labels: ['Person'], props: {name: 'Bob'}}
], 1000)
```

### Hashing Functions (`apoc.hashing.*`)

```cypher
// Cryptographic hashes
RETURN apoc.hashing.md5('hello') AS hash
RETURN apoc.hashing.sha256('password') AS hash
RETURN apoc.hashing.sha512('data') AS hash

// Fast hashes
RETURN apoc.hashing.murmur3('key') AS hash
RETURN apoc.hashing.xxhash('data') AS hash

// Consistent hashing
RETURN apoc.hashing.consistentHash('user-123', 10) AS bucket
```

### Label Operations (`apoc.label.*`)

```cypher
// Add labels
MATCH (n:Person {id: 123})
CALL apoc.label.add(n, ['Employee', 'Manager'])

// Remove labels
MATCH (n:Person)
CALL apoc.label.remove(n, ['Temporary'])

// Check labels
MATCH (n)
WHERE apoc.label.has(n, 'Person')
RETURN n
```

### Load Functions (`apoc.load.*`)

```cypher
// Load JSON from URL
CALL apoc.load.json('https://api.example.com/data')
YIELD value
RETURN value

// Load CSV
CALL apoc.load.csv('/data/file.csv', {header: true})
YIELD map
CREATE (n:Person) SET n = map

// Load from S3
CALL apoc.load.s3('s3://bucket/data.json', {region: 'us-east-1'})

// Load from database
CALL apoc.load.jdbc('jdbc:postgresql://localhost/db', 'SELECT * FROM users')
```

### Lock Functions (`apoc.lock.*`)

```cypher
// Lock nodes
MATCH (n:Resource {id: 'db'})
CALL apoc.lock.nodes([n])
// ... perform operations ...
CALL apoc.lock.unlock([n])

// Read/write locks
CALL apoc.lock.read([node1, node2])
CALL apoc.lock.write([node3])

// Detect deadlocks
CALL apoc.lock.detectDeadlock()
YIELD deadlock
RETURN deadlock
```

### Logging Functions (`apoc.log.*`)

```cypher
// Basic logging
CALL apoc.log.info('Processing started', {count: 100})
CALL apoc.log.debug('Variable value', {var: value})
CALL apoc.log.warn('Deprecated function', {function: 'old'})
CALL apoc.log.error('Operation failed', {error: message})

// Performance logging
WITH apoc.log.timer('query') AS stopTimer
MATCH (n:Person) WHERE n.age > 30
WITH collect(n) AS results, stopTimer
CALL stopTimer()
RETURN results

// Metrics
CALL apoc.log.metrics('query_time', 150, 'ms')
```

### Merge Operations (`apoc.merge.*`)

```cypher
// Merge nodes
CALL apoc.merge.node(['Person'], {email: 'alice@example.com'}, 
  {created: timestamp()}, {updated: timestamp()})
YIELD node
RETURN node

// Merge relationships
MATCH (a:Person {id: 1}), (b:Person {id: 2})
CALL apoc.merge.relationship(a, 'KNOWS', {}, {since: 2020}, {}, b)
YIELD rel
RETURN rel

// Deep merge properties
MATCH (n:Person {id: 123})
CALL apoc.merge.deepMerge(n, {address: {city: 'NYC', zip: '10001'}})
```

### Metadata Functions (`apoc.meta.*`)

```cypher
// Get schema
CALL apoc.meta.schema()
YIELD value
RETURN value

// Get statistics
CALL apoc.meta.stats()
YIELD labelCount, relTypeCount
RETURN labelCount, relTypeCount

// Get node type properties
CALL apoc.meta.nodeTypeProperties('Person')
YIELD propertyName, propertyType
RETURN propertyName, propertyType
```

### Neighbor Traversal (`apoc.neighbors.*`)

```cypher
// Get neighbors at specific distance
MATCH (n:Person {name: 'Alice'})
RETURN apoc.neighbors.atHop(n, 'KNOWS', 2) AS secondDegree

// Get all neighbors up to distance
MATCH (n:Person {name: 'Alice'})
RETURN apoc.neighbors.toHop(n, 'KNOWS', 3) AS network

// BFS traversal
MATCH (n:Person {name: 'Alice'})
CALL apoc.neighbors.bfs(n, 'KNOWS', 5)
YIELD node
RETURN node
```

### Batch Node Operations (`apoc.nodes.*`)

```cypher
// Link nodes in sequence
MATCH (n:Step)
WITH collect(n) AS steps
CALL apoc.nodes.link(steps, 'NEXT')
YIELD rel
RETURN rel

// Group nodes by property
MATCH (n:Person)
WITH collect(n) AS people
RETURN apoc.nodes.group(people, 'department') AS grouped

// Filter nodes
MATCH (n:Person)
WITH collect(n) AS people
RETURN apoc.nodes.filter(people, function(n) { 
  RETURN n.age > 18 
}) AS adults
```

### Number Functions (`apoc.number.*`)

```cypher
// Format numbers
RETURN apoc.number.format(1234.5678, '#,##0.00') AS formatted
// → "1,234.57"

// Roman numerals
RETURN apoc.number.romanize(14) AS roman        // → "XIV"
RETURN apoc.number.arabize('XIV') AS number     // → 14

// Base conversion
RETURN apoc.number.toHex(255) AS hex            // → "FF"
RETURN apoc.number.fromHex('FF') AS decimal     // → 255
RETURN apoc.number.toBinary(10) AS binary       // → "1010"
```

### Advanced Path Operations (`apoc.paths.*`)

```cypher
// Find all paths
MATCH (start:Person {name: 'Alice'}), (end:Person {name: 'Bob'})
CALL apoc.paths.all(start, end, 'KNOWS', 5)
YIELD path
RETURN path

// K-shortest paths
MATCH (start:Person), (end:Person)
CALL apoc.paths.kShortest(start, end, 'KNOWS', 10, 3)
YIELD path
RETURN path

// Node-disjoint paths
CALL apoc.paths.disjoint(start, end, 'KNOWS', 10, 2)
YIELD path
RETURN path
```

### Periodic Execution (`apoc.periodic.*`)

```cypher
// Batch processing
CALL apoc.periodic.iterate(
  'MATCH (n:Person) RETURN n',
  'SET n.processed = true',
  {batchSize: 1000, parallel: true}
)

// Scheduled execution
CALL apoc.periodic.schedule('cleanup', 
  'MATCH (n:Temp) DELETE n', 
  60)  // Every 60 seconds

// Commit in batches
CALL apoc.periodic.commit(
  'MATCH (n:Person) WHERE n.migrated IS NULL 
   WITH n LIMIT $limit 
   SET n.migrated = true 
   RETURN count(*)',
  {limit: 1000}
)
```

### Graph Refactoring (`apoc.refactor.*`)

```cypher
// Merge nodes
MATCH (n1:Person {id: 1}), (n2:Person {id: 2})
CALL apoc.refactor.mergeNodes([n1, n2], {properties: 'combine'})
YIELD node
RETURN node

// Clone subgraph
MATCH path = (n:Person)-[r:KNOWS]->(m:Person)
WITH collect(n) + collect(m) AS nodes, collect(r) AS rels
CALL apoc.refactor.cloneSubgraph(nodes, rels)
YIELD nodes AS newNodes, relationships AS newRels
RETURN newNodes, newRels

// Normalize data
MATCH (n:Person)
CALL apoc.refactor.normalize(n, 'city', 'City', 'LIVES_IN')
```

### Relationship Operations (`apoc.rel.*`)

```cypher
// Get relationship properties
MATCH ()-[r:KNOWS]->()
RETURN apoc.rel.type(r), apoc.rel.properties(r)

// Clone relationship
MATCH ()-[r:KNOWS]->()
WITH r LIMIT 1
CALL apoc.rel.clone(r)
YIELD rel
RETURN rel

// Get relationship weight
MATCH ()-[r:KNOWS]->()
RETURN apoc.rel.weight(r, 'strength', 1.0) AS weight
```

### Schema Management (`apoc.schema.*`)

```cypher
// Create index
CALL apoc.schema.index.create('Person', ['name'])

// Create constraint
CALL apoc.schema.constraint.create('Person', ['email'], 'UNIQUE')

// List all indexes
CALL apoc.schema.node.indexes()
YIELD name, label, properties
RETURN name, label, properties

// Validate schema
CALL apoc.schema.validate()
YIELD valid, errors
RETURN valid, errors
```

### Scoring Functions (`apoc.scoring.*`)

```cypher
// Cosine similarity
RETURN apoc.scoring.cosine([1,2,3], [4,5,6]) AS similarity

// Jaccard similarity
RETURN apoc.scoring.jaccard([1,2,3], [2,3,4]) AS similarity

// TF-IDF
RETURN apoc.scoring.tfidf('hello', 'hello world hello', 100, 30) AS score

// Pearson correlation
RETURN apoc.scoring.pearson([1,2,3,4], [2,4,6,8]) AS correlation
```

### Search Functions (`apoc.search.*`)

```cypher
// Full-text search
CALL apoc.search.fullText('Person', 'name', 'Alice Bob')
YIELD node
RETURN node

// Fuzzy search
CALL apoc.search.fuzzy('Person', 'name', 'Alise', 2)
YIELD node
RETURN node

// Regex search
CALL apoc.search.regex('Person', 'email', '.*@example\\.com')
YIELD node
RETURN node

// Autocomplete
CALL apoc.search.autocomplete('Person', 'name', 'Al')
YIELD suggestion
RETURN suggestion
```

### Spatial Functions (`apoc.spatial.*`)

```cypher
// Calculate distance
WITH {latitude: 40.7128, longitude: -74.0060} AS nyc,
     {latitude: 51.5074, longitude: -0.1278} AS london
RETURN apoc.spatial.distance(nyc, london) AS distanceKm

// Find nearest points
MATCH (p:Place)
WITH {latitude: 40.7128, longitude: -74.0060} AS target,
     collect(p) AS places
RETURN apoc.spatial.kNearest(target, places, 5) AS nearest

// Check if within bounding box
RETURN apoc.spatial.within(
  {latitude: 40.7128, longitude: -74.0060},
  {minLat: 40, maxLat: 41, minLon: -75, maxLon: -73}
) AS isWithin
```

### Statistics Functions (`apoc.stats.*`)

```cypher
// Basic statistics
WITH [1,2,3,4,5,6,7,8,9,10] AS values
RETURN apoc.stats.mean(values) AS mean,
       apoc.stats.median(values) AS median,
       apoc.stats.stdDev(values) AS stdDev

// Correlation
WITH [1,2,3,4,5] AS x, [2,4,6,8,10] AS y
RETURN apoc.stats.correlation(x, y) AS correlation

// Percentiles
WITH [1,2,3,4,5,6,7,8,9,10] AS values
RETURN apoc.stats.percentile(values, 0.95) AS p95
```

### Temporal Functions (`apoc.temporal.*`)

```cypher
// Format dates
RETURN apoc.temporal.format(datetime(), 'yyyy-MM-dd HH:mm:ss') AS formatted

// Parse dates
RETURN apoc.temporal.parse('2024-01-15', 'yyyy-MM-dd') AS date

// Date arithmetic
RETURN apoc.temporal.add(datetime(), 7, 'days') AS nextWeek
RETURN apoc.temporal.subtract(datetime(), 1, 'month') AS lastMonth

// Calculate age
WITH date('1990-01-15') AS birthdate
RETURN apoc.temporal.age(birthdate) AS age

// Timezone conversion
RETURN apoc.temporal.timezone(datetime(), 'America/New_York') AS nyTime
```

### Trigger Management (`apoc.trigger.*`)

```cypher
// Add trigger
CALL apoc.trigger.add('onPersonCreate',
  'MATCH (n:Person) SET n.created = timestamp()',
  {phase: 'after'}
)

// List triggers
CALL apoc.trigger.list()
YIELD name, statement, enabled
RETURN name, statement, enabled

// Pause/resume triggers
CALL apoc.trigger.pause('onPersonCreate')
CALL apoc.trigger.resume('onPersonCreate')

// Remove trigger
CALL apoc.trigger.remove('onPersonCreate')
```

### Warmup Functions (`apoc.warmup.*`)

```cypher
// Warm up entire database
CALL apoc.warmup.run()
YIELD nodesLoaded, relationshipsLoaded, timeTaken
RETURN nodesLoaded, relationshipsLoaded, timeTaken

// Warm up specific labels
CALL apoc.warmup.nodes(['Person', 'Company'])

// Warm up indexes
CALL apoc.warmup.indexes()

// Warm up subgraph
MATCH (n:Person {id: 123})
CALL apoc.warmup.subgraph(n, 3)
```

### XML Functions (`apoc.xml.*`)

```cypher
// Parse XML
WITH '<root><item>value</item></root>' AS xml
RETURN apoc.xml.parse(xml) AS parsed

// Query XML
WITH apoc.xml.parse('<root><item id="1">A</item></root>') AS doc
RETURN apoc.xml.query(doc, '//item[@id="1"]') AS items

// Convert to JSON
WITH '<root><item>value</item></root>' AS xml
RETURN apoc.xml.toJson(xml) AS json
```

## Security Considerations

### Production Recommendations

```bash
# Disable write operations
NORNICDB_APOC_CREATE_ENABLED=false

# Disable file access
NORNICDB_APOC_SECURITY_ALLOW_FILE_ACCESS=false

# Limit collection sizes to prevent OOM
NORNICDB_APOC_SECURITY_MAX_COLLECTION_SIZE=10000

# Disable expensive algorithms if not needed
NORNICDB_APOC_ALGO_ENABLED=false
```

### Plugin Security

- Only load plugins from trusted sources
- Plugins run with full NornicDB permissions
- Use file permissions to restrict plugin directory:

```bash
chmod 755 /opt/nornicdb/plugins
chown root:root /opt/nornicdb/plugins/*.so
```

## Troubleshooting

### Plugin Not Loading

```
Warning: failed to load plugins from /plugins: plugin does not export 'Plugin'
```

**Cause**: The `.so` file doesn't have a `var Plugin` export.

**Fix**: Ensure your plugin exports `var Plugin YourPluginType`.

### Function Not Found

```cypher
RETURN apoc.custom.myFunc('test')
// Error: function apoc.custom.myFunc not found
```

**Possible causes**:
1. Plugin not loaded (check `NORNICDB_APOC_PLUGINS_DIR`)
2. Function category disabled (check `NORNICDB_APOC_<CATEGORY>_ENABLED`)
3. Function specifically disabled in config

### Platform Compatibility

Go plugins are platform-specific. A `.so` built on Linux x86_64 won't work on:
- macOS (use `.dylib`)
- Windows (use `.dll`)
- Linux ARM

Build plugins on the same platform/architecture as your NornicDB deployment.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    NornicDB Binary                       │
├─────────────────────────────────────────────────────────┤
│  Core APOC Functions (450+ compiled in)                 │
│  ├── apoc.coll.*    ├── apoc.text.*   ├── apoc.math.*  │
│  ├── apoc.convert.* ├── apoc.map.*    ├── apoc.date.*  │
│  ├── apoc.json.*    ├── apoc.util.*   ├── apoc.agg.*   │
│  ├── apoc.node.*    ├── apoc.path.*   └── apoc.algo.*  │
├─────────────────────────────────────────────────────────┤
│  Plugin Loader (auto-loads from NORNICDB_APOC_PLUGINS_DIR)│
│  └── Scans *.so → Validates interface → Registers funcs │
├─────────────────────────────────────────────────────────┤
│  Configuration (env vars or YAML)                        │
│  └── Controls which functions are enabled                │
└─────────────────────────────────────────────────────────┘
           ↓
┌─────────────────────────────────────────────────────────┐
│  Optional Plugins Directory (/opt/nornicdb/plugins)      │
│  ├── apoc-ml.so      → apoc.ml.*                        │
│  ├── apoc-kafka.so   → apoc.kafka.*                     │
│  └── custom-*.so     → apoc.custom.*                    │
└─────────────────────────────────────────────────────────┘
```

## Migration from Neo4j APOC

NornicDB's APOC implementation is designed for compatibility:

| Neo4j APOC | NornicDB | Notes |
|------------|----------|-------|
| `apoc.coll.*` | ✅ Same | Full compatibility - all collection functions |
| `apoc.text.*` | ✅ Same | Full compatibility - text processing |
| `apoc.math.*` | ✅ Same | Full compatibility - math operations |
| `apoc.algo.*` | ✅ Same | Real algorithms (PageRank, centrality, etc.) |
| `apoc.atomic.*` | ✅ Same | Atomic operations with locking |
| `apoc.bitwise.*` | ✅ Same | Bitwise operations |
| `apoc.cypher.*` | ✅ Same | Dynamic Cypher execution |
| `apoc.diff.*` | ✅ Same | Diff operations |
| `apoc.export.*` | ✅ Same | Export to JSON, CSV, Cypher, GraphML |
| `apoc.import.*` | ✅ Same | Import from multiple formats |
| `apoc.graph.*` | ✅ Same | Virtual graph operations |
| `apoc.hashing.*` | ✅ Same | Multiple hash algorithms |
| `apoc.label.*` | ✅ Same | Label operations |
| `apoc.load.*` | ✅ Same | Load from JSON, CSV, XML, JDBC, S3, etc. |
| `apoc.lock.*` | ✅ Same | Locking mechanisms |
| `apoc.log.*` | ✅ Same | Logging functions |
| `apoc.merge.*` | ✅ Same | Merge operations |
| `apoc.meta.*` | ✅ Same | Metadata functions |
| `apoc.neighbors.*` | ✅ Same | Neighbor traversal |
| `apoc.nodes.*` | ✅ Same | Batch node operations |
| `apoc.number.*` | ✅ Same | Number formatting and conversion |
| `apoc.paths.*` | ✅ Same | Advanced path operations |
| `apoc.periodic.*` | ✅ Same | Periodic execution and batch processing |
| `apoc.refactor.*` | ✅ Same | Graph refactoring |
| `apoc.rel.*` | ✅ Same | Relationship operations |
| `apoc.schema.*` | ✅ Same | Schema management |
| `apoc.scoring.*` | ✅ Same | Scoring and similarity functions |
| `apoc.search.*` | ✅ Same | Full-text search |
| `apoc.spatial.*` | ✅ Same | Geographic functions |
| `apoc.stats.*` | ✅ Same | Statistical functions |
| `apoc.temporal.*` | ✅ Same | Advanced date/time operations |
| `apoc.trigger.*` | ✅ Same | Trigger management |
| `apoc.warmup.*` | ✅ Same | Database warmup |
| `apoc.xml.*` | ✅ Same | XML processing |

**Migration Notes:**
- Most queries work without modification
- All core APOC functions are implemented
- File operations respect security settings
- Plugin system allows custom extensions
- Performance characteristics may differ (often faster due to native Go implementation)

Test your specific APOC usage when migrating, particularly:
- File I/O operations (check security settings)
- Custom procedures (may need to be rewritten as plugins)
- Performance-sensitive queries (benchmark in your environment)


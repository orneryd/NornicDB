# Graph Traversal Guide

**Path queries and pattern matching in NornicDB**

---

## Overview

NornicDB supports powerful graph traversal capabilities through Cypher queries. This guide covers path finding, pattern matching, and relationship navigation.

---

## Basic Pattern Matching

### Find Connected Nodes

```cypher
-- Find all nodes connected to a specific node
MATCH (start:Person {name: 'Alice'})-[r]->(connected)
RETURN connected, type(r) AS relationship

-- Find nodes connected by a specific relationship type
MATCH (p:Person)-[:KNOWS]->(friend:Person)
RETURN p.name, friend.name
```

### Variable-Length Paths

```cypher
-- Find nodes within 1-3 hops
MATCH (start:Person {name: 'Alice'})-[*1..3]->(end)
RETURN DISTINCT end

-- Find all paths of any length (use with caution)
MATCH path = (a:Person)-[*]->(b:Person)
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN path
```

---

## Path Finding

### Shortest Path

```cypher
-- Find the shortest path between two nodes
MATCH path = shortestPath(
  (a:Person {name: 'Alice'})-[*]-(b:Person {name: 'Bob'})
)
RETURN path, length(path) AS hops
```

### All Shortest Paths

```cypher
-- Find all equally short paths
MATCH path = allShortestPaths(
  (a:Person {name: 'Alice'})-[*]-(b:Person {name: 'Bob'})
)
RETURN path
```

### Path Selectors (GQL)

A selector before a path pattern picks paths for each pair of start and end
nodes, shortest first. Unlike `shortestPath()`, a selector works on any
pattern (chains, quantified path patterns, a minimum length above 1, a start
node that is also the end node), and the clause's `WHERE` filters the
selected paths.

```cypher
MATCH p = ANY SHORTEST (a:Station {name: 'A'})-[:LINK]->+(b:Station {name: 'B'}) RETURN p
MATCH p = ALL SHORTEST (a:Station {name: 'A'})-[:LINK]->+(b:Station {name: 'B'}) RETURN p
MATCH p = SHORTEST 3 (a:Station {name: 'A'})-[:LINK]->+(b:Station) RETURN b, length(p)
MATCH p = SHORTEST 2 GROUPS (a:Station {name: 'A'})-[:LINK]->+(b:Station {name: 'B'}) RETURN p
MATCH p = ANY 5 PATHS (a:Station {name: 'A'})-[:LINK]->+(b:Station {name: 'B'}) RETURN p
MATCH p = SHORTEST $k (a:Station {name: 'A'})-[:LINK]->+(b:Station {name: 'B'}) RETURN p
```

`ALL` (or `ALL PATHS`) keeps every path, as a plain pattern does.

### Path Modes and Match Modes (Cypher 25)

A path mode after the selector says whether a path may repeat elements:
`TRAIL` (the default: no relationship repeats), `ACYCLIC` (no node repeats)
and `WALK`. A match mode after `MATCH` applies to the whole clause:
`DIFFERENT RELATIONSHIPS` (the default: no relationship appears twice in the
clause) or `REPEATABLE ELEMENTS`, under which relationships may repeat and
every quantifier needs an upper bound.

```cypher
MATCH p = ACYCLIC (a:Station {name: 'A'})-->+(b) RETURN p
MATCH p = SHORTEST 2 ACYCLIC (a)-->+(b {name: 'B'}) RETURN p
MATCH REPEATABLE ELEMENTS p = (a {name: 'A'})-->{1,4}(a) RETURN p   -- walks back to a
```

NornicDB reads path modes and match modes in Cypher 5 statements as well.

A relationship variable named twice in one `MATCH` (`(a)-[r]->()-[r]->(a)`)
is a SyntaxError when no match mode is written, as the openCypher TCK
requires. With a match mode written it is one relationship bound to both
places, as in Neo4j: no rows under `DIFFERENT RELATIONSHIPS`, a join under
`REPEATABLE ELEMENTS`.

### Quantified Path Patterns

A parenthesised path followed by a quantifier (`+`, `*`, `{n}`, `{m,n}`)
repeats it. A variable inside it binds a list, one value per repetition, and
its own `WHERE` applies to each repetition:

```cypher
MATCH p = (a:Station {name: 'A'})((x)-[r:LINK]->(y) WHERE r.open){1,3}(b:Station)
RETURN [n IN y | n.name] AS stops, [l IN r | l.minutes] AS legs
```

---

## Filtering Paths

### Filter by Relationship Type

```cypher
-- Only traverse specific relationship types
MATCH path = (a:Person)-[:KNOWS|:WORKS_WITH*1..5]->(b:Person)
WHERE a.name = 'Alice'
RETURN path
```

### Filter by Node Properties

```cypher
-- Only include nodes meeting criteria
MATCH path = (a:Person)-[*1..3]->(b:Person)
WHERE a.name = 'Alice'
  AND ALL(node IN nodes(path) WHERE node.active = true)
RETURN path
```

---

## Path Functions

| Function | Description | Example |
|----------|-------------|---------|
| `nodes(path)` | Get all nodes in path | `RETURN nodes(path)` |
| `relationships(path)` | Get all relationships | `RETURN relationships(path)` |
| `length(path)` | Number of relationships | `RETURN length(path)` |

---

## Common Patterns

### Friend of Friend

```cypher
MATCH (me:Person {name: 'Alice'})-[:KNOWS]->(friend)-[:KNOWS]->(foaf)
WHERE foaf <> me
  AND NOT (me)-[:KNOWS]->(foaf)
RETURN DISTINCT foaf.name AS suggestion
```

### Hierarchical Queries

```cypher
-- Find all reports (direct and indirect)
MATCH path = (manager:Employee {name: 'CEO'})-[:MANAGES*]->(report)
RETURN report.name, length(path) AS level
ORDER BY level
```

### Circular References

```cypher
-- Find cycles in the graph
MATCH path = (n)-[*2..]->(n)
RETURN path LIMIT 10
```

---

## BFS vs DFS

NornicDB exposes both breadth-first and depth-first expansion, with implicit and explicit forms.

### Implicit BFS — `shortestPath` / `allShortestPaths`

`shortestPath()` and `allShortestPaths()` use BFS internally; the first time they reach the target node, the path is shortest by hop count. No flag required.

### Implicit DFS — variable-length patterns

`-[*1..N]->` patterns enumerate paths via DFS. Useful when you want every path, not just the shortest:

```cypher
MATCH path = (a:Person {name: 'Alice'})-[:KNOWS*1..4]->(b:Person {name: 'Bob'})
RETURN path, length(path) AS hops
ORDER BY hops
```

### Explicit BFS / DFS — `apoc.path.expandConfig`

For workloads where you need a controllable spanning tree or an expansion frontier with predicate filters, use `apoc.path.expandConfig`. The `bfs` flag chooses the strategy:

```cypher
-- BFS expansion up to 5 hops, only along KNOWS / WORKS_WITH edges
MATCH (start:Person {name: 'Alice'})
CALL apoc.path.expandConfig(start, {
  relationshipFilter: 'KNOWS|WORKS_WITH>',
  minLevel: 1,
  maxLevel: 5,
  bfs: true,
  uniqueness: 'NODE_GLOBAL'
}) YIELD path
RETURN path

-- DFS spanning tree
MATCH (root:Project {id: 'p1'})
CALL apoc.path.spanningTree(root, {
  relationshipFilter: 'DEPENDS_ON>',
  bfs: false,
  maxLevel: 10
}) YIELD path
RETURN path
```

`uniqueness` modes mirror Neo4j: `NODE_GLOBAL`, `RELATIONSHIP_GLOBAL`, `NODE_PATH`, `RELATIONSHIP_PATH`, `NONE`.

---

## Weighted Path Finding

### Dijkstra (single-source weighted shortest path)

When edges carry a weight property, use `apoc.algo.dijkstra` for shortest path by sum of weights:

```cypher
MATCH (start:City {name: 'San Francisco'}),
      (end:City {name: 'New York'})
CALL apoc.algo.dijkstra(start, end, 'CONNECTED_TO', 'distance_km')
YIELD path, weight
RETURN path, weight
```

Arguments:

- `startNode`, `endNode` — bound from a preceding `MATCH`.
- `relationshipTypes` — comma-separated list, or `''` for all types.
- `weightProperty` — the edge property used as weight (omit / `''` for unit weights).

Returns the path with the lowest sum of `weightProperty` across its relationships.

### A* (heuristic-guided shortest path)

When you have a cheap heuristic (e.g., great-circle distance for geographic routing), `apoc.algo.aStar` is faster than Dijkstra on the same input:

```cypher
MATCH (start:City {name: 'San Francisco'}),
      (end:City {name: 'New York'})
CALL apoc.algo.aStar(
  start, end,
  'CONNECTED_TO',
  'distance_km',
  'lat', 'lon'
) YIELD path, weight
RETURN path, weight
```

The last two arguments name the latitude / longitude properties used to estimate remaining distance. `aStar` falls back to Dijkstra-equivalent behavior when the heuristic is zero.

### All simple paths

To enumerate every simple path (no repeated nodes) between two endpoints:

```cypher
MATCH (a:Person {name: 'Alice'}),
      (b:Person {name: 'Bob'})
CALL apoc.algo.allSimplePaths(a, b, 'KNOWS', 6)
YIELD path
RETURN path, length(path) AS hops
ORDER BY hops
```

The fourth argument bounds the maximum hop count — required, because the count of simple paths grows exponentially with depth.

---

## Neighborhood Queries

### Hop-bounded neighbors

```cypher
-- Every node within exactly N hops
MATCH (start:Person {name: 'Alice'})
CALL apoc.neighbors.byhop(start, 'KNOWS>', 3)
YIELD nodes
RETURN nodes

-- Every node within at most N hops (cumulative)
CALL apoc.neighbors.tohop(start, 'KNOWS>', 3)
YIELD nodes
RETURN nodes
```

`byhop` returns one row per hop level (1, 2, 3); `tohop` returns the union.

### Subgraph extraction

```cypher
-- Pull every node reachable from `start` within 4 hops
MATCH (start:Project {id: 'p1'})
CALL apoc.path.subgraphNodes(start, {
  relationshipFilter: 'DEPENDS_ON>',
  maxLevel: 4
}) YIELD node
RETURN node
```

---

## Centrality and Community Detection

These algorithms run over the full graph (or a relationship-filtered subgraph) and are useful for ranking, recommendation, and clustering workloads.

### PageRank

```cypher
CALL apoc.algo.pageRank(['Person'], ['KNOWS'])
YIELD node, score
RETURN node.name, score
ORDER BY score DESC
LIMIT 20
```

### Betweenness centrality

```cypher
CALL apoc.algo.betweenness(['Person'], ['KNOWS'])
YIELD node, score
RETURN node.name AS broker, score
ORDER BY score DESC
LIMIT 10
```

### Closeness centrality

```cypher
CALL apoc.algo.closeness(['Person'], ['KNOWS'])
YIELD node, score
RETURN node.name, score
ORDER BY score DESC
```

### Louvain communities

```cypher
CALL apoc.algo.louvain(['Person'], ['KNOWS'])
YIELD node, community
RETURN community, collect(node.name) AS members
ORDER BY size(members) DESC
```

### Label propagation

```cypher
CALL apoc.algo.labelPropagation(['Person'], ['KNOWS'])
YIELD node, label
RETURN label AS community, count(node) AS size
ORDER BY size DESC
```

### Weakly connected components

```cypher
CALL apoc.algo.wcc(['Person'], ['KNOWS'])
YIELD node, componentId
RETURN componentId, count(node) AS size
ORDER BY size DESC
```

---

## Link Prediction

NornicDB ships GDS-style link-prediction primitives. Each one scores how likely two nodes are to form an edge based on shared neighborhood structure:

```cypher
-- Common Neighbors: count of mutual neighbors
CALL gds.linkPrediction.commonNeighbors.stream({
  nodeQuery: 'MATCH (n:Person) RETURN id(n) AS id',
  relationshipQuery: 'MATCH (a)-[:KNOWS]->(b) RETURN id(a) AS source, id(b) AS target'
}) YIELD node1, node2, score
RETURN node1, node2, score
ORDER BY score DESC LIMIT 50

-- Adamic-Adar: weighted by inverse log of neighbor degree (rare connectors count more)
CALL gds.linkPrediction.adamicAdar.stream({...}) YIELD node1, node2, score

-- Jaccard similarity: |intersect| / |union| of neighborhoods
CALL gds.linkPrediction.jaccard.stream({...}) YIELD node1, node2, score

-- Preferential attachment: product of node degrees
CALL gds.linkPrediction.preferentialAttachment.stream({...}) YIELD node1, node2, score

-- Resource allocation: penalizes high-degree intermediaries
CALL gds.linkPrediction.resourceAllocation.stream({...}) YIELD node1, node2, score

-- Predict: combined score across the metrics above
CALL gds.linkPrediction.predict.stream({...}) YIELD node1, node2, score
```

Use `gds.fastRP.stream` to generate node embeddings as a feature input for ML pipelines:

```cypher
CALL gds.fastRP.stream({
  nodeProjection: 'Person',
  relationshipProjection: 'KNOWS',
  embeddingDimension: 128
}) YIELD nodeId, embedding
RETURN nodeId, embedding
```

---

## Workload → Procedure Reference

| Workload                                        | Use                                                |
|-------------------------------------------------|----------------------------------------------------|
| Find any path between two nodes                 | Variable-length pattern `-[*1..N]->`               |
| Find shortest unweighted path                   | `shortestPath()`                                   |
| Find all shortest unweighted paths              | `allShortestPaths()`                               |
| Find the k shortest paths per start and end     | `MATCH p = SHORTEST k (a)-->+(b)`                  |
| Repeat a multi-hop step with its own filter     | Quantified path pattern `((x)-[r]->(y) WHERE …)+`  |
| Find shortest weighted path                     | `apoc.algo.dijkstra`                               |
| Find shortest weighted path with heuristic      | `apoc.algo.aStar`                                  |
| Enumerate every simple path (bounded)           | `apoc.algo.allSimplePaths`                         |
| BFS from a starting node, frontier-by-frontier  | `apoc.path.expandConfig({bfs: true})`             |
| DFS spanning tree                               | `apoc.path.spanningTree({bfs: false})`             |
| Pull subgraph N hops out                        | `apoc.path.subgraphNodes`                          |
| Get neighbors at exactly N hops                 | `apoc.neighbors.byhop`                             |
| Get neighbors within N hops                     | `apoc.neighbors.tohop`                             |
| Rank nodes by influence                         | `apoc.algo.pageRank`                               |
| Find brokers / structural holes                 | `apoc.algo.betweenness`                            |
| Cluster nodes into communities                  | `apoc.algo.louvain` / `apoc.algo.labelPropagation` |
| Find disconnected sub-graphs                    | `apoc.algo.wcc`                                    |
| Score candidate edges (link prediction)         | `gds.linkPrediction.*`                             |
| Generate node embeddings                        | `gds.fastRP.stream`                                |
| Detect cycles                                   | `MATCH path = (n)-[*2..]->(n)` (DFS)               |
| Find hierarchical descendants                   | `MATCH (root)-[:CONTAINS*]->(d)`                   |
| Friend-of-friend recommendations                | Two-hop pattern + `NOT EXISTS` exclusion           |

---

## Performance Tips

1. **Limit path length** - Always specify max depth (`-[*1..5]->` not `-[*]->`)
2. **Use relationship types** - Filter by type to reduce search space
3. **Start from selective nodes** - Begin traversal from nodes with fewer connections
4. **Use LIMIT** - Limit results for exploratory queries
5. **Prefer `shortestPath` over variable-length when you only need one path** — BFS terminates as soon as the target is reached; variable-length DFS enumerates every match.
6. **Reach for `apoc.path.expandConfig` over hand-rolled traversal** — its uniqueness modes prevent revisiting nodes / relationships, which a naive variable-length match does not.
7. **Cache embeddings** — `gds.fastRP.stream` is deterministic for a fixed seed; persist results as node properties when running link prediction repeatedly.

---

## Related Documentation

- **[Cypher Queries](cypher-queries.md)** - Complete Cypher guide
- **[Cypher RAG Procedures](cypher-rag-procedures.md)** - Vector search + LLM integration
- **[Vector Search](vector-search.md)** - Semantic search
- **[Complete Examples](complete-examples.md)** - Full working examples

---

**Need more examples?** → **[Complete Examples](complete-examples.md)**

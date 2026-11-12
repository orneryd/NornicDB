# Louvain context API for v1.4.4

`pkg/louvain`, `apoc/community.Louvain` and the `apoc.algo.louvain` caller now
take the caller's context and report a run's interruption as an error. The
previous opaque signatures are retired; there is no forwarding adapter and no
optional context in configuration.

```go
func louvain.Run(ctx context.Context, graph *Graph, options Options) (Result, error)
func louvain.RunUndirected(ctx context.Context, size, relationshipCount int,
    relationship func(int) (int, int, float64, bool), options Options) (Result, error)
func community.Louvain(ctx context.Context, nodes []*Node, rels []*Relationship,
    config LouvainConfig) ([]CommunityResult, error)
```

`louvain.Final(Result)` is unchanged: a stopped run returns the zero `Result`,
so callers must check the error before reading a partition. Standalone callers
choose `context.Background()` themselves; `callApocAlgoLouvain` passes the
statement context, and the registered `apoc.community.louvain` handler receives
it through the existing plugin dispatch.

## What the context observes

The engine checks the context before every relationship it adapts, before it
reserves the adjacency buffer those relationships size, before every adjacency
read, and at each of its own work boundaries: per level, per pass, per colour
class, per node of a moving pass, per node of a modularity or aggregation sweep,
and per node of the output pass. The passes a level runs over every node also
check per node: the degree total, the initial membership and representative
map, the representative numbering, the dense renumbering, and the level's
membership together with the copy it publishes. A scan whose length one node's
arcs or candidate communities control checks once per stride (the `checkStride`
constant, 1024 steps): a node's arcs in the moving pass, its candidate
communities, its arcs in a modularity or aggregation sweep, its neighbours while
colouring, and the distinct targets an aggregate emits. `apoc.algo.louvain`
additionally checks before each node's adjacency enumeration, so a cancelled
call stops reading adjacency instead of finishing the population pass.

A stopped call returns the caller's error unchanged (`context.Canceled` or
`context.DeadlineExceeded`) and publishes nothing: `apoc.algo.louvain` returns no
rows, `community.Louvain` returns no communities, and `louvain.Run` returns the
zero `Result`. The statement's own cancellation gate and final observation are
unchanged.

## What stays uninterruptible

Each check is a boundary, not a preemption point. Work already in flight
completes: the relationship callback currently running (in `apoc.algo.louvain`,
one `ReadAdjacentEdges` call), at most one stride of the arc, candidate or
neighbour scan in flight, the `sort.Ints` calls that order a node's neighbours
and an aggregate's targets, the `math.Pow` of a pass, and the reservation
`NewGraph` makes once a level's arc counts are complete (the checks before it
run first). A cancelled run therefore stops at the next boundary rather than
inside one, and the returned error is observed by whichever boundary comes
first. `louvain.Renumber` is a caller-facing helper without a context and is not
part of a run, and `NewGraph` keeps its exported signature, so a reservation it
is asked to make is never interruptible once started.

## Unchanged surface

Cypher, HTTP and Bolt syntax, `apoc.algo.louvain`'s `(nodes, relationshipTypes,
config)` arguments, community identifiers and their numbering, the weighted,
parallel, self-loop, seeded and tie behaviour, Badger keys, persisted values,
WAL and the storage version are unchanged; the existing GDS reference partitions
still match exactly. No dependency is patched. Go consumers and the bundled
plugin must be rebuilt against the new signatures.

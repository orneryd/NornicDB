package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// relationshipAccessExecution is the only entry to the relationship read
// kernels. It obtains the same compiled record/decision that EXPLAIN projects;
// individual consumers cannot choose an independent scan or seek.
type relationshipAccessExecution struct {
	executor *StorageExecutor
	pattern, where string
	access *relationshipAccess
}

func (e *StorageExecutor) relationshipExecutionFor(ctx context.Context, pattern, where string) relationshipAccessExecution {
	if original, ok := ctx.Value(relationshipSeekPredicateKey{}).(relationshipSeekPredicate); ok && original.pattern == pattern {
		where = original.where
	}
	return relationshipAccessExecution{executor:e, pattern:pattern, where:where, access:e.relationshipAccessFor(ctx, pattern, where)}
}

func (run relationshipAccessExecution) count(ctx context.Context, match *TraversalMatch, item returnItem) (int64, bool, error) {
	if len(run.access.alternatives) != 0 { return 0, false, nil }
	n, ok, err := run.executor.relationshipCountFromStore(ctx, match, item)
	if ok { recordRelationshipSeekFallback(ctx, run.pattern, run.where, match.Relationship.Variable) }
	return n, ok, err
}

func (run relationshipAccessExecution) aggregate(ctx context.Context, match *TraversalMatch, items []returnItem) ([][]interface{}, bool, error) {
	if len(run.access.alternatives) != 0 { return nil, false, nil }
	rows, ok, err := run.executor.tryFastRelationshipAggregations(match, items)
	if ok { recordRelationshipSeekFallback(ctx, run.pattern, run.where, match.Relationship.Variable) }
	return rows, ok, err
}

func (run relationshipAccessExecution) degree(ctx context.Context, node *storage.Node, shape boundDegreeShape) int64 {
	recordRelationshipSeekFallback(ctx, run.pattern, run.where, shape.relationship)
	return run.executor.nodeDegreeCount(ctx, node, shape)
}

// paths keeps the existing seed/streaming/temporal kernels below the one access
// decision. optimizedStart is a node-index pruning result; start/end are actual
// bound endpoints and therefore also constrain a relationship-index result.
func (run relationshipAccessExecution) paths(ctx context.Context, match *TraversalMatch, start, end, optimizedStart []*storage.Node) ([]PathResult, error) {
	e := run.executor
	if paths, sought, err := run.trySeek(ctx, match, start, end); sought || err != nil { return paths, err }
	if optimizedStart == nil { optimizedStart = start }
	return e.scanRelationshipPaths(ctx, match, optimizedStart, end, run.where)
}

func (e *StorageExecutor) scanRelationshipPaths(ctx context.Context, match *TraversalMatch, start, end []*storage.Node, where string) ([]PathResult, error) {
	var paths []PathResult
	if seeded, ok := e.traverseFromEndSeeds(ctx, match, start, end, where); ok { return seeded, nil }
	if len(start) > 0 {
		if match.IsChained && len(match.Segments) > 1 {
			paths = e.traverseChainedGraph(ctx, match, start)
		} else {
			viewport, _ := TemporalViewportFromContext(ctx)
			checker, _ := e.getStorage(ctx).(temporalCurrentNodeChecker)
			paths = e.traverseGraphSequential(ctx, match, start, viewport, checker)
		}
	} else if match.TraversalLimit > 0 && !match.IsChained && len(match.StartNode.properties) == 0 {
		viewport, _ := TemporalViewportFromContext(ctx)
		checker, _ := e.getStorage(ctx).(temporalCurrentNodeChecker)
		var streamed bool
		var err error
		paths, streamed, err = e.traverseGraphWithStreamingStartNodes(ctx, match, viewport, checker)
		if err != nil { return nil, err }
		if !streamed { paths = e.traverseGraph(ctx, match) }
	} else {
		paths = e.traverseGraph(ctx, match)
	}
	if where != "" { paths = e.filterPathsByWhere(ctx, paths, match, where, nil) }
	return paths, nil
}

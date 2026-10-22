package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Release retains its existing traversal fallback; develop supplies its own
// expansion kernel below the same program decision without a feature port.
func (run relationshipAccessExecution) scanRelationshipCount(ctx context.Context, match *TraversalMatch, seed *storage.Node, where string, values map[string]interface{}, first bool) int64 {
	e := run.executor
	var paths []PathResult
	if match.IsChained && len(match.Segments) > 1 {
		paths = run.traverseChainedGraph(ctx, match, []*storage.Node{seed})
	} else {
		paths = run.traverseFromNode(ctx, seed, match)
	}
	return e.countRelationshipPaths(run.statement(), ctx, paths, match, where, values, first)
}

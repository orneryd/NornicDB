package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Release retains its existing traversal fallback; develop supplies its own
// expansion kernel below the same program decision without a feature port.
func (e *StorageExecutor) scanRelationshipCount(ctx context.Context, match *TraversalMatch, seed *storage.Node, where string, values map[string]interface{}, first bool) int64 {
	var paths []PathResult
	if match.IsChained && len(match.Segments) > 1 {
		paths = e.traverseChainedGraph(ctx, match, []*storage.Node{seed})
	} else {
		paths = e.traverseFromNode(ctx, seed, match)
	}
	return e.countRelationshipPaths(ctx, paths, match, where, values, first)
}

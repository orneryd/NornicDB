package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Develop keeps its existing bound expansion kernel. The program decides
// whether it runs; this adapter does not select a relationship access plan.
func (run relationshipAccessExecution) scanRelationshipCount(ctx context.Context, match *TraversalMatch, seed *storage.Node, where string, values map[string]interface{}, first bool) int64 {
	e := run.executor
	if count, ok := e.boundExpandCount(ctx, match, seed, where, values, first); ok {
		return count
	}
	return e.boundTraversalCount(ctx, match, seed, where, values, first)
}

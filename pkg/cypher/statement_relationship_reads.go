package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// relationshipDecisionKey scopes current relationship reads to a scalar graph
// expression inside a mutation. The enclosing MATCH/DELETE traversal keeps its
// ordinary snapshot even when the statement also writes.
type relationshipDecisionKey struct{}

func mutationRelationshipDecisionContext(ctx context.Context) context.Context {
	if _, active := ctx.Value(relationshipDecisionKey{}).(storage.EdgeMatchReadPolicy); active {
		return ctx
	}
	statement, ok := ctx.Value(queryEmbeddingStatementKey{}).(*statementExecutionContext)
	if !ok || statement.guardRelationshipReads != storage.CurrentEdgeMatch {
		return ctx
	}
	return context.WithValue(ctx, relationshipDecisionKey{}, storage.CurrentEdgeMatch)
}

// statementAdjacentRead applies only the active graph-expression decision
// scope. Admission's write effect makes a statement eligible; it does not
// change ordinary MATCH, SET-target or DELETE traversal reads.
func statementAdjacentRead(ctx context.Context, request storage.AdjacentEdgeRead) storage.AdjacentEdgeRead {
	if policy, ok := ctx.Value(relationshipDecisionKey{}).(storage.EdgeMatchReadPolicy); ok {
		request.ReadPolicy = policy
	}
	return request
}

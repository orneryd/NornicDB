package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// statementAdjacentRead applies the admitted statement's relationship view to
// its traversal request. Ordinary statements retain the transaction snapshot;
// decisions in a mutation read current committed relationships plus own writes.
// The immutable statement owner also reaches correlated subqueries, without
// changing the reused transaction adapter or reparsing the query per read.
func statementAdjacentRead(ctx context.Context, request storage.AdjacentEdgeRead) storage.AdjacentEdgeRead {
	if statement, ok := ctx.Value(queryEmbeddingStatementKey{}).(*statementExecutionContext); ok {
		request.ReadPolicy = statement.relationshipReads
	}
	return request
}

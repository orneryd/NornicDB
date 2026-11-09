package cypher

import (
	"context"
	"time"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// registryFunctionContext supplies execution state consistently to every
// registry dispatcher. Each caller supplies evaluation in its own bindings and
// may add Nodes/Rels fast paths without rebuilding the statement identity.
func (e *StorageExecutor) registryFunctionContext(ctx context.Context, eval func(string) (interface{}, error)) cypherfn.Context {
	metadataCtx := ctx
	if inherited := rowStatementContext(valueBindingsFromContext(ctx)); inherited != nil {
		metadataCtx = inherited
	}
	return cypherfn.Context{
		Storage:      e.storage,
		Database:     e.executionDatabaseName(metadataCtx),
		Eval:         eval,
		Now:          time.Now,
		Graphs:       e,
		GraphVisible: catalogDatabaseVisible,
		GraphContext: metadataCtx,
		Cypher25:     cypherVersionFromContext(metadataCtx) == "25",
	}
}

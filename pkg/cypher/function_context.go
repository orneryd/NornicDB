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
	return cypherfn.Context{
		Storage:  e.storage,
		Database: e.executionDatabaseName(ctx),
		Eval:     eval,
		Now:      time.Now,
		Graphs:   e,
		Request:  ctx,
		Cypher25: cypherVersionFromContext(ctx) == "25",
	}
}

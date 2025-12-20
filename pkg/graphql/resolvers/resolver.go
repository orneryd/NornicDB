package resolvers

import (
	"context"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.

// Resolver is the root resolver for the GraphQL schema.
// All operations use namespaced storage to ensure proper database isolation.
type Resolver struct {
	// DB is used only for admin operations (embeddings, search stats, etc.)
	// All node/edge operations use namespaced storage via dbManager.
	DB        *nornicdb.DB
	dbManager *multidb.DatabaseManager // Required: provides namespaced storage for all operations
	StartTime time.Time
}

// NewResolver creates a new resolver that uses the default database's
// namespaced storage engine. This ensures all operations executed via GraphQL
// use the correct database namespace.
//
// Parameters:
//   - db: Base database instance (used for admin operations)
//   - dbManager: Database manager (required for namespaced storage)
func NewResolver(db *nornicdb.DB, dbManager *multidb.DatabaseManager) *Resolver {
	if dbManager == nil {
		panic("dbManager is required for GraphQL resolver")
	}
	return &Resolver{
		DB:        db,
		dbManager: dbManager,
		StartTime: time.Now(),
	}
}

// getCypherExecutor returns the Cypher executor using the default database's namespaced storage.
func (r *Resolver) getCypherExecutor(ctx context.Context) (*cypher.StorageExecutor, error) {
	// Use default database's namespaced storage
	defaultStorage, err := r.dbManager.GetDefaultStorage()
	if err != nil {
		return nil, err
	}
	executor := cypher.NewStorageExecutor(defaultStorage)

	// Copy configuration from base DB's executor if available
	if r.DB != nil {
		baseExecutor := r.DB.GetCypherExecutor()
		if baseExecutor != nil {
			// Note: embedder and callback are set on the base executor,
			// but we can't easily copy them without getters. The namespaced
			// executor will work without them, but embedding generation may
			// not work until the base executor's callbacks are wired up.
		}
	}
	return executor, nil
}

// executeCypher executes a Cypher query using the default database's namespaced storage.
func (r *Resolver) executeCypher(ctx context.Context, query string, params map[string]interface{}) (*nornicdb.CypherResult, error) {
	executor, err := r.getCypherExecutor(ctx)
	if err != nil {
		return nil, err
	}

	result, err := executor.Execute(ctx, query, params)
	if err != nil {
		return nil, err
	}

	return &nornicdb.CypherResult{
		Columns: result.Columns,
		Rows:    result.Rows,
	}, nil
}

// getStorage returns the default database's namespaced storage engine.
func (r *Resolver) getStorage() (storage.Engine, error) {
	return r.dbManager.GetDefaultStorage()
}

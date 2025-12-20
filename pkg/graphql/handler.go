package graphql

import (
	"net/http"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/orneryd/nornicdb/pkg/graphql/generated"
	"github.com/orneryd/nornicdb/pkg/graphql/resolvers"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

// Handler represents the GraphQL HTTP handler.
type Handler struct {
	server           *handler.Server
	playgroundHandle http.Handler
}

// NewHandler creates a new GraphQL handler that uses the default database's
// namespaced storage engine. This ensures all nodes created via GraphQL are
// properly namespaced and visible in the default database.
//
// The handler uses the default database from the database manager, ensuring
// proper multi-database isolation and preventing orphaned nodes.
//
// Parameters:
//   - db: Base database instance (used for admin operations like embeddings)
//   - dbManager: Database manager that provides namespaced storage engines
//
// All GraphQL operations (create, update, delete, query) use namespaced storage
// to ensure proper database isolation.
func NewHandler(db *nornicdb.DB, dbManager *multidb.DatabaseManager) *Handler {
	resolver := resolvers.NewResolver(db, dbManager)
	srv := handler.NewDefaultServer(generated.NewExecutableSchema(generated.Config{Resolvers: resolver}))

	return &Handler{
		server:           srv,
		playgroundHandle: playground.Handler("NornicDB GraphQL Playground", "/graphql"),
	}
}

// ServeHTTP handles GraphQL requests using gqlgen.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.server.ServeHTTP(w, r)
}

// Playground returns the GraphQL playground handler with full introspection.
func (h *Handler) Playground() http.Handler {
	return h.playgroundHandle
}

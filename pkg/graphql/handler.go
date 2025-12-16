package graphql

import (
	"net/http"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/orneryd/nornicdb/pkg/graphql/generated"
	"github.com/orneryd/nornicdb/pkg/graphql/resolvers"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

// Handler represents the GraphQL HTTP handler.
type Handler struct {
	server           *handler.Server
	playgroundHandle http.Handler
}

// NewHandler creates a new GraphQL handler with full introspection support.
func NewHandler(db *nornicdb.DB) *Handler {
	resolver := resolvers.NewResolver(db)
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


package graphql

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	gql "github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/gorilla/websocket"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/graphql/generated"
	"github.com/orneryd/nornicdb/pkg/graphql/resolvers"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// Handler represents the GraphQL HTTP handler.
type Handler struct {
	server           *handler.Server
	playgroundHandle http.Handler
	policy           *memory.Policy
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
	return newHandler(db, dbManager, db.MemoryPolicy())
}

func newHandler(db *nornicdb.DB, dbManager *multidb.DatabaseManager, policy *memory.Policy) *Handler {
	resolver := resolvers.NewResolver(db, dbManager)
	schema := generated.NewExecutableSchema(generated.Config{Resolvers: resolver})
	var srv *handler.Server
	if policy == nil {
		// Preserve the default transport implementation and behavior exactly.
		srv = handler.NewDefaultServer(schema)
	} else {
		// gqlgen's supported adapter hook runs before envelope/init/variables
		// JSON decoding. Keep all other NewDefaultServer settings unchanged.
		srv = handler.New(schema)
		srv.AddTransport(transport.Websocket{Implementation: policyWebsocketImplementation{}, KeepAlivePingInterval: 10 * time.Second})
		srv.AddTransport(transport.Options{})
		srv.AddTransport(transport.GET{})
		srv.AddTransport(transport.POST{})
		srv.AddTransport(transport.MultipartForm{})
		srv.SetQueryCache(lru.New[*ast.QueryDocument](1000))
		srv.Use(extension.Introspection{})
		srv.Use(extension.AutomaticPersistedQuery{Cache: lru.New[string](100)})
	}

	// Author: Codex,NOR-180. One root-field completion owns all statements
	// that its resolver aggregates. Loops keep their ordinary count policies;
	// a produced terminal failure is retained before any result is marshalled.
	srv.AroundFields(func(ctx context.Context, next gql.Resolver) (any, error) {
		// A root field's parent is the operation's synthetic object context,
		// which has no selected field; every nested field's parent has one.
		field := gql.GetFieldContext(ctx)
		if field == nil || (field.Parent != nil && field.Parent.Field.Field != nil) {
			return next(ctx)
		}
		outcome := nornicerrors.NewTerminalOutcomeScope(ctx)
		result, err := next(outcome)
		err = outcome.Complete(err)
		if nornicerrors.HasTerminalOutcome(err) {
			return nil, err
		}
		return result, err
	})
	srv.SetErrorPresenter(presentGraphQLError)
	return &Handler{
		server:           srv,
		policy:           policy,
		playgroundHandle: playground.Handler("NornicDB GraphQL Playground", "/graphql"),
	}
}

// Author: Codex, NOR-180. gqlgen may select a contained primary gqlerror and
// discard its joined siblings. Keep its status, path and extensions while the
// existing shared status owner carries supplementary terminal diagnostics.
func presentGraphQLError(ctx context.Context, err error) *gqlerror.Error {
	presented := gql.DefaultErrorPresenter(ctx, err)
	if presented == nil || !nornicerrors.HasStatusDiagnostic(err) {
		return presented
	}
	retained := *presented
	err = nornicerrors.NormalizeTerminalOutcome(err)
	retained.Message = nornicerrors.AppendStatusDiagnostics(err, "", nil)
	retained.Err = err
	return &retained
}

// ServeHTTP handles GraphQL requests using gqlgen.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.policy == nil {
		h.server.ServeHTTP(w, r)
		return
	}
	if wire := memory.WireRequestFromContext(r.Context()); wire != nil {
		h.serveWithWireRequest(w, r, wire)
		return
	}
	if r.Method == http.MethodGet && websocket.IsWebSocketUpgrade(r) {
		wire := memory.NewWireRequest(h.policy)
		_ = wire.Run(func() error {
			h.serveWithWireRequest(w, r, wire)
			return nil
		})
		return
	}
	// Public standalone handlers use the same body/query/header admission as
	// the HTTP server. Delegated handlers borrow its owner instead of charging
	// twice or releasing credit before their enclosing consumer returns.
	memory.HandleHTTPRequest(h.policy, w, r, memory.HTTPWireOptions{}, writeGraphQLAdmissionError, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.serveWithWireRequest(w, r, memory.WireRequestFromContext(r.Context()))
	}))
}

func (h *Handler) serveWithWireRequest(w http.ResponseWriter, r *http.Request, wire *memory.WireRequest) {
	owner := &websocketRequestOwner{wire: wire}
	defer owner.close()
	h.server.ServeHTTP(w, withWebsocketRequestOwner(r, owner))
}

func writeGraphQLAdmissionError(w http.ResponseWriter, _ *http.Request, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]any{"errors": []map[string]string{{"code": websocketMemoryErrorCode, "message": err.Error()}}})
}

// Playground returns the GraphQL playground handler with full introspection.
func (h *Handler) Playground() http.Handler {
	return h.playgroundHandle
}

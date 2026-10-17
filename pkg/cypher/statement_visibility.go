package cypher

import (
	"context"
	"errors"
	"sync"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// WithPrincipalAccess attaches a principal's one resolver to a statement's
// context for database: its permission check (as
// WithDatabasePermissionResolver) and its graph visibility, which the
// executor applies to every read once the statement is admitted. USE routing
// moves both to the selected database.
func WithPrincipalAccess(ctx context.Context, database string, access PrincipalAccess) context.Context {
	var visible func(string) (*GraphVisibility, error)
	if access.Visibility != nil {
		visible = access.VisibilityOf
	}
	return context.WithValue(ctx, databaseAuthorizationKey{}, databaseAuthorization{
		database: database,
		resolver: access.Allows,
		visible:  visible,
	})
}

// VisibilityOf is the principal's visibility on graph, a database, alias or
// composite constituent: the visibility of the database its data is in,
// mapped as Allows maps it (GraphAccess). A graph the principal may not use
// at all is refused before this is asked; were it asked, it would fail
// closed.
func (p PrincipalAccess) VisibilityOf(graph string) (*GraphVisibility, error) {
	database := graph
	if p.GraphAccess != nil {
		target, allowed := p.GraphAccess(graph)
		if !allowed {
			return nil, &PermissionDeniedError{Permission: "access"}
		}
		database = target
	}
	return p.Visibility(database)
}

// statementRestricted reports whether the statement's principal is
// restricted on the statement's database; a visibility that can't be
// resolved counts as restricted.
func statementRestricted(ctx context.Context) bool {
	authorization, ok := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization)
	if !ok || authorization.visible == nil {
		return false
	}
	visibility, err := authorization.visible(authorization.database)
	return err != nil || visibility != nil
}

// withGraphVisibility gives the statement authorization attached to ctx the
// principal's graph visibility, keeping its database and permission
// resolver.
func withGraphVisibility(ctx context.Context, visible func(database string) (*GraphVisibility, error)) context.Context {
	authorization, _ := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization)
	authorization.visible = visible
	return context.WithValue(ctx, databaseAuthorizationKey{}, authorization)
}

// requestPrincipalAccessKey carries a request's PrincipalAccess for the
// routes that run Cypher on a principal's behalf outside Bolt and the HTTP
// transaction API (GraphQL, Heimdall, MCP), so they attach the same resolver.
type requestPrincipalAccessKey struct{}

// WithRequestPrincipalAccess attaches a request principal's resolver.
func WithRequestPrincipalAccess(ctx context.Context, access PrincipalAccess) context.Context {
	return context.WithValue(ctx, requestPrincipalAccessKey{}, access)
}

// RequestPrincipalAccess is the request principal's resolver, if attached.
func RequestPrincipalAccess(ctx context.Context) (PrincipalAccess, bool) {
	access, ok := ctx.Value(requestPrincipalAccessKey{}).(PrincipalAccess)
	return access, ok
}

// RestrictedOn reports whether access restricts what its principal sees on
// database, and fails closed: a visibility that can't be resolved counts as
// restricted.
func (p PrincipalAccess) RestrictedOn(database string) bool {
	if p.Visibility == nil {
		return false
	}
	visibility, err := p.VisibilityOf(database)
	return err != nil || visibility != nil
}

// statementVisibility is the visibility the statement's principal has on
// the statement's database, or nil when it is unrestricted there (or the
// statement carries no principal).
func (e *StorageExecutor) statementVisibility(ctx context.Context) (*visibilityEvaluator, error) {
	authorization, ok := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization)
	if !ok || authorization.visible == nil {
		return nil, nil
	}
	database := authorization.database
	if database == "" {
		database = e.currentDatabaseName()
	}
	visibility, err := authorization.visible(database)
	if err != nil || visibility == nil {
		return nil, err
	}
	inner := e.storage
	tx := e.txContext
	inTransaction := tx != nil && tx.active
	if inTransaction && tx.storageWrapper != nil {
		inner = tx.storageWrapper
	}
	if view, ok := inner.(*visibleGraphView); ok {
		inner = view.Engine
	}
	eval := newVisibilityEvaluator(visibility, inner)
	// An explicit transaction's statements share what they create.
	if inTransaction {
		if tx.restrictedCreated == nil {
			tx.restrictedCreated = newCreatedNodes()
		}
		eval.created = tx.restrictedCreated
	}
	return eval, nil
}

// withStatementVisibility is a clone of e whose statement reads through
// eval's view.
func (e *StorageExecutor) withStatementVisibility(eval *visibilityEvaluator) *StorageExecutor {
	clone := e.cloneWithStorage(e.storage)
	clone.visibility = eval
	clone.storage = newVisibleGraphView(clone.storage, eval)
	clone.nodeLookupCache = make(map[string]*storage.Node)
	clone.nodeLookupCacheMu = &sync.RWMutex{}
	return clone
}

// applyVisibility gives a clone of a restricted statement's executor the
// statement's view: its storage wrapped, the visibility kept, and its own
// empty MERGE lookup cache, since the shared one can hold hidden nodes.
func (e *StorageExecutor) applyVisibility(clone *StorageExecutor) *StorageExecutor {
	if e.visibility == nil {
		return clone
	}
	clone.visibility = e.visibility
	clone.storage = newVisibleGraphView(clone.storage, e.visibility)
	clone.nodeLookupCache = make(map[string]*storage.Node)
	clone.nodeLookupCacheMu = &sync.RWMutex{}
	return clone
}

// restrictedMultiGraphError refuses a multi-graph statement (Fabric's CALL {
// USE … } routing) for a principal restricted on any database, before it
// runs: those statements run fragments on other executors the view isn't
// installed on. A visibility that can't be resolved refuses too; a database
// the principal can't access at all is no restriction here, since the
// statement's own per-graph authorization refuses it.
func (e *StorageExecutor) restrictedMultiGraphError(ctx context.Context) error {
	authorization, ok := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization)
	if !ok || authorization.visible == nil {
		return nil
	}
	databases := []string{authorization.database, e.currentDatabaseName()}
	if e.dbManager != nil {
		for _, info := range e.dbManager.ListDatabases() {
			databases = append(databases, info.Name())
		}
	}
	for _, database := range databases {
		if database == "" {
			continue
		}
		visibility, err := authorization.visible(database)
		var denied *PermissionDeniedError
		if errors.As(err, &denied) {
			continue
		}
		if err != nil || visibility != nil {
			return localizedStatusError("Neo.ClientError.Security.Forbidden", "Forbidden", localization.CypherAdminRestrictedMultiGraph())
		}
	}
	return nil
}

// RestrictedGraphView is engine as a restricted principal reads it
// (visibility), for routes that read storage directly rather than through
// Cypher: every node and relationship read is filtered, as a statement's
// reads are. A nil visibility returns engine unchanged.
func RestrictedGraphView(engine storage.Engine, visibility *GraphVisibility) storage.Engine {
	if visibility == nil || engine == nil {
		return engine
	}
	return newVisibleGraphView(engine, newVisibilityEvaluator(visibility, engineBeneathView(engine)))
}

// RestrictedGraphViewParts splits a RestrictedGraphView into the engine
// beneath it and its node visibility, for a route that has to read history
// the view can't serve (MVCC snapshots): it decides which nodes are visible
// now through visible, then reads their history from the engine. ok is
// false for an engine that is no restricted view.
func RestrictedGraphViewParts(engine storage.Engine) (inner storage.Engine, visible func(id storage.NodeID) bool, ok bool) {
	view, isView := engine.(*visibleGraphView)
	if !isView {
		return engine, nil, false
	}
	return view.Engine, view.eval.nodeIDVisible, true
}

// visibleGraphProjection is lookupGraphProjection for a statement reading
// engine. For a restricted principal a projection is refused once it holds
// a node the principal can no longer see (its membership or the access
// scope changed since it was projected), so saved topology and properties
// never outlive the visibility they were read under; a node deleted since
// doesn't count. The catalog key (database, executing principal, name)
// separates principals; this check is what keeps a principal's own
// projection within what it sees now.
func visibleGraphProjection(engine storage.Engine, database, name string, contexts ...context.Context) (*GraphProjection, error) {
	projection, err := lookupGraphProjection(database, name, contexts...)
	if err != nil {
		return nil, err
	}
	view, ok := engine.(*visibleGraphView)
	if !ok {
		return projection, nil
	}
	for _, id := range projection.NodeIDs {
		if !view.eval.nodeIDVisible(storage.NodeID(id)) && !orphanedNode(view, id) {
			return nil, localizedError(localization.CypherAdminRestrictedProjectionStale(name), nil)
		}
	}
	return projection, nil
}

// restrictedReader is the statement principal's visibility as every search
// consumer takes it (search.RestrictedReader: node and relationship search,
// graph search, the vector index queries), or nil when it is unrestricted.
// The search ranks only what the principal can see, so its plans, scores and
// top-K are those of the visible corpus.
func (e *StorageExecutor) restrictedReader() *search.RestrictedReader {
	visibility := e.visibility
	if visibility == nil {
		return nil
	}
	return &search.RestrictedReader{
		Node:         func(id string) bool { return visibility.nodeIDVisible(storage.NodeID(id)) },
		Relationship: func(id string) bool { return visibility.edgeIDVisible(storage.EdgeID(id)) },
	}
}

// orphanedNode reports whether the node an index entry names is gone from
// the database itself: through a restricted view, a hidden node reads as
// missing, and its index entries must stay.
func orphanedNode(engine storage.Engine, id string) bool {
	_, err := engineBeneathView(engine).GetNode(storage.NodeID(id))
	return errors.Is(err, storage.ErrNotFound)
}

// engineBeneathView is the storage a restricted executor's view wraps, for
// the code that looks for the transaction engines beneath an executor's
// storage (handleBegin, resolveImplicitTxEngines). Reads never use it.
func engineBeneathView(engine storage.Engine) storage.Engine {
	if view, ok := engine.(*visibleGraphView); ok {
		return view.Engine
	}
	return engine
}

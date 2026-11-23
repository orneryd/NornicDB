package cypher

import (
	"context"
	"sync"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) bindFineGrainedGraph(ctx context.Context, query string) (*StorageExecutor, error) {
	request := auth.FineGrainedRequestFromContext(ctx)
	if request == nil {
		return nil, nil
	}
	if request.PasswordChangeRequired && !isFineGrainedOwnPasswordChange(ctx, query) {
		return nil, &auth.FineGrainedCredentialsExpired{}
	}
	database := e.executionDatabaseName(ctx)
	if !request.CanAccessDatabase(database) {
		return nil, &PermissionDeniedError{Permission: "database.access"}
	}
	if hasRevealCall(query) && !request.AllowsDatabase("database.reveal", database, "") {
		return nil, &PermissionDeniedError{Permission: "database.reveal"}
	}
	if action := fineGrainedSchemaAction(ctx, query); action != "" && !request.AllowsDatabase(action, database, "") {
		return nil, auth.ErrFineGrainedForbidden
	}
	if action := fineGrainedDBMSStatementAction(ctx, query); action != "" && !request.AllowsDBMS(action, "") {
		return nil, auth.ErrFineGrainedForbidden
	}
	for _, procedure := range RegisteredProceduresForCalls(ctx, query, AcceptedSource) {
		action, selector := fineGrainedProcedureAction(procedure.Name)
		if action == "database.procedure.execute" {
			execute, deny := request.Access.Decision("dbms.procedure.execute", auth.FineGrainedResource{Kind: auth.FineGrainedDBMS, Property: selector})
			if !deny && (execute || request.AllowsDatabase(action, database, selector)) {
				continue
			}
		} else if request.AllowsDatabase(action, database, selector) {
			continue
		}
		return nil, auth.ErrFineGrainedForbidden
	}
	engine := e.executionStorage(ctx)
	if request.GraphMatches(e.storage, engine, database) {
		return nil, nil
	}
	scoped := e.cloneWithStorage(request.Graph(engine, database))
	scoped.fineGrainedGraph = request
	scoped.fineGrainedSource = engine
	scoped.nodeLookupCache = make(map[string]*storage.Node)
	scoped.nodeLookupCacheMu = &sync.RWMutex{}
	return scoped, nil
}

// Procedure aliases point at the same action as their registry owner.
func fineGrainedProcedureAction(procedure string) (string, string) {
	switch procedure {
	case "kp.decayInfo", "nornicdb.decay.info":
		return "knowledge.decay.info", ""
	case "kp.info", "nornicdb.knowledgepolicy.info":
		return "knowledge.info", ""
	case "kp.profiles", "nornicdb.knowledgepolicy.profiles":
		return "knowledge.profiles.list", ""
	case "kp.policies", "nornicdb.knowledgepolicy.policies":
		return "knowledge.promotion_policy.list", ""
	case "kp.resolve", "nornicdb.knowledgepolicy.resolve":
		return "knowledge.resolve", ""
	case "kp.deindexStatus", "nornicdb.knowledgepolicy.deindexStatus":
		return "knowledge.deindex.status", ""
	case "db.temporal.asOf":
		return "database.history.read", ""
	case "db.cdc.current", "db.cdc.earliest", "db.cdc.query":
		return "database.cdc.read", ""
	default:
		return "database.procedure.execute", procedure
	}
}

func (e *StorageExecutor) bindBoostedFineGrainedProcedure(ctx context.Context, procedure string) (context.Context, *StorageExecutor) {
	request := auth.FineGrainedRequestFromContext(ctx)
	// Scoped products such as managed policy or change feed access have their
	// own invocation contracts. Their authority never comes from EXECUTE.
	if request == nil || e.fineGrainedSource == nil {
		return ctx, e
	}
	if action, _ := fineGrainedProcedureAction(procedure); action != "database.procedure.execute" {
		return ctx, e
	}
	database := e.executionDatabaseName(ctx)
	boosted := request.WithBoostedProcedure(database, procedure)
	if boosted == nil {
		return ctx, e
	}
	scoped := e.cloneWithStorage(boosted.Graph(e.fineGrainedSource, database))
	scoped.fineGrainedGraph = boosted
	scoped.nodeLookupCache = make(map[string]*storage.Node)
	scoped.nodeLookupCacheMu = &sync.RWMutex{}
	return auth.WithFineGrainedRequest(ctx, boosted), scoped
}

// FineGrainedCypherAccess adapts database selection to statement admission.
// The graph and specific command owners apply the entitlements at execution.
func FineGrainedCypherAccess(manager GraphDatabaseResolver, request *auth.FineGrainedRequest) PrincipalAccess {
	return PrincipalAccess{GraphAccess: func(graph string) (string, bool) {
		target, remote, allowed := GraphAccessTarget(manager, graph, request.CanAccessDatabase)
		return target, allowed && !remote
	}}
}

func fineGrainedSchemaAction(ctx context.Context, query string) string {
	switch {
	case startsWithKeywords(ctx, query, "DROP", "INDEX"):
		return "database.index.drop"
	case startsWithKeywords(ctx, query, "DROP", "CONSTRAINT"):
		return "database.constraint.drop"
	case isCreateConstraintStatement(ctx, query):
		return "database.constraint.create"
	case isOrdinarySchemaDDL(ctx, query):
		return "database.index.create"
	case startsWithKeywords(ctx, query, "SHOW", "INDEX") || startsWithKeywords(ctx, query, "SHOW", "INDEXES"):
		return "database.index.show"
	case startsWithKeywords(ctx, query, "SHOW", "CONSTRAINT") || startsWithKeywords(ctx, query, "SHOW", "CONSTRAINTS"):
		return "database.constraint.show"
	case isCreateProcedureCommand(query) || isDropProcedureCommand(query):
		return "database.schema.manage"
	}
	return ""
}

func fineGrainedDBMSStatementAction(ctx context.Context, query string) string {
	switch {
	case startsWithKeywords(ctx, query, "CREATE", "COMPOSITE DATABASE"):
		return "dbms.database.composite.create"
	case startsWithKeywords(ctx, query, "DROP", "COMPOSITE DATABASE"):
		return "dbms.database.composite.drop"
	case isCreateOrReplaceDatabaseQuery(ctx, query):
		return "dbms.database.replace"
	case startsWithKeywords(ctx, query, "CREATE", "DATABASE"):
		return "dbms.database.create"
	case startsWithKeywords(ctx, query, "DROP", "DATABASE"):
		return "dbms.database.drop"
	case startsWithKeywords(ctx, query, "ALTER", "DATABASE"):
		return "dbms.database.alter"
	case startsWithKeywords(ctx, query, "ALTER", "COMPOSITE DATABASE"):
		return "dbms.database.alter"
	case startsWithKeywords(ctx, query, "CREATE", "ALIAS"):
		return "dbms.alias.create"
	case startsWithKeywords(ctx, query, "DROP", "ALIAS"):
		return "dbms.alias.drop"
	case startsWithKeywords(ctx, query, "SHOW", "ALIASES"):
		return "dbms.alias.show"
	case startsWithKeywords(ctx, query, "SHOW", "USERS") && !isShowUserPrivileges(ctx, query):
		return "dbms.user.show"
	case startsWithKeywords(ctx, query, "SHOW", "SETTINGS"):
		return "dbms.settings.show"
	}
	return ""
}

func isFineGrainedDatabaseStatus(ctx context.Context, query string) bool {
	return auth.FineGrainedRequestFromContext(ctx) != nil &&
		(startsWithKeywords(ctx, query, "START", "DATABASE") || startsWithKeywords(ctx, query, "STOP", "DATABASE"))
}

// executeFineGrainedDatabaseStatus applies the scoped lifecycle privilege to
// the catalog's actual target, including aliases and offline databases.
func (e *StorageExecutor) executeFineGrainedDatabaseStatus(ctx context.Context, request *auth.FineGrainedRequest, query string) (*ExecuteResult, error) {
	cursor := fineGrainedPrivilegeCursor{text: query}
	cursor.skipGap()
	action, status := "database.stop", "offline"
	if cursor.takeKeyword("START") {
		action, status = "database.start", "online"
	} else {
		cursor.takeKeyword("STOP")
	}
	cursor.takeKeyword("DATABASE")
	name, ok := cursor.readName()
	if !ok {
		return nil, fineGrainedPrivilegeSyntaxError("expected a database name")
	}
	cursor.skipGap()
	wait := cursor.takeKeyword("WAIT")
	if !wait {
		cursor.takeKeyword("NOWAIT")
	}
	if cursor.at != len(query) {
		return nil, fineGrainedPrivilegeSyntaxError("unexpected input after database lifecycle command")
	}
	target := resolveGraphSelection(e.dbManager, name)
	if target.err != nil || target.remote || request.DatabaseID(target.target) == "" {
		return nil, newSemanticError("Neo.ClientError.Database.DatabaseNotFound", "DatabaseNotFound", "Unknown database")
	}
	if !request.AllowsDatabase(action, target.target, "") {
		return nil, auth.ErrFineGrainedForbidden
	}
	manager, ok := e.dbManager.(interface {
		SetDatabaseStatus(context.Context, string, string) error
	})
	if !ok {
		return nil, unsupportedAdministrationCommandError(ctx, query)
	}
	if err := manager.SetDatabaseStatus(ctx, target.target, status); err != nil {
		return nil, err
	}
	if wait {
		return &ExecuteResult{
			Columns: []string{"address", "state", "message", "success"},
			Rows:    [][]any{{"", "CaughtUp", "Database state changed", true}},
		}, nil
	}
	return &ExecuteResult{}, nil
}

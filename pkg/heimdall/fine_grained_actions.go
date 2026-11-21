package heimdall

import (
	"context"
	"slices"

	"github.com/orneryd/nornicdb/pkg/auth"
)

func fineGrainedActionDatabase(ctx context.Context, database string) (string, bool) {
	if scope := auth.RequestDatabaseScopeFromContext(ctx); scope != nil {
		return scope.Resolve(database)
	}
	if database == "" {
		if principal := auth.FineGrainedRequestFromContext(ctx); principal != nil {
			database = principal.HomeDatabaseName
		}
	}
	return database, database != ""
}

// ActionCatalogForRequest advertises only actions the principal can invoke on
// the selected database. Execution repeats the check on its final arguments.
func ActionCatalogForRequest(ctx context.Context, database string) map[string][]ActionFunc {
	catalog := ActionCatalog()
	principal := auth.FineGrainedRequestFromContext(ctx)
	if principal == nil {
		return catalog
	}
	database, selectable := fineGrainedActionDatabase(ctx, database)
	if !selectable || !principal.CanAccessDatabase(database) || !principal.AllowsDBMS("dbms.heimdall.actions.discover", "") {
		return nil
	}
	for category, actions := range catalog {
		actions = slices.DeleteFunc(actions, func(action ActionFunc) bool {
			return !principal.AllowsDatabase("heimdall.invoke."+action.Name, database, "")
		})
		if len(actions) == 0 {
			delete(catalog, category)
		} else {
			catalog[category] = actions
		}
	}
	return catalog
}

func fineGrainedMemoryTools(ctx context.Context, tools []MCPTool, database string) []MCPTool {
	principal := auth.FineGrainedRequestFromContext(ctx)
	if principal == nil {
		return tools
	}
	database, selectable := fineGrainedActionDatabase(ctx, database)
	if !selectable || !principal.CanAccessDatabase(database) || !principal.AllowsDBMS("dbms.heimdall.actions.discover", "") {
		return nil
	}
	allowed := make([]MCPTool, 0, len(tools))
	for _, tool := range tools {
		if principal.AllowsDatabase("mcp.invoke."+tool.Name, database, "") {
			allowed = append(allowed, tool)
		}
	}
	return allowed
}

func (h *Handler) callInMemoryTool(ctx context.Context, name string, args map[string]any, database string) (any, error) {
	if principal := auth.FineGrainedRequestFromContext(ctx); principal != nil {
		selection, _ := args["database"].(string)
		if selection == "" {
			selection, _ = args["db"].(string)
		}
		if selection == "" {
			selection = database
		}
		resolved, selectable := fineGrainedActionDatabase(ctx, selection)
		if !selectable || !principal.CanAccessDatabase(resolved) ||
			!principal.AllowsDatabase("mcp.invoke."+name, resolved, "") {
			return nil, auth.ErrFineGrainedForbidden
		}
		database = resolved
	}
	return h.inMemoryRunner.CallTool(ctx, name, args, database)
}

package cypher

import (
	"context"
	"sort"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// AdmitGraph is the one admission decision for the graph a request or
// session selects (a database, an alias or a composite constituent), which
// Bolt, HTTP, Heimdall, GraphQL and MCP each take first, before they check
// that the graph exists, open its storage, bind a connection or create an
// executor or service for it. It asks only the executing principal's
// resolver (PrincipalAccess.Allows with "access", whose graph access reads
// database metadata and nothing else). A selection the principal may not use
// gets Neo4j's Forbidden error naming the selection as given, whether or not
// it exists, so a refused principal can't tell an existing database from a
// missing one, and nothing is opened, cached or started for it. A nil
// resolver (no authentication) admits every selection.
func AdmitGraph(allows func(graph, permission string) bool, selection string) error {
	if allows == nil || allows(selection, "access") {
		return nil
	}
	return localizedStatusError("Neo.ClientError.Security.Forbidden", "Forbidden", localization.DatabaseAccessDenied(selection))
}

// PhysicalUserDatabases is the affected population of global worker/statistics
// operations. It reads database/namespace metadata without opening storage.
func PhysicalUserDatabases(manager DatabaseManagerInterface, namespaceSource any, defaultDatabase string) []string {
	seen := make(map[string]bool)
	if defaultDatabase != "" && defaultDatabase != "system" {
		seen[defaultDatabase] = true
	}
	if manager != nil {
		for _, info := range manager.ListDatabases() {
			if info.Name() != "system" && info.Type() != "composite" {
				seen[info.Name()] = true
			}
		}
	}
	if lister, ok := namespaceSource.(interface{ ListNamespaces() []string }); ok {
		for _, name := range lister.ListNamespaces() {
			if name != "" && name != "system" {
				seen[name] = true
			}
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// AdmitOperation decides a transport operation before its storage or services
// are resolved. It uses the same effect policy and principal resolver as Cypher.
func AdmitOperation(ctx context.Context, allows DatabasePermissionResolver, selection string, class PrivilegeClass) error {
	if err := AdmitGraph(allows, selection); err != nil {
		return err
	}
	ctx = WithDatabasePermissionResolver(ctx, selection, allows)
	return authorizeRequirements(ctx, requirementsForClass(class))
}

// AdmitStatement is the storage-free statement admission used by transports
// before constructing an executor. Execution and nested routing use this same
// decision again on the database each fragment actually selects.
func AdmitStatement(ctx context.Context, manager DatabaseManagerInterface, selection, query string) error {
	return admitSelectedDatabase(ctx, manager, selection, query, true)
}

// AdmitRequestOperation selects a database from a server-issued request scope
// and admits its effect before a transport resolves storage. An embedded caller
// without request authorization retains its explicit/default selection.
func AdmitRequestOperation(ctx context.Context, defaultDatabase, selection string, class PrivilegeClass) (context.Context, string, error) {
	database := selection
	if database == "" {
		database = defaultDatabase
	}
	allows := auth.RequestEntitlementResolverFromContext(ctx)
	scope := auth.RequestDatabaseScopeFromContext(ctx)
	if scope != nil {
		selected, ok := scope.Resolve(selection)
		if !ok {
			return ctx, "", AdmitGraph(func(string, string) bool { return false }, database)
		}
		database = selected
	}
	if allows == nil && scope == nil && auth.RequestDatabaseAccessModeFromContext(ctx) == nil {
		return ctx, database, AdmitOperation(ctx, nil, database, class)
	}
	if allows == nil || scope == nil {
		return ctx, "", AdmitGraph(func(string, string) bool { return false }, database)
	}
	resolver := func(database, permission string) bool { return allows(database, auth.Permission(permission)) }
	ctx = WithDatabasePermissionResolver(ctx, database, resolver)
	return ctx, database, AdmitOperation(ctx, resolver, database, class)
}

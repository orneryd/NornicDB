package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/auth"
)

// catalogVisibility binds the statement metadata without retaining its work
// context. The same typed view serves row functions and catalog commands.
func catalogVisibility(ctx context.Context) auth.CatalogVisibility {
	authorization, _ := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization)
	return auth.NewCatalogVisibility(auth.RequestDatabaseAccessModeFromContext(ctx), auth.RequestDatabaseScopeFromContext(ctx), authorization.resolver)
}

// catalogDatabaseVisible adapts Fabric's execution-context boundary to the same
// typed view used by catalog materialization and retained row functions.
func catalogDatabaseVisible(ctx context.Context, database string) bool {
	return catalogVisibility(ctx).CanSeeDatabase(database)
}

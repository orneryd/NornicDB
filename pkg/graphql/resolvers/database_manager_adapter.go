package resolvers

import (
	"context"

	"fmt"

	"github.com/orneryd/nornicdb/internal/catalogadapter"
	"github.com/orneryd/nornicdb/pkg/multidb"
)

// graphqlDatabaseManagerAdapter wraps multidb.DatabaseManager to satisfy
// cypher.DatabaseManagerInterface for GraphQL executor paths.
type graphqlDatabaseManagerAdapter struct {
	catalogadapter.Catalog
}

func (a *graphqlDatabaseManagerAdapter) CreateDatabase(ctx context.Context, request multidb.DatabaseCreateRequest) error {
	return a.Manager.CreateDatabase(ctx, request)
}
func (a *graphqlDatabaseManagerAdapter) DropDatabase(ctx context.Context, name string) error {
	return a.Manager.DropDatabase(ctx, name)
}
func (a *graphqlDatabaseManagerAdapter) Exists(name string) bool { return a.Manager.Exists(name) }
func (a *graphqlDatabaseManagerAdapter) CreateAlias(ctx context.Context, alias, databaseName string) error {
	return a.Manager.CreateAlias(ctx, alias, databaseName)
}
func (a *graphqlDatabaseManagerAdapter) DropAlias(ctx context.Context, alias string) error {
	return a.Manager.DropAlias(ctx, alias)
}
func (a *graphqlDatabaseManagerAdapter) ListAliases(databaseName string) map[string]string {
	return a.Manager.ListAliases(databaseName)
}
func (a *graphqlDatabaseManagerAdapter) ResolveDatabase(nameOrAlias string) (string, error) {
	return a.Manager.ResolveDatabase(nameOrAlias)
}
func (a *graphqlDatabaseManagerAdapter) SetDatabaseLimits(ctx context.Context, databaseName string, limits interface{}) error {
	limitsPtr, ok := limits.(*multidb.Limits)
	if !ok {
		return fmt.Errorf("invalid limits type")
	}
	return a.Manager.SetDatabaseLimits(ctx, databaseName, limitsPtr)
}
func (a *graphqlDatabaseManagerAdapter) GetDatabaseLimits(databaseName string) (interface{}, error) {
	return a.Manager.GetDatabaseLimits(databaseName)
}
func (a *graphqlDatabaseManagerAdapter) DropCompositeDatabase(ctx context.Context, name string) error {
	return a.Manager.DropCompositeDatabase(ctx, name)
}
func (a *graphqlDatabaseManagerAdapter) RemoveConstituent(ctx context.Context, compositeName string, alias string) error {
	return a.Manager.RemoveConstituent(ctx, compositeName, alias)
}
func (a *graphqlDatabaseManagerAdapter) IsCompositeDatabase(name string) bool {
	return a.Manager.IsCompositeDatabase(name)
}
func (a *graphqlDatabaseManagerAdapter) GetStorageForUse(name string, authToken string) (interface{}, error) {
	return a.Manager.GetStorageWithAuth(name, authToken)
}

func (a *graphqlDatabaseManagerAdapter) StatementCacheScope(name string) (*multidb.StatementCacheScope, error) {
	return a.Manager.StatementCacheScope(name)
}

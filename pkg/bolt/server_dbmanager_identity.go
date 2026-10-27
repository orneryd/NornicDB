package bolt

import (
	"context"

	"fmt"
	"time"

	"github.com/orneryd/nornicdb/internal/catalogadapter"
	"github.com/orneryd/nornicdb/pkg/multidb"
)

type boltDatabaseManagerAdapter struct {
	catalogadapter.Catalog
}

func (a *boltDatabaseManagerAdapter) CreateDatabase(ctx context.Context, request multidb.DatabaseCreateRequest) error {
	return a.Manager.CreateDatabase(ctx, request)
}
func (a *boltDatabaseManagerAdapter) DropDatabase(ctx context.Context, name string) error {
	return a.Manager.DropDatabase(ctx, name)
}
func (a *boltDatabaseManagerAdapter) Exists(name string) bool { return a.Manager.Exists(name) }
func (a *boltDatabaseManagerAdapter) CreateAlias(ctx context.Context, alias, databaseName string) error {
	return a.Manager.CreateAlias(ctx, alias, databaseName)
}
func (a *boltDatabaseManagerAdapter) DropAlias(ctx context.Context, alias string) error {
	return a.Manager.DropAlias(ctx, alias)
}
func (a *boltDatabaseManagerAdapter) ListAliases(databaseName string) map[string]string {
	return a.Manager.ListAliases(databaseName)
}
func (a *boltDatabaseManagerAdapter) ResolveDatabase(nameOrAlias string) (string, error) {
	return a.Manager.ResolveDatabase(nameOrAlias)
}
func (a *boltDatabaseManagerAdapter) SetDatabaseLimits(ctx context.Context, databaseName string, limits interface{}) error {
	limitsPtr, ok := limits.(*multidb.Limits)
	if !ok {
		return fmt.Errorf("invalid limits type")
	}
	return a.Manager.SetDatabaseLimits(ctx, databaseName, limitsPtr)
}
func (a *boltDatabaseManagerAdapter) GetDatabaseLimits(databaseName string) (interface{}, error) {
	return a.Manager.GetDatabaseLimits(databaseName)
}
func (a *boltDatabaseManagerAdapter) DropCompositeDatabase(ctx context.Context, name string) error {
	return a.Manager.DropCompositeDatabase(ctx, name)
}
func (a *boltDatabaseManagerAdapter) RemoveConstituent(ctx context.Context, compositeName string, alias string) error {
	return a.Manager.RemoveConstituent(ctx, compositeName, alias)
}
func (a *boltDatabaseManagerAdapter) IsCompositeDatabase(name string) bool {
	return a.Manager.IsCompositeDatabase(name)
}
func (a *boltDatabaseManagerAdapter) GetStorageForUse(name string, authToken string) (interface{}, error) {
	return a.Manager.GetStorageWithAuth(name, authToken)
}

func (a *boltDatabaseManagerAdapter) ServerIdentity() (string, time.Time) {
	return a.Manager.ServerIdentity()
}

func (a *boltDatabaseManagerAdapter) DatabaseIdentity(name string) (string, time.Time) {
	return a.Manager.DatabaseIdentity(name)
}

func (a *boltDatabaseManagerAdapter) SystemDatabaseName() string {
	return a.Manager.SystemDatabaseName()
}

func (a *boltDatabaseManagerAdapter) StatementCacheScope(name string) (*multidb.StatementCacheScope, error) {
	return a.Manager.StatementCacheScope(name)
}

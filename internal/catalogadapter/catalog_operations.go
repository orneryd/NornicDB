package catalogadapter

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

// Pointer receivers preserve the adapters' required interface method set.
// Protocol adapters retain their own post-success lifecycle overrides.

// CreateDatabase creates a database and delegates its catalog transition with
// the caller's context unchanged.
func (a *Catalog) CreateDatabase(ctx context.Context, request multidb.DatabaseCreateRequest) error {
	return a.Manager.CreateDatabase(ctx, request)
}

// DropDatabase performs the plain native database drop. Protocol-specific
// post-success lifecycle effects remain overrides in the owning package.
func (a *Catalog) DropDatabase(ctx context.Context, name string) error {
	return a.Manager.DropDatabase(ctx, name)
}

// Exists reports whether the catalog holds the database name.
func (a *Catalog) Exists(name string) bool { return a.Manager.Exists(name) }

// CreateAlias creates an alias for a database.
func (a *Catalog) CreateAlias(ctx context.Context, alias, databaseName string) error {
	return a.Manager.CreateAlias(ctx, alias, databaseName)
}

// DropAlias removes an alias.
func (a *Catalog) DropAlias(ctx context.Context, alias string) error {
	return a.Manager.DropAlias(ctx, alias)
}

// ListAliases returns the aliases of a database, or every alias when the
// database name is empty.
func (a *Catalog) ListAliases(databaseName string) map[string]string {
	return a.Manager.ListAliases(databaseName)
}

// ResolveDatabase resolves a database name or alias to its database name.
func (a *Catalog) ResolveDatabase(nameOrAlias string) (string, error) {
	return a.Manager.ResolveDatabase(nameOrAlias)
}

// SetDatabaseLimits is the canonical limits admission boundary.
//
// The limits type assertion runs FIRST, before any manager lookup, database
// resolution or context cancellation handling: an input that is not a
// *multidb.Limits is rejected with a fresh "invalid limits type" error and
// never reaches the manager. An accepted pointer, INCLUDING a typed nil
// (*multidb.Limits)(nil), is passed through unchanged with the caller's
// context, so the native manager keeps its own error and context precedence.
func (a *Catalog) SetDatabaseLimits(ctx context.Context, databaseName string, limits interface{}) error {
	limitsPtr, ok := limits.(*multidb.Limits)
	if !ok {
		return fmt.Errorf("invalid limits type")
	}
	return a.Manager.SetDatabaseLimits(ctx, databaseName, limitsPtr)
}

// GetDatabaseLimits returns the manager's limits value boxed in interface{}.
// The boxing, including a typed-nil *multidb.Limits, is native manager
// behavior and is preserved unchanged.
func (a *Catalog) GetDatabaseLimits(databaseName string) (interface{}, error) {
	return a.Manager.GetDatabaseLimits(databaseName)
}

// DropCompositeDatabase performs the plain native composite drop. Protocol
// specific post-success lifecycle effects remain overrides in the owning
// package.
func (a *Catalog) DropCompositeDatabase(ctx context.Context, name string) error {
	return a.Manager.DropCompositeDatabase(ctx, name)
}

// RemoveConstituent removes one constituent from a composite database.
func (a *Catalog) RemoveConstituent(ctx context.Context, compositeName string, alias string) error {
	return a.Manager.RemoveConstituent(ctx, compositeName, alias)
}

// IsCompositeDatabase reports whether the name is a composite database.
func (a *Catalog) IsCompositeDatabase(name string) bool {
	return a.Manager.IsCompositeDatabase(name)
}

// GetStorageForUse returns the storage engine a USE statement needs, with the
// caller's auth token forwarded to the manager's authenticated lookup.
func (a *Catalog) GetStorageForUse(name string, authToken string) (interface{}, error) {
	return a.Manager.GetStorageWithAuth(name, authToken)
}

package catalogadapter

import (
	"context"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

// The optional catalog capabilities are shared views over the same required
// Catalog owner. Each view is embedded by value, so a protocol adapter keeps its
// own named type, its size, and exactly one native manager pointer: the views
// add no allocation, callbacks, flags or cached state.
//
// Pointer receivers preserve the required contract's method sets: catalog
// values never implement the Cypher manager interface, and each capability is
// advertised only through the view that owns it.

// ExecutionCatalog is the execution-time optional view: database options and
// statement-cache scope. It is the capability set GraphQL executes with.
//
//	adapter := ExecutionCatalog{Catalog: Catalog{Manager: manager}}
//	options, err := adapter.DatabaseOptions("tenant")
type ExecutionCatalog struct {
	Catalog
}

// ForExecution returns the execution-time optional view over an existing native
// manager. It allocates nothing and keeps the single manager pointer.
func ForExecution(manager *multidb.DatabaseManager) ExecutionCatalog {
	return ExecutionCatalog{Catalog: Catalog{Manager: manager}}
}

// DatabaseOptions returns a database's options
// (cypher.DatabaseOptionsManager, #937).
func (a *ExecutionCatalog) DatabaseOptions(name string) (map[string]string, error) {
	return a.Manager.DatabaseOptions(name)
}

// SetDatabaseOption sets a database option, or removes it when value is empty.
func (a *ExecutionCatalog) SetDatabaseOption(ctx context.Context, name, key, value string) error {
	return a.Manager.SetDatabaseOption(ctx, name, key, value)
}

// StatementCacheScope returns the named database's statement-cache scope.
func (a *ExecutionCatalog) StatementCacheScope(name string) (*multidb.StatementCacheScope, error) {
	return a.Manager.StatementCacheScope(name)
}

// IdentifiedExecutionCatalog adds the persisted administrative identity view to
// the execution-time capabilities. HTTP and Bolt adapters use this view.
//
//	adapter := IdentifiedExecutionCatalog{ExecutionCatalog: ExecutionCatalog{Catalog: Catalog{Manager: manager}}}
//	serverID, createdAt := adapter.ServerIdentity()
type IdentifiedExecutionCatalog struct {
	ExecutionCatalog
}

// ForIdentifiedExecution returns the identified optional view over an existing
// native manager. It adds no allocation or state to the execution view.
func ForIdentifiedExecution(manager *multidb.DatabaseManager) IdentifiedExecutionCatalog {
	return IdentifiedExecutionCatalog{ExecutionCatalog: ForExecution(manager)}
}

// ServerIdentity returns the installation UUID and its creation time.
func (a *IdentifiedExecutionCatalog) ServerIdentity() (string, time.Time) {
	return a.Manager.ServerIdentity()
}

// DatabaseIdentity returns a database UUID and its actual creation time.
func (a *IdentifiedExecutionCatalog) DatabaseIdentity(name string) (string, time.Time) {
	return a.Manager.DatabaseIdentity(name)
}

// SystemDatabaseName returns the name of the system database.
func (a *IdentifiedExecutionCatalog) SystemDatabaseName() string {
	return a.Manager.SystemDatabaseName()
}

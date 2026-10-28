package bolt

import (
	"github.com/orneryd/nornicdb/internal/catalogadapter"
)

// boltDatabaseManagerAdapter satisfies cypher.DatabaseManagerInterface from the
// shared required contract in catalogadapter.Catalog (see
// catalogadapter/catalog_operations.go) and Bolt's optional capabilities from
// the shared catalogadapter.IdentifiedExecutionCatalog view (database options,
// statement-cache scope and persisted administrative identity).
// The required methods use pointer receivers, so only *boltDatabaseManagerAdapter,
// not the value type, implements that interface.
type boltDatabaseManagerAdapter struct {
	catalogadapter.IdentifiedExecutionCatalog
}

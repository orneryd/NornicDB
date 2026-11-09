package resolvers

import (
	"github.com/orneryd/nornicdb/internal/catalogadapter"
)

// graphqlDatabaseManagerAdapter wraps multidb.DatabaseManager to satisfy
// cypher.DatabaseManagerInterface for GraphQL executor paths. The required
// operations come from the shared contract in catalogadapter.Catalog (see
// catalogadapter/catalog_operations.go); GraphQL adds only the execution-time
// optional capabilities (database options and statement-cache scope) from the
// shared catalogadapter.ExecutionCatalog view. Persisted administrative identity
// and the system database name are deliberately not part of GraphQL's view.
//
// The required methods use pointer receivers, so only
// *graphqlDatabaseManagerAdapter, not the value type, implements that interface.
type graphqlDatabaseManagerAdapter struct {
	catalogadapter.ExecutionCatalog
}

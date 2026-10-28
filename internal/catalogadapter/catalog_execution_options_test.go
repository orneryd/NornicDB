package catalogadapter_test

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/catalogadapter"
	"github.com/orneryd/nornicdb/internal/testutil"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
)

// TestCatalogExecutionOptionContext carries the three protocol adapter option
// cases into the package that now owns the forwarded implementations. Each
// subcase keeps the original test function name, builds the shared catalog
// view its protocol embeds over a real manager, and runs the same canonical
// testutil.RequireCatalogOptionContext body, so no assertion was cloned,
// weakened or rewritten.
func TestCatalogExecutionOptionContext(t *testing.T) {
	cases := []struct {
		original string
		// HTTP and Bolt embed IdentifiedExecutionCatalog; GraphQL embeds the
		// execution-only view.
		view func(*multidb.DatabaseManager) cypher.DatabaseOptionsManager
	}{
		{
			original: "TestServerAdapterForwardsDatabaseOptions",
			view: func(manager *multidb.DatabaseManager) cypher.DatabaseOptionsManager {
				return &catalogadapter.IdentifiedExecutionCatalog{ExecutionCatalog: catalogadapter.ForExecution(manager)}
			},
		},
		{
			original: "TestBoltAdapterForwardsDatabaseOptions",
			view: func(manager *multidb.DatabaseManager) cypher.DatabaseOptionsManager {
				return &catalogadapter.IdentifiedExecutionCatalog{ExecutionCatalog: catalogadapter.ForExecution(manager)}
			},
		},
		{
			original: "TestGraphQLAdapterForwardsDatabaseOptions",
			view: func(manager *multidb.DatabaseManager) cypher.DatabaseOptionsManager {
				return &catalogadapter.ExecutionCatalog{Catalog: catalogadapter.Catalog{Manager: manager}}
			},
		},
	}
	for _, optionCase := range cases {
		t.Run(optionCase.original, func(t *testing.T) {
			testutil.RequireCatalogOptionContext(t, optionCase.view)
		})
	}
}

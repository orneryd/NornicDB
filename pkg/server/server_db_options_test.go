package server

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/catalogadapter"
	"github.com/orneryd/nornicdb/internal/testutil"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
)

func TestServerAdapterForwardsDatabaseOptions(t *testing.T) {
	testutil.RequireCatalogOptionContext(t, func(manager *multidb.DatabaseManager) cypher.DatabaseOptionsManager {
		return &databaseManagerAdapter{Catalog: catalogadapter.Catalog{Manager: manager}}
	})
}

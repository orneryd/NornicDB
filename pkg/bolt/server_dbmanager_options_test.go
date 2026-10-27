package bolt

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/catalogadapter"
	"github.com/orneryd/nornicdb/internal/testutil"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
)

func TestBoltAdapterForwardsDatabaseOptions(t *testing.T) {
	testutil.RequireCatalogOptionContext(t, func(manager *multidb.DatabaseManager) cypher.DatabaseOptionsManager {
		return &boltDatabaseManagerAdapter{Catalog: catalogadapter.Catalog{Manager: manager}}
	})
}

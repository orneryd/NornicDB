package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

func ei5CompositeManager(t *testing.T) (*multidb.DatabaseManager, *testDatabaseManagerAdapter) {
	return createCompositeTestManager(t, "pcomp", []multidb.ConstituentRef{
		{Alias: "other", DatabaseName: "pother", Type: "local", AccessMode: "read_write"},
		{Alias: "third", DatabaseName: "pthird", Type: "local", AccessMode: "read_write"},
	})
}

func ei5Churn(mgr *multidb.DatabaseManager, i int) error {
	name := fmt.Sprintf("churn%d", i)
	if err := mgr.CreateDatabase(context.Background(), multidb.DatabaseCreateRequest{Name: name}); err != nil {
		return err
	}
	return mgr.DropDatabase(context.Background(), name)
}

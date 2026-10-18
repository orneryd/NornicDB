package server

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Heimdall alias selection then implicit system routing must preserve the selection token.
func TestSolR3PublicAliasAdministrationGeneration(t *testing.T) {
	m, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.CreateDatabase("sol_script_host"); err != nil {
		t.Fatal(err)
	}
	if err := m.CreateAlias("sol_script_alias", "sol_script_host"); err != nil {
		t.Fatal(err)
	}
	router := newHeimdallDBRouter(nil, m, nil)
	defer router.Close()
	const body = "SHOW DATABASES"
	ctx := auth.WithRequestAuthenticationDisabled(context.Background())
	if _, err := router.Query(ctx, "sol_script_alias", body, nil); err != nil {
		t.Fatal(err)
	}
	owner, err := cypher.DatabaseStatementCaches(m, "sol_script_host")
	if err != nil {
		t.Fatal(err)
	}
	review1477AssertPublicVerdict(t, owner, body)
	if err := m.DropAlias("sol_script_alias"); err != nil {
		t.Fatal(err)
	}
	review1477AssertNoPublicVerdict(t, owner, body)
}

// A retained public executor must not regain cacheability through implicit
// system routing after its selected alias generation has been retired.
func TestSolR3RetiredAliasAdministrationRepopulation(t *testing.T) {
	m, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.CreateDatabase("sol_retired_host"); err != nil {
		t.Fatal(err)
	}
	if err := m.CreateAlias("sol_retired_alias", "sol_retired_host"); err != nil {
		t.Fatal(err)
	}
	engine, err := m.GetStorage("sol_retired_host")
	if err != nil {
		t.Fatal(err)
	}
	held := cypher.NewStorageExecutor(engine)
	if err := held.UseDatabaseStatementCaches(m, "sol_retired_alias"); err != nil {
		t.Fatal(err)
	}
	held.SetDatabaseManager(&databaseManagerAdapter{manager: m})
	owner := held.StatementCaches()
	if err := m.DropAlias("sol_retired_alias"); err != nil {
		t.Fatal(err)
	}
	if err := m.CreateAlias("sol_retired_alias", "sol_retired_host"); err != nil {
		t.Fatal(err)
	}
	if held.StatementCacheSelectionValid() {
		t.Fatal("old alias token revived")
	}
	if _, err := held.Execute(context.Background(), "SHOW DATABASES", nil); err != nil {
		t.Fatal(err)
	}
	review1477AssertNoPublicVerdict(t, owner, "SHOW DATABASES")
}

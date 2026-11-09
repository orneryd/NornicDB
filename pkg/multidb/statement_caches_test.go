package multidb

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

type testStatementCacheResource struct{ disposed bool }

func (c *testStatementCacheResource) Dispose()          { c.disposed = true }
func (c *testStatementCacheResource) InvalidateScopes() {}

func TestStatementCacheFamilyManagerLifecycle(t *testing.T) {
	manager, err := NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if err := manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "cache_target"}); err != nil {
		t.Fatal(err)
	}
	if err := manager.CreateAlias(t.Context(), "cache_alias", "cache_target"); err != nil {
		t.Fatal(err)
	}
	create := func(*StatementCacheScope) StatementCacheResource { return &testStatementCacheResource{} }
	owner, canonicalScope, err := manager.DatabaseStatementCaches("cache_target", create)
	if err != nil {
		t.Fatal(err)
	}
	alias, aliasScope, err := manager.DatabaseStatementCaches("cache_alias", create)
	if err != nil {
		t.Fatal(err)
	}
	if alias != owner || aliasScope == canonicalScope {
		t.Fatal("alias must share owner and have a distinct selection lifetime")
	}
	if _, _, err := manager.DatabaseStatementCaches("missing", create); err == nil {
		t.Fatal("missing database received owner")
	}
	if err := manager.DropDatabase(t.Context(), "missing"); err == nil {
		t.Fatal("missing drop succeeded")
	}
	if owner.(*testStatementCacheResource).disposed || !canonicalScope.Valid() || !aliasScope.Valid() {
		t.Fatal("failed drop retired live state")
	}
	if err := manager.DropAlias(t.Context(), "cache_alias"); err != nil {
		t.Fatal(err)
	}
	if owner.(*testStatementCacheResource).disposed || !canonicalScope.Valid() || aliasScope.Valid() || len(manager.statementCaches) != 1 {
		t.Fatal("alias drop retired its live canonical database")
	}
	if err := manager.CreateAlias(t.Context(), "cache_alias", "cache_target"); err != nil {
		t.Fatal(err)
	}
	next, nextScope, err := manager.DatabaseStatementCaches("cache_alias", create)
	if err != nil {
		t.Fatal(err)
	}
	if next != owner || nextScope == aliasScope || nextScope.Generation() == aliasScope.Generation() || !nextScope.Valid() {
		t.Fatal("recreated alias did not get a new selection on the surviving owner")
	}
	if err := manager.DropDatabase(t.Context(), "cache_target"); err != nil {
		t.Fatal(err)
	}
	if !owner.(*testStatementCacheResource).disposed || canonicalScope.Valid() || nextScope.Valid() || len(manager.statementCaches) != 0 || len(manager.statementCacheScopes) != 0 {
		t.Fatal("database drop retained owned state")
	}
	if err := manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "cache_target"}); err != nil {
		t.Fatal(err)
	}
	fresh, scope, err := manager.DatabaseStatementCaches("cache_target", create)
	if err != nil {
		t.Fatal(err)
	}
	if fresh == owner || scope == canonicalScope {
		t.Fatal("recreated database reused retired state")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if !fresh.(*testStatementCacheResource).disposed || scope.Valid() || len(manager.statementCaches) != 0 || len(manager.statementCacheScopes) != 0 {
		t.Fatal("Close retained owned state")
	}
	if _, _, err := manager.DatabaseStatementCaches("cache_target", create); err == nil {
		t.Fatal("closed cache host created a new resource")
	}
	manager.RegisterStatementCacheIndex(func() { t.Error("closed manager registered an index") })
	if len(manager.statementCacheIndexes) != 0 {
		t.Fatal("closed manager retained an index")
	}
}

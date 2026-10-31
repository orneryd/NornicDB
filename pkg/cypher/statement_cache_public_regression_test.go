package cypher

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

// Use only the exported construction/sharing/execution APIs to exercise cold,
// transitive and concurrent owners; Execute itself makes the USE/Fabric clones.
func TestReview1477R2PublicColdFamily(t *testing.T) {
	store := newTestMemoryEngine(t)
	manager, err := multidb.NewDatabaseManager(t.Context(), store, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "review_leaf"}); err != nil {
		t.Fatal(err)
	}
	if err := manager.CreateCompositeDatabase(t.Context(), "review_comp", []multidb.ConstituentRef{
		{Alias: "leaf", DatabaseName: "review_leaf", Type: "local", AccessMode: "read_write"},
	}); err != nil {
		t.Fatal(err)
	}
	root := NewStorageExecutor(store)
	root.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	var wg sync.WaitGroup
	errors := make(chan error, 12)
	for i := range 12 {
		wg.Go(func() {
			parent := NewStorageExecutor(store)
			parent.ShareStatementCachesFrom(root)
			child := NewStorageExecutor(store)
			child.ShareStatementCachesFrom(parent)
			child.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			for _, query := range []string{
				"USE review_leaf RETURN $v AS v",
				"CALL { USE review_comp.leaf RETURN $v AS v } RETURN v",
			} {
				result, err := child.Execute(context.Background(), query, map[string]interface{}{"v": int64(i)})
				if err != nil {
					errors <- fmt.Errorf("%s: %w", query, err)
					continue
				}
				if len(result.Rows) != 1 || result.Rows[0][0] != int64(i) {
					errors <- fmt.Errorf("request %d got %v", i, result.Rows)
				}
			}
			if child.StatementCaches() != root.StatementCaches() {
				errors <- fmt.Errorf("transitive owner changed")
			}
		})
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	if !root.hasCachedValidSyntax("RETURN $v AS v", syntaxStatementAccepted) {
		t.Error("public USE/Fabric children did not populate the root verdict")
	}
	owner := root.StatementCaches()
	owner.Dispose()
	child := NewStorageExecutor(store)
	child.ShareStatementCachesFrom(root)
	for i := range 20 {
		if _, err := child.Execute(context.Background(), fmt.Sprintf("RETURN %d AS retired", i), nil); err != nil {
			t.Fatal(err)
		}
	}
	assertStatementCachesEmpty(t, owner)
}

func TestReview1477R2LiveUseOwnerSurvivesRemoval(t *testing.T) {
	for _, removal := range []string{"database", "alias", "constituent"} {
		t.Run(removal, func(t *testing.T) {
			store := newTestMemoryEngine(t)
			manager, err := multidb.NewDatabaseManager(t.Context(), store, nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"review_live", "review_drop"} {
				if err := manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name}); err != nil {
					t.Fatal(err)
				}
			}
			if err := manager.CreateAlias(t.Context(), "review_alias", "review_drop"); err != nil {
				t.Fatal(err)
			}
			if err := manager.CreateCompositeDatabase(t.Context(), "review_comp", []multidb.ConstituentRef{
				{Alias: "gone", DatabaseName: "review_drop", Type: "local", AccessMode: "read_write"},
			}); err != nil {
				t.Fatal(err)
			}
			owner, err := DatabaseStatementCaches(manager, "nornic")
			if err != nil {
				t.Fatal(err)
			}
			engine, err := manager.GetStorage("nornic")
			if err != nil {
				t.Fatal(err)
			}
			executor := NewStorageExecutor(engine)
			executor.UseStatementCaches(owner)
			executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			const query = "USE review_live MATCH (n:LiveReview) RETURN count(n) AS c"
			if _, err := executor.Execute(context.Background(), query, nil); err != nil {
				t.Fatal(err)
			}
			const body = "MATCH (n:LiveReview) RETURN count(n) AS c"
			if !executor.hasCachedValidSyntax(body, syntaxStatementAccepted) {
				t.Fatal("live USE path never warmed")
			}
			switch removal {
			case "database":
				err = manager.DropDatabase(t.Context(), "review_drop")
			case "alias":
				err = manager.DropAlias(t.Context(), "review_alias")
			case "constituent":
				err = manager.RemoveConstituent(t.Context(), "review_comp", "gone")
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := executor.Execute(context.Background(), query, nil); err != nil {
				t.Fatal(err)
			}
			if owner.Disposed() || !executor.hasCachedValidSyntax(body, syntaxStatementAccepted) {
				t.Fatal("unrelated catalog removal permanently disabled caching for the surviving cross-database USE owner")
			}
		})
	}
}

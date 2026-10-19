package cypher

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

func TestStatementCacheFamilySurvivingUseOwner(t *testing.T) {
	for _, removal := range []string{"database", "alias", "constituent", "composite"} {
		t.Run(removal, func(t *testing.T) {
			store := newTestMemoryEngine(t)
			manager, err := multidb.NewDatabaseManager(store, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = manager.Close() })
			for _, name := range []string{"sol_live", "sol_removed"} {
				if err := manager.CreateDatabase(name); err != nil {
					t.Fatal(err)
				}
			}
			if err := manager.CreateAlias("sol_alias", "sol_removed"); err != nil {
				t.Fatal(err)
			}
			if err := manager.CreateCompositeDatabase("sol_comp", []multidb.ConstituentRef{
				{Alias: "leaf", DatabaseName: "sol_removed", Type: "local", AccessMode: "read_write"},
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
			const query = "USE sol_live MATCH (n:SolLive) RETURN count(n) AS c"
			const body = "MATCH (n:SolLive) RETURN count(n) AS c"
			if _, err := executor.Execute(context.Background(), query, nil); err != nil {
				t.Fatal(err)
			}
			if !executor.hasCachedValidSyntax(body, activeSyntaxValidator()) {
				t.Fatal("USE did not warm its shared verdict")
			}
			switch removal {
			case "database":
				err = manager.DropDatabase("sol_removed")
			case "alias":
				err = manager.DropAlias("sol_alias")
			case "constituent":
				err = manager.RemoveConstituent("sol_comp", "leaf")
			case "composite":
				err = manager.DropCompositeDatabase("sol_comp")
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := executor.Execute(context.Background(), query, nil); err != nil {
				t.Fatal(err)
			}
			if owner.Disposed() || !executor.hasCachedValidSyntax(body, activeSyntaxValidator()) {
				t.Fatal("unrelated removal permanently retired the surviving USE owner's caches")
			}
		})
	}
}

func TestStatementCacheFamilyPublicColdAndRetiredOwner(t *testing.T) {
	store := newTestMemoryEngine(t)
	manager, err := multidb.NewDatabaseManager(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	if err := manager.CreateDatabase("sol_leaf"); err != nil {
		t.Fatal(err)
	}
	if err := manager.CreateCompositeDatabase("sol_comp", []multidb.ConstituentRef{
		{Alias: "leaf", DatabaseName: "sol_leaf", Type: "local", AccessMode: "read_write"},
	}); err != nil {
		t.Fatal(err)
	}
	owner, err := DatabaseStatementCaches(manager, "nornic")
	if err != nil {
		t.Fatal(err)
	}
	root := NewStorageExecutor(store)
	root.UseStatementCaches(owner)
	var wg sync.WaitGroup
	failures := make(chan error, 24)
	for i := range 12 {
		wg.Go(func() {
			child := NewStorageExecutor(store)
			child.UseStatementCaches(root.StatementCaches())
			child.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			for _, query := range []string{"USE sol_leaf RETURN $v AS v", "CALL { USE sol_comp.leaf RETURN $v AS v } RETURN v"} {
				result, err := child.Execute(context.Background(), query, map[string]interface{}{"v": int64(i)})
				if err != nil {
					failures <- err
					continue
				}
				if len(result.Rows) != 1 || result.Rows[0][0] != int64(i) {
					failures <- fmt.Errorf("request %d got %v", i, result.Rows)
				}
			}
		})
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if !root.hasCachedValidSyntax("RETURN $v AS v", activeSyntaxValidator()) {
		t.Fatal("public USE/Fabric clones lost the cold root owner")
	}
	removedOwner, err := DatabaseStatementCaches(manager, "sol_leaf")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.DropDatabase("sol_leaf"); err != nil {
		t.Fatal(err)
	}
	if owner.Disposed() {
		t.Fatal("drop retired the surviving nornic owner")
	}
	// Public Execute calls on retained descendants must not repopulate the retired owner.
	for range 20 {
		child := NewStorageExecutor(store)
		child.UseStatementCaches(removedOwner)
		if _, err := child.Execute(context.Background(), "RETURN 73 AS retired", nil); err != nil {
			t.Fatal(err)
		}
	}
	assertStatementCachesEmpty(t, removedOwner)
}

func TestStatementCacheFamilyPreciseReferenceInvalidation(t *testing.T) {
	for _, removal := range []string{"database", "alias", "constituent", "composite"} {
		t.Run(removal, func(t *testing.T) {
			store := newTestMemoryEngine(t)
			manager, err := multidb.NewDatabaseManager(store, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer manager.Close()
			for _, name := range []string{"cache_live", "cache_removed"} {
				if err := manager.CreateDatabase(name); err != nil {
					t.Fatal(err)
				}
			}
			if err := manager.CreateAlias("cache_alias", "cache_removed"); err != nil {
				t.Fatal(err)
			}
			constituent := multidb.ConstituentRef{Alias: "leaf", DatabaseName: "cache_removed", Type: "local", AccessMode: "read_write"}
			if err := manager.CreateCompositeDatabase("cache_comp", []multidb.ConstituentRef{constituent}); err != nil {
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
			root := NewStorageExecutor(engine)
			// Exercise statement validation again after recreation instead of
			// serving an unchanged result from the independent query cache.
			root.cache = nil
			root.UseStatementCaches(owner)
			root.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			const liveBody = "RETURN 91 AS live"
			const removedBody = "RETURN 73 AS removed"
			if _, err := root.Execute(context.Background(), "USE cache_live "+liveBody, nil); err != nil {
				t.Fatal(err)
			}
			selection := "cache_removed"
			if removal == "alias" {
				selection = "cache_alias"
			}
			if removal == "constituent" || removal == "composite" {
				selection = "cache_comp.leaf"
			}
			retained, _, err := root.scopedExecutorForUse(selection, "")
			if err != nil {
				t.Fatal(err)
			}
			query := "USE " + selection + " " + removedBody
			if _, err := root.Execute(context.Background(), query, nil); err != nil {
				t.Fatal(err)
			}
			if !root.hasCachedValidSyntax(removedBody, activeSyntaxValidator()) {
				t.Fatal("routed body was not cached")
			}
			// Full text/explain and uppercase routing entries reference the same selection.
			if err := root.validateSyntax(context.Background(), query, false); err != nil {
				t.Fatal(err)
			}
			root.cachedUpperQuery(query)
			switch removal {
			case "database":
				err = manager.DropDatabase("cache_removed")
			case "alias":
				err = manager.DropAlias("cache_alias")
			case "constituent":
				err = manager.RemoveConstituent("cache_comp", "leaf")
			case "composite":
				err = manager.DropCompositeDatabase("cache_comp")
			}
			if err != nil {
				t.Fatal(err)
			}
			if owner.Disposed() {
				t.Fatal("removed reference retired the live nornic owner")
			}
			// Check before executing again: an unrelated warm entry must survive.
			if !root.hasCachedValidSyntax(liveBody, activeSyntaxValidator()) {
				t.Fatal("unrelated warm verdict was cleared")
			}
			if root.hasCachedValidSyntax(removedBody, activeSyntaxValidator()) || root.hasCachedValidSyntax(query, activeSyntaxValidator()) {
				t.Fatal("removed reference retained exact/canonical verdicts")
			}
			owner.upper.mu.RLock()
			_, upperRetained := owner.upper.cache[query]
			owner.upper.mu.RUnlock()
			if upperRetained {
				t.Fatal("removed reference retained uppercase routing entry")
			}
			if _, err := retained.Execute(context.Background(), removedBody, nil); err != nil {
				t.Fatal(err)
			}
			if root.hasCachedValidSyntax(removedBody, activeSyntaxValidator()) {
				t.Fatal("retained removed selection repopulated live owner's cache")
			}
			switch removal {
			case "database":
				err = manager.CreateDatabase("cache_removed")
			case "alias":
				err = manager.CreateAlias("cache_alias", "cache_removed")
			case "constituent":
				err = manager.AddConstituent("cache_comp", constituent)
			case "composite":
				err = manager.CreateCompositeDatabase("cache_comp", []multidb.ConstituentRef{constituent})
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := root.Execute(context.Background(), query, nil); err != nil {
				t.Fatal(err)
			}
			if !root.hasCachedValidSyntax(removedBody, activeSyntaxValidator()) {
				t.Fatal("recreated selection could not cache again")
			}
			if _, err := retained.Execute(context.Background(), removedBody, nil); err != nil {
				t.Fatal(err)
			}
			if !root.hasCachedValidSyntax(removedBody, activeSyntaxValidator()) {
				t.Fatal("old selection damaged recreated selection's verdict")
			}
		})
	}
}

func TestStatementCacheFamilyDynamicFabricReference(t *testing.T) {
	store := newTestMemoryEngine(t)
	manager, err := multidb.NewDatabaseManager(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if err := manager.CreateDatabase("cache_leaf"); err != nil {
		t.Fatal(err)
	}
	ref := multidb.ConstituentRef{Alias: "leaf", DatabaseName: "cache_leaf", Type: "local", AccessMode: "read_write"}
	if err := manager.CreateCompositeDatabase("cache_dynamic", []multidb.ConstituentRef{ref}); err != nil {
		t.Fatal(err)
	}
	owner, err := DatabaseStatementCaches(manager, "cache_dynamic")
	if err != nil {
		t.Fatal(err)
	}
	engine, err := manager.GetStorage("cache_dynamic")
	if err != nil {
		t.Fatal(err)
	}
	root := NewStorageExecutor(engine)
	root.UseStatementCaches(owner)
	root.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	const query = "CALL { USE graph.byName($graph) RETURN 81 AS removed } RETURN removed"
	const body = "RETURN 81 AS removed"
	if _, err := root.Execute(context.Background(), query, map[string]interface{}{"graph": "cache_dynamic.leaf"}); err != nil {
		t.Fatal(err)
	}
	if !root.hasCachedValidSyntax(body, activeSyntaxValidator()) {
		t.Fatal("dynamic fragment did not share verdict")
	}
	// Dynamic dispatch records the outer text even before its grammar/log verdict.
	shard := &owner.syntax.shards[boundedCacheStringShardIndex(query)]
	shard.mu.RLock()
	_, recorded := shard.cache[query]
	shard.mu.RUnlock()
	if !recorded {
		t.Fatal("dynamic outer text lost its resolved dependency")
	}
	root.cachedUpperQuery(query)
	if err := manager.RemoveConstituent("cache_dynamic", "leaf"); err != nil {
		t.Fatal(err)
	}
	if owner.Disposed() || root.hasCachedValidSyntax(body, activeSyntaxValidator()) {
		t.Fatal("dynamic selection invalidation retired owner or retained body")
	}
	shard.mu.RLock()
	_, retained := shard.cache[query]
	shard.mu.RUnlock()
	owner.upper.mu.RLock()
	_, upperRetained := owner.upper.cache[query]
	owner.upper.mu.RUnlock()
	if retained || upperRetained {
		t.Fatal("dynamic outer text retained a removed dependency")
	}
	if err := manager.AddConstituent("cache_dynamic", ref); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Execute(context.Background(), query, map[string]interface{}{"graph": "cache_dynamic.leaf"}); err != nil {
		t.Fatal(err)
	}
	if !root.hasCachedValidSyntax(body, activeSyntaxValidator()) {
		t.Fatal("surviving composite could not cache again")
	}
}

func TestStatementCacheFamilyConcurrentReferenceRemoval(t *testing.T) {
	store := newTestMemoryEngine(t)
	manager, err := multidb.NewDatabaseManager(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if err := manager.CreateAlias("cache_concurrent_alias", "nornic"); err != nil {
		t.Fatal(err)
	}
	root := NewStorageExecutor(store)
	if err := root.UseDatabaseStatementCaches(manager, "nornic"); err != nil {
		t.Fatal(err)
	}
	root.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	selected, _, err := root.scopedExecutorForUse("cache_concurrent_alias", "")
	if err != nil {
		t.Fatal(err)
	}
	const live = "RETURN 31 AS live"
	const removed = "RETURN 47 AS removed"
	if _, err := root.Execute(context.Background(), live, nil); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errors := make(chan error, 8)
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			<-start
			for range 20 {
				if _, err := selected.Execute(context.Background(), removed, nil); err != nil {
					errors <- err
					return
				}
				if _, err := root.Execute(context.Background(), live, nil); err != nil {
					errors <- err
					return
				}
			}
		})
	}
	close(start)
	if err := manager.DropAlias("cache_concurrent_alias"); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	if root.StatementCaches().Disposed() || !root.hasCachedValidSyntax(live, activeSyntaxValidator()) {
		t.Fatal("concurrent removal disabled live caching")
	}
	if root.hasCachedValidSyntax(removed, activeSyntaxValidator()) {
		t.Fatal("retired selection repopulated a dependent entry")
	}
}

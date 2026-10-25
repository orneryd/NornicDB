package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

// Test-only attack overlay. The production candidate is frozen separately.
func TestRule73SelectionForms(t *testing.T) {
	for _, form := range []string{"canonical", "alias", "self_alias", "constituent", "colon_use", "explain", "profile", "call_use", "dynamic", "composite_root"} {
		t.Run(form, func(t *testing.T) {
			store := newTestMemoryEngine(t)
			manager, err := multidb.NewDatabaseManager(store, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer manager.Close()
			if err := manager.CreateDatabase("gate_leaf"); err != nil {
				t.Fatal(err)
			}
			aliasTarget := "gate_leaf"
			if form == "self_alias" {
				aliasTarget = "nornic"
			}
			if err := manager.CreateAlias("gate_alias", aliasTarget); err != nil {
				t.Fatal(err)
			}
			ref := multidb.ConstituentRef{Alias: "leaf", DatabaseName: "gate_leaf", Type: "local", AccessMode: "read_write"}
			if err := manager.CreateCompositeDatabase("gate_comp", []multidb.ConstituentRef{ref}); err != nil {
				t.Fatal(err)
			}
			rootName := "nornic"
			if form == "dynamic" {
				rootName = "gate_comp"
			}
			engine, err := manager.GetStorage(rootName)
			if err != nil {
				t.Fatal(err)
			}
			root := NewStorageExecutor(engine, QueryCachePolicy{MaxEntries: 0, TTL: 0})
			if err := root.UseDatabaseStatementCaches(manager, rootName); err != nil {
				t.Fatal(err)
			}
			root.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			const body = "MATCH (n:GateMatrix) RETURN count(n) AS c"
			selection := "gate_leaf"
			query := "USE gate_leaf " + body
			remove := func() error { return manager.DropDatabase("gate_leaf") }
			recreate := func() error { return manager.CreateDatabase("gate_leaf") }
			switch form {
			case "alias", "self_alias":
				selection = "gate_alias"
				query = "USE gate_alias " + body
				remove = func() error { return manager.DropAlias("gate_alias") }
				recreate = func() error { return manager.CreateAlias("gate_alias", aliasTarget) }
			case "constituent":
				selection = "gate_comp.leaf"
				query = "USE gate_comp.leaf " + body
				remove = func() error { return manager.RemoveConstituent("gate_comp", "leaf") }
				recreate = func() error { return manager.AddConstituent("gate_comp", ref) }
			case "colon_use":
				query = ":USE gate_leaf\n" + body
			case "explain":
				query = "EXPLAIN USE gate_leaf " + body
			case "profile":
				query = "PROFILE USE gate_leaf " + body
			case "call_use":
				selection = "gate_comp.leaf"
				query = "CALL { USE gate_comp.leaf " + body + " } RETURN c"
				remove = func() error { return manager.RemoveConstituent("gate_comp", "leaf") }
				recreate = func() error { return manager.AddConstituent("gate_comp", ref) }
			case "dynamic":
				selection = "gate_comp.leaf"
				query = "CALL { USE graph.byName($graph) " + body + " } RETURN c"
				remove = func() error { return manager.RemoveConstituent("gate_comp", "leaf") }
				recreate = func() error { return manager.AddConstituent("gate_comp", ref) }
			case "composite_root":
				selection = "gate_comp"
				query = "USE gate_comp RETURN 73 AS c"
				remove = func() error { return manager.DropCompositeDatabase("gate_comp") }
				recreate = func() error { return manager.CreateCompositeDatabase("gate_comp", []multidb.ConstituentRef{ref}) }
			}
			params := map[string]interface{}{"graph": "gate_comp.leaf"}
			if _, err := root.Execute(context.Background(), query, params); err != nil {
				t.Fatal(err)
			}
			retained, _, err := root.scopedExecutorForUse(selection, "")
			if err != nil {
				t.Fatal(err)
			}
			// Warm only text accepted through the real public query first. Register a
			// body-independent full text too, to attack exact/canonical/upper metadata.
			if form != "colon_use" {
				if err := root.validateSyntax(context.Background(), query, false); err != nil {
					t.Fatal(err)
				}
				root.cachedUpperQuery(query)
			}
			if _, err := root.Execute(context.Background(), "RETURN 17 AS live", nil); err != nil {
				t.Fatal(err)
			}
			if err := remove(); err != nil {
				t.Fatal(err)
			}
			if root.StatementCaches().Disposed() || !root.hasCachedValidSyntax("RETURN 17 AS live", activeSyntaxValidator()) {
				t.Fatal("live owner/verdict retired")
			}
			if form != "colon_use" && root.hasCachedValidSyntax(query, activeSyntaxValidator()) {
				t.Fatal("full verdict survived its removed selection")
			}
			if _, err := retained.Execute(context.Background(), "RETURN 59 AS retained", nil); err != nil {
				t.Fatal(err)
			}
			if root.hasCachedValidSyntax("RETURN 59 AS retained", activeSyntaxValidator()) {
				t.Fatal("removed view repopulated shared cache")
			}
			if err := recreate(); err != nil {
				t.Fatal(err)
			}
			if _, err := root.Execute(context.Background(), query, params); err != nil {
				t.Fatal(err)
			}
			if root.StatementCaches().Disposed() {
				t.Fatal("recreation poisoned owner")
			}
		})
	}
}

func TestRule73ReferenceCycles64(t *testing.T) {
	for _, kind := range []string{"database", "alias", "constituent", "composite"} {
		t.Run(kind, func(t *testing.T) {
			store := newTestMemoryEngine(t)
			manager, err := multidb.NewDatabaseManager(store, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer manager.Close()
			if err := manager.CreateDatabase("gate_cycle_leaf"); err != nil {
				t.Fatal(err)
			}
			if err := manager.CreateCompositeDatabase("gate_cycle_comp", nil); err != nil {
				t.Fatal(err)
			}
			engine, err := manager.GetStorage("nornic")
			if err != nil {
				t.Fatal(err)
			}
			root := NewStorageExecutor(engine, QueryCachePolicy{MaxEntries: 0, TTL: 0})
			if err := root.UseDatabaseStatementCaches(manager, "nornic"); err != nil {
				t.Fatal(err)
			}
			root.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			ref := multidb.ConstituentRef{Alias: "leaf", DatabaseName: "gate_cycle_leaf", Type: "local", AccessMode: "read_write"}
			for i := range 64 {
				selection := fmt.Sprintf("gate_cycle_%d", i)
				switch kind {
				case "database":
					err = manager.CreateDatabase(selection)
				case "alias":
					err = manager.CreateAlias(selection, "gate_cycle_leaf")
				case "constituent":
					ref.Alias = selection
					err = manager.AddConstituent("gate_cycle_comp", ref)
					selection = "gate_cycle_comp." + selection
				case "composite":
					err = manager.CreateCompositeDatabase(selection, []multidb.ConstituentRef{ref})
				}
				if err != nil {
					t.Fatal(err)
				}
				query := "USE " + selection + fmt.Sprintf(" RETURN %d AS cycle_value", i)
				if _, err := root.Execute(context.Background(), query, nil); err != nil {
					t.Fatal(err)
				}
				root.cachedUpperQuery(query)
				switch kind {
				case "database":
					err = manager.DropDatabase(selection)
				case "alias":
					err = manager.DropAlias(selection)
				case "constituent":
					err = manager.RemoveConstituent("gate_cycle_comp", ref.Alias)
				case "composite":
					err = manager.DropCompositeDatabase(selection)
				}
				if err != nil {
					t.Fatal(err)
				}
				if root.hasCachedValidSyntax(query, activeSyntaxValidator()) {
					t.Fatal("removed full verdict retained")
				}
				if root.StatementCaches().Disposed() {
					t.Fatal("cycle retired live owner")
				}
			}
			// No statement in this owner references a surviving selection. Cache
			// metadata must leave with its entries, not accumulate retired tokens.
			for i := range root.StatementCaches().syntax.shards {
				sh := &root.StatementCaches().syntax.shards[i]
				sh.mu.RLock()
				n := len(sh.dependencies)
				sh.mu.RUnlock()
				if n != 0 {
					t.Fatalf("syntax dependency leak: %d", n)
				}
			}
			root.StatementCaches().upper.mu.RLock()
			n := len(root.StatementCaches().upper.dependencies)
			root.StatementCaches().upper.mu.RUnlock()
			if n != 0 {
				t.Fatalf("uppercase dependency leak: %d", n)
			}
		})
	}
}

func TestRule73FabricResultCacheRetarget(t *testing.T) {
	store := newTestMemoryEngine(t)
	manager, err := multidb.NewDatabaseManager(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	for _, name := range []string{"gate_left", "gate_right"} {
		if err := manager.CreateDatabase(name); err != nil {
			t.Fatal(err)
		}
		engine, err := manager.GetStorage(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := engine.CreateNode(&storage.Node{ID: "same_id", Labels: []string{"GateValue"}, Properties: map[string]interface{}{"v": name}}); err != nil {
			t.Fatal(err)
		}
	}
	ref := multidb.ConstituentRef{Alias: "leaf", DatabaseName: "gate_left", Type: "local", AccessMode: "read_write"}
	if err := manager.CreateCompositeDatabase("gate_results", []multidb.ConstituentRef{ref}); err != nil {
		t.Fatal(err)
	}
	engine, err := manager.GetStorage("nornic")
	if err != nil {
		t.Fatal(err)
	}
	root := NewStorageExecutor(engine)
	if err := root.UseDatabaseStatementCaches(manager, "nornic"); err != nil {
		t.Fatal(err)
	}
	root.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	const query = "CALL { USE gate_results.leaf MATCH (n:GateValue) RETURN n.v AS v } RETURN v"
	run := func(want string) {
		t.Helper()
		result, err := root.Execute(context.Background(), query, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Rows) != 1 || result.Rows[0][0] != want {
			t.Fatalf("got %v, want %s", result.Rows, want)
		}
	}
	run("gate_left")
	run("gate_left")
	if err := manager.RemoveConstituent("gate_results", "leaf"); err != nil {
		t.Fatal(err)
	}
	ref.DatabaseName = "gate_right"
	if err := manager.AddConstituent("gate_results", ref); err != nil {
		t.Fatal(err)
	}
	run("gate_right")
}

func TestRule73CatalogAddPreservesWarmOwner(t *testing.T) {
	store := newTestMemoryEngine(t)
	manager, err := multidb.NewDatabaseManager(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	root := NewStorageExecutor(store)
	if err := root.UseDatabaseStatementCaches(manager, "nornic"); err != nil {
		t.Fatal(err)
	}
	root.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	const query = "RETURN 23 AS existing"
	if _, err := root.Execute(context.Background(), query, nil); err != nil {
		t.Fatal(err)
	}
	events := []func() error{func() error { return manager.CreateDatabase("gate_added") }, func() error { return manager.CreateAlias("gate_added_alias", "gate_added") }, func() error { return manager.CreateCompositeDatabase("gate_added_comp", nil) }, func() error {
		return manager.AddConstituent("gate_added_comp", multidb.ConstituentRef{Alias: "leaf", DatabaseName: "gate_added", Type: "local", AccessMode: "read_write"})
	}}
	for _, event := range events {
		if err := event(); err != nil {
			t.Fatal(err)
		}
		if root.StatementCaches().Disposed() || !root.hasCachedValidSyntax(query, activeSyntaxValidator()) {
			t.Fatal("add event discarded existing warm owner/verdict")
		}
	}
}

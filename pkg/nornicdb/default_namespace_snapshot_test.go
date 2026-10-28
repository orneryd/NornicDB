package nornicdb_test

import (
	"context"
	cfgpkg "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenResolvedNamespace(t *testing.T) {
	for _, source := range []string{"explicit", "yaml", "env", "yaml_env_override", "explicit_snapshot", "empty_default", "nil_default"} {
		for _, persistent := range []bool{false, true} {
			mode := "memory"
			if persistent {
				mode = "persistent"
			}
			t.Run(source+"/"+mode, func(t *testing.T) {
				t.Setenv("NORNICDB_DEFAULT_DATABASE", "")
				t.Setenv("NEO4J_dbms_default__database", "")
				cfg := nornicdb.DefaultConfig()
				want := "tenant_fixture"
				switch source {
				case "explicit":
					cfg.Database.DefaultDatabase = want
				case "yaml", "yaml_env_override":
					if source == "yaml_env_override" {
						want = "environment_fixture"
						t.Setenv("NORNICDB_DEFAULT_DATABASE", want)
					}
					file := filepath.Join(t.TempDir(), "config.yaml")
					if err := os.WriteFile(file, []byte("database:\n  default_database: tenant_fixture\n"), 0600); err != nil {
						t.Fatal(err)
					}
					var err error
					cfg, err = cfgpkg.LoadFromFile(file)
					if err != nil {
						t.Fatal(err)
					}
				case "env":
					t.Setenv("NORNICDB_DEFAULT_DATABASE", want)
					cfg = cfgpkg.LoadFromEnv()
				case "explicit_snapshot":
					cfg.Database.DefaultDatabase = want
					t.Setenv("NORNICDB_DEFAULT_DATABASE", "other_environment")
				case "empty_default":
					cfg.Database.DefaultDatabase = ""
					want = "nornic"
					t.Setenv("NORNICDB_DEFAULT_DATABASE", "other_environment")
				case "nil_default":
					cfg = nil
					want = "nornic"
					t.Setenv("NORNICDB_DEFAULT_DATABASE", "other_environment")
				}
				if cfg != nil {
					if cfg.Database.DefaultDatabase != "" && cfg.Database.DefaultDatabase != want {
						t.Fatalf("resolved config=%q want %q", cfg.Database.DefaultDatabase, want)
					}
					cfg.Memory.AutoLinksEnabled = false
					cfg.Memory.DecayEnabled = false
					cfg.Database.WALAutoCompactionEnabled = false
				}
				dir := ""
				if persistent {
					dir = t.TempDir()
				}
				db, err := nornicdb.Open(dir, cfg)
				if err != nil {
					t.Fatal(err)
				}
				opened := db
				t.Cleanup(func() { _ = opened.Close() })
				st, ok := db.GetStorage().(*storage.NamespacedEngine)
				if !ok {
					t.Fatalf("storage=%T", db.GetStorage())
				}
				if got := st.Namespace(); got != want {
					t.Errorf("Open ignored resolved namespace: got=%q want=%q", got, want)
				}
				if _, err := db.ExecuteCypher(context.Background(), "CREATE (:NamespaceFixture {value: 1})", nil); err != nil {
					t.Fatal(err)
				}
				nodes, err := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), want).GetNodesByLabel("NamespaceFixture")
				if err != nil {
					t.Fatal(err)
				}
				if len(nodes) != 1 {
					t.Errorf("configured namespace public write visibility: nodes=%d want1", len(nodes))
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				if persistent {
					reopened, err := nornicdb.Open(dir, cfg)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = reopened.Close() })
					if got := reopened.GetStorage().(*storage.NamespacedEngine).Namespace(); got != want {
						t.Errorf("reopen namespace=%q want=%q", got, want)
					}
					nodes, err := storage.NewNamespacedEngine(reopened.GetBaseStorageForManager(), want).GetNodesByLabel("NamespaceFixture")
					if err != nil {
						t.Fatal(err)
					}
					if len(nodes) != 1 {
						t.Errorf("reopen configured namespace nodes=%d want1", len(nodes))
					}
				}
			})
		}
	}
}

func TestExistingNamespaceSelectionDoesNotMigrate(t *testing.T) {
	t.Setenv("NORNICDB_DEFAULT_DATABASE", "")
	t.Setenv("NEO4J_dbms_default__database", "")
	dir := t.TempDir()
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.AutoLinksEnabled = false
	cfg.Database.WALAutoCompactionEnabled = false
	db, err := nornicdb.Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecuteCypher(context.Background(), "CREATE (:NamespaceFixture {value: 1})", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Database.DefaultDatabase = "tenant_fixture"
	db, err = nornicdb.Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := db.GetStorage().(*storage.NamespacedEngine).Namespace(); got != "tenant_fixture" {
		t.Errorf("new default=%q", got)
	}
	for _, test := range []struct {
		name string
		want int
	}{{"nornic", 1}, {"tenant_fixture", 0}} {
		nodes, err := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), test.name).AllNodes()
		if err != nil {
			t.Fatal(err)
		}
		if len(nodes) != test.want {
			t.Errorf("namespace %q nodes=%d want%d", test.name, len(nodes), test.want)
		}
	}
}

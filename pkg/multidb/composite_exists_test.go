package multidb

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestGetStorageWithAuth_ConstituentDotted_Local(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	mgr, err := NewDatabaseManager(t.Context(), base, nil)
	if err != nil {
		t.Fatalf("NewDatabaseManager failed: %v", err)
	}
	fixtureowner.Own(t, mgr)

	seedCompositeTestCatalog(t, mgr, []string{"shard_local"}, "cmp", []ConstituentRef{
		{Alias: "a", DatabaseName: "shard_local", Type: "local", AccessMode: "read_write"},
	})

	engine, err := mgr.GetStorageWithAuth("cmp.a", "Bearer token")
	if err != nil {
		t.Fatalf("GetStorageWithAuth for dotted constituent failed: %v", err)
	}
	if engine == nil {
		t.Fatal("expected non-nil engine for dotted constituent")
	}
}

func TestGetStorageWithAuth_ConstituentDotted_RemoteFactory(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	var gotAuth string

	mgr, err := NewDatabaseManager(t.Context(), base, &Config{
		RemoteEngineFactory: func(_ ConstituentRef, authToken string) (storage.Engine, error) {
			gotAuth = authToken
			return fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil
		},
	})
	if err != nil {
		t.Fatalf("NewDatabaseManager failed: %v", err)
	}
	fixtureowner.Own(t, mgr)

	if err := mgr.CreateCompositeDatabase(t.Context(), "cmp_remote", []ConstituentRef{
		{Alias: "r", DatabaseName: "tenant_r", Type: "remote", AccessMode: "read_write", URI: "http://remote"},
	}); err != nil {
		t.Fatalf("CreateCompositeDatabase failed: %v", err)
	}

	engine, err := mgr.GetStorageWithAuth("cmp_remote.r", "Bearer forwarded")
	if err != nil {
		t.Fatalf("GetStorageWithAuth for dotted remote constituent failed: %v", err)
	}
	if engine == nil {
		t.Fatal("expected non-nil remote engine for dotted constituent")
	}
	if gotAuth != "Bearer forwarded" {
		t.Fatalf("expected forwarded auth token, got %q", gotAuth)
	}
}

// TestCatalogNameExistence preserves the original independent catalog fixtures
// and results for standard, composite, constituent, alias and absent names.
func TestCatalogNameExistence(t *testing.T) {
	for _, test := range []struct {
		name, database, composite, constituent, alias, query string
		want                                                 bool
		message                                              string
	}{
		{"TestExistsOrIsConstituent_StandardDB", "mydb", "", "", "", "mydb", true, "expected ExistsOrIsConstituent to return true for standard database"},
		{"TestExistsOrIsConstituent_CompositeDB", "shard_a", "comp", "a", "", "comp", true, "expected ExistsOrIsConstituent to return true for composite database"},
		{"TestExistsOrIsConstituent_ConstituentDotted", "shard_a", "translations", "tr", "", "translations.tr", true, "expected ExistsOrIsConstituent to return true for dotted constituent reference"},
		{"TestExistsOrIsConstituent_ConstituentDotted_CaseInsensitive", "shard_a", "translations", "TR", "", "translations.tr", true, "expected case-insensitive constituent alias match"},
		{"TestExistsOrIsConstituent_Unknown", "", "", "", "", "doesnotexist", false, "expected ExistsOrIsConstituent to return false for unknown database"},
		{"TestExistsOrIsConstituent_DottedButNotComposite", "notcomposite", "", "", "", "notcomposite.alias", false, "expected false for dotted name where base is not a composite"},
		{"TestExistsOrIsConstituent_DottedWrongAlias", "shard_a", "comp", "a", "", "comp.nonexistent", false, "expected false for dotted name with wrong alias"},
		{"TestExistsOrIsConstituent_Alias", "realdb", "", "", "myalias", "myalias", true, "expected ExistsOrIsConstituent to return true for database alias"},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			manager, err := NewDatabaseManager(t.Context(), base, nil)
			if err != nil {
				t.Fatalf("NewDatabaseManager failed: %v", err)
			}
			fixtureowner.Own(t, manager)
			if test.database != "" {
				if err := manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: test.database}); err != nil {
					t.Fatalf("CreateDatabase failed: %v", err)
				}
			}
			if test.composite != "" {
				if err := manager.CreateCompositeDatabase(t.Context(), test.composite, []ConstituentRef{{Alias: test.constituent, DatabaseName: test.database, Type: "local", AccessMode: "read_write"}}); err != nil {
					t.Fatalf("CreateCompositeDatabase failed: %v", err)
				}
			}
			if test.composite != "" {
				info, err := manager.GetDatabase(test.composite)
				if err != nil {
					t.Fatalf("GetDatabase(%q) failed: %v", test.composite, err)
				}
				if len(info.Constituents) != 1 || info.Constituents[0].DatabaseName != test.database || info.Constituents[0].Alias != test.constituent {
					t.Fatalf("constituent fixture mismatch: got %+v, want DatabaseName %q Alias %q", info.Constituents, test.database, test.constituent)
				}
			}
			if test.alias != "" {
				if err := manager.CreateAlias(t.Context(), test.alias, test.database); err != nil {
					t.Fatalf("CreateAlias failed: %v", err)
				}
			}
			if manager.ExistsOrIsConstituent(test.query) != test.want {
				t.Fatal(test.message)
			}
		})
	}
}

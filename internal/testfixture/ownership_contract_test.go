package testfixture

import (
	"go/parser"
	"go/token"
	"testing"
)

func TestConstructorEntryLint(t *testing.T) {
	for _, tc := range []struct {
		name, imports, source string
		local, process        bool
		gaps                  int
	}{
		{"direct", "", "func TestX(t *testing.T){e:=storage.NewMemoryEngine();defer e.Close()}", false, false, 1},
		{"cleanup_spelling_irrelevant", "", "func TestX(t *testing.T){e:=storage.NewMemoryEngine();t.Cleanup(func(){e.Close()})}", false, false, 1},
		{"owned_without_registry", "", "func TestX(t *testing.T){_=fixture.Own(t,storage.NewMemoryEngine())}", false, false, 1},
		{"registered", "", "func TestX(t *testing.T){e:=fixture.Track(t,storage.NewMemoryEngine(),storage.ErrStorageClosed);defer e.Close()}", false, false, 0},
		{"generic_registered", "", "func TestX(t *testing.T){_=fixture.Track[*storage.MemoryEngine](t,storage.NewMemoryEngine(),storage.ErrStorageClosed)}", false, false, 0},
		{"result_registered", "", "func TestX(t *testing.T){_,_=fixture.TrackResult[*storage.BadgerEngine](t,storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())}", false, false, 0},
		{"function_value", "", "var factory=storage.NewMemoryEngine", false, false, 1},
		{"aliased_factory", "", "func TestX(t *testing.T){f:=storage.NewMemoryEngine;_=fixture.Track(t,f(),storage.ErrStorageClosed)}", false, false, 1},
		{"unrelated_registry", "", "func TestX(t *testing.T){other.Track(t,storage.NewMemoryEngine(),nil)}", false, false, 1},
		{"shadowed_registry", "", "func TestX(t *testing.T){fixture:=other;fixture.Track(t,storage.NewMemoryEngine(),nil)}", false, false, 1},
		{"process_denied", "", "func TestX(t *testing.T){fixture.TrackProcess(boundary,storage.NewMemoryEngine(),storage.ErrStorageClosed)}", false, false, 1},
		{"process_wrong_function", "", "func TestX(t *testing.T){fixture.TrackProcess(boundary,storage.NewMemoryEngine(),storage.ErrStorageClosed)}", false, true, 1},
		{"listed_process", "", "func TestTestMainRejectsLeakedStore(t *testing.T){fixture.TrackProcess(boundary,storage.NewMemoryEngine(),storage.ErrStorageClosed)}", false, true, 0},
		{"parenthesized", "", "func TestX(t *testing.T){_=fixture.Track(t,(storage.NewMemoryEngine)(),storage.ErrStorageClosed)}", false, false, 0},
		{"mvcc", "", "func TestX(t *testing.T){_=fixture.Track(t,storage.NewMemoryEngineWithMVCCHistory(),storage.ErrStorageClosed)}", false, false, 0},
		{"raw", "badger \"github.com/dgraph-io/badger/v4\";", "func TestX(t *testing.T){_,_=fixture.TrackResult[*badger.DB](t,nil)(badger.OpenManaged(options))}", false, false, 0},
		{"raw_alias", "badger \"github.com/dgraph-io/badger/v4\";", "var factory=badger.Open", false, false, 1},
		{"dot", ". \"" + storageImport + "\";", "func TestX(t *testing.T){_=fixture.Track(t,NewMemoryEngine(),ErrStorageClosed)}", false, false, 0},
		{"renamed", "renamed \"" + storageImport + "\";", "func TestX(t *testing.T){_=fixture.Track(t,renamed.NewMemoryEngine(),renamed.ErrStorageClosed)}", false, false, 0},
		{"local_package", "", "func TestX(t *testing.T){_=fixture.Track(t,NewMemoryEngine(),ErrStorageClosed)}", true, false, 0},
		{"local_unregistered", "", "func TestX(t *testing.T){_=NewMemoryEngine()}", true, false, 1},
		{"managed_unregistered", "", "func TestX(t *testing.T){_,_=openManagedBadger(opts)}", true, false, 1},
		{"managed_function_value", "", "var open=openManagedBadger", true, false, 1},
		{"managed_owned", "", "func TestX(t *testing.T){_,_=fixture.OwnResult[*managedBadgerDB](t,nil)(openManagedBadger(opts))}", true, false, 0},
		{"recovery_unregistered", "", "func TestX(t *testing.T){_,_=storage.RecoverFromWAL(dir,path)}", false, false, 1},
		{"recovery_owned", "", "func TestX(t *testing.T){_,_,_=fixture.OwnResult3[*storage.MemoryEngine,storage.ReplayResult](t,storage.ErrStorageClosed)(storage.RecoverFromWALWithResult(dir,path))}", false, false, 0},
		{"no_resource", "", "func TestX(t *testing.T){fixture.Track(t,nil,nil)}", false, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set := token.NewFileSet()
			source := "package example\nimport(storage \"" + storageImport + "\";fixture \"" + fixtureImport + "\";" + tc.imports + ")\n" + tc.source
			file, err := parser.ParseFile(set, "contract.go", source, 0)
			if err != nil {
				t.Fatal(err)
			}
			if got := len(constructorRegistrationGaps(set, file, tc.local, tc.process)); got != tc.gaps {
				t.Fatalf("got %d registration gaps, want %d", got, tc.gaps)
			}
		})
	}
}

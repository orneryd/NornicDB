package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

func TestManagedFixtureCleanupBoundary(t *testing.T) {
	for _, closeRaw := range []bool{false, true} {
		var resource *managedBadgerDB
		t.Run("construction_boundary", func(t *testing.T) {
			var err error
			resource, err = fixtureowner.OwnResult[*managedBadgerDB](t, nil)(openManagedBadger(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)))
			if err != nil {
				t.Fatal(err)
			}
			if closeRaw {
				// Recovery tests close the raw database to preserve crash state.
				if err := resource.DB.Close(); err != nil {
					t.Fatal(err)
				}
			}
			// An early return must not leave the constructor result open.
		})
		if resource == nil || !resource.IsClosed() {
			t.Fatal("managed resource remained open after its actual test boundary")
		}
	}
}

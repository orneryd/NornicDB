package testfixture

import (
	"fmt"
	"testing"
)

// RunCatalogDropCycles performs count create/use/drop cycles against a catalog
// fixture, naming each database with prefix followed by its zero-based index.
// It fails the test on the first error from create, use or drop.
func RunCatalogDropCycles(t testing.TB, count int, prefix string, create, use, drop func(string) error) {
	t.Helper()
	for i := range count {
		name := fmt.Sprintf("%s%d", prefix, i)
		if err := create(name); err != nil {
			t.Fatal(err)
		}
		if err := use(name); err != nil {
			t.Fatal(err)
		}
		if err := drop(name); err != nil {
			t.Fatal(err)
		}
	}
}

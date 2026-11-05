package main

import (
	"path/filepath"
	"sort"
	"testing"
)

// Check the complete repository population through the generator's readers so a
// missing catalog entry fails tests before generation, with every gap reported.
func TestSourceCatalogCoversTypedMessages(t *testing.T) {
	root := filepath.Join("..", "..")
	catalog, err := loadSourceCatalog(filepath.Join(root, "pkg", "localization", "catalog"))
	if err != nil {
		t.Fatal(err)
	}
	constants, constructors, err := loadGoAPI(filepath.Join(root, "pkg", "localization"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("typed constants: %d; English source entries: %d", len(constants), len(catalog))

	names := make([]string, 0, len(constants))
	for name := range constants {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		id := constants[name]
		if _, exists := catalog[id]; !exists {
			t.Errorf("typed message ID %s (%s) has no source catalog entry", name, id)
		}
	}
	if t.Failed() {
		return
	}
	// Also preserve the generator's checks for orphan entries and ambiguous APIs.
	if _, err := validate(catalog, constants, constructors); err != nil {
		t.Fatal(err)
	}
}

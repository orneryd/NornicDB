package cypher

import (
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/apoc"
	"github.com/stretchr/testify/require"
)

func TestBundledAPOCNativeCatalogMatchesExports(t *testing.T) {
	var catalog []string
	for name := range builtInCypherFunctions {
		if strings.HasPrefix(name, "apoc.") {
			catalog = append(catalog, name)
		}
	}
	sort.Strings(catalog)
	excluded := apoc.BuiltInFunctionNames()
	sort.Strings(excluded)
	require.Equal(t, catalog, excluded, "the export owner must exclude every native APOC name")
}

// The wrapper must never shadow a native APOC owner, including collection
// operations whose result differs from the optional package's handler.
func TestBundledAPOCPluginLeavesNativeCatalog(t *testing.T) {
	exported := bundledAPOCPluginFunctions(t)
	for name := range exported {
		if _, builtIn := builtInCypherFunctions[strings.ToLower(name)]; builtIn {
			t.Errorf("bundled plugin exports native function %s", name)
		}
	}
}

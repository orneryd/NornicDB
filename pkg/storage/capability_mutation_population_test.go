package storage_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
)

func TestCapabilityMutationNarrowWriterPopulation(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"go.mod": "module github.com/orneryd/nornicdb\ngo 1.26.0\n",
		"pkg/storage/engine.go": `package storage
 type Node struct{}
 type Edge struct{}
 type Engine interface { Core(); UpdateNode(*Node)error; UpdateEdge(*Edge)error }
`,
		"plugins/unfamiliar/mutation.go": `package unfamiliar
 import "github.com/orneryd/nornicdb/pkg/storage"
 func narrowNode(e interface{UpdateNode(*storage.Node)error}) { _ = e.UpdateNode(nil) }
 func narrowEdge(e interface{UpdateEdge(*storage.Edge)error}) { _ = e.UpdateEdge(nil) }
 func unrelated(e interface{UpdateNode(string)error}) { _ = e.UpdateNode("value") }
`,
	}
	for name, source := range files {
		path := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, []byte(source), 0644))
	}
	inventory, err := workcensus.Extract(capabilitycensus.Config(root))
	require.NoError(t, err)
	result, err := capabilitycensus.Interpret(inventory)
	require.NoError(t, err)
	expected := map[string]string{"narrowNode": "observed", "narrowEdge": "observed"}
	seen := map[string]bool{}
	for _, site := range result.Sites {
		if site.Population != "observed_mutations" || site.Kind != "call" {
			continue
		}
		for name, contract := range expected {
			if strings.HasSuffix(site.Function, "."+name) {
				require.Equal(t, contract, site.Contract, "typed mutation consumer %s", site.Key)
				seen[name] = true
			}
		}
	}
	require.Len(t, seen, len(expected), "both typed narrow mutation consumers must be discovered")
	for _, site := range result.Sites {
		if site.Population == "observed_mutations" {
			require.False(t, strings.HasSuffix(site.Function, ".unrelated"), "a same-spelled string service is not the storage update contract")
		}
	}
}

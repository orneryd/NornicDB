// Author: Lane #1644 (Codex). Candidate membership comes from actual method
// sets and entity/scalar signatures, not API names or receiver substrings.
package storage_test

import (
	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFoundationErrorlessReadClassControl(t *testing.T) {
	root := t.TempDir()
	write := func(name, body string) {
		p := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0755))
		require.NoError(t, os.WriteFile(p, []byte(body), 0644))
	}
	write("go.mod", "module github.com/orneryd/nornicdb\ngo 1.26.0\n")
	write("pkg/storage/api.go", `package storage
 type Node struct{}
 type NodeID string
 type Engine interface{ GetNode(NodeID)(*Node,error); Legacy() *Node }
 `)
	for _, name := range []string{"Unfamiliar", "Renamed"} {
		write("plugins/opaque/view.go", `package opaque
 import "github.com/orneryd/nornicdb/pkg/storage"
 type `+name+` struct{storage.Engine}
 func(*`+name+`)AdditionalRead() *storage.Node{return nil}
 type Other struct{}
 func(*Other)AdditionalRead()string{return "not an entity operation"}
 `)
		inventory, err := workcensus.Extract(capabilitycensus.Config(root))
		require.NoError(t, err)
		result, err := capabilitycensus.Interpret(inventory)
		require.NoError(t, err)
		found := false
		for _, api := range result.ErrorlessReadAPIs {
			if strings.Contains(api.Method, name+").AdditionalRead") {
				found = true
			}
			require.NotContains(t, api.Method, "Other")
		}
		require.True(t, found, "new compiled receiver %s must remain a candidate after mutation, without reusing Discover’s immutable per-root cache", name)
	}
}

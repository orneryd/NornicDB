// Author: Lane #1644 (Codex). Compiled ID-slice/error operation bodies are
// candidates, never approved effects. A new provider must fail registration.
package storage_test

import (
	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestFoundationBatchIDOperationPopulation(t *testing.T) {
	result, err := capabilitycensus.Discover("../..")
	require.NoError(t, err)
	var members []capabilitycensus.Site
	for _, site := range result.Sites {
		if site.Population == "batch_id_operations" {
			members = append(members, site)
		}
	}
	require.NotEmpty(t, members)
	require.Empty(t, capabilitycensus.CoverageProblems(members, capabilityPopulationFile(t, "batch_id_operations", filepath.Join("testdata", "foundation-batch-id-operations.tsv"))))
	t.Logf("%d compiled ID-batch operation bodies individually reviewed", len(members))
}

func TestFoundationBatchIDOperationControl(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"go.mod": "module github.com/orneryd/nornicdb\ngo 1.26\n",
		"pkg/storage/engine.go": `package storage
type Engine interface{Core()}
type NodeID string
type EdgeID string
type Node struct{}
type Edge struct{}
`,
		"plugins/unfamiliar/operation.go": `package unfamiliar
import "github.com/orneryd/nornicdb/pkg/storage"
type Alias=storage.NodeID
func Arbitrary(keys []Alias)error{return nil}
func Another(keys []storage.EdgeID)error{return nil}
func BulkDeleteNodes(keys []string)error{return nil}
func Unfamiliar[T ~string](keys []T)error{return nil}
func Plain[T ~string](keys []T)error{return nil}
func Invoke()error{_ = Unfamiliar([]storage.NodeID{});_ = Unfamiliar([]storage.EdgeID{});return Plain([]string{})}
`,
	}
	for file, source := range files {
		path := filepath.Join(root, file)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, []byte(source), 0644))
	}
	inventory, err := workcensus.Extract(capabilitycensus.Config(root))
	require.NoError(t, err)
	result, err := capabilitycensus.Interpret(inventory)
	require.NoError(t, err)
	var members []capabilitycensus.Site
	for _, site := range result.Sites {
		if site.Population == "batch_id_operations" {
			members = append(members, site)
			require.Empty(t, site.Disposition)
			require.Empty(t, site.Reason)
		}
	}
	require.Len(t, members, 3, "renaming/aliasing and actual generic instances retain the typed contract once per body; same-named and string-only functions are excluded")
	require.NotEmpty(t, capabilitycensus.CoverageProblems(members, nil))
	require.NotEmpty(t, capabilitycensus.CoverageProblems(members, members), "discovery cannot approve itself")
}

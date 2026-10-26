package storage_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
)

// Author: Lane #1644 (Codex). The signature population is independent of
// direct capability uses: every compiled node/edge batch implementation needs
// its own point-equivalence disposition, including APIs outside storage.
func TestFoundationBatchPointPopulationCoverage(t *testing.T) {
	result, err := capabilitycensus.Discover("../..")
	require.NoError(t, err)
	var members []capabilitycensus.Site
	for _, site := range result.Sites {
		if site.Population == "batch_point_pairs" {
			members = append(members, site)
		}
	}
	require.NotEmpty(t, members)
	require.Empty(t, capabilitycensus.CoverageProblems(members, capabilityPopulationFile(t, "batch_point_pairs", filepath.Join("testdata", "foundation-batch-point.tsv"))))
	t.Logf("%d typed batch/point implementations", len(members))
}

// Author: Lane #1644 (Codex). The new outside-package methods use arbitrary
// names and aliases. Actual entity key/map types discover them; names cannot
// grant a disposition or hide a new batch/point obligation.
func TestFoundationBatchPointTypesOutsidePackage(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module github.com/orneryd/nornicdb\n\ngo 1.26\n"), 0644))
	directory := filepath.Join(root, "pkg", "storage")
	require.NoError(t, os.MkdirAll(directory, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "storage.go"), []byte(`package storage
 type Engine interface{Core()}
 type NodeID string
 type EdgeID string
 type Node struct{}
 type Edge struct{}
 `), 0644))
	discover := func() []capabilitycensus.Site {
		inventory, err := workcensus.Extract(capabilitycensus.Config(root))
		require.NoError(t, err)
		result, err := capabilitycensus.Interpret(inventory)
		require.NoError(t, err)
		var out []capabilitycensus.Site
		for _, site := range result.Sites {
			if site.Population == "batch_point_pairs" {
				out = append(out, site)
			}
		}
		return out
	}
	require.Empty(t, discover())
	directory = filepath.Join(root, "plugins", "unfamiliar")
	require.NoError(t, os.MkdirAll(directory, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "unrelated_names.go"), []byte(`package unfamiliar
 import "github.com/orneryd/nornicdb/pkg/storage"
 type Alias=storage.NodeID
 func Renamed(keys []storage.NodeID)(map[storage.NodeID]*storage.Node,error){return nil,nil}
 func AlsoRenamed(keys []Alias)map[Alias]*storage.Node{return nil}
 func Relationships(keys []storage.EdgeID)(map[storage.EdgeID]*storage.Edge,error){return nil,nil}
 func BatchGetNodes(keys []string)(map[string]string,error){return nil,nil}
 `), 0644))
	actual := discover()
	require.Len(t, actual, 3)
	for _, site := range actual {
		require.Equal(t, "plugins/unfamiliar/unrelated_names.go", site.Path)
		require.Empty(t, site.Disposition)
		require.Empty(t, site.Reason)
	}
	require.NotEmpty(t, capabilitycensus.CoverageProblems(actual, nil), "a new member must fail the same manifest gate")
	require.NotEmpty(t, capabilitycensus.CoverageProblems(actual, actual), "a discovered candidate must stay OPEN until individually read and reviewed")
}

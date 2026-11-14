package storage

import (
	"context"
	"encoding/json"
	"github.com/orneryd/nornicdb/experiments/property-storage/corpus"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

type experimentalDocsMeasurement struct {
	Enabled         bool
	Documents       int
	CorpusSHA256    string
	DirectoryBytes  int64
	SeedNS          int64
	ProjectedReadNS int64
	FullReadNS      int64
}

// TestExperimentalDocsStorage is opt-in because it measures real per-document
// durable writes, reads and clean-close file sizes, rather than codec-only size.
func TestExperimentalDocsStorage(t *testing.T) {
	root := os.Getenv("NORNIC_STORAGE_DOCS_ROOT")
	out := os.Getenv("NORNIC_STORAGE_DOCS_RESULTS")
	if root == "" || out == "" {
		t.Skip("set NORNIC_STORAGE_DOCS_ROOT and NORNIC_STORAGE_DOCS_RESULTS")
	}
	nodes, corpus := loadExperimentalDocs(t, root)
	previous := config.IsExperimentalPropertyStorageEnabled()
	t.Cleanup(func() { config.SetExperimentalPropertyStorageEnabled(previous) })
	var results []experimentalDocsMeasurement
	for _, enabled := range []bool{false, true} {
		config.SetExperimentalPropertyStorageEnabled(enabled)
		directory := t.TempDir()
		engine, e := NewBadgerEngine(directory)
		require.NoError(t, e)
		row := experimentalDocsMeasurement{Enabled: enabled, Documents: len(nodes), CorpusSHA256: corpus}
		begin := time.Now()
		for _, node := range nodes {
			_, e = engine.CreateNode(context.Background(), node)
			require.NoError(t, e)
		}
		row.SeedNS = time.Since(begin).Nanoseconds()
		begin = time.Now()
		for _, node := range nodes {
			got, e := engine.GetNodeProjected(context.Background(), node.ID, []string{"path"})
			require.NoError(t, e)
			require.Equal(t, map[string]any{"path": node.Properties["path"]}, got.Properties)
		}
		row.ProjectedReadNS = time.Since(begin).Nanoseconds()
		begin = time.Now()
		for _, node := range nodes {
			got, e := engine.GetNode(context.Background(), node.ID)
			require.NoError(t, e)
			require.Equal(t, node.Properties, got.Properties)
		}
		row.FullReadNS = time.Since(begin).Nanoseconds()
		require.NoError(t, engine.Close())
		require.NoError(t, filepath.WalkDir(directory, func(p string, d os.DirEntry, e error) error {
			if e != nil {
				return e
			}
			if !d.IsDir() {
				info, e := d.Info()
				if e != nil {
					return e
				}
				row.DirectoryBytes += info.Size()
			}
			return nil
		}))
		engine, e = NewBadgerEngine(directory)
		require.NoError(t, e)
		for _, node := range nodes {
			got, e := engine.GetNode(context.Background(), node.ID)
			require.NoError(t, e)
			require.Equal(t, node.Properties, got.Properties)
		}
		require.NoError(t, engine.Close())
		results = append(results, row)
	}
	data, e := json.MarshalIndent(results, "", "  ")
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(out, data, 0600))
}

// loadExperimentalDocs is shared by size checks and process-isolated profiles.
func loadExperimentalDocs(t *testing.T, root string) ([]*Node, string) {
	t.Helper()
	records, digest, err := corpus.Load(root)
	require.NoError(t, err)
	nodes := make([]*Node, len(records))
	for i, record := range records {
		nodes[i] = &Node{ID: NodeID(record.ID), Labels: []string{"Documentation"}, Properties: record.Properties}
	}
	return nodes, digest
}

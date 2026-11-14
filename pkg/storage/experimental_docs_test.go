package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	var paths []string
	require.NoError(t, filepath.WalkDir(root, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() && strings.HasSuffix(p, ".md") {
			paths = append(paths, p)
		}
		return nil
	}))
	sort.Strings(paths)
	require.NotEmpty(t, paths)
	var nodes []*Node
	digest := sha256.New()
	for _, path := range paths {
		relative, e := filepath.Rel(root, path)
		require.NoError(t, e)
		relative = filepath.ToSlash(relative)
		body, e := os.ReadFile(path)
		require.NoError(t, e)
		hash := sha256.Sum256(body)
		fmt.Fprintf(digest, "%s\x00%x\n", relative, hash)
		id := sha256.Sum256([]byte(relative))
		noise := make([]byte, 4096)
		for i := 0; i < len(noise); i += 32 {
			h := sha256.Sum256([]byte(fmt.Sprintf("%s/%x/%d", relative, hash, i)))
			copy(noise[i:], h[:])
		}
		metrics := make([]int64, 256)
		for i := range metrics {
			metrics[i] = int64((len(body) + i) % 64)
		}
		nodes = append(nodes, &Node{ID: NodeID("docs:" + hex.EncodeToString(id[:16])), Labels: []string{"Documentation"}, Properties: map[string]any{"path": relative, "body": string(body), "noise": noise, "metrics": metrics, "status": "current"}})
	}
	previous := config.IsExperimentalPropertyStorageEnabled()
	t.Cleanup(func() { config.SetExperimentalPropertyStorageEnabled(previous) })
	var results []experimentalDocsMeasurement
	for _, enabled := range []bool{false, true} {
		config.SetExperimentalPropertyStorageEnabled(enabled)
		directory := t.TempDir()
		engine, e := NewBadgerEngine(directory)
		require.NoError(t, e)
		row := experimentalDocsMeasurement{Enabled: enabled, Documents: len(nodes), CorpusSHA256: hex.EncodeToString(digest.Sum(nil))}
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

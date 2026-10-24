package nornicdb

import (
	"archive/zip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// Exercise the real constructor authority with a store and twenty pending
// records written by the unchanged release base, before identities existed.
func TestOpenGenuinePreIdentityWALUpgrade(t *testing.T) {
	archive, err := zip.OpenReader(filepath.Join("..", "storage", "testdata", "wal-pre-identity", "release.zip"))
	require.NoError(t, err)
	defer archive.Close()
	dir := t.TempDir()
	for _, file := range archive.File {
		require.True(t, filepath.IsLocal(file.Name))
		name := strings.TrimPrefix(file.Name, "data/")
		path := filepath.Join(dir, filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
		input, err := file.Open()
		require.NoError(t, err)
		output, err := os.Create(path)
		require.NoError(t, err)
		_, err = io.Copy(output, input)
		require.NoError(t, err)
		require.NoError(t, output.Close())
		require.NoError(t, input.Close())
	}
	cfg := config.LoadDefaults()
	cfg.Database.StrictDurability = false
	cfg.Database.AsyncWritesEnabled = true
	cfg.Database.WALAutoCompactionEnabled = false
	for reopen := 0; reopen < 2; reopen++ {
		db, err := Open(dir, cfg)
		require.NoError(t, err)
		node, err := db.baseStorage.GetNode("upgrade:a")
		require.NoError(t, err)
		require.Equal(t, int64(20), node.Properties["value"])
		stamp, ok := node.Properties["stamp"].(time.Time)
		require.True(t, ok)
		require.True(t, time.Date(2026, 10, 23, 1, 2, 3, 4000, time.UTC).Equal(stamp))
		require.NoError(t, db.Close())
	}
}

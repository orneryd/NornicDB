package nornicdb

import (
	"path/filepath"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// Exercise the real constructor authority with a store and twenty pending
// records written by the unchanged release base, before identities existed.
func TestOpenGenuinePreIdentityWALUpgrade(t *testing.T) {
	dir := fixtureowner.ExtractZip(t, filepath.Join("..", "storage", "testdata", "wal-pre-identity", "release.zip"), "data/")
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

package nornicdb

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// The fixture was written by b6d8e762a: its snapshot contains Before, while
// the acknowledged later Cypher transaction contains After but only begin and
// commit markers in the historical WAL. Upgrade must establish a new recovery
// baseline before the current store can serve traffic.
func TestHistoricalUpgradePublishesRecoverableBaseline(t *testing.T) {
	for _, autoCompaction := range []bool{false, true} {
		t.Run(map[bool]string{false: "manual-compaction", true: "automatic-compaction"}[autoCompaction], func(t *testing.T) {
			dir := fixtureowner.ExtractZip(t, filepath.Join("testdata", "s05-b6-recovery.zip"), "data/")
			cfg := DefaultConfig()
			cfg.Memory.EmbeddingEnabled = false
			cfg.Memory.AutoLinksEnabled = false
			cfg.Memory.DecayEnabled = false
			cfg.Database.AllowStorageUpgrade = true
			cfg.Database.WALAutoCompactionEnabled = autoCompaction
			cfg.Database.WALSnapshotInterval = time.Hour

			assertAfter := func(db *DB) {
				t.Helper()
				result, err := db.ExecuteCypher(t.Context(), "MATCH (n:After {uid:'after'}) RETURN n.value", nil)
				require.NoError(t, err)
				require.Equal(t, [][]any{{"must-recover"}}, result.Rows)
			}
			db, err := Open(dir, cfg)
			require.NoError(t, err)
			assertAfter(db)
			require.NoError(t, db.Close())
			pending, err := historicalRecoveryBaselineIsPending(dir)
			require.NoError(t, err)
			require.False(t, pending)

			// Model a later Badger failure after the successful upgrade. Recovery
			// may only use the snapshots and WAL that successful Open left behind.
			require.NoError(t, os.WriteFile(filepath.Join(dir, "MANIFEST"), []byte("bad manifest"), 0600))
			db, err = Open(dir, cfg)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			assertAfter(db)
		})
	}
}

func TestHistoricalRecoveryBaselinePendingRefusesRebuild(t *testing.T) {
	dir := fixtureowner.ExtractZip(t, filepath.Join("testdata", "s05-b6-recovery.zip"), "data/")
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.DecayEnabled = false
	cfg.Database.AllowStorageUpgrade = true
	db, err := Open(dir, cfg)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// A pending marker models a process that stopped between migration and
	// snapshot publication. Corruption at that point cannot be repaired from
	// a potentially stale historical snapshot and transaction-only WAL.
	require.NoError(t, markHistoricalRecoveryBaselinePending(dir))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "MANIFEST"), []byte("bad manifest"), 0600))
	db, err = Open(dir, cfg)
	require.Nil(t, db)
	require.ErrorContains(t, err, "recovery baseline is pending")
	pending, err := historicalRecoveryBaselineIsPending(dir)
	require.NoError(t, err)
	require.True(t, pending)
	_, err = os.Stat(filepath.Join(dir, "MANIFEST"))
	require.NoError(t, err, "failed rebuild must preserve the original store")
}

func TestHistoricalEncryptedUpgradeKeepsExistingSnapshotPolicy(t *testing.T) {
	dir := fixtureowner.ExtractZip(t, filepath.Join("testdata", "s05-b6-encrypted-upgrade.zip"), "data/")
	before, err := filepath.Glob(filepath.Join(dir, "snapshots", "snapshot-*.json"))
	require.NoError(t, err)
	require.Len(t, before, 1)

	cfg := DefaultConfig()
	cfg.Database.EncryptionEnabled = true
	cfg.Database.EncryptionPassword = "historical-fixture-password"
	cfg.Database.AllowStorageUpgrade = true
	cfg.Database.WALAutoCompactionEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.DecayEnabled = false
	db, err := Open(dir, cfg)
	require.NoError(t, err)
	result, err := db.ExecuteCypher(t.Context(), "MATCH (n:After {uid:'after'}) RETURN n.value", nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"must-recover"}}, result.Rows)
	require.NoError(t, db.Close())

	after, err := filepath.Glob(filepath.Join(dir, "snapshots", "snapshot-*.json"))
	require.NoError(t, err)
	require.Equal(t, before, after, "upgrading an encrypted store must not force a new recovery snapshot")
	pending, err := historicalRecoveryBaselineIsPending(dir)
	require.NoError(t, err)
	require.False(t, pending)
}

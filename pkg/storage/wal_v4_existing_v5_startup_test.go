package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestV4ExistingStoreUpgradesV5SemanticWALAtStartup(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, interrupted := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "interrupted"}[interrupted], func(t *testing.T) {
			dir := t.TempDir()
			options := BadgerOptions{DataDir: dir, LowMemory: true}
			engine, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(options))
			require.NoError(t, err)
			_, err = engine.CreateNode(&Node{ID: "test:existing", Properties: map[string]any{"value": "old"}})
			require.NoError(t, err)
			require.NoError(t, engine.writeSchemaVersion(storageVersionCatalogV4))
			if interrupted {
				require.NoError(t, engine.db.Update(func(tx *badger.Txn) error { return tx.Set(v4WALFormatRecoveryKey, []byte{1}) }))
				require.NoError(t, engine.db.Sync())
			}
			require.NoError(t, engine.Close())

			payload, err := encodeV4GobEntityTransaction("test", "historical-v5", []Operation{{
				Type: OpCreateNode, Node: &Node{ID: "test:replayed", Properties: map[string]any{"count": int64(9)}},
			}}, CommitModeImplicit)
			require.NoError(t, err)
			data, err := marshalWALRecoveryPayload(nil, walEntityCommitData{Payload: payload})
			require.NoError(t, err)
			entry := WALEntry{Sequence: 1, Timestamp: time.Unix(1, 0), Operation: OpEntityCommit, Database: "test", Data: data, Checksum: crc32Checksum(data)}
			walDir := filepath.Join(dir, "wal")
			require.NoError(t, os.MkdirAll(walDir, 0700))
			require.NoError(t, os.WriteFile(filepath.Join(walDir, "wal.log"), priorWALRecord(t, 5, entry), 0600))
			if interrupted {
				injected := errors.New("interrupt after WAL publication")
				_, err = UpgradeWALV4(walDir, WALV4UpgradeOptions{afterPublish: func(string) error { return injected }})
				require.ErrorIs(t, err, injected)
			}

			options.AllowStorageUpgrade = !interrupted
			engine, err = fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(options))
			require.NoError(t, err)
			existing, err := engine.GetStoredNode("test:existing")
			require.NoError(t, err)
			require.Equal(t, "old", existing.Properties["value"])
			replayed, err := engine.GetStoredNode("test:replayed")
			require.NoError(t, err)
			require.Equal(t, int64(9), replayed.Properties["count"])
			require.NoError(t, engine.Close())

			options.AllowStorageUpgrade = false
			engine, err = fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(options))
			require.NoError(t, err)
			replayed, err = engine.GetStoredNode("test:replayed")
			require.NoError(t, err)
			require.Equal(t, int64(9), replayed.Properties["count"])
			count, err := engine.NodeCount()
			require.NoError(t, err)
			require.EqualValues(t, 2, count)
		})
	}
}

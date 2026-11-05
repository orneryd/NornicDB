package nornicdb

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestReplicationWALUpgradeRequiresStorageAuthorization(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, tc := range []struct {
		name, mode string
		corrupt    bool
	}{
		{name: "active", mode: "invalid-mode"},
		{name: "standalone", mode: "standalone"},
		{name: "corrupt", mode: "invalid-mode", corrupt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NORNICDB_CLUSTER_MODE", tc.mode)
			t.Setenv("NORNICDB_CLUSTER_DATA_DIR", "")
			dir := t.TempDir()
			walDir := filepath.Join(dir, "replication", "wal")
			wal, err := storage.NewWAL(walDir, storage.DefaultWALConfig())
			require.NoError(t, err)
			encodeOld := func(value any) []byte {
				t.Helper()
				var encoded bytes.Buffer
				require.NoError(t, gob.NewEncoder(&encoded).Encode(value))
				return encoded.Bytes()
			}
			type oldNode struct {
				ID              string
				Labels          []string
				Properties      map[string]any
				NamedEmbeddings map[string][]float32
				ChunkEmbeddings [][]float32
				CreatedAtUnixNs int64
				UpdatedAtUnixNs int64
			}
			type oldChange struct {
				Type            storage.EntityCommandKind
				Data            []byte
				NamedProperties []byte
				EmbedMeta       []byte
			}
			values, err := storage.EncodeNamedProperties(map[string]any{"count": int64(9)})
			require.NoError(t, err)
			metadata, err := storage.EncodeNamedProperties(nil)
			require.NoError(t, err)
			oldTransaction := encodeOld(struct {
				Version     uint16
				Namespace   string
				LogicalTxID string
				Changes     []oldChange
			}{Version: 1, Namespace: "test", LogicalTxID: "old", Changes: []oldChange{{
				Type: storage.EntityCreateNode, Data: encodeOld(oldNode{ID: "test:n"}), NamedProperties: values, EmbedMeta: metadata,
			}}})
			if tc.corrupt {
				oldTransaction = []byte("invalid historical semantic transaction")
			}
			require.NoError(t, wal.Append(storage.OperationType("replication_command"), map[string]any{
				"ts": int64(7), "cmd": &replication.Command{Type: replication.CmdCommitTransaction, Data: oldTransaction},
			}))
			require.NoError(t, wal.Close())
			walPath := filepath.Join(walDir, "wal.log")
			original, err := os.ReadFile(walPath)
			require.NoError(t, err)
			require.Greater(t, len(original), 5)
			original[4] = 5
			require.NoError(t, os.WriteFile(walPath, original, 0600))

			cfg := DefaultConfig()
			cfg.Database.DataDir = dir
			cfg.Memory.EmbeddingEnabled = false
			cfg.Memory.AutoLinksEnabled = false
			if tc.mode != "standalone" {
				manifest := filepath.Join(dir, "MANIFEST")
				_, statErr := os.Stat(manifest)
				require.True(t, os.IsNotExist(statErr))
				opened, openErr := Open(dir, cfg)
				require.Nil(t, opened)
				require.ErrorIs(t, openErr, storage.ErrWALUnsupportedVersion)
				_, statErr = os.Stat(manifest)
				require.True(t, os.IsNotExist(statErr), "an unsupported replication WAL must be rejected before creating Badger state")
			}
			db := &DB{config: cfg}
			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			got, err := db.maybeEnableReplication(base)
			if tc.mode == "standalone" {
				require.NoError(t, err)
				require.Same(t, base, got)
			} else {
				require.ErrorIs(t, err, storage.ErrWALUnsupportedVersion)
			}
			unchanged, err := os.ReadFile(walPath)
			require.NoError(t, err)
			require.Equal(t, original, unchanged)

			cfg.Database.AllowStorageUpgrade = true
			if tc.corrupt {
				opened, openErr := Open(dir, cfg)
				require.Nil(t, opened)
				require.Error(t, openErr)
				_, statErr := os.Stat(filepath.Join(dir, "MANIFEST"))
				require.True(t, os.IsNotExist(statErr), "a failed replication WAL conversion must precede Badger startup")
				after, readErr := os.ReadFile(walPath)
				require.NoError(t, readErr)
				require.Equal(t, original, after)
				return
			}
			got, err = db.maybeEnableReplication(base)
			if tc.mode == "standalone" {
				require.NoError(t, err)
				require.Same(t, base, got)
			} else {
				require.Error(t, err, "the deliberately invalid mode still cannot start replication")
				require.NotErrorIs(t, err, storage.ErrWALUnsupportedVersion)
			}
			require.NoError(t, storage.ValidateWALFormat(walDir, storage.WALValidateCurrent))
			entries, err := storage.ReadWALEntriesFromDir(walDir)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			var record struct {
				Command *replication.Command `json:"cmd"`
			}
			require.NoError(t, json.Unmarshal(entries[0].Data, &record))
			require.NotNil(t, record.Command)
			envelope, ops, err := storage.DecodeEntityTransaction(record.Command.Data)
			require.NoError(t, err)
			require.Equal(t, "old", envelope.LogicalTxID)
			require.Len(t, ops, 1)
			require.Equal(t, int64(9), ops[0].Node.Properties["count"])
			backup, err := os.ReadFile(filepath.Join(walDir+".before-v4-v6", "wal.log"))
			require.NoError(t, err)
			require.Equal(t, original, backup)
		})
	}
}

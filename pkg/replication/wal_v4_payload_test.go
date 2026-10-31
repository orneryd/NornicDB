package replication

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type historicalReplicationChange struct {
	Type                 storage.EntityCommandKind
	Data                 []byte
	NamedProperties      []byte
	EmbedMeta            []byte
	UpdatedAtUnixNs      int64
	VisibilitySuppressed bool
}

type historicalReplicationEnvelope struct {
	Mode        storage.EntityCommitMode
	Version     uint16
	Namespace   string
	LogicalTxID string
	Changes     []historicalReplicationChange
}

func historicalReplicationTransaction(t *testing.T) []byte {
	t.Helper()
	encode := func(value any) []byte {
		t.Helper()
		var buf bytes.Buffer
		require.NoError(t, gob.NewEncoder(&buf).Encode(value))
		return buf.Bytes()
	}
	properties, err := storage.EncodeNamedProperties(map[string]any{"count": int64(9)})
	require.NoError(t, err)
	metadata, err := storage.EncodeNamedProperties(map[string]any{"source": "replication"})
	require.NoError(t, err)
	change := historicalReplicationChange{Type: storage.EntityCreateNode, Data: encode(struct {
		ID     string
		Labels []string
	}{ID: "test:n", Labels: []string{"Item"}}), NamedProperties: properties, EmbedMeta: metadata}
	return encode(historicalReplicationEnvelope{Version: 1, Namespace: "test", LogicalTxID: "historical", Changes: []historicalReplicationChange{change}})
}

func TestV4ReplicationWALUpgradePreservesSemanticCommand(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	wal, err := storage.NewWAL(dir, storage.DefaultWALConfig())
	require.NoError(t, err)
	historical := historicalReplicationTransaction(t)
	command := &Command{Type: CmdCommitTransaction, Data: historical, RequestID: "request", Timestamp: time.Unix(7, 0).UTC()}
	require.NoError(t, wal.Append(storage.OperationType("replication_command"), replicationWALRecord{Timestamp: 7, Command: command}))
	require.NoError(t, wal.Close())
	path := filepath.Join(dir, "wal.log")
	old, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, len(old), 5)
	old[4] = 5 // v5 uses the same outer record framing and checksum.
	require.NoError(t, os.WriteFile(path, old, 0600))

	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	_, err = NewStorageAdapterWithWAL(engine, dir)
	require.ErrorIs(t, err, storage.ErrWALUnsupportedVersion)
	changed, err := storage.UpgradeWALV4(dir, storage.WALV4UpgradeOptions{Transform: TranscodeV4WALPayload})
	require.NoError(t, err)
	require.True(t, changed)
	backup, err := os.ReadFile(filepath.Join(dir+".before-v4-v6", "wal.log"))
	require.NoError(t, err)
	require.Equal(t, old, backup)

	adapter, err := NewStorageAdapterWithWAL(engine, dir)
	require.NoError(t, err)
	fixtureowner.Own(t, adapter)
	entries, err := adapter.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, command.RequestID, entries[0].Command.RequestID)
	envelope, ops, err := storage.DecodeEntityTransaction(entries[0].Command.Data)
	require.NoError(t, err)
	require.Equal(t, "historical", envelope.LogicalTxID)
	require.Len(t, ops, 1)
	require.Equal(t, map[string]any{"count": int64(9)}, ops[0].Node.Properties)
	require.Equal(t, map[string]any{"source": "replication"}, ops[0].Node.EmbedMeta)
	require.NoError(t, adapter.Close())
	changed, err = storage.UpgradeWALV4(dir, storage.WALV4UpgradeOptions{Transform: TranscodeV4WALPayload})
	require.NoError(t, err)
	require.False(t, changed)
}

func TestV4ReplicationWALTransformRejectsMalformedCommand(t *testing.T) {
	for _, payload := range [][]byte{[]byte(`{`), []byte(`{"cmd":null}`), []byte(`{"cmd":{"Type":255}}`)} {
		_, err := TranscodeV4WALPayload(5, "replication_command", payload)
		require.Error(t, err)
	}
	malformed, err := json.Marshal(replicationWALRecord{Command: &Command{Type: CmdCommitTransaction, Data: []byte("invalid")}})
	require.NoError(t, err)
	_, err = TranscodeV4WALPayload(5, "replication_command", malformed)
	require.Error(t, err)
}

package nornicdb

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/stretchr/testify/require"
)

// b6ReplicationFrame frames one replication-journal record as the b6d8e762a
// writer did (version 3, no size limit).
func b6ReplicationFrame(t *testing.T, seq uint64, data []byte) []byte {
	t.Helper()
	table := crc32.MakeTable(crc32.Castagnoli)
	header, err := json.Marshal(map[string]any{"seq": seq, "ts": time.Unix(1_794_000_000, 0).UTC(), "op": "replication_command", "data": nil, "checksum": crc32.Checksum(data, table)})
	require.NoError(t, err)
	payload := make([]byte, 4+len(header)+4+len(data))
	binary.LittleEndian.PutUint32(payload, uint32(len(header)))
	copy(payload[4:], header)
	binary.LittleEndian.PutUint32(payload[4+len(header):], uint32(len(data)))
	copy(payload[8+len(header):], data)
	frame := make([]byte, 9, 9+len(payload)+12+7)
	binary.LittleEndian.PutUint32(frame, 0x454C4157) // "WALE"
	frame[4] = 3
	binary.LittleEndian.PutUint32(frame[5:], uint32(len(payload)))
	frame = append(frame, payload...)
	frame = binary.LittleEndian.AppendUint32(frame, crc32.Checksum(payload, table))
	frame = binary.LittleEndian.AppendUint64(frame, 0xDEADBEEFFEEDFACE)
	for len(frame)%8 != 0 {
		frame = append(frame, 0)
	}
	return frame
}

func replicationDirDigest(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	out := map[string][32]byte{}
	require.NoError(t, filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		out[rel] = sha256.Sum256(raw)
		return nil
	}))
	return out
}

// TestReplicationWALUpgradeRefusesOversizedHistoricalCommand: the upgrade
// carries old store-journal records over one frame (NOR-412), but the
// replication journal keeps one-frame records: an old version-3 replication command over
// 16 MiB is still refused at upgrade, and no file changes.
func TestReplicationWALUpgradeRefusesOversizedHistoricalCommand(t *testing.T) {
	command, err := json.Marshal(map[string]any{"cmd": map[string]any{"Type": replication.CmdCreateNode, "Data": []byte(strings.Repeat("x", 17<<20))}})
	require.NoError(t, err)
	parent := t.TempDir()
	dir := filepath.Join(parent, "replication", "wal")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "wal.log"), b6ReplicationFrame(t, 1, command), 0o600))
	before := replicationDirDigest(t, parent)

	err = prepareReplicationWAL(dir, true, string(replication.ModeHAStandby))
	require.ErrorContains(t, err, "exceeds maximum 16777216")
	require.Equal(t, before, replicationDirDigest(t, parent), "a refused upgrade must not change any file")
}

// TestReplicationWALUpgradeRefusesOversizedHeaderlessCommand: the same holds
// for a replication journal in the headerless historical format (one JSON
// object per line): this test's command, whose converted record does not fit one frame,
// is refused before anything is written, backed up or replaced.
func TestReplicationWALUpgradeRefusesOversizedHeaderlessCommand(t *testing.T) {
	value, err := json.Marshal(struct{ Prefix string }{Prefix: strings.Repeat("x", 17<<20)})
	require.NoError(t, err)
	command, err := json.Marshal(map[string]any{"cmd": map[string]any{"Type": replication.CmdDeleteByPrefix, "Data": value}})
	require.NoError(t, err)
	line, err := json.Marshal(map[string]any{"seq": 1, "ts": time.Unix(1_794_000_000, 0).UTC(), "op": "replication_command", "data": command, "checksum": crc32.Checksum(command, crc32.MakeTable(crc32.Castagnoli))})
	require.NoError(t, err)
	parent := t.TempDir()
	dir := filepath.Join(parent, "replication", "wal")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "wal.log"), append(line, '\n'), 0o600))
	before := replicationDirDigest(t, parent)

	err = prepareReplicationWAL(dir, true, string(replication.ModeHAStandby))
	require.ErrorContains(t, err, "entry exceeds maximum WAL frame size of 16777216 bytes")
	require.Equal(t, before, replicationDirDigest(t, parent), "a refused upgrade must not change, back up or replace any file")
}

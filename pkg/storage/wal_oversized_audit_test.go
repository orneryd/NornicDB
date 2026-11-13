package storage

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func oversizedAuditFixture(t *testing.T, current bool) []byte {
	t.Helper()
	compressed, err := os.ReadFile("testdata/wal_b6_oversized_embedding.wal.gz")
	require.NoError(t, err)
	z, err := gzip.NewReader(bytes.NewReader(compressed))
	require.NoError(t, err)
	raw, err := io.ReadAll(z)
	require.NoError(t, err)
	require.NoError(t, z.Close())
	require.Equal(t, "0de00f9722ae1ba02c9052d9b299fd67fc11b0c596eb87671c30bb2af152581e", fmt.Sprintf("%x", sha256.Sum256(raw)))
	var versions []byte
	for off := int64(0); off < int64(len(raw)); {
		require.Equal(t, walMagic, binary.LittleEndian.Uint32(raw[off:]))
		versions = append(versions, raw[off+4])
		if current {
			raw[off+4] = walFormatVersionCurrent
		}
		off += alignUp(9 + int64(binary.LittleEndian.Uint32(raw[off+5:])) + 4 + 8)
	}
	require.Equal(t, []byte{3, 3, 3}, versions, "fixture was emitted by the b6 writer")
	return raw
}

func oversizedAuditOffsets(raw []byte) (start, payload, end int64) {
	start = alignUp(9 + int64(binary.LittleEndian.Uint32(raw[5:])) + 4 + 8)
	payload = start + 9
	end = start + alignUp(9+int64(binary.LittleEndian.Uint32(raw[start+5:]))+4+8)
	return
}

func TestOversizedEmbeddingAuditKeepsTailSequenceAndWriteBound(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	raw := oversizedAuditFixture(t, true)
	_, payload, end := oversizedAuditOffsets(raw)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "wal.log"), raw[:end], 0600))
	w, err := NewWAL(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	seq, err := w.AppendWithDatabaseReturningSeq(OpCreateNode, WALNodeData{Node: &Node{ID: "new-suffix"}}, "test")
	require.NoError(t, err)
	require.EqualValues(t, 3, seq, "an omitted audit still owns its sequence")
	require.NoError(t, w.Sync())
	entries, err := ReadWALEntriesFromDir(t.Context(), dir)
	require.NoError(t, err)
	requireOversizedAuditSuffix(t, entries)
	headerLen := int64(binary.LittleEndian.Uint32(raw[payload:]))
	dataLenOff := payload + 4 + headerLen
	dataLen := int64(binary.LittleEndian.Uint32(raw[dataLenOff:]))
	err = w.AppendWithDatabase(OpUpdateEmbedding, json.RawMessage(raw[dataLenOff+4:dataLenOff+4+dataLen]), "test")
	require.ErrorContains(t, err, "exceeds maximum WAL frame size", "new writes must still obey the original bound")
}

func TestOversizedEmbeddingAuditRejectsCorruptAndSemanticFrames(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	// Interrupted writes at each envelope/framing boundary must not turn into
	// an authenticated omission or allow the reader to resynchronize past it.
	for _, fault := range []string{"outer_crc", "inner_crc", "trailer", "data_length", "header_length", "header_json", "semantic_operation", "short_header", "short_json", "short_data_length", "short_data", "short_crc", "short_trailer", "truncated"} {
		t.Run(fault, func(t *testing.T) {
			raw := oversizedAuditFixture(t, true)
			start, payload, end := oversizedAuditOffsets(raw)
			size := int64(binary.LittleEndian.Uint32(raw[start+5:]))
			headerLen := int64(binary.LittleEndian.Uint32(raw[payload:]))
			dataLenOff := payload + 4 + headerLen
			switch fault {
			case "outer_crc":
				raw[payload+size] ^= 1
			case "inner_crc":
				raw[dataLenOff+4] ^= 1
				binary.LittleEndian.PutUint32(raw[payload+size:], crc32Checksum(raw[payload:payload+size]))
			case "trailer":
				raw[payload+size+4] ^= 1
			case "data_length":
				binary.LittleEndian.PutUint32(raw[dataLenOff:], 1)
			case "header_length":
				binary.LittleEndian.PutUint32(raw[payload:], walMaxEntrySize+1)
			case "header_json":
				raw[payload+4] = '!'
			case "semantic_operation":
				old, replacement := []byte(`"update_embedding"`), []byte(`"create_node"`)
				replacement = append(replacement, bytes.Repeat([]byte{' '}, len(old)-len(replacement))...)
				header := bytes.Replace(raw[payload+4:payload+4+headerLen], old, replacement, 1)
				require.NotEqual(t, raw[payload+4:payload+4+headerLen], header)
				copy(raw[payload+4:], header)
				binary.LittleEndian.PutUint32(raw[payload+size:], crc32Checksum(raw[payload:payload+size]))
			case "truncated":
				raw = raw[:end-1]
			case "short_header":
				raw = raw[:payload+2]
			case "short_json":
				raw = raw[:payload+5]
			case "short_data_length":
				raw = raw[:dataLenOff+2]
			case "short_data":
				raw = raw[:dataLenOff+4+4096]
			case "short_crc":
				raw = raw[:payload+size+2]
			case "short_trailer":
				raw = raw[:payload+size+4+4]
			}
			dir := t.TempDir()
			file := filepath.Join(dir, "wal.log")
			require.NoError(t, os.WriteFile(file, raw, 0600))
			_, err := ReadWALEntries(t.Context(), file, nil)
			require.Error(t, err)
			report, err := CheckWALIntegrity(file)
			require.NoError(t, err)
			require.False(t, report.Healthy)
			entries, err := readWALEntriesForTruncation(file, 0)
			require.NoError(t, err)
			require.Len(t, entries, 1, "no resynchronization past a corrupt/semantic oversized frame")
			require.Error(t, sealWALPropertyEpochFile(file))
			// Normal readers and integrity/repair checks still reject every
			// unauthenticated frame above. Only upgrade of an active journal
			// may keep the complete prefix before an interrupted final write.
			switch fault {
			case "short_header", "short_json", "short_data_length", "short_data", "short_crc", "short_trailer", "truncated":
				require.NoError(t, ValidateWALFormat(dir, WALValidateV4Upgrade))
			default:
				require.Error(t, ValidateWALFormat(dir, WALValidateV4Upgrade))
			}
			f, err := os.Open(file)
			require.NoError(t, err)
			offset, _, _, valid, err := scanAtomicWALForRepair(f, int64(len(raw)))
			require.NoError(t, f.Close())
			require.NoError(t, err)
			require.False(t, valid)
			require.Equal(t, start, offset, "existing repair policy still stops at the invalid frame")
		})
	}
}

func TestOversizedEmbeddingAuditRejectsHistoricalEpochVersion(t *testing.T) {
	raw := oversizedAuditFixture(t, false)
	start, _, _ := oversizedAuditOffsets(raw)
	raw[start+4] = 4 // v4 is reserved for property epochs, never node audits.
	dir := t.TempDir()
	file := filepath.Join(dir, "wal.log")
	require.NoError(t, os.WriteFile(file, raw, 0600))
	require.Error(t, ValidateWALFormat(dir, WALValidateV4Upgrade))
	_, err := UpgradeWALV4(dir, WALV4UpgradeOptions{})
	require.Error(t, err)
	unchanged, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, raw, unchanged)
}

func requireOversizedAuditSuffix(t *testing.T, entries []WALEntry) {
	t.Helper()
	require.Len(t, entries, 3, "valid semantic suffix after the audit must remain")
	require.Equal(t, []OperationType{OpCreateNode, OpUpdateEmbedding, OpCreateNode}, walOps(entries))
	for i, entry := range entries {
		require.EqualValues(t, i+1, entry.Sequence)
	}
	require.Less(t, len(entries[1].Data), 1024, "the historical image must not be materialized into the result")
	_, err := EntryPayloadJSON(entries[1])
	require.NoError(t, err, "txlog must render the explicit historical omission")
}

func requireOversizedAuditRecovery(t *testing.T, dir string) {
	t.Helper()
	e, result, err := fixtureowner.OwnResult3[*MemoryEngine, ReplayResult](t, ErrStorageClosed)(RecoverFromWALWithResult(t.Context(), dir, ""))
	require.NoError(t, err)
	require.False(t, result.HasCriticalErrors())
	for _, id := range []NodeID{"test:before", "test:after"} {
		node, err := e.GetNode(t.Context(), id)
		require.NoError(t, err, "recovery must preserve both sides of the audit")
		require.NotEmpty(t, node.Properties["body"])
	}
}

func TestOversizedEmbeddingAuditReaders(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, path := range []string{"b6_upgrade", "current_read", "current_open", "current_recovery", "current_integrity", "current_truncation", "current_compaction", "current_sealing", "current_format_suffix"} {
		t.Run(path, func(t *testing.T) {
			dir := t.TempDir()
			raw := oversizedAuditFixture(t, path != "b6_upgrade")
			file := filepath.Join(dir, "wal.log")
			require.NoError(t, os.WriteFile(file, raw, 0600))
			switch path {
			case "b6_upgrade":
				require.NoError(t, ValidateWALFormat(dir, WALValidateV4Upgrade))
				_, err := UpgradeWALV4(dir, WALV4UpgradeOptions{})
				require.NoError(t, err)
				entries, err := ReadWALEntriesFromDir(t.Context(), dir)
				require.NoError(t, err)
				requireOversizedAuditSuffix(t, entries)
				w, err := NewWAL(dir, nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = w.Close() })
				require.NoError(t, w.ManageSnapshot(t.Context(), 0, WALSnapshotTruncate))
				require.NoError(t, w.Close())
				requireOversizedAuditRecovery(t, dir)
			case "current_open", "current_compaction":
				w, err := NewWAL(dir, nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = w.Close() })
				entries, err := ReadWALEntriesFromDir(t.Context(), dir)
				require.NoError(t, err)
				requireOversizedAuditSuffix(t, entries)
				if path == "current_compaction" {
					require.NoError(t, w.ManageSnapshot(t.Context(), 0, WALSnapshotTruncate))
					info, err := os.Stat(file)
					require.NoError(t, err)
					require.Less(t, info.Size(), int64(walMaxEntrySize))
					entries, err = ReadWALEntriesFromDir(t.Context(), dir)
					require.NoError(t, err)
					requireOversizedAuditSuffix(t, entries)
				} else {
					info, err := os.Stat(file)
					require.NoError(t, err)
					require.EqualValues(t, len(raw), info.Size(), "startup must not truncate the valid audit or suffix")
				}
			case "current_recovery":
				requireOversizedAuditRecovery(t, dir)
			case "current_integrity":
				report, err := CheckWALIntegrity(file)
				require.NoError(t, err)
				require.True(t, report.Healthy, "%v", report.Errors)
				require.Equal(t, 3, report.ValidEntries)
			case "current_truncation":
				entries, err := readWALEntriesForTruncation(file, 0)
				require.NoError(t, err)
				requireOversizedAuditSuffix(t, entries)
			case "current_sealing":
				require.NoError(t, sealWALPropertyEpochFile(file))
			case "current_format_suffix":
				off := int64(0)
				for i := 0; i < 2; i++ {
					off += alignUp(9 + int64(binary.LittleEndian.Uint32(raw[off+5:])) + 4 + 8)
				}
				raw[off+4] = walFormatVersionSpanned + 1
				require.NoError(t, os.WriteFile(file, raw, 0600))
				require.ErrorIs(t, ValidateWALFormat(dir, WALValidateCurrent), ErrWALUnsupportedVersion, "format checks must reach the suffix")
			default:
				entries, err := ReadWALEntries(t.Context(), file, nil)
				require.NoError(t, err)
				requireOversizedAuditSuffix(t, entries)
			}
		})
	}
}

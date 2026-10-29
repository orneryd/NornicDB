package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func priorWALRecord(t *testing.T, version byte, entry WALEntry) []byte {
	t.Helper()
	var payload []byte
	var err error
	if version < 3 {
		payload, err = json.Marshal(entry)
	} else {
		payload, err = processWALEntryPayload(walPayloadEncode, &bytes.Buffer{}, &entry, nil, 0)
	}
	require.NoError(t, err)
	if version >= 2 {
		record, _ := buildAtomicRecord(payload, version)
		return record
	}
	record := make([]byte, 9+len(payload)+4)
	binary.LittleEndian.PutUint32(record, walMagic)
	record[4] = version
	binary.LittleEndian.PutUint32(record[5:], uint32(len(payload)))
	copy(record[9:], payload)
	binary.LittleEndian.PutUint32(record[9+len(payload):], crc32Checksum(payload))
	return record
}

func TestV4UpgradeConvertsHistoricalWALRecords(t *testing.T) {
	stamp := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	entry := WALEntry{Sequence: 8, Timestamp: stamp, Operation: OpTxBegin, Database: "test", Data: []byte(`{"tx_id":"legacy"}`)}
	entry.Checksum = crc32Checksum(entry.Data)
	for _, version := range []byte{0, 1, 2, 3} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			dir := t.TempDir()
			source, dest := filepath.Join(dir, "wal.log"), filepath.Join(dir, "upgraded")
			var old []byte
			if version == 0 {
				var err error
				old, err = json.Marshal(entry)
				require.NoError(t, err)
				old = append(old, '\n')
			} else {
				old = priorWALRecord(t, version, entry)
			}
			require.NoError(t, os.WriteFile(source, old, 0600))
			require.NoError(t, upgradeWALFileV4(source, dest, false))
			unchanged, err := os.ReadFile(source)
			require.NoError(t, err)
			require.Equal(t, old, unchanged)
			upgraded, err := os.ReadFile(dest)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(upgraded), 5)
			require.Equal(t, walFormatVersionCurrent, upgraded[4])
			entries, err := ReadWALEntries(dest, nil)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			require.Equal(t, entry.Sequence, entries[0].Sequence)
			require.True(t, entry.Timestamp.Equal(entries[0].Timestamp))
			require.Equal(t, entry.Operation, entries[0].Operation)
			require.Equal(t, entry.Database, entries[0].Database)
			require.Equal(t, entry.Data, entries[0].Data)
			require.Equal(t, entry.Checksum, entries[0].Checksum)
		})
	}
}

func TestV4UpgradeAcceptsTornActiveMagicPrefixOnly(t *testing.T) {
	for _, prefix := range []string{"W", "WA", "WAL"} {
		t.Run(prefix, func(t *testing.T) {
			dir := t.TempDir()
			source, dest := filepath.Join(dir, "wal.log"), filepath.Join(dir, "converted")
			require.NoError(t, os.WriteFile(source, []byte(prefix), 0600))
			require.Error(t, upgradeWALFileV4(source, dest, false), "sealed segments cannot discard torn records")
			require.NoError(t, upgradeWALFileV4(source, dest, true), "an incomplete active tail can be discarded")
			converted, err := os.ReadFile(dest)
			require.NoError(t, err)
			require.Empty(t, converted)
			original, err := os.ReadFile(source)
			require.NoError(t, err)
			require.Equal(t, []byte(prefix), original)
		})
	}
}

// The first headerless writer used a simple rolling checksum rather than
// CRC32-C. The expected value below is from the original writer's algorithm.
func TestV4UpgradeConvertsOriginalHeaderlessChecksum(t *testing.T) {
	dir := t.TempDir()
	source, dest := filepath.Join(dir, "wal.log"), filepath.Join(dir, "upgraded")
	data := []byte(`{"tx_id":"legacy"}`)
	entry := WALEntry{Sequence: 8, Timestamp: time.Unix(8, 0).UTC(), Operation: OpTxBegin, Data: data, Checksum: 0x227d2204}
	old, err := json.Marshal(entry)
	require.NoError(t, err)
	old = append(old, '\n')
	require.NoError(t, os.WriteFile(source, old, 0600))
	require.NoError(t, upgradeWALFileV4(source, dest, false))
	entries, err := ReadWALEntries(dest, nil)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, data, entries[0].Data)
	require.Equal(t, crc32Checksum(data), entries[0].Checksum)
	unchanged, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, old, unchanged)
}

func TestV4UpgradeRejectsCorruptOriginalHeaderlessChecksum(t *testing.T) {
	dir := t.TempDir()
	source, dest := filepath.Join(dir, "wal.log"), filepath.Join(dir, "upgraded")
	entry := WALEntry{Sequence: 8, Timestamp: time.Unix(8, 0).UTC(), Operation: OpTxBegin, Data: []byte(`{"tx_id":"legacy"}`), Checksum: 0x227d2205}
	old, err := json.Marshal(entry)
	require.NoError(t, err)
	old = append(old, '\n')
	require.NoError(t, os.WriteFile(source, old, 0600))
	require.ErrorIs(t, upgradeWALFileV4(source, dest, false), ErrWALChecksumFailed)
	_, err = os.Stat(dest)
	require.True(t, os.IsNotExist(err))
	unchanged, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, old, unchanged)
}

func TestV4UpgradeRejectsUnsealedEpochInOldDataFrame(t *testing.T) {
	dir := t.TempDir()
	source, dest := filepath.Join(dir, "wal.log"), filepath.Join(dir, "upgraded")
	epoch := &WALPropertyEpoch{state: &walPropertyEpochState{Version: 1, ID: "epoch", DictionaryID: "dictionary", Bindings: []byte{1}}}
	frame := walPropertyEpochFrame{Format: 1, Epoch: epoch, Through: "ffffffffffffffff", Last: "ffffffffffffffff"}
	payload, err := encodeWALPropertyEpochFrame(frame, 1, time.Unix(8, 0).UTC())
	require.NoError(t, err)
	old, _ := buildAtomicRecord(payload, 3)
	require.NoError(t, os.WriteFile(source, old, 0600))
	require.Error(t, upgradeWALFileV4(source, dest, false), "old data framing could not carry an unsealed property epoch")
	_, err = os.Stat(dest)
	require.True(t, os.IsNotExist(err))
	unchanged, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, old, unchanged)
}

func TestV4UpgradePreservesUnsealedVersion4Epoch(t *testing.T) {
	dir := t.TempDir()
	source, dest := filepath.Join(dir, "wal.log"), filepath.Join(dir, "upgraded")
	epoch := &WALPropertyEpoch{state: &walPropertyEpochState{Version: 1, ID: "epoch", DictionaryID: "dictionary", Bindings: []byte{1}}}
	frame := walPropertyEpochFrame{Format: 1, Epoch: epoch, Through: "ffffffffffffffff", Last: "ffffffffffffffff"}
	payload, err := encodeWALPropertyEpochFrame(frame, 1, time.Unix(8, 0).UTC())
	require.NoError(t, err)
	control, _ := buildAtomicRecord(payload, 4)
	entry := WALEntry{Sequence: 2, Timestamp: time.Unix(9, 0).UTC(), Operation: OpBufferedCommit, Data: []byte(`{"tx_id":"b"}`)}
	entry.Checksum = crc32Checksum(entry.Data)
	old := append(control, priorWALRecord(t, 3, entry)...)
	require.NoError(t, os.WriteFile(source, old, 0600))
	require.NoError(t, upgradeWALFileV4(source, dest, false))
	entries, err := ReadWALEntries(dest, nil)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.NotNil(t, entries[0].PropertyEpoch)
	require.Equal(t, "dictionary", entries[0].PropertyEpoch.state.DictionaryID)
}

func TestV4UpgradeRemapsHistoricalPropertyEpochBounds(t *testing.T) {
	dir := t.TempDir()
	source, dest := filepath.Join(dir, "wal.log"), filepath.Join(dir, "upgraded")
	stamp := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	first := WALEntry{Sequence: 1, Timestamp: stamp, Operation: OpTxBegin, Data: []byte(`{"tx_id":"a"}`)}
	first.Checksum = crc32Checksum(first.Data)
	second := WALEntry{Sequence: 2, Timestamp: stamp, Operation: OpBufferedCommit, Data: []byte(`{"tx_id":"b"}`)}
	second.Checksum = crc32Checksum(second.Data)
	before, after := priorWALRecord(t, 2, first), priorWALRecord(t, 3, second)
	epoch := &WALPropertyEpoch{state: &walPropertyEpochState{Version: 1, ID: "epoch", DictionaryID: "dictionary", Bindings: []byte{1}}}
	frame := walPropertyEpochFrame{Format: 1, Epoch: epoch, Through: "0000000000000000", Last: "0000000000000002"}
	payload, err := encodeWALPropertyEpochFrame(frame, 1, stamp)
	require.NoError(t, err)
	control, _ := buildAtomicRecord(payload, 4)
	frame.Through = fmt.Sprintf("%016x", len(before)+len(control)+len(after))
	payload, err = encodeWALPropertyEpochFrame(frame, 1, stamp)
	require.NoError(t, err)
	control, _ = buildAtomicRecord(payload, 4)
	old := append(append(before, control...), after...)
	require.NoError(t, os.WriteFile(source, old, 0600))
	require.NoError(t, upgradeWALFileV4(source, dest, false))
	entries, err := ReadWALEntries(dest, nil)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.NotNil(t, entries[1].PropertyEpoch, "the old authority covers the later record after offset remapping")
	require.Equal(t, "dictionary", entries[1].PropertyEpoch.state.DictionaryID)
}

func TestV4UpgradeRefusesCorruptOrUnknownWALWithoutReplacingIt(t *testing.T) {
	entry := WALEntry{Sequence: 1, Timestamp: time.Now().UTC(), Operation: OpTxBegin, Data: []byte(`{"tx_id":"a"}`)}
	entry.Checksum = crc32Checksum(entry.Data)
	valid := priorWALRecord(t, 3, entry)
	for _, variant := range []string{"truncated", "bad-crc", "bad-trailer", "future"} {
		t.Run(variant, func(t *testing.T) {
			dir := t.TempDir()
			source, dest := filepath.Join(dir, "wal.log"), filepath.Join(dir, "upgraded")
			raw := bytes.Clone(valid)
			switch variant {
			case "truncated":
				raw = raw[:len(raw)-1]
			case "bad-crc":
				raw[10] ^= 1
			case "bad-trailer":
				raw[len(raw)-8] ^= 1
			case "future":
				raw[4] = walFormatVersionCurrent + 1
			}
			require.NoError(t, os.WriteFile(source, raw, 0600))
			require.Error(t, upgradeWALFileV4(source, dest, false))
			after, err := os.ReadFile(source)
			require.NoError(t, err)
			require.Equal(t, raw, after)
			_, err = os.Stat(dest)
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestV4UpgradeRefusesAmbiguousHistoricalBinaryKeys(t *testing.T) {
	dir := t.TempDir()
	source, dest := filepath.Join(dir, "wal.log"), filepath.Join(dir, "upgraded")
	// The old JSON writer replaces an invalid UTF-8 physical key with U+FFFD.
	// The missing bytes cannot be recovered from that projection.
	payload, err := json.Marshal(WALBufferedCommitData{TxID: "lost", Writes: map[string][]byte{string([]byte{prefixNode, 0xff}): {1}}})
	require.NoError(t, err)
	entry := WALEntry{Sequence: 1, Timestamp: time.Now().UTC(), Operation: OpBufferedCommit, Data: payload, Checksum: crc32Checksum(payload)}
	raw := priorWALRecord(t, 3, entry)
	require.NoError(t, os.WriteFile(source, raw, 0600))
	require.ErrorContains(t, upgradeWALFileV4(source, dest, false), "physical key")
	after, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, raw, after)
	_, err = os.Stat(dest)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestV4UpgradeMigratesCompletePrefixOfTornActive(t *testing.T) {
	dir := t.TempDir()
	source, dest := filepath.Join(dir, "wal.log"), filepath.Join(dir, "upgraded")
	entry := WALEntry{Sequence: 1, Timestamp: time.Now().UTC(), Operation: OpTxBegin, Data: []byte(`{"tx_id":"a"}`)}
	entry.Checksum = crc32Checksum(entry.Data)
	good := priorWALRecord(t, 3, entry)
	old := append(bytes.Clone(good), good[:len(good)/2]...)
	require.NoError(t, os.WriteFile(source, old, 0600))
	require.NoError(t, upgradeWALFileV4(source, dest, true))
	entries, err := ReadWALEntries(dest, nil)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, uint64(1), entries[0].Sequence)
	after, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, old, after)
}

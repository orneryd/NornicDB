package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

type epochFailWriter struct{}

func (epochFailWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func epochValidationRecord(t *testing.T, data []byte, version uint8) []byte {
	t.Helper()
	var buffer bytes.Buffer
	payload, err := encodeWALEntryV3(&buffer, &WALEntry{Operation: OpCheckpoint, Data: data, Checksum: crc32Checksum(data)})
	require.NoError(t, err)
	record, _ := buildAtomicRecord(payload, version)
	return record
}

func TestWALPropertyEpochMalformedAuthority(t *testing.T) {
	valid := `{"nornic_property_epoch":1,"authority":null,"through":"ffffffffffffffff","last":"ffffffffffffffff"}`
	for name, data := range map[string]string{
		"json":              `{"nornic_property_epoch":1,`,
		"format":            `{"nornic_property_epoch":1,"nornic_property_epoch":9,"authority":null,"through":"1","last":"1"}`,
		"authority type":    `{"nornic_property_epoch":1,"authority":true,"through":"1","last":"1"}`,
		"authority missing": `{"nornic_property_epoch":1,"authority":{},"through":"1","last":"1"}`,
		"byte span":         `{"nornic_property_epoch":1,"authority":null,"through":"gggggggggggggggg","last":"0000000000000001"}`,
		"sequence span":     `{"nornic_property_epoch":1,"authority":null,"through":"0000000000000001","last":"gggggggggggggggg"}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wal.log")
			raw := epochValidationRecord(t, []byte(data), walFormatVersionPropertyEpoch)
			if name == "byte span" || name == "sequence span" {
				// Keep the actual writer's fixed-width framing: failure must
				// come from invalid authority, not a changed frame byte length.
				var frame walPropertyEpochFrame
				require.NoError(t, json.Unmarshal([]byte(data), &frame))
				payload, err := encodeWALPropertyEpochFrame(frame, 0, time.Unix(0, 0))
				require.NoError(t, err)
				raw, _ = buildAtomicRecord(payload, walFormatVersionPropertyEpoch)
			}
			require.NoError(t, os.WriteFile(path, raw, 0600))
			_, err := ReadWALEntries(path)
			require.Error(t, err)
			require.Error(t, sealWALPropertyEpochFile(path), "clean conversion must use the same authority validator")
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, raw, after)
		})
	}
	path := filepath.Join(t.TempDir(), "wal.log")
	require.NoError(t, os.WriteFile(path, epochValidationRecord(t, []byte(valid), walFormatVersionV3), 0600))
	_, err := ReadWALEntries(path)
	require.ErrorContains(t, err, "requires framing version 4")
	_, err = decodeWALPropertyEpochFrame(WALEntry{Operation: OpCreateNode})
	require.Error(t, err)
	_, err = encodeWALPropertyEpochFrame(walPropertyEpochFrame{}, 0, time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	_, err = encodeWALPropertyEpochFrame(walPropertyEpochFrame{Epoch: &WALPropertyEpoch{state: &walPropertyEpochState{Bindings: make([]byte, walMaxEntrySize)}}}, 0, time.Now())
	require.ErrorContains(t, err, "maximum frame size", "an oversized dictionary must fail before writing an unreadable frame")
}

func TestWALPropertyEpochSealingRejectsDamagedFrames(t *testing.T) {
	data := []byte(`{"nornic_property_epoch":1,"authority":null,"through":"ffffffffffffffff","last":"ffffffffffffffff"}`)
	good := epochValidationRecord(t, data, walFormatVersionPropertyEpoch)
	for _, kind := range []string{"header", "size", "version", "payload", "crc", "trailer", "inner crc", "v4 ordinary", "invalid payload"} {
		t.Run(kind, func(t *testing.T) {
			raw := bytes.Clone(good)
			size := int(binary.LittleEndian.Uint32(raw[5:9]))
			switch kind {
			case "header":
				raw = raw[:6]
			case "size":
				binary.LittleEndian.PutUint32(raw[5:9], walMaxEntrySize+1)
			case "version":
				raw[4] = walFormatVersionPropertyEpoch + 1
			case "payload":
				raw = raw[:12]
			case "crc":
				raw[9+size] ^= 1
			case "trailer":
				raw[13+size] ^= 1
			case "inner crc":
				raw[9+size-1] ^= 1
				binary.LittleEndian.PutUint32(raw[9+size:], crc32Checksum(raw[9:9+size]))
			case "v4 ordinary":
				raw = epochValidationRecord(t, []byte(`{}`), walFormatVersionPropertyEpoch)
			case "invalid payload":
				payload := []byte("invalid")
				raw = buildAtomicRecordRaw(payload, walFormatVersionV3, crc32Checksum(payload), walTrailer, true)
			}
			path := filepath.Join(t.TempDir(), "wal.log")
			require.NoError(t, os.WriteFile(path, raw, 0600))
			require.Error(t, sealWALPropertyEpochFile(path))
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, raw, after)
		})
	}
	path := filepath.Join(t.TempDir(), "wal.log")
	require.Error(t, sealWALPropertyEpochFile(path))
	for _, data := range [][]byte{nil, []byte("{}\n")} {
		require.NoError(t, os.WriteFile(path, data, 0600))
		if len(data) == 0 {
			require.NoError(t, sealWALPropertyEpochFile(path))
		} else {
			require.Error(t, sealWALPropertyEpochFile(path))
		}
	}
	require.NoError(t, os.WriteFile(path, []byte("{\"seq\":1}\n"), 0600))
	require.NoError(t, sealWALPropertyEpochFile(path), "historical JSON is not rewritten")
}

func TestWALPropertyEpochSalvageCannotBorrowAcrossDamage(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source, wal, commit := bindingCostFixture(t, 1)
	require.NoError(t, wal.AppendBufferedCommit(commit, source.propKeyDict))
	require.NoError(t, wal.Close())
	raw, err := os.ReadFile(walActivePath(wal.Config().Dir))
	require.NoError(t, err)
	firstSize := int(binary.LittleEndian.Uint32(raw[5:9]))
	firstEnd := int(alignUp(int64(9 + firstSize + 4 + 8)))
	malformed, _ := buildAtomicRecord([]byte("bad"), walFormatVersionV3)
	path := filepath.Join(t.TempDir(), "wal.log")
	require.NoError(t, os.WriteFile(path, append(append(bytes.Clone(raw[:firstEnd]), malformed...), raw[firstEnd:]...), 0600))
	file, err := os.Open(path)
	require.NoError(t, err)
	entries, err := readAtomicWALEntriesForTruncation(file, 0)
	require.NoError(t, file.Close())
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Nil(t, entries[0].PropertyEpoch)
	dest, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = dest.Close() })
	require.ErrorContains(t, ReplayWALEntry(dest, entries[0]), "missing property name binding")
	for _, legacy := range []bool{false, true} {
		entry, err := decodeWALEntry(raw[9:9+firstSize], raw[4])
		require.NoError(t, err)
		entry.Checksum++
		var data []byte
		if legacy {
			data, err = json.Marshal(entry)
			require.NoError(t, err)
		} else {
			data = bytes.Clone(raw[:firstEnd])
			data[9+firstSize] ^= 1
		}
		require.NoError(t, os.WriteFile(path, data, 0600))
		file, err := os.Open(path)
		require.NoError(t, err)
		if legacy {
			_, err = readLegacyWALEntriesForTruncation(file, 0)
		} else {
			_, err = readAtomicWALEntriesForTruncation(file, 0)
		}
		require.Error(t, err, "a damaged control must stop salvage")
		require.NoError(t, file.Close())
	}
}

func TestWALPropertyEpochCannotAcknowledgeUnboundOrFailedHeader(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source, wal, commit := bindingCostFixture(t, 1)
	require.ErrorContains(t, wal.AppendBufferedCommit(commit, nil), "dictionary is required")
	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	require.Empty(t, entries, "a failed binding must precede any acknowledged body")
	wal.mu.Lock()
	writer := wal.writer
	wal.writer = bufio.NewWriterSize(epochFailWriter{}, 1)
	err = wal.ensurePropertyEpochLocked(source.propKeyDict, true)
	wal.writer = writer
	wal.mu.Unlock()
	require.ErrorIs(t, err, io.ErrClosedPipe)
	// Attachment has no error return, so failed metadata must remain sticky
	// until the next commit can return it to its caller before acknowledgment.
	wal.bindPropertyDictionary(newPropertyKeyDictionary())
	require.ErrorContains(t, wal.AppendBufferedCommit(commit, source.propKeyDict), "identity is uninitialized")
	require.NoError(t, wal.Close())
	wal.mu.Lock()
	wal.propertyEpochError = nil
	err = wal.ensurePropertyEpochLocked(source.propKeyDict, false)
	wal.mu.Unlock()
	require.ErrorIs(t, err, ErrWALClosed)
	require.ErrorIs(t, wal.invalidateCleanPropertyEpoch(), ErrWALClosed)
	var previous *WALPropertyEpoch
	epoch, _, err := source.propKeyDict.captureWALEpoch()
	require.NoError(t, err)
	wal.mu.Lock()
	_, err = wal.writeRetainedPropertyEpochLocked(bufio.NewWriterSize(epochFailWriter{}, 1), &previous, WALEntry{Operation: OpBufferedCommit, PropertyEpoch: epoch})
	wal.mu.Unlock()
	require.ErrorIs(t, err, io.ErrClosedPipe)
	require.Nil(t, previous)
	closed := reviewBindingEngine(t, t.TempDir())
	require.NoError(t, closed.Close())
	closed.SetBufferedCommitWAL(wal)
	require.ErrorIs(t, closed.beginSemanticPropertyRestore(), ErrStorageClosed)
	_, _, err = RecoverIntoEngine(closed, wal.Config().Dir, "")
	require.ErrorIs(t, err, ErrStorageClosed)
	failedSource, failedWAL, failedCommit := bindingCostFixture(t, 1)
	require.NoError(t, failedWAL.file.Close())
	failedWAL.bindPropertyDictionary(failedSource.propKeyDict)
	require.ErrorIs(t, failedWAL.AppendBufferedCommit(failedCommit, failedSource.propKeyDict), os.ErrClosed)
	raw, err := os.ReadFile(walActivePath(failedWAL.Config().Dir))
	require.NoError(t, err)
	require.Empty(t, raw, "attachment flush failure must never acknowledge the following body")
}

func TestWALPropertyEpochPlanSurvivesPartialDrainPersistence(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source, wal, commit := bindingCostFixture(t, 160)
	require.NoError(t, wal.AppendBufferedCommit(commit, source.propKeyDict))
	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	data, err := decodeBufferedWALRecord(entries[0])
	require.NoError(t, err)
	dest, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = dest.Close() })
	prepared, err := dest.importBufferedWALRecord(&data, walForeignImport)
	require.NoError(t, err)
	require.Len(t, prepared.propKeyDrain.pending, 160)
	_, err = dest.CreateNode(&Node{ID: "perf:independent", Properties: map[string]any{"property_000": int64(0)}})
	require.NoError(t, err)
	remaining, err := dest.importBufferedWALRecord(&data, walForeignImport)
	require.NoError(t, err)
	require.Len(t, remaining.propKeyDrain.pending, 159)
	require.Len(t, prepared.propKeyDrain.pending, 160, "a shared earlier drain stays immutable")
	require.NoError(t, ReplayWALEntry(dest, entries[0]))
	assertPropertyDictionaryBijection(t, dest)
	node, err := dest.GetNode("perf:a")
	require.NoError(t, err)
	require.Len(t, node.Properties, 160)
}

func TestWALPropertyEpochUnappliedLogCannotGainCleanProof(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, record := range []map[string]any{{}, {"tx_id": "never-applied"}, {"tx_id": []string{"invalid"}}} {
		dir := t.TempDir()
		dest := reviewBindingEngine(t, dir)
		require.NoError(t, dest.beginSemanticPropertyRestore())
		wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
		require.NoError(t, err)
		dest.SetBufferedCommitWAL(wal)
		require.NoError(t, wal.Append(OpBufferedCommit, record))
		require.Error(t, dest.Close())
		require.NoError(t, wal.Close())
		_, versions := walEpochRawBodies(t, walActivePath(wal.Config().Dir))
		require.Contains(t, versions, walFormatVersionPropertyEpoch, "failed clean shutdown preserves the older-reader refusal")
		dest = reviewBindingEngine(t, dir)
		require.True(t, dest.walPropertySource.foreign)
		require.False(t, dest.walPropertySource.clean)
	}
}

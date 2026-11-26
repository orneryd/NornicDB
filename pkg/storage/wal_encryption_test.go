package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// The encrypted journal is opt-in through WALConfig.EncryptionKey. These tests
// drive the public entries the server layer uses to write, validate, read and
// recover it, plus the one-time plaintext conversion the operator runs before
// NewWAL opens a journal with a key.

// encryptedWALTestKey returns a deterministic 32-byte WAL key.
func encryptedWALTestKey(seed byte) []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = seed*31 + byte(i)*7 + 3
	}
	return key
}

func encryptedWALTestConfig(dir string, key []byte) *WALConfig {
	cfg := DefaultWALConfig()
	cfg.Dir = dir
	cfg.SyncMode = "immediate"
	cfg.EncryptionKey = key
	return cfg
}

// appendEncryptedTestEntry appends one checkpoint record whose JSON payload
// holds marker as plain text, so a test can prove the marker does not appear in
// the file.
func appendEncryptedTestEntry(t *testing.T, w *WAL, marker string) uint64 {
	t.Helper()
	seq, err := w.AppendWithDatabaseReturningSeq(OpCheckpoint, map[string]any{"token": marker}, "test")
	require.NoError(t, err)
	return seq
}

// encryptedWALFrame is one physical frame of a WAL file, parsed and decrypted.
type encryptedWALFrame struct {
	version   uint8
	stored    []byte // payload as stored: nonce+ciphertext, or plaintext
	plaintext []byte // payload after decryption
	end       int64  // aligned end offset
}

// readEncryptedWALFrames parses every physical frame of one WAL file through
// the same reader the WAL uses, so the helpers below check the bytes on disk
// and the format the readers accept together.
func readEncryptedWALFrames(t *testing.T, path string, cipher *walFrameCipher) []encryptedWALFrame {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	reader := walFrameReader{cipher: cipher}
	var frames []encryptedWALFrame
	for offset := int64(0); offset < int64(len(raw)); {
		require.GreaterOrEqual(t, int64(len(raw))-offset, int64(9), "frame header at %d", offset)
		require.Equal(t, walMagic, binary.LittleEndian.Uint32(raw[offset:]))
		version := raw[offset+4]
		size := int64(binary.LittleEndian.Uint32(raw[offset+5:]))
		frameLen := alignUp(9 + size + 4 + 8)
		require.GreaterOrEqual(t, int64(len(raw))-offset, frameLen, "complete frame at %d", offset)
		stored := raw[offset+9 : offset+9+size]
		crcOffset := offset + 9 + size
		require.Equal(t, crc32Checksum(stored), binary.LittleEndian.Uint32(raw[crcOffset:]), "outer CRC of the stored payload at %d", offset)
		require.Equal(t, walTrailer, binary.LittleEndian.Uint64(raw[crcOffset+4:]), "trailer at %d", offset)
		plaintext, _, err := reader.plaintext(stored, version)
		require.NoError(t, err, "frame at %d", offset)
		frames = append(frames, encryptedWALFrame{version: version, stored: stored, plaintext: plaintext, end: offset + frameLen})
		offset += frameLen
	}
	return frames
}

func encryptedWALFrameEntries(t *testing.T, frames []encryptedWALFrame) []WALEntry {
	t.Helper()
	entries := make([]WALEntry, 0, len(frames))
	for _, frame := range frames {
		var entry WALEntry
		_, err := processWALEntryPayload(walPayloadDecode, nil, &entry, frame.plaintext, walPlaintextFrameVersion(frame.version))
		require.NoError(t, err)
		entries = append(entries, entry)
	}
	return entries
}

func encryptedWALSequences(entries []WALEntry) []uint64 {
	seqs := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		seqs = append(seqs, entry.Sequence)
	}
	return seqs
}

// A key that is present must be exactly 32 bytes. Absent, the journal keeps its
// existing plaintext format.
func TestEncryptedWALConfigRequiresExactKeyLength(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, length := range []int{1, 16, 31, 33, 64} {
		t.Run(fmt.Sprintf("key-bytes-%d", length), func(t *testing.T) {
			dir := t.TempDir()
			wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "none", EncryptionKey: make([]byte, length)})
			require.ErrorContains(t, err, "32-byte encryption key")
			require.Nil(t, wal)
			require.NoFileExists(t, walActivePath(dir))
		})
	}

	t.Run("absent key keeps plaintext frames", func(t *testing.T) {
		dir := t.TempDir()
		wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
		require.NoError(t, err)
		appendEncryptedTestEntry(t, wal, "plaintext-record")
		require.NoError(t, wal.Close())

		frames := readEncryptedWALFrames(t, walActivePath(dir), nil)
		require.Len(t, frames, 1)
		require.Equal(t, walFormatVersionCurrent, frames[0].version)
		entries, err := ReadWALEntriesFromDir(t.Context(), dir)
		require.NoError(t, err)
		require.Len(t, entries, 1)
	})
}

// NewWAL with a key writes authenticated frames only: version 8 payloads are
// nonce+ciphertext under the current CRC/trailer envelope, the plaintext record
// is absent from the file, and only a reader holding the key recovers it.
func TestEncryptedWALWritesOnlyAuthenticatedFrames(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	key := encryptedWALTestKey(1)
	wal, err := NewWAL(dir, encryptedWALTestConfig(dir, key))
	require.NoError(t, err)
	first := appendEncryptedTestEntry(t, wal, "wal-encryption-secret-one")
	second := appendEncryptedTestEntry(t, wal, "wal-encryption-secret-two")
	require.NoError(t, wal.Close())

	active := walActivePath(dir)
	raw, err := os.ReadFile(active)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "wal-encryption-secret-one")
	require.NotContains(t, string(raw), "wal-encryption-secret-two")

	cipher, err := newWALFrameCipher(key)
	require.NoError(t, err)
	frames := readEncryptedWALFrames(t, active, cipher)
	require.Len(t, frames, 2)
	for i, frame := range frames {
		require.Equal(t, walFormatVersionEncrypted, frame.version, "frame %d", i)
		require.Len(t, frame.stored, len(frame.plaintext)+walEncryptedFrameOverhead)
		require.Contains(t, string(frame.plaintext), []string{"wal-encryption-secret-one", "wal-encryption-secret-two"}[i])
	}
	require.Equal(t, []uint64{first, second}, encryptedWALSequences(encryptedWALFrameEntries(t, frames)))

	entries, err := ReadWALEntriesFromDirWithKey(t.Context(), dir, key)
	require.NoError(t, err)
	require.Equal(t, []uint64{first, second}, encryptedWALSequences(entries))
	single, err := ReadWALEntriesWithKey(t.Context(), active, nil, key)
	require.NoError(t, err)
	require.Equal(t, encryptedWALSequences(entries), encryptedWALSequences(single))
	visited := 0
	require.NoError(t, VisitWALEntriesAfterFromDirWithKey(t.Context(), dir, 0, key, func(WALEntry) error {
		visited++
		return nil
	}))
	require.Equal(t, 2, visited)
	after, err := ReadWALEntriesAfterWithKey(t.Context(), dir, first, WALReadDirectory, key)
	require.NoError(t, err)
	require.Equal(t, []uint64{second}, encryptedWALSequences(after))

	// The keyed journal continues at the sequence it holds.
	wal, err = NewWAL(dir, encryptedWALTestConfig(dir, key))
	require.NoError(t, err)
	require.Equal(t, second, wal.Sequence())
	require.Equal(t, second+1, appendEncryptedTestEntry(t, wal, "wal-encryption-secret-three"))
	require.NoError(t, wal.Close())
	entries, err = ReadWALEntriesFromDirWithKey(t.Context(), dir, key)
	require.NoError(t, err)
	require.Len(t, entries, 3)
}

// A journal written with one key is refused, without any rewrite, by a reader
// without a key and by a reader with a different key.
func TestEncryptedWALRefusesWrongOrMissingKeyWithoutRepair(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	key := encryptedWALTestKey(2)
	wal, err := NewWAL(dir, encryptedWALTestConfig(dir, key))
	require.NoError(t, err)
	appendEncryptedTestEntry(t, wal, "wal-encryption-secret")
	require.NoError(t, wal.Close())
	active := walActivePath(dir)
	before, err := os.ReadFile(active)
	require.NoError(t, err)

	t.Run("missing key", func(t *testing.T) {
		wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "none"})
		require.ErrorIs(t, err, ErrWALKeyRequired)
		require.Nil(t, wal)
		require.ErrorIs(t, ValidateWALFormat(dir, WALValidateCurrent), ErrWALKeyRequired)
		require.ErrorIs(t, ValidateWALFormatWithKey(dir, WALValidateCurrent, nil), ErrWALKeyRequired)
		_, err = ReadWALEntriesFromDir(t.Context(), dir)
		require.ErrorIs(t, err, ErrWALKeyRequired)
		_, err = ReadWALEntries(t.Context(), active, nil)
		require.ErrorIs(t, err, ErrWALKeyRequired)
		_, err = CheckWALIntegrity(active)
		require.ErrorIs(t, err, ErrWALKeyRequired)
		_, err = CheckWALIntegrityWithKey(active, nil)
		require.ErrorIs(t, err, ErrWALKeyRequired)
		after, readErr := os.ReadFile(active)
		require.NoError(t, readErr)
		require.Equal(t, before, after, "a missing key must never rewrite the journal")
	})

	t.Run("wrong key", func(t *testing.T) {
		wrong := encryptedWALTestKey(3)
		wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "none", EncryptionKey: wrong})
		require.ErrorIs(t, err, ErrWALCorrupted)
		require.Nil(t, wal)
		require.ErrorIs(t, ValidateWALFormatWithKey(dir, WALValidateCurrent, wrong), ErrWALCorrupted)
		_, err = ReadWALEntriesFromDirWithKey(t.Context(), dir, wrong)
		require.ErrorIs(t, err, ErrWALCorrupted)
		_, err = CheckWALIntegrityWithKey(active, wrong)
		require.ErrorIs(t, err, ErrWALCorrupted)
		after, readErr := os.ReadFile(active)
		require.NoError(t, readErr)
		require.Equal(t, before, after, "a wrong key must never rewrite the journal")
	})
}

// Authenticated ciphertext that is altered is corruption, never a record that a
// reader may skip or repair away.
func TestEncryptedWALTamperedFrameIsRefusedWithoutRepair(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	key := encryptedWALTestKey(4)
	wal, err := NewWAL(dir, encryptedWALTestConfig(dir, key))
	require.NoError(t, err)
	appendEncryptedTestEntry(t, wal, "wal-encryption-first")
	appendEncryptedTestEntry(t, wal, "wal-encryption-second")
	require.NoError(t, wal.Close())
	active := walActivePath(dir)

	cipher, err := newWALFrameCipher(key)
	require.NoError(t, err)
	frames := readEncryptedWALFrames(t, active, cipher)
	require.Len(t, frames, 2)
	raw, err := os.ReadFile(active)
	require.NoError(t, err)
	// Flip one ciphertext byte of the first frame and repair its outer CRC, so
	// the envelope the format check reads is consistent and only the
	// authenticated payload can notice the change.
	firstSize := int64(binary.LittleEndian.Uint32(raw[5:]))
	raw[9+walEncryptedFrameOverhead] ^= 0xff
	binary.LittleEndian.PutUint32(raw[9+firstSize:], crc32Checksum(raw[9:9+firstSize]))
	require.NoError(t, os.WriteFile(active, raw, 0600))

	wal, err = NewWAL(dir, encryptedWALTestConfig(dir, key))
	require.ErrorIs(t, err, ErrWALCorrupted)
	require.Nil(t, wal)
	require.ErrorIs(t, ValidateWALFormatWithKey(dir, WALValidateCurrent, key), ErrWALCorrupted)
	_, err = ReadWALEntriesFromDirWithKey(t.Context(), dir, key)
	require.ErrorIs(t, err, ErrWALCorrupted)
	require.ErrorIs(t, VisitWALEntriesAfterFromDirWithKey(t.Context(), dir, 0, key, func(WALEntry) error { return nil }), ErrWALCorrupted)
	_, err = CheckWALIntegrityWithKey(active, key)
	require.ErrorIs(t, err, ErrWALCorrupted)
	after, err := os.ReadFile(active)
	require.NoError(t, err)
	require.Equal(t, raw, after, "an authentication failure is never truncated or rewritten")
}

// A torn tail is the one damage keyed bootstrap repairs: like a plaintext
// journal, the incomplete frame is cut back to the last complete one.
func TestEncryptedWALRepairsOnlyTornTailWithKey(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	key := encryptedWALTestKey(5)
	wal, err := NewWAL(dir, encryptedWALTestConfig(dir, key))
	require.NoError(t, err)
	first := appendEncryptedTestEntry(t, wal, "wal-encryption-kept")
	appendEncryptedTestEntry(t, wal, "wal-encryption-torn")
	require.NoError(t, wal.Close())
	active := walActivePath(dir)

	cipher, err := newWALFrameCipher(key)
	require.NoError(t, err)
	frames := readEncryptedWALFrames(t, active, cipher)
	require.Len(t, frames, 2)
	cut := frames[1].end - 12
	require.NoError(t, os.Truncate(active, cut))

	// Without the key the torn journal is refused as ciphertext and left alone.
	before, err := os.ReadFile(active)
	require.NoError(t, err)
	_, err = NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "none"})
	require.ErrorIs(t, err, ErrWALKeyRequired)
	after, err := os.ReadFile(active)
	require.NoError(t, err)
	require.Equal(t, before, after)

	wal, err = NewWAL(dir, encryptedWALTestConfig(dir, key))
	require.NoError(t, err)
	require.Equal(t, first, wal.Sequence())
	entries, err := ReadWALEntriesFromDirWithKey(t.Context(), dir, key)
	require.NoError(t, err)
	require.Equal(t, []uint64{first}, encryptedWALSequences(entries))
	require.Equal(t, first+1, appendEncryptedTestEntry(t, wal, "wal-encryption-after-repair"))
	require.NoError(t, wal.Close())

	entries, err = ReadWALEntriesFromDirWithKey(t.Context(), dir, key)
	require.NoError(t, err)
	require.Equal(t, []uint64{first, first + 1}, encryptedWALSequences(entries))
}

// Rotation seals encrypted segments and retention removes covered ones; both
// the sealed and the active files stay readable only with the key.
func TestEncryptedWALRotationAndRetention(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())

	open := func(t *testing.T, retention int) (string, []byte, []uint64) {
		t.Helper()
		dir := t.TempDir()
		key := encryptedWALTestKey(6)
		cfg := encryptedWALTestConfig(dir, key)
		cfg.MaxEntries = 1
		cfg.RetentionMaxSegments = retention
		wal, err := NewWAL(dir, cfg)
		require.NoError(t, err)
		var seqs []uint64
		for _, marker := range []string{"wal-encryption-seg-one", "wal-encryption-seg-two", "wal-encryption-seg-three"} {
			seqs = append(seqs, appendEncryptedTestEntry(t, wal, marker))
		}
		require.NoError(t, wal.Close())
		return dir, key, seqs
	}

	t.Run("rotation", func(t *testing.T) {
		dir, key, seqs := open(t, 0)
		manifest, err := accessWALManifest(dir, walReadManifest, nil)
		require.NoError(t, err)
		require.Len(t, manifest.Segments, 3)
		cipher, err := newWALFrameCipher(key)
		require.NoError(t, err)
		for _, segment := range manifest.Segments {
			path, err := resolveWALSegmentPath(dir, segment.Path)
			require.NoError(t, err)
			frames := readEncryptedWALFrames(t, path, cipher)
			require.NotEmpty(t, frames)
			for _, frame := range frames {
				require.Equal(t, walFormatVersionEncrypted, frame.version)
			}
		}
		entries, err := ReadWALEntriesFromDirWithKey(t.Context(), dir, key)
		require.NoError(t, err)
		require.Equal(t, seqs, encryptedWALSequences(entries))
		require.ErrorIs(t, ValidateWALFormat(dir, WALValidateCurrent), ErrWALKeyRequired)
		require.NoError(t, ValidateWALFormatWithKey(dir, WALValidateCurrent, key))
		report, err := CheckWALIntegrityWithKey(walActivePath(dir), key)
		require.NoError(t, err)
		require.True(t, report.Healthy)
	})

	t.Run("retention", func(t *testing.T) {
		dir, key, seqs := open(t, 1)
		cfg := encryptedWALTestConfig(dir, key)
		cfg.MaxEntries = 1
		cfg.RetentionMaxSegments = 1
		wal, err := NewWAL(dir, cfg)
		require.NoError(t, err)
		require.NoError(t, wal.ManageSnapshot(t.Context(), seqs[2], WALSnapshotRetention))
		require.NoError(t, wal.Close())

		manifest, err := accessWALManifest(dir, walReadManifest, nil)
		require.NoError(t, err)
		require.Len(t, manifest.Segments, 1)
		entries, err := ReadWALEntriesFromDirWithKey(t.Context(), dir, key)
		require.NoError(t, err)
		require.Equal(t, []uint64{seqs[2]}, encryptedWALSequences(entries))
		require.NoError(t, ValidateWALFormatWithKey(dir, WALValidateCurrent, key))

		wal, err = NewWAL(dir, cfg)
		require.NoError(t, err)
		require.Equal(t, seqs[2], wal.Sequence())
		require.NoError(t, wal.Close())
	})
}

// Snapshot truncation rewrites the active journal; the replacement holds only
// the retained records, each one an encrypted frame.
func TestEncryptedWALSnapshotTruncationRewritesEncryptedFrames(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	key := encryptedWALTestKey(7)
	cfg := encryptedWALTestConfig(dir, key)
	wal, err := NewWAL(dir, cfg)
	require.NoError(t, err)
	var seqs []uint64
	for _, marker := range []string{"wal-encryption-cut-one", "wal-encryption-cut-two", "wal-encryption-keep-three", "wal-encryption-keep-four"} {
		seqs = append(seqs, appendEncryptedTestEntry(t, wal, marker))
	}
	require.NoError(t, wal.ManageSnapshot(t.Context(), seqs[1], WALSnapshotTruncate))
	require.NoError(t, wal.Close())

	cipher, err := newWALFrameCipher(key)
	require.NoError(t, err)
	active := walActivePath(dir)
	frames := readEncryptedWALFrames(t, active, cipher)
	require.Len(t, frames, 2)
	for _, frame := range frames {
		require.Equal(t, walFormatVersionEncrypted, frame.version)
	}
	raw, err := os.ReadFile(active)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "wal-encryption-cut-one")
	require.NotContains(t, string(raw), "wal-encryption-keep-three")

	entries, err := ReadWALEntriesFromDirWithKey(t.Context(), dir, key)
	require.NoError(t, err)
	require.Equal(t, []uint64{seqs[2], seqs[3]}, encryptedWALSequences(entries))
	_, err = ReadWALEntriesFromDir(t.Context(), dir)
	require.ErrorIs(t, err, ErrWALKeyRequired)

	wal, err = NewWAL(dir, cfg)
	require.NoError(t, err)
	require.Equal(t, seqs[3], wal.Sequence())
	require.Equal(t, seqs[3]+1, appendEncryptedTestEntry(t, wal, "wal-encryption-after-truncate"))
	require.NoError(t, wal.Close())
}

// Property-authority controls are WAL frames too: they are written encrypted,
// and the close-time in-place rewrite that seals them preserves both the file
// length and the keyed reader's view.
func TestEncryptedWALPropertyEpochControlsStayEncrypted(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	engine, _, commit := bindingCostFixture(t, 1)
	dir := t.TempDir()
	key := encryptedWALTestKey(8)
	wal, err := NewWAL(dir, encryptedWALTestConfig(dir, key))
	require.NoError(t, err)
	require.NoError(t, wal.AppendBufferedCommit(commit, engine.propKeyDict))
	require.FileExists(t, walPropertyEpochUnsealedPath(dir))
	require.NoError(t, wal.Close())

	cipher, err := newWALFrameCipher(key)
	require.NoError(t, err)
	active := walActivePath(dir)
	frames := readEncryptedWALFrames(t, active, cipher)
	require.NotEmpty(t, frames)
	controls := 0
	for _, frame := range frames {
		require.Equal(t, walFormatVersionEncrypted, frame.version)
	}
	for _, entry := range encryptedWALFrameEntries(t, frames) {
		if isWALPropertyEpoch(entry) {
			controls++
		}
	}
	require.Equal(t, 1, controls, "the buffered commit is preceded by one epoch control")
	before, err := os.ReadFile(active)
	require.NoError(t, err)
	entriesBefore, err := ReadWALEntriesFromDirWithKey(t.Context(), dir, key)
	require.NoError(t, err)

	wal, err = NewWAL(dir, encryptedWALTestConfig(dir, key))
	require.NoError(t, err)
	wal.mu.Lock()
	sealErr := wal.sealPropertyEpochsLocked()
	wal.mu.Unlock()
	require.NoError(t, sealErr)
	require.NoFileExists(t, walPropertyEpochUnsealedPath(dir))
	after, err := os.ReadFile(active)
	require.NoError(t, err)
	require.Len(t, after, len(before), "sealing rewrites controls in place")
	frames = readEncryptedWALFrames(t, active, cipher)
	for _, frame := range frames {
		require.Equal(t, walFormatVersionEncrypted, frame.version)
	}
	entriesAfter, err := ReadWALEntriesFromDirWithKey(t.Context(), dir, key)
	require.NoError(t, err)
	require.Equal(t, encryptedWALSequences(entriesBefore), encryptedWALSequences(entriesAfter))
	require.NoError(t, wal.Close())
	_, err = ReadWALEntriesFromDir(t.Context(), dir)
	require.ErrorIs(t, err, ErrWALKeyRequired)
}

// A record that needs parts under WALRecordsLarge is written as encrypted part
// frames (version 9) and read back as one record with the key.
func TestEncryptedWALSpannedRecordsRoundtrip(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	key := encryptedWALTestKey(9)
	cfg := encryptedWALTestConfig(dir, key)
	cfg.RecordPolicy = WALRecordsLarge
	wal, err := NewWAL(dir, cfg)
	require.NoError(t, err)
	small, big, last := []byte("wal-encryption-before"), spannedTestData(17<<20), []byte("wal-encryption-after")
	appendSpannedTestRecord(t, wal, small)
	appendSpannedTestRecord(t, wal, big)
	appendSpannedTestRecord(t, wal, last)
	require.NoError(t, wal.Close())

	cipher, err := newWALFrameCipher(key)
	require.NoError(t, err)
	frames := readEncryptedWALFrames(t, walActivePath(dir), cipher)
	require.Greater(t, len(frames), 3)
	require.Equal(t, walFormatVersionEncrypted, frames[0].version)
	for _, frame := range frames[1 : len(frames)-1] {
		require.Equal(t, walFormatVersionEncryptedSpanned, frame.version)
	}
	require.Equal(t, walFormatVersionEncrypted, frames[len(frames)-1].version)

	entries, err := ReadWALEntriesFromDirWithKey(t.Context(), dir, key)
	require.NoError(t, err)
	require.Equal(t, [][]byte{small, big, last}, spannedTestPayloads(t, entries))
	_, err = ReadWALEntriesFromDir(t.Context(), dir)
	require.ErrorIs(t, err, ErrWALKeyRequired)

	wal, err = NewWAL(dir, cfg)
	require.NoError(t, err)
	require.NoError(t, wal.Close())
}

// The plaintext payload bound stays 16 MiB and the ciphertext bound adds the
// nonce and tag, so a maximal record is written and read as one frame.
func TestEncryptedWALAcceptsCiphertextPayloadAbovePlaintextBound(t *testing.T) {
	require.Equal(t, walMaxEntrySize, walFramePayloadLimit(walFormatVersionCurrent))
	require.Equal(t, walMaxEntrySize+walEncryptedFrameOverhead, walFramePayloadLimit(walFormatVersionEncrypted))
	require.Equal(t, walMaxEntrySize+walEncryptedFrameOverhead, walFramePayloadLimit(walFormatVersionEncryptedSpanned))

	key := encryptedWALTestKey(10)
	cipher, err := newWALFrameCipher(key)
	require.NoError(t, err)
	var buf bytes.Buffer
	entry := WALEntry{Sequence: 1, Timestamp: time.Unix(1700000000, 0).UTC(), Operation: OpEntityCommit, Database: "test", Data: []byte("x")}
	entry.Checksum = crc32Checksum(entry.Data)
	payload, err := processWALEntryPayload(walPayloadEncode, &buf, &entry, nil, 0)
	require.NoError(t, err)
	overhead := len(payload) - len(entry.Data)
	entry.Data = spannedTestData(int(walMaxEntrySize) - overhead)
	entry.Checksum = crc32Checksum(entry.Data)
	record, err := encodeWALRecord(&buf, &entry, false)
	require.NoError(t, err)
	require.Len(t, record.single, int(walMaxEntrySize))

	sealed, version, err := cipher.seal(record.single, walFormatVersionCurrent)
	require.NoError(t, err)
	require.Equal(t, walFormatVersionEncrypted, version)
	require.Len(t, sealed, int(walMaxEntrySize)+walEncryptedFrameOverhead)
	require.False(t, walFrameOversized(uint32(len(sealed)), version))
	require.True(t, walFrameOversized(uint32(len(sealed))+1, version))

	dir := t.TempDir()
	path := walActivePath(dir)
	raw, _ := buildAtomicRecord(sealed, version)
	require.NoError(t, os.WriteFile(path, raw, 0600))
	entries, err := ReadWALEntriesWithKey(t.Context(), path, nil, key)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, entry.Data, entries[0].Data)

	// One byte more than the format admits is refused, never read as a record.
	corrupt := bytes.Clone(raw)
	binary.LittleEndian.PutUint32(corrupt[5:], uint32(len(sealed))+1)
	require.NoError(t, os.WriteFile(path, corrupt, 0600))
	_, err = ReadWALEntriesWithKey(t.Context(), path, nil, key)
	require.Error(t, err)
}

// A plaintext journal cannot be opened with a key: the operator converts it
// first, and an undecided journal is never rewritten by the attempt.
func TestPlaintextWALMustBeConvertedBeforeEncryption(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	key := encryptedWALTestKey(11)
	wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
	require.NoError(t, err)
	appendEncryptedTestEntry(t, wal, "wal-encryption-plain-one")
	appendEncryptedTestEntry(t, wal, "wal-encryption-plain-two")
	require.NoError(t, wal.Close())
	active := walActivePath(dir)
	before, err := os.ReadFile(active)
	require.NoError(t, err)
	plainEntries, err := ReadWALEntriesFromDir(t.Context(), dir)
	require.NoError(t, err)
	require.Len(t, plainEntries, 2)

	wal, err = NewWAL(dir, encryptedWALTestConfig(dir, key))
	require.ErrorIs(t, err, ErrWALPlaintextFrames)
	require.Nil(t, wal)
	require.ErrorIs(t, ValidateWALFormatWithKey(dir, WALValidateCurrent, key), ErrWALPlaintextFrames)
	after, err := os.ReadFile(active)
	require.NoError(t, err)
	require.Equal(t, before, after, "opening with a key never converts a plaintext journal")

	require.NoError(t, EncryptWALDirectory(t.Context(), dir, key))
	require.NoError(t, EncryptWALDirectory(t.Context(), dir, key), "conversion is idempotent")
	require.NoError(t, EncryptWALFile(t.Context(), active, key))

	cipher, err := newWALFrameCipher(key)
	require.NoError(t, err)
	for _, frame := range readEncryptedWALFrames(t, active, cipher) {
		require.Equal(t, walFormatVersionEncrypted, frame.version)
	}
	converted, err := ReadWALEntriesFromDirWithKey(t.Context(), dir, key)
	require.NoError(t, err)
	require.Equal(t, encryptedWALSequences(plainEntries), encryptedWALSequences(converted))
	for i := range plainEntries {
		require.Equal(t, plainEntries[i].Data, converted[i].Data)
	}
	_, err = ReadWALEntriesFromDir(t.Context(), dir)
	require.ErrorIs(t, err, ErrWALKeyRequired)
	wal, err = NewWAL(dir, encryptedWALTestConfig(dir, key))
	require.NoError(t, err)
	require.Equal(t, plainEntries[len(plainEntries)-1].Sequence, wal.Sequence())
	require.NoError(t, wal.Close())

	// A key that does not own the converted journal is refused, and the
	// converted bytes stay as they are.
	sealed, err := os.ReadFile(active)
	require.NoError(t, err)
	require.ErrorIs(t, EncryptWALDirectory(t.Context(), dir, encryptedWALTestKey(12)), ErrWALCorrupted)
	require.Equal(t, sealed, mustReadFile(t, active))
}

// A conversion interrupted after some files were replaced finishes on the next
// run, and the manifest then describes the converted files.
func TestEncryptWALDirectoryCompletesPartialConversion(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	key := encryptedWALTestKey(13)
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate", MaxEntries: 1}
	wal, err := NewWAL(dir, cfg)
	require.NoError(t, err)
	var seqs []uint64
	for _, marker := range []string{"wal-encryption-part-one", "wal-encryption-part-two"} {
		seqs = append(seqs, appendEncryptedTestEntry(t, wal, marker))
	}
	require.NoError(t, wal.Close())

	manifest, err := accessWALManifest(dir, walReadManifest, nil)
	require.NoError(t, err)
	require.Len(t, manifest.Segments, 2)
	firstSegment, err := resolveWALSegmentPath(dir, manifest.Segments[0].Path)
	require.NoError(t, err)
	// Simulate a crash after the first file of the directory was replaced.
	require.NoError(t, EncryptWALFile(t.Context(), firstSegment, key))

	require.NoError(t, EncryptWALDirectory(t.Context(), dir, key))
	manifest, err = accessWALManifest(dir, walReadManifest, nil)
	require.NoError(t, err)
	require.Len(t, manifest.Segments, 2)
	for _, segment := range manifest.Segments {
		path, err := resolveWALSegmentPath(dir, segment.Path)
		require.NoError(t, err)
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, info.Size(), segment.SizeBytes)
	}
	entries, err := ReadWALEntriesFromDirWithKey(t.Context(), dir, key)
	require.NoError(t, err)
	require.Equal(t, seqs, encryptedWALSequences(entries))
}

// Frames this build writes with a key are not readable by a build that only
// knows the plaintext versions, and the plaintext V4 upgrade refuses them.
func TestEncryptedWALRefusesFormatUpgradeAndOlderReaders(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	require.False(t, walFrameVersionReadable(walFormatVersionEncrypted))
	require.False(t, walFrameVersionReadable(walFormatVersionEncryptedSpanned))

	dir := t.TempDir()
	key := encryptedWALTestKey(14)
	wal, err := NewWAL(dir, encryptedWALTestConfig(dir, key))
	require.NoError(t, err)
	appendEncryptedTestEntry(t, wal, "wal-encryption-upgrade")
	require.NoError(t, wal.Close())

	require.ErrorIs(t, ValidateWALFormat(dir, WALValidateV4Upgrade), ErrWALUnsupportedVersion)
	require.ErrorIs(t, ValidateWALFormatWithKey(dir, WALValidateV4Upgrade, key), ErrWALUnsupportedVersion)
	_, err = UpgradeWALV4(dir, WALV4UpgradeOptions{})
	require.ErrorIs(t, err, ErrWALKeyRequired)
}

// mustReadFile returns the file's bytes, failing the test on error.
func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

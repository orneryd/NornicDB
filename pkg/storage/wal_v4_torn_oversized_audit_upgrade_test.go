package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// b6AuditJournalFrame locates the embedding audit frame inside a generated b6
// journal.
type b6AuditJournalFrame struct {
	start   int64 // first byte of the audit record
	payload int64 // first byte of that record's payload
	dataLen int64 // the data-length word inside that payload
	end     int64 // first byte after the complete audit record
}

// b6OversizedAuditJournal generates with b6HistoricalFrame the version-3
// journal the b6 writer left for a store with one embedded node: a complete
// record, the complete embedding audit frame over the frame bound, and a
// complete record after it.
func b6OversizedAuditJournal(t *testing.T) ([]byte, b6AuditJournalFrame) {
	t.Helper()
	image := make([]byte, 17<<20)
	rand.New(rand.NewSource(11)).Read(image)
	node := func(id string) []byte {
		raw, err := json.Marshal(WALNodeData{Node: &Node{ID: NodeID(id), Labels: []string{"Doc"}}})
		require.NoError(t, err)
		return raw
	}
	first := b6HistoricalFrame(t, 1, OpCreateNode, node("before"))
	audit := b6HistoricalFrame(t, 2, OpUpdateEmbedding, image)
	last := b6HistoricalFrame(t, 3, OpCreateNode, node("after"))
	require.Greater(t, int(binary.LittleEndian.Uint32(audit[5:])), int(walMaxEntrySize), "the audit frame is over the frame bound")
	frame := b6AuditJournalFrame{
		start:   int64(len(first)),
		payload: int64(len(first)) + 9,
		end:     int64(len(first) + len(audit)),
	}
	frame.dataLen = frame.payload + 4 + int64(binary.LittleEndian.Uint32(audit[9:]))
	require.Greater(t, frame.dataLen, frame.payload+4, "the audit frame carries an image")
	return append(append(bytes.Clone(first), audit...), last...), frame
}

// writeActiveJournal writes raw as the active journal of a fresh store.
func writeActiveJournal(t *testing.T, raw []byte) (store, dir string) {
	t.Helper()
	store = t.TempDir()
	dir = filepath.Join(store, "wal")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "wal.log"), raw, 0o600))
	return store, dir
}

// auditJournalCuts names every byte position inside the audit frame that the
// file can end at: the frame header, its payload, the envelope's length word,
// the envelope, the image length word, the image, the frame checksum, the
// trailer and the padding.
func auditJournalCuts(journal []byte, frame b6AuditJournalFrame) []struct {
	name string
	at   int64
} {
	size := int64(binary.LittleEndian.Uint32(journal[frame.start+5:]))
	return []struct {
		name string
		at   int64
	}{
		{"frame_header", frame.start + 6},
		{"frame_payload", frame.start + 11},
		{"envelope", frame.payload + 12},
		{"data_length", frame.dataLen + 2},
		{"image_start", frame.dataLen + 4},
		{"image", frame.dataLen + 4 + (8 << 20)},
		{"frame_checksum", frame.payload + size + 2},
		{"trailer", frame.payload + size + 8},
		{"padding", frame.end - 1},
	}
}

// oversizedAuditFault is one form of damage to the audit frame of a generated
// journal and the refusal it must produce.
type oversizedAuditFault struct {
	name   string
	damage func([]byte)
	want   string
}

// oversizedAuditFaults damages the audit frame without shortening it: every
// frame below stays complete in its declared length, so none is a torn tail.
func oversizedAuditFaults(journal []byte, frame b6AuditJournalFrame) []oversizedAuditFault {
	size := int64(binary.LittleEndian.Uint32(journal[frame.start+5:]))
	return []oversizedAuditFault{
		{
			"image_checksum",
			func(raw []byte) {
				raw[frame.dataLen+4096] ^= 1
				binary.LittleEndian.PutUint32(raw[frame.payload+size:], crc32Checksum(raw[frame.payload:frame.payload+size]))
			},
			"oversized frame data checksum",
		},
		{"frame_checksum", func(raw []byte) { raw[frame.payload+size] ^= 1 }, "oversized frame checksum"},
		{"trailer", func(raw []byte) { raw[frame.payload+size+4] ^= 1 }, "invalid oversized frame trailer"},
		{"data_length", func(raw []byte) { binary.LittleEndian.PutUint32(raw[frame.dataLen:], 1) }, "invalid oversized frame data length"},
		{"envelope_length", func(raw []byte) { binary.LittleEndian.PutUint32(raw[frame.payload:], walMaxEntrySize+1) }, "invalid oversized frame header length"},
	}
}

// TestV4UpgradeKeepsCompletePrefixOfTornOversizedAuditTail reproduces report #5
// at the public upgrade: a historical store journal whose final frame is an
// embedding audit over the frame bound, cut short inside its declared extent.
// The b6 writer opened that tail; the upgrade must carry the complete records
// before it and drop the torn frame, exactly as it does for an ordinary final
// frame.
func TestV4UpgradeKeepsCompletePrefixOfTornOversizedAuditTail(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	journal, frame := b6OversizedAuditJournal(t)
	for _, cut := range auditJournalCuts(journal, frame) {
		t.Run(cut.name, func(t *testing.T) {
			torn := journal[:cut.at]
			store, dir := writeActiveJournal(t, torn)
			require.ErrorIs(t, ValidateWALFormat(dir, WALValidateCurrent), ErrWALUnsupportedVersion)
			require.NoError(t, ValidateWALFormat(dir, WALValidateV4Upgrade),
				"the active journal tolerates a final frame the file ends inside")

			upgraded, err := UpgradeWALV4(dir, WALV4UpgradeOptions{})
			require.NoError(t, err)
			require.True(t, upgraded)
			require.NoError(t, ValidateWALFormat(dir, WALValidateCurrent))

			entries, err := ReadWALEntriesFromDir(t.Context(), dir)
			require.NoError(t, err)
			require.Len(t, entries, 1, "the records before the torn frame are carried")
			require.Equal(t, uint64(1), entries[0].Sequence)
			require.Equal(t, OpCreateNode, entries[0].Operation)
			recovered, result, err := fixtureowner.OwnResult3[*MemoryEngine, ReplayResult](t, ErrStorageClosed)(RecoverFromWALWithResult(t.Context(), dir, ""))
			require.NoError(t, err)
			t.Cleanup(func() { _ = recovered.Close() })
			require.Zero(t, result.Failed)
			node, err := recovered.GetNode(t.Context(), "nornic:before")
			require.NoError(t, err)
			require.Equal(t, []string{"Doc"}, node.Labels)
			_, err = recovered.GetNode(t.Context(), "nornic:after")
			require.ErrorIs(t, err, ErrNotFound, "a record after the interrupted tail was not present")
			converted, err := os.ReadFile(filepath.Join(dir, "wal.log"))
			require.NoError(t, err)
			require.NotEqual(t, torn, converted, "the torn journal is replaced by its converted records")
			require.Equal(t, map[byte]int{walFormatVersionCurrent: 1}, walFrameVersions(t, converted),
				"the torn frame is dropped, not rewritten")

			// The same torn bytes upgrade to the same journal, and a second
			// upgrade of a converted journal changes nothing.
			_, second := writeActiveJournal(t, torn)
			again, err := UpgradeWALV4(second, WALV4UpgradeOptions{})
			require.NoError(t, err)
			require.True(t, again)
			same, err := os.ReadFile(filepath.Join(second, "wal.log"))
			require.NoError(t, err)
			require.Equal(t, converted, same, "the upgrade is deterministic for the same journal")

			repeat, err := UpgradeWALV4(dir, WALV4UpgradeOptions{})
			require.NoError(t, err)
			require.False(t, repeat)
			after, err := os.ReadFile(filepath.Join(store, "wal", "wal.log"))
			require.NoError(t, err)
			require.Equal(t, converted, after)
		})
	}
}

// TestV4UpgradeRefusesCompleteCorruptOversizedAuditAtTheTail: an oversized audit
// frame that is the final frame and complete in its declared length, but
// corrupt, is still refused and nothing on disk changes. The tolerated tail is
// the file ending inside a frame, never damage inside a complete one.
func TestV4UpgradeRefusesCompleteCorruptOversizedAuditAtTheTail(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	journal, frame := b6OversizedAuditJournal(t)
	tail := journal[:frame.end]
	for _, fault := range oversizedAuditFaults(journal, frame) {
		t.Run(fault.name, func(t *testing.T) {
			raw := bytes.Clone(tail)
			fault.damage(raw)
			store, dir := writeActiveJournal(t, raw)
			before := walDirDigest(t, store)
			require.ErrorContains(t, ValidateWALFormat(dir, WALValidateV4Upgrade), fault.want)
			_, err := UpgradeWALV4(dir, WALV4UpgradeOptions{})
			require.ErrorContains(t, err, fault.want)
			require.Equal(t, before, walDirDigest(t, store), "a refused upgrade changes no file")
		})
	}
}

// TestV4UpgradeRefusesCorruptInteriorOversizedAudit: the same damage inside an
// oversized audit frame that a complete record follows is refused too, so the
// tail tolerance cannot swallow an interior frame or resynchronize past it.
func TestV4UpgradeRefusesCorruptInteriorOversizedAudit(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	journal, frame := b6OversizedAuditJournal(t)
	require.Less(t, frame.end, int64(len(journal)), "a complete record follows the audit")
	for _, fault := range oversizedAuditFaults(journal, frame) {
		t.Run(fault.name, func(t *testing.T) {
			raw := bytes.Clone(journal)
			fault.damage(raw)
			store, dir := writeActiveJournal(t, raw)
			before := walDirDigest(t, store)
			require.ErrorContains(t, ValidateWALFormat(dir, WALValidateV4Upgrade), fault.want)
			_, err := UpgradeWALV4(dir, WALV4UpgradeOptions{})
			require.ErrorContains(t, err, fault.want)
			require.Equal(t, before, walDirDigest(t, store), "a refused upgrade changes no file")
		})
	}
}

// TestV4UpgradeRefusesTornOversizedAuditInSealedFile: a file the upgrade cannot
// treat as the active journal keeps refusing a frame cut short, so the
// tolerance is the active journal's tail rule and not a weaker frame check.
func TestV4UpgradeRefusesTornOversizedAuditInSealedFile(t *testing.T) {
	journal, frame := b6OversizedAuditJournal(t)
	torn := journal[:frame.dataLen+4+(8<<20)]
	dir := t.TempDir()
	source, dest := filepath.Join(dir, "wal.log"), filepath.Join(dir, "converted")
	require.NoError(t, os.WriteFile(source, torn, 0o600))
	require.ErrorContains(t, upgradeWALFileV4(source, dest, false, nil), "incomplete oversized frame",
		"a sealed file cannot discard a torn record")
	_, err := os.Stat(dest)
	require.ErrorIs(t, err, os.ErrNotExist)
	unchanged, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, torn, unchanged)
}

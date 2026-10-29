package search

import (
	"bytes"
	"encoding/gob"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// Upgrading search data must convert existing relationship snapshots in both
// the direct and generation layouts without changing their contents.
func TestConvertSearchIndexDirectoryRelationshipSnapshots(t *testing.T) {
	for _, generation := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "generation"}[generation], func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "tenant")
			if generation {
				dir = filepath.Join(dir, "g-0123456789abcdef")
			}
			require.NoError(t, os.MkdirAll(dir, 0o700))
			path := filepath.Join(dir, "bm25.v2"+relationshipVectorsFileSuffix)
			want := relationshipVectorsFile{
				Format: relationshipVectorsFormat,
				Entries: map[string]*relationshipEntry{
					"edge-1": {
						Source:  math.MaxUint64,
						Vectors: [][]float32{{math.Float32frombits(0x80000000), math.Float32frombits(1), 1}, {0.5, -1}},
						Spans:   []embeddingutil.ChunkSpan{{Chunk: 0, Property: "reason", Start: 1, End: 5}},
					},
				},
			}
			var source bytes.Buffer
			require.NoError(t, gob.NewEncoder(&source).Encode(want))
			require.NoError(t, os.WriteFile(path, source.Bytes(), 0o600))
			require.NoError(t, os.Chmod(path, 0o640))
			beforeInfo, err := os.Stat(path)
			require.NoError(t, err)

			require.NoError(t, ConvertSearchIndexDirFromGobToMsgpack(root))
			converted, err := os.ReadFile(path)
			require.NoError(t, err)
			require.NotEqual(t, source.Bytes(), converted, "the historical gob bytes must actually be replaced")
			afterInfo, err := os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, beforeInfo.Mode().Perm(), afterInfo.Mode().Perm())
			var got relationshipVectorsFile
			require.NoError(t, msgpack.Unmarshal(converted, &got))
			require.Equal(t, want.Format, got.Format)
			require.Equal(t, want.Entries["edge-1"].Source, got.Entries["edge-1"].Source)
			require.Equal(t, want.Entries["edge-1"].Spans, got.Entries["edge-1"].Spans)
			for i, vector := range want.Entries["edge-1"].Vectors {
				require.Len(t, got.Entries["edge-1"].Vectors[i], len(vector))
				for j, value := range vector {
					require.Equal(t, math.Float32bits(value), math.Float32bits(got.Entries["edge-1"].Vectors[i][j]))
				}
			}
			require.NoError(t, ConvertSearchIndexDirFromGobToMsgpack(root))
			again, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, converted, again)
		})
	}
}

// A corrupt source must be reported and kept intact for recovery.
func TestConvertSearchIndexDirectoryPreservesCorruptRelationshipSnapshot(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "tenant")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	path := filepath.Join(dir, "bm25.v2"+relationshipVectorsFileSuffix)
	original := []byte("truncated gob relationship snapshot")
	require.NoError(t, os.WriteFile(path, original, 0o600))
	require.Error(t, ConvertSearchIndexDirFromGobToMsgpack(root))
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, original, got)
}

// The snapshot converter must reject any source that is not exactly one
// relationshipVectorsFile: gob or msgpack, with trailing bytes, with more than
// one value, or in another format. A rejected source must be reported and left
// byte-for-byte unchanged, so a later rerun can still read it.
func TestConvertRelationshipSnapshotFileRejectsUnexpectedSource(t *testing.T) {
	trailingZero := []byte{0}
	trailingNonZero := []byte("keep-this")
	trailingValidValue := []byte{0xc0} // msgpack nil: a complete second value

	tests := []struct {
		name   string
		source []byte
		// wantDecoderErr holds the decoded value of the rejected source and
		// true when the source decodes as exactly one relationshipVectorsFile
		// but is still rejected (a wrong stored format).
		decodeAsSnapshot bool
		wantFormat       int
	}{
		{
			name: "gob with trailing zero byte",
		},
		{
			name: "gob with nonzero trailing bytes",
		},
		{
			name: "msgpack with one trailing byte",
		},
		{
			name: "msgpack with trailing nonzero bytes",
		},
		{
			name: "msgpack with a second complete value",
		},
		{
			name:             "msgpack with wrong stored format",
			decodeAsSnapshot: true,
			wantFormat:       relationshipVectorsFormat + 1,
		},
		{
			name: "msgpack with a malformed stored value",
		},
		{
			name: "msgpack with a wrongly typed stored field",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "bm25.v2"+relationshipVectorsFileSuffix)

			// A known-good snapshot in the shape the source claims to use.
			valid := relationshipVectorsFile{
				Format: relationshipVectorsFormat,
				Entries: map[string]*relationshipEntry{
					"edge-1": {Source: 42, Vectors: [][]float32{{1, 0}}, Spans: []embeddingutil.ChunkSpan{{Chunk: 0, Property: "reason", Start: 0, End: 4}}},
				},
			}
			switch tt.name {
			case "gob with trailing zero byte":
				tt.source = append(encodeGobSnapshot(t, valid), trailingZero...)
			case "gob with nonzero trailing bytes":
				tt.source = append(encodeGobSnapshot(t, valid), trailingNonZero...)
			case "msgpack with one trailing byte":
				tt.source = append(encodeMsgpackValue(t, &valid), trailingZero...)
			case "msgpack with trailing nonzero bytes":
				tt.source = append(encodeMsgpackValue(t, &valid), trailingNonZero...)
			case "msgpack with a second complete value":
				tt.source = append(encodeMsgpackValue(t, &valid), trailingValidValue...)
			case "msgpack with wrong stored format":
				valid.Format = tt.wantFormat
				tt.source = encodeMsgpackValue(t, &valid)
			case "msgpack with a malformed stored value":
				header := encodeMsgpackValue(t, &valid)
				tt.source = header[:len(header)-3]
			case "msgpack with a wrongly typed stored field":
				tt.source = encodeMsgpackValue(t, map[string]any{"Format": "two", "Entries": map[string]any{}})
			}
			require.NotEmpty(t, tt.source)
			require.NoError(t, os.WriteFile(path, tt.source, 0o600))
			original, err := os.ReadFile(path)
			require.NoError(t, err)

			err = UpgradeRelationshipSnapshot(path, true, nil)
			require.Error(t, err, "a source rejected by the converter must report an error")
			require.NotErrorIs(t, err, ErrRelationshipSnapshotUpgradeRequired, "invalid data must not be reported as an authorized historical snapshot")

			got, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			require.Equal(t, original, got, "a rejected source must stay byte-for-byte unchanged")
			require.Equal(t, tt.source, got)
			_, statErr := os.Stat(path + ".tmp")
			require.True(t, os.IsNotExist(statErr), "a rejected source must not leave a temporary file")

			if tt.decodeAsSnapshot {
				// The source is a complete msgpack snapshot that only the
				// stored format rejects; the error must name the format.
				var stored relationshipVectorsFile
				require.NoError(t, msgpack.Unmarshal(original, &stored))
				require.Equal(t, tt.wantFormat, stored.Format)
				require.Contains(t, err.Error(), "format")
			}
		})
	}
}

// A gob snapshot at the current format version must be converted, not
// rejected as an unknown format, and an empty (nil) entry map must survive
// conversion with the same format.
func TestConvertRelationshipSnapshotFileAcceptsCurrentFormatGob(t *testing.T) {
	t.Run("older format remains older", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "bm25.v2"+relationshipVectorsFileSuffix)
		source := relationshipVectorsFile{Format: 1, Entries: map[string]*relationshipEntry{"edge-1": {Source: 7, Vectors: [][]float32{{1, 0}}}}}
		require.NoError(t, os.WriteFile(path, encodeGobSnapshot(t, source), 0o600))
		require.NoError(t, UpgradeRelationshipSnapshot(path, true, nil))
		converted, err := os.ReadFile(path)
		require.NoError(t, err)
		var got relationshipVectorsFile
		require.NoError(t, msgpack.Unmarshal(converted, &got))
		require.Equal(t, source, got)
	})
	t.Run("format is preserved, not rejected", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "bm25.v2"+relationshipVectorsFileSuffix)
		source := relationshipVectorsFile{
			Format: relationshipVectorsFormat,
			Entries: map[string]*relationshipEntry{
				"edge-1": {Source: 7, Vectors: [][]float32{{0, 1}}, Spans: []embeddingutil.ChunkSpan{{Chunk: 0, Property: "reason", Start: 2, End: 6}}},
			},
		}
		require.NoError(t, os.WriteFile(path, encodeGobSnapshot(t, source), 0o600))

		require.NoError(t, UpgradeRelationshipSnapshot(path, true, nil))
		converted, err := os.ReadFile(path)
		require.NoError(t, err)
		var got relationshipVectorsFile
		require.NoError(t, msgpack.Unmarshal(converted, &got))
		require.Equal(t, relationshipVectorsFormat, got.Format, "a current-format gob snapshot must keep its format")
		require.Equal(t, source.Entries["edge-1"].Source, got.Entries["edge-1"].Source)
		require.Equal(t, source.Entries["edge-1"].Spans, got.Entries["edge-1"].Spans)
		require.Equal(t, source.Entries["edge-1"].Vectors, got.Entries["edge-1"].Vectors)
	})

	t.Run("nil entries stay nil", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "bm25.v2"+relationshipVectorsFileSuffix)
		require.NoError(t, os.WriteFile(path, encodeGobSnapshot(t, relationshipVectorsFile{Format: relationshipVectorsFormat}), 0o600))

		require.NoError(t, UpgradeRelationshipSnapshot(path, true, nil))
		converted, err := os.ReadFile(path)
		require.NoError(t, err)
		var got relationshipVectorsFile
		require.NoError(t, msgpack.Unmarshal(converted, &got))
		require.Equal(t, relationshipVectorsFormat, got.Format)
		require.Nil(t, got.Entries, "an empty snapshot must stay empty")
	})
}

// A leftover temporary path that is a symlink to the source must never make
// the converter truncate or rewrite the source through that link.
func TestConvertRelationshipSnapshotFileLeavesSourceThroughTmpSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks are not available to unprivileged processes on Windows")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "bm25.v2"+relationshipVectorsFileSuffix)
	source := relationshipVectorsFile{Format: relationshipVectorsFormat}
	require.NoError(t, os.WriteFile(path, encodeGobSnapshot(t, source), 0o600))
	original, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.Symlink(filepath.Base(path), path+".tmp"))

	err = UpgradeRelationshipSnapshot(path, true, nil)

	got, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	switch {
	case err != nil:
		// A rejected source stays untouched, which the assertions above prove.
		require.Equal(t, original, got)
	default:
		// A converted source must be replaced by a regular file that holds the
		// complete msgpack snapshot, not the source truncated in place.
		info, statErr := os.Lstat(path)
		require.NoError(t, statErr)
		require.Zero(t, info.Mode()&os.ModeSymlink, "the source must not become a symlink")
		converted, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		var stored relationshipVectorsFile
		require.NoError(t, msgpack.Unmarshal(converted, &stored), "the source must hold a complete msgpack snapshot")
		require.Equal(t, relationshipVectorsFormat, stored.Format)
	}
}

func encodeGobSnapshot(t *testing.T, value relationshipVectorsFile) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(value))
	return buf.Bytes()
}

func encodeMsgpackValue(t *testing.T, value any) []byte {
	t.Helper()
	var buf bytes.Buffer
	if snapshot, ok := value.(*relationshipVectorsFile); ok {
		require.NoError(t, encodeRelationshipSnapshot(&buf, snapshot))
	} else {
		require.NoError(t, msgpack.NewEncoder(&buf).Encode(value))
	}
	return buf.Bytes()
}

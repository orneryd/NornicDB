package storage

// Tests for the snapshot-at-rest envelope: format and roundtrip, refusal of a
// wrong or missing key, refusal of plaintext under a key, tampered, reordered,
// dropped and truncated chunks, and a large snapshot written and read in
// bounded chunks.

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func testSnapshotEncryptionKey() []byte {
	return bytes.Repeat([]byte{0x51}, SnapshotEncryptionKeySize)
}

func testSnapshotEncryptionWrongKey() []byte {
	return bytes.Repeat([]byte{0x52}, SnapshotEncryptionKeySize)
}

// plaintextSnapshotSignature is the plaintext NDS2 stream's magic and version.
// It is longer than the four-byte magic so a random ciphertext match is not a
// realistic source of a false failure.
func plaintextSnapshotSignature() []byte {
	return append([]byte(streamSnapshotMagic), streamSnapshotVersion)
}

type snapshotEnvelopeLayout struct {
	header      []byte
	chunkSize   int
	noncePrefix []byte
	frames      [][]byte
}

func parseSnapshotEnvelope(t *testing.T, data []byte) snapshotEnvelopeLayout {
	t.Helper()
	require.GreaterOrEqual(t, len(data), encryptedSnapshotHeaderSize, "envelope header")
	header := append([]byte(nil), data[:encryptedSnapshotHeaderSize]...)
	require.Equal(t, encryptedSnapshotMagic, string(header[:len(encryptedSnapshotMagic)]))
	require.Equal(t, encryptedSnapshotVersion, header[len(encryptedSnapshotMagic)])
	chunkSize := int(binary.LittleEndian.Uint32(header[encryptedSnapshotChunkSizeField:]))
	require.Equal(t, snapshotEncryptionChunkSize, chunkSize)
	prefix := append([]byte(nil), header[encryptedSnapshotPrefixField:encryptedSnapshotPrefixField+snapshotNoncePrefixSize]...)
	var frames [][]byte
	for rest := data[encryptedSnapshotHeaderSize:]; len(rest) > 0; {
		require.GreaterOrEqual(t, len(rest), 4, "frame length prefix")
		length := int(binary.LittleEndian.Uint32(rest[:4]))
		require.GreaterOrEqual(t, len(rest), 4+length, "frame body")
		frames = append(frames, append([]byte(nil), rest[:4+length]...))
		rest = rest[4+length:]
	}
	require.NotEmpty(t, frames)
	return snapshotEnvelopeLayout{header: header, chunkSize: chunkSize, noncePrefix: prefix, frames: frames}
}

func (l snapshotEnvelopeLayout) file(frames [][]byte) []byte {
	out := append([]byte(nil), l.header...)
	for _, frame := range frames {
		out = append(out, frame...)
	}
	return out
}

func (l snapshotEnvelopeLayout) ciphertextLengths() []int {
	lengths := make([]int, len(l.frames))
	for i, frame := range l.frames {
		lengths[i] = len(frame) - 4
	}
	return lengths
}

func (l snapshotEnvelopeLayout) plaintextLength() int {
	total := 0
	for _, length := range l.ciphertextLengths() {
		if length > 16 {
			total += length - 16
		}
	}
	return total
}

// requireSnapshotEnvelopeFormat asserts the framing the envelope owns: one
// completion frame, full data chunks except the last one.
func requireSnapshotEnvelopeFormat(t *testing.T, layout snapshotEnvelopeLayout) {
	t.Helper()
	require.GreaterOrEqual(t, len(layout.frames), 2, "at least one data frame and the completion frame")
	last := len(layout.frames) - 1
	require.Equal(t, 16, len(layout.frames[last])-4, "the completion frame carries only its tag")
	for i := 0; i < last; i++ {
		plaintext := len(layout.frames[i]) - 4 - 16
		require.Positive(t, plaintext, "data frame %d is not empty", i)
		require.LessOrEqual(t, plaintext, layout.chunkSize, "data frame %d exceeds one chunk", i)
		if i < last-1 {
			require.Equal(t, layout.chunkSize, plaintext, "only the last data frame may be short")
		}
	}
}

func encryptedSnapshotBytes(t *testing.T, engine Engine, sequence uint64) []byte {
	t.Helper()
	var output bytes.Buffer
	require.NoError(t, WriteSnapshot(t.Context(), engine, &output, SnapshotOptions{Sequence: sequence, EncryptionKey: testSnapshotEncryptionKey()}))
	return append([]byte(nil), output.Bytes()...)
}

func newSmallEncryptedSnapshotEngine(t *testing.T) *MemoryEngine {
	t.Helper()
	engine := newTestMemoryEngine(t)
	_, err := engine.CreateNode(t.Context(), &Node{ID: "nornic:secret", Labels: []string{"Doc"},
		Properties: map[string]any{"secret": "sensitive-snapshot-envelope-value-3f6c"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "nornic:plain"})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "nornic:rel", StartNode: "nornic:secret", EndNode: "nornic:plain", Type: "LINKS"}))
	return engine
}

func newMultiChunkEncryptedSnapshotEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine := newSnapshotBadger(t)
	_, err := engine.CreateNode(t.Context(), &Node{ID: "nornic:filler", Labels: []string{"Doc"},
		Properties: map[string]any{"filler": strings.Repeat("a", 200<<10)}})
	require.NoError(t, err)
	return engine
}

func TestEncryptedSnapshotRoundTripAndEnvelopeFormat(t *testing.T) {
	engine := newSmallEncryptedSnapshotEngine(t)
	key := testSnapshotEncryptionKey()
	secret := "sensitive-snapshot-envelope-value-3f6c"

	path := filepath.Join(t.TempDir(), "snapshot-0000000000000000002a.json")
	require.NoError(t, SaveStreamingSnapshot(t.Context(), engine, path, SnapshotOptions{Sequence: 42, EncryptionKey: key}))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.False(t, bytes.Contains(data, []byte(secret)), "encrypted snapshot holds the plaintext property value")
	require.False(t, bytes.Contains(data, []byte("nornic:secret")), "encrypted snapshot holds the plaintext entity ID")
	require.False(t, bytes.Contains(data, plaintextSnapshotSignature()), "encrypted snapshot holds the plaintext NDS2 marker")
	require.True(t, bytes.HasPrefix(data, []byte(encryptedSnapshotMagic)), "envelope magic")

	encrypted, err := IsEncryptedSnapshot(path)
	require.NoError(t, err)
	require.True(t, encrypted)
	streaming, err := IsStreamingSnapshot(path)
	require.NoError(t, err)
	require.False(t, streaming, "an encrypted snapshot is not a plaintext streaming snapshot")

	layout := parseSnapshotEnvelope(t, data)
	requireSnapshotEnvelopeFormat(t, layout)
	require.Equal(t, 1, len(layout.frames)-1, "the small snapshot is one data chunk and the completion frame")

	visitor := &collectingSnapshotVisitor{}
	metadata, err := ReadSnapshotWithKey(t.Context(), bytes.NewReader(data), visitor, key)
	require.NoError(t, err)
	require.Equal(t, uint64(42), metadata.Sequence)
	require.Equal(t, uint64(2), metadata.NodeCount)
	require.Equal(t, uint64(1), metadata.EdgeCount)
	require.Len(t, visitor.nodes, 2)
	require.Len(t, visitor.edges, 1)
	found := false
	for _, node := range visitor.nodes {
		if node.ID == "nornic:secret" {
			found = true
			require.Equal(t, secret, node.Properties["secret"])
		}
	}
	require.True(t, found, "the secret node round-trips")

	// The same store encrypted repeatedly keeps the frame layout and changes
	// every nonce prefix, so two files never share a nonce sequence.
	prefixes := make(map[string]bool)
	for i := 0; i < 8; i++ {
		written := encryptedSnapshotBytes(t, engine, 42)
		repeated := parseSnapshotEnvelope(t, written)
		require.Equal(t, layout.ciphertextLengths(), repeated.ciphertextLengths(), "the plaintext frame layout is deterministic")
		require.Equal(t, layout.chunkSize, repeated.chunkSize)
		prefixes[string(repeated.noncePrefix)] = true
	}
	require.Len(t, prefixes, 8, "each encrypted file has its own random nonce prefix")
}

func TestEncryptedSnapshotRejectsWrongMissingAndInvalidKeys(t *testing.T) {
	engine := newSmallEncryptedSnapshotEngine(t)
	data := encryptedSnapshotBytes(t, engine, 1)

	wrong := testSnapshotEncryptionWrongKey()
	var visitor collectingSnapshotVisitor
	_, err := ReadSnapshotWithKey(t.Context(), bytes.NewReader(data), &visitor, wrong)
	require.ErrorIs(t, err, ErrSnapshotEncryptionAuthentication)
	require.Empty(t, visitor.nodes)

	_, err = ReadSnapshotWithKey(t.Context(), bytes.NewReader(data), &collectingSnapshotVisitor{}, nil)
	require.ErrorIs(t, err, ErrSnapshotEncryptionRequired)

	_, err = ReadSnapshot(t.Context(), bytes.NewReader(data), &collectingSnapshotVisitor{})
	require.Error(t, err, "the plaintext reader refuses an encrypted snapshot")

	for _, invalid := range [][]byte{bytes.Repeat([]byte{1}, SnapshotEncryptionKeySize-1), bytes.Repeat([]byte{1}, SnapshotEncryptionKeySize+1)} {
		var output bytes.Buffer
		require.ErrorIs(t, WriteSnapshot(t.Context(), engine, &output, SnapshotOptions{EncryptionKey: invalid}), ErrSnapshotEncryptionKeyInvalid)
		_, err := ReadSnapshotWithKey(t.Context(), bytes.NewReader(data), &collectingSnapshotVisitor{}, invalid)
		require.ErrorIs(t, err, ErrSnapshotEncryptionKeyInvalid)
		_, err = NewSnapshotDecryptingReader(bytes.NewReader(data), invalid)
		require.ErrorIs(t, err, ErrSnapshotEncryptionKeyInvalid)
		path := filepath.Join(t.TempDir(), "invalid-key.nds")
		require.ErrorIs(t, SaveStreamingSnapshot(t.Context(), engine, path, SnapshotOptions{EncryptionKey: invalid}), ErrSnapshotEncryptionKeyInvalid)
		_, statErr := os.Stat(path)
		require.True(t, os.IsNotExist(statErr), "an invalid key leaves no snapshot file")
	}

	_, err = NewSnapshotDecryptingReader(bytes.NewReader(data), nil)
	require.ErrorIs(t, err, ErrSnapshotEncryptionRequired)
}

func TestEncryptedSnapshotRefusesPlaintextUnderKey(t *testing.T) {
	ctx := context.Background()
	engine := newSmallEncryptedSnapshotEngine(t)

	var plaintext bytes.Buffer
	require.NoError(t, WriteSnapshot(ctx, engine, &plaintext, SnapshotOptions{Sequence: 3}))
	raw := plaintext.Bytes()

	_, err := ReadSnapshotWithKey(ctx, bytes.NewReader(raw), &collectingSnapshotVisitor{}, testSnapshotEncryptionKey())
	require.ErrorIs(t, err, ErrSnapshotEncryptionNotEncrypted, "an old plaintext snapshot is never read under a key")
	_, err = NewSnapshotDecryptingReader(bytes.NewReader(raw), testSnapshotEncryptionKey())
	require.ErrorIs(t, err, ErrSnapshotEncryptionNotEncrypted)

	// Without a key the same snapshot keeps its plaintext behavior.
	visitor := &collectingSnapshotVisitor{}
	metadata, err := ReadSnapshotWithKey(ctx, bytes.NewReader(raw), visitor, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3), metadata.Sequence)
	require.Len(t, visitor.nodes, 2)
	require.Len(t, visitor.edges, 1)

	path := filepath.Join(t.TempDir(), "plaintext.nds")
	require.NoError(t, SaveStreamingSnapshot(ctx, engine, path, SnapshotOptions{Sequence: 3}))
	encrypted, err := IsEncryptedSnapshot(path)
	require.NoError(t, err)
	require.False(t, encrypted)
	streaming, err := IsStreamingSnapshot(path)
	require.NoError(t, err)
	require.True(t, streaming)
}

func TestEncryptedSnapshotDetectsTampering(t *testing.T) {
	engine := newMultiChunkEncryptedSnapshotEngine(t)
	data := encryptedSnapshotBytes(t, engine, 2)
	layout := parseSnapshotEnvelope(t, data)
	require.GreaterOrEqual(t, len(layout.frames), 4, "several data chunks")

	flipCiphertext := func() []byte {
		damaged := append([]byte(nil), data...)
		damaged[encryptedSnapshotHeaderSize+4+3] ^= 0xff
		return damaged
	}
	flipHeader := func(offset int) []byte {
		damaged := append([]byte(nil), data...)
		damaged[offset] ^= 0xff
		return damaged
	}
	tamperFrames := func(edit func(frames [][]byte) [][]byte) []byte {
		frames := make([][]byte, len(layout.frames))
		for i := range layout.frames {
			frames[i] = append([]byte(nil), layout.frames[i]...)
		}
		return layout.file(edit(frames))
	}

	damaged := map[string][]byte{
		"ciphertext bit":    flipCiphertext(),
		"envelope magic":    flipHeader(0),
		"envelope version":  flipHeader(len(encryptedSnapshotMagic)),
		"nonce prefix":      flipHeader(encryptedSnapshotPrefixField + 2),
		"declared chunk":    flipHeader(encryptedSnapshotChunkSizeField),
		"completion marker": flipHeader(len(data) - 1),
		"reordered chunks": tamperFrames(func(frames [][]byte) [][]byte {
			frames[0], frames[1] = frames[1], frames[0]
			return frames
		}),
		"replayed chunk": tamperFrames(func(frames [][]byte) [][]byte {
			return append([][]byte{frames[0], frames[0]}, frames[2:]...)
		}),
		"dropped chunk": tamperFrames(func(frames [][]byte) [][]byte {
			return append([][]byte{frames[0]}, frames[2:]...)
		}),
		"frame length": tamperFrames(func(frames [][]byte) [][]byte {
			binary.LittleEndian.PutUint32(frames[0][:4], uint32(snapshotEncryptionChunkSize+16+1))
			return frames
		}),
		"trailing data": append(append([]byte(nil), data...), 0),
	}
	for name, damagedData := range damaged {
		t.Run(name, func(t *testing.T) {
			_, err := ReadSnapshotWithKey(t.Context(), bytes.NewReader(damagedData), &collectingSnapshotVisitor{}, testSnapshotEncryptionKey())
			require.Error(t, err, "damaged encrypted snapshot is refused")
		})
	}
}

func TestEncryptedSnapshotDetectsTruncation(t *testing.T) {
	engine := newMultiChunkEncryptedSnapshotEngine(t)
	data := encryptedSnapshotBytes(t, engine, 2)
	layout := parseSnapshotEnvelope(t, data)

	damaged := map[string][]byte{
		"incomplete header":          append([]byte(nil), data[:encryptedSnapshotHeaderSize-1]...),
		"inside first chunk":         append([]byte(nil), data[:encryptedSnapshotHeaderSize+6]...),
		"missing completion frame":   append([]byte(nil), data[:len(data)-len(layout.frames[len(layout.frames)-1])]...),
		"half a completion frame":    append([]byte(nil), data[:len(data)-8]...),
		"missing last data and tail": append([]byte(nil), data[:encryptedSnapshotHeaderSize+len(layout.frames[0])+5]...),
	}
	for name, damagedData := range damaged {
		t.Run(name, func(t *testing.T) {
			_, err := ReadSnapshotWithKey(t.Context(), bytes.NewReader(damagedData), &collectingSnapshotVisitor{}, testSnapshotEncryptionKey())
			require.ErrorIs(t, err, ErrSnapshotEncryptionFormat, "a truncated envelope fails instead of returning a partial snapshot")
		})
	}

	// A clean read still succeeds, so the truncation cases above isolate the end
	// of the envelope rather than a general read failure.
	metadata, err := ReadSnapshotWithKey(t.Context(), bytes.NewReader(data), &collectingSnapshotVisitor{}, testSnapshotEncryptionKey())
	require.NoError(t, err)
	require.Equal(t, uint64(1), metadata.NodeCount)
}

// snapshotWriteRecorder captures the complete output and the largest single
// write the envelope makes, so bounded buffering is observable.
type snapshotWriteRecorder struct {
	buffer   bytes.Buffer
	maxWrite int
}

func (r *snapshotWriteRecorder) Write(p []byte) (int, error) {
	if len(p) > r.maxWrite {
		r.maxWrite = len(p)
	}
	return r.buffer.Write(p)
}

func TestEncryptedLargeSnapshotRoundTripAndBoundedWrites(t *testing.T) {
	engine := newSnapshotBadger(t)
	marker := "large-encrypted-snapshot-marker-3c0a29e4"
	text := strings.Repeat("a", 1<<20) + marker + strings.Repeat("b", 2<<20)
	_, err := engine.CreateNode(t.Context(), &Node{ID: "nornic:large", Labels: []string{"Doc"}, Properties: map[string]any{"text": text}})
	require.NoError(t, err)
	key := testSnapshotEncryptionKey()

	recorder := &snapshotWriteRecorder{}
	require.NoError(t, WriteSnapshot(t.Context(), engine, recorder, SnapshotOptions{Sequence: 5, EncryptionKey: key}))
	require.LessOrEqual(t, recorder.maxWrite, 4+snapshotEncryptionChunkSize+16, "one frame is the largest single write")
	written := recorder.buffer.Bytes()
	require.False(t, bytes.Contains(written, []byte(marker)), "large encrypted snapshot holds plaintext")
	require.False(t, bytes.Contains(written, []byte(strings.Repeat("a", 4096))), "large encrypted snapshot holds a plaintext run")
	layout := parseSnapshotEnvelope(t, written)
	requireSnapshotEnvelopeFormat(t, layout)
	require.Greater(t, len(layout.frames), 40, "the large snapshot spans many chunks")
	require.Greater(t, layout.plaintextLength(), 3<<20)

	path := filepath.Join(t.TempDir(), "large-encrypted.nds")
	require.NoError(t, SaveStreamingSnapshot(t.Context(), engine, path, SnapshotOptions{Sequence: 5, EncryptionKey: key}))
	fileData, err := os.ReadFile(path)
	require.NoError(t, err)
	require.False(t, bytes.Contains(fileData, []byte(marker)))
	require.True(t, bytes.HasPrefix(fileData, []byte(encryptedSnapshotMagic)))

	visitor := &collectingSnapshotVisitor{}
	metadata, err := ReadSnapshotWithKey(t.Context(), bytes.NewReader(fileData), visitor, key)
	require.NoError(t, err)
	require.Equal(t, uint64(5), metadata.Sequence)
	require.Equal(t, uint64(1), metadata.NodeCount)
	require.Len(t, visitor.nodes, 1)
	require.Equal(t, text, visitor.nodes[0].Properties["text"])
}

func TestCompactionSnapshotEncryptsWhenKeySupplied(t *testing.T) {
	ctx := context.Background()
	engine := newTestMemoryEngine(t)
	secret := "sensitive-compaction-snapshot-80e1c7"
	_, err := engine.CreateNode(ctx, &Node{ID: "nornic:compaction", Labels: []string{"Doc"}, Properties: map[string]any{"secret": secret}})
	require.NoError(t, err)
	key := testSnapshotEncryptionKey()
	path := filepath.Join(t.TempDir(), "snapshot-compaction.nds")
	require.NoError(t, saveCompactionSnapshot(ctx, compactionSnapshot{engine: engine, sequence: 7, encryptionKey: key}, path))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.False(t, bytes.Contains(data, []byte(secret)))
	require.False(t, bytes.Contains(data, plaintextSnapshotSignature()))
	encrypted, err := IsEncryptedSnapshot(path)
	require.NoError(t, err)
	require.True(t, encrypted)
	visitor := &collectingSnapshotVisitor{}
	metadata, err := ReadSnapshotWithKey(ctx, bytes.NewReader(data), visitor, key)
	require.NoError(t, err)
	require.Equal(t, uint64(7), metadata.Sequence)
	require.Equal(t, uint64(1), metadata.NodeCount)
	require.Equal(t, secret, visitor.nodes[0].Properties["secret"])

	// Without a key the compaction snapshot keeps the plaintext behavior.
	plaintextPath := filepath.Join(t.TempDir(), "snapshot-plaintext.nds")
	require.NoError(t, saveCompactionSnapshot(ctx, compactionSnapshot{engine: engine, sequence: 8}, plaintextPath))
	encrypted, err = IsEncryptedSnapshot(plaintextPath)
	require.NoError(t, err)
	require.False(t, encrypted)
	metadata, err = ReadSnapshotWithKey(ctx, bytes.NewReader(mustReadSnapshotFile(t, plaintextPath)), &collectingSnapshotVisitor{}, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(8), metadata.Sequence)
}

func mustReadSnapshotFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

package nornicdb

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackupFormatDetectionDoesNotTreatNativeLengthAsJSON(t *testing.T) {
	for _, tc := range []struct {
		name   string
		length uint64
	}{
		{name: "object-prefix", length: '{'},
		{name: "array-prefix", length: '['},
		{name: "whitespace-then-object-prefix", length: uint64('{')<<8 | uint64(' ')},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Badger's native stream starts with an eight-byte little-endian
			// frame length. Its low bytes can resemble JSON punctuation
			// after TrimSpace removes an initial ASCII whitespace byte.
			data := make([]byte, 8+int(tc.length))
			binary.LittleEndian.PutUint64(data[:8], tc.length)
			data[8] = 0x0a // KVList's first protobuf field.
			path := filepath.Join(t.TempDir(), "native.backup")
			require.NoError(t, os.WriteFile(path, data, 0600))
			isJSON, err := isJSONBackupFile(path)
			require.NoError(t, err)
			require.False(t, isJSON)
		})
	}

	path := filepath.Join(t.TempDir(), "legacy.json")
	require.NoError(t, os.WriteFile(path, []byte(" \n {\"version\":\"2.0\",\"nodes\":[],\"edges\":[]}"), 0600))
	isJSON, err := isJSONBackupFile(path)
	require.NoError(t, err)
	require.True(t, isJSON)

	shortPath := filepath.Join(t.TempDir(), "short.json")
	require.NoError(t, os.WriteFile(shortPath, []byte("{}"), 0600))
	isJSON, err = isJSONBackupFile(shortPath)
	require.NoError(t, err)
	require.True(t, isJSON)
}

func TestRestoreNativeBackupWhoseLengthStartsWithJSONPunctuation(t *testing.T) {
	ctx := context.Background()
	source, err := Open(t.TempDir(), nil)
	require.NoError(t, err)
	want, err := source.CreateNode(ctx, []string{"Restored"}, map[string]any{"name": "native-frame"})
	require.NoError(t, err)
	backupPath := filepath.Join(t.TempDir(), "native.backup")
	require.NoError(t, source.Backup(ctx, backupPath))
	require.NoError(t, source.Close())

	backup, err := os.ReadFile(backupPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(backup), 9)
	frameLength := binary.LittleEndian.Uint64(backup[:8])
	require.LessOrEqual(t, frameLength, uint64(len(backup)-8))

	// Add an ignored protobuf field to the first KVList until its framed
	// length starts with '{'. The resulting stream is still a native backup.
	delta := int((uint64('{') - frameLength%256 + 256) % 256)
	if delta < 3 {
		delta += 256
	}
	var unknown []byte
	for len(unknown) == 0 {
		for payloadLength := 0; payloadLength <= delta; payloadLength++ {
			var length [binary.MaxVarintLen64]byte
			lengthBytes := binary.PutUvarint(length[:], uint64(payloadLength))
			if 1+lengthBytes+payloadLength != delta {
				continue
			}
			unknown = append([]byte{0x7a}, length[:lengthBytes]...)
			unknown = append(unknown, make([]byte, payloadLength)...)
			break
		}
		if len(unknown) == 0 {
			delta += 256
		}
	}
	mutated := make([]byte, 0, len(backup)+len(unknown))
	mutated = append(mutated, backup[:8+frameLength]...)
	mutated = append(mutated, unknown...)
	mutated = append(mutated, backup[8+frameLength:]...)
	binary.LittleEndian.PutUint64(mutated[:8], frameLength+uint64(len(unknown)))
	require.Equal(t, byte('{'), mutated[0])
	require.NoError(t, os.WriteFile(backupPath, mutated, 0600))

	target, err := Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, target.Close()) })
	require.NoError(t, target.Restore(ctx, backupPath))
	got, err := target.GetNode(ctx, want.ID)
	require.NoError(t, err)
	require.Equal(t, "native-frame", got.Properties["name"])
}

package search

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

type countingWriter struct {
	writes int
	bytes.Buffer
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.writes++
	return w.Buffer.Write(p)
}

func TestMsgpackSnapshotEncodingCoalescesWrites(t *testing.T) {
	w := &countingWriter{}
	payload := make([]string, 50_000)
	for i := range payload {
		payload[i] = "search-index-term"
	}

	require.NoError(t, encodeMsgpackBuffered(w, payload))
	require.Less(t, w.writes, 10, "large snapshots must not issue one write per msgpack element")

	var decoded []string
	require.NoError(t, decodeMsgpackReader(bytes.NewReader(w.Bytes()), &decoded))
	require.Equal(t, payload, decoded)
}

func BenchmarkMsgpackSnapshotEncoding(b *testing.B) {
	payload := make([]string, 100_000)
	for i := range payload {
		payload[i] = "search-index-term"
	}
	file, err := os.CreateTemp(b.TempDir(), "snapshot-*.msgpack")
	require.NoError(b, err)
	defer file.Close()

	reset := func() {
		require.NoError(b, file.Truncate(0))
		_, err := file.Seek(0, 0)
		require.NoError(b, err)
	}
	b.Run("unbuffered", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			reset()
			require.NoError(b, msgpack.NewEncoder(file).Encode(payload))
		}
	})
	b.Run("buffered", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			reset()
			require.NoError(b, encodeMsgpackBuffered(file, payload))
		}
	})
}

func TestWriteMsgpackSnapshots(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "bundle")
	err := writeMsgpackSnapshots(target, map[string]any{
		"a.msgpack": map[string]any{"a": 1},
		"b.msgpack": map[string]any{"b": 2},
	})
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(target, "a.msgpack"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(target, "b.msgpack"))
	require.NoError(t, err)
}

func TestWriteMsgpackSnapshotsAtomic(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "bundle")

	require.NoError(t, writeMsgpackSnapshotsAtomic(target, map[string]any{
		"state.msgpack": map[string]any{"v": 1},
	}))
	require.NoError(t, writeMsgpackSnapshotsAtomic(target, map[string]any{
		"state.msgpack": map[string]any{"v": 2},
		"meta.msgpack":  map[string]any{"m": 1},
	}))

	_, err := os.Stat(filepath.Join(target, "state.msgpack"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(target, "meta.msgpack"))
	require.NoError(t, err)
}

func TestWriteMsgpackSnapshot_ErrorOnInvalidParent(t *testing.T) {
	dir := t.TempDir()
	parentFile := filepath.Join(dir, "not-a-dir")
	require.NoError(t, os.WriteFile(parentFile, []byte("x"), 0o644))

	err := writeMsgpackSnapshot(filepath.Join(parentFile, "snapshot.msgpack"), map[string]any{"x": 1})
	require.Error(t, err)
}

func TestWriteMsgpackSnapshotPreservesPreviousFileWhenEncodingFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.msgpack")
	require.NoError(t, writeMsgpackSnapshot(path, map[string]any{"version": 1}))

	err := writeMsgpackSnapshot(path, map[string]any{"invalid": make(chan int)})
	require.Error(t, err)

	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	var snapshot map[string]any
	require.NoError(t, decodeMsgpackReader(file, &snapshot))
	require.EqualValues(t, 1, snapshot["version"])
	_, err = os.Stat(path + ".tmp")
	require.True(t, os.IsNotExist(err))
}

func decodeMsgpackReader(reader io.Reader, target any) error {
	return msgpack.NewDecoder(reader).Decode(target)
}

func TestWriteMsgpackSnapshotsAndAtomic_ErrorPaths(t *testing.T) {
	t.Run("write snapshots fails on invalid parent path", func(t *testing.T) {
		dir := t.TempDir()
		parentFile := filepath.Join(dir, "not-a-dir")
		require.NoError(t, os.WriteFile(parentFile, []byte("x"), 0o644))

		err := writeMsgpackSnapshots(filepath.Join(parentFile, "bundle"), map[string]any{"a.msgpack": map[string]any{"a": 1}})
		require.Error(t, err)
	})

	t.Run("atomic write propagates encode errors and does not leave target", func(t *testing.T) {
		target := filepath.Join(t.TempDir(), "bundle")
		err := writeMsgpackSnapshotsAtomic(target, map[string]any{
			"bad.msgpack": make(chan int), // msgpack cannot encode channels
		})
		require.Error(t, err)
		_, statErr := os.Stat(target)
		require.True(t, os.IsNotExist(statErr))
	})

	t.Run("atomic write fails when parent is a file", func(t *testing.T) {
		dir := t.TempDir()
		parentFile := filepath.Join(dir, "not-a-dir")
		require.NoError(t, os.WriteFile(parentFile, []byte("x"), 0o644))

		err := writeMsgpackSnapshotsAtomic(filepath.Join(parentFile, "bundle"), map[string]any{
			"state.msgpack": map[string]any{"v": 1},
		})
		require.Error(t, err)
	})
}

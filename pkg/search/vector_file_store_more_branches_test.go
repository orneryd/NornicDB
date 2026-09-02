package search

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVectorFileStore_MoreAddAndIterateBranches(t *testing.T) {
	t.Run("add_seek_write_and_late_closed_paths", func(t *testing.T) {
		base := filepath.Join(t.TempDir(), "vectors")
		vfs, err := NewVectorFileStore(base, 2)
		require.NoError(t, err)
		defer func() { _ = vfs.Close() }()

		// Seek error branch: closed file handle but store not marked closed.
		vfs.mu.Lock()
		require.NoError(t, vfs.file.Close())
		vfs.mu.Unlock()
		err = vfs.Add("id-seek", []float32{1, 0})
		require.Error(t, err)

		// Reopen a clean store for remaining Add branches.
		vfs2, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors2"), 2)
		require.NoError(t, err)
		defer func() { _ = vfs2.Close() }()

		vfs2.writeRecord = func(*os.File, string, []float32) error { return io.ErrUnexpectedEOF }
		err = vfs2.Add("id-write", []float32{1, 0})
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)

		vfs2.writeRecord = func(*os.File, string, []float32) error {
			vfs2.mu.Lock()
			vfs2.closed = true
			vfs2.mu.Unlock()
			return nil
		}
		err = vfs2.Add("id-closed", []float32{1, 0})
		require.ErrorIs(t, err, errVecFileClosed)
	})

	t.Run("iterate_seek_error_and_empty_file_final_nil", func(t *testing.T) {
		base := filepath.Join(t.TempDir(), "vectors")
		vfs, err := NewVectorFileStore(base, 2)
		require.NoError(t, err)
		defer func() { _ = vfs.Close() }()
		require.NoError(t, vfs.Add("id-1", []float32{1, 0}))

		vfs.mu.Lock()
		require.NoError(t, vfs.file.Close())
		vfs.mu.Unlock()
		err = vfs.IterateChunked(1, func([]string, [][]float32) error { return nil })
		require.Error(t, err)

		vfs2, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors2"), 2)
		require.NoError(t, err)
		defer func() { _ = vfs2.Close() }()
		calls := 0
		err = vfs2.IterateChunked(1, func([]string, [][]float32) error {
			calls++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 0, calls)
	})

	t.Run("iterate_second_read_unexpected_eof", func(t *testing.T) {
		base := filepath.Join(t.TempDir(), "vectors")
		vfs, err := NewVectorFileStore(base, 2)
		require.NoError(t, err)
		defer func() { _ = vfs.Close() }()

		f, err := os.OpenFile(base+".vec", os.O_RDWR|os.O_APPEND, 0o644)
		require.NoError(t, err)
		_, err = f.Write([]byte("partial"))
		require.NoError(t, err)
		require.NoError(t, f.Close())
		vfs.mu.Lock()
		vfs.idToOrdinal["partial"] = 0
		vfs.mu.Unlock()

		err = vfs.IterateChunked(1, func([]string, [][]float32) error { return nil })
		require.Error(t, err)
	})
}

func TestVectorFileStore_MoreLoadRebuildAndCompactBranches(t *testing.T) {
	t.Run("load_open_meta_error_branch", func(t *testing.T) {
		base := filepath.Join(t.TempDir(), "vectors")
		vfs, err := NewVectorFileStore(base, 2)
		require.NoError(t, err)
		defer func() { _ = vfs.Close() }()

		badDir := filepath.Join(t.TempDir(), "meta-as-dir")
		require.NoError(t, os.MkdirAll(badDir, 0o755))
		require.NoError(t, os.Chmod(badDir, 0o000))
		t.Cleanup(func() { _ = os.Chmod(badDir, 0o755) })
		vfs.metaPath = filepath.Join(badDir, "meta")
		err = vfs.Load()
		require.Error(t, err)
	})

	t.Run("load_requires_metadata_for_non_empty_slab", func(t *testing.T) {
		base := filepath.Join(t.TempDir(), "vectors")
		vfs, err := NewVectorFileStore(base, 2)
		require.NoError(t, err)
		defer func() { _ = vfs.Close() }()

		require.NoError(t, vfs.Add("id-1", []float32{1, 0}))
		require.Error(t, vfs.Load())
	})

	t.Run("compact_clamps_and_stat_error", func(t *testing.T) {
		t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_OBSOLETE", "-5")
		t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_SIZE_MB", "-3")
		t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_DEAD_RATIO", "-1")

		base := filepath.Join(t.TempDir(), "vectors")
		vfs, err := NewVectorFileStore(base, 2)
		require.NoError(t, err)
		defer func() { _ = vfs.Close() }()

		require.NoError(t, vfs.Add("id-1", []float32{1, 0}))
		require.NoError(t, vfs.Add("id-1", []float32{0, 1})) // obsoleteCount > 0

		vfs.mu.Lock()
		require.NoError(t, vfs.file.Close())
		vfs.mu.Unlock()
		compacted, err := vfs.CompactIfNeeded()
		require.False(t, compacted)
		require.Error(t, err)
	})

	t.Run("rewrite_vec_locked_happy_path_and_missing_id", func(t *testing.T) {
		base := filepath.Join(t.TempDir(), "vectors")
		vfs, err := NewVectorFileStore(base, 2)
		require.NoError(t, err)
		defer func() { _ = vfs.Close() }()

		require.NoError(t, vfs.Add("id-1", []float32{1, 0}))
		require.NoError(t, vfs.Add("id-2", []float32{0, 1}))

		vfs.mu.Lock()
		err = vfs.rewriteVecLocked([]string{"missing", "id-2", "id-1"})
		vfs.mu.Unlock()
		require.NoError(t, err)

		require.Equal(t, 2, vfs.Count())
		vec1, ok := vfs.GetVector("id-1")
		require.True(t, ok)
		require.Len(t, vec1, 2)
		require.InDelta(t, 1.0, vec1[0], 1e-5)
		require.InDelta(t, 0.0, vec1[1], 1e-5)
		vec2, ok := vfs.GetVector("id-2")
		require.True(t, ok)
		require.Len(t, vec2, 2)
		require.InDelta(t, 0.0, vec2[0], 1e-5)
		require.InDelta(t, 1.0, vec2[1], 1e-5)
	})

	t.Run("rewrite_vec_locked_empty_ids_clears_index", func(t *testing.T) {
		base := filepath.Join(t.TempDir(), "vectors")
		vfs, err := NewVectorFileStore(base, 2)
		require.NoError(t, err)
		defer func() { _ = vfs.Close() }()

		require.NoError(t, vfs.Add("id-1", []float32{1, 0}))

		vfs.mu.Lock()
		err = vfs.rewriteVecLocked(nil)
		vfs.mu.Unlock()
		require.NoError(t, err)
		require.Equal(t, 0, vfs.Count())
	})
}

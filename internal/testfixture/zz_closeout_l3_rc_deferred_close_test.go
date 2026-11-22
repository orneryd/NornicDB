package testfixture_test

// Closeout lane L3, P-N152-131 (NOR-361) deferred half: ExtractZip
// (internal/testfixture/archive.go:18-20) opens the archive with
// zip.OpenReader and runs `defer archive.Close()`, dropping its result. The
// handle is the *zip.ReadCloser's own read-only *os.File, opened and closed
// exactly once inside the function. This run arranges the plainest conditions
// under which a close could fail and records the Close result of the same
// zip.OpenReader handle in each, then runs ExtractZip itself on the same
// targets. Control: a second Close of the same handle returns an error, so
// Close does report a failure when one occurs. Test-only.

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func closeoutL3WriteZip(t *testing.T, path string) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	w := zip.NewWriter(f)
	entry, err := w.Create("data/member.txt")
	require.NoError(t, err)
	_, err = entry.Write([]byte("closeout-l3 member"))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())
}

func TestCloseoutL3_RC_N152131_DeferredArchiveCloseCannotFail(t *testing.T) {
	type target struct {
		name    string
		prepare func(t *testing.T) string
		unlink  bool
	}
	targets := []target{
		{name: "ordinary", prepare: func(t *testing.T) string {
			p := filepath.Join(t.TempDir(), "a.zip")
			closeoutL3WriteZip(t, p)
			return p
		}},
		{name: "read_only_file_and_dir", prepare: func(t *testing.T) string {
			dir := t.TempDir()
			p := filepath.Join(dir, "a.zip")
			closeoutL3WriteZip(t, p)
			require.NoError(t, os.Chmod(p, 0o400))
			require.NoError(t, os.Chmod(dir, 0o500))
			t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
			return p
		}},
		{name: "unlinked_while_open", unlink: true, prepare: func(t *testing.T) string {
			p := filepath.Join(t.TempDir(), "a.zip")
			closeoutL3WriteZip(t, p)
			return p
		}},
	}
	if st, err := os.Stat("/dev/shm"); err == nil && st.IsDir() {
		targets = append(targets, target{name: "tmpfs_dev_shm", prepare: func(t *testing.T) string {
			dir, err := os.MkdirTemp("/dev/shm", "closeout-l3-")
			require.NoError(t, err)
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			p := filepath.Join(dir, "a.zip")
			closeoutL3WriteZip(t, p)
			return p
		}})
	}
	for _, tc := range targets {
		t.Run(tc.name, func(t *testing.T) {
			path := tc.prepare(t)
			archive, err := zip.OpenReader(path) // the same call as archive.go:18
			require.NoError(t, err, "PRECONDITION: open")
			if tc.unlink {
				require.NoError(t, os.Remove(path))
			}
			first := archive.Close() // the result archive.go:20 drops
			second := archive.Close()
			t.Logf("CLOSEOUT_L3_RC N152131 deferred target=%s firstClose=%v secondClose=%v", tc.name, first, second)
			require.NoError(t, first, "a single Close of the OpenReader handle failed on %s: the dropped result would carry a real error", tc.name)
			require.Error(t, second, "control: a second Close must report an error (Close does report failures)")
			if tc.unlink {
				return
			}
			root := testfixture.ExtractZip(t, path, "")
			got, err := os.ReadFile(filepath.Join(root, "data", "member.txt"))
			require.NoError(t, err)
			require.Equal(t, "closeout-l3 member", string(got))
			t.Logf("CLOSEOUT_L3_RC N152131 deferred target=%s ExtractZip ok", tc.name)
		})
	}
}

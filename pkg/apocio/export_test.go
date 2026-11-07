package apocio

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExportAdmissionRequiresExistingCapability(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  context.Context
		want error
	}{
		{"missing", t.Context(), ErrCapabilityMissing},
		{"denied", WithCapability(t.Context(), NewPolicy(Config{}).Capability()), ErrLocalExportFileAccessDisabled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "nested", "out.txt")
			_, err := AdmitExport(tc.ctx, path)
			require.ErrorIs(t, err, tc.want)
			_, err = os.Stat(filepath.Dir(path))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
	d, err := AdmitExport(t.Context(), "")
	require.NoError(t, err)
	require.NoError(t, d.Write(t.Context(), []byte("no file"), CreateDirectories))
}

// These preserve the destination-owner controls formerly located on Cypher's
// private writeExport method, now retired in favor of this shared owner.
func TestExportDestinationObservesCallerAndDirectoryPolicy(t *testing.T) {
	for _, directories := range []ExportDirectoryPolicy{ExistingDirectory, CreateDirectories} {
		t.Run("canceled after admission/"+string(rune('0'+directories)), func(t *testing.T) {
			root := t.TempDir()
			ctx := WithCapability(t.Context(), NewPolicy(Config{AllowExportFileAccess: true}).Capability())
			out := filepath.Join(root, "made", "export.out")
			d, err := AdmitExport(ctx, out)
			require.NoError(t, err)
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			require.ErrorIs(t, d.Write(canceled, []byte("content"), directories), context.Canceled)
			_, err = os.Stat(filepath.Dir(out))
			require.ErrorIs(t, err, os.ErrNotExist)
			_, err = AdmitExport(canceled, out)
			require.ErrorIs(t, err, context.Canceled)
		})
	}
	root := t.TempDir()
	ctx := WithCapability(t.Context(), NewPolicy(Config{AllowExportFileAccess: true}).Capability())
	out := filepath.Join(root, "made", "export.out")
	d, err := AdmitExport(ctx, out)
	require.NoError(t, err)
	require.Error(t, d.Write(ctx, []byte("content"), ExistingDirectory))
	_, err = os.Stat(filepath.Dir(out))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, d.Write(ctx, []byte("content"), CreateDirectories))
	content, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Equal(t, "content", string(content))
	require.NoError(t, d.Write(ctx, []byte("replacement"), ExistingDirectory))
	content, err = os.ReadFile(out)
	require.NoError(t, err)
	require.Equal(t, "replacement", string(content))
}

func TestExportDestinationUsesRootedWriter(t *testing.T) {
	root := t.TempDir()
	ctx := WithCapability(t.Context(), NewPolicy(Config{AllowExportFileAccess: true, FileAccessRoot: root}).Capability())
	d, err := AdmitExport(ctx, "nested/export.txt")
	require.NoError(t, err)
	require.NoError(t, d.Write(ctx, []byte("rooted"), CreateDirectories))
	content, err := os.ReadFile(filepath.Join(root, "nested", "export.txt"))
	require.NoError(t, err)
	require.Equal(t, "rooted", string(content))
	rebased, err := AdmitExport(ctx, "../rebased.txt")
	require.NoError(t, err)
	require.NoError(t, rebased.Write(ctx, []byte("still rooted"), CreateDirectories))
	content, err = os.ReadFile(filepath.Join(root, "rebased.txt"))
	require.NoError(t, err)
	require.Equal(t, "still rooted", string(content))
	_, err = os.Stat(filepath.Join(filepath.Dir(root), "rebased.txt"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

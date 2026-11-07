package apocio

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExportAdmissionRequiresExistingCapability(t *testing.T) {
	canceled, cancel := context.WithCancel(WithCapability(t.Context(), NewPolicy(Config{}).Capability()))
	cancel()
	for _, tc := range []struct {
		name string
		ctx  context.Context
		want error
	}{
		{"missing", t.Context(), ErrCapabilityMissing},
		{"canceled denied", canceled, context.Canceled},
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
	require.NoError(t, WriteExport(t.Context(), d, []byte("no file"), CreateDirectories))
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
			require.ErrorIs(t, WriteExport(canceled, d, []byte("content"), directories), context.Canceled)
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
	require.Error(t, WriteExport(ctx, d, []byte("content"), ExistingDirectory))
	_, err = os.Stat(filepath.Dir(out))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, WriteExport(ctx, d, []byte("content"), CreateDirectories))
	content, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Equal(t, "content", string(content))
	require.NoError(t, WriteExport(ctx, d, []byte("replacement"), ExistingDirectory))
	content, err = os.ReadFile(out)
	require.NoError(t, err)
	require.Equal(t, "replacement", string(content))
}

func TestExportDestinationUsesRootedWriter(t *testing.T) {
	root := t.TempDir()
	ctx := WithCapability(t.Context(), NewPolicy(Config{AllowExportFileAccess: true, FileAccessRoot: root}).Capability())
	d, err := AdmitExport(ctx, "nested/export.txt")
	require.NoError(t, err)
	require.NoError(t, WriteExport(ctx, d, []byte("rooted"), CreateDirectories))
	content, err := os.ReadFile(filepath.Join(root, "nested", "export.txt"))
	require.NoError(t, err)
	require.Equal(t, "rooted", string(content))
	rebased, err := AdmitExport(ctx, "../rebased.txt")
	require.NoError(t, err)
	require.NoError(t, WriteExport(ctx, rebased, []byte("still rooted"), CreateDirectories))
	content, err = os.ReadFile(filepath.Join(root, "rebased.txt"))
	require.NoError(t, err)
	require.Equal(t, "still rooted", string(content))
	_, err = os.Stat(filepath.Join(filepath.Dir(root), "rebased.txt"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestExportDestinationRejectsUnknownDirectoryPolicy(t *testing.T) {
	root := t.TempDir()
	ctx := WithCapability(t.Context(), NewPolicy(Config{AllowExportFileAccess: true}).Capability())
	destination, err := AdmitExport(ctx, filepath.Join(root, "missing", "out.txt"))
	require.NoError(t, err)
	require.Error(t, WriteExport(ctx, destination, []byte("content"), ExportDirectoryPolicy(99)))
	_, err = os.Stat(filepath.Join(root, "missing"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

// TestWriteExportEmptyDestinationAllocatesNothing pins the empty-destination
// contract of the shared writer: a call that names no file is decided before the
// content becomes a byte slice, so neither representation is copied and no
// filesystem work is attempted. Both 1 MiB payloads are built before measurement,
// so only the measured call can account for an allocation, and the error is
// inspected outside the measured closure.
func TestWriteExportEmptyDestinationAllocatesNothing(t *testing.T) {
	root := t.TempDir()
	ctx := WithCapability(t.Context(), NewPolicy(Config{AllowExportFileAccess: true, FileAccessRoot: root}).Capability())
	destination, err := AdmitExport(ctx, "")
	require.NoError(t, err)
	text := strings.Repeat("x", 1<<20)
	payload := []byte(text)

	t.Run("string", func(t *testing.T) {
		var writeErr error
		allocations := testing.AllocsPerRun(5, func() {
			writeErr = WriteExport(ctx, destination, text, CreateDirectories)
		})
		require.NoError(t, writeErr, "an empty destination reports no error")
		require.Zero(t, allocations, "an empty destination must not convert or copy the text payload")
	})

	t.Run("[]byte", func(t *testing.T) {
		var writeErr error
		allocations := testing.AllocsPerRun(5, func() {
			writeErr = WriteExport(ctx, destination, payload, CreateDirectories)
		})
		require.NoError(t, writeErr, "an empty destination reports no error")
		require.Zero(t, allocations, "an empty destination must not allocate for the byte payload")
	})

	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Empty(t, entries, "an empty destination performs no filesystem work")
}

// TestWriteExportNamedDestinationWritesEitherRepresentationExactly covers the
// allowed side of the same generic writer: a named destination receives the
// caller's content byte for byte from either representation, including a
// replacement write over an existing file.
func TestWriteExportNamedDestinationWritesEitherRepresentationExactly(t *testing.T) {
	root := t.TempDir()
	ctx := WithCapability(t.Context(), NewPolicy(Config{AllowExportFileAccess: true}).Capability())
	text := "<graphml>text content \u2713</graphml>\n"
	binary := []byte{0x00, 0x01, 0xff, 0x80, 'A', '\n', 0x00}

	for _, tc := range []struct {
		name     string
		write    func(ExportDestination) error
		want     []byte
		replace  func(ExportDestination) error
		replaced []byte
	}{
		{
			name: "string",
			write: func(destination ExportDestination) error {
				return WriteExport(ctx, destination, text, CreateDirectories)
			},
			want: []byte(text),
			replace: func(destination ExportDestination) error {
				return WriteExport(ctx, destination, "replacement text", CreateDirectories)
			},
			replaced: []byte("replacement text"),
		},
		{
			name: "[]byte",
			write: func(destination ExportDestination) error {
				return WriteExport(ctx, destination, binary, CreateDirectories)
			},
			want: binary,
			replace: func(destination ExportDestination) error {
				return WriteExport(ctx, destination, binary[:3], CreateDirectories)
			},
			replaced: binary[:3],
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := filepath.Join(root, tc.name, "export.out")
			destination, err := AdmitExport(ctx, out)
			require.NoError(t, err)
			require.NoError(t, tc.write(destination))
			written, err := os.ReadFile(out)
			require.NoError(t, err)
			require.Equal(t, tc.want, written)
			require.NoError(t, tc.replace(destination))
			written, err = os.ReadFile(out)
			require.NoError(t, err)
			require.Equal(t, tc.replaced, written)
		})
	}
}

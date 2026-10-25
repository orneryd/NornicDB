package testfixture

import (
	"archive/zip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// ExtractZip unpacks a checked-in archive into a test-owned directory. trimPrefix
// lets a public constructor use the same fixture with its different disk layout.
func ExtractZip(t *testing.T, archivePath, trimPrefix string) string {
	t.Helper()
	archive, err := zip.OpenReader(archivePath)
	require.NoError(t, err)
	defer archive.Close()
	root := t.TempDir()
	for _, file := range archive.File {
		require.True(t, filepath.IsLocal(file.Name))
		name := strings.TrimPrefix(file.Name, trimPrefix)
		require.True(t, filepath.IsLocal(name))
		path := filepath.Join(root, filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
		input, err := file.Open()
		require.NoError(t, err)
		output, err := os.Create(path)
		if err != nil {
			_ = input.Close()
			t.Fatal(err)
		}
		_, copyErr := io.Copy(output, input)
		outputErr, inputErr := output.Close(), input.Close()
		require.NoError(t, copyErr)
		require.NoError(t, outputErr)
		require.NoError(t, inputErr)
	}
	return root
}

package testfixture

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWalkRepositoryTreeKeepsRootSourcesAndExcludesIndependentCheckouts(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"go.mod", "root.go", "pkg/owned.go", "ignored/owned.go", ".ci-workflow-tools/owned.go", "nested-module/go.mod", "nested-module/owned.go", "arbitrary-helper/.git", "arbitrary-helper/foreign.go", "clone/.git/HEAD", "clone/foreign.go"} {
		path := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, nil, 0o644))
	}
	var sources []string
	err := WalkRepositoryTree(root+string(filepath.Separator)+".", func(path string, entry fs.DirEntry, err error) error {
		require.NoError(t, err)
		if entry.IsDir() && entry.Name() == "ignored" {
			return filepath.SkipDir
		}
		if !entry.IsDir() && strings.HasSuffix(path, ".go") {
			relative, err := filepath.Rel(root, path)
			require.NoError(t, err)
			sources = append(sources, filepath.ToSlash(relative))
		}
		return nil
	})
	require.NoError(t, err)
	sort.Strings(sources)
	require.Equal(t, []string{".ci-workflow-tools/owned.go", "nested-module/owned.go", "pkg/owned.go", "root.go"}, sources,
		"checkout identity, not a directory-name exception, defines the audited source population")
}

func TestWalkRepositoryTreePropagatesFailures(t *testing.T) {
	root := t.TempDir()
	want := errors.New("visitor failed")
	require.ErrorIs(t, WalkRepositoryTree(root, func(string, fs.DirEntry, error) error { return want }), want)
	require.ErrorIs(t, WalkRepositoryTree(filepath.Join(root, "missing"), func(string, fs.DirEntry, error) error {
		t.Fatal("a failed traversal must not supply a successful visitor entry")
		return nil
	}), fs.ErrNotExist)
}

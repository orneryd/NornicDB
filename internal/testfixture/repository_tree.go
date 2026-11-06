package testfixture

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// WalkRepositoryTree visits one checkout without entering nested Git checkouts.
// Verification tools may check out another revision below the repository root;
// its .git directory or worktree file identifies a separate source population,
// regardless of directory name. Ordinary nested Go modules remain visible.
// The visitor retains its own file and directory policy, and isolated source
// fixtures need no Git metadata at their root.
func WalkRepositoryTree(root string, visit fs.WalkDirFunc) error {
	root = filepath.Clean(root)
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if visitErr := visit(path, entry, nil); visitErr != nil {
			return visitErr
		}
		if entry.IsDir() && path != root {
			_, statErr := os.Stat(filepath.Join(path, ".git"))
			if statErr == nil {
				return filepath.SkipDir
			}
			if statErr != nil && !errors.Is(statErr, fs.ErrNotExist) {
				return statErr
			}
		}
		return nil
	})
}

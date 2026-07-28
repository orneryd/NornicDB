package security

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// OpenRootedFile opens a canonical file path through an os.Root rooted at its
// parent directory. Traversal components and paths whose cleaned form differs
// from the supplied path are rejected before any filesystem operation.
func OpenRootedFile(path string, flag int, perm os.FileMode) (*RootedFile, error) {
	dir, name, err := splitCanonicalFilePath(path)
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	file, err := root.OpenFile(name, flag, perm)
	if err != nil {
		root.Close()
		return nil, err
	}
	return &RootedFile{File: file, root: root}, nil
}

// CreateRootedFile creates or truncates a canonical path through os.Root.
func CreateRootedFile(path string, perm os.FileMode) (*RootedFile, error) {
	return OpenRootedFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, perm)
}

// WriteRootedFile writes data to a canonical path through os.Root.
func WriteRootedFile(path string, data []byte, perm os.FileMode) error {
	file, err := CreateRootedFile(path, perm)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

// EnsureRootedParent creates the parent directory of a canonical path through
// an os.Root capability.
func EnsureRootedParent(path string, perm os.FileMode) error {
	root, name, err := openPathRoot(path)
	if err != nil {
		return err
	}
	defer root.Close()
	return root.MkdirAll(filepath.Dir(name), perm)
}

// RootedStat returns information about a canonical path through an os.Root capability.
func RootedStat(path string) (os.FileInfo, error) {
	root, name, err := openPathRoot(path)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	return root.Stat(name)
}

// RemoveAllRootedPath removes a canonical path through an os.Root capability.
func RemoveAllRootedPath(path string) error {
	root, name, err := openPathRoot(path)
	if err != nil {
		return err
	}
	defer root.Close()
	return root.RemoveAll(name)
}

// RemoveRootedPath removes a canonical file or empty directory through an
// os.Root capability.
func RemoveRootedPath(path string) error {
	root, name, err := openPathRoot(path)
	if err != nil {
		return err
	}
	defer root.Close()
	return root.Remove(name)
}

// ReadRootedDir reads a canonical directory through an os.Root capability.
func ReadRootedDir(path string) ([]os.DirEntry, error) {
	root, name, err := openPathRoot(path)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	dir, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	return dir.ReadDir(-1)
}

// RenameRootedFile atomically renames canonical paths in the same directory
// through a single os.Root capability.
func RenameRootedFile(oldPath, newPath string) error {
	oldDir, oldName, err := splitCanonicalFilePath(oldPath)
	if err != nil {
		return err
	}
	newDir, newName, err := splitCanonicalFilePath(newPath)
	if err != nil {
		return err
	}
	if oldDir != newDir {
		return fmt.Errorf("rooted rename requires paths in the same directory")
	}
	root, err := os.OpenRoot(oldDir)
	if err != nil {
		return err
	}
	defer root.Close()
	return root.Rename(oldName, newName)
}

// RootedFile retains the root capability for the lifetime of an open file.
type RootedFile struct {
	*os.File
	root *os.Root
}

// Close closes both the file and its root capability.
func (f *RootedFile) Close() error {
	fileErr := f.File.Close()
	rootErr := f.root.Close()
	if fileErr != nil {
		return fileErr
	}
	return rootErr
}

func splitCanonicalFilePath(path string) (string, string, error) {
	if path == "" || strings.ContainsRune(path, '\x00') {
		return "", "", fmt.Errorf("invalid empty or NUL-containing path")
	}
	clean := filepath.Clean(path)
	if clean != path {
		return "", "", fmt.Errorf("path must be canonical: %q", path)
	}
	name := filepath.Base(clean)
	if name == "." || name == string(filepath.Separator) {
		return "", "", fmt.Errorf("path must identify a file: %q", path)
	}
	dir := filepath.Dir(clean)
	return dir, name, nil
}

func openPathRoot(path string) (*os.Root, string, error) {
	if _, _, err := splitCanonicalFilePath(path); err != nil {
		return nil, "", err
	}
	rootPath := "."
	if filepath.IsAbs(path) {
		rootPath = filepath.VolumeName(path) + string(filepath.Separator)
	}
	name, err := filepath.Rel(rootPath, path)
	if err != nil {
		return nil, "", err
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, "", err
	}
	return root, name, nil
}

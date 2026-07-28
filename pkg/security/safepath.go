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

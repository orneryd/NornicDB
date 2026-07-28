package security

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCreateRootedFileRejectsTraversal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested") + string(filepath.Separator) + ".." + string(filepath.Separator) + "escape"
	if _, err := CreateRootedFile(path, 0o600); err == nil {
		t.Fatal("expected non-canonical traversal path to be rejected")
	}
}

func TestCreateRootedFileWritesCanonicalPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot")
	file, err := CreateRootedFile(path, 0o600)
	if err != nil {
		t.Fatalf("CreateRootedFile: %v", err)
	}
	if _, err := file.WriteString("ok"); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "ok" {
		t.Fatalf("read created file: data=%q err=%v", data, err)
	}
}

func TestRootedPathLifecycle(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested")
	oldPath := filepath.Join(dir, "old")
	newPath := filepath.Join(dir, "new")

	if err := EnsureRootedParent(oldPath, 0o755); err != nil {
		t.Fatalf("EnsureRootedParent: %v", err)
	}
	if err := WriteRootedFile(oldPath, []byte("ok"), 0o600); err != nil {
		t.Fatalf("WriteRootedFile: %v", err)
	}
	if _, err := RootedStat(oldPath); err != nil {
		t.Fatalf("RootedStat: %v", err)
	}
	if err := RenameRootedFile(oldPath, newPath); err != nil {
		t.Fatalf("RenameRootedFile: %v", err)
	}
	entries, err := ReadRootedDir(dir)
	if err != nil {
		t.Fatalf("ReadRootedDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "new" {
		t.Fatalf("unexpected directory entries: %v", entries)
	}
	if err := RemoveRootedPath(newPath); err != nil {
		t.Fatalf("RemoveRootedPath: %v", err)
	}
	if err := RemoveAllRootedPath(dir); err != nil {
		t.Fatalf("RemoveAllRootedPath: %v", err)
	}
}

func TestRootedPathOperationsRejectTraversal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested") + string(filepath.Separator) + ".." + string(filepath.Separator) + "escape"
	operations := map[string]func() error{
		"ensure parent": func() error { return EnsureRootedParent(path, 0o755) },
		"stat": func() error {
			_, err := RootedStat(path)
			return err
		},
		"remove":     func() error { return RemoveRootedPath(path) },
		"remove all": func() error { return RemoveAllRootedPath(path) },
		"read dir": func() error {
			_, err := ReadRootedDir(path)
			return err
		},
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			if err := operation(); err == nil {
				t.Fatal("expected traversal path to be rejected")
			}
		})
	}
}

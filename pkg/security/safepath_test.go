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

// Author: Codex (#1636 lane). Public command outcome class, native stdio owner.
package main

import (
	"errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"golang.org/x/text/language"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOwnerClassCommandNativeWorkerOutcomes(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("NORNICDB_LANGUAGE", "en")
	t.Setenv("NORNICDB_BOLT_TLS_ENABLED", "true")
	t.Setenv("NORNICDB_BOLT_TLS_CERT", "unused-cert")
	t.Setenv("NORNICDB_BOLT_TLS_KEY", "unused-key")
	t.Setenv("NORNICDB_BOLT_TLS_CLIENT_AUTH_MODE", "class-invalid-mode")
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := newTestServerRoot(manager)
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	cfg := filepath.Join(dir, "settings.yaml")
	if err := os.WriteFile(cfg, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	stdout, err := os.Create(filepath.Join(dir, "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := os.Create(filepath.Join(dir, "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	if err := stdout.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stderr.Close(); err != nil {
		t.Fatal(err)
	}
	previousOut, previousErr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = stdout, stderr
	defer func() { os.Stdout, os.Stderr = previousOut, previousErr }()
	root.SetArgs([]string{"serve", "--config=" + cfg, "--headless", "--no-auth", "--data-dir=" + filepath.Join(dir, "data"), "--embedding-enabled=false", "--search-bm25-enabled=false", "--search-vector-enabled=false", "--stdio-log-max-kb=1", "--http-port=0", "--bolt-port=0"})
	result := root.Execute()
	if result == nil || !strings.Contains(result.Error(), "client auth") {
		t.Fatalf("real post-construction setup failure not reached: %v", result)
	}
	if !errors.Is(result, os.ErrClosed) || !strings.Contains(result.Error(), "stdout") || !strings.Contains(result.Error(), "stderr") {
		t.Errorf("command lost its native worker's sibling completion outcomes: %v", result)
	}
}

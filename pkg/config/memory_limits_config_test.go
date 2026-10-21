package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMemoryLimitNamesKeepGoAndApplicationScopesDistinct(t *testing.T) {
	if got := LoadDefaults().Memory.TransactionTotalMax; got != 0 {
		t.Fatalf("unconfigured aggregate transaction limit = %d, want unlimited", got)
	}
	for _, name := range []string{
		"NORNICDB_MEMORY_LIMIT",
		"NORNICDB_GO_MEMORY_LIMIT_BYTES",
		"NORNICDB_MEMORY_APPLICATION_SOFT_MAX",
	} {
		t.Setenv(name, "")
	}

	t.Run("legacy Go target only", func(t *testing.T) {
		t.Setenv("NORNICDB_MEMORY_LIMIT", "7168")
		cfg := LoadFromEnv()
		if cfg.Memory.RuntimeLimit != 7168<<20 || cfg.Memory.ApplicationSoftMax != 0 {
			t.Fatalf("legacy scope changed: Go=%d application=%d", cfg.Memory.RuntimeLimit, cfg.Memory.ApplicationSoftMax)
		}
	})
	t.Run("new Go target only", func(t *testing.T) {
		t.Setenv("NORNICDB_GO_MEMORY_LIMIT_BYTES", "7516192768")
		cfg := LoadFromEnv()
		if cfg.Memory.RuntimeLimit != 7168<<20 || cfg.Memory.ApplicationSoftMax != 0 {
			t.Fatalf("new Go scope wrong: Go=%d application=%d", cfg.Memory.RuntimeLimit, cfg.Memory.ApplicationSoftMax)
		}
	})
	t.Run("application only", func(t *testing.T) {
		t.Setenv("NORNICDB_MEMORY_APPLICATION_SOFT_MAX", "7g")
		cfg := LoadFromEnv()
		if cfg.Memory.ApplicationSoftMax != 7<<30 {
			t.Fatalf("application limit = %d", cfg.Memory.ApplicationSoftMax)
		}
	})
	t.Run("conflicting Go aliases", func(t *testing.T) {
		t.Setenv("NORNICDB_MEMORY_LIMIT", "1024")
		t.Setenv("NORNICDB_GO_MEMORY_LIMIT_BYTES", "2147483648")
		defer func() {
			got := recover()
			if got == nil || !strings.Contains(got.(string), "NORNICDB_GO_MEMORY_LIMIT_BYTES") {
				t.Fatalf("expected alias conflict, got %v", got)
			}
		}()
		_ = LoadFromEnv()
	})
}

func TestMemoryLimitYAMLNamesKeepScopeAndRejectConflicts(t *testing.T) {
	for _, name := range []string{
		"NORNICDB_MEMORY_LIMIT",
		"NORNICDB_GO_MEMORY_LIMIT_BYTES",
		"NORNICDB_MEMORY_APPLICATION_SOFT_MAX",
	} {
		t.Setenv(name, "")
	}
	load := func(contents string) (*Config, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		return LoadFromFile(path)
	}
	cfg, err := load("memory:\n  go_runtime_limit_bytes: \"7516192768\"\n  application_soft_max: \"7g\"\n")
	if err != nil || cfg.Memory.RuntimeLimit != 7168<<20 || cfg.Memory.ApplicationSoftMax != 7<<30 {
		t.Fatalf("new YAML names: cfg=%+v err=%v", cfg.Memory, err)
	}
	_, err = load("memory:\n  runtime_limit: \"1024\"\n  go_runtime_limit_bytes: \"2147483648\"\n")
	if err == nil || !strings.Contains(err.Error(), "memory.go_runtime_limit_bytes") {
		t.Fatalf("expected YAML alias conflict, got %v", err)
	}
}

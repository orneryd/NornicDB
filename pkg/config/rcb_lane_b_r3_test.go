package config

// Root-cause diagnostic round 3, campaign lane B (test-only). G-config-load.

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRCBLoadFromFileMissingSkipsEnv(t *testing.T) {
	t.Setenv("NORNICDB_HTTP_PORT", "17474")
	dir := t.TempDir()
	missing := filepath.Join(dir, "no-such-config.yaml")
	cfg, err := LoadFromFile(missing)
	if cfg == nil {
		t.Fatalf("LoadFromFile(missing) returned nil config, err=%v", err)
	}
	t.Logf("RCBCFG missing file: err=%v HTTPPort=%d", err, cfg.Server.HTTPPort)
	variantErr := applyEnvVars(cfg)
	t.Logf("RCBCFG variant: the same config after the skipped step applyEnvVars: err=%v HTTPPort=%d", variantErr, cfg.Server.HTTPPort)
	empty := filepath.Join(dir, "empty.yaml")
	if err := os.WriteFile(empty, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	control, err := LoadFromFile(empty)
	port := -1
	if control != nil {
		port = control.Server.HTTPPort
	}
	t.Logf("RCBCFG control existing empty file: err=%v HTTPPort=%d", err, port)
	t.Logf("RCBCFG control LoadFromEnv: HTTPPort=%d", LoadFromEnv().Server.HTTPPort)
}

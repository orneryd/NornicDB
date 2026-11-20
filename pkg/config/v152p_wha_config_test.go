package config

// Whole-head issues report, pending entry (untracked; never committed):
// LoadFromFile with a config path that does not exist, and an environment
// setting. Logged as WHAPE lines.

import (
	"path/filepath"
	"testing"
)

func TestWhaPendingLoadFromMissingFile(t *testing.T) {
	t.Setenv("NORNICDB_HTTP_PORT", "17474")
	missing := filepath.Join(t.TempDir(), "no-such-config.yaml")
	cfg, err := LoadFromFile(missing)
	if err != nil {
		t.Logf("WHAPE PE-configenv LoadFromFile(missing) error: %v", err)
	} else {
		t.Logf("WHAPE PE-configenv LoadFromFile(missing): HTTP port %d (env asks 17474)", cfg.Server.HTTPPort)
	}
	env := LoadFromEnv()
	t.Logf("WHAPE PE-configenv control LoadFromEnv(): HTTP port %d", env.Server.HTTPPort)
}

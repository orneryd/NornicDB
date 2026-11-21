// Verification test for hunt lead L-H9-004 (v1.5.2 verification campaign, H9
// verifier lane, 2026-11-21). Test-only, not for merge.
package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// verifyH9ImageEnv reads the ENV block of the CPU image Dockerfile.
func verifyH9ImageEnv(t *testing.T) map[string]string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "docker", "Dockerfile.amd64-cpu"))
	require.NoError(t, err)
	text := string(data)
	start := strings.Index(text, "\nENV NORNICDB_")
	require.GreaterOrEqual(t, start, 0, "ENV block")
	env := map[string]string{}
	pair := regexp.MustCompile(`^\s*(?:ENV\s+)?(NORNICDB_[A-Z_]+)=(\S*)`)
	for _, line := range strings.Split(text[start+1:], "\n") {
		match := pair.FindStringSubmatch(line)
		if match == nil {
			break
		}
		env[match[1]] = strings.TrimSuffix(match[2], "\\")
		if !strings.HasSuffix(strings.TrimSpace(line), "\\") {
			break
		}
	}
	t.Logf("VERIFY_H9 image ENV %v", env)
	return env
}

// verifyH9Entrypoint runs docker/entrypoint.sh with env and a recorder in place
// of the binary, and returns the arguments the binary receives.
func verifyH9Entrypoint(t *testing.T, env map[string]string) []string {
	t.Helper()
	dir := t.TempDir()
	out := filepath.Join(dir, "args.txt")
	recorder := filepath.Join(dir, "recorder.sh")
	require.NoError(t, os.WriteFile(recorder, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \""+out+"\"\n"), 0o755))
	script, err := filepath.Abs(filepath.Join("..", "..", "docker", "entrypoint.sh"))
	require.NoError(t, err)
	cmd := exec.Command("sh", script)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "NORNICDB_BIN=" + recorder}
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	output, err := cmd.CombinedOutput()
	t.Logf("VERIFY_H9 entrypoint output %q err=%v", string(output), err)
	require.NoError(t, err)
	data, err := os.ReadFile(out)
	require.NoError(t, err)
	args := strings.Fields(string(data))
	t.Logf("VERIFY_H9 binary args %v", args)
	return args
}

func verifyH9ServeAuth(t *testing.T, env map[string]string) (bool, *config.Config) {
	t.Helper()
	args := verifyH9Entrypoint(t, env)
	noAuth := false
	for _, arg := range args {
		if arg == "--no-auth" {
			noAuth = true
		}
	}
	for key, value := range env {
		t.Setenv(key, value)
	}
	cfg := config.LoadFromEnv()
	enabled := serveAuthEnabled(cfg, noAuth)
	t.Logf("VERIFY_H9 noAuth=%v cfg.Auth.Enabled=%v serveAuthEnabled=%v InitialUsername=%q", noAuth, cfg.Auth.Enabled, enabled, cfg.Auth.InitialUsername)
	return enabled, cfg
}

func verifyH9Merge(base map[string]string, extra map[string]string) map[string]string {
	merged := map[string]string{}
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range extra {
		merged[k] = v
	}
	return merged
}

// (a) docs/start.md quick start: image ENV plus NORNICDB_AUTH and the JWT secret.
func TestVerifyH9_004_QuickStartAuthIsEnforced(t *testing.T) {
	image := verifyH9ImageEnv(t)
	require.Equal(t, "true", image["NORNICDB_NO_AUTH"], "premise: image sets NORNICDB_NO_AUTH=true")
	enabled, _ := verifyH9ServeAuth(t, verifyH9Merge(image, map[string]string{
		"NORNICDB_AUTH":            "admin:verify-h9-long-password-1",
		"NORNICDB_AUTH_JWT_SECRET": "verify-h9-jwt-secret-at-least-32-characters",
	}))
	assert.True(t, enabled, "serve starts with authentication")
}

// (b) docs/packaging/docker.md table: NORNICDB_AUTH_ENABLED/_USER/_PASSWORD.
func TestVerifyH9_004_DockerTableAuthIsEnforced(t *testing.T) {
	image := verifyH9ImageEnv(t)
	enabled, cfg := verifyH9ServeAuth(t, verifyH9Merge(image, map[string]string{
		"NORNICDB_AUTH_ENABLED":  "true",
		"NORNICDB_AUTH_USER":     "admin",
		"NORNICDB_AUTH_PASSWORD": "verify-h9-long-password-2",
	}))
	assert.True(t, enabled, "serve starts with authentication")
	assert.Equal(t, "verify-h9-long-password-2", cfg.Auth.InitialPassword, "documented password is the admin password")
}

// Control: the same quick-start settings without the image's NORNICDB_NO_AUTH.
func TestVerifyH9_004_Control_WithoutImageNoAuth(t *testing.T) {
	image := verifyH9ImageEnv(t)
	delete(image, "NORNICDB_NO_AUTH")
	enabled, _ := verifyH9ServeAuth(t, verifyH9Merge(image, map[string]string{
		"NORNICDB_AUTH":            "admin:verify-h9-long-password-1",
		"NORNICDB_AUTH_JWT_SECRET": "verify-h9-jwt-secret-at-least-32-characters",
	}))
	assert.True(t, enabled)
}

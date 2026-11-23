package buildinfo

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncVersionPrefersCheckoutVersion(t *testing.T) {
	root := t.TempDir()
	script := "sync-version.sh"
	program := "sh"
	var args []string
	if runtime.GOOS == "windows" {
		script = "sync-version.ps1"
		program = "powershell"
		args = []string{"-NoProfile", "-ExecutionPolicy", "Bypass", "-File"}
	} else if _, err := exec.LookPath("perl"); err != nil {
		t.Skip("the shell version sync requires perl")
	}
	for _, tool := range []string{"git", program} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("version sync requires %s", tool)
		}
	}

	scripts := filepath.Join(root, "scripts")
	versionFile := filepath.Join(root, "pkg", "buildinfo", "VERSION")
	readmeFile := filepath.Join(root, "README.md")
	require.NoError(t, os.MkdirAll(scripts, 0o755))
	require.NoError(t, os.MkdirAll(filepath.Dir(versionFile), 0o755))
	source, err := os.ReadFile(filepath.Join("..", "..", "scripts", script))
	require.NoError(t, err)
	target := filepath.Join(scripts, script)
	require.NoError(t, os.WriteFile(target, source, 0o644))

	git := func(args ...string) {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "git", args...)
		cmd.Dir = root
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
	}
	git("init", "--quiet")
	git("-c", "user.name=Version fixture", "-c", "user.email=version@example.invalid",
		"-c", "commit.gpgSign=false", "-c", "core.hooksPath="+filepath.Join(root, "no-hooks"),
		"commit", "--quiet", "--allow-empty", "-m", "Previous release")
	git("tag", "v1.5.1")

	check := func(t *testing.T, override, want string) {
		t.Helper()
		require.NoError(t, os.WriteFile(readmeFile,
			[]byte(`<img src="https://img.shields.io/badge/version-1.5.1-success" alt="Version 1.5.1">`+"\n"), 0o644))
		cmd := exec.CommandContext(t.Context(), program, append(args, target)...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "VERSION="+override)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
		version, err := os.ReadFile(versionFile)
		require.NoError(t, err)
		require.Equal(t, want, strings.TrimSpace(string(version)), "embedded build version")
		readme, err := os.ReadFile(readmeFile)
		require.NoError(t, err)
		require.Contains(t, string(readme), "badge/version-"+want+"-success")
		require.Contains(t, string(readme), `alt="Version `+want+`"`)
	}

	t.Run("untagged release checkout", func(t *testing.T) {
		require.NoError(t, os.WriteFile(versionFile, []byte("1.5.2\n"), 0o644))
		check(t, "latest", "1.5.2")
	})
	t.Run("explicit override", func(t *testing.T) {
		check(t, "v2.0.0", "2.0.0")
	})
	t.Run("tag when the checkout version is missing", func(t *testing.T) {
		require.NoError(t, os.Remove(versionFile))
		check(t, "", "1.5.1")
	})
}

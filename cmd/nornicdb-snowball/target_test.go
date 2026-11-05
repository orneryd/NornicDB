package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search/stemmer"
	"github.com/stretchr/testify/require"
)

// buildStemmerHost builds testdata/stemmerhost, a --target that checks plugins
// as `nornicdb stemmer check` does, with the given environment and go build
// flags.
func buildStemmerHost(t *testing.T, env []string, flags ...string) string {
	t.Helper()
	host := filepath.Join(t.TempDir(), "stemmerhost")
	args := append([]string{"build"}, flags...)
	args = append(args, "-o", host, "./testdata/stemmerhost")
	cmd := exec.Command("go", args...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "build stemmerhost %v %v: %s", env, flags, out)
	return host
}

// packageForTarget packages the fixture module for target into a fresh
// directory and returns the plugin path and the packaging error.
func packageForTarget(t *testing.T, moduleDir, target string, timeout time.Duration) (string, error) {
	t.Helper()
	output := filepath.Join(t.TempDir(), "snowball-ukrainian.so")
	err := packageSnowball(packageOptions{
		Language:     "ukrainian",
		ID:           "snowball.ukrainian",
		Version:      "1.0.0",
		Module:       moduleDir,
		Source:       filepath.Join(moduleDir, "stemmer.go"),
		Output:       output,
		Target:       target,
		CheckTimeout: timeout,
	})
	return output, err
}

func requireRemoved(t *testing.T, output string) {
	t.Helper()
	require.NoFileExists(t, output, "a refused plugin must be removed")
	require.NoFileExists(t, manifestPath(output), "a refused plugin's manifest must be removed")
}

// Packaging with --target keeps a plugin only when the target's own loader
// accepts it, whatever the target's build settings and the packaging
// environment. Where the recorded build settings let the packager match the
// target, packaging succeeds; where they cannot (several -gcflags, a shared
// dependency at another version, a target that cannot load plugins), the
// target refuses the plugin and both files are removed.
func TestPackageWithTargetKeepsOnlyPluginsTheTargetLoads(t *testing.T) {
	if runtime.GOOS == "windows" || !stemmer.DynamicLoadSupported() {
		t.Skip("Go plugins are unavailable on this platform")
	}
	moduleDir := writeSnowballFixtureModule(t)

	loads := []struct {
		name       string
		hostEnv    []string
		hostFlags  []string
		packageEnv map[string]string
	}{
		{name: "plain"},
		{name: "trimpath, as the Homebrew binaries are built", hostFlags: []string{"-trimpath"}},
		{name: "race", hostFlags: []string{"-race"}},
		{name: "target GOEXPERIMENT", hostEnv: []string{"GOEXPERIMENT=arenas"}},
		{name: "packaging environment differs from the target's", packageEnv: map[string]string{"GOFLAGS": "-trimpath", "GOEXPERIMENT": "arenas"}},
	}
	for _, tc := range loads {
		t.Run("loads/"+tc.name, func(t *testing.T) {
			host := buildStemmerHost(t, tc.hostEnv, tc.hostFlags...)
			for key, value := range tc.packageEnv {
				t.Setenv(key, value)
			}
			output, err := packageForTarget(t, moduleDir, host, 0)
			require.NoError(t, err)
			require.FileExists(t, output)
			require.FileExists(t, manifestPath(output))
			out, err := exec.Command(host, "stemmer", "check", manifestPath(output)).CombinedOutput()
			require.NoError(t, err, "%s", out)
		})
	}

	// The check runs from a temporary directory, so a relative target, as the
	// docs write it (./bin/nornicdb), and a bare name found in PATH must
	// still name the binary the caller meant.
	t.Run("loads/relative target path and bare name in PATH", func(t *testing.T) {
		host := buildStemmerHost(t, nil)
		t.Chdir(filepath.Dir(filepath.Dir(host)))
		output, err := packageForTarget(t, moduleDir, "./"+filepath.Join(filepath.Base(filepath.Dir(host)), "stemmerhost"), 0)
		require.NoError(t, err)
		require.FileExists(t, output)
		require.FileExists(t, manifestPath(output))

		t.Setenv("PATH", filepath.Dir(host)+string(os.PathListSeparator)+os.Getenv("PATH"))
		output, err = packageForTarget(t, moduleDir, "stemmerhost", 0)
		require.NoError(t, err)
		require.FileExists(t, output)
		require.FileExists(t, manifestPath(output))
	})

	// Exit status 0 is not proof that the check finished: the packager also
	// requires the check's result line for this plugin.
	t.Run("refused/target exits 0 without checking", func(t *testing.T) {
		host := buildStemmerHost(t, nil)
		t.Setenv("STEMMERHOST_MODE", "silent")
		output, err := packageForTarget(t, moduleDir, host, 0)
		require.ErrorContains(t, err, "exited without confirming that it loaded the plugin")
		requireRemoved(t, output)
	})

	t.Run("refused/plugin init exits 0 inside plugin.Open", func(t *testing.T) {
		host := buildStemmerHost(t, nil)
		exiting := writeSnowballFixtureModule(t)
		require.NoError(t, os.WriteFile(filepath.Join(exiting, "exit.go"), []byte("package main\n\nimport \"os\"\n\nfunc init() { os.Exit(0) }\n"), 0o644))
		output, err := packageForTarget(t, exiting, host, 0)
		require.ErrorContains(t, err, "exited without confirming that it loaded the plugin")
		requireRemoved(t, output)
	})

	t.Run("refused/several gcflags, only the last is recorded", func(t *testing.T) {
		host := buildStemmerHost(t, nil, "-gcflags=all=-l", "-gcflags=fmt=-N")
		output, err := packageForTarget(t, moduleDir, host, 0)
		require.ErrorContains(t, err, "refuses the plugin")
		require.ErrorContains(t, err, "plugin was built with a different version of package")
		requireRemoved(t, output)
	})

	t.Run("refused/shared dependency at another version", func(t *testing.T) {
		host := buildStemmerHost(t, nil)
		output, err := packageForTarget(t, withSharedTextDependency(t, writeSnowballFixtureModule(t)), host, 0)
		require.ErrorContains(t, err, "plugin was built with a different version of package golang.org/x/text/unicode/norm")
		require.ErrorContains(t, err, "module golang.org/x/text: target ", "the recorded differences explain the refusal")
		requireRemoved(t, output)
	})

	t.Run("refused/target cannot load plugins", func(t *testing.T) {
		host := buildStemmerHost(t, []string{"CGO_ENABLED=0"})
		output, err := packageForTarget(t, moduleDir, host, 0)
		require.Error(t, err)
		requireRemoved(t, output)
	})

	t.Run("refused/target without the stemmer check command", func(t *testing.T) {
		host := buildStemmerHost(t, nil)
		t.Setenv("STEMMERHOST_MODE", "old")
		output, err := packageForTarget(t, moduleDir, host, 0)
		require.ErrorContains(t, err, "cannot verify plugins")
		requireRemoved(t, output)
	})

	t.Run("refused/target check crashes", func(t *testing.T) {
		host := buildStemmerHost(t, nil)
		t.Setenv("STEMMERHOST_MODE", "crash")
		output, err := packageForTarget(t, moduleDir, host, 0)
		require.ErrorContains(t, err, "refuses the plugin")
		require.ErrorContains(t, err, "simulated crash")
		requireRemoved(t, output)
	})

	t.Run("refused/target check hangs", func(t *testing.T) {
		host := buildStemmerHost(t, nil)
		t.Setenv("STEMMERHOST_MODE", "hang")
		start := time.Now()
		output, err := packageForTarget(t, moduleDir, host, time.Second)
		require.ErrorContains(t, err, "did not finish checking the plugin")
		require.Less(t, time.Since(start), time.Second+checkGrace+15*time.Second)
		requireRemoved(t, output)
	})

	t.Run("unverified plugin is refused by a trimpath target", func(t *testing.T) {
		if strings.Contains(os.Getenv("GOFLAGS"), "-trimpath") {
			t.Skip("GOFLAGS already builds the unverified plugin with -trimpath")
		}
		host := buildStemmerHost(t, nil, "-trimpath")
		output, err := packageForTarget(t, moduleDir, "", 0)
		require.NoError(t, err, "without --target nothing is verified")
		out, err := exec.Command(host, "stemmer", "check", manifestPath(output)).CombinedOutput()
		require.Error(t, err)
		require.Contains(t, string(out), "plugin was built with a different version of package")
	})
}

// withSharedTextDependency makes the fixture import golang.org/x/text/unicode/norm
// from a vendored v0.0.1 whose source differs from the version nornicdb (and
// stemmerhost) links.
func withSharedTextDependency(t *testing.T, dir string) string {
	t.Helper()
	normDir := filepath.Join(dir, "vendor", "golang.org", "x", "text", "unicode", "norm")
	require.NoError(t, os.MkdirAll(normDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(normDir, "norm.go"), []byte("package norm\n\n// Fixture is not the real golang.org/x/text/unicode/norm.\nfunc Fixture() string { return \"fixture\" }\n"), 0o644))
	appendFile(t, filepath.Join(dir, "go.mod"), "\nrequire golang.org/x/text v0.0.1\n")
	appendFile(t, filepath.Join(dir, "vendor", "modules.txt"), "# golang.org/x/text v0.0.1\n## explicit; go 1.26.4\ngolang.org/x/text/unicode/norm\n")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "stemmer.go"), []byte(`package main

import (
	snowballRuntime "example.com/snowball/runtime"
	"golang.org/x/text/unicode/norm"
)

func Stem(env *snowballRuntime.Env) bool {
	env.SetCurrent(env.Current() + "-" + norm.Fixture())
	return true
}
`), 0o644))
	return dir
}

func appendFile(t *testing.T, path, text string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = file.WriteString(text)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}

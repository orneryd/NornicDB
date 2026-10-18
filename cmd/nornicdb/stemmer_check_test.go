package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/internal/stemmertest"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/search/stemmer"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

// runStemmerCheckCommand runs `nornicdb stemmer check <args>` through the root
// command from an empty working directory, with NORNICDB_DATA_DIR, HOME and
// NORNICDB_CONFIG pointing at paths that do not exist. It returns the output,
// that working directory and the error.
func runStemmerCheckCommand(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	stemmer.ResetForTest()
	t.Cleanup(stemmer.ResetForTest)
	work := t.TempDir()
	t.Setenv("NORNICDB_DATA_DIR", filepath.Join(work, "data"))
	t.Setenv("NORNICDB_CONFIG", filepath.Join(work, "config.yaml"))
	t.Setenv("HOME", filepath.Join(work, "home"))
	t.Chdir(work)

	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	root := newRootCommand(manager)
	var output bytes.Buffer
	root.SetOut(&output)
	root.SetErr(&output)
	root.SetArgs(append([]string{"stemmer", "check"}, args...))
	err = root.Execute()
	return output.String(), work, err
}

func requireUntouched(t *testing.T, work string) {
	t.Helper()
	entries, err := os.ReadDir(work)
	require.NoError(t, err)
	require.Empty(t, entries, "stemmer check must not create a data directory, config or any other file")
}

func TestStemmerCheckLoadsPluginWithoutTouchingDataDirectory(t *testing.T) {
	if !stemmer.DynamicLoadSupported() {
		t.Skip("Go plugins are unavailable on this platform")
	}
	manifestPath := stemmertest.Build(t, t.TempDir(), "cli.suffix", stemmertest.SuffixSource)

	output, work, err := runStemmerCheckCommand(t, manifestPath)
	require.NoError(t, err, output)
	require.Contains(t, output, "Stemmer plugin cli.suffix 1.0.0 loads: ")
	requireUntouched(t, work)
	registration, ok := stemmer.Lookup("cli.suffix")
	require.True(t, ok)
	require.Equal(t, "token-stem", registration.Stem("token"))
	result, ok := stemmer.ParseCheckResult(output)
	require.True(t, ok, "the check must print its result line: %s", output)
	require.Equal(t, stemmer.CheckResult{ID: "cli.suffix", Version: "1.0.0", APIVersion: stemmer.APIVersion, SHA256: registration.Digest}, result)
}

func TestStemmerCheckReportsARefusedPlugin(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "broken.stemmer.json")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`{"schema_version":0}`), 0o644))

	output, work, err := runStemmerCheckCommand(t, manifestPath)
	require.Error(t, err, output)
	require.Equal(t, "stemmer plugin "+manifestPath+" does not load: unsupported stemmer manifest schema 0", err.Error())
	requireUntouched(t, work)
}

func TestStemmerCheckTimesOut(t *testing.T) {
	if !stemmer.DynamicLoadSupported() {
		t.Skip("Go plugins are unavailable on this platform")
	}
	// Loading holds the runtime's plugin lock until init returns, so the init
	// sleeps for a bounded time rather than forever.
	manifestPath := stemmertest.Build(t, t.TempDir(), "cli.hang", `package main

import "time"

func init() { time.Sleep(3 * time.Second) }

type suffix struct{}

func (suffix) Stem(token string) string { return token }

var Plugin suffix
`)
	output, work, err := runStemmerCheckCommand(t, "--timeout", "200ms", manifestPath)
	require.Error(t, err, output)
	require.Equal(t, "stemmer plugin "+manifestPath+" did not finish loading within 200ms", err.Error())
	requireUntouched(t, work)
}

func TestStemmerCheckEnglishHelp(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	check, _, err := newRootCommand(manager).Find([]string{"stemmer", "check"})
	require.NoError(t, err)
	require.Equal(t, "Check that a stemmer plugin loads into this binary, as it would at startup", check.Short)
	require.Equal(t, "Maximum time to load the plugin", check.Flag("timeout").Usage)
	require.Equal(t, defaultStemmerCheckTimeout.String(), check.Flag("timeout").DefValue)
}

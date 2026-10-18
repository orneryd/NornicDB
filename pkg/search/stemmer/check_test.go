package stemmer_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/stemmertest"
	"github.com/orneryd/nornicdb/pkg/search/stemmer"
	"github.com/stretchr/testify/require"
)

func TestCheckManifestLoadsLikeStartup(t *testing.T) {
	if !stemmer.DynamicLoadSupported() {
		t.Skip("Go plugins are unavailable on this platform")
	}
	stemmer.ResetForTest()
	t.Cleanup(stemmer.ResetForTest)
	manifestPath := stemmertest.Build(t, t.TempDir(), "check.suffix", stemmertest.SuffixSource)

	registration, err := stemmer.CheckManifest(manifestPath, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "check.suffix", registration.ID)
	require.Equal(t, "token-stem", registration.Stem("token"))
	registered, ok := stemmer.Lookup("check.suffix")
	require.True(t, ok)
	require.Equal(t, registration.Digest, registered.Digest)

	// A changed library fails the digest check before the plugin is opened.
	tampered := filepath.Join(t.TempDir(), filepath.Base(manifestPath))
	raw, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(tampered, raw, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(tampered), filepath.Base(registration.Path)), []byte("not a plugin"), 0o644))
	_, err = stemmer.CheckManifest(tampered, time.Minute)
	require.ErrorContains(t, err, "digest mismatch")
}

func TestCheckManifestReportsInvalidManifest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broken.stemmer.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"schema_version":0}`), 0o644))
	_, err := stemmer.CheckManifest(path, time.Minute)
	require.ErrorContains(t, err, "unsupported stemmer manifest schema 0")
}

// A plugin whose init does not return is reported as a timeout. Loading holds
// the runtime's plugin lock until init finishes, so the init sleeps for a
// bounded time and no other test in this binary waits long behind it.
func TestCheckManifestTimesOutOnHangingInit(t *testing.T) {
	if !stemmer.DynamicLoadSupported() {
		t.Skip("Go plugins are unavailable on this platform")
	}
	stemmer.ResetForTest()
	t.Cleanup(stemmer.ResetForTest)
	manifestPath := stemmertest.Build(t, t.TempDir(), "check.hang", `package main

import "time"

func init() { time.Sleep(3 * time.Second) }

type suffix struct{}

func (suffix) Stem(token string) string { return token }

var Plugin suffix
`)
	start := time.Now()
	_, err := stemmer.CheckManifest(manifestPath, 200*time.Millisecond)
	require.ErrorIs(t, err, stemmer.ErrCheckTimedOut)
	require.Less(t, time.Since(start), 2*time.Second)
}

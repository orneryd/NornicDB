package main

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPackageTargetFailurePreservesExistingArtifacts(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Go plugins are unavailable")
	}
	host := buildStemmerHost(t, nil)
	module := writeSnowballFixtureModule(t)
	for _, mode := range []string{"silent", "crash", "old"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("STEMMERHOST_MODE", mode)
			output := filepath.Join(t.TempDir(), "existing.so")
			require.NoError(t, os.WriteFile(output, []byte("previous library"), 0600))
			require.NoError(t, os.WriteFile(manifestPath(output), []byte("previous manifest"), 0600))
			err := packageSnowball(packageOptions{Language: "ukrainian", ID: "snowball.ukrainian", Version: "1.0.0", Module: module, Source: filepath.Join(module, "stemmer.go"), Output: output, Target: host})
			require.Error(t, err)
			library, err := os.ReadFile(output)
			require.NoError(t, err)
			require.Equal(t, "previous library", string(library))
			manifest, err := os.ReadFile(manifestPath(output))
			require.NoError(t, err)
			require.Equal(t, "previous manifest", string(manifest))
		})
	}
}

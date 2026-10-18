package main

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPackageFailurePreservesExistingArtifacts(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Go plugins are unavailable")
	}
	for _, phase := range []string{"build", "manifest-publication"} {
		t.Run(phase, func(t *testing.T) {
			module := writeSnowballFixtureModule(t)
			output := filepath.Join(t.TempDir(), "existing.so")
			require.NoError(t, os.WriteFile(output, []byte("previous library"), 0600))
			manifest := manifestPath(output)
			if phase == "build" {
				require.NoError(t, os.WriteFile(manifest, []byte("previous manifest"), 0600))
				require.NoError(t, os.WriteFile(filepath.Join(module, "broken.go"), []byte("package main\nfunc broken(){ undefinedFunction() }\n"), 0600))
			} else {
				require.NoError(t, os.Mkdir(manifest, 0700))
				require.NoError(t, os.WriteFile(filepath.Join(manifest, "keep"), []byte("previous directory"), 0600))
			}
			err := packageSnowball(packageOptions{Language: "ukrainian", ID: "snowball.ukrainian", Version: "1.0.0", Module: module, Source: filepath.Join(module, "stemmer.go"), Output: output})
			require.Error(t, err)
			library, readErr := os.ReadFile(output)
			require.NoError(t, readErr)
			require.Equal(t, "previous library", string(library))
			if phase == "build" {
				bytes, err := os.ReadFile(manifest)
				require.NoError(t, err)
				require.Equal(t, "previous manifest", string(bytes))
			} else {
				bytes, err := os.ReadFile(filepath.Join(manifest, "keep"))
				require.NoError(t, err)
				require.Equal(t, "previous directory", string(bytes))
			}
		})
	}
}

// Package stemmertest builds stemmer plugins that the running test binary can
// load, for tests of the stemmer loader and of `nornicdb stemmer check`.
package stemmertest

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/pluginbuild"
	"github.com/orneryd/nornicdb/pkg/search/stemmer"
	"github.com/stretchr/testify/require"
)

// SuffixSource is a stemmer plugin that appends "-stem" to every token.
const SuffixSource = `package main

type suffix struct{}

func (suffix) Stem(token string) string { return token + "-stem" }

var Plugin suffix
`

// Build compiles source (a main package defining the Plugin symbol) as a Go
// plugin with the running binary's build configuration, writes it into dir
// with a manifest for id, and returns the manifest path.
func Build(t testing.TB, dir, id, source string) string {
	t.Helper()
	buildConfig, err := pluginbuild.Running()
	require.NoError(t, err)
	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "plugin.go")
	require.NoError(t, os.WriteFile(srcPath, []byte(source), 0o600))
	library := filepath.Join(dir, strings.ReplaceAll(id, ".", "-")+".so")

	args := append([]string{"build"}, buildConfig.Flags...)
	args = append(args, "-buildmode=plugin", "-o", library, srcPath)
	cmd := exec.Command("go", args...)
	cmd.Dir = srcDir
	cmd.Env = append(os.Environ(), buildConfig.Env...)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "go build plugin: %s", out)

	digest, err := stemmer.SHA256File(library)
	require.NoError(t, err)
	raw, err := json.Marshal(stemmer.Manifest{
		SchemaVersion: stemmer.ManifestSchemaVersion,
		APIVersion:    stemmer.APIVersion,
		Type:          stemmer.ManifestType,
		ID:            id,
		Version:       "1.0.0",
		Library:       filepath.Base(library),
		SHA256:        digest,
		Entrypoint:    stemmer.EntrypointSymbol,
	})
	require.NoError(t, err)
	manifestPath := strings.TrimSuffix(library, ".so") + ".stemmer.json"
	require.NoError(t, os.WriteFile(manifestPath, raw, 0o644))
	return manifestPath
}

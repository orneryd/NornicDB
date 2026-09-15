package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"plugin"
	"runtime"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search/stemmer"
	"github.com/stretchr/testify/require"
)

func TestParsePackageArgsAcceptsDocumentedShape(t *testing.T) {
	opts, err := parsePackageArgs([]string{
		"--language", "ukrainian",
		"--id", "snowball.ukrainian",
		"--version", "1.0.0",
		"--module", "./stemmer-module",
		"--source", "./stemmer-module/stemmer.go",
		"--output", "./plugins/stemmers/snowball-ukrainian.so",
	})
	require.NoError(t, err)
	require.Equal(t, "ukrainian", opts.Language)
	require.Equal(t, "snowball.ukrainian", opts.ID)
	require.Equal(t, "1.0.0", opts.Version)
	require.Equal(t, "./stemmer-module", opts.Module)
	require.Equal(t, "./stemmer-module/stemmer.go", opts.Source)
	require.Equal(t, "./plugins/stemmers/snowball-ukrainian.so", opts.Output)
}

func TestDocumentedLanguagePackageExamplesParse(t *testing.T) {
	tests := []struct {
		language string
		id       string
		version  string
		module   string
		source   string
		output   string
	}{
		{
			language: "french",
			id:       "snowball.french",
			version:  "3.0.1",
			module:   "./build/stemmers/french",
			source:   "./build/stemmers/french/stemmer.go",
			output:   "./plugins/stemmers/snowball-french.so",
		},
		{
			language: "spanish",
			id:       "snowball.spanish",
			version:  "3.0.1",
			module:   "./build/stemmers/spanish",
			source:   "./build/stemmers/spanish/stemmer.go",
			output:   "./plugins/stemmers/snowball-spanish.so",
		},
		{
			language: "dutch",
			id:       "snowball.dutch",
			version:  "3.0.1",
			module:   "./build/stemmers/dutch",
			source:   "./build/stemmers/dutch/stemmer.go",
			output:   "./plugins/stemmers/snowball-dutch.so",
		},
		{
			language: "ukrainian",
			id:       "snowball.ukrainian",
			version:  "local-2026.09",
			module:   "./build/stemmers/ukrainian",
			source:   "./build/stemmers/ukrainian/stemmer.go",
			output:   "./plugins/stemmers/snowball-ukrainian.so",
		},
		{
			language: "chinese",
			id:       "snowball.chinese",
			version:  "local-2026.09",
			module:   "./build/stemmers/chinese",
			source:   "./build/stemmers/chinese/stemmer.go",
			output:   "./plugins/stemmers/snowball-chinese.so",
		},
	}

	for _, tt := range tests {
		t.Run(tt.language, func(t *testing.T) {
			opts, err := parsePackageArgs([]string{
				"--language", tt.language,
				"--id", tt.id,
				"--version", tt.version,
				"--module", tt.module,
				"--source", tt.source,
				"--output", tt.output,
			})
			require.NoError(t, err)
			require.Equal(t, tt.language, opts.Language)
			require.Equal(t, tt.id, opts.ID)
			require.Equal(t, tt.version, opts.Version)
			require.Equal(t, tt.module, opts.Module)
			require.Equal(t, tt.source, opts.Source)
			require.Equal(t, tt.output, opts.Output)
			require.True(t, strings.HasSuffix(opts.Output, ".so"))
			require.NotContains(t, opts.ID, "/")
		})
	}
}

func TestPackageArgsRejectPathAsPluginID(t *testing.T) {
	_, err := parsePackageArgs([]string{
		"--language", "ukrainian",
		"--id", "./plugins/stemmers/snowball-ukrainian.so",
		"--version", "1.0.0",
		"--module", "./stemmer-module",
		"--source", "./stemmer-module/stemmer.go",
		"--output", "./plugins/stemmers/snowball-ukrainian.so",
	})
	require.ErrorContains(t, err, "invalid stemmer plugin id")
}

func TestRunRejectsAdminStyleSnowballCommand(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{"stemmer", "package-snowball"}, &stdout, &stderr)
	require.Equal(t, 2, code)
	require.Contains(t, stderr.String(), "unknown command")
	require.NotContains(t, stdout.String(), "nornicdb-admin")
}

func TestDetectRuntimeImportAndRenderBridge(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "stemmer.go")
	require.NoError(t, os.WriteFile(source, []byte(`package main

import snowballRuntime "example.com/snowball/runtime"

func Stem(env *snowballRuntime.Env) bool { return true }
`), 0o644))

	runtimeImport, err := detectSnowballRuntimeImport(source)
	require.NoError(t, err)
	require.Equal(t, "example.com/snowball/runtime", runtimeImport)
	bridge := renderBridgeSource(runtimeImport)
	require.Contains(t, bridge, `snowballRuntime "example.com/snowball/runtime"`)
	require.Contains(t, bridge, "var Plugin generatedStemmer")
	require.Contains(t, bridge, "func (generatedStemmer) Stem(token string) string")
	require.Contains(t, bridge, "func (generatedStemmer) StemTokens(tokens []string) []string")
	require.Contains(t, bridge, "env := envPool.Get().(*snowballRuntime.Env)")
	require.Contains(t, bridge, "for i, token := range tokens")
}

func TestPackageSnowballBuildsPluginAndManifest(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Go buildmode=plugin is not available on Windows")
	}
	moduleDir := writeSnowballFixtureModule(t)
	output := filepath.Join(t.TempDir(), "plugins", "stemmers", "snowball-ukrainian.so")

	err := packageSnowball(packageOptions{
		Language: "ukrainian",
		ID:       "snowball.ukrainian",
		Version:  "1.0.0",
		Module:   moduleDir,
		Source:   filepath.Join(moduleDir, "stemmer.go"),
		Output:   output,
	})
	require.NoError(t, err)

	manifestBytes, err := os.ReadFile(manifestPath(output))
	require.NoError(t, err)
	var manifest stemmer.Manifest
	require.NoError(t, json.Unmarshal(manifestBytes, &manifest))
	require.Equal(t, stemmer.ManifestSchemaVersion, manifest.SchemaVersion)
	require.Equal(t, stemmer.APIVersion, manifest.APIVersion)
	require.Equal(t, stemmer.ManifestType, manifest.Type)
	require.Equal(t, "snowball.ukrainian", manifest.ID)
	require.Equal(t, "1.0.0", manifest.Version)
	require.Equal(t, "ukrainian", manifest.Language)
	require.Equal(t, "snowball-ukrainian.so", manifest.Library)
	require.Equal(t, stemmer.EntrypointSymbol, manifest.Entrypoint)
	require.Len(t, manifest.SHA256, 64)
	require.NoError(t, stemmer.ValidateManifest(manifest))
	libraryPath, err := stemmer.VerifyLibrary(manifestPath(output), manifest)
	require.NoError(t, err)
	require.Equal(t, output, libraryPath)

	loaded, err := plugin.Open(output)
	require.NoError(t, err)
	sym, err := loaded.Lookup("Plugin")
	require.NoError(t, err)
	impl, ok := sym.(interface{ Stem(string) string })
	require.True(t, ok)
	require.Equal(t, "україна-stem", impl.Stem("україна"))
	batch, ok := sym.(interface{ StemTokens([]string) []string })
	require.True(t, ok)
	require.Equal(t, []string{"україна-stem", "пошук-stem"}, batch.StemTokens([]string{"україна", "пошук"}))
}

func writeSnowballFixtureModule(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "vendor", "example.com", "snowball", "runtime"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte(`module example.com/fixture

go 1.26.4

require example.com/snowball/runtime v0.0.0
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.sum"), nil, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "vendor", "modules.txt"), []byte(`# example.com/snowball/runtime v0.0.0
## explicit; go 1.26.4
example.com/snowball/runtime
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "vendor", "example.com", "snowball", "runtime", "env.go"), []byte(`package runtime

type Env struct{ current string }

func NewEnv(current string) *Env { return &Env{current: current} }
func (e *Env) SetCurrent(current string) { e.current = current }
func (e *Env) Current() string { return e.current }
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "stemmer.go"), []byte(`package main

import snowballRuntime "example.com/snowball/runtime"

func Stem(env *snowballRuntime.Env) bool {
	env.SetCurrent(env.Current() + "-stem")
	return true
}
`), 0o644))
	return dir
}

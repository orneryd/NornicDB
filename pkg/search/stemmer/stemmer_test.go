package stemmer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type suffixPlugin struct{}

func (suffixPlugin) Stem(token string) string { return token + "-stem" }

func TestManifestValidationRequiresMinimalContractFields(t *testing.T) {
	valid := Manifest{
		SchemaVersion: ManifestSchemaVersion,
		APIVersion:    APIVersion,
		Type:          ManifestType,
		ID:            "snowball.ukrainian",
		Version:       "1.0.0",
		Language:      "ukrainian",
		Library:       "snowball-ukrainian.so",
		SHA256:        strings.Repeat("a", 64),
		Entrypoint:    EntrypointSymbol,
	}
	require.NoError(t, ValidateManifest(valid))

	for _, test := range []struct {
		name     string
		mutate   func(*Manifest)
		wantText string
	}{
		{name: "path library", mutate: func(m *Manifest) { m.Library = "../snowball.so" }, wantText: "adjacent basename"},
		{name: "path id", mutate: func(m *Manifest) { m.ID = "./snowball.so" }, wantText: "invalid stemmer plugin id"},
		{name: "bad digest", mutate: func(m *Manifest) { m.SHA256 = "ABC" }, wantText: "sha256"},
		{name: "wrong type", mutate: func(m *Manifest) { m.Type = "function" }, wantText: "unsupported stemmer type"},
	} {
		t.Run(test.name, func(t *testing.T) {
			manifest := valid
			test.mutate(&manifest)
			require.ErrorContains(t, ValidateManifest(manifest), test.wantText)
		})
	}
}

func TestVerifyLibraryChecksAdjacentDigest(t *testing.T) {
	dir := t.TempDir()
	library := filepath.Join(dir, "snowball-ukrainian.so")
	require.NoError(t, os.WriteFile(library, []byte("fixture plugin bytes"), 0o644))
	digest, err := SHA256File(library)
	require.NoError(t, err)

	manifest := Manifest{
		SchemaVersion: ManifestSchemaVersion,
		APIVersion:    APIVersion,
		Type:          ManifestType,
		ID:            "snowball.ukrainian",
		Version:       "1.0.0",
		Library:       filepath.Base(library),
		SHA256:        digest,
		Entrypoint:    EntrypointSymbol,
	}
	manifestPath := filepath.Join(dir, "snowball-ukrainian.stemmer.json")
	got, err := VerifyLibrary(manifestPath, manifest)
	require.NoError(t, err)
	require.Equal(t, library, got)

	manifest.SHA256 = strings.Repeat("b", 64)
	_, err = VerifyLibrary(manifestPath, manifest)
	require.ErrorContains(t, err, "digest mismatch")
}

func TestRegistryRejectsDuplicateIDs(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)
	reg := Registration{
		APIVersion: APIVersion,
		ID:         "snowball.ukrainian",
		Version:    "1.0.0",
		Digest:     strings.Repeat("c", 64),
		Stem:       suffixPlugin{}.Stem,
	}
	require.NoError(t, Register(reg))
	require.ErrorContains(t, Register(reg), "duplicate")

	got, ok := Lookup("snowball.ukrainian")
	require.True(t, ok)
	require.Equal(t, "word-stem", got.Stem("word"))
	require.Len(t, Available(), 1)
}

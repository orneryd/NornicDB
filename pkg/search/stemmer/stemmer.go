// Package stemmer defines NornicDB's dependency-free BM25 stemmer plugin ABI.
package stemmer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
)

const (
	// APIVersion is the current NornicDB stemmer plugin ABI version.
	APIVersion = 1
	// ManifestSchemaVersion is the current .stemmer.json schema version.
	ManifestSchemaVersion = 1
	// ManifestType identifies BM25 stemmer plugin manifests.
	ManifestType = "bm25_stemmer"
	// EntrypointSymbol is the only accepted Go plugin symbol for stemmers.
	EntrypointSymbol = "Plugin"
	// NoneID disables stemming and preserves the default BM25 analyzer.
	NoneID = "none"
)

// Plugin is the runtime ABI implemented by loaded stemmer plugins.
//
// Stem receives one normalized, non-empty UTF-8 token and returns one
// normalized, non-empty UTF-8 token. Implementations must be deterministic and
// safe for concurrent calls.
type Plugin interface {
	Stem(token string) string
}

// Manifest describes a local stemmer plugin artifact.
type Manifest struct {
	SchemaVersion int    `json:"schema_version"`
	APIVersion    int    `json:"api_version"`
	Type          string `json:"type"`
	ID            string `json:"id"`
	Version       string `json:"version"`
	Language      string `json:"language,omitempty"`
	Library       string `json:"library"`
	SHA256        string `json:"sha256"`
	Entrypoint    string `json:"entrypoint"`
}

// Registration is the immutable runtime entry stored in the process registry.
type Registration struct {
	APIVersion int
	ID         string
	Version    string
	Language   string
	Digest     string
	Path       string
	Stem       func(string) string
	StemTokens func([]string) []string
}

var (
	registryMu sync.RWMutex
	registry   = map[string]Registration{}
)

var (
	pluginIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)
	hex64Pattern    = regexp.MustCompile(`^[a-f0-9]{64}$`)
)

// NormalizeSelection returns the canonical configured stemmer ID.
func NormalizeSelection(raw string) string {
	id := strings.ToLower(strings.TrimSpace(raw))
	if id == "" {
		return NoneID
	}
	return id
}

// ValidPluginID reports whether id is a legal operator-facing plugin ID.
func ValidPluginID(id string) bool {
	return pluginIDPattern.MatchString(id) && id != NoneID
}

// ValidateManifest validates the manifest fields that do not require filesystem access.
func ValidateManifest(m Manifest) error {
	if m.SchemaVersion != ManifestSchemaVersion {
		return fmt.Errorf("unsupported stemmer manifest schema %d", m.SchemaVersion)
	}
	if m.APIVersion != APIVersion {
		return fmt.Errorf("unsupported stemmer API version %d", m.APIVersion)
	}
	if m.Type != ManifestType {
		return fmt.Errorf("unsupported stemmer type %q", m.Type)
	}
	if !ValidPluginID(m.ID) {
		return fmt.Errorf("invalid stemmer plugin id %q", m.ID)
	}
	if strings.TrimSpace(m.Version) == "" {
		return errors.New("stemmer version is required")
	}
	if filepath.Base(m.Library) != m.Library || m.Library == "." || m.Library == "" || strings.ContainsAny(m.Library, `/\`) {
		return fmt.Errorf("stemmer library must be an adjacent basename: %q", m.Library)
	}
	if filepath.Ext(m.Library) != ".so" {
		return fmt.Errorf("stemmer library must be a .so file: %q", m.Library)
	}
	if !hex64Pattern.MatchString(m.SHA256) {
		return errors.New("stemmer sha256 must be a lowercase 64-character hex digest")
	}
	if m.Entrypoint != EntrypointSymbol {
		return fmt.Errorf("unsupported stemmer entrypoint %q", m.Entrypoint)
	}
	return nil
}

// ReadManifest reads and validates a .stemmer.json file.
func ReadManifest(path string) (Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, err
	}
	if err := ValidateManifest(manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

// LibraryPath resolves manifest.Library next to manifestPath after validation.
func LibraryPath(manifestPath string, manifest Manifest) (string, error) {
	if err := ValidateManifest(manifest); err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(manifestPath), manifest.Library), nil
}

// SHA256File returns the lowercase SHA-256 digest for path.
func SHA256File(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// VerifyLibrary checks that the adjacent library exists, is regular, and matches the manifest digest.
func VerifyLibrary(manifestPath string, manifest Manifest) (string, error) {
	libraryPath, err := LibraryPath(manifestPath, manifest)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(libraryPath)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("stemmer library must not be a symlink: %s", libraryPath)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("stemmer library is not a regular file: %s", libraryPath)
	}
	digest, err := SHA256File(libraryPath)
	if err != nil {
		return "", err
	}
	if digest != manifest.SHA256 {
		return "", fmt.Errorf("stemmer digest mismatch for %s", filepath.Base(libraryPath))
	}
	return libraryPath, nil
}

// Register adds one immutable registration to the process registry.
func Register(reg Registration) error {
	id := NormalizeSelection(reg.ID)
	if !ValidPluginID(id) {
		return fmt.Errorf("invalid stemmer plugin id %q", reg.ID)
	}
	if reg.APIVersion != APIVersion {
		return fmt.Errorf("unsupported stemmer API version %d", reg.APIVersion)
	}
	if strings.TrimSpace(reg.Version) == "" {
		return errors.New("stemmer version is required")
	}
	if strings.TrimSpace(reg.Digest) == "" {
		return errors.New("stemmer digest is required")
	}
	if reg.Stem == nil {
		return errors.New("stemmer function is required")
	}
	reg.ID = id
	reg.Digest = strings.ToLower(strings.TrimSpace(reg.Digest))

	registryMu.Lock()
	defer registryMu.Unlock()
	if existing, exists := registry[id]; exists {
		if sameArtifact(existing, reg) {
			return nil
		}
		return fmt.Errorf("duplicate stemmer plugin id %q", id)
	}
	registry[id] = reg
	return nil
}

func sameArtifact(a, b Registration) bool {
	return a.Path != "" && b.Path != "" &&
		a.APIVersion == b.APIVersion &&
		a.ID == b.ID &&
		a.Version == b.Version &&
		a.Language == b.Language &&
		a.Digest == b.Digest &&
		a.Path == b.Path
}

// Lookup returns a registered stemmer by ID.
func Lookup(id string) (Registration, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	reg, ok := registry[NormalizeSelection(id)]
	return reg, ok
}

// Available returns registered stemmers ordered by ID.
func Available() []Registration {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make([]Registration, 0, len(registry))
	for _, reg := range registry {
		out = append(out, reg)
	}
	slices.SortFunc(out, func(a, b Registration) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// ResetForTest clears the process registry for package tests.
func ResetForTest() {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry = map[string]Registration{}
}

// LoadDir scans *.stemmer.json manifests, verifies adjacent libraries, opens Go plugins,
// and registers their Stem implementations.
func LoadDir(dir string) error {
	_, err := loadDir(dir, false)
	return err
}

// LoadDirBestEffort loads every valid plugin independently and returns
// per-plugin diagnostics for artifacts that were quarantined. Directory-level
// errors are returned because no plugin discovery was possible.
func LoadDirBestEffort(dir string) ([]error, error) {
	return loadDir(dir, true)
}

func loadDir(dir string, continueOnPluginError bool) ([]error, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, nil
	}
	info, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("checking stemmer plugins directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("stemmer plugins path is not a directory: %s", dir)
	}
	matches, err := filepath.Glob(filepath.Join(dir, "*.stemmer.json"))
	if err != nil {
		return nil, fmt.Errorf("scanning stemmer plugins directory: %w", err)
	}
	slices.Sort(matches)
	var diagnostics []error
	for _, manifestPath := range matches {
		if err := loadManifest(manifestPath); err != nil {
			diagnostic := fmt.Errorf("%s: %w", filepath.Base(manifestPath), err)
			if !continueOnPluginError {
				return diagnostics, diagnostic
			}
			diagnostics = append(diagnostics, diagnostic)
		}
	}
	return diagnostics, nil
}

func loadManifest(manifestPath string) error {
	manifest, err := ReadManifest(manifestPath)
	if err != nil {
		return err
	}
	libraryPath, err := VerifyLibrary(manifestPath, manifest)
	if err != nil {
		return err
	}
	pluginImpl, err := openPlugin(libraryPath, manifest.Entrypoint)
	if err != nil {
		return err
	}
	return Register(Registration{
		APIVersion: manifest.APIVersion,
		ID:         manifest.ID,
		Version:    manifest.Version,
		Language:   manifest.Language,
		Digest:     manifest.SHA256,
		Path:       libraryPath,
		Stem:       pluginImpl.Stem,
		StemTokens: batchStemFunc(pluginImpl),
	})
}

type batchPlugin interface {
	StemTokens(tokens []string) []string
}

func batchStemFunc(plugin Plugin) func([]string) []string {
	if plugin == nil {
		return nil
	}
	batch, ok := plugin.(batchPlugin)
	if !ok {
		return nil
	}
	return batch.StemTokens
}

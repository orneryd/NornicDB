// Package tck provides reproducible inventory and baseline helpers for the
// official openCypher Technology Compatibility Kit corpus vendored with NornicDB.
package tck

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	gherkin "github.com/cucumber/gherkin/go/v26"
	messages "github.com/cucumber/messages/go/v21"
)

// UpstreamRevision is the immutable openCypher commit used by the corpus.
const UpstreamRevision = "370fe27f417730dca2ef712dd1c0c5dadcb99ef8"

// ArchiveSHA256 identifies the exact GitHub source archive used to vendor the corpus.
const ArchiveSHA256 = "9f8e8bb027664e1a951b529a37fe44330cbb7f670cf41fdbd4af65d3ac5369aa"

// Inventory describes the complete expanded corpus rather than only its files.
type Inventory struct {
	UpstreamRevision string         `json:"upstream_revision"`
	ArchiveSHA256    string         `json:"archive_sha256"`
	Files            int            `json:"feature_files"`
	Scenarios        int            `json:"expanded_scenarios"`
	Steps            int            `json:"expanded_steps"`
	Families         map[string]int `json:"scenarios_by_family"`
	CorpusSHA256     string         `json:"corpus_sha256"`
}

// BuildInventory parses every feature and expands scenario outlines to Pickles.
func BuildInventory(featuresRoot string) (Inventory, error) {
	paths := make([]string, 0, 220)
	err := filepath.WalkDir(featuresRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".feature") {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return Inventory{}, fmt.Errorf("walk TCK features: %w", err)
	}
	sort.Strings(paths)

	inv := Inventory{
		UpstreamRevision: UpstreamRevision,
		ArchiveSHA256:    ArchiveSHA256,
		Families:         make(map[string]int),
	}
	digest := sha256.New()
	ids := &messages.Incrementing{}
	for _, path := range paths {
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return Inventory{}, fmt.Errorf("read %s: %w", path, readErr)
		}
		rel, relErr := filepath.Rel(featuresRoot, path)
		if relErr != nil {
			return Inventory{}, fmt.Errorf("relative path %s: %w", path, relErr)
		}
		_, _ = digest.Write([]byte(filepath.ToSlash(rel)))
		_, _ = digest.Write([]byte{0})
		_, _ = digest.Write(content)
		_, _ = digest.Write([]byte{0})

		doc, parseErr := gherkin.ParseGherkinDocument(strings.NewReader(string(content)), ids.NewId)
		if parseErr != nil {
			return Inventory{}, fmt.Errorf("parse %s: %w", rel, parseErr)
		}
		pickles := gherkin.Pickles(*doc, filepath.ToSlash(rel), ids.NewId)
		family := strings.Split(filepath.ToSlash(rel), "/")[0]
		inv.Files++
		inv.Scenarios += len(pickles)
		inv.Families[family] += len(pickles)
		for _, pickle := range pickles {
			inv.Steps += len(pickle.Steps)
		}
	}
	inv.CorpusSHA256 = hex.EncodeToString(digest.Sum(nil))
	return inv, nil
}

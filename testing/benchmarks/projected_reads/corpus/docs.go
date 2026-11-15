// Package corpus builds deterministic mixed-entropy documentation records for storage experiments.
package corpus

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Record keeps fixture identity and typed properties independent of an engine.
type Record struct {
	ID         string
	Properties map[string]any
}

// Replicate keeps source text/metrics but gives each record independent byte
// noise. Repeated high-entropy payloads must not become artificial compression wins.
func Replicate(base []Record, count int) []Record {
	var records []Record
	for replica := 0; replica < count; replica++ {
		for _, source := range base {
			properties := make(map[string]any, len(source.Properties))
			for k, v := range source.Properties {
				properties[k] = v
			}
			id := fmt.Sprintf("%s-%02d", source.ID, replica)
			noise := make([]byte, 4096)
			for at := 0; at < len(noise); at += sha256.Size {
				hash := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", id, at)))
				copy(noise[at:], hash[:])
			}
			properties["noise"] = noise
			records = append(records, Record{ID: id, Properties: properties})
		}
	}
	return records
}
func Load(root string) ([]Record, string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() && strings.HasSuffix(p, ".md") {
			paths = append(paths, p)
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return nil, "", fmt.Errorf("no Markdown documents under %s", root)
	}
	var nodes []Record
	digest := sha256.New()
	for _, path := range paths {
		relative, e := filepath.Rel(root, path)
		if e != nil {
			return nil, "", e
		}
		relative = filepath.ToSlash(relative)
		body, e := os.ReadFile(path)
		if e != nil {
			return nil, "", e
		}
		hash := sha256.Sum256(body)
		fmt.Fprintf(digest, "%s\x00%x\n", relative, hash)
		id := sha256.Sum256([]byte(relative))
		noise := make([]byte, 4096)
		for i := 0; i < len(noise); i += 32 {
			h := sha256.Sum256([]byte(fmt.Sprintf("%s/%x/%d", relative, hash, i)))
			copy(noise[i:], h[:])
		}
		metrics := make([]int64, 256)
		for i := range metrics {
			metrics[i] = int64((len(body) + i) % 64)
		}
		nodes = append(nodes, Record{ID: "docs:" + hex.EncodeToString(id[:16]), Properties: map[string]any{"path": relative, "body": string(body), "noise": noise, "metrics": metrics, "status": "current"}})
	}
	return nodes, hex.EncodeToString(digest.Sum(nil)), nil
}

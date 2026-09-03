package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const walManifestVersion = 1

// WALSegment describes a sealed WAL segment stored on disk.
type WALSegment struct {
	FirstSeq  uint64    `json:"first_seq"`
	LastSeq   uint64    `json:"last_seq"`
	SizeBytes int64     `json:"size_bytes"`
	CreatedAt time.Time `json:"created_at"`
	Path      string    `json:"path"`
}

// WALManifest indexes all sealed WAL segments.
type WALManifest struct {
	Version  int          `json:"version"`
	Segments []WALSegment `json:"segments"`
}

func walSegmentsDir(walDir string) string {
	return filepath.Join(walDir, "segments")
}

func walManifestPath(walDir string) string {
	return filepath.Join(walSegmentsDir(walDir), "manifest.json")
}

func walActivePath(walDir string) string {
	return filepath.Join(walDir, "wal.log")
}

func loadWALManifest(walDir string) (*WALManifest, error) {
	manifestPath := walManifestPath(walDir)
	file, err := os.Open(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &WALManifest{Version: walManifestVersion}, nil
		}
		return nil, err
	}
	defer file.Close()

	var manifest WALManifest
	if err := json.NewDecoder(file).Decode(&manifest); err != nil {
		return nil, err
	}
	if manifest.Version == 0 {
		manifest.Version = walManifestVersion
	}
	sort.Slice(manifest.Segments, func(i, j int) bool {
		return manifest.Segments[i].FirstSeq < manifest.Segments[j].FirstSeq
	})
	return &manifest, nil
}

func writeWALManifest(walDir string, manifest *WALManifest) error {
	if manifest == nil {
		return fmt.Errorf("wal: manifest is nil")
	}
	if manifest.Version == 0 {
		manifest.Version = walManifestVersion
	}

	segmentDir := walSegmentsDir(walDir)
	if err := os.MkdirAll(segmentDir, 0755); err != nil {
		return err
	}

	tmpPath := walManifestPath(walDir) + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(manifest); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}
	file.Close()

	if err := os.Rename(tmpPath, walManifestPath(walDir)); err != nil {
		os.Remove(tmpPath)
		return err
	}

	return syncDir(segmentDir)
}

func scanSegmentsFromDir(walDir string) ([]WALSegment, error) {
	segmentDir := walSegmentsDir(walDir)
	entries, err := os.ReadDir(segmentDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var segments []WALSegment
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		firstSeq, lastSeq, ok := parseSegmentFilename(entry.Name())
		if !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		segments = append(segments, WALSegment{
			FirstSeq:  firstSeq,
			LastSeq:   lastSeq,
			SizeBytes: info.Size(),
			CreatedAt: info.ModTime(),
			Path:      entry.Name(),
		})
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].FirstSeq < segments[j].FirstSeq
	})
	return segments, nil
}

func parseSegmentFilename(name string) (uint64, uint64, bool) {
	var first, last uint64
	if _, err := fmt.Sscanf(name, "seg-%d-%d.wal", &first, &last); err != nil {
		return 0, 0, false
	}
	return first, last, true
}

func resolveWALSegmentPath(walDir, segmentName string) (string, error) {
	if segmentName == "" {
		return "", fmt.Errorf("wal: empty segment path in manifest")
	}
	if filepath.IsAbs(segmentName) {
		return "", fmt.Errorf("wal: invalid absolute segment path in manifest: %q", segmentName)
	}

	cleanName := filepath.Clean(segmentName)
	if cleanName == "." || cleanName == ".." {
		return "", fmt.Errorf("wal: invalid segment path in manifest: %q", segmentName)
	}
	// Segments are single files directly under the segments dir; disallow nested paths.
	if filepath.Base(cleanName) != cleanName || strings.Contains(cleanName, string(filepath.Separator)) {
		return "", fmt.Errorf("wal: invalid segment path in manifest: %q", segmentName)
	}
	if _, _, ok := parseSegmentFilename(cleanName); !ok {
		return "", fmt.Errorf("wal: invalid segment filename in manifest: %q", segmentName)
	}

	segmentDir := walSegmentsDir(walDir)
	segmentPath := filepath.Join(segmentDir, cleanName)
	rel, err := filepath.Rel(segmentDir, segmentPath)
	if err != nil {
		return "", fmt.Errorf("wal: resolve segment path %q: %w", segmentName, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("wal: segment path escapes segments directory: %q", segmentName)
	}
	return segmentPath, nil
}

// ReadWALEntriesFromDir reads WAL entries across all segments and the active WAL file.
func ReadWALEntriesFromDir(walDir string) ([]WALEntry, error) {
	manifest, err := loadWALManifest(walDir)
	if err != nil {
		return nil, err
	}

	if len(manifest.Segments) == 0 {
		if scanned, scanErr := scanSegmentsFromDir(walDir); scanErr == nil && len(scanned) > 0 {
			manifest.Segments = scanned
		}
	}

	var entries []WALEntry
	for _, segment := range manifest.Segments {
		segmentPath, err := resolveWALSegmentPath(walDir, segment.Path)
		if err != nil {
			return nil, err
		}
		segmentEntries, err := ReadWALEntries(segmentPath)
		if err != nil {
			return nil, err
		}
		entries = append(entries, segmentEntries...)
	}

	activePath := walActivePath(walDir)
	if _, err := os.Stat(activePath); err == nil {
		activeEntries, err := ReadWALEntries(activePath)
		if err != nil {
			return nil, err
		}
		entries = append(entries, activeEntries...)
	}

	return entries, nil
}

// ReadWALEntriesAfterFromDir reads WAL entries after a given sequence across all segments.
func ReadWALEntriesAfterFromDir(walDir string, afterSeq uint64) ([]WALEntry, error) {
	all, err := ReadWALEntriesFromDir(walDir)
	if err != nil {
		return nil, err
	}

	filtered := make([]WALEntry, 0, len(all))
	for _, entry := range all {
		if entry.Sequence > afterSeq {
			filtered = append(filtered, entry)
		}
	}
	return filtered, nil
}

// VisitWALEntriesAfterFromDir visits WAL entries after the given sequence in
// segment order without retaining the complete WAL in memory.
func VisitWALEntriesAfterFromDir(walDir string, afterSeq uint64, visit func(WALEntry) error) error {
	manifest, err := loadWALManifest(walDir)
	if err != nil {
		return err
	}
	if len(manifest.Segments) == 0 {
		if scanned, scanErr := scanSegmentsFromDir(walDir); scanErr == nil && len(scanned) > 0 {
			manifest.Segments = scanned
		}
	}

	visitFile := func(path string) error {
		return visitWALEntriesWithLogger(path, discardWALSlog(), func(entry WALEntry) error {
			if entry.Sequence <= afterSeq {
				return nil
			}
			return visit(entry)
		})
	}
	for _, segment := range manifest.Segments {
		segmentPath, err := resolveWALSegmentPath(walDir, segment.Path)
		if err != nil {
			return err
		}
		if err := visitFile(segmentPath); err != nil {
			return err
		}
	}

	activePath := walActivePath(walDir)
	if _, err := os.Stat(activePath); err == nil {
		return visitFile(activePath)
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}

// ReadWALEntriesRangeFromDir reads entries in [fromSeq, toSeq] (inclusive).
func ReadWALEntriesRangeFromDir(walDir string, fromSeq, toSeq uint64) ([]WALEntry, error) {
	all, err := ReadWALEntriesFromDir(walDir)
	if err != nil {
		return nil, err
	}

	filtered := make([]WALEntry, 0, len(all))
	for _, entry := range all {
		if entry.Sequence < fromSeq {
			continue
		}
		if toSeq > 0 && entry.Sequence > toSeq {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered, nil
}

// FindWALEntriesByTxID scans entries and returns those with a matching tx_id.
// Use maxEntries <= 0 to return all matches.
func FindWALEntriesByTxID(walDir, txID string, maxEntries int) ([]WALEntry, error) {
	if txID == "" {
		return nil, fmt.Errorf("tx_id is required")
	}

	all, err := ReadWALEntriesFromDir(walDir)
	if err != nil {
		return nil, err
	}

	matches := make([]WALEntry, 0)
	for _, entry := range all {
		if entryTxID := GetEntryTxID(entry); entryTxID == txID {
			matches = append(matches, entry)
			if maxEntries > 0 && len(matches) >= maxEntries {
				break
			}
		}
	}

	return matches, nil
}

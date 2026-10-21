package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Auto-compaction owns this name and its interrupted-write .tmp sibling.
const autoSnapshotNameLayout = "snapshot-20060102-150405.000000000.json"

func removeInterruptedSnapshotFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("wal: read interrupted snapshots: %w", err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".tmp")
		stamp, err := time.Parse(autoSnapshotNameLayout, name)
		if err != nil || stamp.Format(autoSnapshotNameLayout) != name {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
			return fmt.Errorf("wal: remove interrupted snapshot %q: %w", entry.Name(), err)
		}
	}
	return nil
}

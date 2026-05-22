package nornicdb

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// autoRecoverOnCorruptionEnabled controls whether NornicDB should attempt to recover
// from WAL snapshots when the primary Badger store fails to open.
//
// The original data directory is always preserved via rename before rebuilding a fresh store.
func autoRecoverOnCorruptionEnabled() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("NORNICDB_AUTO_RECOVER_ON_CORRUPTION")))
	if v == "" {
		// Default to enabled: it matches the "Neo4j behavior" expectation that an unclean
		// shutdown should not require manual deletion to restart.
		return true
	}
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func looksLikeCorruption(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	// Heuristics: prefer catching real corruption/format issues, not permissions.
	return strings.Contains(s, "corrupt") ||
		strings.Contains(s, "checksum") ||
		strings.Contains(s, "verify()") ||
		strings.Contains(s, "verify") ||
		strings.Contains(s, "manifest") ||
		strings.Contains(s, "log truncate required") ||
		strings.Contains(s, "badger") && strings.Contains(s, "truncate") ||
		strings.Contains(s, "property key id") && strings.Contains(s, "not in dictionary") ||
		strings.Contains(s, "value log")
}

// hasRecoverableArtifacts returns true if the data directory appears to contain recovery inputs:
// snapshots and/or a WAL (active wal.log or sealed segments).
//
// This is used to avoid "recovering" into an empty database when there is nothing to replay.
func hasRecoverableArtifacts(dataDir string) bool {
	// Snapshots.
	if _, err := latestSnapshotPath(filepath.Join(dataDir, "snapshots")); err == nil {
		return true
	}

	// WAL: active file.
	walDir := filepath.Join(dataDir, "wal")
	activeWAL := filepath.Join(walDir, "wal.log")
	if st, err := os.Stat(activeWAL); err == nil && st.Size() > 0 {
		return true
	}

	// WAL: sealed segments (seg-*-*.wal).
	segmentsDir := filepath.Join(walDir, "segments")
	if entries, err := os.ReadDir(segmentsDir); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if strings.HasPrefix(name, "seg-") && strings.HasSuffix(name, ".wal") {
				return true
			}
		}
	}

	return false
}

func latestSnapshotPath(snapshotDir string) (string, error) {
	entries, err := os.ReadDir(snapshotDir)
	if err != nil {
		return "", err
	}

	type cand struct {
		path    string
		modTime time.Time
	}
	candidates := make([]cand, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, "snapshot-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		candidates = append(candidates, cand{
			path:    filepath.Join(snapshotDir, name),
			modTime: info.ModTime(),
		})
	}

	if len(candidates) == 0 {
		return "", fmt.Errorf("no snapshots found in %s", snapshotDir)
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].modTime.After(candidates[j].modTime)
	})
	return candidates[0].path, nil
}

// recoverBadgerFromSnapshotAndWAL rebuilds a new Badger store in-place from the latest
// snapshot + WAL replay, preserving the original directory by renaming it.
func recoverBadgerFromSnapshotAndWAL(dataDir string, badgerOpts storage.BadgerOptions) (*storage.BadgerEngine, string, error) {
	walDir := filepath.Join(dataDir, "wal")
	snapshotDir := filepath.Join(dataDir, "snapshots")

	snapPath, snapErr := latestSnapshotPath(snapshotDir)
	if snapErr != nil {
		// No snapshots yet (e.g., new DB) or snapshot directory missing — attempt WAL-only recovery.
		// This can fully recover data when WAL still contains the full history (pre-compaction),
		// and is still better than "delete the data directory" when snapshots haven't been created.
		fmt.Printf("⚠️  Auto-recover: no snapshots found (%v); attempting WAL-only recovery\n", snapErr)
		snapPath = ""
	}

	// Rebuild state in memory from snapshot + WAL. This does not depend on Badger.
	memEngine, replay, err := storage.RecoverFromWALWithResult(walDir, snapPath)
	if err != nil {
		return nil, "", fmt.Errorf("auto-recover: wal replay failed: %w", err)
	}

	nodes, err := memEngine.AllNodes()
	if err != nil {
		return nil, "", fmt.Errorf("auto-recover: read recovered nodes: %w", err)
	}
	edges, err := memEngine.AllEdges()
	if err != nil {
		return nil, "", fmt.Errorf("auto-recover: read recovered edges: %w", err)
	}

	// Preserve original directory for forensics/manual recovery.
	ts := time.Now().Format("20060102-150405")
	backupDir := fmt.Sprintf("%s.corrupted-%s", strings.TrimRight(dataDir, string(os.PathSeparator)), ts)
	for i := 1; ; i++ {
		if _, err := os.Stat(backupDir); os.IsNotExist(err) {
			break
		}
		backupDir = fmt.Sprintf("%s.corrupted-%s-%d", strings.TrimRight(dataDir, string(os.PathSeparator)), ts, i)
	}

	if err := os.Rename(dataDir, backupDir); err != nil {
		return nil, "", fmt.Errorf("auto-recover: failed to preserve corrupted data dir (%s → %s): %w", dataDir, backupDir, err)
	}

	// Recreate data directory and a fresh Badger store.
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, backupDir, fmt.Errorf("auto-recover: failed to recreate data dir %s: %w", dataDir, err)
	}

	badgerOpts.DataDir = dataDir
	newStore, err := storage.NewBadgerEngineWithOptions(badgerOpts)
	if err != nil {
		return nil, backupDir, fmt.Errorf("auto-recover: failed to open fresh badger store: %w", err)
	}

	// Restore recovered state into the fresh store.
	if err := storage.BulkCreateNodesForRecovery(newStore, nodes); err != nil {
		_ = newStore.Close()
		return nil, backupDir, fmt.Errorf("auto-recover: failed to restore nodes into fresh store: %w", err)
	}
	if err := storage.BulkCreateEdgesForRecovery(newStore, edges); err != nil {
		_ = newStore.Close()
		return nil, backupDir, fmt.Errorf("auto-recover: failed to restore edges into fresh store: %w", err)
	}

	// Best-effort: surface replay health in logs (callers can decide how to report).
	if replay.Failed > 0 {
		fmt.Printf("⚠️  Auto-recover replay completed with errors: %s\n", replay.Summary())
	}

	return newStore, backupDir, nil
}

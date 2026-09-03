package nornicdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// RecoveryPhase identifies the current corruption-recovery step.
type RecoveryPhase string

const (
	RecoveryPhaseInspect  RecoveryPhase = "inspect"
	RecoveryPhasePreserve RecoveryPhase = "preserve"
	RecoveryPhaseOpen     RecoveryPhase = "open_destination"
	RecoveryPhaseReplay   RecoveryPhase = "replay"
	RecoveryPhaseComplete RecoveryPhase = "complete"
)

// RecoveryStatus is persisted beside preserved data for operator diagnostics.
type RecoveryStatus struct {
	Phase             RecoveryPhase `json:"phase"`
	StartedAt         time.Time     `json:"started_at"`
	CompletedAt       time.Time     `json:"completed_at,omitempty"`
	SourceDataDir     string        `json:"source_data_dir"`
	PreservedDataDir  string        `json:"preserved_data_dir,omitempty"`
	SnapshotPath      string        `json:"snapshot_path,omitempty"`
	SnapshotStreaming bool          `json:"snapshot_streaming"`
	SnapshotNodes     uint64        `json:"snapshot_nodes"`
	SnapshotEdges     uint64        `json:"snapshot_edges"`
	WALEntries        uint64        `json:"wal_entries"`
	Applied           int           `json:"applied"`
	Skipped           int           `json:"skipped"`
	Failed            int           `json:"failed"`
	Error             string        `json:"error,omitempty"`
}

// StoreUnavailableError reports that the store could not safely complete recovery.
type StoreUnavailableError struct {
	Status RecoveryStatus
	Cause  error
}

func (e *StoreUnavailableError) Error() string {
	return fmt.Sprintf("store unavailable: recovery failed during %s: %v", e.Status.Phase, e.Cause)
}

func (e *StoreUnavailableError) Unwrap() error { return e.Cause }

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
// snapshot + WAL replay, preserving the original store before rebuilding it.
func recoverBadgerFromSnapshotAndWAL(dataDir string, badgerOpts storage.BadgerOptions) (*storage.BadgerEngine, string, error) {
	status := RecoveryStatus{
		Phase:         RecoveryPhaseInspect,
		StartedAt:     time.Now().UTC(),
		SourceDataDir: dataDir,
	}
	info, err := os.Stat(dataDir)
	if err != nil || !info.IsDir() {
		if err == nil {
			err = fmt.Errorf("data path is not a directory")
		}
		return nil, "", recoveryUnavailable(status, err)
	}

	snapshotDir := filepath.Join(dataDir, "snapshots")
	snapPath, snapErr := latestSnapshotPath(snapshotDir)
	if snapErr != nil {
		// No snapshots yet (e.g., new DB) or snapshot directory missing — attempt WAL-only recovery.
		// This can fully recover data when WAL still contains the full history (pre-compaction),
		// and is still better than "delete the data directory" when snapshots haven't been created.
		fmt.Printf("⚠️  Auto-recover: no snapshots found (%v); attempting WAL-only recovery\n", snapErr)
		snapPath = ""
	}
	status.SnapshotPath = snapPath

	// Preserve original directory for forensics/manual recovery.
	ts := time.Now().Format("20060102-150405")
	backupDir := fmt.Sprintf("%s.corrupted-%s", strings.TrimRight(dataDir, string(os.PathSeparator)), ts)
	for i := 1; ; i++ {
		if _, err := os.Stat(backupDir); os.IsNotExist(err) {
			break
		}
		backupDir = fmt.Sprintf("%s.corrupted-%s-%d", strings.TrimRight(dataDir, string(os.PathSeparator)), ts, i)
	}

	status.Phase = RecoveryPhasePreserve
	preservedDir, err := preserveCorruptedDataDir(dataDir, backupDir, os.Rename)
	if err != nil {
		return nil, "", recoveryUnavailable(status, fmt.Errorf("failed to preserve corrupted data dir (%s → %s): %w", dataDir, backupDir, err))
	}
	backupDir = preservedDir
	status.PreservedDataDir = backupDir
	walDir := filepath.Join(backupDir, "wal")
	if snapPath != "" {
		relativeSnapshot, relErr := filepath.Rel(dataDir, snapPath)
		if relErr != nil {
			return nil, backupDir, finishRecoveryFailure(status, backupDir, relErr)
		}
		snapPath = filepath.Join(backupDir, relativeSnapshot)
		status.SnapshotPath = snapPath
	}

	// Recreate data directory and a fresh Badger store.
	status.Phase = RecoveryPhaseOpen
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, backupDir, finishRecoveryFailure(status, backupDir, fmt.Errorf("failed to recreate data dir %s: %w", dataDir, err))
	}

	badgerOpts.DataDir = dataDir
	newStore, err := storage.NewBadgerEngineWithOptions(badgerOpts)
	if err != nil {
		return nil, backupDir, finishRecoveryFailure(status, backupDir, fmt.Errorf("failed to open fresh badger store: %w", err))
	}

	status.Phase = RecoveryPhaseReplay
	replay, streamStatus, err := storage.RecoverIntoEngine(newStore, walDir, snapPath)
	status.SnapshotStreaming = streamStatus.SnapshotStreaming
	status.SnapshotNodes = streamStatus.SnapshotNodes
	status.SnapshotEdges = streamStatus.SnapshotEdges
	status.WALEntries = streamStatus.WALEntries
	status.Applied = replay.Applied
	status.Skipped = replay.Skipped
	status.Failed = replay.Failed
	if err != nil {
		_ = newStore.Close()
		return nil, backupDir, finishRecoveryFailure(status, backupDir, err)
	}

	// Best-effort: surface replay health in logs (callers can decide how to report).
	if replay.Failed > 0 {
		fmt.Printf("⚠️  Auto-recover replay completed with errors: %s\n", replay.Summary())
	}
	status.Phase = RecoveryPhaseComplete
	status.CompletedAt = time.Now().UTC()
	if err := writeRecoveryManifest(backupDir, status); err != nil {
		_ = newStore.Close()
		return nil, backupDir, recoveryUnavailable(status, fmt.Errorf("write recovery manifest: %w", err))
	}

	return newStore, backupDir, nil
}

func recoveryUnavailable(status RecoveryStatus, err error) error {
	status.Error = err.Error()
	status.CompletedAt = time.Now().UTC()
	return &StoreUnavailableError{Status: status, Cause: err}
}

func finishRecoveryFailure(status RecoveryStatus, backupDir string, err error) error {
	unavailable := recoveryUnavailable(status, err)
	failedStatus := unavailable.(*StoreUnavailableError).Status
	if manifestErr := writeRecoveryManifest(backupDir, failedStatus); manifestErr != nil {
		return &StoreUnavailableError{Status: status, Cause: fmt.Errorf("%w (write recovery manifest: %v)", err, manifestErr)}
	}
	if manifestErr := writeRecoveryManifest(status.SourceDataDir, failedStatus); manifestErr != nil {
		return &StoreUnavailableError{Status: status, Cause: fmt.Errorf("%w (write unavailable marker: %v)", err, manifestErr)}
	}
	return unavailable
}

func readRecoveryManifest(dataDir string) (RecoveryStatus, error) {
	var status RecoveryStatus
	file, err := os.Open(filepath.Join(dataDir, "recovery-manifest.json"))
	if err != nil {
		return status, err
	}
	defer file.Close()
	if err := json.NewDecoder(file).Decode(&status); err != nil {
		return status, err
	}
	return status, nil
}

func writeRecoveryManifest(backupDir string, status RecoveryStatus) error {
	if backupDir == "" {
		return nil
	}
	path := filepath.Join(backupDir, "recovery-manifest.json")
	tempPath := path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	removeTemp := true
	defer func() {
		_ = file.Close()
		if removeTemp {
			_ = os.Remove(tempPath)
		}
	}()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(status); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	removeTemp = false
	dir, err := os.Open(backupDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

type renamePathFunc func(oldPath, newPath string) error

// preserveCorruptedDataDir first attempts to move the complete data directory.
// Bind-mount roots cannot be renamed on Linux, so EBUSY falls back to moving the
// directory's children into a hidden preservation directory within the mount.
func preserveCorruptedDataDir(dataDir, backupDir string, rename renamePathFunc) (string, error) {
	if err := rename(dataDir, backupDir); err == nil {
		return backupDir, nil
	} else if !errors.Is(err, syscall.EBUSY) {
		return "", err
	}

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return "", fmt.Errorf("read mount-root data dir: %w", err)
	}

	base := "." + filepath.Base(backupDir)
	mountBackupDir := filepath.Join(dataDir, base)
	for i := 1; ; i++ {
		if _, statErr := os.Stat(mountBackupDir); os.IsNotExist(statErr) {
			break
		} else if statErr != nil {
			return "", fmt.Errorf("inspect mount-root preserve dir %s: %w", mountBackupDir, statErr)
		}
		mountBackupDir = filepath.Join(dataDir, fmt.Sprintf("%s-%d", base, i))
	}
	if err := os.Mkdir(mountBackupDir, 0755); err != nil {
		return "", fmt.Errorf("create mount-root preserve dir %s: %w", mountBackupDir, err)
	}

	moved := make([]string, 0, len(entries))
	for _, entry := range entries {
		source := filepath.Join(dataDir, entry.Name())
		destination := filepath.Join(mountBackupDir, entry.Name())
		if err := rename(source, destination); err != nil {
			rollbackErr := rollbackPreservedChildren(dataDir, mountBackupDir, moved, rename)
			if rollbackErr != nil {
				return "", fmt.Errorf("move %s into mount-root preserve dir: %w (rollback failed: %v)", source, err, rollbackErr)
			}
			return "", fmt.Errorf("move %s into mount-root preserve dir: %w", source, err)
		}
		moved = append(moved, entry.Name())
	}

	return mountBackupDir, nil
}

func rollbackPreservedChildren(dataDir, backupDir string, moved []string, rename renamePathFunc) error {
	for i := len(moved) - 1; i >= 0; i-- {
		name := moved[i]
		if err := rename(filepath.Join(backupDir, name), filepath.Join(dataDir, name)); err != nil {
			return err
		}
	}
	return os.Remove(backupDir)
}

package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

func (w *WAL) backupCorruptedWAL(walPath string) string {
	timestamp := time.Now().Format("20060102-150405")
	backupName := fmt.Sprintf("wal-corrupted-%s.log", timestamp)
	backupPath := filepath.Join(w.config.Dir, backupName)

	src, err := os.Open(walPath)
	if err != nil {
		w.config.Logger.Log("warn", "wal backup open failed", map[string]any{
			"wal_path": walPath,
			"error":    err.Error(),
		})
		return ""
	}
	defer src.Close()

	dst, err := os.Create(backupPath)
	if err != nil {
		w.config.Logger.Log("warn", "wal backup create failed", map[string]any{
			"wal_path":    walPath,
			"backup_path": backupPath,
			"error":       err.Error(),
		})
		return ""
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		w.config.Logger.Log("warn", "wal backup copy failed", map[string]any{
			"wal_path":    walPath,
			"backup_path": backupPath,
			"error":       err.Error(),
		})
		_ = os.Remove(backupPath)
		return ""
	}

	// Sync backup to ensure it's durable
	if err := dst.Sync(); err != nil {
		w.config.Logger.Log("warn", "wal backup sync failed", map[string]any{
			"backup_path": backupPath,
			"error":       err.Error(),
		})
	}

	// Sync directory entry so the backup filename itself is durable.
	if err := syncDir(w.config.Dir); err != nil {
		w.config.Logger.Log("warn", "wal backup directory sync failed", map[string]any{
			"wal_dir": w.config.Dir,
			"error":   err.Error(),
		})
	}

	return backupPath
}

func (w *WAL) reportCorruption(diag *CorruptionDiagnostics, cause error) {
	if diag == nil {
		return
	}

	w.degraded.Store(true)
	w.lastCorruption.Store(diag)

	fields := map[string]any{
		"wal_path":         diag.WALPath,
		"file_size":        diag.FileSize,
		"corrupted_seq":    diag.CorruptedSeq,
		"last_good_seq":    diag.LastGoodSeq,
		"expected_crc":     diag.ExpectedCRC,
		"actual_crc":       diag.ActualCRC,
		"recovery_action":  diag.RecoveryAction,
		"backup_path":      diag.BackupPath,
		"suspected_cause":  diag.SuspectedCause,
		"timestamp_rfc3339": diag.Timestamp.Format(time.RFC3339),
	}
	if cause != nil {
		fields["error"] = cause.Error()
	}

	// Persist JSON artifact for forensics/programmatic access.
	diagPath := filepath.Join(filepath.Dir(diag.WALPath),
		fmt.Sprintf("wal-corruption-%s.json", diag.Timestamp.Format("20060102-150405")))
	if data, err := json.MarshalIndent(diag, "", "  "); err == nil {
		if err := os.WriteFile(diagPath, data, 0644); err == nil {
			fields["diagnostics_path"] = diagPath
		}
	}

	w.config.Logger.Log("error", "wal corruption detected", fields)

	if w.config.OnCorruption != nil {
		w.config.OnCorruption(diag, cause)
	}
}


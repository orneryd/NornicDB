package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// repairWALTailIfNeeded detects a partially-written or corrupted tail record and truncates
// the WAL back to the last known-good record boundary.
//
// Why this exists:
//   - A process crash (OOM kill, segfault, power loss) can leave a torn WAL record at EOF.
//   - If we then append AFTER that torn record, the WAL becomes permanently unreadable because
//     parsing will hit the invalid tail bytes before it can reach newer valid records.
//
// This repair is intentionally conservative:
// - It only truncates from the first bad record boundary to EOF
// - It never modifies *valid* prefix records
// - It does not attempt resynchronization (no "scan for next magic") to avoid false positives
func repairWALTailIfNeeded(walPath string, logger WALLogger) (bool, *CorruptionDiagnostics, error) {
	if walPath == "" {
		return false, nil, nil
	}

	fi, err := os.Stat(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil, nil
		}
		return false, nil, fmt.Errorf("wal: startup repair stat failed: %w", err)
	}
	if fi.Mode().IsRegular() && fi.Size() == 0 {
		return false, nil, nil
	}

	// Only attempt repair for the atomic WAL format (magic header).
	// Legacy JSON format is kept for backward compatibility but is not repaired here.
	f, err := os.Open(walPath)
	if err != nil {
		return false, nil, fmt.Errorf("wal: startup repair open failed: %w", err)
	}
	header := make([]byte, 4)
	n, readErr := io.ReadFull(f, header)
	_ = f.Close()
	if readErr != nil {
		// If we can't even read the header, treat as partial tail and truncate to 0.
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			return truncateWALFile(walPath, 0, 0, "incomplete_tail", logger)
		}
		return false, nil, fmt.Errorf("wal: startup repair header read failed: %w", readErr)
	}
	if n != 4 {
		return truncateWALFile(walPath, 0, 0, "incomplete_tail", logger)
	}

	if binary.LittleEndian.Uint32(header) != walMagic {
		// Not atomic format: no-op.
		return false, nil, nil
	}

	// Open RW so we can truncate in place if needed.
	rw, err := os.OpenFile(walPath, os.O_RDWR, 0)
	if err != nil {
		return false, nil, fmt.Errorf("wal: startup repair open(rw) failed: %w", err)
	}
	defer rw.Close()

	truncateOffset, lastGoodSeq, reason, ok, err := scanAtomicWALForRepair(rw, fi.Size())
	if err != nil {
		return false, nil, err
	}
	if ok {
		return false, nil, nil
	}

	return truncateWALFile(walPath, truncateOffset, lastGoodSeq, reason, logger)
}

func scanAtomicWALForRepair(file *os.File, fileSize int64) (truncateOffset int64, lastGoodSeq uint64, reason string, ok bool, err error) {
	offset := int64(0)
	lastGoodSeq = 0

	for {
		if offset == fileSize {
			return 0, lastGoodSeq, "", true, nil
		}
		if offset > fileSize {
			// Should be impossible, but be safe.
			return offset, lastGoodSeq, "inconsistent_size", false, nil
		}

		// Need at least a full header to start a record.
		if fileSize-offset < 9 {
			return offset, lastGoodSeq, "incomplete_tail", false, nil
		}

		header := make([]byte, 9)
		if _, err := file.ReadAt(header, offset); err != nil {
			if err == io.EOF {
				return offset, lastGoodSeq, "incomplete_tail", false, nil
			}
			return 0, 0, "", false, fmt.Errorf("wal: startup repair read header failed: %w", err)
		}

		magic := binary.LittleEndian.Uint32(header[0:4])
		if magic != walMagic {
			// Treat as corrupted tail: truncate from this boundary.
			return offset, lastGoodSeq, "invalid_magic", false, nil
		}

		version := header[4]
		if version > walFormatVersionPropertyEpoch {
			// A newer format is not evidence of a torn write. Never destroy data
			// merely because this binary cannot read it.
			return 0, lastGoodSeq, "", false, fmt.Errorf("wal: unsupported WAL version %d (max supported: %d)", version, walFormatVersionPropertyEpoch)
		}

		payloadLen := binary.LittleEndian.Uint32(header[5:9])
		if payloadLen > walMaxEntrySize {
			return offset, lastGoodSeq, "invalid_payload_size", false, nil
		}

		// Ensure record fits in file (payload + crc at minimum).
		minRecordLen := int64(9) + int64(payloadLen) + int64(4)
		if fileSize-offset < minRecordLen {
			return offset, lastGoodSeq, "incomplete_tail", false, nil
		}

		// Read payload.
		payload := make([]byte, payloadLen)
		payloadOff := offset + 9
		if _, err := file.ReadAt(payload, payloadOff); err != nil {
			if err == io.EOF {
				return offset, lastGoodSeq, "incomplete_tail", false, nil
			}
			return 0, 0, "", false, fmt.Errorf("wal: startup repair read payload failed: %w", err)
		}

		// Read CRC.
		crcBuf := make([]byte, 4)
		crcOff := payloadOff + int64(payloadLen)
		if _, err := file.ReadAt(crcBuf, crcOff); err != nil {
			if err == io.EOF {
				return offset, lastGoodSeq, "incomplete_tail", false, nil
			}
			return 0, 0, "", false, fmt.Errorf("wal: startup repair read crc failed: %w", err)
		}
		storedCRC := binary.LittleEndian.Uint32(crcBuf)
		computedCRC := crc32Checksum(payload)
		if storedCRC != computedCRC {
			return offset, lastGoodSeq, "crc_mismatch", false, nil
		}

		// Version 2+: verify trailer and padding.
		recordLen := minRecordLen
		if version >= 2 {
			// Trailer must be present.
			if fileSize-offset < recordLen+8 {
				return offset, lastGoodSeq, "incomplete_tail", false, nil
			}

			trailerBuf := make([]byte, 8)
			trailerOff := crcOff + 4
			if _, err := file.ReadAt(trailerBuf, trailerOff); err != nil {
				if err == io.EOF {
					return offset, lastGoodSeq, "incomplete_tail", false, nil
				}
				return 0, 0, "", false, fmt.Errorf("wal: startup repair read trailer failed: %w", err)
			}
			if binary.LittleEndian.Uint64(trailerBuf) != walTrailer {
				return offset, lastGoodSeq, "invalid_trailer", false, nil
			}

			// Account for trailer + padding alignment.
			rawRecordLen := recordLen + 8
			alignedRecordLen := alignUp(rawRecordLen)
			if fileSize-offset < alignedRecordLen {
				return offset, lastGoodSeq, "incomplete_tail", false, nil
			}
			recordLen = alignedRecordLen
		}

		// Decode entry to track last-good sequence (best-effort).
		entry, err := decodeWALEntry(payload, version)
		if err != nil {
			return offset, lastGoodSeq, "invalid_json", false, nil
		}
		// Verify inner data checksum (defense-in-depth).
		if entry.Checksum != crc32Checksum(entry.Data) {
			return offset, lastGoodSeq, "data_checksum_mismatch", false, nil
		}

		lastGoodSeq = entry.Sequence
		offset += recordLen
	}
}

func truncateWALFile(walPath string, truncateOffset int64, lastGoodSeq uint64, reason string, logger WALLogger) (bool, *CorruptionDiagnostics, error) {
	if truncateOffset < 0 {
		truncateOffset = 0
	}

	fi, err := os.Stat(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil, nil
		}
		return false, nil, fmt.Errorf("wal: startup repair stat(truncate) failed: %w", err)
	}
	if fi.Mode().IsRegular() && truncateOffset >= fi.Size() {
		// Nothing to do.
		return false, nil, nil
	}

	now := time.Now()
	diag := &CorruptionDiagnostics{
		Timestamp:      now,
		WALPath:        walPath,
		CorruptedSeq:   lastGoodSeq + 1,
		FileSize:       fi.Size(),
		LastGoodSeq:    lastGoodSeq,
		RecoveryAction: "truncate_incomplete_tail",
	}
	if reason != "" && reason != "incomplete_tail" {
		diag.RecoveryAction = "truncate_corrupt_tail"
		diag.Operation = reason
	}
	diag.diagnoseCause()

	// Persist JSON artifact (best-effort, matches reportCorruption behavior).
	diagPath := filepath.Join(filepath.Dir(walPath),
		fmt.Sprintf("wal-corruption-%s.json", now.Format("20060102-150405")))
	if data, err := json.MarshalIndent(diag, "", "  "); err == nil {
		_ = os.WriteFile(diagPath, data, 0644)
	}

	f, err := os.OpenFile(walPath, os.O_RDWR, 0)
	if err != nil {
		return false, diag, fmt.Errorf("wal: startup repair open(truncate) failed: %w", err)
	}
	defer f.Close()

	if err := f.Truncate(truncateOffset); err != nil {
		return false, diag, fmt.Errorf("wal: startup repair truncate failed: %w", err)
	}
	// Best-effort sync so the repaired tail is durable.
	_ = f.Sync()
	_ = syncDir(filepath.Dir(walPath))

	if logger != nil {
		logger.Log("warn", "wal repaired by truncating corrupted tail", map[string]any{
			"wal_path":          walPath,
			"truncate_offset":   truncateOffset,
			"original_size":     fi.Size(),
			"last_good_seq":     lastGoodSeq,
			"recovery_action":   diag.RecoveryAction,
			"suspected_cause":   diag.SuspectedCause,
			"timestamp_rfc3339": now.Format(time.RFC3339),
		})
	}

	return true, diag, nil
}

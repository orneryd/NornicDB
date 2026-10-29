package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// ErrWALUnsupportedVersion means the selected reader cannot use the WAL frame
// version. Supported older frames can be converted by the V4 upgrade; unknown
// formats are never treated as a torn current record.
var ErrWALUnsupportedVersion = errors.New("unsupported WAL format")

func unsupportedWALVersion(version byte) error {
	return fmt.Errorf("%w: unsupported WAL version %d (supported: %d)", ErrWALUnsupportedVersion, version, walFormatVersionCurrent)
}

// WALFormatValidation selects the read-only format check at a WAL boundary.
type WALFormatValidation uint8

const (
	// WALValidateCurrent admits only records readable by the running WAL.
	WALValidateCurrent WALFormatValidation = iota
	// WALValidateV4Upgrade validates historical records for the one-time V4 migration.
	WALValidateV4Upgrade
)

// ValidateWALFormat checks the active file and retained segments without
// repairing or rewriting them. Upgrade validation also checks checksums and
// historical property-authority boundaries before the storage is opened.
func ValidateWALFormat(dir string, mode WALFormatValidation) error {
	if mode != WALValidateCurrent && mode != WALValidateV4Upgrade {
		return fmt.Errorf("wal: unknown validation mode %d", mode)
	}
	manifest, err := accessWALManifest(dir, walReadManifest, nil)
	if err != nil {
		return fmt.Errorf("wal: read manifest: %w", err)
	}
	for _, segment := range manifest.Segments {
		path, err := resolveWALSegmentPath(dir, segment.Path)
		if err != nil {
			return err
		}
		if mode == WALValidateV4Upgrade {
			if _, err := planV4WALMigration(path, false); err != nil {
				return err
			}
		} else if err := validateWALFileVersion(path, false); err != nil {
			return err
		}
	}
	active := walActivePath(dir)
	if mode == WALValidateV4Upgrade {
		if _, err := os.Stat(active); os.IsNotExist(err) {
			return nil
		} else if err != nil {
			return err
		}
		_, err := planV4WALMigration(active, true)
		return err
	}
	return validateWALFileVersion(active, true)
}

func validateWALFileVersion(path string, optional bool) error {
	file, err := os.Open(path)
	if optional && os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	var offset int64
	var header [9]byte
	for {
		n, err := file.ReadAt(header[:], offset)
		if err != nil && err != io.EOF {
			return fmt.Errorf("wal: read %s: %w", path, err)
		}
		if n == 0 {
			return nil
		}
		if n < 4 {
			if offset == 0 && !bytes.HasPrefix([]byte("WALE"), header[:n]) {
				return fmt.Errorf("%w: unrecognized WAL header in %s", ErrWALUnsupportedVersion, path)
			}
			return nil // a partial current record is handled by the repair owner
		}
		if binary.LittleEndian.Uint32(header[:4]) != walMagic {
			trimmed := bytes.TrimSpace(header[:n])
			if offset == 0 || len(trimmed) > 0 && trimmed[0] == '{' {
				return fmt.Errorf("%w: unrecognized WAL header in %s", ErrWALUnsupportedVersion, path)
			}
			return nil // a damaged current tail is handled by the repair owner
		}
		if n >= 5 && header[4] != walFormatVersionCurrent {
			return fmt.Errorf("%s: %w", path, unsupportedWALVersion(header[4]))
		}
		if n < len(header) {
			return nil
		}
		size := binary.LittleEndian.Uint32(header[5:])
		if size > walMaxEntrySize {
			return nil // repair/integrity owner reports the malformed record
		}
		offset += alignUp(int64(9) + int64(size) + 4 + 8)
	}
}

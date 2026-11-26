package storage

import (
	"errors"
	"fmt"
	"os"
)

// ValidateWALFormatForEncryptionUpgrade checks a journal that may be only
// partially converted after an interrupted encrypted-store upgrade. Each
// file must be wholly readable as either the old plaintext format or the new
// authenticated format. It changes nothing and runs before Badger may upgrade
// its own files; EncryptWALDirectory performs the later atomic conversion.
func ValidateWALFormatForEncryptionUpgrade(dir string, key []byte, allowLegacy bool) error {
	keyed, err := walFrameReaderForKey(key)
	if err != nil {
		return err
	}
	if keyed.cipher == nil {
		return ErrWALKeyRequired
	}
	manifest, err := accessWALManifest(dir, walReadManifest, nil)
	if err != nil {
		return err
	}
	for _, segment := range manifest.Segments {
		path, err := resolveWALSegmentPath(dir, segment.Path)
		if err != nil {
			return err
		}
		if err := validateWALFileForEncryptionUpgrade(path, false, keyed, allowLegacy); err != nil {
			return err
		}
	}
	return validateWALFileForEncryptionUpgrade(walActivePath(dir), true, keyed, allowLegacy)
}

func validateWALFileForEncryptionUpgrade(path string, optional bool, keyed walFrameReader, allowLegacy bool) error {
	if _, err := os.Lstat(path); optional && os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	encrypted, err := walFileEncrypted(path)
	if err != nil {
		return err
	}
	if encrypted {
		return validateWALFileVersion(path, false, keyed)
	}
	err = validateWALFileVersion(path, false, walFrameReader{})
	if err == nil || !allowLegacy || !errors.Is(err, ErrWALUnsupportedVersion) {
		return err
	}
	if _, err := planV4WALMigration(path, optional, nil, true); err != nil {
		return fmt.Errorf("validate historical WAL %s for encryption upgrade: %w", path, err)
	}
	return nil
}

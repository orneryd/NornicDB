package nornicdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/orneryd/nornicdb/pkg/encryption"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const passwordSaltFile = "db.salt"
const providerKeyFile = "db.kms_dek.json"

// InitializeStorageEncryption provisions or opens an installation's Badger key.
// A new installation gets its own salt or provider-wrapped data key.
func InitializeStorageEncryption(dataDir string, cfg *Config) ([]byte, error) {
	return storageEncryptionKey(dataDir, cfg, false)
}

// ReadStorageEncryptionKey opens existing key metadata without provisioning or
// rotating it. Offline tools use it even while the server owns the root store.
func ReadStorageEncryptionKey(dataDir string, cfg *Config) ([]byte, error) {
	if !cfg.Database.EncryptionEnabled {
		for _, name := range []string{passwordSaltFile, providerKeyFile} {
			if _, err := os.Lstat(filepath.Join(dataDir, name)); !errors.Is(err, os.ErrNotExist) {
				if err != nil {
					return nil, err
				}
				return nil, errors.New("configure database encryption to use this data directory")
			}
		}
	}
	key, err := storageEncryptionKey(dataDir, cfg, true)
	if err != nil || len(key) == 0 {
		return key, err
	}
	if err := storage.VerifyBadgerEncryptionKey(dataDir, key); err != nil {
		return nil, err
	}
	return key, nil
}

func storageEncryptionKey(dataDir string, cfg *Config, existingOnly bool) ([]byte, error) {
	if !cfg.Database.EncryptionEnabled {
		return nil, nil
	}
	provider := strings.ToLower(strings.TrimSpace(cfg.Database.EncryptionProvider))
	if provider != "" && provider != "password" {
		if existingOnly {
			return providerManagedDBKey(dataDir, cfg, provider, true)
		}
		return resolveProviderManagedDBKey(dataDir, cfg, provider)
	}
	if cfg.Database.EncryptionPassword == "" {
		return nil, localizedError(localization.NornicDBCoreEncryptionPasswordRequired(), nil)
	}
	if !existingOnly {
		if err := os.MkdirAll(dataDir, 0700); err != nil {
			return nil, localizedError(localization.NornicDBCoreDataDirectoryCreateFailed(err), err)
		}
	}
	path := filepath.Join(dataDir, passwordSaltFile)
	salt, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) && !existingOnly {
		salt, err = createStorageSalt(dataDir, path)
	}
	if err != nil {
		return nil, err
	}
	if len(salt) != 32 {
		return nil, fmt.Errorf("invalid database encryption salt: %w", storage.ErrInvalidData)
	}
	return encryption.DeriveKey([]byte(cfg.Database.EncryptionPassword), salt, 600000), nil
}

func createStorageSalt(dir, path string) ([]byte, error) {
	salt, err := encryption.GenerateSalt()
	if err != nil {
		return nil, localizedError(localization.NornicDBCoreEncryptionSaltGenerateFailed(err), err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, localizedError(localization.NornicDBCoreEncryptionSaltSaveFailed(err), err)
	}
	_, writeErr := file.Write(salt)
	if err := errors.Join(writeErr, file.Sync(), file.Close(), storage.SyncDir(dir)); err != nil {
		return nil, localizedError(localization.NornicDBCoreEncryptionSaltSaveFailed(err), err)
	}
	return salt, nil
}

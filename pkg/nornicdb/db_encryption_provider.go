package nornicdb

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/kms"
	"github.com/orneryd/nornicdb/pkg/localization"
)

type persistedProviderDEK struct {
	Provider       string `json:"provider"`
	KeyURI         string `json:"key_uri"`
	CiphertextB64  string `json:"ciphertext_b64"`
	Algorithm      string `json:"algorithm"`
	Version        uint32 `json:"version"`
	CreatedAtRFC33 string `json:"created_at_rfc3339"`
}

func resolveProviderManagedDBKey(dataDir string, cfg *nornicConfig.Config, providerMode string) ([]byte, error) {
	archiver, err := newProviderAuditArchiver(dataDir, cfg)
	if err != nil {
		return nil, err
	}

	var masterKey []byte
	if providerMode == "local" {
		var err error
		masterKey, err = decodeProviderMasterKey(strings.TrimSpace(cfg.Database.EncryptionMasterKey))
		if err != nil {
			return nil, localizedError(localization.NornicDBCoreEncryptionMasterKeyInvalid(err), err)
		}
	}
	provider, err := kms.NewProvider(kms.FactoryConfig{
		Provider:   providerMode,
		KeyURI:     strings.TrimSpace(cfg.Database.EncryptionKeyURI),
		MasterKey:  masterKey,
		Archiver:   archiver,
		ProviderID: providerMode,
		AWS: kms.AWSProviderConfig{
			KeyID:                strings.TrimSpace(cfg.Database.EncryptionAWSKMSKeyID),
			Region:               strings.TrimSpace(cfg.Database.EncryptionAWSRegion),
			Endpoint:             strings.TrimSpace(cfg.Database.EncryptionAWSEndpoint),
			RoleARN:              strings.TrimSpace(cfg.Database.EncryptionAWSRoleARN),
			RoleSessionName:      strings.TrimSpace(cfg.Database.EncryptionAWSRoleSessionName),
			AccessKey:            strings.TrimSpace(cfg.Database.EncryptionAWSAccessKey),
			SecretKey:            strings.TrimSpace(cfg.Database.EncryptionAWSSecretKey),
			SessionToken:         strings.TrimSpace(cfg.Database.EncryptionAWSSessionToken),
			SharedCredsFilename:  strings.TrimSpace(cfg.Database.EncryptionAWSSharedCredsFilename),
			SharedCredsProfile:   strings.TrimSpace(cfg.Database.EncryptionAWSSharedCredsProfile),
			WebIdentityTokenFile: strings.TrimSpace(cfg.Database.EncryptionAWSWebIdentityTokenFile),
		},
		Azure: kms.AzureProviderConfig{
			VaultName:    strings.TrimSpace(cfg.Database.EncryptionAzureVaultName),
			KeyName:      strings.TrimSpace(cfg.Database.EncryptionAzureKeyName),
			TenantID:     strings.TrimSpace(cfg.Database.EncryptionAzureTenantID),
			ClientID:     strings.TrimSpace(cfg.Database.EncryptionAzureClientID),
			ClientSecret: strings.TrimSpace(cfg.Database.EncryptionAzureClientSecret),
			Environment:  strings.TrimSpace(cfg.Database.EncryptionAzureEnvironment),
			Resource:     strings.TrimSpace(cfg.Database.EncryptionAzureResource),
		},
		GCP: kms.GCPProviderConfig{
			Project:         strings.TrimSpace(cfg.Database.EncryptionGCPProject),
			Location:        strings.TrimSpace(cfg.Database.EncryptionGCPLocation),
			KeyRing:         strings.TrimSpace(cfg.Database.EncryptionGCPKeyRing),
			KeyName:         strings.TrimSpace(cfg.Database.EncryptionGCPKeyName),
			CredentialsFile: strings.TrimSpace(cfg.Database.EncryptionGCPCredentialsFile),
		},
	})
	if err != nil {
		return nil, localizedError(localization.NornicDBCoreEncryptionProviderInitializeFailed(providerMode, err), err)
	}
	defer kms.CloseProvider(provider)

	metadataPath := filepath.Join(dataDir, "db.kms_dek.json")
	if raw, readErr := os.ReadFile(metadataPath); readErr == nil {
		var persisted persistedProviderDEK
		if err := json.Unmarshal(raw, &persisted); err != nil {
			return nil, localizedError(localization.NornicDBCoreEncryptionDEKMetadataDecodeFailed(err), err)
		}
		if persisted.Provider != "" && persisted.Provider != providerMode {
			return nil, localizedError(localization.NornicDBCoreEncryptionDEKProviderMismatch(persisted.Provider, providerMode), nil)
		}
		encDEK, err := base64.StdEncoding.DecodeString(persisted.CiphertextB64)
		if err != nil {
			return nil, localizedError(localization.NornicDBCoreEncryptionDEKCiphertextDecodeFailed(err), err)
		}
		plain, err := provider.DecryptDataKey(context.Background(), encDEK, kms.DecryptOpts{KeyURI: persisted.KeyURI})
		if err != nil {
			return nil, localizedError(localization.NornicDBCoreEncryptionDEKUnwrapFailed(err), err)
		}
		if rotationDue(cfg, persisted) {
			rotated, err := provider.RotateDataKey(context.Background(), encDEK, kms.RotateOpts{
				KeyURI: persisted.KeyURI,
				Label:  "nornicdb-storage-key-rotation",
				TTL:    cfg.Database.EncryptionRotationInterval,
			})
			if err != nil {
				return nil, localizedError(localization.NornicDBCoreEncryptionDEKRotateFailed(err), err)
			}
			if err := persistProviderDEK(metadataPath, providerMode, rotated); err != nil {
				return nil, err
			}
			if len(rotated.Plaintext) != 0 {
				plain = rotated.Plaintext
			}
		}
		return plain, nil
	}

	dataKey, err := provider.GenerateDataKey(context.Background(), kms.KeyGenOpts{
		Algorithm: "AES-256-GCM",
		TTL:       90 * 24 * time.Hour,
		Label:     "nornicdb-storage-key",
	})
	if err != nil {
		return nil, localizedError(localization.NornicDBCoreEncryptionDEKGenerateFailed(err), err)
	}
	if len(dataKey.Plaintext) == 0 {
		return nil, localizedError(localization.NornicDBCoreEncryptionDEKEmpty(), nil)
	}
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, localizedError(localization.NornicDBCoreEncryptionDEKDirectoryCreateFailed(err), err)
	}

	if err := persistProviderDEK(metadataPath, providerMode, dataKey); err != nil {
		return nil, err
	}
	return dataKey.Plaintext, nil
}

func newProviderAuditArchiver(dataDir string, cfg *nornicConfig.Config) (*kms.AuditArchiver, error) {
	path := strings.TrimSpace(cfg.Database.EncryptionAuditLogPath)
	if path == "" {
		path = filepath.Join(dataDir, "encryption-audit.jsonl")
	}
	return kms.NewAuditArchiver(kms.AuditArchiverConfig{
		LocalPath:  path,
		SignEvents: cfg.Database.EncryptionAuditSignEvents,
		SignKey:    []byte(strings.TrimSpace(cfg.Database.EncryptionAuditSignKey)),
	})
}

func rotationDue(cfg *nornicConfig.Config, persisted persistedProviderDEK) bool {
	if cfg == nil || !cfg.Database.EncryptionRotationEnabled {
		return false
	}
	if cfg.Database.EncryptionRotationInterval <= 0 {
		return false
	}
	if strings.TrimSpace(persisted.CreatedAtRFC33) == "" {
		return true
	}
	createdAt, err := time.Parse(time.RFC3339Nano, persisted.CreatedAtRFC33)
	if err != nil {
		return true
	}
	return time.Since(createdAt) >= cfg.Database.EncryptionRotationInterval
}

func persistProviderDEK(metadataPath string, providerMode string, dataKey *kms.DataKey) error {
	persisted := persistedProviderDEK{
		Provider:       providerMode,
		KeyURI:         dataKey.KeyURI,
		CiphertextB64:  base64.StdEncoding.EncodeToString(dataKey.Ciphertext),
		Algorithm:      dataKey.Algorithm,
		Version:        dataKey.Version,
		CreatedAtRFC33: dataKey.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
	raw, err := json.MarshalIndent(persisted, "", "  ")
	if err != nil {
		return localizedError(localization.NornicDBCoreEncryptionDEKMetadataEncodeFailed(err), err)
	}
	if err := os.WriteFile(metadataPath, raw, 0600); err != nil {
		return localizedError(localization.NornicDBCoreEncryptionDEKMetadataPersistFailed(err), err)
	}
	return nil
}

func decodeProviderMasterKey(value string) ([]byte, error) {
	if value == "" {
		return nil, localizedError(localization.NornicDBCoreEncryptionMasterKeyRequired(), nil)
	}
	if raw, err := base64.StdEncoding.DecodeString(value); err == nil && len(raw) == 32 {
		return raw, nil
	}
	if raw, err := hex.DecodeString(value); err == nil && len(raw) == 32 {
		return raw, nil
	}
	raw := []byte(value)
	if len(raw) == 32 {
		return raw, nil
	}
	return nil, localizedError(localization.NornicDBCoreEncryptionMasterKeyLengthInvalid(), nil)
}

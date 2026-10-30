package nornicdb

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/kms"
	"github.com/stretchr/testify/require"
)

func TestDecodeProviderMasterKey(t *testing.T) {
	t.Parallel()
	raw := "example-test-key-do-not-use-0001"

	k, err := decodeProviderMasterKey(raw)
	require.NoError(t, err)
	require.Len(t, k, 32)

	hexKey := hex.EncodeToString([]byte(raw))
	k2, err := decodeProviderMasterKey(hexKey)
	require.NoError(t, err)
	require.Equal(t, []byte(raw), k2)

	b64Key := base64.StdEncoding.EncodeToString([]byte(raw))
	k3, err := decodeProviderMasterKey(b64Key)
	require.NoError(t, err)
	require.Equal(t, []byte(raw), k3)
}

func TestResolveProviderManagedDBKey_PersistsAndReuses(t *testing.T) {
	t.Parallel()
	cfg := nornicConfig.LoadDefaults()
	cfg.Database.EncryptionEnabled = true
	cfg.Database.EncryptionProvider = "local"
	cfg.Database.EncryptionMasterKey = "example-test-key-do-not-use-0001"
	cfg.Database.EncryptionKeyURI = "kms://local/nornicdb-test"

	dir := t.TempDir()
	k1, err := resolveProviderManagedDBKey(dir, cfg, "local")
	require.NoError(t, err)
	require.Len(t, k1, 32)

	k2, err := resolveProviderManagedDBKey(dir, cfg, "local")
	require.NoError(t, err)
	require.Equal(t, k1, k2)

	auditRaw, err := os.ReadFile(filepath.Join(dir, "encryption-audit.jsonl"))
	require.NoError(t, err)
	require.Contains(t, string(auditRaw), "KEY_GENERATED")
	require.Contains(t, string(auditRaw), "KEY_DECRYPTED")
}

func TestResolveProviderManagedDBKey_RotatesWrappedDEKMetadata(t *testing.T) {
	t.Parallel()
	cfg := nornicConfig.LoadDefaults()
	cfg.Database.EncryptionEnabled = true
	cfg.Database.EncryptionProvider = "local"
	cfg.Database.EncryptionMasterKey = "example-test-key-do-not-use-0001"
	cfg.Database.EncryptionKeyURI = "kms://local/nornicdb-test"
	cfg.Database.EncryptionRotationEnabled = true
	cfg.Database.EncryptionRotationInterval = time.Nanosecond

	dir := t.TempDir()
	firstKey, err := resolveProviderManagedDBKey(dir, cfg, "local")
	require.NoError(t, err)

	metadataPath := filepath.Join(dir, "db.kms_dek.json")
	raw, err := os.ReadFile(metadataPath)
	require.NoError(t, err)
	var persisted persistedProviderDEK
	require.NoError(t, json.Unmarshal(raw, &persisted))
	persisted.CreatedAtRFC33 = time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano)
	rewritten, err := json.MarshalIndent(persisted, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(metadataPath, rewritten, 0600))

	secondKey, err := resolveProviderManagedDBKey(dir, cfg, "local")
	require.NoError(t, err)
	require.Equal(t, firstKey, secondKey)

	raw, err = os.ReadFile(metadataPath)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &persisted))
	require.Equal(t, uint32(2), persisted.Version)
}

func TestResolveProviderManagedDBKey_MetadataErrorBranches(t *testing.T) {
	cfg := nornicConfig.LoadDefaults()
	cfg.Database.EncryptionEnabled = true
	cfg.Database.EncryptionProvider = "local"
	cfg.Database.EncryptionMasterKey = "example-test-key-do-not-use-0001"
	cfg.Database.EncryptionKeyURI = "kms://local/nornicdb-test"

	t.Run("invalid local master key", func(t *testing.T) {
		badCfg := nornicConfig.LoadDefaults()
		badCfg.Database.EncryptionMasterKey = "short"
		_, err := resolveProviderManagedDBKey(t.TempDir(), badCfg, "local")
		require.ErrorContains(t, err, "invalid encryption master key")
	})

	t.Run("malformed metadata json", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "db.kms_dek.json"), []byte("{"), 0600))
		_, err := resolveProviderManagedDBKey(dir, cfg, "local")
		require.ErrorContains(t, err, "failed to decode persisted DEK metadata")
	})

	t.Run("provider mismatch", func(t *testing.T) {
		dir := t.TempDir()
		metadata := persistedProviderDEK{Provider: "aws", CiphertextB64: base64.StdEncoding.EncodeToString([]byte("wrapped"))}
		raw, err := json.Marshal(metadata)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "db.kms_dek.json"), raw, 0600))
		_, err = resolveProviderManagedDBKey(dir, cfg, "local")
		require.ErrorContains(t, err, "persisted DEK was created with provider")
	})

	t.Run("invalid ciphertext encoding", func(t *testing.T) {
		dir := t.TempDir()
		metadata := persistedProviderDEK{Provider: "local", CiphertextB64: "not-base64!!!"}
		raw, err := json.Marshal(metadata)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "db.kms_dek.json"), raw, 0600))
		_, err = resolveProviderManagedDBKey(dir, cfg, "local")
		require.ErrorContains(t, err, "failed to decode persisted DEK ciphertext")
	})

	t.Run("audit signing config error", func(t *testing.T) {
		badCfg := nornicConfig.LoadDefaults()
		badCfg.Database.EncryptionAuditSignEvents = true
		_, err := resolveProviderManagedDBKey(t.TempDir(), badCfg, "local")
		require.ErrorContains(t, err, "audit signing key is required")
	})

	t.Run("unsupported provider", func(t *testing.T) {
		_, err := resolveProviderManagedDBKey(t.TempDir(), cfg, "unknown-kms")
		require.ErrorContains(t, err, "failed to initialize encryption provider")
	})

	t.Run("decrypt failure for persisted wrapped key", func(t *testing.T) {
		dir := t.TempDir()
		metadata := persistedProviderDEK{
			Provider:      "local",
			KeyURI:        "kms://local/nornicdb-test",
			CiphertextB64: base64.StdEncoding.EncodeToString([]byte("not-a-wrapped-key")),
		}
		raw, err := json.Marshal(metadata)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "db.kms_dek.json"), raw, 0600))
		_, err = resolveProviderManagedDBKey(dir, cfg, "local")
		require.ErrorContains(t, err, "failed to unwrap persisted DEK")
	})

	t.Run("data directory is a file", func(t *testing.T) {
		dir := t.TempDir()
		dataPath := filepath.Join(dir, "data-file")
		require.NoError(t, os.WriteFile(dataPath, []byte("x"), 0600))
		fileCfg := *cfg
		fileCfg.Database.EncryptionAuditLogPath = filepath.Join(dir, "audit.jsonl")
		_, err := resolveProviderManagedDBKey(dataPath, &fileCfg, "local")
		require.ErrorContains(t, err, "failed to create data directory")
	})
}

func TestPersistProviderDEK_WriteErrorBranch(t *testing.T) {
	dir := t.TempDir()
	err := persistProviderDEK(dir, "local", &kms.DataKey{Ciphertext: []byte("wrapped"), CreatedAt: time.Now()})
	require.ErrorContains(t, err, "failed to persist DEK metadata")
}

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadDefaults_EncryptionProviderDefaults(t *testing.T) {
	t.Parallel()
	cfg := LoadDefaults()
	require.Equal(t, "password", cfg.Database.EncryptionProvider)
	require.Empty(t, cfg.Database.EncryptionKeyURI)
	require.Empty(t, cfg.Database.EncryptionMasterKey)
}

func TestLoadFromEnv_EncryptionProvider(t *testing.T) {
	t.Setenv("NORNICDB_ENCRYPTION_ENABLED", "true")
	t.Setenv("NORNICDB_ENCRYPTION_PROVIDER", "local")
	t.Setenv("NORNICDB_ENCRYPTION_KEY_URI", "kms://local/testing")
	t.Setenv("NORNICDB_ENCRYPTION_MASTER_KEY", "example-test-key-do-not-use-0001")
	t.Setenv("NORNICDB_ENCRYPTION_AUDIT_LOG_PATH", "/tmp/nornicdb-audit.jsonl")
	t.Setenv("NORNICDB_ENCRYPTION_AUDIT_SIGN_EVENTS", "true")
	t.Setenv("NORNICDB_ENCRYPTION_AUDIT_SIGN_KEY", "audit-sign-key")
	t.Setenv("NORNICDB_ENCRYPTION_ROTATION_ENABLED", "false")
	t.Setenv("NORNICDB_ENCRYPTION_ROTATION_INTERVAL", "48h")
	t.Setenv("NORNICDB_ENCRYPTION_AWS_REGION", "us-east-1")
	t.Setenv("NORNICDB_ENCRYPTION_AZURE_VAULT_NAME", "vault-name")
	t.Setenv("NORNICDB_ENCRYPTION_GCP_PROJECT", "my-project")

	cfg := LoadFromEnv()
	require.True(t, cfg.Database.EncryptionEnabled)
	require.Equal(t, "local", cfg.Database.EncryptionProvider)
	require.Equal(t, "kms://local/testing", cfg.Database.EncryptionKeyURI)
	require.Equal(t, "example-test-key-do-not-use-0001", cfg.Database.EncryptionMasterKey)
	require.Equal(t, "/tmp/nornicdb-audit.jsonl", cfg.Database.EncryptionAuditLogPath)
	require.True(t, cfg.Database.EncryptionAuditSignEvents)
	require.Equal(t, "audit-sign-key", cfg.Database.EncryptionAuditSignKey)
	require.False(t, cfg.Database.EncryptionRotationEnabled)
	require.Equal(t, 48*time.Hour, cfg.Database.EncryptionRotationInterval)
	require.Equal(t, "us-east-1", cfg.Database.EncryptionAWSRegion)
	require.Equal(t, "vault-name", cfg.Database.EncryptionAzureVaultName)
	require.Equal(t, "my-project", cfg.Database.EncryptionGCPProject)
}

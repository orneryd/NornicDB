package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidate_EncryptionProviderRequirements(t *testing.T) {
	t.Parallel()

	t.Run("local provider requires master key", func(t *testing.T) {
		cfg := LoadDefaults()
		cfg.Database.EncryptionEnabled = true
		cfg.Database.EncryptionProvider = "local"
		require.ErrorContains(t, cfg.Validate(), "encryption_master_key")
	})

	t.Run("aws provider requires region and key id", func(t *testing.T) {
		cfg := LoadDefaults()
		cfg.Database.EncryptionEnabled = true
		cfg.Database.EncryptionProvider = "aws-kms"
		require.ErrorContains(t, cfg.Validate(), "encryption_aws_region")
	})

	t.Run("audit signing requires key", func(t *testing.T) {
		cfg := LoadDefaults()
		cfg.Database.EncryptionAuditSignEvents = true
		require.ErrorContains(t, cfg.Validate(), "signing key")
	})

	t.Run("negative rotation interval rejected", func(t *testing.T) {
		cfg := LoadDefaults()
		cfg.Database.EncryptionRotationInterval = -time.Hour
		require.ErrorContains(t, cfg.Validate(), "rotation interval")
	})

	t.Run("valid local provider config passes", func(t *testing.T) {
		cfg := LoadDefaults()
		cfg.Database.EncryptionEnabled = true
		cfg.Database.EncryptionProvider = "local"
		cfg.Database.EncryptionMasterKey = "example-test-key-do-not-use-0001"
		cfg.Database.EncryptionAuditSignEvents = true
		cfg.Database.EncryptionAuditSignKey = "audit-sign-key"
		require.NoError(t, cfg.Validate())
	})
}

package kms

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuditedProvider_ArchivesLifecycleEvents(t *testing.T) {
	t.Parallel()
	provider, err := NewLocalProvider(LocalConfig{
		MasterKey: []byte("0123456789abcdef0123456789abcdef"),
		KeyURI:    "kms://local/audited",
	})
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "audit.jsonl")
	archiver, err := NewAuditArchiver(AuditArchiverConfig{LocalPath: path})
	require.NoError(t, err)

	audited := WithAudit(provider, archiver, "local")
	dek, err := audited.GenerateDataKey(context.Background(), KeyGenOpts{Label: "db-key"})
	require.NoError(t, err)

	_, err = audited.DecryptDataKey(context.Background(), dek.Ciphertext, DecryptOpts{KeyURI: dek.KeyURI})
	require.NoError(t, err)

	_, err = audited.RotateDataKey(context.Background(), dek.Ciphertext, RotateOpts{KeyURI: dek.KeyURI, Label: "rewrap"})
	require.NoError(t, err)

	_, err = audited.DecryptDataKey(context.Background(), []byte("bad-ciphertext"), DecryptOpts{KeyURI: dek.KeyURI})
	require.Error(t, err)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	text := string(raw)
	require.Contains(t, text, "\"event_type\":\"KEY_GENERATED\"")
	require.Contains(t, text, "\"event_type\":\"KEY_DECRYPTED\"")
	require.Contains(t, text, "\"event_type\":\"KEY_ROTATED\"")
	require.Contains(t, text, "\"status\":\"FAILURE\"")
	require.True(t, strings.Count(text, "KEY_DECRYPTED") >= 2)
}

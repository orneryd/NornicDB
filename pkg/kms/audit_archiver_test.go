package kms

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAuditArchiver_Archive(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	arch, err := NewAuditArchiver(AuditArchiverConfig{
		LocalPath:  path,
		SignEvents: true,
		SignKey:    []byte("audit-archiver-signing-key"),
	})
	require.NoError(t, err)

	err = arch.Archive(context.Background(), AuditEvent{
		EventType: "KEY_GENERATED",
		KeyURI:    "kms://local/test",
		Status:    "SUCCESS",
		Timestamp: time.Now().UTC(),
	})
	require.NoError(t, err)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(raw), "\"event_type\":\"KEY_GENERATED\"")
	require.Contains(t, string(raw), "\"signature\":\"")
	require.True(t, strings.HasSuffix(string(raw), "\n"))
}

func TestAuditArchiver_ConfigValidation(t *testing.T) {
	t.Parallel()

	_, err := NewAuditArchiver(AuditArchiverConfig{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "path is required")

	_, err = NewAuditArchiver(AuditArchiverConfig{
		LocalPath:  filepath.Join(t.TempDir(), "audit.jsonl"),
		SignEvents: true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "signing key is required")
}

func TestAuditArchiver_ArchiveWithoutSigningAndCanceledContext(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "nested", "audit.jsonl")
	arch, err := NewAuditArchiver(AuditArchiverConfig{LocalPath: path})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = arch.Archive(ctx, AuditEvent{EventType: "KEY_GENERATED", Status: "SUCCESS"})
	require.ErrorIs(t, err, context.Canceled)
	_, statErr := os.Stat(path)
	require.Error(t, statErr)

	ctx = context.Background()
	require.NoError(t, arch.Archive(ctx, AuditEvent{EventType: "KEY_GENERATED", Status: "SUCCESS"}))
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(string(raw), "\n"))

	var event AuditEvent
	require.NoError(t, json.Unmarshal(raw, &event))
	require.Equal(t, "KEY_GENERATED", event.EventType)
	require.Equal(t, "SUCCESS", event.Status)
	require.True(t, event.Timestamp.After(time.Time{}))
	require.Empty(t, event.Signature)
}

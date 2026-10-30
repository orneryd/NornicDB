package kms

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditSigner_VerifyEmptySignature(t *testing.T) {
	t.Parallel()
	signer := NewAuditSigner([]byte("key-32-bytes-key-32-bytes-key-32"))
	event := AuditEvent{EventType: "TEST", Timestamp: time.Now()}
	// No signature set — Verify should return false
	assert.False(t, signer.Verify(event))
}

func TestAuditSigner_VerifyCorruptedSignature(t *testing.T) {
	t.Parallel()
	signer := NewAuditSigner([]byte("key-32-bytes-key-32-bytes-key-32"))
	event := AuditEvent{
		EventType: "TEST",
		Timestamp: time.Unix(1700000000, 0).UTC(),
		Status:    "SUCCESS",
	}
	signed, err := signer.Sign(event)
	require.NoError(t, err)

	// Corrupt the signature
	signed.Signature = "not-a-valid-hex"
	assert.False(t, signer.Verify(signed))

	// Valid hex but wrong value
	signed.Signature = "aabbccdd"
	assert.False(t, signer.Verify(signed))
}

func TestAuditSigner_DifferentKeysProduceDifferentSignatures(t *testing.T) {
	t.Parallel()
	signer1 := NewAuditSigner([]byte("key-one-32-bytes-key-one-32-byt"))
	signer2 := NewAuditSigner([]byte("key-two-32-bytes-key-two-32-byt"))
	event := AuditEvent{
		EventType: "TEST",
		Timestamp: time.Unix(1700000000, 0).UTC(),
		Status:    "SUCCESS",
	}
	signed1, err := signer1.Sign(event)
	require.NoError(t, err)
	signed2, err := signer2.Sign(event)
	require.NoError(t, err)

	assert.NotEqual(t, signed1.Signature, signed2.Signature)
	// Each signer only verifies its own
	assert.True(t, signer1.Verify(signed1))
	assert.False(t, signer1.Verify(signed2))
}

func TestNopAuditProvider(t *testing.T) {
	t.Parallel()
	var nop NopAuditProvider
	err := nop.SignAuditEvent(context.Background(), AuditEvent{EventType: "TEST"})
	assert.NoError(t, err)
}

func TestAuditArchiver_RequiresPath(t *testing.T) {
	t.Parallel()
	_, err := NewAuditArchiver(AuditArchiverConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "path is required")
}

func TestAuditArchiver_RequiresSignKeyWhenSignEnabled(t *testing.T) {
	t.Parallel()
	_, err := NewAuditArchiver(AuditArchiverConfig{
		LocalPath:  "/tmp/test",
		SignEvents: true,
		// No SignKey
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "signing key is required")
}

func TestAuditArchiver_ArchiveWithoutSigning(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "unsigned.jsonl")
	arch, err := NewAuditArchiver(AuditArchiverConfig{
		LocalPath: path,
	})
	require.NoError(t, err)

	err = arch.Archive(context.Background(), AuditEvent{
		EventType: "TEST",
		Status:    "SUCCESS",
	})
	require.NoError(t, err)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"event_type":"TEST"`)
	// No signature when signing disabled
	assert.NotContains(t, string(raw), `"signature":"`)
}

func TestAuditArchiver_ArchiveSetsTimestampWhenZero(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "ts.jsonl")
	arch, err := NewAuditArchiver(AuditArchiverConfig{LocalPath: path})
	require.NoError(t, err)

	err = arch.Archive(context.Background(), AuditEvent{
		EventType: "TEST",
		Status:    "SUCCESS",
		// Timestamp is zero
	})
	require.NoError(t, err)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	// Should have a non-zero timestamp
	assert.Contains(t, string(raw), `"timestamp":"`)
	assert.NotContains(t, string(raw), `"0001-01-01T00:00:00Z"`)
}

func TestAuditArchiver_ArchiveCanceledContext(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "canceled.jsonl")
	arch, err := NewAuditArchiver(AuditArchiverConfig{LocalPath: path})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err = arch.Archive(ctx, AuditEvent{
		EventType: "TEST",
		Status:    "SUCCESS",
		Timestamp: time.Now(),
	})
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestAuditArchiver_ArchiveMultipleEvents(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "multi.jsonl")
	arch, err := NewAuditArchiver(AuditArchiverConfig{LocalPath: path})
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		err = arch.Archive(context.Background(), AuditEvent{
			EventType: "TEST",
			Status:    "SUCCESS",
			Timestamp: time.Now(),
		})
		require.NoError(t, err)
	}

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	// Each event is a newline-delimited JSON line
	lines := 0
	for _, b := range raw {
		if b == '\n' {
			lines++
		}
	}
	assert.Equal(t, 5, lines)
}

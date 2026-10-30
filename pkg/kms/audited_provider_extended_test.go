package kms

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithAudit_NilArchiver_ReturnsUnwrapped(t *testing.T) {
	t.Parallel()
	p, err := NewLocalProvider(LocalConfig{MasterKey: testMasterKey()})
	require.NoError(t, err)

	// Nil archiver should return the provider unwrapped
	wrapped := WithAudit(p, nil, "test")
	assert.Equal(t, p, wrapped, "nil archiver should return original provider")
}

func TestWithAudit_NilProvider_ReturnsNil(t *testing.T) {
	t.Parallel()
	archiver, err := NewAuditArchiver(AuditArchiverConfig{
		LocalPath: filepath.Join(t.TempDir(), "audit.jsonl"),
	})
	require.NoError(t, err)

	wrapped := WithAudit(nil, archiver, "test")
	assert.Nil(t, wrapped)
}

func TestAuditedProvider_GetKeyMetadata_Success(t *testing.T) {
	t.Parallel()
	p, err := NewLocalProvider(LocalConfig{
		MasterKey: testMasterKey(),
		KeyURI:    "kms://local/meta",
	})
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "audit.jsonl")
	archiver, err := NewAuditArchiver(AuditArchiverConfig{LocalPath: path})
	require.NoError(t, err)

	audited := WithAudit(p, archiver, "local")

	meta, err := audited.GetKeyMetadata(context.Background(), "kms://local/meta")
	require.NoError(t, err)
	assert.Equal(t, "kms://local/meta", meta.KeyURI)
	assert.Equal(t, "local", meta.Provider)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"event_type":"KEY_METADATA_READ"`)
	assert.Contains(t, string(raw), `"status":"SUCCESS"`)
}

func TestAuditedProvider_GetKeyMetadata_Failure(t *testing.T) {
	t.Parallel()
	p, err := NewLocalProvider(LocalConfig{
		MasterKey: testMasterKey(),
		KeyURI:    "kms://local/meta",
	})
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "audit.jsonl")
	archiver, err := NewAuditArchiver(AuditArchiverConfig{LocalPath: path})
	require.NoError(t, err)

	audited := WithAudit(p, archiver, "local")

	_, err = audited.GetKeyMetadata(context.Background(), "kms://local/wrong-key")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrKeyNotFound))

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"status":"FAILURE"`)
	assert.Contains(t, string(raw), `"error_code":"GET_KEY_METADATA_FAILED"`)
}

func TestAuditedProvider_RotateDataKey_Failure(t *testing.T) {
	t.Parallel()
	p, err := NewLocalProvider(LocalConfig{
		MasterKey: testMasterKey(),
	})
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "audit.jsonl")
	archiver, err := NewAuditArchiver(AuditArchiverConfig{LocalPath: path})
	require.NoError(t, err)

	audited := WithAudit(p, archiver, "local")

	// Bad ciphertext should fail rotation
	_, err = audited.RotateDataKey(context.Background(), []byte("bad"), RotateOpts{Label: "fail"})
	require.Error(t, err)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"event_type":"KEY_ROTATED"`)
	assert.Contains(t, string(raw), `"status":"FAILURE"`)
}

func TestAuditedProvider_SignAuditEvent(t *testing.T) {
	t.Parallel()
	p, err := NewLocalProvider(LocalConfig{
		MasterKey: testMasterKey(),
	})
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "audit.jsonl")
	archiver, err := NewAuditArchiver(AuditArchiverConfig{LocalPath: path})
	require.NoError(t, err)

	audited := WithAudit(p, archiver, "local")

	err = audited.SignAuditEvent(context.Background(), AuditEvent{
		EventType: "CUSTOM_EVENT",
		Status:    "SUCCESS",
	})
	require.NoError(t, err)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"event_type":"CUSTOM_EVENT"`)
}

func TestAuditedProvider_Close(t *testing.T) {
	t.Parallel()
	p, err := NewLocalProvider(LocalConfig{
		MasterKey: testMasterKey(),
	})
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "audit.jsonl")
	archiver, err := NewAuditArchiver(AuditArchiverConfig{LocalPath: path})
	require.NoError(t, err)

	audited := WithAudit(p, archiver, "local")
	err = audited.Close(context.Background())
	require.NoError(t, err)

	// Inner provider should be closed
	_, err = p.GenerateDataKey(context.Background(), KeyGenOpts{})
	assert.Equal(t, ErrClosed, err)
}

func TestAuditedProvider_GenerateDataKey_Failure(t *testing.T) {
	t.Parallel()
	p, err := NewLocalProvider(LocalConfig{
		MasterKey: testMasterKey(),
	})
	require.NoError(t, err)
	// Close provider so generate fails
	require.NoError(t, p.Close(context.Background()))

	path := filepath.Join(t.TempDir(), "audit.jsonl")
	archiver, err := NewAuditArchiver(AuditArchiverConfig{LocalPath: path})
	require.NoError(t, err)

	audited := WithAudit(p, archiver, "local")

	_, err = audited.GenerateDataKey(context.Background(), KeyGenOpts{Label: "fail-gen"})
	require.Error(t, err)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"event_type":"KEY_GENERATED"`)
	assert.Contains(t, string(raw), `"status":"FAILURE"`)
	assert.Contains(t, string(raw), `"error_code":"GENERATE_DATA_KEY_FAILED"`)
}

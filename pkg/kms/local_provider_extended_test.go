package kms

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalProvider_DefaultKeyURI(t *testing.T) {
	t.Parallel()
	p, err := NewLocalProvider(LocalConfig{
		MasterKey: testMasterKey(),
		// No KeyURI — should default
	})
	require.NoError(t, err)
	defer CloseProvider(p)

	meta, err := p.GetKeyMetadata(context.Background(), "")
	require.NoError(t, err)
	assert.Equal(t, "kms://local/default", meta.KeyURI)
}

func TestLocalProvider_GetKeyMetadata(t *testing.T) {
	t.Parallel()
	p, err := NewLocalProvider(LocalConfig{
		MasterKey: testMasterKey(),
		KeyURI:    "kms://local/meta-test",
	})
	require.NoError(t, err)
	defer CloseProvider(p)

	// Empty keyURI returns provider's own metadata
	meta, err := p.GetKeyMetadata(context.Background(), "")
	require.NoError(t, err)
	assert.Equal(t, "kms://local/meta-test", meta.KeyURI)
	assert.Equal(t, uint32(1), meta.Version)
	assert.Equal(t, "AES-256-GCM", meta.Algorithm)
	assert.Equal(t, "local", meta.Provider)
	assert.Equal(t, "software-module", meta.FIPSLevel)

	// Matching keyURI succeeds
	meta, err = p.GetKeyMetadata(context.Background(), "kms://local/meta-test")
	require.NoError(t, err)
	assert.Equal(t, "kms://local/meta-test", meta.KeyURI)

	// Non-matching keyURI returns ErrKeyNotFound
	_, err = p.GetKeyMetadata(context.Background(), "kms://local/other")
	assert.True(t, errors.Is(err, ErrKeyNotFound))
}

func TestLocalProvider_GetKeyMetadata_VersionIncrementsOnRotate(t *testing.T) {
	t.Parallel()
	p, err := NewLocalProvider(LocalConfig{
		MasterKey: testMasterKey(),
		KeyURI:    "kms://local/version-test",
	})
	require.NoError(t, err)
	defer CloseProvider(p)

	dek, err := p.GenerateDataKey(context.Background(), KeyGenOpts{})
	require.NoError(t, err)

	_, err = p.RotateDataKey(context.Background(), dek.Ciphertext, RotateOpts{KeyURI: dek.KeyURI})
	require.NoError(t, err)

	meta, err := p.GetKeyMetadata(context.Background(), "")
	require.NoError(t, err)
	assert.Equal(t, uint32(2), meta.Version)
}

func TestLocalProvider_Close_ZerosMasterKey(t *testing.T) {
	t.Parallel()
	p, err := NewLocalProvider(LocalConfig{
		MasterKey: testMasterKey(),
	})
	require.NoError(t, err)

	// Close should zero the master key
	err = p.Close(context.Background())
	require.NoError(t, err)
	for _, b := range p.master {
		assert.Equal(t, byte(0), b, "master key should be zeroed after close")
	}
	assert.True(t, p.closed)

	// Double close is safe
	err = p.Close(context.Background())
	assert.NoError(t, err)
}

func TestLocalProvider_ClosedState(t *testing.T) {
	t.Parallel()
	p, err := NewLocalProvider(LocalConfig{
		MasterKey: testMasterKey(),
		KeyURI:    "kms://local/close-test",
	})
	require.NoError(t, err)

	// Generate a key before closing
	dek, err := p.GenerateDataKey(context.Background(), KeyGenOpts{TTL: time.Hour})
	require.NoError(t, err)

	require.NoError(t, p.Close(context.Background()))

	// All operations should return ErrClosed
	_, err = p.GenerateDataKey(context.Background(), KeyGenOpts{})
	assert.Equal(t, ErrClosed, err)

	_, err = p.DecryptDataKey(context.Background(), dek.Ciphertext, DecryptOpts{})
	assert.Equal(t, ErrClosed, err)

	_, err = p.RotateDataKey(context.Background(), dek.Ciphertext, RotateOpts{})
	assert.Equal(t, ErrClosed, err)

	_, err = p.GetKeyMetadata(context.Background(), "")
	assert.Equal(t, ErrClosed, err)
}

func TestLocalProvider_GenerateDataKey_NoTTL(t *testing.T) {
	t.Parallel()
	p, err := NewLocalProvider(LocalConfig{
		MasterKey: testMasterKey(),
	})
	require.NoError(t, err)
	defer CloseProvider(p)

	dek, err := p.GenerateDataKey(context.Background(), KeyGenOpts{
		// No TTL
	})
	require.NoError(t, err)
	assert.True(t, dek.ExpiresAt.IsZero(), "ExpiresAt should be zero when no TTL")
	assert.Equal(t, "AES-256-GCM", dek.Algorithm)
}

func TestLocalProvider_GenerateDataKey_CustomAlgorithm(t *testing.T) {
	t.Parallel()
	p, err := NewLocalProvider(LocalConfig{
		MasterKey: testMasterKey(),
	})
	require.NoError(t, err)
	defer CloseProvider(p)

	dek, err := p.GenerateDataKey(context.Background(), KeyGenOpts{
		Algorithm: "ChaCha20-Poly1305",
	})
	require.NoError(t, err)
	assert.Equal(t, "ChaCha20-Poly1305", dek.Algorithm)
}

func TestLocalProvider_DecryptDataKey_InvalidCiphertext(t *testing.T) {
	t.Parallel()
	p, err := NewLocalProvider(LocalConfig{
		MasterKey: testMasterKey(),
	})
	require.NoError(t, err)
	defer CloseProvider(p)

	// Too short
	_, err = p.DecryptDataKey(context.Background(), []byte("short"), DecryptOpts{})
	assert.True(t, errors.Is(err, ErrDecryptFailed))

	// Wrong version byte
	badVersion := make([]byte, 50)
	badVersion[0] = 99 // not headerVersionV1
	_, err = p.DecryptDataKey(context.Background(), badVersion, DecryptOpts{})
	assert.True(t, errors.Is(err, ErrDecryptFailed))
}

func TestLocalProvider_RotateDataKey_WithTTL(t *testing.T) {
	t.Parallel()
	p, err := NewLocalProvider(LocalConfig{
		MasterKey: testMasterKey(),
	})
	require.NoError(t, err)
	defer CloseProvider(p)

	dek, err := p.GenerateDataKey(context.Background(), KeyGenOpts{})
	require.NoError(t, err)

	// Rotate with TTL
	rot, err := p.RotateDataKey(context.Background(), dek.Ciphertext, RotateOpts{
		TTL: 24 * time.Hour,
	})
	require.NoError(t, err)
	assert.False(t, rot.ExpiresAt.IsZero(), "ExpiresAt should be set with TTL")
	assert.Equal(t, uint32(2), rot.Version)

	// Rotate without TTL
	rot2, err := p.RotateDataKey(context.Background(), rot.Ciphertext, RotateOpts{})
	require.NoError(t, err)
	assert.True(t, rot2.ExpiresAt.IsZero(), "ExpiresAt should be zero without TTL")
	assert.Equal(t, uint32(3), rot2.Version)
}

func TestLocalProvider_SignAuditEvent_Noop(t *testing.T) {
	t.Parallel()
	p, err := NewLocalProvider(LocalConfig{
		MasterKey: testMasterKey(),
	})
	require.NoError(t, err)
	defer CloseProvider(p)

	err = p.SignAuditEvent(context.Background(), AuditEvent{EventType: "test"})
	assert.NoError(t, err)
}

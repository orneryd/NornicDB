package kms

import (
	"context"
	"errors"
	"testing"
	"time"

	wrapping "github.com/hashicorp/go-kms-wrapping/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestWrappingAdapter(t *testing.T) *wrappingAdapter {
	t.Helper()
	tw := wrapping.NewTestEnvelopeWrapper([]byte("0123456789abcdef0123456789abcdef"))
	tw.SetKeyId("kms://test/wrapper-ext")
	return newWrappingAdapter(tw)
}

func TestWrappingAdapter_GetKeyMetadata(t *testing.T) {
	t.Parallel()
	a := newTestWrappingAdapter(t)

	// Empty keyURI returns metadata
	meta, err := a.GetKeyMetadata(context.Background(), "")
	require.NoError(t, err)
	assert.Equal(t, "kms://test/wrapper-ext", meta.KeyURI)
	assert.Equal(t, "AES-256-GCM", meta.Algorithm)

	// Matching keyURI
	meta, err = a.GetKeyMetadata(context.Background(), "kms://test/wrapper-ext")
	require.NoError(t, err)
	assert.Equal(t, "kms://test/wrapper-ext", meta.KeyURI)

	// Non-matching keyURI returns ErrKeyNotFound
	_, err = a.GetKeyMetadata(context.Background(), "kms://test/wrong-key")
	assert.True(t, errors.Is(err, ErrKeyNotFound))
}

func TestWrappingAdapter_Close(t *testing.T) {
	t.Parallel()
	a := newTestWrappingAdapter(t)
	err := a.Close(context.Background())
	assert.NoError(t, err)
}

func TestWrappingAdapter_SignAuditEvent(t *testing.T) {
	t.Parallel()
	a := newTestWrappingAdapter(t)
	err := a.SignAuditEvent(context.Background(), AuditEvent{EventType: "TEST"})
	assert.NoError(t, err)
}

func TestWrappingAdapter_DecryptDataKey_InvalidBlob(t *testing.T) {
	t.Parallel()
	a := newTestWrappingAdapter(t)

	// Invalid JSON
	_, err := a.DecryptDataKey(context.Background(), []byte("not-json"), DecryptOpts{})
	require.Error(t, err)

	// Valid JSON but nil blob
	_, err = a.DecryptDataKey(context.Background(), []byte(`{"blob":null}`), DecryptOpts{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDecryptFailed))

	// Empty JSON object
	_, err = a.DecryptDataKey(context.Background(), []byte(`{}`), DecryptOpts{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDecryptFailed))
}

func TestWrappingAdapter_GenerateDataKey_DefaultAlgorithm(t *testing.T) {
	t.Parallel()
	a := newTestWrappingAdapter(t)

	dek, err := a.GenerateDataKey(context.Background(), KeyGenOpts{
		// No Algorithm or TTL
	})
	require.NoError(t, err)
	assert.Equal(t, "AES-256-GCM", dek.Algorithm)
	assert.True(t, dek.ExpiresAt.IsZero())
}

func TestWrappingAdapter_GenerateDataKey_WithTTL(t *testing.T) {
	t.Parallel()
	a := newTestWrappingAdapter(t)

	dek, err := a.GenerateDataKey(context.Background(), KeyGenOpts{
		TTL: 2 * time.Hour,
	})
	require.NoError(t, err)
	assert.False(t, dek.ExpiresAt.IsZero())
	assert.True(t, dek.ExpiresAt.After(dek.CreatedAt))
}

func TestWrappingAdapter_RotateDataKey_WithTTL(t *testing.T) {
	t.Parallel()
	a := newTestWrappingAdapter(t)

	dek, err := a.GenerateDataKey(context.Background(), KeyGenOpts{})
	require.NoError(t, err)

	rot, err := a.RotateDataKey(context.Background(), dek.Ciphertext, RotateOpts{
		TTL: time.Hour,
	})
	require.NoError(t, err)
	assert.False(t, rot.ExpiresAt.IsZero())
	assert.Equal(t, "AES-256-GCM", rot.Algorithm)

	// Without TTL
	rot2, err := a.RotateDataKey(context.Background(), rot.Ciphertext, RotateOpts{})
	require.NoError(t, err)
	assert.True(t, rot2.ExpiresAt.IsZero())
}

func TestWrappingAdapter_RotateDataKey_InvalidCiphertext(t *testing.T) {
	t.Parallel()
	a := newTestWrappingAdapter(t)

	_, err := a.RotateDataKey(context.Background(), []byte("bad-ct"), RotateOpts{})
	require.Error(t, err)
}

func TestDecodeBlob_EdgeCases(t *testing.T) {
	t.Parallel()

	// Empty input
	_, err := decodeBlob([]byte{})
	require.Error(t, err)

	// Null blob
	_, err = decodeBlob([]byte(`{"blob":null}`))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDecryptFailed))
}

func TestEncodeDecodeBlob_RoundTrip(t *testing.T) {
	t.Parallel()
	blob := &wrapping.BlobInfo{
		Ciphertext: []byte("encrypted-data"),
		Iv:         []byte("nonce-12-byte"),
		KeyInfo:    &wrapping.KeyInfo{KeyId: "test-key"},
	}

	encoded, err := encodeBlob(blob)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	decoded, err := decodeBlob(encoded)
	require.NoError(t, err)
	assert.Equal(t, blob.Ciphertext, decoded.Ciphertext)
	assert.Equal(t, blob.Iv, decoded.Iv)
	assert.Equal(t, "test-key", decoded.KeyInfo.KeyId)
}

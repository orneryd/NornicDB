package encryption

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/kms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnvelopeEncryptor_RoundTripExtended(t *testing.T) {
	t.Parallel()
	provider := testLocalProvider(t)
	enc := NewEnvelopeEncryptor(provider, EnvelopeConfig{
		DEKCacheTTL: time.Hour,
		Label:       "test-envelope",
	})

	plaintext := []byte("sensitive data that needs envelope encryption")
	ct, err := enc.Encrypt(context.Background(), plaintext)
	require.NoError(t, err)
	require.NotEmpty(t, ct)

	// Verify it's a JSON payload
	var payload envelopePayload
	require.NoError(t, json.Unmarshal(ct, &payload))
	assert.Equal(t, 1, payload.Version)
	assert.NotEmpty(t, payload.EncryptedDEK)
	assert.NotEmpty(t, payload.Nonce)
	assert.NotEmpty(t, payload.Ciphertext)

	// Decrypt
	decrypted, err := enc.Decrypt(context.Background(), ct)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decrypted)
}

func TestEnvelopeEncryptor_DecryptInvalidJSON(t *testing.T) {
	t.Parallel()
	provider := testLocalProvider(t)
	enc := NewEnvelopeEncryptor(provider, EnvelopeConfig{})

	_, err := enc.Decrypt(context.Background(), []byte("not-json"))
	require.Error(t, err)
}

func TestEnvelopeEncryptor_DecryptUnsupportedVersion(t *testing.T) {
	t.Parallel()
	provider := testLocalProvider(t)
	enc := NewEnvelopeEncryptor(provider, EnvelopeConfig{})

	badPayload, _ := json.Marshal(envelopePayload{Version: 99})
	_, err := enc.Decrypt(context.Background(), badPayload)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported envelope version")
}

func TestEnvelopeEncryptor_DecryptBadDEK(t *testing.T) {
	t.Parallel()
	provider := testLocalProvider(t)
	enc := NewEnvelopeEncryptor(provider, EnvelopeConfig{})

	badPayload, _ := json.Marshal(envelopePayload{
		Version:      1,
		EncryptedDEK: []byte("bad-encrypted-dek"),
		Nonce:        make([]byte, 12),
		Ciphertext:   []byte("ciphertext"),
	})
	_, err := enc.Decrypt(context.Background(), badPayload)
	require.Error(t, err)
}

func TestEnvelopeEncryptor_DecryptBadNonce(t *testing.T) {
	t.Parallel()
	provider := testLocalProvider(t)
	enc := NewEnvelopeEncryptor(provider, EnvelopeConfig{
		DEKCacheTTL: time.Hour,
	})

	// Encrypt something valid first
	ct, err := enc.Encrypt(context.Background(), []byte("test"))
	require.NoError(t, err)

	// Tamper with nonce
	var payload envelopePayload
	require.NoError(t, json.Unmarshal(ct, &payload))
	payload.Nonce = []byte("wrong-size")
	tampered, _ := json.Marshal(payload)

	_, err = enc.Decrypt(context.Background(), tampered)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid nonce size")
}

func TestEnvelopeEncryptor_MultipleEncrypts_UseCache(t *testing.T) {
	t.Parallel()
	provider := testLocalProvider(t)
	enc := NewEnvelopeEncryptor(provider, EnvelopeConfig{
		DEKCacheTTL: time.Hour,
		Label:       "cache-test",
	})

	// Multiple encrypts should use the same cached DEK
	ct1, err := enc.Encrypt(context.Background(), []byte("message 1"))
	require.NoError(t, err)
	ct2, err := enc.Encrypt(context.Background(), []byte("message 2"))
	require.NoError(t, err)

	// Both should decrypt correctly
	d1, err := enc.Decrypt(context.Background(), ct1)
	require.NoError(t, err)
	assert.Equal(t, []byte("message 1"), d1)

	d2, err := enc.Decrypt(context.Background(), ct2)
	require.NoError(t, err)
	assert.Equal(t, []byte("message 2"), d2)

	// Same DEK URI should be used (from cache)
	var p1, p2 envelopePayload
	json.Unmarshal(ct1, &p1)
	json.Unmarshal(ct2, &p2)
	assert.Equal(t, p1.KeyURI, p2.KeyURI)
	assert.Equal(t, p1.KeyVersion, p2.KeyVersion)
}

func TestDEKCache_DefaultMaxAge(t *testing.T) {
	t.Parallel()
	cache := newDEKCache(testLocalProvider(t), 0, "test")
	assert.Equal(t, 24*time.Hour, cache.maxAge)
}

func TestDEKCache_GetOrGenerate_CachesKey(t *testing.T) {
	t.Parallel()
	cache := newDEKCache(testLocalProvider(t), time.Hour, "test")

	k1, err := cache.GetOrGenerate(context.Background())
	require.NoError(t, err)
	require.NotNil(t, k1)

	k2, err := cache.GetOrGenerate(context.Background())
	require.NoError(t, err)
	// Should return same cached key
	assert.Equal(t, k1.KeyURI, k2.KeyURI)
	assert.Equal(t, k1.CreatedAt, k2.CreatedAt)
}

func TestDEKCache_GetOrGenerate_ProviderError(t *testing.T) {
	t.Parallel()
	p, err := kms.NewLocalProvider(kms.LocalConfig{
		MasterKey: []byte("0123456789abcdef0123456789abcdef"),
	})
	require.NoError(t, err)
	require.NoError(t, p.Close(context.Background()))

	cache := newDEKCache(p, time.Hour, "test")
	_, err = cache.GetOrGenerate(context.Background())
	require.Error(t, err)
}

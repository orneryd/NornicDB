package multidb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoteCredentialCipher_RoundTrip(t *testing.T) {
	cipher, err := newRemoteCredentialCipher("test-secret-key")
	require.NoError(t, err)

	encrypted, err := cipher.encrypt("my-password-123")
	require.NoError(t, err)
	assert.Contains(t, encrypted, remoteCredentialPrefix)

	decrypted, err := cipher.decrypt(encrypted)
	require.NoError(t, err)
	assert.Equal(t, "my-password-123", decrypted)
}

func TestRemoteCredentialCipher_DifferentNonces(t *testing.T) {
	cipher, err := newRemoteCredentialCipher("key")
	require.NoError(t, err)

	enc1, err := cipher.encrypt("same-text")
	require.NoError(t, err)
	enc2, err := cipher.encrypt("same-text")
	require.NoError(t, err)

	// Each encryption must use a unique nonce → different ciphertexts
	assert.NotEqual(t, enc1, enc2)

	// Both must decrypt to the same value
	dec1, _ := cipher.decrypt(enc1)
	dec2, _ := cipher.decrypt(enc2)
	assert.Equal(t, dec1, dec2)
}

func TestRemoteCredentialCipher_DecryptWrongKey(t *testing.T) {
	cipher1, _ := newRemoteCredentialCipher("key-one")
	cipher2, _ := newRemoteCredentialCipher("key-two")

	encrypted, err := cipher1.encrypt("secret")
	require.NoError(t, err)

	_, err = cipher2.decrypt(encrypted)
	assert.Error(t, err, "decrypting with wrong key must fail")
}

func TestRemoteCredentialCipher_DecryptMalformedInput(t *testing.T) {
	cipher, _ := newRemoteCredentialCipher("key")

	_, err := cipher.decrypt("not-encrypted-at-all")
	assert.Error(t, err, "non-prefixed input must fail")
	requireLocalizedMessageID(t, err, localization.MessageMultidbCredentialFormatInvalid)

	_, err = cipher.decrypt(remoteCredentialPrefix + "!!!invalid-base64!!!")
	assert.Error(t, err, "invalid base64 must fail")

	_, err = cipher.decrypt(remoteCredentialPrefix + "dG9v") // valid b64 but too short for nonce
	assert.Error(t, err, "truncated payload must fail")
	requireLocalizedMessageID(t, err, localization.MessageMultidbCredentialPayloadTruncated)
}

func TestIsEncryptedRemoteCredential(t *testing.T) {
	assert.True(t, isEncryptedRemoteCredential("enc:v1:abc123"))
	assert.True(t, isEncryptedRemoteCredential("  enc:v1:abc123"))
	assert.False(t, isEncryptedRemoteCredential("plaintext-password"))
	assert.False(t, isEncryptedRemoteCredential(""))
}

func TestIsCredentialFreeSimpleHTTPURL(t *testing.T) {
	tests := []struct {
		name string
		uri  string
		want bool
	}{
		{name: "plain host", uri: "http://example.test", want: true},
		{name: "escaped path", uri: "https://example.test/path%20item", want: true},
		{name: "userinfo", uri: "http://synthetic@example.test"},
		{name: "query", uri: "http://example.test/?user=synthetic"},
		{name: "fragment", uri: "http://example.test/#synthetic"},
		{name: "bad escape", uri: "http://example.test/%GG"},
		{name: "bad port", uri: "http://example.test:port"},
		{name: "noncanonical scheme", uri: "HTTP://example.test"},
		{name: "space", uri: "http://example.test/path with space"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isCredentialFreeSimpleHTTPURL(tt.uri))
		})
	}
}

func TestCredentialFreeAliasStorageLookupDoesNotAllocate(t *testing.T) {
	remote := storage.NewMemoryEngine()
	defer remote.Close()
	ref := ConstituentRef{Alias: "remote", DatabaseName: "remote", Type: "remote", URI: "http://example.test"}
	manager := &DatabaseManager{databases: map[string]*DatabaseInfo{
		"probe": &DatabaseInfo{Type: "composite", Constituents: []ConstituentRef{ref}},
	}, remoteEngineFactory: func(ConstituentRef, string) (storage.Engine, error) { return remote, nil }}
	manager.primeRuntimeCredentialFastPaths()
	resolved, _, credentialFree, found := manager.resolveCompositeConstituentInternal("probe.remote")
	if !found || !credentialFree {
		t.Fatal("canonical credential-free endpoint was not classified for the fast path")
	}
	allocs := testing.AllocsPerRun(1000, func() {
		var err error
		_, err = manager.getRemoteStorageInternal(resolved, "", credentialFree)
		if err != nil {
			t.Fatal("getting storage for a credential-free alias failed")
		}
	})
	assert.Zero(t, allocs)
}

func TestRuntimeCredentialFreeFastPathRequiresCredentialFreeMetadata(t *testing.T) {
	tests := []struct {
		ref  ConstituentRef
		want bool
	}{
		{ref: ConstituentRef{Type: "remote", URI: "http://example.test"}, want: true},
		{ref: ConstituentRef{Type: "remote", URI: "http://synthetic-user:synthetic-pass@example.test"}},
		{ref: ConstituentRef{Type: "remote", URI: "http://example.test/?token=synthetic-token"}},
		{ref: ConstituentRef{Type: "remote", URI: "http://example.test", User: "synthetic-user"}},
		{ref: ConstituentRef{Type: "remote", URI: "http://example.test", Password: "synthetic-pass"}},
		{ref: ConstituentRef{Type: "remote", URI: "http://example.test", SecretRef: "synthetic-ref"}},
		{ref: ConstituentRef{Type: "remote", URI: "http://example.test", AuthMode: "user_password"}},
		{ref: ConstituentRef{Type: "local", URI: "http://example.test"}},
	}
	for _, tt := range tests {
		if got := runtimeCredentialFreeRemoteRef(tt.ref); got != tt.want {
			t.Fatal("credential fast-path classification did not match the expected security boundary")
		}
	}
}

func TestDatabaseManager_EncryptRemotePassword(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, &Config{
		RemoteCredentialEncryptionKey: "integration-test-key",
	})
	require.NoError(t, err)
	defer manager.Close()

	encrypted, err := manager.encryptRemotePassword("remote-pass")
	require.NoError(t, err)
	assert.True(t, isEncryptedRemoteCredential(encrypted))
}

func TestDatabaseManager_EncryptRemotePassword_EmptyReject(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, &Config{
		RemoteCredentialEncryptionKey: "key",
	})
	require.NoError(t, err)
	defer manager.Close()

	_, err = manager.encryptRemotePassword("")
	assert.Error(t, err, "empty password must be rejected")
	requireLocalizedMessageID(t, err, localization.MessageMultidbRemotePasswordEmpty)

	_, err = manager.encryptRemotePassword("   ")
	assert.Error(t, err, "whitespace-only password must be rejected")
}

func TestDatabaseManager_EncryptRemotePassword_NoCipherConfigured(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	_, err = manager.encryptRemotePassword("pass")
	assert.Error(t, err, "must fail when no encryption key is configured")
	requireLocalizedMessageID(t, err, localization.MessageMultidbCredentialKeyConfigRequired)
}

func TestDatabaseManager_DecryptStoredRemotePassword(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, &Config{
		RemoteCredentialEncryptionKey: "test-key",
	})
	require.NoError(t, err)
	defer manager.Close()

	// Encrypt then decrypt
	encrypted, err := manager.encryptRemotePassword("my-secret")
	require.NoError(t, err)

	decrypted, err := manager.decryptStoredRemotePassword(encrypted)
	require.NoError(t, err)
	assert.Equal(t, "my-secret", decrypted)
}

func TestDatabaseManager_DecryptStoredRemotePassword_PlaintextFallback(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, &Config{
		RemoteCredentialEncryptionKey: "key",
	})
	require.NoError(t, err)
	defer manager.Close()

	// Legacy plaintext password (no enc:v1: prefix) should pass through
	decrypted, err := manager.decryptStoredRemotePassword("legacy-plaintext")
	require.NoError(t, err)
	assert.Equal(t, "legacy-plaintext", decrypted)
}

func TestDatabaseManager_DecryptStoredRemotePassword_EmptyReject(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	_, err = manager.decryptStoredRemotePassword("")
	assert.Error(t, err)
	requireLocalizedMessageID(t, err, localization.MessageMultidbStoredPasswordMissing)

	_, err = manager.decryptStoredRemotePassword("   ")
	assert.Error(t, err)
}

func TestDatabaseManager_DecryptStoredRemotePassword_NoCipherConfigured(t *testing.T) {
	inner := storage.NewMemoryEngine()
	defer inner.Close()

	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	// Encrypted value but no cipher configured
	_, err = manager.decryptStoredRemotePassword("enc:v1:someciphertext")
	assert.Error(t, err, "must fail when cipher is not configured")
	requireLocalizedMessageID(t, err, localization.MessageMultidbCredentialDecryptKeyRequired)
}

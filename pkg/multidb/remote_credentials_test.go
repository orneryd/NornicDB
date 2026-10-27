package multidb

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

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

func TestPrecomputedAliasStorageLookupDoesNotAllocate(t *testing.T) {
	remote := runtimeFixtureOwner.Own(t, runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	defer remote.Close()
	manager := &DatabaseManager{
		databases: map[string]*DatabaseInfo{
			"probe": &DatabaseInfo{Type: "composite", Constituents: []ConstituentRef{{Alias: "remote", DatabaseName: "remote", Type: "remote", URI: "http://example.test", AuthMode: "user_password", User: "fixture-user", Password: "fixture-password"}}},
		},
		remoteEngineFactory: func(ConstituentRef, string) (storage.Engine, error) { return remote, nil },
	}
	manager.rebuildRuntimeConstituents()
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := manager.GetStorage("probe.remote"); err != nil {
			t.Fatal("precomputed remote alias lookup failed")
		}
	})
	assert.Zero(t, allocs)
}

func TestRuntimeConstituentIndexUsesCanonicalWinnerForRegisteredSpellings(t *testing.T) {
	manager := &DatabaseManager{databases: map[string]*DatabaseInfo{
		"probe": &DatabaseInfo{Type: "composite", Constituents: []ConstituentRef{
			{Alias: "R", DatabaseName: "first", Type: "remote", URI: "http://first.example.test"},
			{Alias: "r", DatabaseName: "last", Type: "remote", URI: "http://last.example.test"},
		}},
	}}
	manager.rebuildRuntimeConstituents()
	indexed := manager.databases["probe"].runtimeConstituentsByAlias
	upper, lower := indexed["R"], indexed["r"]
	if upper == nil || lower == nil {
		t.Fatal("registered alias spellings were not indexed")
	}
	assert.Same(t, upper, lower, "case-folded duplicate spellings must share the canonical entry")
	assert.Equal(t, "last", upper.ref.DatabaseName)
	assert.Equal(t, "http://last.example.test", upper.storedRef.URI)
}

func TestRuntimeConstituentCacheInvalidatesCredentialAndAliasChanges(t *testing.T) {
	ref := ConstituentRef{Alias: "remote", DatabaseName: "remote", Type: "remote", URI: "http://example.test", AuthMode: "user_password", User: "fixture-user", Password: "fixture-password"}
	info := &DatabaseInfo{Type: "composite", Constituents: []ConstituentRef{ref}}
	manager := &DatabaseManager{databases: map[string]*DatabaseInfo{"probe": info}}
	manager.rebuildRuntimeConstituents()
	first := info.runtimeConstituentsByAlias["remote"]
	if first == nil {
		t.Fatal("registered remote constituent was not cached")
	}

	ref.Password = "fixture-password-updated"
	info.Constituents[0] = ref
	manager.rebuildRuntimeConstituents()
	second := info.runtimeConstituentsByAlias["remote"]
	if second == nil || first == second {
		t.Fatal("credential changes did not replace the cached runtime identity")
	}

	ref.Alias = "renamed"
	info.Constituents[0] = ref
	manager.rebuildRuntimeConstituents()
	third := info.runtimeConstituentsByAlias["renamed"]
	if _, exists := info.runtimeConstituentsByAlias["remote"]; exists {
		t.Fatal("old alias remained in the runtime index")
	}
	if third == nil || second == third {
		t.Fatal("alias change did not replace the cached runtime identity")
	}
}

func TestDatabaseManager_EncryptRemoteCredential_EmptyReject(t *testing.T) {
	inner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	manager, err := NewDatabaseManager(t.Context(), inner, &Config{
		RemoteCredentialEncryptionKey: "key",
	})
	require.NoError(t, err)
	defer manager.Close()

	_, err = manager.encryptRemoteCredential(remoteCredentialRequireValue, "")
	assert.Error(t, err, "empty password must be rejected")
	requireLocalizedMessageID(t, err, localization.MessageMultidbRemotePasswordEmpty)

	_, err = manager.encryptRemoteCredential(remoteCredentialRequireValue, "   ")
	assert.Error(t, err, "whitespace-only password must be rejected")
	requireLocalizedMessageID(t, err, localization.MessageMultidbRemotePasswordEmpty)
}

func TestDatabaseManager_EncryptRemoteCredential_AllowEmptyContrast(t *testing.T) {
	inner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	manager, err := NewDatabaseManager(t.Context(), inner, &Config{
		RemoteCredentialEncryptionKey: "key",
	})
	require.NoError(t, err)
	defer manager.Close()

	// URL-origin credentials may be blank and must round-trip byte-exactly.
	for _, value := range []string{"", "   "} {
		encrypted, err := manager.encryptRemoteCredential(remoteCredentialAllowEmpty, value)
		require.NoError(t, err, "allow-empty policy must accept a blank credential")
		assert.True(t, isEncryptedRemoteCredential(encrypted))

		decrypted, err := manager.decryptStoredRemotePassword(encrypted)
		require.NoError(t, err)
		if value == "" {
			// A blank stored value has no ciphertext to recover.
			assert.Empty(t, decrypted)
			continue
		}
		assert.Equal(t, value, decrypted)
	}
}

func TestDatabaseManager_RemoteCredentialNoCipherConfigured(t *testing.T) {
	inner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	tests := []struct {
		name       string
		operation  func() (string, error)
		expectedID localization.MessageID
	}{
		{
			name:       "encrypt-required",
			operation:  func() (string, error) { return manager.encryptRemoteCredential(remoteCredentialRequireValue, "pass") },
			expectedID: localization.MessageMultidbCredentialKeyConfigRequired,
		},
		{
			name:       "encrypt-allow-empty",
			operation:  func() (string, error) { return manager.encryptRemoteCredential(remoteCredentialAllowEmpty, "pass") },
			expectedID: localization.MessageMultidbCredentialKeyConfigRequired,
		},
		{
			name:       "decrypt",
			operation:  func() (string, error) { return manager.decryptStoredRemotePassword("enc:v1:someciphertext") },
			expectedID: localization.MessageMultidbCredentialDecryptKeyRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.operation()
			assert.Error(t, err)
			requireLocalizedMessageID(t, err, tt.expectedID)
		})
	}
}

func TestDatabaseManager_DecryptStoredRemotePassword_EmptyReject(t *testing.T) {
	inner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()

	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	defer manager.Close()

	_, err = manager.decryptStoredRemotePassword("")
	assert.Error(t, err)
	requireLocalizedMessageID(t, err, localization.MessageMultidbStoredPasswordMissing)

	_, err = manager.decryptStoredRemotePassword("   ")
	assert.Error(t, err)
}

func TestDatabaseManager_RemoteCredentialRecovery(t *testing.T) {
	inner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer inner.Close()
	manager, err := NewDatabaseManager(t.Context(), inner, &Config{RemoteCredentialEncryptionKey: "test-key"})
	require.NoError(t, err)
	defer manager.Close()
	encrypted, err := manager.encryptRemoteCredential(remoteCredentialRequireValue, "my-secret")
	require.NoError(t, err)
	require.True(t, isEncryptedRemoteCredential(encrypted))
	for _, tc := range []struct{ name, stored, want string }{
		{"ciphertext", encrypted, "my-secret"},
		{"legacy-plaintext", "legacy-plaintext", "legacy-plaintext"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := manager.decryptStoredRemotePassword(tc.stored)
			require.NoError(t, err)
			require.Equal(t, tc.want, actual)
		})
	}
}

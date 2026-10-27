package multidb

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTaggedRemoteConnectionRuntimePreservesLiteralURICipherPrefix(t *testing.T) {
	cipher, err := newRemoteCredentialCipher("synthetic-descriptor-key")
	require.NoError(t, err)
	// Independent golden V1 bytes: version, URI-fallback origin, length-framed
	// raw URI, then empty USER/PASSWORD/auth mode. URI text is never ciphertext.
	const golden = "\x01\x02\x34http://u:p@example.invalid/path?token=enc:v1:literal\x00\x00\x00"
	sealed, err := cipher.encrypt(golden)
	require.NoError(t, err)
	encoded, err := json.Marshal(map[string]any{"alias": "r", "database_name": "nornic", "type": "remote", "access_mode": "read", "connection_v1": sealed})
	require.NoError(t, err)
	var ref ConstituentRef
	require.NoError(t, json.Unmarshal(encoded, &ref))
	manager := &DatabaseManager{remoteCredentialCipher: cipher}
	runtime, err := manager.resolveRemoteCredentials(ref, "")
	require.NoError(t, err)
	require.Equal(t, "http://example.invalid/path?token=enc:v1:literal", runtime.URI)
	require.Equal(t, "u", runtime.User)
	require.Equal(t, "p", runtime.Password)
	forwarded, err := manager.resolveRemoteCredentials(ref, "Bearer synthetic-forwarded")
	require.NoError(t, err)
	require.Empty(t, forwarded.User)
	require.Empty(t, forwarded.Password)
	require.Equal(t, runtime.URI, forwarded.URI)
}

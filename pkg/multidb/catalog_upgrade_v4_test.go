package multidb

import (
	"context"
	"encoding/json"
	"github.com/orneryd/nornicdb/internal/jsontext"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCatalogV4TransformerPreservesLegacyMeaningAndUnknownFields(t *testing.T) {
	const key = "synthetic-v4-transform-key"
	cipher, err := newRemoteCredentialCipher(key)
	require.NoError(t, err)
	encryptedPassword, err := cipher.encrypt(" explicit-password ")
	require.NoError(t, err)
	catalog := map[string]any{" c ": map[string]any{
		"name": " c ", "type": "composite", "future_database": map[string]any{"n": json.Number("9007199254740993"), "s": `literal\u0061`},
		"constituents": []any{
			map[string]any{"alias": " r ", "database_name": "nornic", "type": "remote", "access_mode": "read", "uri": "http://u:p@example.invalid/path?token=enc:v1:literal#access_token=enc:v1:fragment", "future_constituent": []any{"preserve", json.Number("9007199254740993")}},
			map[string]any{"alias": "explicit", "database_name": "nornic", "type": "remote", "access_mode": "read", "uri": "http://ignored:ignored@example.invalid", "auth_mode": "user_password", "user": " enc:v1:literal-user ", "password": encryptedPassword},
			map[string]any{"alias": "ordinary", "database_name": "nornic", "type": "remote", "access_mode": "read", "uri": "http://example.invalid/plain"},
			map[string]any{"alias": "local", "database_name": "nornic", "type": "local", "access_mode": "read"},
		},
	}}
	original, err := json.Marshal(catalog)
	require.NoError(t, err)
	plan, err := NewCatalogUpgradeV4(" system ", key)
	require.NoError(t, err)
	require.Equal(t, " system :databases:metadata", string(plan.NodeID))
	encoded, changed, err := plan.Transform(t.Context(), string(original))
	require.NoError(t, err)
	require.True(t, changed)
	require.NotContains(t, encoded, "token=enc:v1:literal")
	require.NotContains(t, encoded, "ignored:ignored")
	var before, after map[string]map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(original, &before))
	require.NoError(t, json.Unmarshal([]byte(encoded), &after))
	require.JSONEq(t, string(before[" c "]["future_database"]), string(after[" c "]["future_database"]))
	var refs []ConstituentRef
	require.NoError(t, json.Unmarshal(after[" c "]["constituents"], &refs))
	require.Len(t, refs, 4)
	manager := &DatabaseManager{remoteCredentialCipher: cipher}
	runtime, err := manager.resolveRemoteCredentials(refs[0], "")
	require.NoError(t, err)
	require.Equal(t, "u", runtime.User)
	require.Equal(t, "p", runtime.Password)
	require.Equal(t, "http://example.invalid/path?token=enc:v1:literal#access_token=enc:v1:fragment", runtime.URI)
	explicit, err := manager.resolveRemoteCredentials(refs[1], "Bearer forwarded")
	require.NoError(t, err)
	require.Equal(t, " enc:v1:literal-user ", explicit.User)
	require.Equal(t, " explicit-password ", explicit.Password)
	require.Equal(t, "user_password", explicit.AuthMode)
	require.Equal(t, "http://example.invalid/plain", refs[2].URI)
	require.Empty(t, refs[2].ConnectionV1)
	require.Equal(t, "local", refs[3].Type)
	again, changed, err := plan.Transform(t.Context(), encoded)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, encoded, again, "repeat preserves ciphertext and original encoded catalog")
	wrong, err := NewCatalogUpgradeV4("system", "wrong-synthetic-key")
	require.NoError(t, err)
	rejected, changed, err := wrong.Transform(t.Context(), encoded)
	require.Error(t, err)
	require.False(t, changed)
	require.Empty(t, rejected)
	missing, err := NewCatalogUpgradeV4("system", "")
	require.NoError(t, err)
	rejected, changed, err = missing.Transform(t.Context(), string(original))
	require.Error(t, err)
	require.False(t, changed)
	require.Empty(t, rejected)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = plan.Transform(ctx, string(original))
	require.ErrorIs(t, err, context.Canceled)
}

func TestRemoteConnectionDescriptorPreservesExactInputAndRejectsConflicts(t *testing.T) {
	cipher, err := newRemoteCredentialCipher("synthetic-descriptor-test-key")
	require.NoError(t, err)
	manager := &DatabaseManager{remoteCredentialCipher: cipher}
	ref := ConstituentRef{Type: "remote", URI: "http://example.invalid/path?token=enc:v1:literal", User: " user\xed\xa0\x80 ", Password: " enc:v1:password\xed\xb0\x80 ", AuthMode: "user_password"}
	secured, err := manager.sealRemoteConnection(ref)
	require.NoError(t, err)
	require.Empty(t, secured.URI)
	require.Empty(t, secured.User)
	require.Empty(t, secured.Password)
	require.Empty(t, secured.AuthMode)
	decoded, err := manager.openRemoteConnection(secured)
	require.NoError(t, err)
	require.Equal(t, ref.URI, decoded.uri)
	require.Equal(t, ref.User, decoded.user)
	require.Equal(t, ref.Password, decoded.password)
	runtime := manager.resolveRuntimeConstituent(secured)
	require.True(t, runtime.explicitAuth, "forwarded tokens cannot override stored explicit identity")
	require.Equal(t, ref.Password, runtime.forwardedRef.Password)
	require.Empty(t, runtime.forwardedRef.ConnectionV1)
	conflict := secured
	conflict.URI = "http://other.invalid"
	_, err = manager.openRemoteConnection(conflict)
	require.Error(t, err)
	malformed := secured
	malformed.ConnectionV1 = "plaintext-not-ciphertext"
	_, err = manager.openRemoteConnection(malformed)
	require.Error(t, err)
	for _, payload := range []string{"", "\x02\x01", "\x01\x03", "\x01\x01\xff", "\x01\x01\x00\x00\x00\x00tail"} {
		bad := secured
		bad.ConnectionV1, err = cipher.encrypt(payload)
		require.NoError(t, err)
		_, err = manager.openRemoteConnection(bad)
		require.Error(t, err)
	}
	require.NotEqual(t, secured.ConnectionV1, strings.TrimSpace(ref.Password))
}

func TestDescriptorDisplayIsDerivedAndHasNoExecutionAuthority(t *testing.T) {
	cipher, err := newRemoteCredentialCipher("synthetic-descriptor-display-key")
	require.NoError(t, err)
	manager := &DatabaseManager{remoteCredentialCipher: cipher}
	for _, explicit := range []bool{false, true} {
		ref := ConstituentRef{Type: "remote", Alias: "r", URI: "http://uri-user:synthetic-uri-password@example.invalid/ordinary?token=synthetic-uri-token"}
		if explicit {
			ref.AuthMode = "user_password"
			ref.User = "enc:v1:literal-public-user"
			ref.Password = "synthetic-explicit-password"
		}
		stored, err := manager.sealRemoteConnection(ref)
		require.NoError(t, err)
		view, err := manager.RemoteConstituentForDisplay(stored)
		require.NoError(t, err)
		require.NotContains(t, view.URI, "synthetic-")
		require.Contains(t, view.URI, "example.invalid/ordinary")
		require.Empty(t, view.Password)
		require.Empty(t, view.ConnectionV1)
		if explicit {
			require.Equal(t, ref.User, view.User)
			require.Equal(t, ref.User, RemoteUserForDisplay(view.User, view.AuthMode))
		} else {
			require.NotContains(t, view.User, "uri-user")
		}
		require.Empty(t, stored.URI)
		require.NotEmpty(t, stored.ConnectionV1, "display must not alter stored authority")
	}
}

func TestCatalogV4TransformRetainsEveryAdmittedRegistration(t *testing.T) {
	const data = `{"c":{"name":"c","type":"composite","options":{"limit":9007199254740993},"constituents":[{"alias":"r","database_name":"nornic","type":"remote","access_mode":"read","uri":"http://u:first@example.invalid","extra":{"keep":"literal\\u0061"}},{"alias":"R","database_name":"nornic","type":"remote","access_mode":"read","uri":"http://u:second@example.invalid"},{"alias":" r ","database_name":"nornic","type":"remote","access_mode":"read","uri":"http://u:third@example.invalid"}]}}`
	plan, err := NewCatalogUpgradeV4("system", "synthetic-registration-transform-key")
	require.NoError(t, err)
	encoded, changed, err := plan.Transform(t.Context(), data)
	require.NoError(t, err)
	require.True(t, changed)
	var parsed map[string]struct {
		Options      json.RawMessage `json:"options"`
		Constituents []struct {
			Alias      string          `json:"alias"`
			Extra      json.RawMessage `json:"extra"`
			Connection string          `json:"connection_v1"`
		} `json:"constituents"`
	}
	require.NoError(t, json.Unmarshal([]byte(encoded), &parsed))
	require.Len(t, parsed["c"].Constituents, 3)
	require.Equal(t, []string{"r", "R", " r "}, []string{parsed["c"].Constituents[0].Alias, parsed["c"].Constituents[1].Alias, parsed["c"].Constituents[2].Alias})
	require.JSONEq(t, `{"limit":9007199254740993}`, string(parsed["c"].Options))
	require.JSONEq(t, `{"keep":"literal\\u0061"}`, string(parsed["c"].Constituents[0].Extra))
	for _, ref := range parsed["c"].Constituents {
		require.NotEmpty(t, ref.Connection)
	}
}

func TestDescriptorRuntimeDoesNotReinterpretLegacyOpaqueQueryBytes(t *testing.T) {
	cipher, err := newRemoteCredentialCipher("synthetic-opaque-uri-key")
	require.NoError(t, err)
	manager := &DatabaseManager{remoteCredentialCipher: cipher}
	ref := ConstituentRef{Type: "remote", URI: "http://u:p@example.invalid/path?token=%zz&bad%key=keep"}
	secured, err := manager.sealLegacyRemoteConnection(ref)
	require.NoError(t, err)
	resolved, err := manager.resolveRemoteCredentials(secured, "")
	require.NoError(t, err)
	require.Equal(t, "http://example.invalid/path?token=%zz&bad%key=keep", resolved.URI)
	require.Equal(t, "u", resolved.User)
	require.Equal(t, "p", resolved.Password)
}

func TestCatalogV4TransformerPreservesJSONCodeUnits(t *testing.T) {
	const key = "synthetic-v4-code-unit-key"
	const original = `{"c":{"name":"c","type":"composite","future":{"text":"\ud800","literal":"\\ud800","n":9007199254740993},"constituents":[{"alias":"r","database_name":"nornic","type":"remote","uri":"http://example.invalid","auth_mode":"user_password","user":"plain","password":"\ud800"}]}}`
	plan, err := NewCatalogUpgradeV4("system", key)
	require.NoError(t, err)
	encoded, changed, err := plan.Transform(t.Context(), original)
	require.NoError(t, err)
	require.True(t, changed)
	var catalog map[string]struct {
		Future       jsontext.RawMessage `json:"future"`
		Constituents []ConstituentRef    `json:"constituents"`
	}
	require.NoError(t, jsontext.Unmarshal([]byte(encoded), &catalog))
	require.Len(t, catalog["c"].Constituents, 1)
	cipher, err := newRemoteCredentialCipher(key)
	require.NoError(t, err)
	manager := &DatabaseManager{remoteCredentialCipher: cipher}
	connection, err := manager.openRemoteConnection(catalog["c"].Constituents[0])
	require.NoError(t, err)
	require.Equal(t, []byte{0xed, 0xa0, 0x80}, []byte(connection.password))
	var future map[string]jsontext.RawMessage
	require.NoError(t, jsontext.Unmarshal(catalog["c"].Future, &future))
	var text, literal string
	require.NoError(t, jsontext.Unmarshal(future["text"], &text))
	require.NoError(t, jsontext.Unmarshal(future["literal"], &literal))
	require.Equal(t, []byte{0xed, 0xa0, 0x80}, []byte(text))
	require.Equal(t, `\ud800`, literal)
	require.Equal(t, `9007199254740993`, string(future["n"]))
}

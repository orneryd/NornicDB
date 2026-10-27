package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestAuthJSONAdmittedIdentityReopenAndTokens(t *testing.T) {
	for _, tc := range []struct{ name, unit string }{
		{"high", string([]byte{0xed, 0xa0, 0x80})}, {"low", string([]byte{0xed, 0xb0, 0x80})}, {"scalar", "😀"}, {"literal", `\ud800`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultAuthConfig()
			cfg.BcryptCost = 4
			cfg.JWTSecret = []byte("synthetic-auth-json-stable-signing-key")
			dir := t.TempDir()
			engine, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
			require.NoError(t, err)
			t.Cleanup(func() {
				if engine != nil {
					_ = engine.Close()
				}
			})
			users, err := NewAuthenticator(cfg, engine)
			require.NoError(t, err)
			name := "user-" + tc.unit
			password := "synthetic-password-" + tc.unit
			email := "mail-" + tc.unit + "@example.invalid"
			roles := []Role{RoleViewer, Role("role-" + tc.unit)}
			created, err := users.CreateUser(name, password, roles)
			require.NoError(t, err)
			require.Equal(t, name, created.Username)
			metadata := map[string]string{"key-" + tc.unit: "value-" + tc.unit, "literal": `\ud800`}
			require.NoError(t, users.UpdateUser(name, email, metadata))
			require.NoError(t, engine.Close())
			engine, err = fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
			require.NoError(t, err)
			users, err = NewAuthenticator(cfg, engine)
			require.NoError(t, err)
			stored, err := users.GetUser(name)
			require.NoError(t, err)
			require.Equal(t, name, stored.Username)
			require.Equal(t, email, stored.Email)
			require.Equal(t, roles, stored.Roles)
			require.Equal(t, metadata, stored.Metadata)
			token, user, err := users.Authenticate(name, password, "127.0.0.1", "identity-test")
			require.NoError(t, err)
			require.Equal(t, name, user.Username)
			check := func(token string) {
				claims, err := users.ValidateToken(token)
				require.NoError(t, err)
				require.Equal(t, name, claims.Username)
				require.Equal(t, email, claims.Email)
				require.Equal(t, []string{"viewer", "role-" + tc.unit}, claims.Roles)
			}
			check(token.AccessToken)
			api, err := users.GenerateAPIToken(user, "identity-test", time.Hour)
			require.NoError(t, err)
			check(api)
			for _, expiry := range []time.Duration{0, time.Hour} {
				var cluster string
				if expiry == 0 {
					cluster, err = users.GenerateClusterToken("node-"+tc.unit, Role("cluster-role-"+tc.unit))
				} else {
					cluster, err = users.GenerateClusterTokenWithExpiry("node-"+tc.unit, Role("cluster-role-"+tc.unit), expiry)
				}
				require.NoError(t, err)
				claims, err := users.ValidateToken(cluster)
				require.NoError(t, err)
				require.Equal(t, "node-"+tc.unit, claims.Username)
				require.Equal(t, []string{"cluster-role-" + tc.unit}, claims.Roles)
			}
		})
	}
}

func TestAuthJSONAcceptsExistingStandardSignedTokens(t *testing.T) {
	cfg := DefaultAuthConfig()
	cfg.BcryptCost = 4
	cfg.JWTSecret = []byte("synthetic-auth-json-old-token-key")
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = engine.Close() })
	users, err := NewAuthenticator(cfg, engine)
	require.NoError(t, err)
	for _, name := range []string{"old-ascii", "old-é-😀", `old-literal-\ud800`} {
		user, err := users.CreateUser(name, "synthetic-password", []Role{RoleEditor})
		require.NoError(t, err)
		header, err := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
		require.NoError(t, err)
		payload, err := json.Marshal(JWTClaims{Sub: user.ID, Username: name, Email: user.Email, Roles: []string{"editor"}, Iat: time.Now().Unix(), Exp: time.Now().Add(time.Hour).Unix()})
		require.NoError(t, err)
		message := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
		mac := hmac.New(sha256.New, cfg.JWTSecret)
		_, err = mac.Write([]byte(message))
		require.NoError(t, err)
		token := message + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
		claims, err := users.ValidateToken(token)
		require.NoError(t, err)
		require.Equal(t, name, claims.Username)
		require.Equal(t, []string{"editor"}, claims.Roles)
		parts := strings.Split(token, ".")
		parts[1] = base64.RawURLEncoding.EncodeToString([]byte(`{"username":"another"}`))
		_, err = users.ValidateToken(strings.Join(parts, "."))
		require.ErrorIs(t, err, ErrInvalidToken)
	}
}

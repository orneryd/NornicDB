package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Compare the public issuer wire products against the pre-consolidation
// standard-JSON HS256 contract independently of the private issuance owner.
func TestJWTIssuerPurposeWireContract(t *testing.T) {
	a := newTestAuthenticator(t)
	a.config.TokenExpiry = 2500 * time.Millisecond
	user, err := a.CreateUser("issuer-λ", "synthetic-password-issuer", []Role{RoleEditor, RoleViewer})
	require.NoError(t, err)
	user.Email = "issuer@example.invalid"
	check := func(token string, expected JWTClaims, expiry time.Duration, before int64) {
		t.Helper()
		parts := strings.Split(token, ".")
		require.Len(t, parts, 3)
		raw, err := base64.RawURLEncoding.DecodeString(parts[1])
		require.NoError(t, err)
		var actual JWTClaims
		require.NoError(t, json.Unmarshal(raw, &actual))
		require.GreaterOrEqual(t, actual.Iat, before)
		require.LessOrEqual(t, actual.Iat, time.Now().Unix())
		expected.Iat = actual.Iat
		if expiry > 0 {
			expected.Exp = actual.Iat + int64(expiry.Seconds())
		}
		require.Equal(t, expected, actual)
		header, err := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
		require.NoError(t, err)
		payload, err := json.Marshal(expected)
		require.NoError(t, err)
		message := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
		mac := hmac.New(sha256.New, a.config.JWTSecret)
		_, err = mac.Write([]byte(message))
		require.NoError(t, err)
		require.Equal(t, message+"."+base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), token)
	}
	expectedUser := JWTClaims{Sub: user.ID, Username: user.Username, Email: user.Email, Roles: []string{"editor", "viewer"}}
	for _, expiry := range []time.Duration{-time.Second, 0, time.Nanosecond, 2500 * time.Millisecond} {
		before := time.Now().Unix()
		token, err := a.GenerateAPIToken(user, "audit-only-subject", expiry)
		require.NoError(t, err)
		check(token, expectedUser, expiry, before)
		before = time.Now().Unix()
		token, err = a.GenerateClusterTokenWithExpiry("node-λ", RoleViewer, expiry)
		require.NoError(t, err)
		check(token, JWTClaims{Sub: clusterSubjectPrefix + "node-λ", Username: "node-λ", Roles: []string{"viewer"}}, expiry, before)
	}
	before := time.Now().Unix()
	token, err := a.GenerateClusterToken("node-λ", RoleAdmin)
	require.NoError(t, err)
	check(token, JWTClaims{Sub: clusterSubjectPrefix + "node-λ", Username: "node-λ", Roles: []string{"admin"}}, a.config.TokenExpiry, before)
	// Authenticate reloads its persisted user, retaining its independently stored email.
	before = time.Now().Unix()
	response, authenticated, err := a.Authenticate(user.Username, "synthetic-password-issuer", "127.0.0.1", "issuer-contract")
	require.NoError(t, err)
	check(response.AccessToken, JWTClaims{Sub: authenticated.ID, Username: authenticated.Username, Email: authenticated.Email, Roles: []string{"editor", "viewer"}}, a.config.TokenExpiry, before)
	a.config.JWTSecret = nil
	_, err = a.GenerateAPIToken(nil, "subject", time.Hour)
	require.Equal(t, ErrMissingSecret, err)
	_, err = a.GenerateClusterToken("node", RoleAdmin)
	require.Equal(t, ErrMissingSecret, err)
	_, err = a.GenerateClusterTokenWithExpiry("node", RoleAdmin, time.Hour)
	require.Equal(t, ErrMissingSecret, err)
}

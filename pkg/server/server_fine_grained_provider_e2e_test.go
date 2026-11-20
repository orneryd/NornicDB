package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

// TestFineGrainedLinkedOAuthHTTPWorkflow exercises the published HTTP
// authorization-code workflow end to end: the redirect and its state, the
// callback that redeems the code at the issuer, and an authenticated endpoint
// whose permission follows a role assigned from the verified provider profile.
func TestFineGrainedLinkedOAuthHTTPWorkflow(t *testing.T) {
	const (
		providerName      = "oidc-work"
		externalAccountID = "employee-12"
		localUsername     = "analyst"
		profileRole       = "analysts"
		department        = "analytics"
		authorizationCode = "fixture-authorization-code"
		accessToken       = "fixture-access-token"
		callbackURL       = "http://127.0.0.1:7474/auth/oauth/callback"
	)

	// The loopback issuer stands in for the operator's identity provider. It
	// serves only the two routes the callback uses and counts them, so the test
	// proves the callback really redeemed the code instead of short-circuiting.
	var tokenRequests, userinfoRequests atomic.Int32
	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth2/v1/token":
			tokenRequests.Add(1)
			if err := r.ParseForm(); err != nil ||
				r.PostForm.Get("grant_type") != "authorization_code" ||
				r.PostForm.Get("code") != authorizationCode {
				http.Error(w, "unexpected token request", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"access_token":%q,"token_type":"Bearer","expires_in":300}`, accessToken)
		case "/oauth2/v1/userinfo":
			userinfoRequests.Add(1)
			if r.Header.Get("Authorization") != "Bearer "+accessToken {
				http.Error(w, "unexpected userinfo authorization", http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w,
				`{"sub":%q,"preferred_username":"external-profile-name","email":"analyst@fixture.invalid","department":%q}`,
				externalAccountID, department)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(issuer.Close)

	server, users := setupTestServer(t)
	require.NoError(t, server.initializeFineGrainedRuntime(t.Context()))
	directory := server.fineGrainedRuntime

	// The local account owns the provider binding. Its only access to the
	// protected endpoint arrives through the profile-derived role: the account
	// carries no RBAC role of its own.
	require.NoError(t, directory.CreateRole(t.Context(), profileRole))
	require.NoError(t, directory.CreateUserAccount(t.Context(), auth.FineGrainedAccount{
		Name:      localUsername,
		Providers: map[string]string{providerName: externalAccountID},
	}))
	require.NoError(t, directory.EditCatalog(t.Context(), func(catalog *auth.FineGrainedCatalog) error {
		catalog.Definitions = append(catalog.Definitions, auth.FineGrainedEntitlement{
			ID:     "status-read",
			Action: "dbms.service.status.read",
			Scope:  auth.FineGrainedScope{Kind: auth.FineGrainedDBMS},
		})
		if catalog.Assignments == nil {
			catalog.Assignments = map[string][]string{}
		}
		catalog.Assignments[profileRole] = append(catalog.Assignments[profileRole], "status-read")
		catalog.AuthRules = append(catalog.AuthRules, auth.FineGrainedAuthRule{
			Name:      "profile-department",
			Condition: "abac.oidc.user_attribute('department') = 'analytics'",
			Enabled:   true,
			Roles:     []string{profileRole},
		})
		return nil
	}))

	// Process flags opt this server into the linked-user path and name the
	// provider and verified profile attributes the binding and rule use.
	t.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
	t.Setenv("NORNICDB_OAUTH_ISSUER", issuer.URL)
	t.Setenv("NORNICDB_OAUTH_CLIENT_ID", "fixture-client")
	t.Setenv("NORNICDB_OAUTH_CLIENT_SECRET", "fixture-client-secret")
	t.Setenv("NORNICDB_OAUTH_CALLBACK_URL", callbackURL)
	t.Setenv("NORNICDB_FGAC_REQUIRE_LOCAL_USER", "true")
	t.Setenv("NORNICDB_FGAC_OIDC_PROVIDER", providerName)
	t.Setenv("NORNICDB_FGAC_OIDC_ATTRIBUTES", "department")

	router := server.buildRouter()
	request := func(path, token string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		return response
	}

	// 1. Ordinary redirect: the server stores a fresh state and sends the
	// browser to the configured issuer's authorization endpoint.
	redirect := request("/auth/oauth/redirect", "")
	require.Equal(t, http.StatusFound, redirect.Code, redirect.Body.String())
	authorize, err := url.Parse(redirect.Header().Get("Location"))
	require.NoError(t, err)
	require.Equal(t, issuer.URL+"/oauth2/v1/authorize", authorize.Scheme+"://"+authorize.Host+authorize.Path)
	require.Equal(t, "code", authorize.Query().Get("response_type"))
	require.Equal(t, "fixture-client", authorize.Query().Get("client_id"))
	require.Equal(t, callbackURL, authorize.Query().Get("redirect_uri"))
	state := authorize.Query().Get("state")
	require.NotEmpty(t, state)

	// 2. Callback: the server redeems the code at the issuer, matches the
	// verified subject to the local binding and sets the browser session.
	callback := request("/auth/oauth/callback?code="+url.QueryEscape(authorizationCode)+"&state="+url.QueryEscape(state), "")
	require.Equal(t, http.StatusFound, callback.Code, callback.Body.String())
	require.Equal(t, "/", callback.Header().Get("Location"))
	require.Equal(t, int32(1), tokenRequests.Load())
	require.Equal(t, int32(1), userinfoRequests.Load())
	var session *http.Cookie
	for _, cookie := range callback.Result().Cookies() {
		if cookie.Name == "nornicdb_token" {
			session = cookie
		}
	}
	require.NotNil(t, session, "callback did not set the session cookie")
	require.True(t, session.HttpOnly)

	// The issued session is a verified provider session for the linked account.
	claims, provider, err := users.ValidateTokenWithFineGrainedProvider(t.Context(), session.Value)
	require.NoError(t, err)
	require.NotNil(t, provider)
	require.Equal(t, localUsername, claims.Username)
	require.NotEmpty(t, claims.Sub)

	// 3. Identity: the authenticated session resolves to the linked local
	// account, and the named provider binding names the verified subject.
	me := request("/auth/me", session.Value)
	require.Equal(t, http.StatusOK, me.Code, me.Body.String())
	var identity struct {
		Username string   `json:"username"`
		Roles    []string `json:"roles"`
	}
	require.NoError(t, json.Unmarshal(me.Body.Bytes(), &identity))
	require.Equal(t, localUsername, identity.Username)
	require.Empty(t, identity.Roles, "the profile-derived role is not an RBAC assignment")

	providers, err := directory.UserProviders(t.Context())
	require.NoError(t, err)
	var linked []auth.FineGrainedUserProvider
	for _, entry := range providers {
		if entry.Name == providerName {
			linked = append(linked, entry)
		}
	}
	require.Len(t, linked, 1)
	require.Equal(t, externalAccountID, linked[0].Subject)
	require.Equal(t, localUsername, linked[0].User.Username)
	require.False(t, linked[0].HasNative)

	// 4. The admission carries the entitlement assigned to the profile-derived
	// role, so the protected endpoint's permission follows that role.
	effective := request("/auth/fgac/effective", session.Value)
	require.Equal(t, http.StatusOK, effective.Code, effective.Body.String())
	var granted struct {
		Enforced bool                          `json:"enforced"`
		Rules    []auth.FineGrainedEntitlement `json:"rules"`
	}
	require.NoError(t, json.Unmarshal(effective.Body.Bytes(), &granted))
	require.True(t, granted.Enforced)
	grantedStatus := -1
	for i, rule := range granted.Rules {
		if rule.Action == "dbms.service.status.read" {
			grantedStatus = i
		}
	}
	require.NotEqual(t, -1, grantedStatus, "the profile-derived role granted no status permission")
	require.Equal(t, auth.FineGrainedDBMS, granted.Rules[grantedStatus].Scope.Kind)
	require.Empty(t, granted.Rules[grantedStatus].Scope.DatabaseID)

	status := request("/status", session.Value)
	require.Equal(t, http.StatusOK, status.Code, status.Body.String())
	var report struct {
		Status string `json:"status"`
	}
	require.NoError(t, json.Unmarshal(status.Body.Bytes(), &report))
	require.Equal(t, "running", report.Status)

	require.NoError(t, directory.EditCatalog(t.Context(), func(catalog *auth.FineGrainedCatalog) error {
		catalog.AuthRules[0].Enabled = false
		return nil
	}))
	deniedStatus := request("/status", session.Value)
	require.Equal(t, http.StatusForbidden, deniedStatus.Code, deniedStatus.Body.String())
	require.NotContains(t, deniedStatus.Body.String(), `"running"`)
	require.NoError(t, directory.EditCatalog(t.Context(), func(catalog *auth.FineGrainedCatalog) error {
		catalog.AuthRules[0].Enabled = true
		return nil
	}))
	require.Equal(t, http.StatusOK, request("/status", session.Value).Code)

	assertRejectedSignIn := func() {
		t.Helper()
		redirect := request("/auth/oauth/redirect", "")
		require.Equal(t, http.StatusFound, redirect.Code)
		location, err := url.Parse(redirect.Header().Get("Location"))
		require.NoError(t, err)
		state := location.Query().Get("state")
		require.NotEmpty(t, state)
		callback := request("/auth/oauth/callback?code="+url.QueryEscape(authorizationCode)+"&state="+url.QueryEscape(state), "")
		require.Equal(t, http.StatusBadRequest, callback.Code, callback.Body.String())
		require.Empty(t, callback.Result().Cookies())
	}

	require.NoError(t, directory.SetUserSuspended(t.Context(), localUsername, true))
	suspended := request("/auth/me", session.Value)
	require.Equal(t, http.StatusUnauthorized, suspended.Code, suspended.Body.String())
	assertRejectedSignIn()
	require.NoError(t, directory.SetUserSuspended(t.Context(), localUsername, false))

	require.NoError(t, directory.AlterUser(t.Context(), localUsername, auth.FineGrainedUserAlteration{
		Providers: &auth.FineGrainedProviderChange{Set: map[string]string{providerName: "employee-replacement"}},
	}))
	rebound := request("/auth/me", session.Value)
	require.Equal(t, http.StatusUnauthorized, rebound.Code, rebound.Body.String())
	assertRejectedSignIn()
}

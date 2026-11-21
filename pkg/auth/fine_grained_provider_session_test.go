package auth

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFineGrainedLinkedOAuthSession(t *testing.T) {
	users, roles := currentRolesAuthenticator(t)
	ctx := t.Context()
	runtime := NewFineGrainedRuntime(roles, fgacTestActions, func(string) string { return "reports-id" }, func() string { return "reports" })
	runtime.SetSecurityDirectory(users, RoleMutationStores{Users: users})
	runtime.SetAuthRuleCompiler(func(expression string) (FineGrainedAuthRuleCondition, error) {
		require.Equal(t, "configured department", expression)
		return func(input FineGrainedAuthRuleInput) bool { return input.Claims["department"] == "analytics" }, nil
	})
	require.NoError(t, runtime.CreateRole(ctx, "operators"))
	require.NoError(t, runtime.CreateUserAccount(ctx, FineGrainedAccount{
		Name: "analyst", Providers: map[string]string{"oidc-work": "employee-12"},
	}))
	require.NoError(t, runtime.ReplaceCatalog(ctx, "", FineGrainedCatalog{
		Definitions: []FineGrainedEntitlement{{ID: "retry", Action: "embedding.failures.retry",
			Scope: FineGrainedScope{Kind: FineGrainedDatabase, DatabaseID: "reports-id"}}},
		Assignments: map[string][]string{"operators": {"retry"}},
		AuthRules: []FineGrainedAuthRule{
			{Name: "department", Condition: "configured department", Enabled: true, Roles: []string{"operators"}},
		},
	}))

	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/oauth2/v1/userinfo" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"sub":"employee-12","preferred_username":"external-display-name","department":"analytics","region":"example"}`)
	}))
	t.Cleanup(issuer.Close)
	t.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
	t.Setenv("NORNICDB_OAUTH_ISSUER", issuer.URL)
	t.Setenv("NORNICDB_OAUTH_CLIENT_ID", "example-integration")
	t.Setenv("NORNICDB_OAUTH_CLIENT_SECRET", "fixture-only")
	t.Setenv("NORNICDB_OAUTH_CALLBACK_URL", issuer.URL+"/callback")
	t.Setenv("NORNICDB_FGAC_OIDC_PROVIDER", "oidc-work")
	t.Setenv("NORNICDB_FGAC_OIDC_ATTRIBUTES", "department")

	expiry := time.Now().Add(2 * time.Minute)
	linked, token, tokenExpiry, err := NewOAuthManager(users).admitLinkedOAuthUser(ctx, "fixture-access-token", expiry)
	require.NoError(t, err)
	require.Equal(t, "analyst", linked.Username)
	require.Equal(t, expiry, tokenExpiry)
	claims, provider, err := users.ValidateTokenWithFineGrainedProvider(ctx, token)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"department": "analytics"}, provider.attributes)
	identity := RequestIdentity{ID: PrincipalID(claims), Username: claims.Username}
	request, err := runtime.Admit(WithRequestIdentity(provider.WithContext(ctx), identity), claims.Roles)
	require.NoError(t, err)
	require.True(t, request.AllowsDatabase("embedding.failures.retry", "reports", ""))
	require.NoError(t, request.Revalidate(ctx, identity))

	require.ErrorIs(t, runtime.AlterUser(ctx, "analyst", FineGrainedUserAlteration{
		Providers: &FineGrainedProviderChange{RemoveAll: true},
	}), ErrFineGrainedProviderRequired)
	require.NoError(t, runtime.AlterUser(ctx, "analyst", FineGrainedUserAlteration{
		Providers: &FineGrainedProviderChange{Set: map[string]string{"oidc-work": "employee-24"}},
	}))
	entries, err := runtime.UserProviders(ctx)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "employee-24", entries[0].Subject)
	require.False(t, entries[0].HasNative)
	_, err = users.ValidateToken(token)
	require.ErrorIs(t, err, ErrInvalidCredentials)
}

func TestFineGrainedProviderBindingCanBeReassignedAfterRBACDeletion(t *testing.T) {
	users, roles := currentRolesAuthenticator(t)
	ctx := t.Context()
	runtime := NewFineGrainedRuntime(roles, fgacTestActions, nil, nil)
	runtime.SetSecurityDirectory(users, RoleMutationStores{Users: users})
	account := FineGrainedAccount{
		Name: "analyst", Providers: map[string]string{"oidc-work": "employee-12"},
	}
	require.NoError(t, runtime.CreateUserAccount(ctx, account))
	before, err := users.fineGrainedUserForProvider(ctx, "oidc-work", "employee-12")
	require.NoError(t, err)

	rbac, err := NewAuthenticator(users.config, users.storage)
	require.NoError(t, err)
	require.NoError(t, rbac.DeleteUser(ctx, account.Name))
	bound, err := fineGrainedProviderUserID(ctx, roles.storage, "oidc-work", "employee-12")
	require.NoError(t, err)
	require.Equal(t, before.ID, bound, "RBAC leaves the FGAC sidecar dormant")

	require.NoError(t, runtime.CreateUserAccount(ctx, account))
	after, err := users.fineGrainedUserForProvider(ctx, "oidc-work", "employee-12")
	require.NoError(t, err)
	require.NotEqual(t, before.ID, after.ID)
}

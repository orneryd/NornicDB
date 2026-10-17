package localization

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

// The refusals and impersonation messages read in Neo4j 5's words, in the
// English catalog as in their fallback, naming the principal by its sorted,
// distinct roles (SecurityContext.description).
func TestAccessControlMessages(t *testing.T) {
	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	for _, test := range []struct {
		message Message
		text    string
	}{
		{DatabaseAccessDenied("alice", []string{"reader", "editor", "reader"}), "Database access is not allowed for user 'alice' with roles [editor, reader]."},
		{DatabaseAccessDenied("", nil), "Database access is not allowed for user '' with no roles."},
		{DatabaseWriteDenied("alice", []string{"reader"}), "Write operations are not allowed for user 'alice' with roles [reader]."},
		{DatabaseWriteDenied("bob", nil), "Write operations are not allowed for user 'bob' with no roles."},
		{BoltImpersonationForbidden("bob"), "Permission denied: impersonating user 'bob' is not allowed."},
		{BoltImpersonationAuthDisabled(), "Impersonation is not supported with auth disabled."},
		{BoltImpersonationUnsupported(), "Impersonation is not supported by this server's authenticator."},
		{BoltImpersonatedUserInvalid(), "imp_user must be a string or null."},
		{CypherAdminPrivilegeDenyRoleNotFound("impersonate", "ghost"), "Failed to deny impersonate privilege to role 'ghost': Role does not exist."},
		{CypherAdminImpersonationSystemUpdateDenied(), "Not allowed to run updating system commands when impersonating a user."},
		{CypherAdminImpersonationRemoteDenied("remote_db"), "Impersonating a user is not supported on remote database 'remote_db'."},
		{OperationContractRequired(), "Operation requires a valid privilege contract"},
	} {
		require.Equal(t, test.text, test.message.Fallback, test.message.ID)
		rendered, _, err := manager.Render(context.Background(), test.message)
		require.NoError(t, err, test.message.ID)
		require.Equal(t, test.text, rendered, test.message.ID)
	}
	require.Equal(t, map[string]any{"User": "alice", "Roles": "editor, reader", "HasRoles": true},
		DatabaseAccessDenied("alice", []string{"reader", "editor"}).Data)
	require.Equal(t, map[string]any{"User": "bob", "Roles": "", "HasRoles": false}, DatabaseWriteDenied("bob", nil).Data)
}

// The query log keeps both users while impersonating (Neo4j's
// authenticatedUser and executingUser); otherwise only the user.
func TestBoltQueryEventAttribution(t *testing.T) {
	keys := func(event LogEvent) map[string]string {
		values := map[string]string{}
		for _, attr := range event.Attrs {
			values[attr.Key] = attr.Value.String()
		}
		return values
	}
	impersonating := keys(BoltQueryEvent("svc", "alice", "10.0.0.1:7687", "RETURN 1", nil))
	require.Equal(t, "svc", impersonating["user"])
	require.Equal(t, "svc", impersonating["authenticatedUser"])
	require.Equal(t, "alice", impersonating["executingUser"])
	for _, executing := range []string{"", "svc"} {
		signedIn := keys(BoltQueryEvent("svc", executing, "10.0.0.1:7687", "RETURN 1", map[string]any{"x": 1}))
		require.Equal(t, "svc", signedIn["user"])
		require.NotContains(t, signedIn, "authenticatedUser")
		require.NotContains(t, signedIn, "executingUser")
		require.Contains(t, signedIn, "params")
	}

	cleanup := OperationStorageCleanupFailedEvent("tenant", errors.New("closed"))
	require.Equal(t, EventID("cypher.operation.storage_cleanup_failed"), cleanup.ID)
	attrs := keys(cleanup)
	require.Equal(t, "tenant", attrs["database"])
	require.Equal(t, "cleanup_failed", attrs["outcome"])
	require.Equal(t, "closed", attrs["error"])
}

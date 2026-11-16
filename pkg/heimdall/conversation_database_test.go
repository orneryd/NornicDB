package heimdall

import (
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConversationDatabaseUsesResolvedSourceAndLiveVisibility(t *testing.T) {
	allowed := true
	ctx := auth.WithRequestPrincipal(t.Context(), func(database string, permission auth.Permission) bool {
		return allowed && database == "nornic" && permission == auth.PermRead
	}, auth.NewRequestDatabaseScope("nornic", map[string]string{"source-alias": "nornic", "nornic": "nornic"}))
	router := newConversationDatabase(&mockDBRouter{}, &PromptContext{Context: ctx, SourceDatabase: "ignored-fallback"})
	require.Equal(t, "nornic", router.DefaultDatabaseName())
	for _, selection := range []string{"", "other"} {
		resolved, err := router.ResolveDatabase(selection)
		require.NoError(t, err)
		expected := selection
		if selection == "" {
			expected = "nornic"
		}
		require.Equal(t, expected, resolved, "name resolution is delegated; the host separately authorizes actual reads")
	}
	require.Equal(t, []string{"nornic"}, router.ListDatabases())
	allowed = false
	require.Empty(t, router.ListDatabases(), "retained conversation routers observe revoked visibility")
	fallback := newConversationDatabase(&mockDBRouter{}, &PromptContext{Context: t.Context(), SourceDatabase: "archive"})
	require.Equal(t, "archive", fallback.DefaultDatabaseName())
	resolved, err := fallback.ResolveDatabase("")
	require.NoError(t, err)
	require.Equal(t, "archive", resolved)
}

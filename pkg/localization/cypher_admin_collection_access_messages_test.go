package localization

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

// The role command, access scope and restricted-view messages (#935) keep
// their English text as the fallback and in the en-US catalog, and render
// from the es-ES and en-XA catalogs with their data.
func TestCypherAdminCollectionAccessMessagesRender(t *testing.T) {
	testCases := []struct {
		message Message
		id      MessageID
		data    map[string]any
	}{
		{CypherAdminAccessScopeExists("lib"), MessageCypherAdminAccessScopeExists, map[string]any{"Graph": "lib"}},
		{CypherAdminAccessScopeNotFound("collections", "lib"), MessageCypherAdminAccessScopeNotFound, map[string]any{"Name": "collections", "Graph": "lib"}},
		{CypherAdminHiddenRelationshipDelete(), MessageCypherAdminHiddenRelationshipDelete, nil},
		{CypherAdminRestrictedMultiGraph(), MessageCypherAdminRestrictedMultiGraph, nil},
		{CypherAdminRestrictedOperation("gds.graph.project"), MessageCypherAdminRestrictedOperation, map[string]any{"Operation": "gds.graph.project"}},
		{CypherAdminRestrictedProjectionStale("topics"), MessageCypherAdminRestrictedProjectionStale, map[string]any{"Graph": "topics"}},
		{CypherAdminRoleAlreadyExists("ops"), MessageCypherAdminRoleAlreadyExists, map[string]any{"Role": "ops"}},
		{CypherAdminRoleBuiltin("admin"), MessageCypherAdminRoleBuiltin, map[string]any{"Role": "admin"}},
		{CypherAdminRoleCommandInTransaction(), MessageCypherAdminRoleCommandInTransaction, nil},
		{CypherAdminRoleCommandInvalid("CREATE ROLE", "a role name is required"), MessageCypherAdminRoleCommandInvalid, map[string]any{"Command": "CREATE ROLE", "Cause": "a role name is required"}},
		{CypherAdminRoleNameInvalid(""), MessageCypherAdminRoleNameInvalid, map[string]any{"Role": ""}},
		{CypherAdminRoleNotFound("ops"), MessageCypherAdminRoleNotFound, map[string]any{"Role": "ops"}},
		{CypherAdminRoleUserNotFound("alice"), MessageCypherAdminRoleUserNotFound, map[string]any{"User": "alice"}},
	}

	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	pseudoTag := language.MustParse("en-XA")
	for _, testCase := range testCases {
		id := testCase.message.ID
		require.Equal(t, testCase.id, id)
		require.NotEmpty(t, testCase.message.Fallback, id)
		if testCase.data == nil {
			require.Empty(t, testCase.message.Data, id)
		} else {
			require.Equal(t, testCase.data, testCase.message.Data, id)
		}

		english, tag, err := manager.Render(WithPreferences(context.Background(), language.AmericanEnglish), testCase.message)
		require.NoError(t, err, id)
		require.Equal(t, language.AmericanEnglish, tag, id)
		require.Equal(t, testCase.message.Fallback, english, id)

		spanish, tag, err := manager.Render(WithPreferences(context.Background(), language.EuropeanSpanish), testCase.message)
		require.NoError(t, err, id)
		require.Equal(t, language.EuropeanSpanish, tag, id)
		require.NotEqual(t, english, spanish, id)

		pseudo, tag, err := manager.Render(WithPreferences(context.Background(), pseudoTag), testCase.message)
		require.NoError(t, err, id)
		require.Equal(t, pseudoTag, tag, id)
		require.Equal(t, "[!! "+english+" !!]", pseudo, id)
	}
}

package localization

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

// TestCypherAdminProcedurePrivilegeMessagesRender pins the procedure
// privilege messages: each keeps Neo4j 5.26.30 Enterprise's English text as
// its fallback and in the en-US catalog, and renders from the es-ES and
// en-XA catalogs with its data.
func TestCypherAdminProcedurePrivilegeMessagesRender(t *testing.T) {
	testCases := []struct {
		message          Message
		id               MessageID
		data             map[string]any
		english, spanish string
	}{
		{
			CypherAdminImmutablePrivilegeGrantForbidden(), MessageCypherAdminImmutablePrivilegeGrantForbidden, nil,
			"Permission cannot be granted for ASSIGN IMMUTABLE PRIVILEGE.",
			"No se puede conceder el permiso ASSIGN IMMUTABLE PRIVILEGE.",
		},
		{
			CypherAdminImmutablePrivilegeRevokeForbidden(), MessageCypherAdminImmutablePrivilegeRevokeForbidden, nil,
			"Immutable privileges cannot be revoked.",
			"Los privilegios inmutables no se pueden revocar.",
		},
		{
			CypherAdminPrivilegeRoleNotFound("execute_boosted", "nobody"), MessageCypherAdminPrivilegeRoleNotFound, map[string]any{"Action": "execute_boosted", "Role": "nobody"},
			"Failed to grant execute_boosted privilege to role 'nobody': Role does not exist.",
			"No se pudo conceder el privilegio execute_boosted al rol 'nobody': el rol no existe.",
		},
	}

	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	pseudoTag := language.MustParse("en-XA")
	for _, testCase := range testCases {
		id := testCase.message.ID
		require.Equal(t, testCase.id, id)
		require.Equal(t, testCase.english, testCase.message.Fallback, id)
		if testCase.data == nil {
			require.Empty(t, testCase.message.Data, id)
		} else {
			require.Equal(t, testCase.data, testCase.message.Data, id)
		}
		for tag, want := range map[language.Tag]string{
			language.AmericanEnglish: testCase.english,
			language.EuropeanSpanish: testCase.spanish,
			pseudoTag:                "[!! " + testCase.english + " !!]",
		} {
			rendered, renderedTag, err := manager.Render(WithPreferences(context.Background(), tag), testCase.message)
			require.NoError(t, err, id)
			require.Equal(t, tag, renderedTag, id)
			require.Equal(t, want, rendered, id)
		}
	}
}

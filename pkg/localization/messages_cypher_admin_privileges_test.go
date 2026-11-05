package localization

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

// TestCypherAdminProcedurePrivilegeMessagesRender pins the procedure
// privilege and system database messages: each keeps its English text
// (Neo4j 5.26.30's where Neo4j has one) as its fallback and in the en-US
// catalog, and renders from the es-ES and en-XA catalogs with its data.
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
		{
			CypherAdminSystemGraphAccessUnsupported(), MessageCypherAdminSystemGraphAccessUnsupported, nil,
			"Graph access is not supported on the system database",
			"El acceso al grafo no se admite en la base de datos del sistema",
		},
		{
			CypherAdminSystemProcedureUnsupported("CALL db.createLabel('X')"), MessageCypherAdminSystemProcedureUnsupported, map[string]any{"Query": "CALL db.createLabel('X')"},
			"Not a recognised system command or procedure. This Cypher command can only be executed in a user database: CALL db.createLabel('X')",
			"No es un comando ni un procedimiento de sistema reconocido. Este comando Cypher solo se puede ejecutar en una base de datos de usuario: CALL db.createLabel('X')",
		},
		{
			CypherAdminTransactionStatementType("Administration command", "Read query"), MessageCypherAdminTransactionStatementType, map[string]any{"Previous": "Administration command", "Requested": "Read query"},
			"Tried to execute Read query after executing Administration command",
			"Se intentó ejecutar Read query después de ejecutar Administration command",
		},
		{
			CypherAdminShowClauseUnexpected("WITH"), MessageCypherAdminShowClauseUnexpected, map[string]any{"Clause": "WITH"},
			"Invalid input 'WITH': expected ',', 'AS', 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SHOW', 'SKIP', 'TERMINATE', 'WHERE' or <EOF>",
			"Entrada no válida 'WITH': se esperaba ',', 'AS', 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SHOW', 'SKIP', 'TERMINATE', 'WHERE' o <EOF>",
		},
		{
			CypherAdminShowExpressionInvalid("COUNT"), MessageCypherAdminShowExpressionInvalid, map[string]any{"Expression": "COUNT"},
			"The COUNT expression is not valid on SHOW commands.",
			"La expresión COUNT no es válida en los comandos SHOW.",
		},
		{
			CypherAdminShowPatternComprehensionInvalid(), MessageCypherAdminShowPatternComprehensionInvalid, nil,
			"Pattern comprehensions are not valid on SHOW commands.",
			"Las comprensiones de patrón no son válidas en los comandos SHOW.",
		},
		{
			CypherAdminShowPatternExpressionInvalid(), MessageCypherAdminShowPatternExpressionInvalid, nil,
			"Pattern expressions are not valid on SHOW commands.",
			"Las expresiones de patrón no son válidas en los comandos SHOW.",
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
			rendered, renderedTag, err := manager.Render(WithPreferences(context.Background(), tag), MessageRequest(testCase.message))
			require.NoError(t, err, id)
			require.Equal(t, tag, renderedTag, id)
			require.Equal(t, want, rendered, id)
		}
	}
}

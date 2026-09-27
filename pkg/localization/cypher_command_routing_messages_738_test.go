package localization

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

// TestCypherCommandRoutingUseGraphMessagesRender pins the USE / graph
// function / transaction-routing messages (#738): each keeps Neo4j's exact
// English text as its fallback and in the en-US catalog, and renders from
// the es-ES and en-XA catalogs with its data.
func TestCypherCommandRoutingUseGraphMessagesRender(t *testing.T) {
	const clauseAfterGraph = "expected a database name, '(', 'FOREACH', '.', 'ALTER', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'START DATABASE', 'STOP DATABASE', 'DEALLOCATE', 'DELETE', 'DENY', 'DETACH', 'DROP', 'DRYRUN', 'FINISH', 'GRANT', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REALLOCATE', 'REMOVE', 'RENAME', 'RETURN', 'REVOKE', 'ENABLE SERVER', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>"
	const subqueryClauseAfterGraph = "expected a database name, '(', 'FOREACH', '.', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or '}'"
	const spanishClauseAfterGraph = "se esperaba a database name, '(', 'FOREACH', '.', 'ALTER', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'START DATABASE', 'STOP DATABASE', 'DEALLOCATE', 'DELETE', 'DENY', 'DETACH', 'DROP', 'DRYRUN', 'FINISH', 'GRANT', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REALLOCATE', 'REMOVE', 'RENAME', 'RETURN', 'REVOKE', 'ENABLE SERVER', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WITH' o <EOF>"
	const spanishSubqueryClauseAfterGraph = "se esperaba a database name, '(', 'FOREACH', '.', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' o '}'"

	testCases := []struct {
		message Message
		id      MessageID
		data    map[string]any
		english string
		spanish string
	}{
		{
			CypherCommandRoutingUseQueryCannotConclude(), MessageCypherCommandRoutingUseQueryCannotConclude, nil,
			"Query cannot conclude with USE GRAPH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
			"La consulta no puede terminar con USE GRAPH (debe ser una cláusula RETURN, una cláusula FINISH, una cláusula de actualización, una llamada a subconsulta unitaria o una llamada a procedimiento sin YIELD).",
		},
		{
			CypherCommandRoutingUseSubqueryMustConclude(), MessageCypherCommandRoutingUseSubqueryMustConclude, nil,
			"Query must conclude with a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD.",
			"La consulta debe terminar con una cláusula RETURN, una cláusula FINISH, una cláusula de actualización, una llamada a subconsulta unitaria o una llamada a procedimiento sin YIELD.",
		},
		{
			CypherCommandRoutingUseInvalidClauseAfterGraph("x"), MessageCypherCommandRoutingUseInvalidClauseAfterGraph, map[string]any{"Input": "x"},
			"Invalid input 'x': " + clauseAfterGraph,
			"Entrada no válida 'x': " + spanishClauseAfterGraph,
		},
		{
			CypherCommandRoutingUseInvalidSubqueryClauseAfterGraph("x"), MessageCypherCommandRoutingUseInvalidSubqueryClauseAfterGraph, map[string]any{"Input": "x"},
			"Invalid input 'x': " + subqueryClauseAfterGraph,
			"Entrada no válida 'x': " + spanishSubqueryClauseAfterGraph,
		},
		{
			CypherCommandRoutingUseInvalidGraphReference("1"), MessageCypherCommandRoutingUseInvalidGraphReference, map[string]any{"Input": "1"},
			"Invalid input '1': expected an identifier, '(' or 'GRAPH'",
			"Entrada no válida '1': se esperaba un identificador, '(' o 'GRAPH'",
		},
		{
			CypherCommandRoutingUseInvalidGraphNamePart("1"), MessageCypherCommandRoutingUseInvalidGraphNamePart, map[string]any{"Input": "1"},
			"Invalid input '1': expected a database name or an identifier",
			"Entrada no válida '1': se esperaba un nombre de base de datos o un identificador",
		},
		{
			CypherCommandRoutingUseInvalidGraphFunctionArgument("MATCH"), MessageCypherCommandRoutingUseInvalidGraphFunctionArgument, map[string]any{"Input": "MATCH"},
			"Invalid input 'MATCH': expected an expression, ')' or ','",
			"Entrada no válida 'MATCH': se esperaba una expresión, ')' o ','",
		},
		{
			CypherCommandRoutingUseNotFirstClause(), MessageCypherCommandRoutingUseNotFirstClause, nil,
			"USE clause must be either the first clause in a (sub-)query or preceded by an importing WITH clause in a sub-query.",
			"La cláusula USE debe ser la primera cláusula de una (sub)consulta o ir precedida de una cláusula WITH de importación en una subconsulta.",
		},
		{
			CypherCommandRoutingUseAdministrationCommand(), MessageCypherCommandRoutingUseAdministrationCommand, nil,
			"The `USE` clause is not required for Administration Commands. Retry your query omitting the `USE` clause and it will be routed automatically.",
			"La cláusula `USE` no es necesaria para los comandos de administración. Vuelva a intentar la consulta sin la cláusula `USE` y se enrutará automáticamente.",
		},
		{
			CypherCommandRoutingUseDynamicLookupNotAllowed("cmp.a"), MessageCypherCommandRoutingUseDynamicLookupNotAllowed, map[string]any{"Graph": "cmp.a"},
			"Dynamic graph lookup not allowed here. This feature is only available on composite databases.\nAttempted to access graph cmp.a",
			"La búsqueda dinámica de grafos no está permitida aquí. Esta función solo está disponible en bases de datos compuestas.\nSe intentó acceder al grafo cmp.a",
		},
		{
			CypherCommandRoutingGraphNotFound("cmp.missing"), MessageCypherCommandRoutingGraphNotFound, map[string]any{"Graph": "cmp.missing"},
			"Graph not found: cmp.missing",
			"Grafo no encontrado: cmp.missing",
		},
		{
			CypherCommandRoutingGraphFunctionUnknown("graph.nope"), MessageCypherCommandRoutingGraphFunctionUnknown, map[string]any{"Function": "graph.nope"},
			"Unknown function 'graph.nope'",
			"Función desconocida 'graph.nope'",
		},
		{
			CypherCommandRoutingGraphFunctionArgumentCount("graph.byName", 2), MessageCypherCommandRoutingGraphFunctionArgumentCount, map[string]any{"Function": "graph.byName", "Count": 2},
			"graph.byName takes 1 argument, got 2",
			"graph.byName acepta 1 argumento, se recibieron 2",
		},
		{
			CypherCommandRoutingGraphFunctionArgumentType("name", "INTEGER"), MessageCypherCommandRoutingGraphFunctionArgumentType, map[string]any{"Argument": "name", "Type": "INTEGER"},
			"Expected name to be a STRING, but it was INTEGER",
			"Se esperaba que name fuera un STRING, pero era INTEGER",
		},
		{
			CypherCommandRoutingGraphFunctionArgumentInvalid("$g"), MessageCypherCommandRoutingGraphFunctionArgumentInvalid, map[string]any{"Argument": "$g"},
			"Invalid graph reference argument: $g",
			"Argumento de referencia de grafo no válido: $g",
		},
		{
			CypherCommandRoutingGraphElementIDInvalid("bad-id"), MessageCypherCommandRoutingGraphElementIDInvalid, map[string]any{"ElementID": "bad-id"},
			"Element ID bad-id has an unexpected format.",
			"El ID de elemento bad-id tiene un formato inesperado.",
		},
		{
			CypherCommandRoutingTransactionSecondDatabaseWrite("db", "da"), MessageCypherCommandRoutingTransactionSecondDatabaseWrite, map[string]any{"Target": "db", "Current": "da"},
			"Writing to more than one database per transaction is not allowed. Attempted write to db, currently writing to da",
			"No se permite escribir en más de una base de datos por transacción. Se intentó escribir en db, actualmente se escribe en da",
		},
		{
			CypherCommandRoutingTransactionSecondDatabaseAccess("db", "da"), MessageCypherCommandRoutingTransactionSecondDatabaseAccess, map[string]any{"Target": "db", "Current": "da"},
			"Accessing more than one database per transaction is not allowed. Attempted access to db, currently using da",
			"No se permite acceder a más de una base de datos por transacción. Se intentó acceder a db, actualmente se usa da",
		},
		{
			CypherCommandRoutingGraphFunctionOnlyInUse("graph.byName"), MessageCypherCommandRoutingGraphFunctionOnlyInUse, map[string]any{"Function": "graph.byName"},
			"`graph.byName` is only allowed at the first position of a USE clause.",
			"`graph.byName` solo está permitido en la primera posición de una cláusula USE.",
		},
	}
	require.Len(t, testCases, 19)

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

		english, tag, err := manager.Render(WithPreferences(context.Background(), language.AmericanEnglish), testCase.message)
		require.NoError(t, err, id)
		require.Equal(t, language.AmericanEnglish, tag, id)
		require.Equal(t, testCase.english, english, id)

		spanish, tag, err := manager.Render(WithPreferences(context.Background(), language.EuropeanSpanish), testCase.message)
		require.NoError(t, err, id)
		require.Equal(t, language.EuropeanSpanish, tag, id)
		require.Equal(t, testCase.spanish, spanish, id)

		pseudo, tag, err := manager.Render(WithPreferences(context.Background(), pseudoTag), testCase.message)
		require.NoError(t, err, id)
		require.Equal(t, pseudoTag, tag, id)
		require.Equal(t, "[!! "+testCase.english+" !!]", pseudo, id)
	}
}

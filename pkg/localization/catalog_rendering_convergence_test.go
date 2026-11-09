package localization

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

// catalogRenderingCase is one former standalone catalog rendering test: the original
// message expression plus the exact expected renders in the original assertion order.
// tag is both the language preference presented to the manager and the exact tag the
// manager must report back; text is the exact rendered string.
type catalogRenderingCase struct {
	Message Message
	renders []catalogRenderingExpectation
}

type catalogRenderingExpectation struct {
	tag  language.Tag
	text string
}

// TestCatalogMessageRenderingNamespaces is the single data-driven owner of the catalog
// manager rendering flow: for every case, construct a fresh manager over English
// defaults, render the message through MessageRequest under each expected language
// preference, and assert no error, the reported language tag and the exact text.
//
// Each subtest keeps its original standalone test function name.
func TestCatalogMessageRenderingNamespaces(t *testing.T) {
	pseudoTag := language.MustParse("en-XA")

	testCases := []struct {
		name string
		catalogRenderingCase
	}{
		{
			name: "TestCypherCommandRoutingCatalogRendering",
			catalogRenderingCase: catalogRenderingCase{
				Message: CypherCommandRoutingUnknownProcedure("missing.procedure"),
				renders: []catalogRenderingExpectation{
					{language.EuropeanSpanish, "procedimiento desconocido: missing.procedure (pruebe SHOW PROCEDURES para ver los procedimientos disponibles)"},
					{pseudoTag, "[!! unknown procedure: missing.procedure (try SHOW PROCEDURES for available procedures) !!]"},
				},
			},
		},
		{
			name: "TestCypherCompositeCatalogRendering",
			catalogRenderingCase: catalogRenderingCase{
				Message: CypherCompositeConstituentUnexpectedToken("BROKEN"),
				renders: []catalogRenderingExpectation{
					{language.EuropeanSpanish, "sintaxis de componente no válida: token inesperado 'BROKEN'"},
					{pseudoTag, "[!! invalid constituent syntax: unexpected token 'BROKEN' !!]"},
				},
			},
		},
		{
			name: "TestCypherMergeCatalogRendering",
			catalogRenderingCase: catalogRenderingCase{
				Message: CypherMergeUnwindParameterNotFound("rows"),
				renders: []catalogRenderingExpectation{
					{language.EuropeanSpanish, "no se encontró el parámetro UNWIND $rows o es nulo"},
					{pseudoTag, "[!! UNWIND parameter $rows not found or is null !!]"},
				},
			},
		},
		{
			name: "TestCypherSchemaCatalogRendering",
			catalogRenderingCase: catalogRenderingCase{
				Message: CypherSchemaRangeIndexSinglePropertyRequired(2),
				renders: []catalogRenderingExpectation{
					{language.EuropeanSpanish, "RANGE INDEX solo admite una propiedad; se obtuvieron 2"},
					{pseudoTag, "[!! RANGE INDEX only supports single property, got 2 !!]"},
				},
			},
		},
		{
			name: "TestCypherTransactionsCatalogRendering",
			catalogRenderingCase: catalogRenderingCase{
				Message: CypherTransactionsInvalidScriptAction("ABORT"),
				renders: []catalogRenderingExpectation{
					{language.EuropeanSpanish, "acción de script de transacción no válida: ABORT"},
					{pseudoTag, "[!! invalid transaction script action: ABORT !!]"},
				},
			},
		},
		{
			name: "TestNornicDBRetentionSweepBudgetRendersAtBoundary",
			catalogRenderingCase: catalogRenderingCase{
				Message: NornicDBRetentionSweepBudgetExhausted(25),
				renders: []catalogRenderingExpectation{
					{language.EuropeanSpanish, "presupuesto del barrido agotado (25 registros)"},
					{pseudoTag, "[!! sweep budget exhausted (25 records) !!]"},
				},
			},
		},
		{
			// The ANTLR entrypoint is the only case that also pins the English render,
			// and it does so before Spanish and pseudo.
			name: "TestParseEntrypointCatalogRendering",
			catalogRenderingCase: catalogRenderingCase{
				Message: CypherANTLRParseSyntaxError("line 1:7 unexpected token"),
				renders: []catalogRenderingExpectation{
					{language.AmericanEnglish, "syntax error: line 1:7 unexpected token"},
					{language.EuropeanSpanish, "error de sintaxis: line 1:7 unexpected token"},
					{pseudoTag, "[!! syntax error: line 1:7 unexpected token !!]"},
				},
			},
		},
		{
			name: "TestCypherSubqueriesCatalogRendering",
			catalogRenderingCase: catalogRenderingCase{
				Message: CypherSubqueriesCallError(errors.New("boom")),
				renders: []catalogRenderingExpectation{
					{language.EuropeanSpanish, "error de subconsulta CALL: boom"},
					{pseudoTag, "[!! CALL subquery error: boom !!]"},
				},
			},
		},
		{
			name: "TestCypherCoreCatalogRendering",
			catalogRenderingCase: catalogRenderingCase{
				Message: CypherCoreCaseThenRequired("n.active"),
				renders: []catalogRenderingExpectation{
					{language.EuropeanSpanish, "la cláusula WHEN debe tener THEN: n.active"},
					{pseudoTag, "[!! WHEN clause must have THEN: n.active !!]"},
				},
			},
		},
		{
			name: "TestNornicDBLocalizedErrorRendersAtBoundary",
			catalogRenderingCase: catalogRenderingCase{
				Message: NornicDBCoreQueryDimensionMismatchForDatabase("tenant-a", 3, 2),
				renders: []catalogRenderingExpectation{
					{language.EuropeanSpanish, "base de datos \"tenant-a\": las dimensiones del embedding de consulta no coinciden (dimensiones del índice 3, dimensiones de la consulta 2)"},
					{pseudoTag, "[!! database \"tenant-a\": query embedding dimension mismatch (index dims 3, query dims 2) !!]"},
				},
			},
		},
		{
			name: "TestCypherGraphProceduresCatalogRendering",
			catalogRenderingCase: catalogRenderingCase{
				Message: CypherGraphProceduresGraphNotFound("missing", "neo4j"),
				renders: []catalogRenderingExpectation{
					{language.EuropeanSpanish, "El grafo con el nombre `missing` no existe en la base de datos `neo4j`. Podría existir en otra base de datos."},
					{pseudoTag, "[!! Graph with name `missing` does not exist on database `neo4j`. It might exist on another database. !!]"},
				},
			},
		},
		{
			name: "TestQdrantDeletePointFailureCatalog",
			catalogRenderingCase: catalogRenderingCase{
				Message: QdrantDeletePointFailed("point-7"),
				renders: []catalogRenderingExpectation{
					{language.AmericanEnglish, "failed to delete point point-7"},
					{language.EuropeanSpanish, "no se pudo eliminar el punto point-7"},
					{pseudoTag, "[!! failed to delete point point-7 !!]"},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
			require.NoError(t, err)

			for _, expectation := range testCase.renders {
				rendered, tag, err := manager.Render(WithPreferences(context.Background(), expectation.tag), MessageRequest(testCase.Message))
				require.NoError(t, err, testCase.name)
				require.Equal(t, expectation.tag, tag, testCase.name)
				require.Equal(t, expectation.text, rendered, testCase.name)
			}
		})
	}
}

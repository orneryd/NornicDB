package localization

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestCypherGraphProceduresDescriptorsPreserveExactEnglish(t *testing.T) {
	cause := errors.New("forced graph failure")
	testCases := []struct {
		message Message
		text    string
	}{
		{CypherGraphProceduresGraphNameRequired("gds.graph.project"), "graph name required for gds.graph.project"},
		{CypherGraphProceduresStreamNodesFailed(cause), "failed to stream nodes: forced graph failure"},
		{CypherGraphProceduresStreamEdgesFailed(cause), "failed to stream edges: forced graph failure"},
		{CypherGraphProceduresGraphDoesNotExist("missing"), "graph 'missing' does not exist"},
		{CypherGraphProceduresGraphDoesNotExistProjectFirst("missing"), "graph 'missing' does not exist. Create it with gds.graph.project first"},
		{CypherGraphProceduresBuildGraphFailed(cause), "failed to build graph: forced graph failure"},
		{CypherGraphProceduresInvalidProcedureCallSyntax(), "invalid procedure call syntax"},
		{CypherGraphProceduresVariableNotFound("person"), `variable "person" not found in query context (id(person) cannot be resolved)`},
		{CypherGraphProceduresSourceNodeRequired(), "sourceNode parameter required"},
	}

	require.Len(t, testCases, 9)
	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	for _, testCase := range testCases {
		require.Equal(t, testCase.text, testCase.message.Fallback, testCase.message.ID)
		rendered, tag, err := manager.Render(WithPreferences(context.Background(), language.AmericanEnglish), testCase.message)
		require.NoError(t, err, testCase.message.ID)
		require.Equal(t, language.AmericanEnglish, tag, testCase.message.ID)
		require.Equal(t, testCase.text, rendered, testCase.message.ID)
	}
}

func TestCypherGraphProceduresLocalizedErrorsPreserveCauseIdentity(t *testing.T) {
	cause := errors.New("forced graph failure")
	messages := []Message{
		CypherGraphProceduresStreamNodesFailed(cause),
		CypherGraphProceduresStreamEdgesFailed(cause),
		CypherGraphProceduresBuildGraphFailed(cause),
	}

	for _, message := range messages {
		err := NewLocalizedError(string(message.ID), message, cause)
		require.ErrorIs(t, err, cause, message.ID)
		var localizedErr *LocalizedError
		require.ErrorAs(t, err, &localizedErr, message.ID)
		require.Equal(t, message.ID, localizedErr.Message.ID)
		require.Equal(t, cause.Error(), localizedErr.Message.Data["Cause"])
	}
}

func TestCypherGraphProceduresCatalogRendering(t *testing.T) {
	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	message := CypherGraphProceduresGraphDoesNotExistProjectFirst("missing")

	spanish, tag, err := manager.Render(WithPreferences(context.Background(), language.EuropeanSpanish), message)
	require.NoError(t, err)
	require.Equal(t, language.EuropeanSpanish, tag)
	require.Equal(t, "el grafo 'missing' no existe. Créelo primero con gds.graph.project", spanish)

	pseudoTag := language.MustParse("en-XA")
	pseudo, tag, err := manager.Render(WithPreferences(context.Background(), pseudoTag), message)
	require.NoError(t, err)
	require.Equal(t, pseudoTag, tag)
	require.Equal(t, "[!! graph 'missing' does not exist. Create it with gds.graph.project first !!]", pseudo)
}

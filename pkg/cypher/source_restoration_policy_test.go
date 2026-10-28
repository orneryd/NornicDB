package cypher

import (
	"errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSourceRestorationKeepsDiagnosticAndValuePolicies(t *testing.T) {
	_, rewrite := canonicalizeQueryText("MATCH (n) WHERE n.x IS  NULL RETURN n")
	require.NotNil(t, rewrite)
	value, changed := (resultRestoration{source: rewrite}).value("n.x IS NULL", sourceValueText)
	require.True(t, changed)
	require.Equal(t, "n.x IS  NULL", value)
	diagnostic, changed := (resultRestoration{source: rewrite}).value("n.x IS NULL", sourceDiagnosticText)
	require.False(t, changed)
	require.Equal(t, "n.x IS NULL", diagnostic)
	data := map[string]any{"fragment": "n.x IS NULL", "number": int64(9007199254740993)}
	message := localization.Message{ID: "test.source.policy", Fallback: "n.x IS NULL", Data: data}
	restored, changed := (resultRestoration{source: rewrite}).value(message, sourceValueText)
	require.True(t, changed)
	got := restored.(localization.Message)
	require.Equal(t, message.ID, got.ID)
	require.Equal(t, message.Fallback, got.Fallback)
	require.Equal(t, "n.x IS  NULL", got.Data["fragment"])
	require.IsType(t, int64(0), got.Data["number"])
	require.Equal(t, data["number"], got.Data["number"])
	require.Equal(t, "n.x IS NULL", data["fragment"], "cached input remains immutable")
	sequence := localization.MessageSequence{Messages: []localization.Message{message}}
	restored, changed = (resultRestoration{source: rewrite}).value(sequence, sourceValueText)
	require.True(t, changed)
	require.Equal(t, got, restored.(localization.MessageSequence).Messages[0])
	require.Equal(t, "n.x IS NULL", sequence.Messages[0].Data["fragment"])
}

func TestSourceRestorationParserUnicodeCRLF(t *testing.T) {
	query := "RETURN 'π'\r\n  @"
	canonical, rewrite := canonicalizeQueryText(query)
	require.NotNil(t, rewrite)
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	cause := executor.validateSyntax(t.Context(), canonical, false, syntaxANTLRCore)
	require.Error(t, cause)
	_, err := (resultRestoration{source: rewrite}).restore(nil, cause, nil)
	require.ErrorIs(t, err, cause)
	var diagnostic *localization.LocalizedError
	require.True(t, errors.As(err, &diagnostic))
	require.Equal(t, localization.MessageCypherANTLRValidateSyntaxError, diagnostic.Message.ID)
	sequence := diagnostic.Message.Data["Detail"].(localization.MessageSequence)
	require.NotEmpty(t, sequence.Messages)
	for _, message := range sequence.Messages {
		require.Equal(t, localization.MessageCypherANTLRUnexpectedInput, message.ID)
		require.Equal(t, 2, message.Data["Line"])
		require.Equal(t, 2, message.Data["Column"])
	}
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", errorStatusCode(err))
}

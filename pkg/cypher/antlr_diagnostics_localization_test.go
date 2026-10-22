package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestANTLRDiagnosticsRetainLocalizedIdentityThroughExecutor(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	query := "MATCH (n) WHERE n.password = 'private-diagnostic-literal' RETURN n @"
	err = exec.validateSyntaxANTLR(context.Background(), query, false)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", errorStatusCode(err))
	var typed *localization.LocalizedError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, localization.MessageCypherANTLRValidateSyntaxError, typed.Message.ID)
	for locale, diagnostic := range map[string]string{"en-US": "line ", "es-ES": "línea ", "en-XA": "[!! line "} {
		text, _, renderErr := typed.Render(localization.WithPreferences(context.Background(), language.MustParse(locale)), manager)
		require.NoError(t, renderErr)
		require.Contains(t, text, diagnostic)
		require.NotContains(t, text, "private-diagnostic-literal")
		if locale == "es-ES" {
			require.NotContains(t, text, "unexpected input")
		}
	}
}

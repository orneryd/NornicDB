package antlr

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func requireANTLRLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	require.Nil(t, localizedErr.Cause)
	require.False(t, errors.Is(err, errors.New(text)))
	return localizedErr
}

func TestParseEntrypointErrorsHaveLocalizedIdentity(t *testing.T) {
	t.Run("Parse empty query", func(t *testing.T) {
		result, err := Parse(" \t\n")
		require.Nil(t, result)
		requireANTLRLocalizedError(t, err, localization.MessageCypherANTLRParseEmptyQuery, "empty query")
	})

	t.Run("Parse syntax error", func(t *testing.T) {
		result, err := Parse("MATCH (")
		require.NotNil(t, result)
		require.NotEmpty(t, result.Errors)
		localizedErr := requireANTLRLocalizedError(t, err, localization.MessageCypherANTLRParseSyntaxError, "syntax error: "+result.Errors[0])
		require.Equal(t, result.Errors[0], localizedErr.Message.Data["Detail"])
	})

	t.Run("Validate empty query", func(t *testing.T) {
		err := Validate(" \t\n")
		requireANTLRLocalizedError(t, err, localization.MessageCypherANTLRValidateEmptyQuery, "empty query")
	})

	t.Run("Validate syntax error", func(t *testing.T) {
		err := Validate("MATCH (")
		localizedErr := requireANTLRLocalizedError(t, err, localization.MessageCypherANTLRValidateSyntaxError, err.Error())
		detail, ok := localizedErr.Message.Data["Detail"].(string)
		require.True(t, ok)
		require.Equal(t, "syntax error: "+detail, err.Error())
	})
}

func TestParseEntrypointCatalogRendering(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	message := localization.CypherANTLRParseSyntaxError("line 1:7 unexpected token")

	english, tag, err := manager.Render(localization.WithPreferences(context.Background(), language.AmericanEnglish), message)
	require.NoError(t, err)
	require.Equal(t, language.AmericanEnglish, tag)
	require.Equal(t, "syntax error: line 1:7 unexpected token", english)

	spanish, tag, err := manager.Render(localization.WithPreferences(context.Background(), language.EuropeanSpanish), message)
	require.NoError(t, err)
	require.Equal(t, language.EuropeanSpanish, tag)
	require.Equal(t, "error de sintaxis: line 1:7 unexpected token", spanish)

	pseudoTag := language.MustParse("en-XA")
	pseudo, tag, err := manager.Render(localization.WithPreferences(context.Background(), pseudoTag), message)
	require.NoError(t, err)
	require.Equal(t, pseudoTag, tag)
	require.Equal(t, "[!! syntax error: line 1:7 unexpected token !!]", pseudo)
}

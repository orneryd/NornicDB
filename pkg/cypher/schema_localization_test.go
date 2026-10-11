package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func requireCypherSchemaLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	return localizedErr
}

func TestCypherSchemaParserErrorsHaveTypedIdentity(t *testing.T) {
	message := localization.CypherSchemaCompositeDDLNotAllowed()
	require.Equal(t, "Neo.ClientError.Statement.NotAllowed: Schema DDL on composite databases requires a constituent target. Use USE <composite>.<alias> to target a specific constituent", message.Fallback)
	require.Equal(t, "Neo.ClientError.Statement.NotAllowed", message.Data["Code"])
}

func TestCypherSchemaCatalogRendering(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	message := localization.CypherSchemaRangeIndexSinglePropertyRequired(2)

	spanish, tag, err := manager.Render(localization.WithPreferences(context.Background(), language.EuropeanSpanish), message)
	require.NoError(t, err)
	require.Equal(t, language.EuropeanSpanish, tag)
	require.Equal(t, "RANGE INDEX solo admite una propiedad; se obtuvieron 2", spanish)

	pseudoTag := language.MustParse("en-XA")
	pseudo, tag, err := manager.Render(localization.WithPreferences(context.Background(), pseudoTag), message)
	require.NoError(t, err)
	require.Equal(t, pseudoTag, tag)
	require.Equal(t, "[!! RANGE INDEX only supports single property, got 2 !!]", pseudo)
}

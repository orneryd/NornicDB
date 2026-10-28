package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
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

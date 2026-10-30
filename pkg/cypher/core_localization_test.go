package cypher

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

func requireCypherCoreLocalizedError(t *testing.T, err error, id localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, id, localizedErr.Message.ID)
	require.Equal(t, string(id), localizedErr.Code)
	return localizedErr
}

func TestCypherCoreRepresentativeErrorsHaveTypedIdentityAndExactEnglish(t *testing.T) {
	t.Run("parser", func(t *testing.T) {
		_, err := NewParser().Parse("")
		requireCypherCoreLocalizedError(t, err, "cyphercore.empty_query", "empty query")
	})

	t.Run("case expression", func(t *testing.T) {
		_, err := parseCaseExpression("CASE END")
		requireCypherCoreLocalizedError(t, err, "cyphercore.case_when_required", "CASE expression must have at least one WHEN clause")
	})

	t.Run("fulltext", func(t *testing.T) {
		_, err := ParseFulltextQuery("alpha^")
		requireCypherCoreLocalizedError(t, err, "cyphercore.fulltext_number_after_boost_expected", "query cannot be parsed: expected number after ^")
	})

	t.Run("typed result", func(t *testing.T) {
		err := decodeRow([]string{"value"}, []interface{}{1}, nil)
		requireCypherCoreLocalizedError(t, err, "cyphercore.typed_destination_pointer_required", "dest must be a non-nil pointer")
	})

	t.Run("embedding", func(t *testing.T) {
		_, err := embedQueryChunked(context.Background(), nil, "query")
		requireCypherCoreLocalizedError(t, err, "cyphercore.embedder_not_configured", "no embedder configured")
	})
}

func TestCypherCoreLocalizedErrorsPreserveCauses(t *testing.T) {
	t.Run("typed time parse", func(t *testing.T) {
		field := reflect.New(reflect.TypeOf(time.Time{})).Elem()
		err := assignValue(field, "not-a-time")
		requireCypherCoreLocalizedError(t, err, "cyphercore.typed_time_parse_failed", "cannot parse time: not-a-time")

		var parseErr *time.ParseError
		require.ErrorAs(t, err, &parseErr)
	})

	t.Run("parse wrapper", func(t *testing.T) {
		_, err := NewExecutor().ParseAndValidate(context.Background(), "'unterminated", nil)
		localizedErr := requireCypherCoreLocalizedError(t, err, "cyphercore.parse_failed", "parse error: unterminated string literal")
		var cause *localization.LocalizedError
		require.ErrorAs(t, localizedErr.Cause, &cause)
		require.Equal(t, localization.MessageID("cyphercore.unterminated_string_literal"), cause.Message.ID)
		require.True(t, errors.Is(err, cause))
	})
}

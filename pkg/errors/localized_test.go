package errors

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestLocalizedErrorPreservesIdentityAndRendersAtBoundary(t *testing.T) {
	cause := stderrors.New("storage unavailable")
	err := NewLocalized("Neo.ClientError.Database.DatabaseNotFound", localization.DatabaseNotFound("analytics"), cause)

	require.Equal(t, "Database 'analytics' does not exist", err.Error())
	require.ErrorIs(t, err, cause)
	var typed *Localized
	require.ErrorAs(t, err, &typed)
	require.Equal(t, "Neo.ClientError.Database.DatabaseNotFound", typed.Code)

	manager, managerErr := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, managerErr)
	text, tag, renderErr := err.Render(localization.WithPreferences(context.Background(), language.EuropeanSpanish), manager)
	require.NoError(t, renderErr)
	require.Equal(t, language.EuropeanSpanish, tag)
	require.Equal(t, "La base de datos 'analytics' no existe", text)
}

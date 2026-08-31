package bolt

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestSessionLocalizesRunFailuresWithoutChangingFallback(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)

	spanish := &Session{
		server:   &Server{config: &Config{Localizer: manager}},
		language: language.EuropeanSpanish,
	}
	require.Equal(t, "Sin autenticar", spanish.localize(localization.NotAuthenticated()))
	require.Equal(t, "La base de datos 'analytics' no existe", spanish.localize(localization.DatabaseNotFound("analytics")))

	fallback := (&Session{}).localize(localization.WritePermissionRequired())
	require.Equal(t, "Write operations require write permission", fallback)
}

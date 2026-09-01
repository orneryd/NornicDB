package bolt

import (
	"errors"
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

func TestSessionLocalizesHelloAuthenticationMessagesFromProcessDefault(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.EuropeanSpanish}, nil)
	require.NoError(t, err)
	session := &Session{server: &Server{config: &Config{Localizer: manager}}, language: language.Und}

	require.Equal(t, "Se requiere autenticación", session.localize(localization.BoltAuthenticationRequired()))
	require.Equal(t, "Credenciales no válidas", session.localize(localization.BoltInvalidCredentials()))
	require.Equal(t, "Token no válido o caducado", session.localize(localization.BoltInvalidOrExpiredToken()))
	require.Equal(t, "Se requiere autenticación, pero no está configurada", session.localize(localization.BoltAuthenticationNotConfigured()))
	require.Equal(t, "Esquema de autenticación no compatible: custom", session.localize(localization.BoltUnsupportedAuthScheme("custom")))
	require.Equal(t, "La base de datos 'analytics' no existe", session.localize(localization.DatabaseNotFound("analytics")))
	require.Equal(t, "No se encontró la base de datos 'analytics': unavailable", session.localize(localization.BoltDatabaseNotFoundWithCause("analytics", errors.New("unavailable"))))
	require.Equal(t, "No hay ninguna transacción que confirmar", session.localize(localization.BoltNoTransactionToCommit()))
	require.Equal(t, "Access to database 'analytics' is not allowed.", localization.DatabaseAccessDenied("analytics").Fallback)
	require.Equal(t, "Write on database 'analytics' is not allowed.", localization.DatabaseWriteDenied("analytics").Fallback)
	require.Equal(t, "Authentication required", localization.BoltAuthenticationRequired().Fallback)
	require.Equal(t, "Invalid credentials", localization.BoltInvalidCredentials().Fallback)
	require.Equal(t, "Invalid or expired token", localization.BoltInvalidOrExpiredToken().Fallback)
	require.Equal(t, "Authentication required but not configured", localization.BoltAuthenticationNotConfigured().Fallback)
	require.Equal(t, "Unsupported auth scheme: custom", localization.BoltUnsupportedAuthScheme("custom").Fallback)
	require.Equal(t, "Database 'analytics' not found: unavailable", localization.BoltDatabaseNotFoundWithCause("analytics", errors.New("unavailable")).Fallback)
	require.Equal(t, "No transaction to commit", localization.BoltNoTransactionToCommit().Fallback)
}

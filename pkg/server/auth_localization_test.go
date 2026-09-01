package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestLocalizedAuthFlowResponsesPreserveContracts(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	tests := []struct {
		name     string
		status   int
		message  localization.Message
		spanish  string
		fallback string
	}{
		{name: "grant", status: http.StatusBadRequest, message: localization.UnsupportedGrantType(), spanish: "grant_type no compatible", fallback: "unsupported grant_type"},
		{name: "admin", status: http.StatusForbidden, message: localization.APITokenAdminRequired(), spanish: "se requiere el rol de administrador para generar tokens de API", fallback: "admin role required to generate API tokens"},
		{name: "expires", status: http.StatusBadRequest, message: localization.InvalidExpiresIn(), spanish: "formato de expires_in no válido", fallback: "invalid expires_in format"},
		{name: "expires help", status: http.StatusBadRequest, message: localization.InvalidExpiresInWithHelp(), spanish: "formato de expires_in no válido (use: 1h, 24h, 7d, 365d, 0 para nunca)", fallback: "invalid expires_in format (use: 1h, 24h, 7d, 365d, 0 for never)"},
		{name: "token", status: http.StatusInternalServerError, message: localization.APITokenGenerationFailed(), spanish: "no se pudo generar el token", fallback: "failed to generate token"},
		{name: "OAuth", status: http.StatusBadRequest, message: localization.OAuthCallbackFailed("access_denied", "provider offline"), spanish: "Error de OAuth: access_denied - provider offline", fallback: "OAuth error: access_denied - provider offline"},
		{name: "code", status: http.StatusBadRequest, message: localization.MissingAuthorizationCode(), spanish: "falta el código de autorización", fallback: "missing authorization code"},
		{name: "state", status: http.StatusBadRequest, message: localization.MissingStateParameter(), spanish: "falta el parámetro state", fallback: "missing state parameter"},
		{name: "context", status: http.StatusUnauthorized, message: localization.NoUserContext(), spanish: "falta el contexto del usuario", fallback: "no user context"},
		{name: "password", status: http.StatusUnauthorized, message: localization.OldPasswordIncorrect(), spanish: "la contraseña anterior es incorrecta", fallback: "old password incorrect"},
		{name: "PUT", status: http.StatusMethodNotAllowed, message: localization.PutRequired(), spanish: "se requiere PUT", fallback: "PUT required"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &Server{localizer: manager}
			request := httptest.NewRequest(http.MethodPost, "/", nil)
			request.Header.Set("Accept-Language", "es-ES")
			response := httptest.NewRecorder()
			server.localizationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				server.writeLocalizedError(w, r, test.status, test.message, ErrBadRequest)
			})).ServeHTTP(response, request)

			require.Equal(t, test.status, response.Code)
			var body map[string]any
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			require.Equal(t, test.spanish, body["message"])
			require.Equal(t, test.fallback, test.message.Fallback)
		})
	}
}

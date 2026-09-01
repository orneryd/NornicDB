package heimdall

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestLocalizedHTTPErrorsPreservePlainTextContract(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	handler := NewHandler(&Manager{}, Config{Enabled: true}, nil, nil)
	handler.SetLocalizer(manager)
	tests := []struct {
		name     string
		language string
		message  localization.Message
		status   int
		body     string
		tag      string
	}{
		{name: "method", language: "es-ES", message: localization.HeimdallMethodNotAllowed(), status: http.StatusMethodNotAllowed, body: "Método no permitido\n", tag: "es-ES"},
		{name: "body", language: "es-ES", message: localization.HeimdallInvalidRequestBody(), status: http.StatusBadRequest, body: "Cuerpo de solicitud no válido\n", tag: "es-ES"},
		{name: "streaming", language: "es-ES", message: localization.HeimdallStreamingNotSupported(), status: http.StatusInternalServerError, body: "Transmisión no compatible\n", tag: "es-ES"},
		{name: "malformed language", language: "not_a_locale_@", message: localization.HeimdallMethodNotAllowed(), status: http.StatusMethodNotAllowed, body: "Method not allowed\n", tag: "en-US"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.Header.Set("Accept-Language", test.language)
			response := httptest.NewRecorder()

			handler.writeLocalizedError(response, request, test.message, test.status)

			require.Equal(t, test.status, response.Code)
			require.Equal(t, test.body, response.Body.String())
			require.Equal(t, test.tag, response.Header().Get("Content-Language"))
			require.Contains(t, response.Header().Values("Vary"), "Accept-Language")
		})
	}
}

func TestLocalizedOwnedErrorsPreserveStatusAndCauses(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	handler := NewHandler(&Manager{}, Config{Enabled: true}, nil, nil)
	handler.SetLocalizer(manager)
	cause := errors.New("model offline")
	tests := []struct {
		message localization.Message
		status  int
		body    string
	}{
		{message: localization.HeimdallBifrostNotEnabled(), status: http.StatusServiceUnavailable, body: "Bifrost no está habilitado\n"},
		{message: localization.HeimdallQueryParameterRequired(), status: http.StatusBadRequest, body: "se requiere el parámetro query\n"},
		{message: localization.HeimdallAutocompleteFailed(cause), status: http.StatusInternalServerError, body: "Error de autocompletado: model offline\n"},
		{message: localization.HeimdallGenerationFailed(cause), status: http.StatusInternalServerError, body: "Error de generación: model offline\n"},
	}
	for _, test := range tests {
		request := httptest.NewRequest(http.MethodPost, "/", nil)
		request.Header.Set("Accept-Language", "es-ES")
		response := httptest.NewRecorder()

		handler.writeLocalizedError(response, request, test.message, test.status)

		require.Equal(t, test.status, response.Code)
		require.Equal(t, test.body, response.Body.String())
	}
	require.Equal(t, "Autocomplete error: model offline", localization.HeimdallAutocompleteFailed(cause).Fallback)
	require.Equal(t, "Generation error: model offline", localization.HeimdallGenerationFailed(cause).Fallback)
}

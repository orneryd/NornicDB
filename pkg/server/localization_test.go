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

func TestLocalizedInvalidRequestBody(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	server := &Server{localizer: manager}
	handler := server.localizationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		server.writeInvalidRequestBody(w, r)
	}))

	tests := []struct {
		name     string
		language string
		message  string
		tag      string
	}{
		{name: "source fallback", message: "invalid request body", tag: "en-US"},
		{name: "Spanish", language: "es-ES, en;q=0.5", message: "cuerpo de solicitud no válido", tag: "es-ES"},
		{name: "malformed header", language: "not_a_locale_@", message: "invalid request body", tag: "en-US"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/", nil)
			request.Header.Set("Accept-Language", test.language)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			require.Equal(t, http.StatusBadRequest, response.Code)
			require.Equal(t, test.tag, response.Header().Get("Content-Language"))
			require.Contains(t, response.Header().Values("Vary"), "Accept-Language")
			var body map[string]any
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			require.Equal(t, true, body["error"])
			require.Equal(t, test.message, body["message"])
			require.Equal(t, float64(http.StatusBadRequest), body["code"])
		})
	}
}

func TestLocalizedPostRequiredPreservesNeo4jCode(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	server := &Server{localizer: manager}
	handler := server.localizationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		server.writeNeo4jPostRequired(w, r, "Neo.ClientError.Request.Invalid")
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Accept-Language", "es-ES")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusMethodNotAllowed, response.Code)
	require.Equal(t, "es-ES", response.Header().Get("Content-Language"))
	require.Contains(t, response.Header().Values("Vary"), "Accept-Language")
	var body TransactionResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Len(t, body.Errors, 1)
	require.Equal(t, "Neo.ClientError.Request.Invalid", body.Errors[0].Code)
	require.Equal(t, "se requiere POST", body.Errors[0].Message)
}

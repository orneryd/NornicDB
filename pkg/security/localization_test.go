package security

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestSecurityMiddlewareLocalizesErrorsAndPreservesDiagnostics(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	middleware := NewSecurityMiddleware()
	middleware.SetLocalizer(manager)
	handler := middleware.ValidateRequest(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	tests := []struct {
		language string
		body     string
		tag      string
	}{
		{language: "es-ES", body: "Encabezado X-Test no válido: header value contains invalid control characters\n", tag: "es-ES"},
		{language: "en-XA", body: "[!! Invalid header X-Test: header value contains invalid control characters !!]\n", tag: "en-XA"},
	}
	for _, test := range tests {
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		request.Header["X-Test"] = []string{"invalid\nvalue"}
		request.Header.Set("Accept-Language", test.language)
		response := httptest.NewRecorder()

		handler.ServeHTTP(response, request)

		require.Equal(t, http.StatusBadRequest, response.Code)
		require.Equal(t, test.body, response.Body.String())
		require.Equal(t, test.tag, response.Header().Get("Content-Language"))
	}
}

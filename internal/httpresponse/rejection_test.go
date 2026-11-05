package httpresponse

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestLocalizedRejectionWireContract(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	for _, test := range []struct {
		name                      string
		manager                   *localization.Manager
		message                   localization.Message
		header                    string
		contextTag                language.Tag
		status                    int
		plain, jsonText, language string
	}{
		{"nil manager keeps literal", nil, localization.InvalidRequestBody(), "es-ES", language.EuropeanSpanish, 400, "invalid request body\n", "{\"error\":\"invalid request body\"}\n", "en-US"},
		{"nil empty fallback stays empty", nil, localization.Message{ID: "not.a.fallback"}, "es-ES", language.EuropeanSpanish, 401, "\n", "{\"error\":\"\"}\n", "en-US"},
		{"valid header selects Spanish", manager, localization.InvalidRequestBody(), "es-ES", language.AmericanEnglish, 400, "cuerpo de solicitud no válido\n", "{\"error\":\"cuerpo de solicitud no válido\"}\n", "es-ES"},
		{"invalid header retains context", manager, localization.InvalidRequestBody(), "?", language.EuropeanSpanish, 401, "cuerpo de solicitud no válido\n", "{\"error\":\"cuerpo de solicitud no válido\"}\n", "es-ES"},
		{"valid header overrides context", manager, localization.InvalidRequestBody(), "en-US", language.EuropeanSpanish, 400, "invalid request body\n", "{\"error\":\"invalid request body\"}\n", "en-US"},
		{"render failure keeps raw fallback", manager, localization.Message{ID: "missing.boundary", Fallback: "literal \"detail\""}, "es-ES", language.EuropeanSpanish, 429, "literal \"detail\"\n", "{\"error\":\"literal \\\"detail\\\"\"}\n", "en-US"},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, envelope := range []RejectionEnvelope{PlainText, JSONErrorText} {
				request := httptest.NewRequest(http.MethodPost, "/", nil)
				request.Header.Set("Accept-Language", test.header)
				request = request.WithContext(localization.WithPreferences(t.Context(), test.contextTag))
				response := httptest.NewRecorder()
				response.Header().Add("Vary", "Origin")
				WriteLocalizedRejection(response, request, test.manager, test.message, test.status, envelope)
				want := test.plain
				if envelope == JSONErrorText {
					want = test.jsonText
				}
				require.Equal(t, test.status, response.Code)
				require.Equal(t, want, response.Body.String())
				require.Equal(t, "text/plain; charset=utf-8", response.Header().Get("Content-Type"))
				require.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
				require.Equal(t, test.language, response.Header().Get("Content-Language"))
				require.Equal(t, []string{"Origin", "Accept-Language"}, response.Header().Values("Vary"))
			}
		})
	}
}

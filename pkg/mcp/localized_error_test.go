package mcp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestRenderErrorLocalizesTypedErrorsAndPreservesCause(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	server := NewServer(nil, &ServerConfig{Localizer: manager})
	cause := errors.New("storage offline")
	typed := nornicerrors.NewLocalized("mcp.test", localization.QueryChunkFailed(cause), cause)
	ctx := localization.WithPreferences(context.Background(), language.EuropeanSpanish)

	require.Equal(t, "no se pudo dividir la consulta: storage offline", server.renderError(ctx, typed))
	require.True(t, errors.Is(typed, cause))
	require.Equal(t, "failed to chunk query: storage offline", typed.Error())
	require.Equal(t, "ordinary failure", server.renderError(ctx, errors.New("ordinary failure")))
}

func TestAuthMiddlewareLocalizesJSONErrorContract(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	middleware := NewAuthMiddleware(nil, DefaultAuthConfig())
	middleware.SetLocalizer(manager)
	handler := middleware.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for _, test := range []struct {
		language string
		body     string
		tag      string
	}{
		{language: "es-ES", body: "{\"error\":\"se requiere autenticación\"}\n", tag: "es-ES"},
		{language: "en-XA", body: "{\"error\":\"[!! authentication required !!]\"}\n", tag: "en-XA"},
	} {
		request := httptest.NewRequest(http.MethodPost, "/mcp", nil)
		request.Header.Set("Accept-Language", test.language)
		response := httptest.NewRecorder()

		handler.ServeHTTP(response, request)

		require.Equal(t, http.StatusUnauthorized, response.Code)
		require.Equal(t, test.body, response.Body.String())
		require.Equal(t, test.tag, response.Header().Get("Content-Language"))
	}
}

package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestMCPRenderRequestPublicTypedToolErrors(t *testing.T) {
	for _, test := range []struct {
		name, locale, text string
		localize           bool
	}{
		{"source English without manager", "es-ES", "unknown tool: render-fixture-missing", false},
		{"English catalog", "en-US", "unknown tool: render-fixture-missing", true},
		{"Spanish catalog", "es-ES", "herramienta desconocida: render-fixture-missing", true},
		{"pseudo catalog", "en-XA", "[!! unknown tool: render-fixture-missing !!]", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := DefaultServerConfig()
			if test.localize {
				manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
				require.NoError(t, err)
				config.Localizer = manager
			}
			server := NewServer(nil, config)
			t.Cleanup(func() { require.NoError(t, server.Stop(context.Background())) })
			request := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewBufferString(`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"render-fixture-missing","arguments":{}}}`))
			request.Header.Set("Accept-Language", test.locale)
			response := httptest.NewRecorder()
			server.ServeHTTP(response, request)
			require.Equal(t, http.StatusOK, response.Code)
			var reply struct {
				JSONRPC string           `json:"jsonrpc"`
				ID      int              `json:"id"`
				Error   json.RawMessage  `json:"error"`
				Result  CallToolResponse `json:"result"`
			}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &reply))
			require.Equal(t, "2.0", reply.JSONRPC)
			require.Equal(t, 7, reply.ID)
			require.Empty(t, reply.Error)
			require.True(t, reply.Result.IsError)
			require.Equal(t, []Content{{Type: "text", Text: test.text}}, reply.Result.Content)
		})
	}
}

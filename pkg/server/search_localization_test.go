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

func TestLocalizedSearchBoundaryResponsesPreserveContracts(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	t.Run("auto embed Neo4j", func(t *testing.T) {
		server := &Server{localizer: manager}
		request := httptest.NewRequest(http.MethodPost, "/", nil)
		request.Header.Set("Accept-Language", "es-ES")
		response := httptest.NewRecorder()
		server.localizationMiddleware(http.HandlerFunc(server.writeNeo4jAutoEmbedNotEnabled)).ServeHTTP(response, request)

		require.Equal(t, http.StatusServiceUnavailable, response.Code)
		var body TransactionResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		require.Equal(t, "Neo.DatabaseError.General.UnknownError", body.Errors[0].Code)
		require.Equal(t, "El embedding automático no está habilitado", body.Errors[0].Message)
		require.Equal(t, "Auto-embed not enabled", localization.AutoEmbedNotEnabled().Fallback)
	})

	tests := []struct {
		name       string
		status     int
		descriptor localization.Message
		message    string
		fallback   string
		write      func(*Server, http.ResponseWriter, *http.Request)
	}{
		{name: "service", status: http.StatusServiceUnavailable, descriptor: localization.SearchServiceUnavailable(), message: "servicio de búsqueda no disponible", fallback: "search service unavailable", write: (*Server).writeSearchServiceUnavailable},
		{name: "chunking", status: http.StatusBadRequest, descriptor: localization.QueryChunkingFailed(), message: "no se pudo dividir la consulta", fallback: "failed to chunk query", write: (*Server).writeQueryChunkingFailed},
		{name: "embedding", status: http.StatusBadRequest, descriptor: localization.NodeHasNoEmbedding(), message: "El nodo no tiene embedding", fallback: "Node has no embedding", write: (*Server).writeNodeHasNoEmbedding},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &Server{localizer: manager}
			request := httptest.NewRequest(http.MethodPost, "/", nil)
			request.Header.Set("Accept-Language", "es-ES")
			response := httptest.NewRecorder()
			server.localizationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				test.write(server, w, r)
			})).ServeHTTP(response, request)

			require.Equal(t, test.status, response.Code)
			var body map[string]any
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			require.Equal(t, test.message, body["message"])
			require.Equal(t, test.fallback, test.descriptor.Fallback)
		})
	}
}

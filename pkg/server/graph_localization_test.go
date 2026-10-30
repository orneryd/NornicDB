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

func TestLocalizedGraphResponsesPreserveContracts(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	tests := []struct {
		name     string
		message  localization.Message
		spanish  string
		fallback string
	}{
		{name: "neighborhood", message: localization.HistoricalNeighborhoodRoute(), spanish: "el recorrido histórico del vecindario está disponible mediante /nornicdb/graph/{database}/temporal o /nornicdb/graph/{database}/diff", fallback: "historical neighborhood traversal is exposed via /nornicdb/graph/{database}/temporal or /nornicdb/graph/{database}/diff"},
		{name: "node ids", message: localization.PathNodeIDsRequired(), spanish: "se requieren source_node_id y target_node_id", fallback: "source_node_id and target_node_id are required"},
		{name: "path", message: localization.HistoricalPathRoute(), spanish: "el recorrido histórico de rutas aún no está disponible en /nornicdb/graph/{database}/path; use /nornicdb/graph/{database}/temporal para reconstruir instantáneas", fallback: "historical path traversal is not yet exposed on /nornicdb/graph/{database}/path; use /nornicdb/graph/{database}/temporal for snapshot reconstruction"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &Server{localizer: manager}
			request := httptest.NewRequest(http.MethodPost, "/", nil)
			request.Header.Set("Accept-Language", "es-ES")
			response := httptest.NewRecorder()
			server.localizationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				server.writeLocalizedError(w, r, http.StatusBadRequest, test.message, ErrBadRequest)
			})).ServeHTTP(response, request)

			require.Equal(t, http.StatusBadRequest, response.Code)
			var body map[string]any
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			require.Equal(t, test.spanish, body["message"])
			require.Equal(t, test.fallback, test.message.Fallback)
		})
	}

	server := &Server{localizer: manager}
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Accept-Language", "es-ES")
	response := httptest.NewRecorder()
	server.localizationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		server.writeLocalizedNeo4jError(w, r, http.StatusForbidden, "Neo.ClientError.Security.Forbidden", localization.GraphDatabaseAccessDenied())
	})).ServeHTTP(response, request)
	var body TransactionResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", body.Errors[0].Code)
	require.Equal(t, "No se permite el acceso a la base de datos solicitada.", body.Errors[0].Message)
}

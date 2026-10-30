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

func TestLocalizedDatabaseConfigResponsesPreserveNeo4jContracts(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	tests := []struct {
		name     string
		status   int
		code     string
		message  localization.Message
		spanish  string
		fallback string
	}{
		{name: "system", status: http.StatusBadRequest, code: "Neo.ClientError.General.BadRequest", message: localization.SystemDatabaseOverridesUnsupported(), spanish: "la base de datos del sistema no puede tener anulaciones de configuración", fallback: "system database cannot have config overrides"},
		{name: "store", status: http.StatusServiceUnavailable, code: "Neo.ClientError.General.Unavailable", message: localization.DatabaseConfigUnavailable(), spanish: "la configuración por base de datos no está disponible (la base de datos del sistema no está disponible)", fallback: "per-database config not available (system DB unavailable)"},
		{name: "manager", status: http.StatusServiceUnavailable, code: "Neo.ClientError.General.Unavailable", message: localization.DatabaseManagerUnavailable(), spanish: "administrador de bases de datos no disponible", fallback: "database manager unavailable"},
		{name: "composite", status: http.StatusBadRequest, code: "Neo.ClientError.Statement.NotSupported", message: localization.MVCCCompositeUnsupported(), spanish: "los controles del ciclo de vida de mvcc no son compatibles con bases de datos compuestas", fallback: "mvcc lifecycle controls are not supported for composite databases"},
		{name: "schedule", status: http.StatusBadRequest, code: "Neo.ClientError.Statement.NotSupported", message: localization.MVCCScheduleUnsupported(), spanish: "el control de programación del ciclo de vida de mvcc no es compatible con esta base de datos", fallback: "mvcc lifecycle schedule control is not supported for this database"},
		{name: "interval", status: http.StatusBadRequest, code: "Neo.ClientError.General.BadRequest", message: localization.InvalidInterval(), spanish: "intervalo no válido", fallback: "invalid interval"},
		{name: "debt", status: http.StatusBadRequest, code: "Neo.ClientError.Statement.NotSupported", message: localization.MVCCDebtUnsupported(), spanish: "la inspección de deuda del ciclo de vida de mvcc no es compatible con esta base de datos", fallback: "mvcc lifecycle debt inspection is not supported for this database"},
		{name: "limit", status: http.StatusBadRequest, code: "Neo.ClientError.General.BadRequest", message: localization.InvalidLimit(), spanish: "límite no válido", fallback: "invalid limit"},
		{name: "key", status: http.StatusBadRequest, code: "Neo.ClientError.General.BadRequest", message: localization.DisallowedOrUnknownConfigKey("search.vector.enabled"), spanish: "clave no permitida o desconocida: search.vector.enabled", fallback: "disallowed or unknown key: search.vector.enabled"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &Server{localizer: manager}
			request := httptest.NewRequest(http.MethodPut, "/", nil)
			request.Header.Set("Accept-Language", "es-ES")
			response := httptest.NewRecorder()
			server.localizationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				server.writeLocalizedNeo4jError(w, r, test.status, test.code, test.message)
			})).ServeHTTP(response, request)

			require.Equal(t, test.status, response.Code)
			var body TransactionResponse
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			require.Equal(t, test.code, body.Errors[0].Code)
			require.Equal(t, test.spanish, body.Errors[0].Message)
			require.Equal(t, test.fallback, test.message.Fallback)
		})
	}
}

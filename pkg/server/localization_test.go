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

func TestLocalizedNeo4jInvalidRequestBodyPreservesCode(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	server := &Server{localizer: manager}
	handler := server.localizationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		server.writeNeo4jInvalidRequestBody(w, r, "Neo.ClientError.Request.InvalidFormat")
	}))
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Header.Set("Accept-Language", "es-ES")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusBadRequest, response.Code)
	var body TransactionResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Len(t, body.Errors, 1)
	require.Equal(t, "Neo.ClientError.Request.InvalidFormat", body.Errors[0].Code)
	require.Equal(t, "cuerpo de solicitud no válido", body.Errors[0].Message)
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

func TestLocalizedGetRequiredPreservesContracts(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	t.Run("HTTP", func(t *testing.T) {
		server := &Server{localizer: manager}
		request := httptest.NewRequest(http.MethodPost, "/", nil)
		request.Header.Set("Accept-Language", "es-ES")
		response := httptest.NewRecorder()
		server.localizationMiddleware(http.HandlerFunc(server.writeGetRequired)).ServeHTTP(response, request)
		require.Equal(t, http.StatusMethodNotAllowed, response.Code)
		require.Contains(t, response.Body.String(), "se requiere GET")
	})
	t.Run("Neo4j", func(t *testing.T) {
		server := &Server{localizer: manager}
		request := httptest.NewRequest(http.MethodPost, "/", nil)
		request.Header.Set("Accept-Language", "es-ES")
		response := httptest.NewRecorder()
		server.localizationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			server.writeNeo4jGetRequired(w, r, "Neo.ClientError.General.BadRequest")
		})).ServeHTTP(response, request)
		var body TransactionResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		require.Equal(t, "Neo.ClientError.General.BadRequest", body.Errors[0].Code)
		require.Equal(t, "se requiere GET", body.Errors[0].Message)
	})
}

func TestLocalizedDatabaseAccessDeniedPreservesNeo4jContract(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	server := &Server{localizer: manager}
	handler := server.localizationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		server.writeNeo4jDatabaseAccessDenied(w, r, "tenant-a")
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Accept-Language", "es-ES")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusForbidden, response.Code)
	require.Equal(t, "es-ES", response.Header().Get("Content-Language"))
	var body TransactionResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Len(t, body.Errors, 1)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", body.Errors[0].Code)
	require.Equal(t, "No se permite el acceso a la base de datos 'tenant-a'.", body.Errors[0].Message)
	require.Equal(t, "Access to database 'tenant-a' is not allowed.", localization.DatabaseAccessDenied("tenant-a").Fallback)
}

func TestLocalizedDatabaseWriteDeniedPreservesNeo4jContract(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	server := &Server{localizer: manager}
	handler := server.localizationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		server.writeNeo4jDatabaseWriteDenied(w, r, "tenant-a")
	}))
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Header.Set("Accept-Language", "es-ES")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusForbidden, response.Code)
	var body TransactionResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Len(t, body.Errors, 1)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", body.Errors[0].Code)
	require.Equal(t, "No se permite escribir en la base de datos 'tenant-a'.", body.Errors[0].Message)
	require.Equal(t, "Write on database 'tenant-a' is not allowed.", localization.DatabaseWriteDenied("tenant-a").Fallback)
}

func TestLocalizedHTTPAuthenticationResponsesPreserveStatus(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	tests := []struct {
		name    string
		status  int
		message string
		write   func(*Server, http.ResponseWriter, *http.Request)
	}{
		{name: "not configured", status: http.StatusServiceUnavailable, message: "autenticación no configurada", write: (*Server).writeAuthenticationNotConfigured},
		{name: "not authenticated", status: http.StatusUnauthorized, message: "sin autenticar", write: (*Server).writeNotAuthenticated},
		{name: "user not found", status: http.StatusNotFound, message: "usuario no encontrado", write: (*Server).writeUserNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &Server{localizer: manager}
			handler := server.localizationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				test.write(server, w, r)
			}))
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.Header.Set("Accept-Language", "es-ES")
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			require.Equal(t, test.status, response.Code)
			var body map[string]any
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			require.Equal(t, test.message, body["message"])
			require.Equal(t, float64(test.status), body["code"])
		})
	}
}

func TestLocalizedMethodNotAllowedPreservesResponseContracts(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	t.Run("HTTP", func(t *testing.T) {
		server := &Server{localizer: manager}
		request := httptest.NewRequest(http.MethodPost, "/", nil)
		request.Header.Set("Accept-Language", "es-ES")
		response := httptest.NewRecorder()
		server.localizationMiddleware(http.HandlerFunc(server.writeMethodNotAllowed)).ServeHTTP(response, request)
		require.Equal(t, http.StatusMethodNotAllowed, response.Code)
		var body map[string]any
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		require.Equal(t, "método no permitido", body["message"])
	})
	t.Run("Neo4j", func(t *testing.T) {
		server := &Server{localizer: manager}
		request := httptest.NewRequest(http.MethodPost, "/", nil)
		request.Header.Set("Accept-Language", "es-ES")
		response := httptest.NewRecorder()
		server.localizationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			server.writeNeo4jMethodNotAllowed(w, r, "Neo.ClientError.General.BadRequest")
		})).ServeHTTP(response, request)
		var body TransactionResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		require.Equal(t, "Neo.ClientError.General.BadRequest", body.Errors[0].Code)
		require.Equal(t, "método no permitido", body.Errors[0].Message)
	})
}

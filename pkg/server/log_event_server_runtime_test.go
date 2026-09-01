package server

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestHTTPRequestLogPreservesFieldsAcrossLocales(t *testing.T) {
	tests := []struct {
		name    string
		tag     language.Tag
		message string
	}{
		{name: "English", tag: language.AmericanEnglish, message: "http request"},
		{name: "Spanish", tag: language.EuropeanSpanish, message: "solicitud HTTP"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&output, nil))
			manager, err := localization.NewManager([]language.Tag{test.tag}, logger)
			require.NoError(t, err)
			server := &Server{log: logger, localizer: manager}
			request := httptest.NewRequest(http.MethodPost, "/db/neo4j/tx/commit", nil)

			server.logRequest(request, http.StatusCreated, 1250*time.Millisecond)

			var record map[string]any
			require.NoError(t, json.Unmarshal(output.Bytes(), &record))
			require.Equal(t, test.message, record["msg"])
			require.Equal(t, "INFO", record["level"])
			require.Equal(t, "server.http.request", record["event_id"])
			require.Equal(t, "http", record["subsystem"])
			require.Equal(t, http.MethodPost, record["method"])
			require.Equal(t, "/db/neo4j/tx/commit", record["path"])
			require.Equal(t, float64(http.StatusCreated), record["status"])
			require.Equal(t, float64(1250*time.Millisecond), record["duration"])
		})
	}
}

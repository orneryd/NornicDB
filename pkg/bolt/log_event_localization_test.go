package bolt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestBoltLogEventUsesProcessLocaleAndPreservesFields(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	manager, err := localization.NewManager([]language.Tag{language.EuropeanSpanish}, nil)
	require.NoError(t, err)

	server := New(&Config{Logger: logger, Localizer: manager}, nil)
	server.logEvent(context.Background(), slog.LevelInfo, localization.BoltServerListeningEvent("127.0.0.1", 7687))

	var record map[string]any
	require.NoError(t, json.Unmarshal(output.Bytes(), &record))
	require.Equal(t, "El servidor Bolt está escuchando", record["msg"])
	require.Equal(t, "INFO", record["level"])
	require.Equal(t, "bolt.server.listening", record["event_id"])
	require.Equal(t, "bolt", record["component"])
	require.Equal(t, "127.0.0.1", record["host"])
	require.Equal(t, float64(7687), record["port"])
}

func TestBoltLogEventsRenderDeterministicallyAcrossLocales(t *testing.T) {
	testErr := errors.New("test failure")
	tests := []struct {
		name    string
		event   localization.LogEvent
		english string
		spanish string
	}{
		{"server listening", localization.BoltServerListeningEvent("localhost", 7687), "bolt server listening", "El servidor Bolt está escuchando"},
		{"connection panic", localization.BoltConnectionHandlerPanicEvent("panic"), "connection handler panic", "pánico en el controlador de conexión"},
		{"unencrypted rejected", localization.BoltUnencryptedConnectionRejectedEvent("remote"), "rejecting unencrypted connection", "rechazando conexión sin cifrar"},
		{"transport sniff", localization.BoltTransportSniffFailedEvent("remote", testErr), "transport sniff failed", "error al detectar el transporte"},
		{"handshake", localization.BoltHandshakeFailedEvent("remote", testErr), "handshake failed", "error en la negociación"},
		{"message handling", localization.BoltMessageHandlingErrorEvent("remote", testErr), "message handling error", "error al procesar el mensaje"},
		{"hello none", localization.BoltHelloSchemeNoneEvent(true, false), "hello scheme=none", "saludo con esquema=none"},
		{"cookie bearer", localization.BoltWebSocketCookieBearerRejectedEvent("remote", testErr), "ws cookie bearer rejected", "token bearer de cookie WebSocket rechazado"},
		{"basic auth", localization.BoltBasicAuthenticationFailedEvent("alice", "remote", testErr), "auth failed", "error de autenticación"},
		{"bearer auth", localization.BoltBearerAuthenticationFailedEvent("remote", testErr), "auth failed", "error de autenticación"},
		{"hello", localization.BoltHelloEvent("remote", "alice", []string{"reader"}, "neo4j"), "hello", "saludo"},
		{"query", localization.BoltQueryEvent("alice", "remote", "RETURN 1", nil), "query", "consulta"},
		{"query error", localization.BoltQueryErrorEvent(), "query error", "error de consulta"},
		{"run error", localization.BoltRunErrorEvent("neo4j", "ERROR", 0, time.Second, "failure"), "run", "ejecución"},
		{"run", localization.BoltRunEvent("neo4j", "OK", 1, time.Second), "run", "ejecución"},
		{"discovery", localization.BoltDiscoveryRefreshFailedEvent(testErr), "discovery refresh failed", "error al actualizar el descubrimiento"},
		{"ws request", localization.BoltWebSocketUpgradeReadRequestFailedEvent("remote", testErr), "ws upgrade read request failed", "error al leer la solicitud de actualización WebSocket"},
		{"ws credentials", localization.BoltWebSocketUpgradeCredentialsEvent("remote", true, false, true), "ws upgrade credentials", "credenciales de actualización WebSocket"},
		{"ws upgrade", localization.BoltWebSocketUpgradeFailedEvent("remote", testErr), "ws upgrade failed", "error en la actualización WebSocket"},
		{"transaction terminated", localization.BoltTransactionTerminatedEvent("rollback", "neo4j", time.Second), "explicit transaction terminated", "transacción explícita terminada"},
		{"transaction timeout requested", localization.BoltTransactionTimeoutCleanupRequestedEvent("timeout_cleanup_requested", "neo4j", time.Second), "explicit transaction timeout cleanup requested", "solicitada la limpieza por tiempo de espera de la transacción explícita"},
		{"transaction commit failed", localization.BoltTransactionCommitFailedEvent("commit", "neo4j", time.Second, testErr), "explicit transaction commit failed", "error al confirmar la transacción explícita"},
		{"transaction cleanup failed", localization.BoltTransactionCleanupFailedEvent("rollback", "neo4j", time.Second, testErr), "explicit transaction cleanup failed", "error al limpiar la transacción explícita"},
		{"transaction timeout completed", localization.BoltTransactionTimeoutCleanupCompletedEvent("timeout", "neo4j", time.Second), "explicit transaction timeout cleanup completed", "completada la limpieza por tiempo de espera de la transacción explícita"},
	}

	manager, err := localization.NewManager(nil, nil)
	require.NoError(t, err)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NotEmpty(t, test.event.ID)
			for _, locale := range []struct {
				tag  language.Tag
				want string
			}{
				{language.AmericanEnglish, test.english},
				{language.EuropeanSpanish, test.spanish},
				{language.MustParse("en-XA"), "[!! " + test.english + " !!]"},
			} {
				got, _, renderErr := manager.Render(localization.WithPreferences(context.Background(), locale.tag), test.event.Message)
				require.NoError(t, renderErr)
				require.Equal(t, locale.want, got)
			}
		})
	}
}

func TestBoltTypedLogEventsPreserveSecurityAndErrorFields(t *testing.T) {
	testErr := errors.New("invalid credential")
	authEvent := localization.BoltBasicAuthenticationFailedEvent("alice", "127.0.0.1:1234", testErr)
	require.Equal(t, localization.EventBoltAuthenticationFailed, authEvent.ID)
	require.Equal(t, "basic", authEvent.Attrs[0].Value.String())
	require.Equal(t, "alice", authEvent.Attrs[1].Value.String())
	require.Equal(t, "127.0.0.1:1234", authEvent.Attrs[2].Value.String())
	require.Same(t, testErr, authEvent.Attrs[3].Value.Any())

	params := map[string]any{"limit": 5}
	queryEvent := localization.BoltQueryEvent("alice", "remote", "RETURN $limit", params)
	require.Equal(t, "params", queryEvent.Attrs[3].Key)
	require.Equal(t, params, queryEvent.Attrs[3].Value.Any())

	cleanupEvent := localization.BoltTransactionCleanupFailedEvent("rollback", "neo4j", 2*time.Second, testErr)
	require.Equal(t, "reason", cleanupEvent.Attrs[0].Key)
	require.Equal(t, "database", cleanupEvent.Attrs[1].Key)
	require.Equal(t, "duration", cleanupEvent.Attrs[2].Key)
	require.Equal(t, "cleanup_error", cleanupEvent.Attrs[3].Key)
	require.Same(t, testErr, cleanupEvent.Attrs[3].Value.Any())
}

package localization

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestServerSearchReconcileEventsPreserveFields(t *testing.T) {
	errUnavailable := errors.New("storage unavailable")
	errReconcile := errors.New("build failed")

	tests := []struct {
		name      string
		event     LogEvent
		eventID   EventID
		messageID MessageID
		err       error
	}{
		{
			name:      "storage unavailable",
			event:     ServerSearchReconcileStorageUnavailableEvent("neo4j", errUnavailable),
			eventID:   EventServerSearchReconcileStorageUnavailable,
			messageID: MessageServerLogSearchReconcileStorageUnavailable,
			err:       errUnavailable,
		},
		{
			name:      "reconcile failed",
			event:     ServerSearchReconcileFailedEvent("neo4j", errReconcile),
			eventID:   EventServerSearchReconcileFailed,
			messageID: MessageServerLogSearchReconcileFailed,
			err:       errReconcile,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.eventID, test.event.ID)
			require.Equal(t, test.messageID, test.event.Message.ID)
			require.Equal(t, "search", test.event.Attrs[0].Value.String())
			require.Equal(t, "neo4j", test.event.Attrs[1].Value.String())
			require.Equal(t, test.err, test.event.Attrs[2].Value.Any())
		})
	}
}

func TestRemainingServerLogEventsRenderAcrossLocales(t *testing.T) {
	testErr := errors.New("test failure")
	tests := []struct {
		name    string
		event   LogEvent
		english string
		spanish string
	}{
		{"heimdall initializing", ServerHeimdallInitializingEvent(), "heimdall AI assistant initializing asynchronously", "inicializando de forma asíncrona el asistente de IA Heimdall"},
		{"heimdall provider", ServerHeimdallProviderResolvedEvent("local", "NORNICDB_HEIMDALL_PROVIDER"), "heimdall provider resolved", "proveedor de Heimdall resuelto"},
		{"heimdall failure", ServerHeimdallInitializationFailedEvent(testErr, "check models"), "heimdall initialization failed", "error al inicializar Heimdall"},
		{"apoc loading", ServerAPOCPluginsLoadingEvent("/plugins"), "loading APOC plugins", "cargando complementos APOC"},
		{"apoc failure", ServerAPOCPluginsLoadFailedEvent("/plugins", testErr), "failed to load APOC plugins", "no se pudieron cargar los complementos APOC"},
		{"heimdall plugins loading", ServerHeimdallPluginsLoadingEvent("/heimdall"), "loading Heimdall plugins", "cargando complementos de Heimdall"},
		{"heimdall plugins failure", ServerHeimdallPluginsLoadFailedEvent("/heimdall", testErr), "failed to load Heimdall plugins", "no se pudieron cargar los complementos de Heimdall"},
		{"heimdall plugins empty", ServerHeimdallPluginsDirectoryEmptyEvent(), "heimdall plugins dir is empty", "el directorio de complementos de Heimdall está vacío"},
		{"heimdall plugins duplicate", ServerHeimdallPluginsDirectoryDuplicateEvent("/plugins", "/plugins"), "heimdall plugins dir same as plugins dir; skipping", "el directorio de complementos de Heimdall coincide con el directorio de complementos; se omite"},
		{"heimdall ready", ServerHeimdallReadyEvent("model", 2, 3, "/chat", "/status"), "heimdall AI assistant ready", "asistente de IA Heimdall listo"},
		{"heimdall plugins missing", ServerHeimdallPluginsMissingEvent("install plugin"), "no heimdall plugins loaded — watcher logs will be absent", "no se cargaron complementos de Heimdall; no habrá registros del observador"},
		{"heimdall action", ServerHeimdallActionRegisteredEvent("recall"), "heimdall action registered", "acción de Heimdall registrada"},
		{"reranker unavailable", ServerSearchRerankerModelUnavailableEvent(testErr), "search reranker model unavailable; stage-2 reranking disabled, RRF order only", "modelo de reordenación de búsqueda no disponible; reordenación de segunda etapa deshabilitada, solo orden RRF"},
		{"reranker health", ServerSearchRerankerHealthCheckFailedEvent(testErr), "search reranker failed health check", "error en la comprobación de estado del reordenador de búsqueda"},
		{"embedding loading", ServerEmbeddingModelLoadingEvent("model", "local", "async"), "loading embedding model", "cargando modelo de incrustaciones"},
		{"embedding stopped", ServerEmbeddingRetryLoopStoppedEvent(), "embedding init retry loop stopped: server shutting down", "bucle de reintentos de inicialización de incrustaciones detenido: el servidor se está cerrando"},
		{"embedding cache", ServerEmbeddingCacheEnabledEvent(100, 4), "embedding cache enabled", "caché de incrustaciones habilitada"},
		{"embeddings local ready", ServerEmbeddingsReadyLocalEvent("model", 768), "embeddings ready", "incrustaciones listas"},
		{"embeddings remote ready", ServerEmbeddingsReadyRemoteEvent("openai", "https://example.test", "model", 768), "embeddings ready", "incrustaciones listas"},
		{"embedding local failure", ServerEmbeddingInitializationAttemptFailedLocalEvent(2, "model", testErr), "embedding init attempt failed", "falló el intento de inicialización de incrustaciones"},
		{"embedding remote failure", ServerEmbeddingInitializationAttemptFailedRemoteEvent(2, "openai", "model", "https://example.test", testErr), "embedding init attempt failed", "falló el intento de inicialización de incrustaciones"},
		{"embedding retry", ServerEmbeddingInitializationRetryingEvent(2 * time.Second), "retrying embedding init (exponential backoff)", "reintentando la inicialización de incrustaciones (espera exponencial)"},
		{"embedding interrupted", ServerEmbeddingInitializationRetryInterruptedEvent(), "embedding init retry interrupted by server shutdown", "reintento de inicialización de incrustaciones interrumpido por el cierre del servidor"},
		{"embedding capped", ServerEmbeddingRetryIntervalCappedEvent(5 * time.Minute), "embedding init retry interval capped; continuing periodic retries", "el intervalo de reintento de inicialización de incrustaciones alcanzó el límite; continúan los reintentos periódicos"},
		{"rbac roles", ServerRBACRolesLoadFailedEvent(testErr), "failed to load RBAC roles", "no se pudieron cargar los roles de RBAC"},
		{"rbac allowlist", ServerRBACAllowlistLoadFailedEvent(testErr), "failed to load RBAC allowlist", "no se pudo cargar la lista de acceso de RBAC"},
		{"rbac seed", ServerRBACAllowlistSeedFailedEvent(), "failed to seed RBAC allowlist", "no se pudo inicializar la lista de acceso de RBAC"},
		{"rbac privileges", ServerRBACPrivilegesLoadFailedEvent(testErr), "failed to load RBAC privileges", "no se pudieron cargar los privilegios de RBAC"},
		{"rbac entitlements", ServerRBACRoleEntitlementsLoadFailedEvent(testErr), "failed to load RBAC role entitlements", "no se pudieron cargar los derechos de rol de RBAC"},
		{"db config", ServerDatabaseConfigStoreLoadFailedEvent(), "failed to load per-DB config store", "no se pudo cargar el almacén de configuración por base de datos"},
		{"http serve", ServerHTTPServeFailedEvent(testErr), "http server error", "error del servidor HTTP"},
	}

	manager, err := NewManager(nil, slog.Default())
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
				got, _, renderErr := manager.Render(WithPreferences(context.Background(), locale.tag), test.event.Message)
				require.NoError(t, renderErr)
				require.Equal(t, locale.want, got)
			}
		})
	}
}

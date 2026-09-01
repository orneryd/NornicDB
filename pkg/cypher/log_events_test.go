package cypher

import (
	"log/slog"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestStorageExecutorLogEventLocalizesProseAndPreservesFields(t *testing.T) {
	testEnv := observability.NewTestEnv(t)
	testEnv.CaptureRecords()
	manager, err := localization.NewManager([]language.Tag{language.EuropeanSpanish}, nil)
	require.NoError(t, err)

	exec := NewStorageExecutor(newTestMemoryEngine(t))
	exec.SetLogger(testEnv.Logger)
	exec.SetLocalizationRenderer(manager)
	exec.logEvent(slog.LevelWarn, localization.CypherSlowQueryEvent(
		"0123456789abcdef",
		42,
		"MATCH (n {name: 'Alice'}) RETURN n",
	))

	records := testEnv.LoggedRecords()
	require.Len(t, records, 1)
	record := records[0]
	require.Equal(t, "WARN", record["level"])
	require.Equal(t, "consulta lenta", record["msg"])
	require.Equal(t, "cypher", record["component"])
	require.Equal(t, "cypher.slow_query", record["event_id"])
	require.Equal(t, "slow_query", record["event"])
	require.Equal(t, "0123456789abcdef", record["plan_hash"])
	require.Equal(t, float64(42), record["cypher.duration_ms"])
	require.Equal(t, "MATCH (n {name: 'Alice'}) RETURN n", record["query"])
}

package cypher

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

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

func TestExecutorRejectionReportRedactsAndExcludesRuntimeFailures(t *testing.T) {
	testEnv := observability.NewTestEnv(t)
	testEnv.CaptureRecords()
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	exec.SetLogger(testEnv.Logger)

	_, err := exec.Execute(context.Background(), "RETURN 1 IN 'sensitive-literal' AS value // comment-secret", map[string]interface{}{"secret": "sensitive-parameter"})
	require.Error(t, err)
	_, err = exec.Execute(context.Background(), "RETURN 1 IN 'other-literal' AS value", nil)
	require.Error(t, err)
	_, err = exec.Execute(context.Background(), "SHOW WHATEVER", nil)
	require.Error(t, err)
	_, err = exec.Execute(context.Background(), "RETURN 1 IN 'long-secret' AS value"+strings.Repeat(", n AS n", 80), nil)
	require.Error(t, err)
	_, err = exec.Execute(context.Background(), "RETURN 1 AS value", nil)
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = exec.Execute(canceled, "RETURN 1 AS value", nil)
	require.ErrorIs(t, err, context.Canceled)

	var reports int
	var returnShapeHash string
	var truncated bool
	for _, record := range testEnv.LoggedRecords() {
		if record["event"] != "query_rejected" {
			continue
		}
		reports++
		require.Equal(t, "INFO", record["level"])
		require.Equal(t, "syntax_error", record["reason"])
		shape, ok := record["query"].(string)
		require.True(t, ok)
		require.LessOrEqual(t, len(shape), 500)
		if len(shape) == 500 {
			truncated = true
		}
		require.NotEmpty(t, record["shape_hash"])
		if record["statement_class"] == "RETURN" && reports <= 2 {
			if returnShapeHash == "" {
				returnShapeHash, _ = record["shape_hash"].(string)
			} else {
				require.Equal(t, returnShapeHash, record["shape_hash"])
			}
		} else {
			require.Contains(t, []string{"SHOW", "RETURN"}, record["statement_class"])
		}
		require.NotContains(t, shape, "sensitive-literal")
		require.NotContains(t, shape, "sensitive-parameter")
		require.NotContains(t, shape, "comment-secret")
		require.NotContains(t, shape, "long-secret")
		require.False(t, strings.Contains(shape, "RETURN 1 AS value"))
	}
	require.Equal(t, 4, reports)
	require.True(t, truncated)
}

func TestSlowQueryReportOmitsCommentSecrets(t *testing.T) {
	testEnv := observability.NewTestEnv(t)
	testEnv.CaptureRecords()
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	exec.SetLogger(testEnv.Logger)
	exec.SetSlowQueryThreshold(time.Nanosecond)
	exec.emitSlowQueryLog(t.Context(), "RETURN 'literal-secret' AS value // comment-secret", nil, time.Millisecond)

	var reports int
	for _, record := range testEnv.LoggedRecords() {
		if record["event"] != "slow_query" {
			continue
		}
		reports++
		shape, ok := record["query"].(string)
		require.True(t, ok)
		require.NotContains(t, shape, "literal-secret")
		require.NotContains(t, shape, "comment-secret")
	}
	require.Equal(t, 1, reports)
}

func TestTruncateRuneSafeNeverSplitsUTF8(t *testing.T) {
	require.Equal(t, "abc", truncateRuneSafe("abc", 500))
	require.Equal(t, "héllo wörld", truncateRuneSafe("héllo wörld", 500))
	require.True(t, utf8.ValidString(truncateRuneSafe("héllo wörld", 6)))
	require.Equal(t, "", truncateRuneSafe("", 500))
	// Truncation inside a multi-byte rune lands on the rune boundary.
	require.Equal(t, "hé", truncateRuneSafe("héllo", 3))
}

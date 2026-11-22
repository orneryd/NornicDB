package providerbatch

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func captureUsageLogs(t *testing.T, level slog.Level) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: level})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &logs
}

// The shared owner reports the provider's own total once, named by the
// operation that made the request, so embedding and rerank callers emit the
// same record shape.
func TestRecordUsageReportsOneProviderTotal(t *testing.T) {
	logs := captureUsageLogs(t, slog.LevelDebug)

	RecordUsage(context.Background(), "rerank", "voyage", "rerank-2.5", 16, 4096)

	var record struct {
		Message     string `json:"msg"`
		Provider    string `json:"provider"`
		Model       string `json:"model"`
		Inputs      int    `json:"inputs"`
		TotalTokens int    `json:"total_tokens"`
	}
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &record))
	require.Equal(t, "rerank provider usage", record.Message)
	require.Equal(t, "voyage", record.Provider)
	require.Equal(t, "rerank-2.5", record.Model)
	require.Equal(t, 16, record.Inputs)
	require.Equal(t, 4096, record.TotalTokens)
}

// Debug-level accounting stays out of ordinary logs.
func TestRecordUsageStaysOutOfNonDebugLogs(t *testing.T) {
	logs := captureUsageLogs(t, slog.LevelInfo)

	RecordUsage(context.Background(), "embedding", "voyage", "voyage-4-large", 2, 14)

	require.Empty(t, logs.String())
}

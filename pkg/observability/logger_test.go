package observability

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewLogger_DefaultJSON — cfg.Format="json" returns a *slog.Logger whose
// Handler() chain has *recoveringHandler outermost (Pitfall 3 / D-09).
func TestNewLogger_DefaultJSON(t *testing.T) {
	cfg := LoggerConfig{
		Level:  "info",
		Format: "json",
		Output: "stderr",
	}
	info := ServiceInfo{Name: "nornicdb", Version: "test", NodeID: "n1"}

	logger, _, err := NewLogger(cfg, info)
	require.NoError(t, err)
	require.NotNil(t, logger)

	// Outer handler MUST be *recoveringHandler (D-09 keystone).
	_, ok := logger.Handler().(*recoveringHandler)
	require.True(t, ok, "outermost handler must be *recoveringHandler (D-09)")
}

// TestNewLogger_LevelVarPlumbing — cfg.Level="debug" enables Debug level;
// cfg.Level="warn" disables Info level (D-02d).
func TestNewLogger_LevelVarPlumbing(t *testing.T) {
	infoSI := ServiceInfo{Name: "nornicdb", Version: "test"}

	debugLogger, _, err := NewLogger(LoggerConfig{Level: "debug", Output: "stderr"}, infoSI)
	require.NoError(t, err)
	assert.True(t, debugLogger.Enabled(context.Background(), slog.LevelDebug),
		"cfg.Level=debug must enable Debug records")

	warnLogger, _, err := NewLogger(LoggerConfig{Level: "warn", Output: "stderr"}, infoSI)
	require.NoError(t, err)
	assert.False(t, warnLogger.Enabled(context.Background(), slog.LevelInfo),
		"cfg.Level=warn must disable Info records")
}

// TestNewLogger_BadOutputFallsBack — bogus path returns a usable logger
// + a non-nil error, NEVER a nil logger (OBS-11 fail-closed analog).
func TestNewLogger_BadOutputFallsBack(t *testing.T) {
	bad := filepath.Join(t.TempDir(), "no", "such", "dir", "x.log")
	logger, writer, err := NewLogger(LoggerConfig{
		Level:  "info",
		Output: bad,
	}, ServiceInfo{Name: "nornicdb", Version: "test"})
	require.Error(t, err, "bad path must surface error so operator sees the misconfig")
	require.NotNil(t, logger, "logger must remain usable so the process keeps running")
	require.NotNil(t, writer)
	// Process must keep running — emit a record without panicking.
	logger.Info("post-fallback", "k", "v")
}

// TestNewLogger_FileOutputRoundtrip — happy-path file output.
func TestNewLogger_FileOutputRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.log")
	logger, writer, err := NewLogger(LoggerConfig{
		Level:  "info",
		Output: path,
	}, ServiceInfo{Name: "nornicdb", Version: "test", NodeID: "node-1"})
	require.NoError(t, err)
	require.NotNil(t, logger)

	logger.Info("hello", "k", "v")

	// Sync the file (D-09a opportunistic Sync) before reading.
	if syncer, ok := writer.(interface{ Sync() error }); ok {
		_ = syncer.Sync()
	}
	if closer, ok := writer.(interface{ Close() error }); ok {
		_ = closer.Close()
	}

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, len(data), 0)
}

// TestParseLevel — exercises every supported level and the default.
func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":   slog.LevelDebug,
		"DEBUG":   slog.LevelDebug,
		"info":    slog.LevelInfo,
		"":        slog.LevelInfo,
		"unknown": slog.LevelInfo,
		"warn":    slog.LevelWarn,
		"WARNING": slog.LevelWarn,
		"error":   slog.LevelError,
	}
	for in, want := range cases {
		require.Equal(t, want, parseLevel(in), "parseLevel(%q)", in)
	}
}

// TestOpenLogWriter — happy paths for stdout/stderr/explicit-stdout.
func TestOpenLogWriter(t *testing.T) {
	w, err := openLogWriter("")
	require.NoError(t, err)
	require.Equal(t, os.Stdout, w)

	w, err = openLogWriter("stdout")
	require.NoError(t, err)
	require.Equal(t, os.Stdout, w)

	w, err = openLogWriter("stderr")
	require.NoError(t, err)
	require.Equal(t, os.Stderr, w)
}

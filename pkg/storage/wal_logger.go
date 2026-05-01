package storage

import (
	"io"
	"log/slog"
)

// WALLogger receives structured diagnostics emitted by WAL recovery / corruption handlers.
//
// This is intentionally minimal to avoid coupling storage to a specific logging library.
// Implementations should treat fields as a stable machine-readable contract.
//
// Phase 2 LOG-01 note: the legacy default implementation routed records through
// stdlib log printers. That implementation is gone — defaultWALLogger now wraps
// a *slog.Logger so all storage-package emissions flow through the production
// 4-layer slog handler stack (recovering → mandatory → redactor → JSON).
type WALLogger interface {
	Log(level string, msg string, fields map[string]any)
}

// slogWALLogger adapts the WALLogger interface onto a *slog.Logger so existing
// pkg/storage call sites that hold a WALLogger reference (e.g., w.config.Logger)
// emit through the structured slog stream. This is the LOG-01 chokepoint that
// removes the previous stdlib-printer-backed defaultWALLogger.
type slogWALLogger struct {
	logger *slog.Logger
}

// newSlogWALLogger wraps a *slog.Logger as a WALLogger. The provided logger
// is expected to already carry component=storage,subsystem=wal attributes
// (the WAL ctor binds those before constructing this adapter).
func newSlogWALLogger(logger *slog.Logger) WALLogger {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return slogWALLogger{logger: logger.With("subsystem", "wal")}
}

func (s slogWALLogger) Log(level string, msg string, fields map[string]any) {
	level = mapWALLogLevel(level)
	attrs := make([]any, 0, len(fields)*2)
	for k, v := range fields {
		attrs = append(attrs, k, v)
	}
	switch level {
	case "error":
		s.logger.Error(msg, attrs...)
	case "warn":
		s.logger.Warn(msg, attrs...)
	case "debug":
		s.logger.Debug(msg, attrs...)
	default:
		s.logger.Info(msg, attrs...)
	}
}

// defaultWALLogger is retained as a public type alias so any external code
// that constructs `defaultWALLogger{}` literals (none found at LOG-01 grep
// time, but keep the symbol as a safety net) routes through the slog stream.
type defaultWALLogger struct{}

func (defaultWALLogger) Log(level string, msg string, fields map[string]any) {
	newSlogWALLogger(discardWALSlog()).Log(level, msg, fields)
}

// discardWALSlog returns a discard-handler *slog.Logger used when WAL helpers
// run without a configured structured logger (e.g., tests, scripts/perf_direct,
// pre-Phase-2 callers of ReadWALEntries / RecoverFromWAL). Single allocation
// per call site; no global mutable state — LOG-09 forbids the stdlib slog
// default-logger accessor across the in-scope packages.
func discardWALSlog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// mapWALLogLevel normalises the WALLogger.Log level string (lowercase Go
// idiom inherited from the original interface) onto slog levels.
func mapWALLogLevel(level string) string {
	switch level {
	case "ERROR", "error":
		return "error"
	case "WARN", "warn", "warning":
		return "warn"
	case "DEBUG", "debug":
		return "debug"
	default:
		return "info"
	}
}

package observability

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

// LoggerConfig is the local mirror of the relevant subset of
// pkg/config.LoggingConfig. We can't import pkg/config here because pkg/config
// already imports pkg/observability for ObservabilityConfig (would create an
// import cycle). The runServe site translates between the two structs.
//
// Fields:
//   - Level: "debug" / "info" / "warn" / "error" (case-insensitive). Default: info.
//   - Format: "json" (default) or "text". Phase 2 only ships JSON; the field
//     is held for forward compat with future TextHandler dev mode.
//   - Output: "stdout" (default), "stderr", or a filesystem path.
type LoggerConfig struct {
	Level  string
	Format string
	Output string
}

// NewLogger constructs the production *slog.Logger with the 4-layer handler
// stack per D-02a (outermost → innermost):
//
//	recoveringHandler          (D-09: catches panics in any inner layer)
//	  └─ mandatoryFieldsHandler  (D-05: service/version/node_id + trace ctx)
//	       └─ redactingHandler   (D-03/D-03b: PII allow-list + CRLF strip)
//	            └─ nornicdbJSONHandler (D-02: ≤2 allocs/record)
//
// Returns:
//   - *slog.Logger: the assembled logger; never nil even on error.
//   - io.Writer: the underlying writer (file/stderr/stdout) so the caller
//     can stash it in Provider.writerRef for D-09a opportunistic Sync().
//   - error: non-nil if cfg.Output points at an unopenable path. The logger
//     remains usable (writes to stderr) so the process keeps running —
//     OBS-11 fail-closed analog.
//
// Per D-08 bootstrap order: cmd/nornicdb MUST call NewLogger BEFORE
// observability.New so the *slog.Logger can be threaded through the
// Provider construction.
//
// Per Pitfall 10: the LevelVar is allocated as a pointer (`&slog.LevelVar{}`)
// so the handler holds a stable address that survives the function return.
func NewLogger(cfg LoggerConfig, info ServiceInfo) (*slog.Logger, io.Writer, error) {
	levelVar := &slog.LevelVar{}
	levelVar.Set(parseLevel(cfg.Level))

	writer, openErr := openLogWriter(cfg.Output)
	// On open failure the writer is os.Stderr so the process keeps running.

	inner := newNornicdbJSONHandler(writer, levelVar)
	redact := newRedactingHandler(inner, redactSetFromEnv())
	mandatory := newMandatoryFieldsHandler(redact, info)
	outer := newRecoveringHandler(mandatory)

	return slog.New(outer), writer, openErr
}

// parseLevel maps a config string to slog.Level. Default INFO on unknown.
func parseLevel(s string) slog.Level {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// openLogWriter resolves cfg.Output to an io.Writer.
//
//   - ""        → os.Stdout (Phase-1 default)
//   - "stdout"  → os.Stdout
//   - "stderr"  → os.Stderr
//   - <path>    → os.OpenFile(path, O_APPEND|O_CREATE|O_WRONLY, 0o644)
//
// On failure to open a file path, returns (os.Stderr, error). Caller logic
// is responsible for treating the error as a WARN-level operator signal —
// process startup is unconditionally robust against logging misconfig.
func openLogWriter(out string) (io.Writer, error) {
	switch strings.ToLower(strings.TrimSpace(out)) {
	case "", "stdout":
		return os.Stdout, nil
	case "stderr":
		return os.Stderr, nil
	}
	f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return os.Stderr, fmt.Errorf("logger: open %q: %w (falling back to stderr)", out, err)
	}
	return f, nil
}

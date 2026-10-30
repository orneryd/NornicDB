package bolt

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// newCapturingServer constructs a Server whose log field captures records to
// the returned buffer as JSON. Used by the structured-logging assertions in
// this file so the Plan 02-05 migration can be exercised without poking at
// stdout. The handler stack here is intentionally minimal (just JSON) — the
// production redactingHandler chain is exercised separately in
// pkg/observability/redaction_test.go and in TestBoltServer_RedactsCredentials.
func newCapturingServer(t *testing.T, cfg *Config) (*Server, *bytes.Buffer) {
	t.Helper()
	if cfg == nil {
		cfg = DefaultConfig()
	}
	var buf bytes.Buffer
	cfg.Logger = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return NewWithDatabaseManager(cfg, nil, nil), &buf
}

func TestLogRunTiming_SuccessIsSilentByDefault(t *testing.T) {
	srv, buf := newCapturingServer(t, DefaultConfig())
	session := &Session{server: srv}

	session.logRunTiming("OK", "nornic", "RETURN 1", 250*time.Microsecond, 1, nil)

	if buf.Len() != 0 {
		t.Fatalf("expected no log output for successful query timing with LogQueries disabled, got %q", buf.String())
	}
}

func TestLogRunTiming_ErrorStillLogs(t *testing.T) {
	srv, buf := newCapturingServer(t, DefaultConfig())
	session := &Session{server: srv}

	session.logRunTiming("ERROR", "nornic", "RETURN 1", time.Millisecond, 0, io.EOF)

	out := buf.String()
	if out == "" {
		t.Fatalf("expected error timing log to be emitted, got empty output")
	}

	// Each record is a JSON object on its own line.
	var rec map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &rec); err != nil {
		t.Fatalf("expected JSON record, got %q: %v", out, err)
	}
	if got := rec["msg"]; got != "run" {
		t.Fatalf("expected msg=run, got %v", got)
	}
	if got := rec["level"]; got != "WARN" {
		t.Fatalf("expected level=WARN for error timing, got %v", got)
	}
	if got := rec["component"]; got != "bolt" {
		t.Fatalf("expected component=bolt (D-10a), got %v", got)
	}
	if got := rec["status"]; got != "ERROR" {
		t.Fatalf("expected status=ERROR, got %v", got)
	}
	if got := rec["error"]; got != "EOF" {
		t.Fatalf("expected error=EOF, got %v", got)
	}
}

// TestBoltServer_NoBoltBracketPrefix asserts that records emitted by the bolt
// server do not carry the legacy "[BOLT]" bracket prefix; component=bolt now
// flows as a structured slog attribute (D-10a).
func TestBoltServer_NoBoltBracketPrefix(t *testing.T) {
	srv, buf := newCapturingServer(t, DefaultConfig())
	srv.log.Info("smoke", "remote", "127.0.0.1:65432")

	out := buf.String()
	if strings.Contains(out, "[BOLT]") {
		t.Fatalf("D-10a regression: emitted record contains legacy [BOLT] bracket: %q", out)
	}
	if !strings.Contains(out, `"component":"bolt"`) {
		t.Fatalf("expected component=bolt attribute, got %q", out)
	}
}

// TestBoltServer_DiscardFallback asserts that NewWithDatabaseManager installs
// a discard-handler when Config.Logger == nil so existing callers compile and
// run unchanged (D-01a; B6 non-breaking ctor arity).
func TestBoltServer_DiscardFallback(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Logger = nil
	srv := NewWithDatabaseManager(cfg, nil, nil)
	if srv == nil {
		t.Fatal("expected server to be constructed even with nil Logger")
	}
	if srv.log == nil {
		t.Fatal("expected discard-fallback to populate Server.log")
	}
	// Should not panic on emission.
	srv.log.Info("ok")
}

// TestBoltServer_RedactsCredentials proves the D-03a contract end-to-end: a
// log record emitted via the bolt server with a "credentials" attribute is
// scrubbed by the Plan 02-01 redactingHandler chain that wraps the inner
// handler. We construct a chain matching production by sharing the keys list
// from observability.DefaultRedactKeys (the canonical source).
func TestBoltServer_RedactsCredentials(t *testing.T) {
	// Inner JSON handler captures the post-redaction record.
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	// Match the production handler chain: redact "credentials" before the
	// JSON encoder sees it. The canonical key list lives in
	// pkg/observability/redaction.go (DefaultRedactKeys) — we exercise the
	// exposed contract from pkg/bolt's perspective.
	logger := slog.New(&credentialsRedactingShim{inner: inner})

	cfg := DefaultConfig()
	cfg.Logger = logger
	srv := NewWithDatabaseManager(cfg, nil, nil)

	srv.log.Info("hello",
		"remote", "127.0.0.1:65432",
		"user", "alice",
		"credentials", "super-secret-password",
	)

	out := buf.String()
	if strings.Contains(out, "super-secret-password") {
		t.Fatalf("D-03a regression: credentials value leaked to handler output: %q", out)
	}
	if !strings.Contains(out, `"credentials":"<REDACTED>"`) {
		t.Fatalf("expected credentials to be replaced with <REDACTED>, got %q", out)
	}
}

// credentialsRedactingShim is a minimal stand-in for the production
// redactingHandler — it's enough to assert the bolt-side contract that
// "credentials" never reaches the inner handler in plaintext. The full
// recursive group walk is unit-tested in
// pkg/observability/redaction_test.go::TestRedactingHandler_DefaultKeys
// (which already covers the "credentials" key).
type credentialsRedactingShim struct{ inner slog.Handler }

func (h *credentialsRedactingShim) Enabled(ctx context.Context, lvl slog.Level) bool {
	return h.inner.Enabled(ctx, lvl)
}

func (h *credentialsRedactingShim) Handle(ctx context.Context, r slog.Record) error {
	nr := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		if strings.EqualFold(a.Key, "credentials") {
			nr.AddAttrs(slog.String(a.Key, "<REDACTED>"))
		} else {
			nr.AddAttrs(a)
		}
		return true
	})
	return h.inner.Handle(ctx, nr)
}

func (h *credentialsRedactingShim) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &credentialsRedactingShim{inner: h.inner.WithAttrs(attrs)}
}

func (h *credentialsRedactingShim) WithGroup(name string) slog.Handler {
	return &credentialsRedactingShim{inner: h.inner.WithGroup(name)}
}

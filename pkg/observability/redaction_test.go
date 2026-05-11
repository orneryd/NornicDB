package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func newCapturingLogger(t *testing.T, mid func(slog.Handler) slog.Handler) (*slog.Logger, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	lv := &slog.LevelVar{}
	lv.Set(slog.LevelDebug)
	inner := newNornicdbJSONHandler(&buf, lv)
	wrapped := mid(inner)
	return slog.New(wrapped), &buf
}

// TestRedactingHandler_DefaultKeys — every key in DefaultRedactKeys is
// redacted; case-insensitive (`Password`, `TOKEN`, `Api_Key` all match).
func TestRedactingHandler_DefaultKeys(t *testing.T) {
	cases := []string{
		"password", "Password", "PASSWORD",
		"token", "TOKEN", "Token",
		"authorization", "Authorization",
		"secret", "Secret",
		"api_key", "API_KEY", "Api_Key",
		"credentials", "Credentials",
	}
	for _, key := range cases {
		t.Run(key, func(t *testing.T) {
			logger, buf := newCapturingLogger(t, func(h slog.Handler) slog.Handler {
				return newRedactingHandler(h, defaultRedactSet())
			})
			logger.Info("auth", key, "secret-value-do-not-leak")

			var rec map[string]any
			require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
			require.Equal(t, "<REDACTED>", rec[key], "key %s must be redacted", key)
			require.NotContains(t, buf.String(), "secret-value-do-not-leak")
		})
	}
}

// TestRedactingHandler_NestedGroupRedaction — slog.Group("auth", slog.String("password","hunter2"))
// redacts to `{"auth":{"password":"<REDACTED>"}}`.
func TestRedactingHandler_NestedGroupRedaction(t *testing.T) {
	logger, buf := newCapturingLogger(t, func(h slog.Handler) slog.Handler {
		return newRedactingHandler(h, defaultRedactSet())
	})
	logger.Info("login", slog.Group("auth",
		slog.String("user", "alice"),
		slog.String("password", "hunter2"),
	))

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	auth, ok := rec["auth"].(map[string]any)
	require.True(t, ok, "auth group must remain a group")
	require.Equal(t, "alice", auth["user"], "non-sensitive sibling preserved")
	require.Equal(t, "<REDACTED>", auth["password"])
	require.NotContains(t, buf.String(), "hunter2")
}

// TestRedactingHandler_DeepGroupRedaction — recursion goes deeper than 1 level.
func TestRedactingHandler_DeepGroupRedaction(t *testing.T) {
	logger, buf := newCapturingLogger(t, func(h slog.Handler) slog.Handler {
		return newRedactingHandler(h, defaultRedactSet())
	})
	logger.Info("deep", slog.Group("outer", slog.Group("inner", slog.String("token", "tk"))))

	require.NotContains(t, buf.String(), `"tk"`)
	require.Contains(t, buf.String(), `"<REDACTED>"`)
}

// TestRedactingHandler_CRLFStrip — `"line1\r\nline2"` for non-redacted key
// emerges as `"line1line2"` (LOG-03).
func TestRedactingHandler_CRLFStrip(t *testing.T) {
	logger, buf := newCapturingLogger(t, func(h slog.Handler) slog.Handler {
		return newRedactingHandler(h, defaultRedactSet())
	})
	logger.Info("crlf", "msg_field", "line1\r\nline2")

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	require.Equal(t, "line1line2", rec["msg_field"])
}

// TestRedactingHandler_EnvExtraKeys — NORNICDB_LOG_REDACT_EXTRA adds keys.
func TestRedactingHandler_EnvExtraKeys(t *testing.T) {
	t.Setenv("NORNICDB_LOG_REDACT_EXTRA", "session_id,custom_secret")

	logger, buf := newCapturingLogger(t, func(h slog.Handler) slog.Handler {
		return newRedactingHandler(h, redactSetFromEnv())
	})
	logger.Info("env", "session_id", "abc", "custom_secret", "xyz", "ok", "visible")

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	require.Equal(t, "<REDACTED>", rec["session_id"])
	require.Equal(t, "<REDACTED>", rec["custom_secret"])
	require.Equal(t, "visible", rec["ok"])
}

// TestRedactor_NoEmojiInJSONOutput — NO U+2000..U+1FAFF runes in rendered
// JSON output (D-10a falsifiability gate).
func TestRedactor_NoEmojiInJSONOutput(t *testing.T) {
	logger, buf := newCapturingLogger(t, func(h slog.Handler) slog.Handler {
		return newRedactingHandler(h, defaultRedactSet())
	})
	// Note: the redactor does NOT strip emoji from msg/values; the test gate
	// asserts the migration leaves no emoji in production records. We simulate
	// a clean migration by emitting plain ASCII; this test fails if a future
	// migration commit accidentally re-introduces emoji into the record shape
	// produced by handler-internal code paths.
	logger.Info("plain ascii message", "k", "value")

	out := buf.String()
	for _, r := range out {
		if r >= 0x2000 && r <= 0x1FAFF {
			t.Fatalf("found emoji-range rune U+%04X in JSON output: %s", r, out)
		}
	}
	require.True(t, utf8.ValidString(out), "output must be valid UTF-8")
}

// TestRedactSetFromEnv_DefaultsAreLowercase — sanity test on set construction.
func TestRedactSetFromEnv_DefaultsAreLowercase(t *testing.T) {
	t.Setenv("NORNICDB_LOG_REDACT_EXTRA", "")
	set := redactSetFromEnv()
	for _, k := range DefaultRedactKeys {
		_, ok := set[k] // DefaultRedactKeys are already lowercase
		require.True(t, ok, "default key %q must be present in lowercased set", k)
	}
}

// TestRedactingHandler_WithAttrsRedactsAtConstruction — `.With("password","x")`
// scrubs the value so derived loggers never leak.
func TestRedactingHandler_WithAttrsRedactsAtConstruction(t *testing.T) {
	logger, buf := newCapturingLogger(t, func(h slog.Handler) slog.Handler {
		return newRedactingHandler(h, defaultRedactSet())
	})
	derived := logger.With("password", "leak-me", "ok", "visible")
	derived.Info("hi")
	require.NotContains(t, buf.String(), "leak-me")
	require.Contains(t, buf.String(), "<REDACTED>")
}

// TestRedactingHandler_WithGroup — group derivation preserves redaction.
func TestRedactingHandler_WithGroup(t *testing.T) {
	logger, buf := newCapturingLogger(t, func(h slog.Handler) slog.Handler {
		return newRedactingHandler(h, defaultRedactSet())
	})
	g := logger.WithGroup("x")
	g.Info("hi", "password", "leak-me")
	require.NotContains(t, buf.String(), "leak-me")
}

// TestRedactingHandler_EnabledDelegates — gate flows through.
func TestRedactingHandler_EnabledDelegates(t *testing.T) {
	lv := &slog.LevelVar{}
	lv.Set(slog.LevelError)
	var buf bytes.Buffer
	inner := newNornicdbJSONHandler(&buf, lv)
	rh := newRedactingHandler(inner, defaultRedactSet())
	require.False(t, rh.Enabled(context.Background(), slog.LevelInfo))
}

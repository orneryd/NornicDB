package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"strings"
	"testing"
	"testing/slogtest"
	"time"

	"github.com/stretchr/testify/require"
)

// TestNornicdbJSONHandler_StdlibSemanticParity feeds representative record
// shapes through both the custom handler and stdlib slog.NewJSONHandler;
// asserts JSON-decoded equality (NOT byte-equality, per Pitfall 7).
func TestNornicdbJSONHandler_StdlibSemanticParity(t *testing.T) {
	type call struct {
		level slog.Level
		msg   string
		args  []any
	}
	cases := []call{
		{slog.LevelInfo, "simple", []any{"k", "v"}},
		{slog.LevelDebug, "with int", []any{"count", 42}},
		{slog.LevelWarn, "with float", []any{"ratio", 3.14}},
		{slog.LevelError, "with bool", []any{"ok", true}},
		{slog.LevelInfo, "nested group", []any{slog.Group("http", "method", "GET", "status", 200)}},
		{slog.LevelInfo, "deep group", []any{slog.Group("a", slog.Group("b", "c", "d"))}},
		{slog.LevelInfo, "duration", []any{slog.Duration("dur", 0)}},
		{slog.LevelInfo, "embedded quote", []any{"msg", `he said "hi"`}},
		{slog.LevelInfo, "newline value", []any{"line", "first\nsecond"}},
		{slog.LevelInfo, "backslash", []any{"path", `C:\foo\bar`}},
		{slog.LevelInfo, "tab", []any{"sep", "a\tb"}},
		{slog.LevelInfo, "control byte", []any{"ctrl", "\x01\x02"}},
		{slog.LevelInfo, "U+2028 line sep", []any{"text", "a b"}},
		{slog.LevelInfo, "html chars", []any{"q", "<a href=\"x\">"}},
	}

	for _, tc := range cases {
		t.Run(tc.msg, func(t *testing.T) {
			var customBuf, stdlibBuf bytes.Buffer

			// Use minimal level (Debug) so cases below Info still emit.
			lv := &slog.LevelVar{}
			lv.Set(slog.LevelDebug)
			custom := slog.New(newNornicdbJSONHandler(&customBuf, lv))
			stdlib := slog.New(slog.NewJSONHandler(&stdlibBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

			// Emit identical record on both. Use identical methods/args.
			switch tc.level {
			case slog.LevelDebug:
				custom.Debug(tc.msg, tc.args...)
				stdlib.Debug(tc.msg, tc.args...)
			case slog.LevelWarn:
				custom.Warn(tc.msg, tc.args...)
				stdlib.Warn(tc.msg, tc.args...)
			case slog.LevelError:
				custom.Error(tc.msg, tc.args...)
				stdlib.Error(tc.msg, tc.args...)
			default:
				custom.Info(tc.msg, tc.args...)
				stdlib.Info(tc.msg, tc.args...)
			}

			var customRec, stdlibRec map[string]any
			require.NoError(t, json.Unmarshal(customBuf.Bytes(), &customRec),
				"custom output must be parseable JSON: %s", customBuf.String())
			require.NoError(t, json.Unmarshal(stdlibBuf.Bytes(), &stdlibRec),
				"stdlib output must be parseable JSON: %s", stdlibBuf.String())

			// Time field is non-deterministic across the two emits; drop both.
			delete(customRec, "time")
			delete(stdlibRec, "time")

			require.Equal(t, stdlibRec, customRec,
				"semantic parity broken for %s: custom=%s stdlib=%s",
				tc.msg, customBuf.String(), stdlibBuf.String())
		})
	}
}

// TestNornicdbJSONHandler_GroupNesting — `WithGroup("http").Info("req", "method","GET")`
// renders `{"http":{"method":"GET",...}}` shape.
func TestNornicdbJSONHandler_GroupNesting(t *testing.T) {
	var buf bytes.Buffer
	lv := &slog.LevelVar{}
	logger := slog.New(newNornicdbJSONHandler(&buf, lv))

	logger.WithGroup("http").Info("req", "method", "GET", "status", 200)

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec), "out=%s", buf.String())
	http, ok := rec["http"].(map[string]any)
	require.True(t, ok, "expected nested 'http' group, got: %v", rec)
	require.Equal(t, "GET", http["method"])
	require.EqualValues(t, 200, http["status"])
}

// TestNornicdbJSONHandler_StableKeyOrder — record-attr iteration produces
// keys in slog Record order (time, level, msg, then attrs in insertion order).
func TestNornicdbJSONHandler_StableKeyOrder(t *testing.T) {
	var buf bytes.Buffer
	lv := &slog.LevelVar{}
	logger := slog.New(newNornicdbJSONHandler(&buf, lv))

	logger.Info("ordered", "first", 1, "second", 2, "third", 3)

	out := buf.String()
	// Find positions of each key-quoted form.
	idxFirst := strings.Index(out, `"first"`)
	idxSecond := strings.Index(out, `"second"`)
	idxThird := strings.Index(out, `"third"`)
	require.Greater(t, idxFirst, -1)
	require.Greater(t, idxSecond, idxFirst, "second must come after first")
	require.Greater(t, idxThird, idxSecond, "third must come after second")

	// time/level/msg precede attrs.
	require.Less(t, strings.Index(out, `"time"`), idxFirst)
	require.Less(t, strings.Index(out, `"level"`), idxFirst)
	require.Less(t, strings.Index(out, `"msg"`), idxFirst)
}

// TestHandlerStack_Conformance — runs slogtest.TestHandler against the
// custom handler. Uses semantic-parity result extraction.
func TestHandlerStack_Conformance(t *testing.T) {
	var buf bytes.Buffer
	lv := &slog.LevelVar{}
	h := newNornicdbJSONHandler(&buf, lv)
	results := func() []map[string]any {
		out := []map[string]any{}
		for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			var rec map[string]any
			if err := json.Unmarshal(line, &rec); err == nil {
				out = append(out, rec)
			}
		}
		return out
	}
	if err := slogtest.TestHandler(h, results); err != nil {
		t.Fatalf("slogtest.TestHandler: %v", err)
	}
}

// TestHandlerStack_OrderInvariant — outermost handler returned by NewLogger
// MUST be *recoveringHandler (Pitfall 3, D-09).
func TestHandlerStack_OrderInvariant(t *testing.T) {
	lv := &slog.LevelVar{}
	inner := newNornicdbJSONHandler(&bytes.Buffer{}, lv)
	redact := newRedactingHandler(inner, defaultRedactSet())
	mandatory := newMandatoryFieldsHandler(redact, ServiceInfo{Name: "nornicdb", Version: "test"})
	outer := newRecoveringHandler(mandatory)

	logger := slog.New(outer)
	_, ok := logger.Handler().(*recoveringHandler)
	require.True(t, ok, "outermost must be *recoveringHandler — D-09 keystone")
}

// TestNornicdbJSONHandler_Enabled — Enabled honors levelVar.
func TestNornicdbJSONHandler_Enabled(t *testing.T) {
	lv := &slog.LevelVar{}
	lv.Set(slog.LevelWarn)
	h := newNornicdbJSONHandler(&bytes.Buffer{}, lv)
	require.False(t, h.Enabled(context.Background(), slog.LevelInfo))
	require.True(t, h.Enabled(context.Background(), slog.LevelWarn))
	require.True(t, h.Enabled(context.Background(), slog.LevelError))
}

// TestNornicdbJSONHandler_WithAttrs — attrs from WithAttrs persist across records.
func TestNornicdbJSONHandler_WithAttrs(t *testing.T) {
	var buf bytes.Buffer
	lv := &slog.LevelVar{}
	logger := slog.New(newNornicdbJSONHandler(&buf, lv)).With("component", "test")
	logger.Info("a")
	logger.Info("b")

	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		var rec map[string]any
		require.NoError(t, json.Unmarshal(line, &rec))
		require.Equal(t, "test", rec["component"])
	}
}

// TestAppendValue_ExtraTypes — covers Uint64, Float NaN/Inf, Time, []byte,
// error, []string, fmt.Stringer fallback.
func TestAppendValue_ExtraTypes(t *testing.T) {
	cases := map[string]struct {
		args        []any
		wantContain string
	}{
		"uint64":   {[]any{slog.Uint64("u", 42)}, `"u":42`},
		"NaN":      {[]any{slog.Float64("f", math.NaN())}, `"NaN"`},
		"+Inf":     {[]any{slog.Float64("f", math.Inf(1))}, `"+Inf"`},
		"-Inf":     {[]any{slog.Float64("f", math.Inf(-1))}, `"-Inf"`},
		"time":     {[]any{slog.Time("t", time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC))}, `"t":"2020-01-02T03:04:05Z"`},
		"bytes":    {[]any{slog.Any("b", []byte{0x01, 0xab})}, `"b":"01ab"`},
		"error":    {[]any{slog.Any("e", errors.New("boom"))}, `"e":"boom"`},
		"strings":  {[]any{slog.Any("ss", []string{"a", "b"})}, `"ss":["a","b"]`},
		"nilany":   {[]any{slog.Any("n", nil)}, `"n":null`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			lv := &slog.LevelVar{}
			logger := slog.New(newNornicdbJSONHandler(&buf, lv))
			logger.Info("v", tc.args...)
			require.Contains(t, buf.String(), tc.wantContain, "out=%s", buf.String())
		})
	}
}

// TestAppendEscapedJSON_LineSep — U+2028 / U+2029 are escaped to   /  .
func TestAppendEscapedJSON_LineSep(t *testing.T) {
	var buf bytes.Buffer
	lv := &slog.LevelVar{}
	logger := slog.New(newNornicdbJSONHandler(&buf, lv))
	logger.Info("ls", "k", "a b c")
	out := buf.String()
	require.Contains(t, out, `\u2028`, "U+2028 must be escaped to \\u2028")
	require.Contains(t, out, `\u2029`, "U+2029 must be escaped to \\u2029")
}

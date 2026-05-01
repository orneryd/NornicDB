package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func newMandatoryCapturing(t *testing.T, info ServiceInfo) (*slog.Logger, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	lv := &slog.LevelVar{}
	lv.Set(slog.LevelDebug)
	inner := newNornicdbJSONHandler(&buf, lv)
	mh := newMandatoryFieldsHandler(inner, info)
	return slog.New(mh), &buf
}

// TestMandatoryFields_AllFields — every record carries time (RFC3339Nano),
// level, msg, service, version, node_id.
func TestMandatoryFields_AllFields(t *testing.T) {
	info := ServiceInfo{Name: "nornicdb", Version: "v1.2.3", NodeID: "node-77"}
	logger, buf := newMandatoryCapturing(t, info)
	logger.Info("hello")

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec), "buf=%s", buf.String())

	require.Equal(t, "INFO", rec["level"])
	require.Equal(t, "hello", rec["msg"])
	require.Equal(t, "nornicdb", rec["service"])
	require.Equal(t, "v1.2.3", rec["version"])
	require.Equal(t, "node-77", rec["node_id"])
	require.NotEmpty(t, rec["time"])
	// Parseable timestamp (RFC3339Nano).
	_, err := time.Parse(time.RFC3339Nano, rec["time"].(string))
	require.NoError(t, err, "time must be RFC3339Nano-parseable; got %v", rec["time"])

	// trace_id/span_id absent for default ctx (Phase 1 noop, Pitfall 4).
	_, traceOk := rec["trace_id"]
	require.False(t, traceOk, "trace_id must be absent without active span; got: %v", rec)
	_, spanOk := rec["span_id"]
	require.False(t, spanOk, "span_id must be absent without active span")
}

// TestMandatoryFields_TraceContextResolved — wrap ctx with a tracetest
// SpanContext where IsValid()==true; assert trace_id (32 hex) and span_id (16 hex).
func TestMandatoryFields_TraceContextResolved(t *testing.T) {
	info := ServiceInfo{Name: "nornicdb", Version: "v0", NodeID: "n1"}
	logger, buf := newMandatoryCapturing(t, info)

	traceID, err := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("1112131415161718")
	require.NoError(t, err)

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	logger.InfoContext(ctx, "with-trace")

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	require.Equal(t, "0102030405060708090a0b0c0d0e0f10", rec["trace_id"])
	require.Equal(t, "1112131415161718", rec["span_id"])
}

// TestMandatoryFields_TraceIDOmittedWhenInvalid — default ctx (Phase 1 noop)
// yields ZERO trace_id/span_id keys (D-05 + Pitfall 4).
func TestMandatoryFields_TraceIDOmittedWhenInvalid(t *testing.T) {
	info := ServiceInfo{Name: "nornicdb", Version: "v0", NodeID: "n1"}
	logger, buf := newMandatoryCapturing(t, info)
	logger.InfoContext(context.Background(), "no-span")

	out := buf.String()
	require.NotContains(t, out, "trace_id", "trace_id must be absent on noop ctx")
	require.NotContains(t, out, "span_id", "span_id must be absent on noop ctx")
}

// BenchmarkMandatoryFields_NoActiveSpan — assert allocs/op stays low; documents
// the zero-overhead claim for Phase-1 noop default.
func BenchmarkMandatoryFields_NoActiveSpan(b *testing.B) {
	info := ServiceInfo{Name: "nornicdb", Version: "v0", NodeID: "n1"}
	var buf bytes.Buffer
	lv := &slog.LevelVar{}
	inner := newNornicdbJSONHandler(&buf, lv)
	mh := newMandatoryFieldsHandler(inner, info)
	logger := slog.New(mh)
	ctx := context.Background()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		logger.InfoContext(ctx, "x", "k", "v")
		buf.Reset()
	}
}

// TestMandatoryFields_WithAttrsPreservesWrapper — derived handlers retain
// captured service/version/node_id.
func TestMandatoryFields_WithAttrsPreservesWrapper(t *testing.T) {
	info := ServiceInfo{Name: "nornicdb", Version: "v1", NodeID: "n1"}
	logger, buf := newMandatoryCapturing(t, info)

	derived := logger.With("component", "test")
	derivedG := logger.WithGroup("g")
	require.NotNil(t, derived)
	require.NotNil(t, derivedG)

	derived.Info("hello")
	out := buf.String()
	require.Contains(t, out, `"service":"nornicdb"`)
	require.Contains(t, out, `"component":"test"`)
}

// TestMandatoryFields_EnabledDelegates — Enabled gates from inner.
func TestMandatoryFields_EnabledDelegates(t *testing.T) {
	lv := &slog.LevelVar{}
	lv.Set(slog.LevelError)
	var buf bytes.Buffer
	inner := newNornicdbJSONHandler(&buf, lv)
	mh := newMandatoryFieldsHandler(inner, ServiceInfo{Name: "x", Version: "y"})
	require.False(t, mh.Enabled(context.Background(), slog.LevelInfo))
	require.True(t, mh.Enabled(context.Background(), slog.LevelError))
}

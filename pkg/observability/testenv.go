package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestEnv carries per-test isolated observability primitives. It is the
// canonical TEST-01 fixture (ADR §2.8.1 / A10b) — every Phase 3+ test
// package SHOULD construct one of these via NewTestEnv(t).
//
// Each TestEnv has:
//   - its own *prometheus.Registry (never DefaultRegisterer);
//   - its own *tracetest.InMemoryExporter wired through SimpleSpanProcessor
//     so emitted spans are visible synchronously (no BSP batching);
//   - its own *slog.Logger using a discard handler (suppresses unless a
//     test explicitly writes against a captured handler);
//   - a *Provider built against those primitives (sampler:
//     sdktrace.AlwaysSample so tests CAN observe spans they emit, unlike
//     the production NeverSample default);
//   - a fresh *Health registry.
//
// Provider.Shutdown is registered on t.Cleanup automatically — callers
// don't need to call it explicitly.
type TestEnv struct {
	Registry *prometheus.Registry
	Exporter *tracetest.InMemoryExporter
	Logger   *slog.Logger
	Provider *Provider
	Health   *Health

	// Buffer is the lazily-allocated record-capture sink. Populated by the
	// first call to CaptureRecords(); nil otherwise. Per D-12 the discard
	// handler stays the default; tests opt-in to capture via CaptureRecords.
	Buffer *bytes.Buffer

	// captureMu serializes Buffer mutation across concurrent loggers (race
	// safety under -race -count=10). The bytes.Buffer itself is not
	// safe for concurrent writes; the slog.JSONHandler uses an internal
	// mutex for its own writes, but having all per-record bytes land
	// atomically requires the JSONHandler's serialization plus our own
	// guard around buffer ownership swaps.
	captureMu sync.Mutex
}

// NewTestEnv constructs an isolated observability environment for one
// test. Race-detector stable across `go test -race -count=10`.
//
// The constructed *Provider uses SimpleSpanProcessor(exp) + AlwaysSample
// rather than the production BSP + NeverSample combination, so tests can
// observe spans synchronously via env.Exporter.GetSpans(). This is a
// test-only path; production code goes through observability.New.
func NewTestEnv(t *testing.T) *TestEnv {
	t.Helper()

	reg := prometheus.NewRegistry()
	// MET-17 prep: same collectors as production newRegistry, so /metrics
	// in tests has the same shape as production.
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	exp := tracetest.NewInMemoryExporter()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	info := ServiceInfo{Name: "nornicdb-test", Version: "0.0.0"}
	res := buildResource(info)

	// OTel→Prom bridge against OUR registry (not DefaultRegisterer — TEST-01).
	bridge, err := otelprom.New(
		otelprom.WithRegisterer(reg),
		otelprom.WithoutUnits(),
		otelprom.WithNamespace("nornicdb_otel"),
	)
	if err != nil {
		t.Fatalf("NewTestEnv: otelprom.New: %v", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(bridge),
		sdkmetric.WithResource(res),
	)

	// SimpleSpanProcessor (NOT BSP) so tests see spans without flushing.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exp)),
		sdktrace.WithResource(res),
	)

	cfg := DefaultConfig()
	// Tests bind to ephemeral ports.
	cfg.Metrics.Listen = "127.0.0.1:0"
	cfg.Tracing.Enabled = false // exporter is wired manually above

	instanceID, instanceIDSrc := resolveInstanceID(info.NodeID)

	prov := &Provider{
		tracerProvider: tp,
		meterProvider:  mp,
		registry:       reg,
		serviceInfo:    info,
		instanceID:     instanceID,
		instanceIDSrc:  instanceIDSrc,
		metricsEnabled: true,
		cfg:            cfg,
	}

	h := NewHealth()

	t.Cleanup(func() {
		if err := prov.Shutdown(context.Background()); err != nil {
			t.Logf("TestEnv provider shutdown: %v", err)
		}
	})

	return &TestEnv{
		Registry: reg,
		Exporter: exp,
		Logger:   logger,
		Provider: prov,
		Health:   h,
	}
}

// CaptureRecords rewires te.Logger to write JSON records into te.Buffer
// (D-12). Idempotent: subsequent calls are no-ops, preserving any records
// already written. The default discard handler is replaced only on the
// first call so tests can opt-in to capture without resetting state.
//
// Concurrency: the underlying slog.JSONHandler serializes its writes via
// its own internal mutex; te.Buffer is therefore safe for concurrent
// loggers spawned after CaptureRecords returns. Use a sync.Mutex-guarded
// buffer wrapper if you need ordering guarantees across multiple goroutines.
func (te *TestEnv) CaptureRecords() {
	te.captureMu.Lock()
	defer te.captureMu.Unlock()
	if te.Buffer != nil {
		return
	}
	te.Buffer = &bytes.Buffer{}
	te.Logger = slog.New(slog.NewJSONHandler(&lockedWriter{w: te.Buffer}, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

// LoggedRecords parses the captured buffer line-by-line into a slice of
// JSON-decoded maps. Tolerates an empty buffer (returns nil) and skips
// blank trailing lines. Each call re-parses the buffer so tests CAN call
// it multiple times if they wish to observe streaming.
func (te *TestEnv) LoggedRecords() []map[string]any {
	te.captureMu.Lock()
	defer te.captureMu.Unlock()
	if te.Buffer == nil {
		return nil
	}
	raw := te.Buffer.Bytes()
	if len(raw) == 0 {
		return nil
	}
	var out []map[string]any
	for _, line := range bytes.Split(bytes.TrimRight(raw, "\n"), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			continue // skip malformed lines (e.g. partial concurrent writes)
		}
		out = append(out, rec)
	}
	return out
}

// lockedWriter serializes Write calls so concurrent loggers cannot
// interleave bytes mid-record. slog.JSONHandler already serializes within
// a single Logger but multi-goroutine fan-in into the same handler is the
// stress case under -race -count=10.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (lw *lockedWriter) Write(p []byte) (int, error) {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	return lw.w.Write(p)
}

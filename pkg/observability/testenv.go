package observability

import (
	"context"
	"io"
	"log/slog"
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

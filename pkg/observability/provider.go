package observability

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Provider is the entry point for all observability surfaces. Plan 03
// listeners and Plan 04 main.go consume Provider via its accessors.
//
// Provider is goroutine-safe for read; mutation after construction is
// forbidden.
type Provider struct {
	tracerProvider trace.TracerProvider // interface: SDK or noop
	meterProvider  *sdkmetric.MeterProvider
	registry       *prometheus.Registry
	serviceInfo    ServiceInfo
	instanceID     string
	instanceIDSrc  string
	metricsEnabled bool
	cfg            ObservabilityConfig

	// logger is the production *slog.Logger constructed via NewLogger and
	// passed into observability.New per D-08's two-phase bootstrap. May be
	// nil only in legacy test paths that bypass the new constructor.
	logger *slog.Logger

	// writerRef is the underlying writer the logger emits to (file/stderr/
	// stdout). Held so Provider.Shutdown can opportunistically attempt
	// Sync() per D-09a (captures drain-time finalize logs to disk).
	writerRef io.Writer

	// shutdownOnce makes Shutdown idempotent: Plan-03's telemetryListener
	// calls Provider.Shutdown as part of its OQ4 ordering, AND test fixtures
	// (TestEnv) call it from t.Cleanup. The OTel SDK returns "reader is
	// shutdown" on a second Shutdown call to MeterProvider, which is
	// surprising for a Provider whose docstring promises idempotency. The
	// sync.Once collapses the second-call path to a no-op success.
	shutdownOnce sync.Once
	shutdownErr  error
}

// New constructs a *Provider following the OBS-03 init order:
//
//  1. resource attributes (service.name/version/instance.id resolved via
//     OBS-10 chain).
//  2. Prometheus registry + OTel→Prom bridge (skipped when
//     cfg.Metrics.Enabled=false — OBS-04).
//  3. TracerProvider (SDK + BSP + OTLP exporter, OR noop on failure —
//     OBS-11).
//
// New NEVER returns a non-nil error from OTLP failure — telemetry init failure
// is logged at WARN and a noop tracer provider is installed. Process startup
// is unconditionally robust against observability misconfiguration.
//
// The provided ctx bounds OTLP exporter dial. A context-with-timeout derived
// from cfg.Tracing.Timeout (default 5s) further bounds the dial so a
// misconfigured collector cannot hang startup (Pitfall 2).
//
// Per D-08 two-phase bootstrap: the caller MUST call observability.NewLogger
// BEFORE this function so logger / writerRef can be threaded through. logger
// MAY be nil for legacy callers (provider falls back to a discard logger);
// writerRef MAY be nil (no Sync attempt during Shutdown).
func New(ctx context.Context, cfg ObservabilityConfig, info ServiceInfo, logger *slog.Logger, writerRef io.Writer) (*Provider, error) {
	// Step 1: Resource — also resolves and logs service.instance.id (OBS-10).
	res := buildResource(info)
	instanceID, instanceIDSrc := resolveInstanceID(info.NodeID)

	// Step 2: Registry + OTel→Prom bridge (OBS-04 — skipped when disabled).
	var (
		reg            *prometheus.Registry
		mp             *sdkmetric.MeterProvider
		metricsEnabled = cfg.Metrics.Enabled
	)
	if metricsEnabled {
		r, m, err := newRegistry(info)
		if err != nil {
			return nil, fmt.Errorf("observability: build registry: %w", err)
		}
		reg, mp = r, m
	}

	// Step 3: TracerProvider — SDK or noop (OBS-11).
	tp := buildTracerProvider(ctx, cfg.Tracing, res)

	return &Provider{
		tracerProvider: tp,
		meterProvider:  mp,
		registry:       reg,
		serviceInfo:    info,
		instanceID:     instanceID,
		instanceIDSrc:  instanceIDSrc,
		metricsEnabled: metricsEnabled,
		cfg:            cfg,
		logger:         logger,
		writerRef:      writerRef,
	}, nil
}

// buildTracerProvider constructs the real OTLP-backed TracerProvider, or
// returns a noop one (with WARN log) if the exporter cannot be initialized
// or configuration is unsafe (TRC-09 plaintext reject).
//
// Per OBS-11 contract this NEVER returns an error: telemetry init failure is
// logged and the noop provider is installed. The exporter init is bounded by
// cfg.Timeout (default 5s) via a context.WithTimeout so a misconfigured
// collector endpoint cannot hang startup (Pitfall 2).
//
// Phase 6 additions:
//   - TRC-09: reject plaintext OTLP endpoints (http:// scheme or explicit
//     Insecure=true with no override) when NORNICDB_OTLP_INSECURE != true.
//   - TRC-05/06/07: root sampler is built from cfg.ParentMode + SampleRatio
//     rather than NeverSample().
//   - TRC-11: W3C traceparent/tracestate/baggage set as global propagator.
//   - TRC-02: BSP is wrapped by bspSelfMetrics so nornicdb_otel_bsp_queue_depth
//   - _dropped_spans_total reflect real pipeline state.
func buildTracerProvider(ctx context.Context, cfg TracingConfig, res *resource.Resource) trace.TracerProvider {
	if !cfg.Enabled {
		return noop.NewTracerProvider()
	}

	// TRC-09: plaintext guard. If the resolved endpoint starts with http://
	// and neither cfg.Insecure nor NORNICDB_OTLP_INSECURE=true is set, we
	// refuse to build a real provider and fall back to noop + WARN. This is
	// a cheap check before we spend 5s dialing.
	if ep, _ := cfg.OTLPEndpoint(); strings.HasPrefix(strings.ToLower(ep), "http://") && !cfg.Insecure {
		log.Printf("WARN observability: OTLP endpoint %q is plaintext but NORNICDB_OTLP_INSECURE is not set; installing noop tracer provider (TRC-09)", ep)
		return noop.NewTracerProvider()
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	exporterCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	exporter, err := buildSpanExporter(exporterCtx, cfg)
	if err != nil {
		log.Printf("WARN observability: span exporter init failed: %v; installing noop tracer provider — process continues", err)
		return noop.NewTracerProvider()
	}

	// TRC-11: set the global propagator chain to W3C traceparent + tracestate +
	// baggage. Done here (not in New) so that a Provider with tracing disabled
	// leaves the global propagator untouched.
	// SEC-02: FilteredBaggagePropagator drops keys not in BaggageAllowList.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		FilteredBaggagePropagator{},
	))

	// TRC-02: wrap the real exporter-backed BSP with a span processor that
	// feeds queue depth + dropped-span state into the Phase-1 placeholder
	// Prometheus metrics. The inner BSP still owns the real export pipeline.
	bsp := sdktrace.NewBatchSpanProcessor(exporter,
		sdktrace.WithMaxQueueSize(8192),          // ADR §2.4.1 / A6
		sdktrace.WithMaxExportBatchSize(1024),    // ADR §2.4.1 / A6
		sdktrace.WithBatchTimeout(2*time.Second), // ADR §2.4.1 / A6
	)
	// SEC-01: redacting processor strips sensitive attributes before export.
	redactor := NewRedactingSpanProcessor(bsp)
	sp := newBSPSelfMetrics(redactor, 8192)

	// TRC-05/06/07: root sampler.
	mode, modeErr := parseParentMode(cfg.ParentMode)
	if modeErr != nil {
		log.Printf("WARN observability: %v; falling back to default sampler mode (TRC-05)", modeErr)
	}
	if mode == ParentModeStrict {
		log.Printf("WARN observability: parent_strict sampler honors upstream decisions unconditionally; trace volume is unbounded (TRC-07)")
	}
	sampler := buildSampler(mode, cfg.SampleRatio, cfg.ParentMaxQPS)

	return sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sampler),
		sdktrace.WithSpanProcessor(sp),
		sdktrace.WithResource(res),
	)
}

// otlpExporterOptions builds otlptracegrpc options honoring OBS-12 precedence.
//
// If the env var is set, we DO NOT pass WithEndpoint(yaml) — the SDK reads
// the env var directly. Pitfall 9: passing WithEndpoint would override the
// env var and silently break operator expectations.
func otlpExporterOptions(cfg TracingConfig) []otlptracegrpc.Option {
	var opts []otlptracegrpc.Option
	endpoint, fromEnv := cfg.OTLPEndpoint()
	if !fromEnv && endpoint != "" {
		opts = append(opts, otlptracegrpc.WithEndpoint(endpoint))
	}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	if cfg.Timeout > 0 {
		opts = append(opts, otlptracegrpc.WithTimeout(cfg.Timeout))
	}
	return opts
}

// buildSpanExporter selects the exporter implementation based on cfg.Protocol:
//   - "grpc" (default): OTLP/gRPC (TRC-01)
//   - "http":           OTLP/HTTP fallback (TRC-03)
//   - "stdout":         stdout exporter for DEV builds (TRC-04)
func buildSpanExporter(ctx context.Context, cfg TracingConfig) (sdktrace.SpanExporter, error) {
	switch strings.ToLower(cfg.Protocol) {
	case "http":
		var opts []otlptracehttp.Option
		endpoint, fromEnv := cfg.OTLPEndpoint()
		if !fromEnv && endpoint != "" {
			opts = append(opts, otlptracehttp.WithEndpoint(endpoint))
		}
		if cfg.Insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		if cfg.Timeout > 0 {
			opts = append(opts, otlptracehttp.WithTimeout(cfg.Timeout))
		}
		return otlptracehttp.New(ctx, opts...)
	case "stdout":
		return stdouttrace.New(stdouttrace.WithPrettyPrint())
	default:
		opts := otlpExporterOptions(cfg)
		return otlptracegrpc.New(ctx, opts...)
	}
}

// TracerProvider returns the tracer provider. Always non-nil; may be a noop
// (when cfg.Tracing.Enabled=false OR OTLP exporter init failed — OBS-11).
func (p *Provider) TracerProvider() trace.TracerProvider { return p.tracerProvider }

// MeterProvider returns the OTel meter provider. nil when metrics disabled.
func (p *Provider) MeterProvider() *sdkmetric.MeterProvider { return p.meterProvider }

// Registry returns the Prometheus registry. nil when metrics disabled (OBS-04).
// Plan 03 listener uses this nil-ness to skip /metrics handler registration.
func (p *Provider) Registry() *prometheus.Registry { return p.registry }

// InstanceID returns the resolved service.instance.id (OBS-10).
func (p *Provider) InstanceID() string { return p.instanceID }

// InstanceIDSource returns the resolution leg that fired ("config", "POD_NAME",
// "hostname", or "fallback"). Useful for Plan 03 /version handler.
func (p *Provider) InstanceIDSource() string { return p.instanceIDSrc }

// MetricsEnabled mirrors cfg.Metrics.Enabled (OBS-04).
func (p *Provider) MetricsEnabled() bool { return p.metricsEnabled }

// Logger returns the production *slog.Logger that downstream business
// packages (pkg/server, pkg/cypher, pkg/storage, pkg/bolt) consume per the
// D-01 constructor-injection pattern. Returns nil if the Provider was
// constructed via a legacy code path that did not pass a logger; callers
// SHOULD nil-guard with `slog.New(slog.NewTextHandler(io.Discard, nil))` as
// a fallback.
func (p *Provider) Logger() *slog.Logger { return p.logger }

// Config returns a copy of the construction-time config.
func (p *Provider) Config() ObservabilityConfig { return p.cfg }

// Shutdown flushes the BSP and shuts down the meter provider. Idempotent in
// the sense that it can be called multiple times safely; the underlying SDK
// providers are themselves idempotent on Shutdown.
//
// Called by the telemetry listener's Shutdown in Plan 03 (per Open Question 4
// resolution — the lifecycle.Component owns the Provider's flush budget).
func (p *Provider) Shutdown(ctx context.Context) error {
	p.shutdownOnce.Do(func() {
		var errs error
		// noop.NewTracerProvider() returns a trace.TracerProvider interface that
		// has no Shutdown method; only the SDK provider does. Type-assert to
		// handle both paths cleanly.
		if tp, ok := p.tracerProvider.(interface {
			Shutdown(context.Context) error
		}); ok {
			if err := tp.Shutdown(ctx); err != nil {
				errs = errors.Join(errs, fmt.Errorf("tracer provider shutdown: %w", err))
			}
		}
		if p.meterProvider != nil {
			if err := p.meterProvider.Shutdown(ctx); err != nil {
				errs = errors.Join(errs, fmt.Errorf("meter provider shutdown: %w", err))
			}
		}
		// D-09a: opportunistic Sync() against the logger writer. *os.File
		// flushes to disk; *bytes.Buffer (tests) and the standard streams
		// no-op. Cost ~1-3ms; well under the 5s flush budget. Errors are
		// intentionally swallowed — Sync failure during shutdown is not
		// actionable, and we do not want it to mask earlier exporter errors.
		if syncer, ok := p.writerRef.(interface{ Sync() error }); ok {
			_ = syncer.Sync()
		}
		p.shutdownErr = errs
	})
	return p.shutdownErr
}

package observability

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
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
func New(ctx context.Context, cfg ObservabilityConfig, info ServiceInfo) (*Provider, error) {
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
	}, nil
}

// buildTracerProvider constructs the real OTLP-backed TracerProvider, or
// returns a noop one (with WARN log) if the exporter cannot be initialized.
//
// Per OBS-11 contract this NEVER returns an error: telemetry init failure is
// logged and the noop provider is installed. The exporter init is bounded by
// cfg.Timeout (default 5s) via a context.WithTimeout so a misconfigured
// collector endpoint cannot hang startup (Pitfall 2).
func buildTracerProvider(ctx context.Context, cfg TracingConfig, res *resource.Resource) trace.TracerProvider {
	if !cfg.Enabled {
		return noop.NewTracerProvider()
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	exporterCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	opts := otlpExporterOptions(cfg)
	exporter, err := otlptracegrpc.New(exporterCtx, opts...)
	if err != nil {
		log.Printf("WARN observability: OTLP/gRPC exporter init failed: %v; installing noop tracer provider — process continues", err)
		return noop.NewTracerProvider()
	}

	bsp := sdktrace.NewBatchSpanProcessor(exporter,
		sdktrace.WithMaxQueueSize(8192),          // ADR §2.4.1 / A6
		sdktrace.WithMaxExportBatchSize(1024),    // ADR §2.4.1 / A6
		sdktrace.WithBatchTimeout(2*time.Second), // ADR §2.4.1 / A6
	)

	return sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.NeverSample()), // Phase-1 default per D-02; Phase 6 swaps
		sdktrace.WithSpanProcessor(bsp),
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

// Config returns a copy of the construction-time config.
func (p *Provider) Config() ObservabilityConfig { return p.cfg }

// Shutdown flushes the BSP and shuts down the meter provider. Idempotent in
// the sense that it can be called multiple times safely; the underlying SDK
// providers are themselves idempotent on Shutdown.
//
// Called by the telemetry listener's Shutdown in Plan 03 (per Open Question 4
// resolution — the lifecycle.Component owns the Provider's flush budget).
func (p *Provider) Shutdown(ctx context.Context) error {
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
	return errs
}

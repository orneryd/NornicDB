package observability

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Provider is the entry point for all observability surfaces. Plan 03 listeners
// and Plan 04 main.go consume Provider via its accessors.
//
// Construct via New. Provider is goroutine-safe for read; mutation is forbidden
// after construction.
type Provider struct {
	tracerProvider trace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
	registry       *prometheus.Registry
	serviceInfo    ServiceInfo
	instanceID     string
	instanceIDSrc  string
	metricsEnabled bool
	cfg            ObservabilityConfig
}

// New is the Phase-1 stub. Full pipeline lands in Task 01-02-02.
func New(ctx context.Context, cfg ObservabilityConfig, info ServiceInfo) (*Provider, error) {
	_ = ctx
	return &Provider{
		tracerProvider: noop.NewTracerProvider(),
		serviceInfo:    info,
		cfg:            cfg,
	}, nil
}

// TracerProvider returns the tracer provider. Always non-nil; may be a noop.
func (p *Provider) TracerProvider() trace.TracerProvider { return p.tracerProvider }

// MeterProvider returns the OTel meter provider. May be nil if metrics disabled.
func (p *Provider) MeterProvider() *sdkmetric.MeterProvider { return p.meterProvider }

// Registry returns the Prometheus registry. nil when metrics disabled (OBS-04).
func (p *Provider) Registry() *prometheus.Registry { return p.registry }

// InstanceID returns the resolved service.instance.id (OBS-10).
func (p *Provider) InstanceID() string { return p.instanceID }

// MetricsEnabled mirrors the cfg.Metrics.Enabled flag (OBS-04).
func (p *Provider) MetricsEnabled() bool { return p.metricsEnabled }

// Config returns a copy of the construction-time config.
func (p *Provider) Config() ObservabilityConfig { return p.cfg }

// Shutdown is the Phase-1 stub. Full BSP-drain lands in Task 01-02-02.
func (p *Provider) Shutdown(ctx context.Context) error {
	_ = ctx
	return nil
}

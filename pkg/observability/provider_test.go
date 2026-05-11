package observability

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProvider_InitOrder covers OBS-03: init order logger → resource →
// providers → registries. Verified by side-effect on the resulting Provider.
func TestProvider_InitOrder(t *testing.T) {
	cfg := ObservabilityConfig{
		Metrics: MetricsConfig{Enabled: true},
		Tracing: TracingConfig{Enabled: false},
	}
	info := ServiceInfo{Name: "nornicdb", Version: "test", NodeID: "node-init-order"}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	prov, err := New(ctx, cfg, info, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, prov)
	t.Cleanup(func() { _ = prov.Shutdown(context.Background()) })

	// Resource resolved BEFORE providers built → InstanceID non-empty.
	assert.NotEmpty(t, prov.InstanceID(), "instanceID must be resolved before providers built")
	// Registry built (metrics enabled) → non-nil.
	assert.NotNil(t, prov.Registry(), "registry must be built when metrics enabled")
	// TracerProvider built last → non-nil.
	assert.NotNil(t, prov.TracerProvider(), "tracer provider must be non-nil")
}

// TestProvider_MetricsDisabled covers OBS-04: metrics.enabled=false → no
// metrics surface (Plan 03 listener will skip /metrics handler registration).
func TestProvider_MetricsDisabled(t *testing.T) {
	cfg := ObservabilityConfig{
		Metrics: MetricsConfig{Enabled: false},
		Tracing: TracingConfig{Enabled: false},
	}
	info := ServiceInfo{Name: "nornicdb", Version: "test"}

	prov, err := New(context.Background(), cfg, info, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, prov)
	t.Cleanup(func() { _ = prov.Shutdown(context.Background()) })

	assert.False(t, prov.MetricsEnabled())
	assert.Nil(t, prov.Registry(), "registry must be nil when metrics disabled (OBS-04)")
}

// TestProvider_OTLPFailureUsesNoop is the OBS-11 keystone test:
// bad endpoint never crashes startup AND never hangs waiting for collector.
func TestProvider_OTLPFailureUsesNoop(t *testing.T) {
	cfg := ObservabilityConfig{
		Tracing: TracingConfig{
			Enabled:  true,
			Endpoint: "127.0.0.1:1", // port 1: nothing listens here
			Protocol: "grpc",
			Insecure: true,
			Timeout:  200 * time.Millisecond,
		},
		Metrics: MetricsConfig{Enabled: true},
	}
	info := ServiceInfo{Name: "nornicdb", Version: "test", NodeID: "test-node"}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	prov, err := New(ctx, cfg, info, nil, nil)
	elapsed := time.Since(start)

	require.NoError(t, err, "OBS-11: New must not propagate OTLP init failure")
	require.NotNil(t, prov)
	t.Cleanup(func() { _ = prov.Shutdown(context.Background()) })

	require.Less(t, elapsed, time.Second, "New must not block on OTLP dial")

	tp := prov.TracerProvider()
	require.NotNil(t, tp)
	_, span := tp.Tracer("test").Start(context.Background(), "x")
	require.False(t, span.IsRecording(), "OBS-11: noop provider must produce non-recording spans")
	span.End()
}

// TestProvider_TracingDisabled — when cfg.Tracing.Enabled=false, the provider
// should still install a noop tracer (no SDK provider, no exporter init).
func TestProvider_TracingDisabled(t *testing.T) {
	cfg := ObservabilityConfig{
		Metrics: MetricsConfig{Enabled: true},
		Tracing: TracingConfig{Enabled: false},
	}
	info := ServiceInfo{Name: "nornicdb", Version: "test"}

	prov, err := New(context.Background(), cfg, info, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = prov.Shutdown(context.Background()) })

	tp := prov.TracerProvider()
	_, span := tp.Tracer("x").Start(context.Background(), "y")
	assert.False(t, span.IsRecording())
	span.End()
}

// TestProvider_Accessors covers the small read-only accessors (MeterProvider,
// InstanceIDSource, Config) so the package coverage hits ≥90%.
func TestProvider_Accessors(t *testing.T) {
	cfg := ObservabilityConfig{
		Metrics: MetricsConfig{Enabled: true, Listen: ":9090"},
		Tracing: TracingConfig{Enabled: false},
		Pprof:   PprofConfig{Listen: "127.0.0.1:9091"},
	}
	info := ServiceInfo{Name: "nornicdb", Version: "v0", NodeID: "n1"}

	prov, err := New(context.Background(), cfg, info, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = prov.Shutdown(context.Background()) })

	assert.NotNil(t, prov.MeterProvider())
	assert.Equal(t, "config", prov.InstanceIDSource())
	assert.Equal(t, ":9090", prov.Config().Metrics.Listen)
}

// TestProvider_Logger — when New is called with a logger + writerRef,
// Provider.Logger() returns the same instance.
func TestProvider_Logger(t *testing.T) {
	cfg := ObservabilityConfig{
		Metrics: MetricsConfig{Enabled: false},
		Tracing: TracingConfig{Enabled: false},
	}
	logger, writer, err := NewLogger(LoggerConfig{Level: "info", Output: "stderr"}, ServiceInfo{Name: "x", Version: "y"})
	require.NoError(t, err)
	require.NotNil(t, logger)

	prov, err := New(context.Background(), cfg, ServiceInfo{Name: "x", Version: "y"}, logger, writer)
	require.NoError(t, err)
	t.Cleanup(func() { _ = prov.Shutdown(context.Background()) })

	require.Same(t, logger, prov.Logger(), "Provider.Logger() must return the injected *slog.Logger")
}

// TestProvider_Logger_NilCallerOK — a legacy nil-logger caller still gets
// a usable Provider; Provider.Logger() returns nil and callers MUST
// nil-guard (documented contract).
func TestProvider_Logger_NilCallerOK(t *testing.T) {
	cfg := ObservabilityConfig{
		Metrics: MetricsConfig{Enabled: false},
		Tracing: TracingConfig{Enabled: false},
	}
	prov, err := New(context.Background(), cfg, ServiceInfo{Name: "x", Version: "y"}, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = prov.Shutdown(context.Background()) })
	require.Nil(t, prov.Logger())
}

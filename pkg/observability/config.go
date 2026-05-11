package observability

import (
	"os"
	"strings"
	"time"
)

// ObservabilityConfig is the root telemetry config block bound from
// nornicdb.yaml's `observability:` section and overlaid with NORNICDB_*
// env vars.
type ObservabilityConfig struct {
	// Metrics controls the :9090 Prometheus surface.
	Metrics MetricsConfig
	// Tracing controls the OTLP trace exporter.
	Tracing TracingConfig
	// Pprof controls the optional :9091 pprof listener.
	Pprof PprofConfig
}

// MetricsConfig governs the :9090/metrics surface.
type MetricsConfig struct {
	// Enabled defaults to true. When false, pkg/observability.New returns a
	// Provider with a nil registry and the Plan-03 listener does NOT register
	// the /metrics handler (OBS-04).
	Enabled bool
	// Listen is the bind address for the telemetry mux. Default ":9090".
	Listen string
	// TenantLabelsEnabled is the resolved per-process tenant-labels switch.
	// Phase 5 startup hook (cmd/nornicdb/main.go) writes this from the
	// explicit YAML value (TenantLabelsExplicit) or K8s autodetect
	// (DefaultK8sProbe + ResolveTenantLabels) BEFORE any Phase 4 bag
	// constructor reads it. Bag constructors continue to read this bool
	// directly per Phase 4 D-08 plumbing — do NOT bypass them by reading
	// TenantLabelsExplicit. Default false (D-02c) until startup hook runs.
	TenantLabelsEnabled bool
	// TenantLabelsExplicit is the operator's YAML intent before defaulting.
	// nil = field omitted in YAML; non-nil = operator explicitly set true
	// or false. Phase 5 ResolveTenantLabels reads this to enforce
	// precedence (explicit YAML > K8s autodetect > default false).
	// R-02: this sentinel is the smallest blast-radius change to allow
	// YAML "explicit false" to win over autodetect "true on K8s".
	TenantLabelsExplicit *bool
}

// TracingConfig governs OTLP/gRPC trace egress.
type TracingConfig struct {
	// Enabled gates whether the SDK TracerProvider is built. When false a noop
	// provider is installed and no exporter is initialized.
	Enabled bool
	// Endpoint is the YAML-configured OTLP collector address. The actual
	// endpoint used at runtime is resolved by OTLPEndpoint() — env vars
	// override this value (OBS-12).
	Endpoint string
	// Protocol is "grpc" (default) or "http". Phase 1 wires gRPC; HTTP is the
	// fallback for Phase 6 hardening.
	Protocol string
	// Insecure controls TLS for the OTLP connection.
	Insecure bool
	// Timeout bounds exporter init. Default 5s. A misconfigured collector
	// MUST NOT hang process startup (OBS-11).
	Timeout time.Duration
}

// PprofConfig governs the optional :9091 debug surface.
type PprofConfig struct {
	// Enabled gates whether the pprof listener is started.
	Enabled bool
	// Listen is the bind address. Default "127.0.0.1:9091" per ADR §A9 to
	// keep pprof off non-loopback interfaces by default.
	Listen string
}

// DefaultConfig returns the Phase-1 defaults. Operators may override any
// field via YAML or NORNICDB_* env vars.
func DefaultConfig() ObservabilityConfig {
	return ObservabilityConfig{
		Metrics: MetricsConfig{
			Enabled:              true,
			Listen:               ":9090",
			TenantLabelsEnabled:  false,
			TenantLabelsExplicit: nil, // R-02: nil sentinel = "no explicit operator intent".
		},
		Tracing: TracingConfig{
			Enabled:  false,
			Protocol: "grpc",
			Timeout:  5 * time.Second,
		},
		Pprof: PprofConfig{
			Enabled: false,
			Listen:  "127.0.0.1:9091",
		},
	}
}

// OTLPEndpoint resolves the OBS-12 precedence chain:
//
//	OTEL_EXPORTER_OTLP_ENDPOINT > OTEL_EXPORTER_OTLP_TRACES_ENDPOINT >
//	c.Endpoint (YAML) > "" (default — caller installs noop or relies on SDK
//	default).
//
// The fromEnv return tells the caller whether the value came from an env var.
// When fromEnv is true, the caller MUST NOT pass otlptracegrpc.WithEndpoint
// (the SDK reads the env var itself; passing the option overrides the env
// — see Pitfall 9).
func (c TracingConfig) OTLPEndpoint() (endpoint string, fromEnv bool) {
	if v := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")); v != "" {
		return v, true
	}
	if v := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")); v != "" {
		return v, true
	}
	return strings.TrimSpace(c.Endpoint), false
}

// ApplyEnv overlays NORNICDB_* env vars onto c. Env vars take precedence over
// YAML-set fields (env > YAML > default), per OBS-02.
//
// OTEL_EXPORTER_OTLP_* env vars are intentionally NOT consumed here — they
// are read on-demand by OTLPEndpoint() at exporter-init time, otherwise the
// resolved value would be frozen at config-load and break OBS-12 precedence.
func (c *ObservabilityConfig) ApplyEnv() {
	if v := os.Getenv("NORNICDB_TELEMETRY_LISTEN"); v != "" {
		c.Metrics.Listen = v
	}
	// Port-only fallback. Only honored if the full Listen is unset.
	if v := os.Getenv("NORNICDB_TELEMETRY_PORT"); v != "" && c.Metrics.Listen == "" {
		c.Metrics.Listen = ":" + v
	}
	if v := os.Getenv("NORNICDB_TRACING_ENABLED"); v != "" {
		c.Tracing.Enabled = v == "true"
	}
	if v := os.Getenv("NORNICDB_PPROF_ENABLED"); v != "" {
		c.Pprof.Enabled = v == "true"
	}
	if v := os.Getenv("NORNICDB_PPROF_LISTEN"); v != "" {
		c.Pprof.Listen = v
	}
}

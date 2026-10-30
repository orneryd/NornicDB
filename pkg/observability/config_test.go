package observability

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConfig_YAMLAndEnvBinding asserts that ObservabilityConfig values can be
// constructed and that ApplyEnv overlays NORNICDB_* env vars on top of a
// YAML-derived base.
//
// Covers: OBS-02 (config block + YAML/env binding).
func TestConfig_YAMLAndEnvBinding(t *testing.T) {
	tests := []struct {
		name      string
		envVars   map[string]string
		base      ObservabilityConfig
		assertion func(t *testing.T, c ObservabilityConfig)
	}{
		{
			name: "yaml only",
			base: ObservabilityConfig{
				Metrics: MetricsConfig{Enabled: true, Listen: ":9090"},
				Tracing: TracingConfig{Enabled: false, Endpoint: "yaml-host:4317"},
				Pprof:   PprofConfig{Enabled: false, Listen: "127.0.0.1:9091"},
			},
			assertion: func(t *testing.T, c ObservabilityConfig) {
				assert.True(t, c.Metrics.Enabled)
				assert.Equal(t, ":9090", c.Metrics.Listen)
				assert.Equal(t, "yaml-host:4317", c.Tracing.Endpoint)
				assert.False(t, c.Pprof.Enabled)
			},
		},
		{
			name: "env only",
			envVars: map[string]string{
				"NORNICDB_TELEMETRY_LISTEN": ":19090",
				"NORNICDB_PPROF_ENABLED":    "true",
				"NORNICDB_PPROF_LISTEN":     "127.0.0.1:19091",
				"NORNICDB_TRACING_ENABLED":  "true",
			},
			base: ObservabilityConfig{},
			assertion: func(t *testing.T, c ObservabilityConfig) {
				assert.Equal(t, ":19090", c.Metrics.Listen)
				assert.True(t, c.Pprof.Enabled)
				assert.Equal(t, "127.0.0.1:19091", c.Pprof.Listen)
				assert.True(t, c.Tracing.Enabled)
			},
		},
		{
			name: "env overrides yaml",
			envVars: map[string]string{
				"NORNICDB_TELEMETRY_LISTEN": ":29090",
			},
			base: ObservabilityConfig{
				Metrics: MetricsConfig{Listen: ":9090"},
			},
			assertion: func(t *testing.T, c ObservabilityConfig) {
				assert.Equal(t, ":29090", c.Metrics.Listen)
			},
		},
		{
			name: "defaults when neither",
			base: DefaultConfig(),
			assertion: func(t *testing.T, c ObservabilityConfig) {
				assert.Equal(t, ":9090", c.Metrics.Listen)
				assert.True(t, c.Metrics.Enabled)
				assert.Equal(t, "127.0.0.1:9091", c.Pprof.Listen)
				assert.Equal(t, 5*time.Second, c.Tracing.Timeout)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}
			cfg := tt.base
			cfg.ApplyEnv()
			tt.assertion(t, cfg)
		})
	}
}

// TestOTLPConfig_PrecedenceOrder verifies OBS-12: env > YAML > default.
//
// Covers: OBS-12 (OTLP endpoint precedence).
func TestOTLPConfig_PrecedenceOrder(t *testing.T) {
	tests := []struct {
		name         string
		envEndpoint  string
		envTraces    string
		yamlEndpoint string
		wantEndpoint string
		wantFromEnv  bool
	}{
		{
			name:         "env wins over yaml",
			envEndpoint:  "https://env-collector:4317",
			yamlEndpoint: "yaml-collector:4317",
			wantEndpoint: "https://env-collector:4317",
			wantFromEnv:  true,
		},
		{
			name:         "yaml wins when env unset",
			yamlEndpoint: "yaml-collector:4317",
			wantEndpoint: "yaml-collector:4317",
			wantFromEnv:  false,
		},
		{
			name:         "default empty when neither set",
			wantEndpoint: "",
			wantFromEnv:  false,
		},
		{
			name:         "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT honored",
			envTraces:    "https://traces-only:4317",
			yamlEndpoint: "yaml-collector:4317",
			wantEndpoint: "https://traces-only:4317",
			wantFromEnv:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", tt.envEndpoint)
			t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", tt.envTraces)
			cfg := TracingConfig{Endpoint: tt.yamlEndpoint}
			ep, fromEnv := cfg.OTLPEndpoint()
			require.Equal(t, tt.wantEndpoint, ep)
			require.Equal(t, tt.wantFromEnv, fromEnv)
		})
	}
}

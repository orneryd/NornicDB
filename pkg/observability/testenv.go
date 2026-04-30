package observability

// Plan 03 Task 01 compile-only stub. Replaced by full implementation in
// Plan 03 Task 02.

import (
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type TestEnv struct {
	Registry *prometheus.Registry
	Exporter *tracetest.InMemoryExporter
	Logger   *slog.Logger
	Provider *Provider
	Health   *Health
}

// NewTestEnv is a placeholder satisfied by Task 02.
func NewTestEnv(t *testing.T) *TestEnv {
	t.Helper()
	return nil
}

package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// newRegistry is the Phase-1 stub. Full bridge wiring lands in Task 01-02-02.
func newRegistry(serviceInfo ServiceInfo) (*prometheus.Registry, *sdkmetric.MeterProvider, error) {
	_ = serviceInfo
	return nil, nil, nil
}

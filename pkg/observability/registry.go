package observability

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// newRegistry constructs a process-isolated *prometheus.Registry plus the
// OTel→Prom bridge MeterProvider that emits into it.
//
// The returned registry is suitable for `promhttp.HandlerFor(reg, ...)` in
// Plan 03's listener. It includes:
//
//   - Go runtime collector (collectors.NewGoCollector — MET-17 prep).
//   - Process collector (collectors.NewProcessCollector — MET-17 prep).
//   - OTel→Prom bridge with WithoutUnits() (suppresses auto-suffix per
//     ADR §2.2.1 / A1, Pitfall 2) and WithNamespace("nornicdb_otel") so
//     bridge-emitted metrics never collide with hand-instrumented
//     nornicdb_* series.
//   - BSP self-instrumentation gauges as Phase-1 placeholders. Population
//     ships in Phase 6 (TRC-02) once a custom SpanProcessor wrapper exposes
//     queue depth and dropped-span counts.
//
// MustRegister is used here because reg is fresh and duplicate registration
// is a programming bug — the panic IS the desired startup behavior (Pitfall 8).
func newRegistry(serviceInfo ServiceInfo) (*prometheus.Registry, *sdkmetric.MeterProvider, error) {
	reg := prometheus.NewRegistry()

	// MET-17 prep: stdlib runtime + process collectors.
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	// Phase-1 BSP placeholders (TRC-02 foundation; populated in Phase 6).
	// Declaring them now means Phase 6 only has to populate, not register.
	bspQueueDepth := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "nornicdb_otel",
		Subsystem: "bsp",
		Name:      "queue_depth",
		Help:      "Current depth of the BatchSpanProcessor queue (TRC-02).",
	})
	bspDroppedSpans := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "nornicdb_otel",
		Subsystem: "bsp",
		Name:      "dropped_spans_total",
		Help:      "Cumulative spans dropped due to BSP backpressure (TRC-02).",
	})
	reg.MustRegister(bspQueueDepth)
	reg.MustRegister(bspDroppedSpans)

	// Phase 6 TRC-02: publish the metric refs so buildTracerProvider's
	// bspSelfMetrics wrapper can feed queue depth + drop counts.
	setBSPMetricsRefs(bspQueueDepth, bspDroppedSpans)

	// TRC-24: sample-rate-mismatch counter. Registered now (Phase 6) so the
	// family is scrape-discoverable; actual increment wiring ships in Phase 7
	// when replication peers exchange codec version + sample-rate metadata.
	sampleRateMismatch := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "nornicdb_otel",
		Name:      "sample_rate_mismatch_total",
		Help:      "Cumulative peer-replica sample-rate mismatches observed (TRC-24). Populated by Phase 7 replication wiring.",
	})
	reg.MustRegister(sampleRateMismatch)

	// OTel→Prom bridge.
	//   - WithRegisterer pins the bridge to OUR registry (NOT prometheus.DefaultRegisterer
	//     — that's TEST-01's poison).
	//   - WithoutUnits suppresses OTel's auto-suffix translation
	//     (`_milliseconds_total`, `_bytes`) per ADR §2.2.1 / A1.
	//   - WithNamespace puts bridge-emitted series under nornicdb_otel_*.
	promExporter, err := otelprom.New(
		otelprom.WithRegisterer(reg),
		otelprom.WithoutUnits(),
		otelprom.WithNamespace("nornicdb_otel"),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("otelprom.New: %w", err)
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(promExporter),
		sdkmetric.WithResource(buildResource(serviceInfo)),
	)

	return reg, mp, nil
}

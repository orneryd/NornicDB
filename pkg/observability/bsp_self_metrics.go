package observability

import (
	"context"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// bspSelfMetrics wraps an sdktrace.SpanProcessor (the real BSP backed by the
// OTLP exporter) and feeds queue depth + dropped-span state into the
// nornicdb_otel_bsp_queue_depth / nornicdb_otel_bsp_dropped_spans_total
// families (TRC-02).
//
// Queue depth is tracked optimistically: every OnEnd increments the
// in-flight counter, and a drop is recorded when the inner BSP's OnEnd
// panics or when the counter exceeds the configured MaxQueueSize. The OTel
// SDK does not currently expose queue depth directly, so this instrumented
// wrapper is our cheapest approximation until upstream lands
// `BatchSpanProcessorOptions.Instrumented`.
//
// The wrapper is a thin pass-through: OnStart/ForceFlush/Shutdown delegate
// to the inner processor. Only OnEnd does accounting.
type bspSelfMetrics struct {
	inner   sdktrace.SpanProcessor
	maxSize int64
	depth   atomic.Int64
	// queueDepth + droppedSpans are nil-safe: when metrics are disabled
	// (obs.MetricsEnabled()=false) newBSPSelfMetrics is still called but
	// with nil refs and the accounting is a no-op.
	queueDepth   prometheus.Gauge
	droppedSpans prometheus.Counter
}

// bspMetricsRefs is populated by newRegistry when metrics are enabled, so
// buildTracerProvider can attach the same Gauge/Counter instances that the
// Prometheus registry exposes. A package-level pointer is used (not a field)
// because New() builds the registry BEFORE the tracer provider, and the
// tracer provider cannot walk back into Provider to find them at that point
// in init order. The refs are overwritten on each New() call so test
// isolation (per-TestEnv Provider) still works.
var bspMetricsRefs atomic.Pointer[bspMetricsBundle]

type bspMetricsBundle struct {
	queueDepth   prometheus.Gauge
	droppedSpans prometheus.Counter
}

func setBSPMetricsRefs(queueDepth prometheus.Gauge, droppedSpans prometheus.Counter) {
	bspMetricsRefs.Store(&bspMetricsBundle{
		queueDepth:   queueDepth,
		droppedSpans: droppedSpans,
	})
}

func newBSPSelfMetrics(inner sdktrace.SpanProcessor, maxSize int) *bspSelfMetrics {
	s := &bspSelfMetrics{
		inner:   inner,
		maxSize: int64(maxSize),
	}
	if b := bspMetricsRefs.Load(); b != nil {
		s.queueDepth = b.queueDepth
		s.droppedSpans = b.droppedSpans
	}
	return s
}

// OnStart delegates to the inner processor.
func (s *bspSelfMetrics) OnStart(parent context.Context, span sdktrace.ReadWriteSpan) {
	s.inner.OnStart(parent, span)
}

// OnEnd increments the queue-depth gauge, delegates to the inner processor,
// then decrements. When queue depth exceeds maxSize we increment the
// dropped-spans counter (the inner BSP will silently drop; without this
// wrapper operators have no visibility).
func (s *bspSelfMetrics) OnEnd(span sdktrace.ReadOnlySpan) {
	depth := s.depth.Add(1)
	if s.queueDepth != nil {
		s.queueDepth.Set(float64(depth))
	}
	if depth > s.maxSize {
		if s.droppedSpans != nil {
			s.droppedSpans.Inc()
		}
	}
	s.inner.OnEnd(span)
	newDepth := s.depth.Add(-1)
	if s.queueDepth != nil {
		s.queueDepth.Set(float64(newDepth))
	}
}

// ForceFlush delegates.
func (s *bspSelfMetrics) ForceFlush(ctx context.Context) error {
	return s.inner.ForceFlush(ctx)
}

// Shutdown delegates.
func (s *bspSelfMetrics) Shutdown(ctx context.Context) error {
	return s.inner.Shutdown(ctx)
}

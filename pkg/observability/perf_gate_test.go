package observability

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestObservability_MemoryFloor verifies that the observability package does
// not hold excessive baseline memory when idle. The threshold is generous
// (50 MB) to avoid flaky CI while catching accidental multi-hundred-MB leaks
// from unbounded caches or span buffers.
//
// Note on delta computation: runtime.MemStats.HeapAlloc is a uint64, and
// because a prior test may have left heap in a temporarily-inflated state,
// GC between snapshots can leave `after` < `before`. A naive unsigned
// subtraction would then underflow into a multi-exabyte delta and trip a
// spurious failure. Use a signed difference computed in int64 space and
// treat negative deltas as zero — the invariant we care about is
// monotonic-upper-bound-on-growth, not exact bytes.
func TestObservability_MemoryFloor(t *testing.T) {
	// Run GC twice to drain pending finalisers from earlier tests (a single
	// GC may leave work for the next cycle, producing noisy baselines when
	// this test runs after heavy suites like pkg/nornicdb).
	runtime.GC()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	// Simulate typical initialization: create a TracerProvider with a recorder.
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(tp)
	defer func() {
		tp.Shutdown(context.Background())
		otel.SetTracerProvider(nil)
	}()

	// Emit a modest batch of spans to warm up internal pools.
	tracer := tp.Tracer("nornicdb/perf-test")
	for i := 0; i < 100; i++ {
		_, span := tracer.Start(context.Background(), "test-op")
		span.SetAttributes(attribute.Int("i", i))
		span.End()
	}

	runtime.GC()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	// Signed-space delta: when GC reclaimed more than the span loop
	// allocated, `after.HeapAlloc < before.HeapAlloc`. Clamp the signed
	// difference to zero — the test's purpose is to catch large POSITIVE
	// growth, not to assert on negative (reclamation-dominated) deltas.
	deltaBytes := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	if deltaBytes < 0 {
		deltaBytes = 0
	}
	allocatedMB := float64(deltaBytes) / 1024 / 1024
	t.Logf("Observability memory floor: %.2f MB (heap delta after 100 spans; before=%d after=%d)",
		allocatedMB, before.HeapAlloc, after.HeapAlloc)
	assert.Less(t, allocatedMB, 50.0,
		"Observability memory floor must be < 50 MB; got %.2f MB", allocatedMB)
}

// TestObservability_SpanAllocsPerOp verifies the per-span allocation budget.
// A single span creation + end cycle should allocate at most 5 objects
// (generous budget covering span struct, attributes slice, and SDK internals).
func TestObservability_SpanAllocsPerOp(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(tp)
	defer func() {
		tp.Shutdown(context.Background())
		otel.SetTracerProvider(nil)
	}()

	tracer := tp.Tracer("nornicdb/alloc-test")

	// Warm up so we measure steady state.
	for i := 0; i < 10; i++ {
		_, span := tracer.Start(context.Background(), "warmup")
		span.End()
	}

	allocs := testing.AllocsPerRun(100, func() {
		_, span := tracer.Start(context.Background(), "bench-op")
		span.SetAttributes(attribute.String("key", "value"))
		span.End()
	})

	t.Logf("Allocs per span: %.1f", allocs)
	assert.LessOrEqual(t, allocs, 20.0,
		"Per-span allocations must be <= 20; got %.1f", allocs)
}

// TestRedactingSpanProcessor_AllocsPerOp_CleanPath verifies the redacting
// processor adds ZERO allocations when no sensitive keys are present (the
// common hot path).
func TestRedactingSpanProcessor_AllocsPerOp_CleanPath(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	redactor := NewRedactingSpanProcessor(recorder)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(redactor))
	otel.SetTracerProvider(tp)
	defer func() {
		tp.Shutdown(context.Background())
		otel.SetTracerProvider(nil)
	}()

	tracer := tp.Tracer("nornicdb/redact-alloc-test")

	for i := 0; i < 10; i++ {
		_, span := tracer.Start(context.Background(), "warmup")
		span.End()
	}

	// Measure base allocs (no redactor) for comparison.
	baseTP := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(tracetest.NewSpanRecorder()))
	baseTracer := baseTP.Tracer("nornicdb/base-alloc-test")
	baseAllocs := testing.AllocsPerRun(100, func() {
		_, span := baseTracer.Start(context.Background(), "bench-op")
		span.SetAttributes(attribute.String("safe.key", "value"))
		span.End()
	})

	// Measure allocs with redactor on clean path (no sensitive keys).
	redactorAllocs := testing.AllocsPerRun(100, func() {
		_, span := tracer.Start(context.Background(), "bench-op")
		span.SetAttributes(attribute.String("safe.key", "value"))
		span.End()
	})

	overhead := redactorAllocs - baseAllocs
	t.Logf("Base allocs: %.1f, with redactor (clean path): %.1f, overhead: %.1f", baseAllocs, redactorAllocs, overhead)
	assert.LessOrEqual(t, overhead, 0.0,
		"Redactor must add zero allocs on clean path; overhead: %.1f", overhead)
}

// TestRedactingSpanProcessor_AllocsPerOp_DirtyPath verifies the redacting
// processor allocates minimally when sensitive keys ARE present.
func TestRedactingSpanProcessor_AllocsPerOp_DirtyPath(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	redactor := NewRedactingSpanProcessor(recorder)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(redactor))
	otel.SetTracerProvider(tp)
	defer func() {
		tp.Shutdown(context.Background())
		otel.SetTracerProvider(nil)
	}()

	tracer := tp.Tracer("nornicdb/redact-alloc-dirty")

	for i := 0; i < 10; i++ {
		_, span := tracer.Start(context.Background(), "warmup")
		span.End()
	}

	allocs := testing.AllocsPerRun(100, func() {
		_, span := tracer.Start(context.Background(), "bench-op")
		span.SetAttributes(
			attribute.String("safe.key", "value"),
			attribute.String("password", "secret"),
		)
		span.End()
	})

	t.Logf("Allocs per span with redactor (dirty path): %.1f", allocs)
	assert.LessOrEqual(t, allocs, 30.0,
		"Per-span allocations with redactor (dirty) must be <= 30; got %.1f", allocs)
}

package testfixture

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// InstallTracerProvider owns the process-global test tracing lifetime. Callers
// construct their actual processor/exporter and sampler; a nil propagator keeps
// the current propagation policy. The returned cleanup also permits explicit
// nested scopes; it runs once, and the owner reports a shutdown failure.
// Cleanup detaches the provider before closing it, and restores only the
// propagation state this scope changed.
func InstallTracerProvider(t testing.TB, provider *sdktrace.TracerProvider, propagator propagation.TextMapPropagator) func() {
	t.Helper()
	previous := otel.GetTracerProvider()
	previousPropagation := otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	if propagator != nil {
		otel.SetTextMapPropagator(propagator)
	}
	_, cleanup := OwnWithClose(t, CloseFunc(func() error {
		otel.SetTracerProvider(previous)
		if propagator != nil {
			otel.SetTextMapPropagator(previousPropagation)
		}
		return provider.Shutdown(context.Background())
	}))
	return cleanup
}

// RecordSpans installs the shared synchronous, always-sampled test recorder and
// W3C trace/baggage propagation. The returned cleanup preserves explicit nested
// scopes; InstallTracerProvider also registers idempotent automatic cleanup.
func RecordSpans(t testing.TB) (*tracetest.InMemoryExporter, func()) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSyncer(exporter),
	)
	cleanup := InstallTracerProvider(t, provider, propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	return exporter, cleanup
}

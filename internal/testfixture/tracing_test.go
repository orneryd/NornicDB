package testfixture

import (
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestTracerScopeExplicitAndAutomaticCleanup(t *testing.T) {
	previous := otel.GetTracerProvider()
	previousPropagation := otel.GetTextMapPropagator()
	defer otel.SetTracerProvider(previous)
	defer otel.SetTextMapPropagator(previousPropagation)
	outside := noop.NewTracerProvider()
	outsidePropagation := &propagation.Baggage{}
	otel.SetTracerProvider(outside)
	otel.SetTextMapPropagator(outsidePropagation)
	t.Run("nested scope", func(t *testing.T) {
		provider := sdktrace.NewTracerProvider()
		cleanup := InstallTracerProvider(t, provider, propagation.TraceContext{})
		if otel.GetTracerProvider() != provider {
			t.Fatal("provider was not installed")
		}
		cleanup()
		if otel.GetTracerProvider() != outside {
			t.Fatal("explicit cleanup did not restore the outer scope")
		}
		// The automatic backup must not overwrite a later scope after an explicit
		// close. This also catches mixed defer/t.Cleanup lifetime inversions.
		otel.SetTracerProvider(previous)
	})
	if otel.GetTracerProvider() != previous {
		t.Fatal("automatic cleanup repeated an already closed scope")
	}
	if otel.GetTextMapPropagator() != outsidePropagation {
		t.Fatal("propagation was not restored")
	}
}

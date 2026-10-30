package bolt

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// startSessionSpan creates nornicdb.bolt.session (TRC-13).
func startSessionSpan(remoteAddr string) (context.Context, trace.Span) {
	ctx, span := otel.Tracer("nornicdb/bolt").Start(context.Background(), "nornicdb.bolt.session",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("net.peer", remoteAddr),
		),
	)
	return ctx, span
}

// startMessageSpan creates nornicdb.bolt.<op> as a child of the session span (TRC-13).
func startMessageSpan(ctx context.Context, op string) (context.Context, trace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, span := otel.Tracer("nornicdb/bolt").Start(ctx, "nornicdb.bolt."+op,
		trace.WithSpanKind(trace.SpanKindServer),
	)
	return ctx, span
}

// extractTraceparent attempts to extract a W3C traceparent from Bolt message
// metadata (TRC-14). Drivers may pass nornicdb.traceparent in BEGIN/RUN extras.
func extractTraceparent(ctx context.Context, metadata map[string]any) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if metadata == nil {
		return ctx
	}
	tp, _ := metadata["nornicdb.traceparent"].(string)
	ts, _ := metadata["nornicdb.tracestate"].(string)
	if tp == "" {
		return ctx
	}
	carrier := propagation.MapCarrier{}
	carrier.Set("traceparent", tp)
	if ts != "" {
		carrier.Set("tracestate", ts)
	}
	prop := propagation.TraceContext{}
	return prop.Extract(ctx, carrier)
}

// recordBoltError sets span status to error.
func recordBoltError(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}
}

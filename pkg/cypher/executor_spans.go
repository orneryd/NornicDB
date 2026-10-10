package cypher

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

var internalSpanStartOptions = []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindInternal)}

// tracingDisabled reports whether the global tracer provider is the default
// noop provider. When it is, starting a span only allocates a span object and
// wraps the context with a value — pure overhead on the hot Execute path — so
// the span helpers return the incoming context and a noop span instead. The
// comparison is allocation-free: both sides are comparable empty structs.
func tracingDisabled() bool {
	return otel.GetTracerProvider() == noop.NewTracerProvider()
}

// startExecuteSpan begins the top-level nornicdb.cypher.execute span. The
// returned context carries the span; callers must defer span.End().
func startExecuteSpan(ctx context.Context, opType string, query string) (context.Context, trace.Span) {
	if tracingDisabled() {
		return ctx, trace.SpanFromContext(ctx)
	}
	ctx, span := otel.Tracer("nornicdb/cypher").Start(ctx, "nornicdb.cypher.execute",
		internalSpanStartOptions[0],
		trace.WithAttributes(
			attribute.String("cypher.op_type", opType),
			attribute.String("cypher.query", truncateQuery(query, 512)),
		),
	)
	return ctx, span
}

// startPlanSpan begins the nornicdb.cypher.plan child span around the
// query analysis/planning phase.
func startPlanSpan(ctx context.Context) (context.Context, trace.Span) {
	if tracingDisabled() {
		return ctx, trace.SpanFromContext(ctx)
	}
	ctx, span := otel.Tracer("nornicdb/cypher").Start(ctx, "nornicdb.cypher.plan", internalSpanStartOptions...)
	return ctx, span
}

// startOperatorSpan begins a nornicdb.cypher.exec.<op> span for PROFILE mode.
func startOperatorSpan(ctx context.Context, op *PlanOperator) (context.Context, trace.Span) {
	if tracingDisabled() {
		return ctx, trace.SpanFromContext(ctx)
	}
	name := "nornicdb.cypher.exec." + op.OperatorType
	ctx, span := otel.Tracer("nornicdb/cypher").Start(ctx, name, internalSpanStartOptions...)
	return ctx, span
}

// endOperatorSpan records actual_rows and ends the span.
func endOperatorSpan(span trace.Span, actualRows int64) {
	span.SetAttributes(attribute.Int64("rows", actualRows))
	span.End()
}

// recordSpanError marks a span with error status and the error message.
func recordSpanError(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}
}

// emitOperatorSpans walks the plan tree and emits a span per operator (TRC-16).
// Each span records estimated_rows and actual_rows. Children are nested so the
// trace tree mirrors the plan operator tree.
func emitOperatorSpans(ctx context.Context, op *PlanOperator) {
	if op == nil {
		return
	}
	childCtx, span := startOperatorSpan(ctx, op)
	for _, child := range op.Children {
		emitOperatorSpans(childCtx, child)
	}
	endOperatorSpan(span, op.ActualRows)
}

package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/pkg/observability"
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
func (e *StorageExecutor) startExecuteSpan(ctx context.Context, opType string, query string) (context.Context, trace.Span) {
	if tracingDisabled() {
		return ctx, trace.SpanFromContext(ctx)
	}
	ctx, span := otel.Tracer("nornicdb/cypher").Start(ctx, "nornicdb.cypher.execute",
		internalSpanStartOptions[0],
		trace.WithAttributes(
			attribute.String("cypher.op_type", opType),
		),
	)
	// Query projection validates syntax and can allocate substantially on a
	// cold grammar. A span that does not record has no query to export.
	if span.IsRecording() {
		span.SetAttributes(attribute.String("cypher.query", truncateQuery(e.redactForLog(ctx, query), 512)))
	}
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

// recordSpanError marks a span with error status and the error message; ctx
// is the statement's context.
func recordSpanError(ctx context.Context, span trace.Span, err error) {
	if err != nil && span.IsRecording() {
		message := redactSpanErrorText(ctx, err.Error())
		span.SetStatus(codes.Error, message)
		// RecordError appends err.Error() after caller attributes, so a message
		// override cannot hide it. Emit the standard exception event directly,
		// retaining the SDK's type naming and the caller's original error.
		typ := reflect.TypeOf(err)
		typeName := typ.String()
		if typ.PkgPath() != "" || typ.Name() != "" {
			typeName = typ.PkgPath() + "." + typ.Name()
		}
		span.AddEvent("exception", trace.WithAttributes(
			attribute.String("exception.type", typeName),
			attribute.String("exception.message", message),
		))
	}
}

// Error prose is not a Cypher statement. Locate its quoted input fragments with
// the existing scanner, then delegate their projection to the query-literal
// owner. Parsing the whole prose would discard useful reasons and error codes.
func redactSpanErrorText(ctx context.Context, text string) string {
	text = observability.RedactText(text)
	var out strings.Builder
	last := 0
	for i := 0; i < len(text); i++ {
		quote := text[i]
		if quote != '\'' && quote != '"' {
			continue
		}
		// A prose apostrophe (can't, isn't) does not open an input literal.
		if quote == '\'' && i > 0 && i+1 < len(text) && cyphertext.NameStartAt(text, i-1) && cyphertext.NameStartAt(text, i+1) {
			continue
		}
		end, _ := scanCypherQuotedText(text, i, quote)
		out.WriteString(text[last:i])
		out.WriteString(strings.TrimPrefix(redactLiterals(ctx, "RETURN "+text[i:end], redactGrammarAccepts), "RETURN "))
		last = end
		i = end - 1
	}
	if last == 0 {
		return text
	}
	out.WriteString(text[last:])
	return out.String()
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

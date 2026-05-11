package search

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// startSearchSpan creates nornicdb.search (TRC-19) at the Service.Search chokepoint.
func startSearchSpan(ctx context.Context, query string, hasEmbedding bool, mode string) (context.Context, trace.Span) {
	ctx, span := otel.Tracer("nornicdb/search").Start(ctx, "nornicdb.search",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("search.mode", mode),
			attribute.Bool("search.has_embedding", hasEmbedding),
			attribute.Int("search.query_len", len(query)),
		),
	)
	return ctx, span
}

func recordSearchError(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}
}

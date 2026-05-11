package replication

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// startAppendSpan creates nornicdb.replication.append (leader-side).
func startAppendSpan(ctx context.Context, peerID string, term uint64, entryCount int) (context.Context, trace.Span) {
	ctx, span := otel.Tracer("nornicdb/replication").Start(ctx, "nornicdb.replication.append",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("peer", peerID),
			attribute.Int64("term", int64(term)),
			attribute.Int("entries", entryCount),
		),
	)
	return ctx, span
}

// startApplySpan creates nornicdb.replication.apply (follower-side).
func startApplySpan(ctx context.Context, leaderID string, term, prevLogIndex uint64, entryCount int) (context.Context, trace.Span) {
	ctx, span := otel.Tracer("nornicdb/replication").Start(ctx, "nornicdb.replication.apply",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("peer", leaderID),
			attribute.Int64("term", int64(term)),
			attribute.Int64("prev_log_index", int64(prevLogIndex)),
			attribute.Int("entries", entryCount),
		),
	)
	return ctx, span
}

func recordReplicationError(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}
}

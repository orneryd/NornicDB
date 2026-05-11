package embed

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// TracedEmbedder wraps an Embedder with nornicdb.embed.* spans (TRC-18).
type TracedEmbedder struct {
	inner Embedder
}

// NewTracedEmbedder wraps inner with OTel span instrumentation.
func NewTracedEmbedder(inner Embedder) *TracedEmbedder {
	return &TracedEmbedder{inner: inner}
}

func (t *TracedEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	ctx, span := otel.Tracer("nornicdb/embed").Start(ctx, "nornicdb.embed.single",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("embed.model", t.inner.Model()),
			attribute.String("embed.backend", t.inner.Backend()),
			attribute.Int("embed.input_len", len(text)),
		),
	)
	defer span.End()

	result, err := t.inner.Embed(ctx, text)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}
	return result, err
}

func (t *TracedEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	ctx, span := otel.Tracer("nornicdb/embed").Start(ctx, "nornicdb.embed.batch",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("embed.model", t.inner.Model()),
			attribute.String("embed.backend", t.inner.Backend()),
			attribute.Int("embed.batch_size", len(texts)),
		),
	)
	defer span.End()

	result, err := t.inner.EmbedBatch(ctx, texts)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}
	return result, err
}

func (t *TracedEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return t.inner.ChunkText(text, maxTokens, overlap)
}

func (t *TracedEmbedder) Dimensions() int {
	return t.inner.Dimensions()
}

func (t *TracedEmbedder) Model() string {
	return t.inner.Model()
}

func (t *TracedEmbedder) Backend() string {
	return t.inner.Backend()
}

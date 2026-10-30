package observability

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

// representativeAttrs is a cypher.exec-shaped record (~10 attrs including a
// slog.Group, a slog.Duration, and a string list). Bench gates the alloc
// budget for the inner JSON handler.
func representativeAttrs() []any {
	return []any{
		"db", "neo4j",
		"user", "alice",
		"query_id", "q-12345",
		slog.Duration("duration", 12*time.Millisecond),
		slog.Group("cypher",
			slog.String("plan_hash", "deadbeefcafef00d"),
			slog.Int("rows", 42),
		),
		"params", []string{"a", "b", "c"},
		"ok", true,
	}
}

// BenchmarkSlogHandler_Hot — custom handler over the representative shape.
func BenchmarkSlogHandler_Hot(b *testing.B) {
	lv := &slog.LevelVar{}
	logger := slog.New(newNornicdbJSONHandler(io.Discard, lv))
	args := representativeAttrs()
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		logger.InfoContext(ctx, "cypher.exec", args...)
	}
}

// BenchmarkSlogHandler_Stdlib — same shape through stdlib slog.NewJSONHandler.
func BenchmarkSlogHandler_Stdlib(b *testing.B) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	args := representativeAttrs()
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		logger.InfoContext(ctx, "cypher.exec", args...)
	}
}

// BenchmarkFullStack_Hot — measures the complete 4-layer stack against the
// representative shape (informational; budget is on inner handler).
func BenchmarkFullStack_Hot(b *testing.B) {
	lv := &slog.LevelVar{}
	inner := newNornicdbJSONHandler(io.Discard, lv)
	redact := newRedactingHandler(inner, defaultRedactSet())
	mandatory := newMandatoryFieldsHandler(redact, ServiceInfo{
		Name: "nornicdb", Version: "v0", NodeID: "n1",
	})
	outer := newRecoveringHandler(mandatory)
	logger := slog.New(outer)

	args := representativeAttrs()
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		logger.InfoContext(ctx, "cypher.exec", args...)
	}
}

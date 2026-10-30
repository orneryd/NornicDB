package server

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRunEmbedWithTimeout_DeadlineExceeded(t *testing.T) {
	t.Parallel()
	_, err := runEmbedWithTimeout(context.Background(), 5*time.Millisecond, func(ctx context.Context) ([]float32, error) {
		select {
		case <-time.After(50 * time.Millisecond):
			return []float32{1}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func BenchmarkRunEmbedWithTimeout_Direct(b *testing.B) {
	b.ReportAllocs()
	parent := context.Background()
	timeout := 8 * time.Second
	embedFn := func(ctx context.Context) ([]float32, error) {
		_ = ctx
		return []float32{0.1, 0.2, 0.3}, nil
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = runEmbedWithTimeout(parent, timeout, embedFn)
	}
}

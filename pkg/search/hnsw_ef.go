package search

import "context"

// hnswEfKey carries a search's own HNSW beam width (SearchOptions.HNSWEf) from
// adaptiveVectorSearch to the HNSW-based candidate generators, which the
// pipeline calls without per-request options.
type hnswEfKey struct{}

// withHNSWEf returns ctx carrying ef as the search's HNSW beam width; ef <= 0
// leaves ctx unchanged, so the configured preset applies.
func withHNSWEf(ctx context.Context, ef int) context.Context {
	if ef <= 0 {
		return ctx
	}
	return context.WithValue(ctx, hnswEfKey{}, ef)
}

// hnswEfFromContext returns the search's own HNSW beam width, or 0 when the
// configured preset applies.
func hnswEfFromContext(ctx context.Context) int {
	if ctx == nil {
		return 0
	}
	ef, _ := ctx.Value(hnswEfKey{}).(int)
	return ef
}

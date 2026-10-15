package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

const maxVectorQueryEmbedCacheEntries = 2048

func canonicalizeVectorQueryText(text string) string {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return ""
	}
	// Normalize to improve cache hit-rate for semantically equivalent queries
	// that only differ in casing/spacing.
	return strings.Join(strings.Fields(lowerASCII(trimmed)), " ")
}

func cloneFloat32Slice(src []float32) []float32 {
	if len(src) == 0 {
		return nil
	}
	out := make([]float32, len(src))
	copy(out, src)
	return out
}

func (e *StorageExecutor) embedVectorQueryText(ctx context.Context, text string) ([]float32, error) {
	state := e.queryEmbeddingState(ctx)
	if state == nil || state.embedder == nil {
		return nil, localizedError(localization.CypherCoreEmbedderNotConfigured(), nil)
	}
	key := canonicalizeVectorQueryText(text)
	if key == "" {
		return embedQueryChunked(ctx, state.embedder, text)
	}

	state.mu.Lock()
	if vec, ok := state.cache[key]; ok {
		state.mu.Unlock()
		return cloneFloat32Slice(vec), nil
	}
	if in, ok := state.inflight[key]; ok {
		state.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-in.done:
			if in.err != nil {
				return nil, in.err
			}
			return cloneFloat32Slice(in.vec), nil
		}
	}

	in := &vectorEmbedInflight{done: make(chan struct{})}
	state.inflight[key] = in
	state.mu.Unlock()

	vec, err := embedQueryChunked(ctx, state.embedder, key)
	in.vec = cloneFloat32Slice(vec)
	in.err = err

	state.mu.Lock()
	delete(state.inflight, key)
	if err == nil && len(in.vec) > 0 {
		if len(state.cache) >= maxVectorQueryEmbedCacheEntries {
			// Keep eviction policy simple and deterministic: clear on capacity.
			state.cache = make(map[string][]float32, 512)
		}
		state.cache[key] = cloneFloat32Slice(in.vec)
	}
	close(in.done)
	state.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return cloneFloat32Slice(in.vec), nil
}

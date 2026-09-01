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
	return strings.Join(strings.Fields(strings.ToLower(trimmed)), " ")
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
	if e.embedder == nil {
		return nil, localizedError(localization.CypherCoreEmbedderNotConfigured(), nil)
	}
	key := canonicalizeVectorQueryText(text)
	if key == "" {
		return embedQueryChunked(ctx, e.embedder, text)
	}

	e.vectorQueryEmbedMu.Lock()
	if vec, ok := e.vectorQueryEmbedCache[key]; ok {
		e.vectorQueryEmbedMu.Unlock()
		return cloneFloat32Slice(vec), nil
	}
	if in, ok := e.vectorQueryEmbedInflight[key]; ok {
		e.vectorQueryEmbedMu.Unlock()
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
	e.vectorQueryEmbedInflight[key] = in
	e.vectorQueryEmbedMu.Unlock()

	vec, err := embedQueryChunked(ctx, e.embedder, key)
	in.vec = cloneFloat32Slice(vec)
	in.err = err

	e.vectorQueryEmbedMu.Lock()
	delete(e.vectorQueryEmbedInflight, key)
	if err == nil && len(in.vec) > 0 {
		if len(e.vectorQueryEmbedCache) >= maxVectorQueryEmbedCacheEntries {
			// Keep eviction policy simple and deterministic: clear on capacity.
			e.vectorQueryEmbedCache = make(map[string][]float32, 512)
		}
		e.vectorQueryEmbedCache[key] = cloneFloat32Slice(in.vec)
	}
	close(in.done)
	e.vectorQueryEmbedMu.Unlock()

	if err != nil {
		return nil, err
	}
	return cloneFloat32Slice(in.vec), nil
}

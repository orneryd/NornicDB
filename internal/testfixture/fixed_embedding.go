package testfixture

import "context"

// FixedEmbedding2D is the canonical single-text embedding fixture: its zero
// value reports the same two-dimensional unit vector for every input. Embed
// returns a fresh slice on each call, so a caller that owns a returned vector
// cannot observe another caller's mutation.
//
// FixedEmbedding2D carries only the single-text capability. A fixture that
// also needs batch embedding composes FixedBatchEmbedding2D beside it, keeping
// its method set to exactly the optional capabilities it serves.
type FixedEmbedding2D struct{}

// Embed returns a fresh [1, 0] vector, or the context's error when ctx is
// already canceled or past its deadline.
func (FixedEmbedding2D) Embed(ctx context.Context, _ string) ([]float32, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return []float32{1, 0}, nil
}

// FixedBatchEmbedding2D is the canonical batch embedding fixture: its zero
// value reports one [1, 0] vector per input text in input order. Every row is
// a fresh slice, so rows never share a backing array.
type FixedBatchEmbedding2D struct{}

// EmbedBatch returns len(texts) fresh [1, 0] vectors, or the context's error
// when ctx is already canceled or past its deadline.
func (FixedBatchEmbedding2D) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	vectors := make([][]float32, len(texts))
	for index := range vectors {
		vectors[index] = []float32{1, 0}
	}
	return vectors, nil
}

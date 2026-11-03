package nornicdb

// Author: Codex. Constructed provider fixture shared by the provider publication tests.
import (
	"context"
	"sync/atomic"
)

type classReleaseEmbedder struct {
	closes  atomic.Int32
	failure error
}

func (*classReleaseEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{1, 2, 3, 4}, nil
}
func (*classReleaseEmbedder) EmbedBatch(context.Context, []string) ([][]float32, error) {
	return nil, nil
}
func (*classReleaseEmbedder) ChunkText(s string, _, _ int) ([]string, error) { return []string{s}, nil }
func (*classReleaseEmbedder) Dimensions() int                                { return 4 }
func (*classReleaseEmbedder) Model() string                                  { return "class-provider" }
func (*classReleaseEmbedder) Backend() string                                { return "cpu" }
func (p *classReleaseEmbedder) Close() error                                 { p.closes.Add(1); return p.failure }

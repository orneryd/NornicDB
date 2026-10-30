package nornicdb

// Author: Codex. Actual constructed provider, adopted by the public DB setter.
import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
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
func TestOwnerClassDBReleaseProviderOutcome(t *testing.T) {
	db, err := Open(t.TempDir(), nil)
	require.NoError(t, err)
	p := &classReleaseEmbedder{failure: errors.New("class provider disposal outcome")}
	db.SetEmbedder(p)
	err = db.Close()
	require.ErrorIs(t, err, p.failure)
	require.EqualValues(t, 1, p.closes.Load())
}

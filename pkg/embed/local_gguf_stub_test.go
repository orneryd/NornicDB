//go:build !localllm

package embed

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLocalGGUFStub_Embed(t *testing.T) {
	stub := &LocalGGUFEmbedder{}
	ctx := context.Background()

	_, err := stub.Embed(ctx, "hello")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not available")
}

func TestLocalGGUFStub_EmbedBatch(t *testing.T) {
	stub := &LocalGGUFEmbedder{}
	ctx := context.Background()

	_, err := stub.EmbedBatch(ctx, []string{"a", "b"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not available")
}

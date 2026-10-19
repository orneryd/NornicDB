package nornicdb

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestLocalEmbedderRegistryPreservesGPULayers(t *testing.T) {
	for _, layers := range []int{0, 4} {
		global := &mockEmbedder{dims: 7, model: "same-model"}
		selected := &mockEmbedder{dims: 7, model: "same-model"}
		defaultCfg := &embed.Config{Provider: "local", Model: "same-model", Dimensions: 7, GPULayers: -1}
		requested := *defaultCfg
		requested.GPULayers = layers
		db := &DB{embedQueue: &EmbedQueue{embedder: global}}
		db.SetDefaultEmbedConfig(defaultCfg)
		db.SetEmbedConfigForDB(func(string) (*embed.Config, error) { return &requested, nil })
		called := false
		db.embedderFactory = func(ctx context.Context, cfg *embed.Config) (embed.Embedder, error) {
			called = true
			require.Equal(t, layers, cfg.GPULayers)
			return selected, nil
		}
		got, err := db.getOrCreateEmbedderForDB("tenant")
		require.NoError(t, err)
		require.True(t, called, "different GPU-layer choice must reach the constructor")
		require.Same(t, selected, got)
	}
}

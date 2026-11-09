package gpu

import (
	"runtime"
	"strings"
	"testing"
	"unsafe"
	"weak"

	"github.com/stretchr/testify/require"
)

func TestRetiredEmbeddingIDsCollectible(t *testing.T) {
	cases := map[string]func(string) any{
		"embedding_remove": func(id string) any {
			owner := &EmbeddingIndex{nodeIDs: []string{id}, idToIndex: map[string]int{id: 0}, dimensions: 1, cpuVectors: []float32{1}}
			require.True(t, owner.Remove(id))
			return owner
		},
		"embedding_clear": func(id string) any {
			owner := &EmbeddingIndex{nodeIDs: []string{id}, idToIndex: map[string]int{id: 0}}
			owner.Clear()
			return owner
		},
		"accelerator_remove": func(id string) any {
			owner := &GPUEmbeddingIndex{nodeIDs: []string{id}, idToIndex: map[string]int{id: 0}, dimensions: 1, cpuData: []float32{1}}
			require.True(t, owner.Remove(id))
			return owner
		},
		"cluster_updates_clear": func(id string) any {
			owner := &ClusterIndex{EmbeddingIndex: &EmbeddingIndex{}, pendingUpdates: []nodeUpdate{{id: id}}}
			owner.Clear()
			return owner
		},
	}
	for name, retire := range cases {
		t.Run(name, func(t *testing.T) {
			owner, reference := func() (any, weak.Pointer[byte]) {
				id := strings.Repeat("retired", 64)
				return retire(id), weak.Make(unsafe.StringData(id))
			}()
			runtime.GC()
			require.Nil(t, reference.Value())
			runtime.KeepAlive(owner)
		})
	}
}

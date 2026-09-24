package search

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// getOrCreateVectorPipeline never hands back a nil pipeline without an error
// while index builds reset it concurrently (#593). Run with -race to also
// check the field is only read under pipelineMu.
func TestGetOrCreateVectorPipelineConcurrentWithReset(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	svc := NewServiceWithDimensions(engine, 4)
	for i := 0; i < 8; i++ {
		require.NoError(t, svc.IndexNode(&storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("n-%d", i)),
			Labels:     []string{"Doc"},
			Properties: map[string]any{"embedding": []float32{float32(i), 1, 0, 0}},
		}))
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				svc.resetANNForBuild()
			}
		}
	}()
	for i := 0; i < 2000; i++ {
		pipeline, err := svc.getOrCreateVectorPipeline(ctx)
		if err == nil {
			require.NotNil(t, pipeline, "a nil pipeline is returned only with an error")
		}
	}
	close(stop)
	wg.Wait()
}

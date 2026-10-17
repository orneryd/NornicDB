package search

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestVectorPipelineWaitSurvivesRetiredConstruction: the first ANN read waits
// for the shared construction job. A build or retirement that cancels that job
// pauses it for the reader, whose own context is live: the reader is answered
// by the exact fallback, not with the job's cancellation.
func TestVectorPipelineWaitSurvivesRetiredConstruction(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	node := &storage.Node{ID: "item", ChunkEmbeddings: [][]float32{{1, 0, 0}}}
	_, err := svc.engine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(node))

	entered, release := make(chan struct{}), make(chan struct{})
	var parkOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	previous := hnswBeforeInstallHook
	// Only the first construction parks; the reader's retry builds freely.
	hnswBeforeInstallHook = func(context.Context) {
		parkOnce.Do(func() { close(entered); <-release })
	}
	created := make(chan struct{})
	var factoryErr error
	defer func() {
		unblock()
		_ = svc.Close()
		select {
		case <-created:
		case <-time.After(5 * time.Second):
			t.Error("pipeline factory did not join during cleanup")
		}
		hnswBeforeInstallHook = previous
	}()
	go func() {
		_, factoryErr = svc.getOrCreateVectorPipeline(context.Background())
		close(created)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("construction did not reach installation")
	}
	svc.indexMu.Lock()
	svc.retireVectorSourceLocked(false)
	svc.indexMu.Unlock()
	unblock()
	select {
	case <-created:
		require.NoError(t, factoryErr, "a retired construction must not fail the waiting reader")
	case <-time.After(10 * time.Second):
		t.Fatal("pipeline factory did not return after its construction was retired")
	}
}

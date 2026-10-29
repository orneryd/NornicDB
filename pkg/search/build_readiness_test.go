package search

import (
	"context"

	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBuildReadinessPublishedAfterSuccessfulFinalReplay(t *testing.T) {
	engine := &triageSnapshotEngine{MemoryEngine: newTestMemoryEngine(t)}
	t.Cleanup(func() { _ = engine.Close() })
	_, err := engine.CreateNode(triageNode("nornic:baseline"))
	require.NoError(t, err)
	svc := newSearchPersistenceTestService(t, engine, t.TempDir(), false)
	reached, published := false, false
	svc.SetLogger(slog.New(&triageReplayLog{match: "starting vector pipeline warmup", during: func() {
		svc.SetPersistenceEnabled(true)
		svc.vectorFileStore.mu.Lock()
		svc.vectorFileStore.writeRecord = func(file *os.File, id string, vec []float32) error {
			if strings.Contains(id, "tail") {
				reached = true
				published = svc.IsReady()
			}
			return writeVectorRecord(file, id, vec)
		}
		svc.vectorFileStore.mu.Unlock()
		tail := triageNode("nornic:tail")
		_, err := engine.CreateNode(tail)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(tail))
	}}))
	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.True(t, reached)
	require.False(t, published, "success must not publish readiness before installing the tail")
	require.True(t, svc.IsReady(), "successful final replay must publish readiness")
	_, found := svc.getVectorLookup()(nodeNamedVectorKey("nornic:tail", "v"))
	require.True(t, found)
}

// Overlapping builds transfer an acknowledged tail to the next owner. Park
// that owner before it opens its baseline to observe the handoff boundary.
type readinessHandoffEngine struct {
	*storage.MemoryEngine
	opened           atomic.Int32
	entered, release chan struct{}
}

func (e *readinessHandoffEngine) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	if buildWriteCallerOnStack("openBuildSnapshot") && e.opened.Add(1) == 2 {
		close(e.entered)
		<-e.release
	}
	return e.MemoryEngine.OpenReadSnapshot(ctx)
}

func TestBuildReadinessWaitsForOverlappingFinalSequence(t *testing.T) {
	engine := &readinessHandoffEngine{MemoryEngine: newTestMemoryEngine(t), entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { _ = engine.Close() })
	_, err := engine.CreateNode(triageNode("nornic:baseline"))
	require.NoError(t, err)
	svc := newSearchPersistenceTestService(t, engine, t.TempDir(), false)
	gate := &finalPersistGateLog{entered: make(chan struct{}), release: make(chan struct{})}
	openFirst := sync.OnceFunc(func() { close(gate.release) })
	openSecond := sync.OnceFunc(func() { close(engine.release) })
	t.Cleanup(openSecond)
	t.Cleanup(openFirst)
	svc.SetLogger(slog.New(gate))
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- svc.BuildIndexes(context.Background()) }()
	select {
	case <-gate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("first build never reached final persistence")
	}
	go func() { second <- svc.BuildIndexes(context.Background()) }()
	require.Eventually(t, func() bool {
		svc.buildWrites.mu.Lock()
		defer svc.buildWrites.mu.Unlock()
		return svc.buildWrites.owners == 2
	}, 5*time.Second, time.Millisecond)
	tail := triageNode("nornic:tail")
	_, err = engine.CreateNode(tail)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(tail))
	openFirst()
	select {
	case err := <-first:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("first build did not finish")
	}
	select {
	case <-engine.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("second build never took ownership")
	}
	readyAtHandoff := svc.IsReady()
	openSecond()
	select {
	case err := <-second:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("second build did not finish")
	}
	require.False(t, readyAtHandoff, "pending final sequence published readiness")
	require.True(t, svc.IsReady())
	_, found := svc.getVectorLookup()(nodeNamedVectorKey("nornic:tail", "v"))
	require.True(t, found)
}

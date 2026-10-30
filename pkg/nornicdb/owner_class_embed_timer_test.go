// Author: Codex (#1636 lane). Real worker completion through public admission.
package nornicdb_test

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"sync"
	"testing"
	"time"
)

type classTimerEmbedder struct{}

func (*classTimerEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{1, 2, 3, 4}, nil
}
func (*classTimerEmbedder) EmbedBatch(_ context.Context, s []string) ([][]float32, error) {
	v := make([][]float32, len(s))
	for i := range v {
		v[i] = []float32{1, 2, 3, 4}
	}
	return v, nil
}
func (*classTimerEmbedder) ChunkText(s string, _, _ int) ([]string, error) { return []string{s}, nil }
func (*classTimerEmbedder) Dimensions() int                                { return 4 }
func (*classTimerEmbedder) Model() string                                  { return "lifetime-class" }
func (*classTimerEmbedder) Backend() string                                { return "cpu" }
func TestOwnerClassEmbedTimerActualCompletion(t *testing.T) {
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	engine.SetEmbeddingsEnabled(true)
	defer engine.Close()
	_, err := engine.CreateNode(&storage.Node{ID: "docs:note", Labels: []string{"Note"}, Properties: map[string]interface{}{"content": "accepted embedding work must retain its completion callback"}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := nornicdb.DefaultEmbedWorkerConfig()
	cfg.DeferWorkerStart = true
	cfg.BatchDelay = 0
	cfg.TriggerDebounceDelay = 0
	cfg.ClusterDebounceDelay = time.Millisecond
	cfg.ClusterMinBatchSize = 1
	worker := nornicdb.NewEmbedWorker(&classTimerEmbedder{}, engine, cfg)
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	worker.SetOnQueueEmpty(func(int) { close(entered); <-release; close(returned) })
	worker.StartWorkers()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		worker.Close()
		t.Fatal("real embedding worker did not reach debounced completion callback")
	}
	closed := make(chan struct{})
	go func() { worker.Close(); close(closed) }()
	early := false
	select {
	case <-closed:
		early = true
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	select {
	case <-returned:
	case <-time.After(3 * time.Second):
		t.Fatal("callback did not return")
	}
	if !early {
		select {
		case <-closed:
		case <-time.After(3 * time.Second):
			t.Fatal("worker did not join after callback return")
		}
	}
	if early {
		t.Error("EmbedWorker.Close completed while its accepted debounce callback was still running")
	}
}

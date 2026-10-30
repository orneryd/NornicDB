package nornicdb

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/pkg/inference"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type r4InferenceWriter struct {
	storage.Engine
	gate, entered chan struct{}
	once          sync.Once
	calls         atomic.Int32
	first, second error
}

func (w *r4InferenceWriter) CreateEdge(e *storage.Edge) error {
	w.once.Do(func() { close(w.entered) })
	<-w.gate
	if w.calls.Add(1)%2 == 0 {
		return w.second
	}
	return w.first
}
func TestDBR4EmbeddedInferenceLifetimeAndOutcomes(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Database.AsyncWritesEnabled = false
	cfg.Memory.EmbeddingEnabled = true
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	defer db.Close()
	db.embedWorkerConfig.BatchDelay = 0
	db.embedWorkerConfig.ScanInterval = time.Hour
	a, err := db.CreateNode(context.Background(), []string{"Memory"}, map[string]interface{}{"content": "alpha"})
	require.NoError(t, err)
	b, err := db.CreateNode(context.Background(), []string{"Memory"}, map[string]interface{}{"content": "beta"})
	require.NoError(t, err)
	c, err := db.CreateNode(context.Background(), []string{"Memory"}, map[string]interface{}{"content": "gamma"})
	require.NoError(t, err)
	_ = a
	d, err := db.CreateNode(context.Background(), []string{"Memory"}, map[string]interface{}{"content": "delta"})
	require.NoError(t, err)
	engine, err := db.GetOrCreateInferenceService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)
	engine.SetSimilaritySearch(func(context.Context, []float32, int) ([]inference.SimilarityResult, error) {
		return []inference.SimilarityResult{{ID: b.ID, Score: .99}, {ID: c.ID, Score: .98}, {ID: d.ID, Score: .97}}, nil
	})
	writer := &r4InferenceWriter{Engine: db.storage, gate: make(chan struct{}), entered: make(chan struct{}), first: errors.New("r4 inference edge a"), second: errors.New("r4 inference edge b")}
	var once sync.Once
	release := func() { once.Do(func() { close(writer.gate) }) }
	defer release()
	db.SetDatabaseStorageResolver(func(string) (storage.Engine, error) { return writer, nil })
	db.SetEmbedder(&staticTestEmbedder{dims: 3})
	select {
	case <-writer.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("embedding-origin inference did not reach native write")
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case e := <-closed:
		t.Errorf("DB.Close passed accepted embedded inference: %v", e)
		closed = nil
	case <-time.After(100 * time.Millisecond):
	}
	release()
	if closed != nil {
		select {
		case e := <-closed:
			require.ErrorIs(t, e, writer.first)
			require.ErrorIs(t, e, writer.second)
		case <-time.After(3 * time.Second):
			t.Fatal("DB.Close did not join inference")
		}
	}
}

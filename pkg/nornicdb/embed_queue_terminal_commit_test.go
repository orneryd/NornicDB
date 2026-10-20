package nornicdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type terminalCommitProvider struct {
	*mockEmbedder
	failing atomic.Bool
}

func (p *terminalCommitProvider) Embed(ctx context.Context, text string) ([]float32, error) {
	if p.failing.Load() && strings.Contains(text, "poison") {
		return nil, errors.New("permanent provider failure")
	}
	return p.mockEmbedder.Embed(ctx, text)
}

func (p *terminalCommitProvider) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	results := make([][]float32, len(texts))
	for i, text := range texts {
		vector, err := p.Embed(ctx, text)
		if err != nil {
			return nil, err
		}
		results[i] = vector
	}
	return results, nil
}

type terminalCommitStore struct {
	*storage.BadgerEngine
	rejects  atomic.Int32
	attempts atomic.Int32
	paused   atomic.Bool
}

func (s *terminalCommitStore) UpdateNodeEmbeddingSidecar(node *storage.Node) error {
	if failed, _ := node.EmbedMeta["embedding_failed"].(bool); failed {
		s.attempts.Add(1)
		if s.rejects.Add(-1) >= 0 {
			s.paused.Store(true)
			return errors.New("transient terminal-marker sidecar failure")
		}
	}
	return s.BadgerEngine.UpdateNodeEmbeddingSidecar(node)
}

// Only the DB-to-worker wiring is assembled here. Claims, provider failures,
// marker writes, drain checks and recovery all use their exported public APIs.
func TestEmbedWorkerTerminalMarkerCommitFailureRemainsPending(t *testing.T) {
	for _, recoverProvider := range []bool{false, true} {
		name := "durably_park"
		if recoverProvider {
			name = "provider_recovers"
		}
		t.Run(name, func(t *testing.T) {
			path := t.TempDir()
			disk, err := storage.NewBadgerEngine(path)
			require.NoError(t, err)
			defer disk.Close()
			disk.SetEmbeddingsEnabled(true)
			store := &terminalCommitStore{BadgerEngine: disk}
			store.rejects.Store(2)
			provider := &terminalCommitProvider{mockEmbedder: newMockEmbedder()}
			provider.failing.Store(true)
			cfg := DefaultEmbedWorkerConfig()
			cfg.DeferWorkerStart = true
			cfg.NumWorkers, cfg.EmbedBatchSize, cfg.MaxRetries = 1, 1, 1
			cfg.BatchDelay, cfg.TriggerDebounceDelay = 0, 0
			cfg.ScanInterval = time.Hour
			cfg.ProviderRetryBackoff, cfg.ProviderRetryBackoffMax = 400*time.Millisecond, 400*time.Millisecond
			worker := NewEmbedWorker(provider, store, cfg)
			defer worker.Close()
			worker.SetShouldYield(store.paused.Load)
			db := &DB{baseStorage: store, embedQueue: worker}
			_, err = store.CreateNode(&storage.Node{ID: "terminal:poison", Properties: map[string]any{"text": "poison"}})
			require.NoError(t, err)
			worker.StartWorkers()

			for rejected := int32(1); rejected <= 2; rejected++ {
				require.Eventually(t, func() bool {
					return store.attempts.Load() == rejected && store.paused.Load() && worker.Stats().InFlight == 0
				}, 5*time.Second, time.Millisecond)
				stored, err := store.GetNode("terminal:poison")
				require.NoError(t, err)
				require.Empty(t, stored.ChunkEmbeddings)
				require.NotEqual(t, true, stored.EmbedMeta["embedding_failed"], "the rejected marker is not durable")
				failures, err := worker.ParkedEmbeddingFailures(context.Background(), 10)
				require.NoError(t, err)
				require.Empty(t, failures)
				require.Zero(t, worker.Stats().Parked)
				pending := db.PendingEmbeddingsCount()
				assert.Equal(t, 1, pending, "failed terminal commit must remain owned by the retry queue")
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				waitErr := db.WaitForEmbeddings(ctx)
				cancel()
				assert.ErrorIs(t, waitErr, context.DeadlineExceeded, "draining cannot complete before vectors or the terminal marker commit")
				if pending != 1 || waitErr == nil {
					return // Preserve both original public failure assertions.
				}
				_, err = store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("terminal:healthy-%d", rejected)), Properties: map[string]any{"text": "healthy MAIN"}})
				require.NoError(t, err)
				if rejected == 2 && recoverProvider {
					provider.failing.Store(false)
				}
				store.paused.Store(false)
				worker.TriggerImmediate()
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, db.WaitForEmbeddings(ctx))
			require.Zero(t, db.PendingEmbeddingsCount())
			stored, err := store.GetNode("terminal:poison")
			require.NoError(t, err)
			if recoverProvider {
				require.NotEmpty(t, stored.ChunkEmbeddings)
				require.Zero(t, worker.Stats().Parked)
				require.Equal(t, int32(2), store.attempts.Load())
				require.Equal(t, 3, worker.Stats().Processed)
			} else {
				require.Equal(t, true, stored.EmbedMeta["embedding_failed"])
				require.Equal(t, 1, worker.Stats().Parked)
				require.Equal(t, int32(3), store.attempts.Load())
				require.Equal(t, 2, worker.Stats().Processed, "healthy MAIN keeps moving during marker retries")
				worker.Close()
				require.NoError(t, disk.Close())
				reopened, err := storage.NewBadgerEngine(path)
				require.NoError(t, err)
				defer reopened.Close()
				node, err := reopened.GetNode("terminal:poison")
				require.NoError(t, err)
				require.Equal(t, true, node.EmbedMeta["embedding_failed"], "draining requires a marker that survives reopen")
			}
		})
	}
}

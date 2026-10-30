package nornicdb_test

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type reviewRejectedUpdateStore struct {
	*storage.NamespacedEngine
	attempts atomic.Int64
	paused   atomic.Bool
}

func (s *reviewRejectedUpdateStore) UpdateNodeEmbeddingSidecar(node *storage.Node) error {
	if s.attempts.Add(1) == 1 {
		s.paused.Store(true)
		return errors.New("review: first sidecar rejected")
	}
	return s.NamespacedEngine.UpdateNodeEmbeddingSidecar(node)
}

// The retired retained-save repro is now a current-body retry contract. Update
// after the rejected attempt, then require one fresh provider request and no
// old-body publication, even when the provider's text did not change.
func TestReviewR73RejectedSaveUpdateRequestsCurrentBody(t *testing.T) {
	for _, mode := range []string{"unconsumed_enqueued", "unconsumed_no_enqueue", "consumed_enqueued"} {
		t.Run(mode, func(t *testing.T) {
			disk, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			defer disk.Close()
			disk.SetEmbeddingsEnabled(true)
			s := &reviewRejectedUpdateStore{NamespacedEngine: storage.NewNamespacedEngine(disk, "rejectedupdate")}
			_, err = s.CreateNode(&storage.Node{ID: "doc", Properties: map[string]any{"text": "same provider input", "counter": int64(1)}})
			require.NoError(t, err)
			cfg := nornicdb.DefaultEmbedWorkerConfig()
			cfg.DeferWorkerStart, cfg.NumWorkers, cfg.EmbedBatchSize = true, 1, 1
			cfg.PropertiesInclude = []string{"text"}
			cfg.BatchDelay, cfg.TriggerDebounceDelay = 0, 0
			cfg.ScanInterval = time.Hour
			cfg.ProviderRetryBackoff, cfg.ProviderRetryBackoffMax = 20*time.Millisecond, 20*time.Millisecond
			provider := &dlqPublicProvider{}
			worker := nornicdb.NewEmbedWorker(provider, s, cfg)
			defer worker.Close()
			var stale atomic.Bool
			check := func(node *storage.Node) {
				if fmt.Sprint(node.Properties["counter"]) != "2" {
					stale.Store(true)
				}
			}
			s.OnNodeUpdated(check)
			worker.SetOnEmbedded(check)
			worker.SetShouldYield(s.paused.Load)
			worker.StartWorkers()
			require.Eventually(t, func() bool { return s.paused.Load() && worker.Stats().InFlight == 0 }, 3*time.Second, time.Millisecond)
			node, err := s.GetNode("doc")
			require.NoError(t, err)
			node.Properties["counter"] = int64(2)
			if mode == "consumed_enqueued" {
				node.Properties["text"] = "new provider input"
			}
			require.NoError(t, s.UpdateNode(storage.NodeUpdateRequest{After: node}))
			if mode != "unconsumed_no_enqueue" {
				worker.Enqueue("doc")
			}
			s.paused.Store(false)
			worker.TriggerImmediate()
			require.Eventually(t, func() bool { return worker.Stats().Processed == 1 && worker.Stats().InFlight == 0 }, 3*time.Second, time.Millisecond)
			stored, err := s.GetNode("doc")
			require.NoError(t, err)
			require.EqualValues(t, 2, stored.Properties["counter"])
			require.NotEmpty(t, stored.ChunkEmbeddings)
			require.False(t, stale.Load(), "only the updated authoritative body may publish")
			provider.mu.Lock()
			calls := append([]string(nil), provider.calls...)
			provider.mu.Unlock()
			require.Len(t, calls, 2, "a rejected save requires exactly one provider re-request after this update")
			require.Equal(t, int64(2), s.attempts.Load())
			if mode == "consumed_enqueued" {
				require.Contains(t, calls[1], "new provider input")
			} else {
				require.Equal(t, calls[0], calls[1], "unchanged input is requested again too")
			}
		})
	}
}

package nornicdb_test

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Keep the real pending index and mutation APIs. Only reject the first save.
type reviewR73RejectOnce struct {
	*storage.NamespacedEngine
	rejected atomic.Bool
	paused   atomic.Bool
}

func (s *reviewR73RejectOnce) UpdateNodeEmbeddingSidecar(n *storage.Node) error {
	if s.rejected.CompareAndSwap(false, true) {
		s.paused.Store(true)
		return errors.New("first sidecar rejected")
	}
	return s.NamespacedEngine.UpdateNodeEmbeddingSidecar(n)
}

func TestReviewR73DLQUpdateWithoutEnqueue(t *testing.T) {
	for _, mode := range []string{"idempotent", "changed_source", "advanced_timestamp", "enqueue"} {
		t.Run(mode, func(t *testing.T) {
			disk, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			defer disk.Close()
			disk.SetEmbeddingsEnabled(true)
			s := &reviewR73RejectOnce{NamespacedEngine: storage.NewNamespacedEngine(disk, "dlqupdate")}
			_, err = s.CreateNode(&storage.Node{ID: "doc", Properties: map[string]any{"text": "before"}})
			require.NoError(t, err)
			cfg := nornicdb.DefaultEmbedWorkerConfig()
			cfg.DeferWorkerStart, cfg.NumWorkers, cfg.EmbedBatchSize = true, 1, 1
			cfg.BatchDelay, cfg.TriggerDebounceDelay = 0, 0
			cfg.ScanInterval, cfg.ProviderRetryBackoff, cfg.ProviderRetryBackoffMax = time.Hour, time.Hour, time.Hour
			p := &dlqPublicProvider{}
			w := nornicdb.NewEmbedWorker(p, s, cfg)
			defer w.Close()
			w.SetShouldYield(s.paused.Load)
			w.StartWorkers()
			require.Eventually(t, func() bool { return s.paused.Load() && w.Stats().InFlight == 0 }, 3*time.Second, time.Millisecond)
			node, err := s.GetNode("doc")
			require.NoError(t, err)
			stamp := node.UpdatedAt
			if mode != "idempotent" {
				node.Properties["text"] = "after"
			}
			if mode == "advanced_timestamp" {
				node.UpdatedAt = stamp.Add(time.Second)
			}
			require.NoError(t, s.UpdateNode(node))
			current, err := s.GetNode("doc")
			require.NoError(t, err)
			if mode != "advanced_timestamp" {
				require.True(t, stamp.Equal(current.UpdatedAt), "the body API preserves a supplied timestamp")
			}
			require.Equal(t, 1, s.PendingEmbeddingsCount(), "the body update restored the real MAIN marker")
			s.paused.Store(false)
			if mode == "enqueue" {
				w.Enqueue("doc")
			}
			w.TriggerImmediate() // wake/scan, deliberately no mutation notification
			recovered := assert.Eventually(t, func() bool { return w.Stats().Processed == 1 }, time.Second, 5*time.Millisecond)
			t.Logf("recovered=%v processed=%d publicPending=%d durablePending=%d", recovered, w.Stats().Processed, w.PendingEmbeddingsCount(), s.PendingEmbeddingsCount())
		})
	}
}

// The successful-attempt suppression window is also revision-owned. A changed
// source must be eligible immediately even without an Enqueue notification.
func TestReviewR73UpdateAfterSuccessWithoutEnqueue(t *testing.T) {
	disk, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	defer disk.Close()
	disk.SetEmbeddingsEnabled(true)
	s := &reviewR73RejectOnce{NamespacedEngine: storage.NewNamespacedEngine(disk, "updatedsuccess")}
	s.rejected.Store(true)
	_, err = s.CreateNode(&storage.Node{ID: "doc", Properties: map[string]any{"text": "before"}})
	require.NoError(t, err)
	cfg := nornicdb.DefaultEmbedWorkerConfig()
	cfg.DeferWorkerStart, cfg.NumWorkers, cfg.EmbedBatchSize = true, 1, 1
	cfg.BatchDelay, cfg.TriggerDebounceDelay, cfg.ScanInterval = 0, 0, time.Hour
	w := nornicdb.NewEmbedWorker(&dlqPublicProvider{}, s, cfg)
	defer w.Close()
	w.SetShouldYield(s.paused.Load)
	w.SetOnEmbedded(func(*storage.Node) { s.paused.Store(true) })
	w.StartWorkers()
	require.Eventually(t, func() bool { return w.Stats().Processed == 1 && w.Stats().InFlight == 0 }, 3*time.Second, time.Millisecond)
	node, err := s.GetNode("doc")
	require.NoError(t, err)
	stamp := node.UpdatedAt
	node.Properties["text"] = "after"
	require.NoError(t, s.UpdateNode(node))
	current, err := s.GetNode("doc")
	require.NoError(t, err)
	require.True(t, stamp.Equal(current.UpdatedAt))
	require.Equal(t, 1, s.PendingEmbeddingsCount())
	s.paused.Store(false)
	w.TriggerImmediate()
	require.Eventually(t, func() bool { return w.Stats().Processed == 2 && w.Stats().InFlight == 0 }, 3*time.Second, time.Millisecond)
	require.Zero(t, w.PendingEmbeddingsCount())
}

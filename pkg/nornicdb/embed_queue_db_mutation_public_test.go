package nornicdb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mutationWakeProvider struct {
	*mockEmbedder
	failed      atomic.Bool
	hold        atomic.Bool
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
}

func (p *mutationWakeProvider) Embed(ctx context.Context, text string) ([]float32, error) {
	if p.failed.CompareAndSwap(false, true) {
		return nil, errors.New("first provider request fails")
	}
	if p.hold.Load() {
		p.enteredOnce.Do(func() { close(p.entered) })
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return p.mockEmbedder.Embed(ctx, text)
}

func (p *mutationWakeProvider) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	results := make([][]float32, len(texts))
	for i, text := range texts {
		v, err := p.Embed(ctx, text)
		if err != nil {
			return nil, err
		}
		results[i] = v
	}
	return results, nil
}

type mutationWakeStore struct {
	*storage.BadgerEngine
	provider  *mutationWakeProvider
	misses    atomic.Int32
	findNil   atomic.Bool
	idleAfter int32
	once      sync.Once
	idle      chan struct{}
}

func (s *mutationWakeStore) FindNodeNeedingEmbedding() *storage.Node {
	n := s.BadgerEngine.FindNodeNeedingEmbedding()
	s.findNil.Store(n == nil)
	if n == nil {
		s.emptySearch()
	}
	return n
}

func (s *mutationWakeStore) emptySearch() {
	if s.provider.failed.Load() && s.misses.Add(1) >= s.idleAfter {
		s.once.Do(func() { close(s.idle) })
	}
}

func (s *mutationWakeStore) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	stopped := false
	err := s.BadgerEngine.StreamNodesWithOptions(ctx, opts, func(n *storage.Node) error {
		err := visit(n)
		stopped = stopped || err != nil
		return err
	})
	// Observe a complete empty claim through either finder or fallback,
	// counting only once if the same claim consults both interfaces.
	if err == nil && !stopped && !s.findNil.Load() {
		s.emptySearch()
	}
	return err
}

// Private fields assemble the DB/worker/search fixture. Mutations, queue
// startup, status and draining use their public APIs; no manual wake is sent.
func TestEmbedWorkerDLQPublicDBMutationWakesAndDrains(t *testing.T) {
	for _, action := range []string{"idempotent_update", "source_preserving_update", "anonymize_user_data", "delete", "delete_user_data", "delete_after_update_claim"} {
		t.Run(action, func(t *testing.T) {
			disk, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
			fixtureowner.Own(t, disk)
			require.NoError(t, err)
			disk.SetEmbeddingsEnabled(true)
			provider := &mutationWakeProvider{mockEmbedder: &mockEmbedder{dims: 2, model: "mutation-wake"}, entered: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(provider.release) }) }
			store := &mutationWakeStore{BadgerEngine: disk, provider: provider, idle: make(chan struct{}), idleAfter: 1}
			ns := storage.NewNamespacedEngine(store, "nornic")
			cfg := DefaultEmbedWorkerConfig()
			cfg.DeferWorkerStart, cfg.NumWorkers, cfg.EmbedBatchSize = true, 1, 1
			cfg.BatchDelay, cfg.TriggerDebounceDelay = 0, 0
			cfg.ScanInterval, cfg.ProviderRetryBackoff, cfg.ProviderRetryBackoffMax = time.Hour, time.Hour, time.Hour
			cfg.PropertiesInclude = []string{"text"}
			worker := NewEmbedWorker(provider, store, cfg)
			config := DefaultConfig()
			config.EmbeddingWorker.PropertiesInclude = []string{"text"}
			config.Compliance.SubjectIdentifierProperties = []string{"user_id"}
			config.Compliance.SubjectPseudonymizeProperties = []string{"user_id"}
			svc := search.NewService(ns, 2, nil)
			done := make(chan struct{})
			close(done)
			entry := &dbSearchService{dbName: "nornic", engine: ns, svc: svc, buildDone: done}
			entry.buildOnce.Do(func() {})
			db := &DB{storage: ns, baseStorage: store, config: config, embedQueue: worker,
				searchServices: map[string]*dbSearchService{"nornic": entry}}
			t.Cleanup(func() { release(); worker.Close(); _ = svc.Close(); _ = disk.Close() })
			require.NoError(t, svc.BuildIndexes(context.Background()))
			_, err = ns.CreateNode(&storage.Node{ID: "doc", Properties: map[string]any{"text": "same source", "annotation": "before", "user_id": "subject"}})
			require.NoError(t, err)
			worker.StartWorkers()
			select {
			case <-store.idle:
			case <-time.After(5 * time.Second):
				t.Fatal("worker did not finish its empty claims after provider failure")
			}
			require.Equal(t, 1, worker.Stats().Failed)
			require.Equal(t, 1, db.PendingEmbeddingsCount())
			require.Zero(t, worker.Stats().InFlight)

			if action == "delete" || action == "delete_user_data" {
				if action == "delete_user_data" {
					require.NoError(t, db.DeleteUserData(context.Background(), "subject"))
				} else {
					require.NoError(t, db.DeleteNode(context.Background(), "doc"))
				}
				_, err := ns.GetNode("doc")
				require.ErrorIs(t, err, storage.ErrNotFound)
				assert.Zero(t, db.PendingEmbeddingsCount(), "a deleted DLQ node must stop counting immediately")
				ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
				defer cancel()
				assert.NoError(t, db.WaitForEmbeddings(ctx), "draining cannot wait for a deleted node's old backoff")
				return
			}

			before, err := ns.GetNode("doc")
			require.NoError(t, err)
			properties := map[string]any{"text": "same source"}
			if action == "source_preserving_update" {
				properties = map[string]any{"annotation": "after"}
			}
			if action == "delete_after_update_claim" {
				provider.hold.Store(true)
			}
			if action == "anonymize_user_data" {
				err = db.AnonymizeUserData(context.Background(), "subject")
			} else {
				_, err = db.UpdateNode(context.Background(), "doc", properties)
			}
			require.NoError(t, err)
			after, err := ns.GetNode("doc")
			require.NoError(t, err)
			if action == "anonymize_user_data" {
				require.NotEqual(t, "subject", after.Properties["user_id"])
			}
			require.True(t, storage.EmbeddingSourceMatches(before, after, embeddingutil.TextPolicy(embeddingutil.EmbedTextOptionsFromConfig(config))))
			same, err := before.EmbeddingRevision().MatchesNode(after)
			require.NoError(t, err)
			require.False(t, same, "even the idempotent public update creates a new body attempt")
			if action == "delete_after_update_claim" {
				select {
				case <-provider.entered:
				case <-time.After(time.Second):
					t.Fatal("the public update did not wake its fresh claim")
				}
				require.NoError(t, db.DeleteNode(context.Background(), "doc"))
				release()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				require.NoError(t, db.WaitForEmbeddings(ctx))
				require.Zero(t, disk.PendingEmbeddingsCount(), "releasing an invalidated claim cannot resurrect the deleted MAIN marker")
				require.Zero(t, worker.Stats().Processed)
				return
			}
			// WaitForEmbeddings itself triggers the worker. Check the update's
			// own notification before using that public drain API.
			require.Eventually(t, func() bool { return worker.Stats().Processed == 1 }, time.Second, time.Millisecond,
				"the public update must wake MAIN without waiting for the hour-long DLQ deadline")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			require.NoError(t, db.WaitForEmbeddings(ctx))
			stored, err := ns.GetNode("doc")
			require.NoError(t, err)
			require.NotEmpty(t, stored.ChunkEmbeddings)
		})
	}
}

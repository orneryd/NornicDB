package search

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// sol r3 finding 1: a BM25 checkpoint is encoded from a snapshot the encoder
// owns. Live writes are admitted while it is written, so encoding the live V2
// maps raced them (a data race under -race; a concurrent-map crash or a torn
// snapshot in production). Both the base checkpoint and the background
// persist run here while a writer indexes and removes documents.
func TestBaseBM25CheckpointEncodesOwnedSnapshot(t *testing.T) {
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			engine := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			svc := configureNodeAdmissionService(t, engine, t.TempDir(), backend, nil)
			require.NoError(t, svc.BuildIndexes(context.Background()))
			// Enough documents that encoding a checkpoint takes measurable time.
			const seeds = 3000
			for i := range seeds {
				n := admissionNode(fmt.Sprintf("nornic:seed%d", i), fmt.Sprintf("seedterm%d common", i), []float32{1, float32(i % 7), 0}, "named")
				_, err := engine.CreateNode(n)
				require.NoError(t, err)
				require.NoError(t, svc.IndexNode(n))
			}

			stop := make(chan struct{})
			var writes atomic.Int64
			writerDone := make(chan error, 1)
			go func() {
				for i := 0; ; i++ {
					select {
					case <-stop:
						writerDone <- nil
						return
					default:
					}
					id := fmt.Sprintf("nornic:live%d", i%50)
					n := admissionNode(id, fmt.Sprintf("liveterm%d", i), []float32{0, 1, float32(i % 5)}, "named")
					_, _ = engine.CreateNode(n)
					if err := svc.IndexNode(n); err != nil {
						writerDone <- err
						return
					}
					if i%3 == 0 {
						if err := svc.RemoveNode(storage.NodeID(id)); err != nil {
							writerDone <- err
							return
						}
					}
					writes.Add(1)
				}
			}()
			for range 3 {
				require.NoError(t, svc.persistBaseIndexes())
				svc.persistBM25Background(svc.fulltextIndexPath)
			}
			close(stop)
			require.NoError(t, <-writerDone)
			require.Positive(t, writes.Load(), "no write ran during the checkpoints")

			// The saved checkpoint decodes to a consistent index holding every
			// seed document.
			probe, _ := newBM25Index(backend)
			require.NoError(t, probe.Load(svc.fulltextIndexPath))
			require.GreaterOrEqual(t, probe.Count(), seeds)
			hits := probe.Search("seedterm42", 10)
			require.NotEmpty(t, hits)
			require.Equal(t, "nornic:seed42", hits[0].ID)
		})
	}
}

// sol r3 finding 2: writes accepted after the base BM25 save of an explicit
// rebuild survive the warmup and readiness: the build keeps its live BM25
// index instead of clearing it and reloading the older checkpoint.
func TestWritesAfterBaseBM25SaveSurviveWarmup(t *testing.T) {
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			engine := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			_, err := engine.CreateNode(admissionNode("nornic:a", "oldterm", []float32{1, 0, 0}, "named"))
			require.NoError(t, err)
			dir := t.TempDir()
			svc := configureNodeAdmissionService(t, engine, dir, backend, nil)
			var hookErrs []error
			writeAfterSave := func(s *Service) {
				if s != svc {
					return
				}
				updated := admissionNode("nornic:a", "newterm", []float32{1, 0, 0}, "named")
				inserted := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
				_, createErr := engine.CreateNode(inserted)
				hookErrs = append(hookErrs, engine.UpdateNode(updated), svc.IndexNode(updated), createErr, svc.IndexNode(inserted))
			}
			basePersistedHook.Store(&writeAfterSave)
			t.Cleanup(func() { basePersistedHook.Store(nil) })
			require.NoError(t, svc.BuildIndexes(context.Background()))
			basePersistedHook.Store(nil)
			require.Len(t, hookErrs, 4, "the writes ran after the base save")
			for _, err := range hookErrs {
				require.NoError(t, err)
			}
			require.True(t, svc.IsReady())
			require.Equal(t, []string{"nornic:a"}, searchIDs(t, svc, "newterm"), "an update after the base save was lost")
			require.Empty(t, searchIDs(t, svc, "oldterm"), "the older checkpoint came back")
			require.Equal(t, []string{"nornic:b"}, searchIDs(t, svc, "bterm"), "an insert after the base save was lost")

			// The final persist saved those writes: the checkpoint a restart
			// restores holds them, and the restarted service serves them.
			saved, _ := newBM25Index(backend)
			require.NoError(t, saved.Load(filepath.Join(dir, "bm25")))
			require.Len(t, saved.Search("newterm", 10), 1, "the saved checkpoint lacks the update")
			require.Empty(t, saved.Search("oldterm", 10), "the saved checkpoint kept the replaced text")
			require.Len(t, saved.Search("bterm", 10), 1, "the saved checkpoint lacks the insert")
			restarted := configureNodeAdmissionService(t, engine, dir, backend, nil)
			require.NoError(t, restarted.BuildIndexes(context.Background()))
			require.True(t, restarted.IsReady())
			require.Equal(t, []string{"nornic:a"}, searchIDs(t, restarted, "newterm"))
			require.Empty(t, searchIDs(t, restarted, "oldterm"))
			require.Equal(t, []string{"nornic:b"}, searchIDs(t, restarted, "bterm"))
		})
	}
}

// sol r3 finding 3: a BM25 save is acknowledged only once it is published.
// A rejected publication or a failed final rename leaves the index dirty, so
// a later persist retries and updates the final file.
func TestBM25SaveAcknowledgedOnlyAfterPublication(t *testing.T) {
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			engine := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			_, err := engine.CreateNode(admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named"))
			require.NoError(t, err)
			dir := t.TempDir()
			svc := configureNodeAdmissionService(t, engine, dir, backend, nil)
			require.NoError(t, svc.BuildIndexes(context.Background()))
			bm25Path := filepath.Join(dir, "bm25")
			b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
			_, err = engine.CreateNode(b)
			require.NoError(t, err)
			require.NoError(t, svc.IndexNode(b))
			require.True(t, svc.fulltext().IsDirty())

			// A population starts between the save and its publication.
			rejectPublish := func(s *Service, path string) {
				if s == svc && path == bm25Path {
					s.buildInProgress.Store(true)
				}
			}
			artifactPublishHook.Store(&rejectPublish)
			svc.persistBM25Background(bm25Path)
			artifactPublishHook.Store(nil)
			svc.buildInProgress.Store(false)
			require.True(t, svc.fulltext().IsDirty(), "a rejected publication marked the index persisted")

			// The final rename fails: the staged file is gone when it is published.
			loseStaged := func(s *Service, path string) {
				if s == svc && path == bm25Path {
					_ = os.Remove(path + ".staged")
				}
			}
			artifactPublishHook.Store(&loseStaged)
			svc.persistBM25Background(bm25Path)
			artifactPublishHook.Store(nil)
			require.True(t, svc.fulltext().IsDirty(), "a failed final rename marked the index persisted")

			// The retry publishes the update.
			svc.persistBM25Background(bm25Path)
			require.False(t, svc.fulltext().IsDirty())
			probe, _ := newBM25Index(backend)
			require.NoError(t, probe.Load(bm25Path))
			require.Equal(t, 2, probe.Count(), "the final file lacks the update")
		})
	}
}

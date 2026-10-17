package search

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// sol r3 finding 1: the base BM25 checkpoint is encoded from a snapshot the
// encoder owns, so a write admitted during the encode cannot race it (run
// with -race) or tear it, and the write does not wait for the disk I/O.
func TestBaseBM25CheckpointEncodesOwnedSnapshot(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	_, err := engine.CreateNode(admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named"))
	require.NoError(t, err)
	svc := configureNodeAdmissionService(t, engine, t.TempDir(), "v2", nil)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
	_, err = engine.CreateNode(b)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(b)) // dirty: the base persist encodes BM25

	entered, release := make(chan struct{}), make(chan struct{})
	holdEncode := func() {
		select {
		case <-entered:
		default:
			close(entered)
			<-release
		}
	}
	bm25SnapshotEncodeHook.Store(&holdEncode)
	t.Cleanup(func() { bm25SnapshotEncodeHook.Store(nil) })
	persisted := make(chan error, 1)
	go func() { persisted <- svc.persistBaseIndexes() }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the base persist did not reach the BM25 encode")
	}
	// Writes admitted while the snapshot is being encoded.
	c := admissionNode("nornic:c", "cterm", []float32{0, 0, 1}, "named")
	_, err = engine.CreateNode(c)
	require.NoError(t, err)
	wrote := make(chan error, 1)
	go func() {
		if err := svc.IndexNode(c); err != nil {
			wrote <- err
			return
		}
		wrote <- svc.RemoveNode("nornic:a")
	}()
	select {
	case err := <-wrote:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("a live write waited for the BM25 checkpoint encode")
	}
	close(release)
	select {
	case err := <-persisted:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the base persist did not finish")
	}
	require.Equal(t, []string{"nornic:c"}, searchIDs(t, svc, "cterm"))
	require.Empty(t, searchIDs(t, svc, "aterm"))
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
			svc := configureNodeAdmissionService(t, engine, t.TempDir(), backend, nil)
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

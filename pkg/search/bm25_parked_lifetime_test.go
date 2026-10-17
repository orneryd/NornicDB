package search

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The parked-BM25 lifetime: an explicit build parks BM25 after its base
// persist and reloads the checkpoint after the warmup. Every build exit leaves
// BM25 resident or parked behind a valid checkpoint, no persist serializes the
// parked placeholder, and a restart accepts a checkpoint only when it matches
// storage.

const (
	warmupStartMessage   = "starting vector pipeline warmup"
	bm25RebuildMessage   = "rebuilding BM25 from storage"
	bm25MismatchMessage  = "BM25 checkpoint does not match storage"
	iterationDoneMessage = "base index iteration complete"
)

func parkedLifetimeEngine(t *testing.T) storage.Engine {
	t.Helper()
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	for _, node := range []*storage.Node{
		admissionNode("nornic:a", "alpha", []float32{1, 0, 0}, "named"),
		admissionNode("nornic:b", "beta", []float32{0, 1, 0}, "named"),
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	return engine
}

// hookedService is a persisted service whose log messages also go to hook.
func hookedService(t *testing.T, engine storage.Engine, dir, backend string, hook func(svc *Service, message string)) (*Service, *recordingLog) {
	t.Helper()
	svc := configureNodeAdmissionService(t, engine, dir, backend, nil)
	logs := &recordingLog{}
	if hook != nil {
		logs.onMessage = func(message string) { hook(svc, message) }
	}
	svc.SetLogger(slog.New(logs))
	return svc, logs
}

// checkpointDocs reads the saved BM25 file with an unbuilt service of the
// same backend.
func checkpointDocs(t *testing.T, engine storage.Engine, dir, backend string) int {
	t.Helper()
	reader := configureNodeAdmissionService(t, engine, t.TempDir(), backend, nil)
	accepted, err := reader.fulltext().loadSnapshot(filepath.Join(dir, "bm25"))
	require.NoError(t, err)
	require.True(t, accepted, "the BM25 checkpoint is readable")
	return reader.fulltext().Count()
}

func requireFulltextFinds(t *testing.T, svc *Service, want map[string]string) {
	t.Helper()
	require.True(t, svc.IsReady())
	for term, id := range want {
		require.Equal(t, []string{id}, parkingSearchIDs(t, svc, term), "fulltext search for %q", term)
	}
}

func parkedLifetimeBackends(t *testing.T, run func(t *testing.T, backend string)) {
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) { run(t, backend) })
	}
}

// E3: a build cancelled during the warmup, after NODE admission, leaves BM25
// resident; an immediate persist keeps the checkpoint, and a live write, a
// following build and a restart all serve every document.
func TestParkedBM25CancelledDuringWarmup(t *testing.T) {
	parkedLifetimeBackends(t, func(t *testing.T, backend string) {
		engine := parkedLifetimeEngine(t)
		dir := t.TempDir()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		svc, logs := hookedService(t, engine, dir, backend, func(_ *Service, message string) {
			if strings.Contains(message, warmupStartMessage) {
				cancel()
			}
		})
		require.ErrorIs(t, svc.BuildIndexes(ctx), context.Canceled)
		require.True(t, logs.saw(bm25ParkedMessage))

		svc.PersistIndexesToDisk()
		require.Equal(t, 2, checkpointDocs(t, engine, dir, backend), "the persist kept the checkpoint")
		require.Equal(t, 2, svc.FulltextDocCount(), "BM25 is resident after the build exit")

		c := admissionNode("nornic:c", "gamma", []float32{0, 0, 1}, "named")
		_, err := engine.CreateNode(c)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(c))
		require.Equal(t, 3, svc.FulltextDocCount())

		require.NoError(t, svc.BuildIndexes(context.Background()))
		requireFulltextFinds(t, svc, map[string]string{"alpha": "nornic:a", "beta": "nornic:b", "gamma": "nornic:c"})
		svc.PersistIndexesToDisk()
		require.NoError(t, svc.Close())

		restarted := configureNodeAdmissionService(t, engine, dir, backend, nil)
		require.NoError(t, restarted.BuildIndexes(context.Background()))
		requireFulltextFinds(t, restarted, map[string]string{"alpha": "nornic:a", "beta": "nornic:b", "gamma": "nornic:c"})
	})
}

// E2: a build cancelled after parking and before NODE admission leaves BM25
// resident and the checkpoint intact.
func TestParkedBM25CancelledBeforeNodeAdmission(t *testing.T) {
	parkedLifetimeBackends(t, func(t *testing.T, backend string) {
		engine := parkedLifetimeEngine(t)
		dir := t.TempDir()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		svc, _ := hookedService(t, engine, dir, backend, func(_ *Service, message string) {
			if strings.Contains(message, bm25ParkedMessage) {
				cancel()
			}
		})
		require.ErrorIs(t, svc.BuildIndexes(ctx), context.Canceled)
		svc.PersistIndexesToDisk()
		svc.runPersist()
		require.Equal(t, 2, checkpointDocs(t, engine, dir, backend))
		require.Equal(t, 2, svc.FulltextDocCount(), "BM25 is resident after the build exit")

		require.NoError(t, svc.BuildIndexes(context.Background()))
		requireFulltextFinds(t, svc, map[string]string{"alpha": "nornic:a", "beta": "nornic:b"})
	})
}

// E1: a base persist that cannot save BM25 fails the build before parking;
// BM25 stays resident.
func TestParkedBM25NotParkedWhenTheBasePersistFails(t *testing.T) {
	parkedLifetimeBackends(t, func(t *testing.T, backend string) {
		engine := parkedLifetimeEngine(t)
		dir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "bm25", "occupied"), 0o755))
		svc, logs := hookedService(t, engine, dir, backend, nil)
		err := svc.BuildIndexes(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "save BM25 index")
		require.False(t, logs.saw(bm25ParkedMessage))
		require.Equal(t, 2, svc.FulltextDocCount())
	})
}

// E4: when the checkpoint cannot be reloaded after the warmup, the same build
// rebuilds BM25 from storage. Until that rebuild completes, search reports
// that the index is building instead of answering from a partial index.
func TestParkedBM25RebuiltWhenTheCheckpointCannotBeReloaded(t *testing.T) {
	corruptions := map[string]func(t *testing.T, svc *Service, path string){
		"rejected_snapshot": func(t *testing.T, _ *Service, path string) {
			require.NoError(t, os.WriteFile(path, []byte("not a BM25 snapshot"), 0o644))
		},
		"document_count_mismatch": func(_ *testing.T, svc *Service, _ string) {
			svc.indexMu.Lock()
			svc.nodeSources.fulltextCheckpointCount++
			svc.indexMu.Unlock()
		},
	}
	for name, corrupt := range corruptions {
		t.Run(name, func(t *testing.T) {
			parkedLifetimeBackends(t, func(t *testing.T, backend string) {
				engine := parkedLifetimeEngine(t)
				dir := t.TempDir()
				var corruptOnce sync.Once
				var mu sync.Mutex
				rebuilding := false
				var windowErrs []error
				var windowReady []bool
				svc, logs := hookedService(t, engine, dir, backend, func(svc *Service, message string) {
					switch {
					case strings.Contains(message, warmupStartMessage):
						corruptOnce.Do(func() { corrupt(t, svc, filepath.Join(dir, "bm25")) })
					case strings.Contains(message, bm25RebuildMessage):
						mu.Lock()
						rebuilding = true
						mu.Unlock()
						fallthrough
					case strings.Contains(message, iterationDoneMessage):
						mu.Lock()
						inWindow := rebuilding
						mu.Unlock()
						if !inWindow {
							return
						}
						_, err := svc.Search(context.Background(), "alpha", nil, &SearchOptions{Limit: 10})
						mu.Lock()
						windowErrs = append(windowErrs, err)
						windowReady = append(windowReady, svc.IsReady())
						mu.Unlock()
					}
				})
				require.NoError(t, svc.BuildIndexes(context.Background()))
				require.True(t, logs.saw(bm25RebuildMessage), "the build rebuilt BM25 itself")
				mu.Lock()
				require.NotEmpty(t, windowErrs, "searched during the rebuild")
				for i, err := range windowErrs {
					require.ErrorIs(t, err, ErrSearchIndexBuilding, "a search during the rebuild reports building")
					require.False(t, windowReady[i])
				}
				mu.Unlock()
				requireFulltextFinds(t, svc, map[string]string{"alpha": "nornic:a", "beta": "nornic:b"})
				require.Equal(t, 2, checkpointDocs(t, engine, dir, backend), "the rebuild wrote a valid checkpoint")

				svc.PersistIndexesToDisk()
				require.NoError(t, svc.Close())
				restarted := configureNodeAdmissionService(t, engine, dir, backend, nil)
				require.NoError(t, restarted.BuildIndexes(context.Background()))
				requireFulltextFinds(t, restarted, map[string]string{"alpha": "nornic:a", "beta": "nornic:b"})
			})
		})
	}
}

// E6: Close during the warmup leaves BM25 parked with the service gone. The
// checkpoint on disk is used by the restart only when it matches storage;
// otherwise the restart rebuilds BM25 from storage.
func TestParkedBM25CloseDuringWarmupRestart(t *testing.T) {
	type restartCase struct {
		change  func(t *testing.T, engine storage.Engine, dir, backend string)
		want    map[string]string
		rebuilt bool
	}
	cases := map[string]restartCase{
		"checkpoint_matches_storage": {
			want: map[string]string{"alpha": "nornic:a", "beta": "nornic:b"},
		},
		"storage_changed": {
			change: func(t *testing.T, engine storage.Engine, _, _ string) {
				_, err := engine.CreateNode(admissionNode("nornic:c", "gamma", []float32{0, 0, 1}, "named"))
				require.NoError(t, err)
			},
			want:    map[string]string{"alpha": "nornic:a", "beta": "nornic:b", "gamma": "nornic:c"},
			rebuilt: true,
		},
		"document_count_mismatch": {
			change: func(t *testing.T, engine storage.Engine, dir, backend string) {
				other := configureNodeAdmissionService(t, engine, t.TempDir(), backend, nil)
				other.fulltext().Index("nornic:a", "alpha")
				require.NoError(t, other.fulltext().SaveNoCopy(filepath.Join(dir, "bm25")))
			},
			want:    map[string]string{"alpha": "nornic:a", "beta": "nornic:b"},
			rebuilt: true,
		},
		"rejected_snapshot": {
			change: func(t *testing.T, _ storage.Engine, dir, _ string) {
				require.NoError(t, os.WriteFile(filepath.Join(dir, "bm25"), []byte("not a BM25 snapshot"), 0o644))
			},
			want: map[string]string{"alpha": "nornic:a", "beta": "nornic:b"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			parkedLifetimeBackends(t, func(t *testing.T, backend string) {
				engine := parkedLifetimeEngine(t)
				dir := t.TempDir()
				closed := make(chan error, 1)
				var closeOnce sync.Once
				svc, _ := hookedService(t, engine, dir, backend, func(svc *Service, message string) {
					if strings.Contains(message, warmupStartMessage) {
						closeOnce.Do(func() {
							go func() { closed <- svc.Close() }()
							<-svc.lifecycleCtx.Done()
						})
					}
				})
				require.Error(t, svc.BuildIndexes(context.Background()))
				require.NoError(t, <-closed)
				svc.PersistIndexesToDisk()
				require.Equal(t, 2, checkpointDocs(t, engine, dir, backend), "the checkpoint survives Close")

				if tc.change != nil {
					tc.change(t, engine, dir, backend)
				}
				restarted, logs := hookedService(t, engine, dir, backend, nil)
				require.NoError(t, restarted.BuildIndexes(context.Background()))
				requireFulltextFinds(t, restarted, tc.want)
				require.Equal(t, tc.rebuilt, logs.saw(bm25MismatchMessage), "restart verified the checkpoint against storage")
				if !tc.rebuilt && name == "checkpoint_matches_storage" {
					// E7: the restore path never parks.
					require.False(t, logs.saw(bm25ParkedMessage))
				}
			})
		})
	}
}

// P1-P3 backstop: no persist entry point serializes a parked placeholder; each
// keeps the checkpoint, and the next writer restores BM25 from it.
func TestParkedBM25PlaceholderIsNeverPersisted(t *testing.T) {
	parkedLifetimeBackends(t, func(t *testing.T, backend string) {
		engine := parkedLifetimeEngine(t)
		dir := t.TempDir()
		svc := configureNodeAdmissionService(t, engine, dir, backend, nil)
		require.NoError(t, svc.BuildIndexes(context.Background()))
		svc.PersistIndexesToDisk()

		svc.indexMu.Lock()
		svc.setNodeFulltextCheckpointLocked(filepath.Join(dir, "bm25"), svc.fulltext().Count())
		svc.fulltext().Clear()
		svc.indexMu.Unlock()

		svc.runPersist()
		svc.PersistIndexesToDisk()
		require.NoError(t, svc.persistBaseIndexes())
		require.Equal(t, 2, checkpointDocs(t, engine, dir, backend), "no persist wrote the placeholder")

		c := admissionNode("nornic:c", "gamma", []float32{0, 0, 1}, "named")
		_, err := engine.CreateNode(c)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(c))
		require.Equal(t, 3, svc.FulltextDocCount())
		requireFulltextFinds(t, svc, map[string]string{"alpha": "nornic:a", "gamma": "nornic:c"})
	})
}

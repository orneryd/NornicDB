package search

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// persistedRestartService saves a complete image holding node a, then returns
// a fresh service over the same storage and paths whose initial population has
// not run: the next population restores that image.
func persistedRestartService(t *testing.T, backend string) (*Service, *storage.MemoryEngine) {
	t.Helper()
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	_, err := engine.CreateNode(admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named"))
	require.NoError(t, err)
	dir := t.TempDir()
	seed := configureNodeAdmissionService(t, engine, dir, backend, nil)
	require.NoError(t, seed.BuildIndexes(context.Background()))
	seed.PersistIndexesToDisk()
	require.NoError(t, seed.Close())
	return configureNodeAdmissionService(t, engine, dir, backend, nil), engine
}

func searchIDs(t *testing.T, svc *Service, term string) []string {
	t.Helper()
	resp, err := svc.Search(context.Background(), term, nil, &SearchOptions{Limit: 10})
	require.NoError(t, err)
	ids := make([]string, 0, len(resp.Results))
	for _, r := range resp.Results {
		ids = append(ids, r.ID)
	}
	return ids
}

// Review 4300779913: a population cancelled after it withdrew the recorded
// live writes must re-apply them. The writes already returned success, so
// they stay in effect, by text and by vector, whether or not the population
// completes; a recorded removal also holds against the restored image.
func TestCancelledPopulationReappliesWithdrawnWrites(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			svc, engine := persistedRestartService(t, backend)
			entered, release := make(chan struct{}), make(chan struct{})
			holdStage := func(s *Service) {
				if s == svc {
					close(entered)
					<-release
				}
			}
			populationStageHook.Store(&holdStage)
			cancelAfterWithdraw := func(s *Service) {
				if s == svc {
					s.cancelWorkers(workerAdmission)
				}
			}
			populationRestoreHook.Store(&cancelAfterWithdraw)
			t.Cleanup(func() {
				populationStageHook.Store(nil)
				populationRestoreHook.Store(nil)
			})

			b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
			_, err := engine.CreateNode(b)
			require.NoError(t, err)
			require.NoError(t, svc.IndexNode(b)) // starts the write-started population
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("the write-started population did not reach staging")
			}
			populationStageHook.Store(nil)
			require.NoError(t, engine.DeleteNode("nornic:a"))
			require.NoError(t, svc.RemoveNode("nornic:a")) // recorded removal of a saved node
			close(release)
			require.Eventually(t, func() bool { return svc.pendingNodeAdmission() == nil }, 10*time.Second, 10*time.Millisecond,
				"the write-started population did not finish after cancellation")
			populationRestoreHook.Store(nil)

			require.Equal(t, []string{"nornic:b"}, searchIDs(t, svc, "bterm"),
				"a successful write disappeared when its population was cancelled after withdrawal")
			require.Contains(t, vectorHitNodeIDs(t, svc, []float32{0, 1, 0}), "nornic:b", "the write's vector")
			require.Empty(t, searchIDs(t, svc, "aterm"), "a successful removal came back with the restored image")
			require.NotContains(t, vectorHitNodeIDs(t, svc, []float32{1, 0, 0}), "nornic:a", "the removed node's vector")
		})
	}
}

// Review 4300779916: restoring the saved image reads it from disk without the
// live-write lock, so a write during that read returns at once.
func TestPersistedRestoreDoesNotBlockLiveWrites(t *testing.T) {
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			svc, engine := persistedRestartService(t, backend)
			entered, release := make(chan struct{}), make(chan struct{})
			holdStage := func(s *Service) {
				if s == svc {
					close(entered)
					<-release
				}
			}
			populationStageHook.Store(&holdStage)
			t.Cleanup(func() { populationStageHook.Store(nil) })
			var released bool
			releaseStage := func() {
				if !released {
					released = true
					close(release)
				}
			}
			defer releaseStage()

			built := make(chan error, 1)
			go func() { built <- svc.BuildIndexes(context.Background()) }()
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("the population did not reach the saved-image read")
			}
			b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
			_, err := engine.CreateNode(b)
			require.NoError(t, err)
			wrote := make(chan error, 1)
			go func() { wrote <- svc.IndexNode(b) }()
			select {
			case err := <-wrote:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("a live write waited for the saved-image read")
			}
			releaseStage()
			populationStageHook.Store(nil)
			select {
			case err := <-built:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("the population did not finish")
			}
			require.Equal(t, []string{"nornic:a"}, searchIDs(t, svc, "aterm"), "the restored image")
			require.Equal(t, []string{"nornic:b"}, searchIDs(t, svc, "bterm"), "the write made during the restore")
		})
	}
}

// F-I86: a search during a restore step must not miss a write that already
// returned success. The step withdraws the recorded writes and re-applies
// them before releasing indexMu; searches do not take indexMu, so they must
// not observe the step half-done.
func TestSearchDuringRestoreStepSeesRecordedWrites(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			svc, engine := persistedRestartService(t, backend)
			type seen struct{ text, vector []string }
			during := make(chan seen, 1)
			searchMidStep := func(s *Service) {
				if s != svc {
					return
				}
				go func() {
					var got seen
					if resp, err := svc.Search(context.Background(), "bterm", nil, &SearchOptions{Limit: 10}); err == nil {
						for _, r := range resp.Results {
							got.text = append(got.text, r.ID)
						}
					}
					if hits, err := svc.VectorSearchCandidates(context.Background(), []float32{0, 1, 0}, &SearchOptions{Limit: 10}); err == nil {
						for _, h := range hits {
							got.vector = append(got.vector, normalizeVectorResultIDToNodeID(h.ID))
						}
					}
					during <- got
				}()
				// Keep the step open while that search runs.
				time.Sleep(100 * time.Millisecond)
			}
			populationRestoreHook.Store(&searchMidStep)
			t.Cleanup(func() { populationRestoreHook.Store(nil) })

			b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
			_, err := engine.CreateNode(b)
			require.NoError(t, err)
			require.NoError(t, svc.IndexNode(b))
			var got seen
			select {
			case got = <-during:
			case <-time.After(10 * time.Second):
				t.Fatal("the population did not reach its restore step")
			}
			populationRestoreHook.Store(nil)
			require.Contains(t, got.text, "nornic:b", "a text search during the restore step missed a successful write")
			require.Contains(t, got.vector, "nornic:b", "a vector search during the restore step missed a successful write")
		})
	}
}

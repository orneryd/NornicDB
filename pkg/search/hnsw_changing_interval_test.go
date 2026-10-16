package search

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Gate the real rebuild and factory using existing operator logs. In particular,
// warmup logs after the changing flag becomes false, before taking pipelineMu.
type changingIntervalLog struct {
	entered [3]chan struct{}
	release [3]chan struct{}
	once    [3]sync.Once
}

func (h *changingIntervalLog) Enabled(context.Context, slog.Level) bool { return true }
func (h *changingIntervalLog) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *changingIntervalLog) WithGroup(string) slog.Handler            { return h }
func (h *changingIntervalLog) Handle(_ context.Context, r slog.Record) error {
	for i, text := range []string{"BM25 disabled — skipping fulltext build", "Vector search strategy:", "Warming up vector search pipeline"} {
		if strings.Contains(r.Message, text) {
			h.once[i].Do(func() { close(h.entered[i]); <-h.release[i] })
		}
	}
	return nil
}

func TestVectorFactoryChangingIntervalCannotBecomeCurrent(t *testing.T) {
	for _, replace := range []bool{true, false} {
		name := "replacement"
		if !replace {
			name = "compatible_source"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
			var svc *Service
			if replace {
				svc = factoryRetirementService(t, true)
				// Force a real file-to-memory replacement, including the final
				// retirement barrier that now precedes warmup.
				svc.vectorStorageMode = "memory"
			} else {
				svc = NewServiceWithDimensions(newNamespacedEngine(t), 3)
				t.Cleanup(func() { _ = svc.Close() })
			}
			svc.SetIndexFlags(false, true)
			dir := t.TempDir()
			path := filepath.Join(dir, "vectors")
			svc.SetVectorIndexPath(path)
			svc.SetPersistenceEnabled(true)
			node := &storage.Node{ID: "replacement", ChunkEmbeddings: [][]float32{{0, 1, 0}}}
			_, err := svc.engine.CreateNode(node)
			require.NoError(t, err)
			if replace {
				settings := svc.currentSearchBuildSettings()
				settings.Vector += ";previous-layout"
				require.NoError(t, saveSearchBuildSettings(searchBuildSettingsPath("", path, ""), settings))
			} else {
				// Admit both NODE sources through the real owner. The repeated
				// compatible build keeps this file and has no later replacement
				// barrier, isolating availability across the changing interval.
				svc.SetFulltextIndexPath(filepath.Join(dir, "fulltext"))
				require.NoError(t, svc.BuildIndexes(context.Background()))
			}
			old := svc.vectorFileStore
			require.NotNil(t, old)
			h := &changingIntervalLog{}
			var releaseOnce [3]sync.Once
			for i := range h.entered {
				h.entered[i], h.release[i] = make(chan struct{}), make(chan struct{})
			}
			release := func(i int) { releaseOnce[i].Do(func() { close(h.release[i]) }) }
			svc.SetLogger(slog.New(h))
			await := func(ch <-chan struct{}, label string) {
				t.Helper()
				select {
				case <-ch:
				case <-time.After(5 * time.Second):
					t.Fatalf("%s did not reach its gate", label)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			buildDone, factoryDone := make(chan struct{}), make(chan struct{})
			var buildErr, factoryErr error
			factoryStarted := false
			defer func() {
				cancel()
				for i := range h.release {
					release(i)
				}
				for _, done := range []<-chan struct{}{factoryDone, buildDone} {
					if done == factoryDone && !factoryStarted {
						continue
					}
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("changing-interval worker did not join during cleanup")
					}
				}
			}()
			buildID := make(chan string, 1)
			go func() {
				buildID <- serviceTestGoroutineID()
				buildErr = svc.BuildIndexes(ctx)
				close(buildDone)
			}()
			builder := <-buildID
			await(h.entered[0], "rebuild after reset")
			require.True(t, svc.hnswVectorsChanging.Load())
			generation := svc.hnswGeneration.Load()
			factoryStarted = true
			go func() {
				_, factoryErr = svc.getOrCreateVectorPipeline(ctx)
				close(factoryDone)
			}()
			await(h.entered[1], "factory with captured source")
			release(0)
			if replace {
				// The factory owns pipelineMu at its log. Replacement must
				// retire through that mutex before warmup can begin.
				require.Eventually(t, func() bool {
					return serviceStackContains(builder, "(*Service).resetBaseVectorSource", "(*Service).retireVectorSourceLocked", "sync.(*RWMutex).Lock")
				}, 5*time.Second, time.Millisecond, "replacement did not reach the factory publication barrier")
				require.True(t, svc.hnswVectorsChanging.Load())
				require.Greater(t, svc.hnswGeneration.Load(), generation)
				release(1)
				await(factoryDone, "factory before replacement warmup")
				require.NoError(t, factoryErr)
			}
			await(h.entered[2], "warmup after NODE population")
			require.False(t, svc.hnswVectorsChanging.Load())
			svc.mu.RLock()
			current := svc.vectorFileStore
			svc.mu.RUnlock()
			if replace {
				require.NotSame(t, old, current)
			} else {
				require.Same(t, old, current)
				require.Equal(t, generation, svc.hnswGeneration.Load(), "proof requires the same generation on both sides of the changing interval")
				release(1)
				await(factoryDone, "factory after changing interval")
				require.NoError(t, factoryErr)
			}
			svc.pipelineMu.RLock()
			cached := cachedVectorPipeline(svc)
			svc.pipelineMu.RUnlock()
			require.Nil(t, cached, "factory admitted during replacement cached a source after warmup cleared the changing flag")
			release(2)
			await(buildDone, "completed rebuild")
			require.NoError(t, buildErr)
			candidates, err := svc.VectorSearchCandidates(context.Background(), []float32{0, 1, 0}, &SearchOptions{Limit: 1, MaxCandidateLimit: 1})
			require.NoError(t, err)
			require.Len(t, candidates, 1)
			require.Equal(t, "replacement", candidates[0].ID)
		})
	}
}

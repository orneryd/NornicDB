package nornicdb

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Block the real construction worker at its snapshot log, without exporting a
// search-internal test hook. Teardown must cancel it and wait for its return.
type blockedDatabaseHNSWBuild struct {
	entered    chan struct{}
	release    chan struct{}
	once       sync.Once
	installed  atomic.Int32
	fileBacked atomic.Bool
}

func (h *blockedDatabaseHNSWBuild) Enabled(context.Context, slog.Level) bool { return true }
func (h *blockedDatabaseHNSWBuild) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *blockedDatabaseHNSWBuild) WithGroup(string) slog.Handler            { return h }
func (h *blockedDatabaseHNSWBuild) Handle(_ context.Context, record slog.Record) error {
	if strings.Contains(record.Message, "[HNSW] Building from") {
		h.fileBacked.Store(strings.Contains(record.Message, "file store"))
		h.once.Do(func() { close(h.entered) })
		<-h.release
	}
	if strings.Contains(record.Message, "HNSW index created:") {
		h.installed.Add(1)
	}
	return nil
}

func databaseHNSWWorkerCount() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	stacks := string(buf[:n])
	return strings.Count(stacks, "pkg/search.(*Service).startHNSWBuild.func") +
		strings.Count(stacks, "pkg/search.(*Service).ensureHNSWMaintenance.func")
}

func TestDatabaseSearchTeardownJoinsHNSWBuild(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	for _, operation := range []string{"reset", "drop", "close"} {
		for _, fileBacked := range []bool{false, true} {
			name := "memory"
			if fileBacked {
				name = "file"
			}
			t.Run(operation+"/"+name, func(t *testing.T) {
				workersBefore := databaseHNSWWorkerCount()
				cfg := DefaultConfig()
				cfg.Memory.EmbeddingDimensions = 3
				cfg.Database.PersistSearchIndexes = fileBacked
				db, err := Open(t.TempDir(), cfg)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, db.Close()) })
				const dbName = "hnsw-lifecycle"
				svc, err := db.GetOrCreateSearchService(dbName, nil)
				require.NoError(t, err)
				handler := &blockedDatabaseHNSWBuild{entered: make(chan struct{}), release: make(chan struct{})}
				svc.SetLogger(slog.New(handler))
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(handler.release) }) }
				t.Cleanup(func() { release(); require.NoError(t, svc.Close()) })
				node := &storage.Node{ID: "before", ChunkEmbeddings: [][]float32{{1, 0, 0}}}
				require.NoError(t, svc.IndexNode(node))
				if fileBacked {
					svc.PersistIndexesToDisk()
				}
				_, err = svc.VectorSearchCandidates(context.Background(), []float32{1, 0, 0}, nil)
				require.NoError(t, err)
				select {
				case <-handler.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("HNSW construction did not start")
				}
				require.Equal(t, fileBacked, handler.fileBacked.Load(), "exercise the intended vector store")
				teardownDone := make(chan error, 1)
				go func() {
					switch operation {
					case "reset":
						db.ResetSearchService(dbName)
					case "drop":
						db.DropSearchServiceState(dbName)
					case "close":
						teardownDone <- db.Close()
						return
					}
					teardownDone <- nil
				}()
				// Observe cancellation through the public search API while the
				// worker is still blocked. Returning from teardown now would leak it.
				require.Eventually(t, func() bool {
					_, err := svc.VectorSearchCandidates(context.Background(), []float32{1, 0, 0}, nil)
					return errors.Is(err, context.Canceled)
				}, 2*time.Second, time.Millisecond, "discarded service was not cancelled")
				select {
				case <-teardownDone:
					t.Fatal("database teardown returned before joining construction")
				default:
				}
				// Keep late preflight calls active across worker release and
				// file-store retirement; they belong to callers, not the joined job.
				stopLate := make(chan struct{})
				var late sync.WaitGroup
				lateReady := make(chan struct{}, 8)
				for i := 0; i < cap(lateReady); i++ {
					late.Add(1)
					go func() {
						defer late.Done()
						lateReady <- struct{}{}
						for {
							select {
							case <-stopLate:
								return
							default:
							}
							_, _ = svc.VectorSearchCandidates(context.Background(), []float32{1, 0, 0}, nil)
						}
					}()
				}
				for i := 0; i < cap(lateReady); i++ {
					<-lateReady
				}
				defer func() { close(stopLate); late.Wait() }()
				release()
				select {
				case err := <-teardownDone:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Fatal("database teardown did not join construction")
				}
				require.Zero(t, handler.installed.Load(), "discarded build published a graph")
				if operation != "close" {
					replacement, err := db.GetOrCreateSearchService(dbName, nil)
					require.NoError(t, err)
					require.NotSame(t, svc, replacement)
					require.Zero(t, replacement.EmbeddingCount())
				}
				require.NoError(t, db.Close())
				require.Eventually(t, func() bool { return databaseHNSWWorkerCount() <= workersBefore },
					5*time.Second, time.Millisecond, "database teardown leaked construction or maintenance workers")
			})
		}
	}
}

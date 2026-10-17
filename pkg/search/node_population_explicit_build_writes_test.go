package search

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Writes during an explicit build (#1372's buildMu model, decided 17 Oct):
// a node write or removal waits for the whole build, ANN included; a build
// that is cancelled under it turns it into an unpopulated write (#1413); and
// Close releases it only once the build has observed cancellation, after
// which it is refused.

// messageGate parks the first log record containing text until released.
type messageGate struct {
	text             string
	entered, release chan struct{}
	once             sync.Once
}

func newMessageGate(text string) *messageGate {
	return &messageGate{text: text, entered: make(chan struct{}), release: make(chan struct{})}
}

func (h *messageGate) Enabled(context.Context, slog.Level) bool { return true }
func (h *messageGate) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *messageGate) WithGroup(string) slog.Handler            { return h }
func (h *messageGate) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, h.text) {
		h.once.Do(func() { close(h.entered); <-h.release })
	}
	return nil
}

// waitsOnBuildMu reports whether goroutine id is parked on buildMu in fn.
func waitsOnBuildMu(id, fn string) bool {
	return serviceStackContains(id, "(*Service)."+fn, "sync.(*RWMutex).RLock")
}

func TestNodePopulationWritesWaitForWholeExplicitBuild(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			engine := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			a := admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named")
			_, err := engine.CreateNode(a)
			require.NoError(t, err)
			svc := configureNodeAdmissionService(t, engine, t.TempDir(), backend, nil)
			graphStarted, graphRelease := make(chan struct{}), make(chan struct{})
			var graphOnce sync.Once
			releaseGraph := func() { graphOnce.Do(func() { close(graphRelease) }) }
			defer releaseGraph()
			hnswBeforeInstallHook = func(ctx context.Context) {
				close(graphStarted)
				select {
				case <-graphRelease:
				case <-ctx.Done():
				}
			}
			defer func() { hnswBeforeInstallHook = nil }()
			buildDone := make(chan error, 1)
			go func() { buildDone <- svc.BuildIndexes(context.Background()) }()
			select {
			case <-graphStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("build did not reach its ANN install")
			}

			b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
			_, err = engine.CreateNode(b)
			require.NoError(t, err)
			require.NoError(t, engine.DeleteNode(a.ID))
			writeDone, removeDone := make(chan error, 1), make(chan error, 1)
			writerID, removerID := make(chan string, 1), make(chan string, 1)
			go func() { writerID <- serviceTestGoroutineID(); writeDone <- svc.IndexNode(b) }()
			go func() { removerID <- serviceTestGoroutineID(); removeDone <- svc.RemoveNode(a.ID) }()
			writer, remover := <-writerID, <-removerID
			require.Eventually(t, func() bool { return waitsOnBuildMu(writer, "IndexNode") }, 5*time.Second, time.Millisecond,
				"a node write during an explicit build waits for the build")
			require.Eventually(t, func() bool { return waitsOnBuildMu(remover, "RemoveNode") }, 5*time.Second, time.Millisecond,
				"a node removal during an explicit build waits for the build")
			for name, done := range map[string]chan error{"write": writeDone, "removal": removeDone} {
				select {
				case err := <-done:
					t.Fatalf("%s finished during the build's ANN phase: %v", name, err)
				default:
				}
			}

			releaseGraph()
			require.NoError(t, <-buildDone)
			for name, done := range map[string]chan error{"write": writeDone, "removal": removeDone} {
				select {
				case err := <-done:
					require.NoError(t, err, name)
				case <-time.After(5 * time.Second):
					t.Fatalf("%s did not finish after the build", name)
				}
			}
			resp, err := svc.Search(context.Background(), "bterm", nil, &SearchOptions{Limit: 10})
			require.NoError(t, err)
			require.Len(t, resp.Results, 1)
			assert.Equal(t, "nornic:b", resp.Results[0].ID)
			resp, err = svc.Search(context.Background(), "aterm", nil, &SearchOptions{Limit: 10})
			require.NoError(t, err)
			assert.Empty(t, resp.Results)
			graph, err := svc.getOrCreateHNSWIndex(context.Background(), 3)
			require.NoError(t, err)
			hits, err := graph.Search(context.Background(), []float32{0, 1, 0}, 10, -1)
			require.NoError(t, err)
			require.Len(t, hits, 1)
			assert.Equal(t, "nornic:b-named-v", hits[0].ID)
		})
	}
}

func TestNodePopulationCancelledBuildLeavesWaitingWriteUnpopulated(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			engine := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			dir := t.TempDir()
			a := admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named")
			_, err := engine.CreateNode(a)
			require.NoError(t, err)
			// Saved images that do not contain b.
			seed := configureNodeAdmissionService(t, engine, dir, backend, nil)
			require.NoError(t, seed.BuildIndexes(ctx))
			require.NoError(t, seed.Close())
			// Without its BM25 checkpoint the build must iterate storage, while
			// the saved vector and graph images (without b) stay on disk.
			require.NoError(t, os.RemoveAll(filepath.Join(dir, "bm25")))

			live := configureNodeAdmissionService(t, engine, dir, backend, nil)
			gate := newMessageGate("BuildIndexes: building from storage")
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(gate.release) }) }
			defer release()
			live.SetLogger(slog.New(gate))
			buildCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			buildDone := make(chan error, 1)
			go func() { buildDone <- live.BuildIndexes(buildCtx) }()
			select {
			case <-gate.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("explicit build did not start")
			}
			b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
			_, err = engine.CreateNode(b)
			require.NoError(t, err)
			writeDone, writerID := make(chan error, 1), make(chan string, 1)
			go func() { writerID <- serviceTestGoroutineID(); writeDone <- live.IndexNode(b) }()
			writer := <-writerID
			require.Eventually(t, func() bool { return waitsOnBuildMu(writer, "IndexNode") }, 5*time.Second, time.Millisecond)

			cancel()
			release()
			require.ErrorIs(t, <-buildDone, context.Canceled)
			select {
			case err := <-writeDone:
				require.NoError(t, err, "the waiting write is applied once the cancelled build returns")
			case <-time.After(5 * time.Second):
				t.Fatal("waiting write did not finish after the cancelled build")
			}
			live.indexMu.RLock()
			unpopulated := live.nodeSources.unpopulatedLiveWrites
			live.indexMu.RUnlock()
			assert.True(t, unpopulated, "the write is recorded as unpopulated")

			// The next population covers it; no saved image (which lacks b) is
			// restored over it, and it is applied once.
			live.SetLogger(slog.New(slog.DiscardHandler))
			require.NoError(t, live.BuildIndexes(ctx))
			checkOnce := func(svc *Service) {
				t.Helper()
				for term, id := range map[string]string{"aterm": "nornic:a", "bterm": "nornic:b"} {
					resp, err := svc.Search(ctx, term, nil, &SearchOptions{Limit: 10})
					require.NoError(t, err)
					if assert.Len(t, resp.Results, 1, term) {
						assert.Equal(t, id, resp.Results[0].ID)
					}
				}
				svc.indexMu.RLock()
				named := make(map[string]string, len(svc.nodeNamedVector["nornic:b"]))
				for name, id := range svc.nodeNamedVector["nornic:b"] {
					named[name] = id
				}
				svc.indexMu.RUnlock()
				assert.Equal(t, map[string]string{"v": "nornic:b-named-v"}, named)
				hits, err := svc.VectorQueryNodes(ctx, []float32{0, 1, 0}, VectorQuerySpec{Label: "Doc", Property: "v", Similarity: "dot", Limit: 10})
				require.NoError(t, err)
				matches := 0
				for _, hit := range hits {
					if hit.ID == "nornic:b" {
						matches++
					}
				}
				assert.Equal(t, 1, matches, "b is indexed once")
			}
			checkOnce(live)
			live.PersistIndexesToDisk()
			require.NoError(t, live.Close())
			restored := configureNodeAdmissionService(t, engine, dir, backend, nil)
			require.NoError(t, restored.BuildIndexes(ctx))
			checkOnce(restored)
		})
	}
}

func TestNodePopulationCloseRefusesWriteWaitingOnBuild(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			engine := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			_, err := engine.CreateNode(admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named"))
			require.NoError(t, err)
			svc := configureNodeAdmissionService(t, engine, t.TempDir(), backend, nil)
			gate := newMessageGate("base index iteration complete")
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(gate.release) }) }
			defer release()
			svc.SetLogger(slog.New(gate))
			buildDone := make(chan error, 1)
			go func() { buildDone <- svc.BuildIndexes(context.Background()) }()
			select {
			case <-gate.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("explicit build did not reach its node phase")
			}
			writeDone, writerID := make(chan error, 1), make(chan string, 1)
			go func() {
				writerID <- serviceTestGoroutineID()
				writeDone <- svc.IndexNode(admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named"))
			}()
			writer := <-writerID
			require.Eventually(t, func() bool { return waitsOnBuildMu(writer, "IndexNode") }, 5*time.Second, time.Millisecond)

			closed := make(chan error, 1)
			go func() { closed <- svc.Close() }()
			// The gate ignores cancellation, so the writer stays parked until the
			// build returns; a real build observes Close's cancellation (see
			// TestBuildIndexesObservesCancellationInEveryPhase).
			release()
			require.Error(t, <-buildDone)
			select {
			case err := <-writeDone:
				require.True(t, errors.Is(err, context.Canceled), "the released write is refused: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not release the waiting write after the build returned")
			}
			require.NoError(t, <-closed)
			assert.False(t, svc.IsReady())
			assert.Empty(t, svc.fulltext().Search("bterm", 10))
			svc.indexMu.RLock()
			_, applied := svc.nodeNamedVector["nornic:b"]
			svc.indexMu.RUnlock()
			assert.False(t, applied, "nothing is applied to the closed indexes")
			svc.persistMu.Lock()
			timer := svc.persistTimer
			svc.persistMu.Unlock()
			assert.Nil(t, timer)
		})
	}
}

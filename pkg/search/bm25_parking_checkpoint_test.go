package search

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// recordingLog keeps every log message and passes each one to onMessage.
type recordingLog struct {
	mu        sync.Mutex
	messages  []string
	onMessage func(message string)
}

func (h *recordingLog) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingLog) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *recordingLog) WithGroup(string) slog.Handler            { return h }
func (h *recordingLog) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.messages = append(h.messages, r.Message)
	h.mu.Unlock()
	if h.onMessage != nil {
		h.onMessage(r.Message)
	}
	return nil
}

func (h *recordingLog) saw(message string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range h.messages {
		if strings.Contains(m, message) {
			return true
		}
	}
	return false
}

const bm25ParkedMessage = "cleared BM25 in-memory state (will reload after warmup)"

// BM25 is parked through the ANN warmup only when the base persist wrote its
// checkpoint. A BM25 index the build rebuilt from storage is written even when
// it is empty and never changed, so the first build of an empty persisted
// service writes, parks and reloads its empty checkpoint; a dirty empty index
// and a populated one are written and parked as before. A save that fails or
// is skipped never parks (TestParkedBM25NotParkedWhenTheBasePersistFails).
func TestBM25ParkedOnlyWhenTheBaseCheckpointWasWritten(t *testing.T) {
	for _, backend := range []string{"v1", "v2"} {
		for _, kind := range []string{"empty_never_written", "empty_dirty", "populated"} {
			t.Run(backend+"/"+kind, func(t *testing.T) {
				engine := storage.NewMemoryEngine()
				t.Cleanup(func() { require.NoError(t, engine.Close()) })
				if kind == "populated" {
					_, err := engine.CreateNode(admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named"))
					require.NoError(t, err)
				}
				dir := t.TempDir()
				svc := configureNodeAdmissionService(t, engine, dir, backend, nil)
				logs := &recordingLog{}
				svc.SetLogger(slog.New(logs))
				if kind == "empty_dirty" {
					svc.fulltext().Index("transient", "gone")
					svc.fulltext().Remove("transient")
					require.True(t, svc.fulltext().IsDirty())
					require.Zero(t, svc.fulltext().Count())
				}
				require.NoError(t, svc.BuildIndexes(context.Background()))
				require.True(t, svc.IsReady())
				require.True(t, logs.saw(bm25ParkedMessage), "parked after a written checkpoint")
				require.FileExists(t, filepath.Join(dir, "bm25"))
				if kind == "populated" {
					require.Equal(t, []string{"nornic:a"}, parkingSearchIDs(t, svc, "aterm"))
				}

				// The first live write after the build is served, and a restart
				// restores what was persisted.
				b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
				_, err := engine.CreateNode(b)
				require.NoError(t, err)
				require.NoError(t, svc.IndexNode(b))
				require.Equal(t, []string{"nornic:b"}, parkingSearchIDs(t, svc, "bterm"))
				svc.PersistIndexesToDisk()
				require.NoError(t, svc.Close())
				restarted := configureNodeAdmissionService(t, engine, dir, backend, nil)
				require.NoError(t, restarted.BuildIndexes(context.Background()))
				require.True(t, restarted.IsReady())
				require.Equal(t, []string{"nornic:b"}, parkingSearchIDs(t, restarted, "bterm"))
			})
		}
	}
}

func parkingSearchIDs(t *testing.T, svc *Service, term string) []string {
	t.Helper()
	resp, err := svc.Search(context.Background(), term, nil, &SearchOptions{Limit: 10})
	require.NoError(t, err)
	ids := make([]string, 0, len(resp.Results))
	for _, result := range resp.Results {
		ids = append(ids, result.ID)
	}
	return ids
}

// BM25 is parked at the checkpoint the base persist wrote. A fulltext path
// changed after the build started is where that persist saves, so the warmup
// reloads from it rather than from the path the build started with.
func TestBM25ParkedAtTheCheckpointThePersistWrote(t *testing.T) {
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			engine := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			_, err := engine.CreateNode(admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named"))
			require.NoError(t, err)
			dir := t.TempDir()
			moved := filepath.Join(dir, "bm25-moved")
			svc := configureNodeAdmissionService(t, engine, dir, backend, nil)
			var move sync.Once
			logs := &recordingLog{onMessage: func(message string) {
				if strings.Contains(message, "base index iteration complete") {
					move.Do(func() { svc.SetFulltextIndexPath(moved) })
				}
			}}
			svc.SetLogger(slog.New(logs))
			require.NoError(t, svc.BuildIndexes(context.Background()))
			require.True(t, svc.IsReady())
			require.True(t, logs.saw(bm25ParkedMessage), "parked at the written checkpoint")
			require.FileExists(t, moved)
			require.NoFileExists(t, filepath.Join(dir, "bm25"))
			require.Equal(t, []string{"nornic:a"}, parkingSearchIDs(t, svc, "aterm"))
		})
	}
}

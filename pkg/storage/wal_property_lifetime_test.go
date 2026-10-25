package storage

import (
	"encoding/json"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

type walReplayLifetimeResult struct {
	err        error
	panicValue any
}

func runWALReplayLifetimeProbe(replay func() error, result chan<- walReplayLifetimeResult) {
	r := walReplayLifetimeResult{}
	defer func() {
		r.panicValue = recover()
		result <- r
	}()
	r.err = replay()
}

func walReplayLifetimeFixture(t *testing.T, mode string) (*BadgerEngine, func() error, string) {
	t.Helper()
	_, data := r2BindingData(t, 1)
	dir := t.TempDir()
	b := reviewBindingEngine(t, dir)
	_, err := b.CreateNode(&Node{ID: "perf:sentinel", Properties: map[string]any{"other": int64(0)}})
	require.NoError(t, err)
	if mode == "importer" {
		return b, func() error { _, err := b.importBufferedWALRecord(&data, walForeignImport); return err }, dir
	}
	if mode == "batch" {
		t.Cleanup(config.WithWALEnabled())
		w, err := NewWAL(t.TempDir(), DefaultWALConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = w.Close() })
		require.NoError(t, w.Append(OpBufferedCommit, &data))
		entries, err := ReadWALEntriesFromDir(w.Config().Dir)
		require.NoError(t, err)
		require.Len(t, entries, 1, "the batch replay must reach the source owner")
		return b, func() error { _, err := RecoverBufferedCommits(b, w); return err }, dir
	}
	entry := WALEntry{Database: "perf", Operation: OpBufferedCommit}
	entry.Data, err = json.Marshal(data)
	if mode == "named" {
		entry.Database, entry.Operation = "shutdown", OpCreateNode
		entry.Data, err = json.Marshal(WALNodeData{Node: &Node{ID: "shutdown:replay", Properties: map[string]any{"value": int64(1)}}})
	}
	require.NoError(t, err)
	return b, func() error { return ReplayWALEntry(b, entry) }, dir
}

func TestReviewR73ReplayLifetime(t *testing.T) {
	for _, mode := range []string{"named", "buffered", "importer", "batch"} {
		t.Run(mode, func(t *testing.T) {
			for _, schedule := range []string{"after_close", "first_replay_close_interleaving"} {
				t.Run(schedule, func(t *testing.T) {
					b, replay, dir := walReplayLifetimeFixture(t, mode)
					result := make(chan walReplayLifetimeResult, 1)
					if schedule == "after_close" {
						require.NoError(t, b.Close())
						runWALReplayLifetimeProbe(replay, result)
						r := <-result
						require.Nil(t, r.panicValue)
						require.ErrorIs(t, r.err, ErrStorageClosed)
						return
					}
					// The real source mutex gates admission immediately before the first
					// provenance write. Observe the blocked probe frame, not a sleep or
					// a production test hook, to establish that it reached this gate.
					b.walPropertySource.Lock()
					locked := true
					defer func() {
						if locked {
							b.walPropertySource.Unlock()
						}
					}()
					go runWALReplayLifetimeProbe(replay, result)
					require.Eventually(t, func() bool {
						stack := make([]byte, 1<<20)
						n := runtime.Stack(stack, true)
						for _, frame := range strings.Split(string(stack[:n]), "\n\n") {
							if strings.Contains(frame, "runWALReplayLifetimeProbe") &&
								strings.Contains(frame, "[sync.Mutex.Lock]") &&
								(strings.Contains(frame, "wal_property_source.go") || strings.Contains(frame, "wal_property_bindings.go")) {
								return true
							}
						}
						return false
					}, 5*time.Second, time.Millisecond)
					closed := make(chan error, 1)
					go func() { closed <- b.Close() }()
					require.Eventually(t, func() bool {
						b.mu.RLock()
						defer b.mu.RUnlock()
						return b.closed
					}, 5*time.Second, time.Millisecond)
					// Close has stopped admission, but cannot clear an admitted handle.
					b.mu.RLock()
					handleRetained := b.db != nil
					b.mu.RUnlock()
					select {
					case err := <-closed:
						t.Errorf("Close returned before the admitted source operation: %v", err)
						closed <- err
					default:
					}
					b.walPropertySource.Unlock()
					locked = false
					var r walReplayLifetimeResult
					select {
					case r = <-result:
					case <-time.After(5 * time.Second):
						t.Fatal("replay did not leave the source gate")
					}
					require.True(t, handleRetained)
					require.Nil(t, r.panicValue)
					if r.err != nil {
						require.ErrorIs(t, r.err, ErrStorageClosed)
					}
					select {
					case err := <-closed:
						require.NoError(t, err)
					case <-time.After(5 * time.Second):
						t.Fatal("Close did not drain the admitted replay")
					}
					// Even if later CRUD sees closed, provenance must have become
					// durable before the admitted operation released its handle.
					raw, err := fixtureowner.OwnResult[*badger.DB](t, badger.ErrDBClosed)(badger.Open(badger.DefaultOptions(dir).WithReadOnly(true).WithLogger(nil)))
					require.NoError(t, err)
					require.NoError(t, raw.View(func(tx *badger.Txn) error { _, err := tx.Get(walPropertyForeignKey); return err }))
					require.NoError(t, raw.Close())
				})
			}
		})
	}
}

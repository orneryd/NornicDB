package search

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type baseCheckpointBuildLog struct {
	entered, release chan struct{}
	once             sync.Once
}

func (h *baseCheckpointBuildLog) Enabled(context.Context, slog.Level) bool { return true }
func (h *baseCheckpointBuildLog) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *baseCheckpointBuildLog) WithGroup(string) slog.Handler            { return h }
func (h *baseCheckpointBuildLog) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "base index iteration complete") {
		h.once.Do(func() { close(h.entered); <-h.release })
	}
	return nil
}

func TestBaseBuildPersistenceDisablePreservesCurrentFulltext(t *testing.T) {
	for _, backend := range []string{"v1", "v2"} {
		for _, traced := range []bool{false, true} {
			for _, kind := range []string{"missing", "equal_count_old", "transfer_refused", "disabled_before_entry", "enabled"} {
				t.Run(fmt.Sprintf("%s/traced=%t/%s", backend, traced, kind), func(t *testing.T) {
					ctx := context.Background()
					engine := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
					t.Cleanup(func() { require.NoError(t, engine.Close()) })
					_, err := engine.CreateNode(&storage.Node{ID: "nornic:a", NamedEmbeddings: map[string][]float32{"v": {1, 0, 0}}, Properties: map[string]any{"old": "oldterm", "next": "newterm"}})
					require.NoError(t, err)
					var source storage.Engine = engine
					if traced {
						source = storage.NewTracedEngine(engine)
					}
					_, iterator := source.(NodeIterator)
					require.Equal(t, !traced, iterator)
					dir := t.TempDir()
					configure := func(svc *Service, property string) {
						svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
						svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
						svc.SetFulltextProperties([]string{property})
						svc.SetPersistenceEnabled(true)
					}
					if kind == "equal_count_old" {
						first := NewServiceWithDimensionsAndBM25EngineAndOptions(source, 3, backend, nil)
						t.Cleanup(func() { require.NoError(t, first.Close()) })
						configure(first, "old")
						require.NoError(t, first.BuildIndexes(ctx))
						require.Equal(t, 1, first.fulltext().Count())
						require.NoError(t, first.Close())
					}
					options := &ServiceOptions{}
					if kind == "transfer_refused" {
						options.VectorMemoryMaxBytes = 8
					}
					svc := NewServiceWithDimensionsAndBM25EngineAndOptions(source, 3, backend, options)
					t.Cleanup(func() { require.NoError(t, svc.Close()) })
					configure(svc, "next")
					if kind == "disabled_before_entry" {
						svc.SetPersistenceEnabled(false)
					}
					if kind == "missing" || kind == "equal_count_old" || kind == "transfer_refused" {
						h := &baseCheckpointBuildLog{entered: make(chan struct{}), release: make(chan struct{})}
						var once sync.Once
						release := func() { once.Do(func() { close(h.release) }) }
						defer release()
						svc.SetLogger(slog.New(h))
						buildDone := make(chan error, 1)
						go func() { buildDone <- svc.BuildIndexes(ctx) }()
						select {
						case <-h.entered:
						case <-time.After(5 * time.Second):
							t.Fatal("build did not reach the post-path, post-ingestion gate")
						}
						disabled := make(chan struct{})
						go func() { svc.SetPersistenceEnabled(false); close(disabled) }()
						require.Eventually(t, func() bool {
							svc.baseBuildControlMu.Lock()
							defer svc.baseBuildControlMu.Unlock()
							return svc.baseBuildPaused > 0
						}, 5*time.Second, time.Millisecond, "disable must join the admitted build before releasing its source")
						assert.True(t, svc.persistEnabled.Load(), "admitted checkpoint policy must remain enabled until the build joins")
						select {
						case <-disabled:
							t.Fatal("disable returned before the gated build")
						default:
						}
						release()
						select {
						case err := <-buildDone:
							assert.NoError(t, err)
						case <-time.After(5 * time.Second):
							t.Fatal("build did not finish")
						}
						select {
						case <-disabled:
						case <-time.After(5 * time.Second):
							t.Fatal("disable did not finish after joining the build")
						}
					} else {
						require.NoError(t, svc.BuildIndexes(ctx))
					}
					assert.True(t, svc.IsReady())
					assert.Equal(t, 1, svc.fulltext().Count())
					current, err := svc.Search(ctx, "newterm", nil, &SearchOptions{Limit: 5})
					require.NoError(t, err)
					assert.Len(t, current.Results, 1, "configured current projection must survive the toggle")
					old, err := svc.Search(ctx, "oldterm", nil, &SearchOptions{Limit: 5})
					require.NoError(t, err)
					assert.Empty(t, old.Results, "equal-count stale artifacts must not replace current content")
					assert.Equal(t, 1, svc.EmbeddingCount())
					if kind == "transfer_refused" {
						assert.True(t, svc.persistEnabled.Load())
						require.NotNil(t, svc.vectorFileStore)
						assert.True(t, svc.vectorFileStore.Has("nornic:a-named-v"))
					} else if kind != "enabled" {
						assert.False(t, svc.persistEnabled.Load())
						assert.Nil(t, svc.vectorFileStore)
						vector, ok := svc.vectorIndex.GetVector("nornic:a-named-v")
						assert.True(t, ok)
						assert.Equal(t, []float32{1, 0, 0}, vector)
					}
				})
			}
		}
	}
}

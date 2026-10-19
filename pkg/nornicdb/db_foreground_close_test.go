package nornicdb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/stretchr/testify/require"
)

// An active call owns native model state until it returns. Close must not
// release that state while a foreground query is still using it.
type solReviewForegroundEmbedder struct {
	mockEmbedder
	entered, proceed, closeCalled chan struct{}
	active, unsafeCloses          atomic.Int32
	once                          sync.Once
}

func (e *solReviewForegroundEmbedder) Embed(ctx context.Context, _ string) ([]float32, error) {
	e.active.Add(1)
	defer e.active.Add(-1)
	close(e.entered)
	select {
	case <-e.proceed:
		return []float32{1, 0, 0, 0}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (e *solReviewForegroundEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	vectors := make([][]float32, len(texts))
	for i, text := range texts {
		vector, err := e.Embed(ctx, text)
		if err != nil {
			return nil, err
		}
		vectors[i] = vector
	}
	return vectors, nil
}
func (e *solReviewForegroundEmbedder) Close() error {
	if e.active.Load() != 0 {
		e.unsafeCloses.Add(1)
	}
	e.once.Do(func() { close(e.closeCalled) })
	return nil
}

func TestSolReviewCloseQuiescesForegroundEmbeddings(t *testing.T) {
	for _, mode := range []string{"global", "snapshot", "chunks", "per_database"} {
		for _, cacheSize := range []int{0, 16} {
			t.Run(fmt.Sprintf("%s/cache%d", mode, cacheSize), func(t *testing.T) {
				db, err := Open("", nil)
				require.NoError(t, err)
				e := &solReviewForegroundEmbedder{mockEmbedder: mockEmbedder{dims: 4, model: "foreground"}, entered: make(chan struct{}), proceed: make(chan struct{}), closeCalled: make(chan struct{})}
				var release sync.Once
				allowQuery := func() { release.Do(func() { close(e.proceed) }) }
				defer allowQuery()
				defer db.Close()
				var provider embed.Embedder = e
				if cacheSize > 0 {
					provider = embed.NewCachedEmbedder(provider, cacheSize)
				}
				require.True(t, db.SetEmbedder(provider))
				if mode == "per_database" {
					db.embedderFactory = func(context.Context, *embed.Config) (embed.Embedder, error) { return provider, nil }
					db.SetEmbedConfigForDB(func(string) (*embed.Config, error) {
						return &embed.Config{Provider: "local", Model: "foreground", Dimensions: 4}, nil
					})
				}
				queryDone := make(chan error, 1)
				go func() {
					var queryErr error
					switch mode {
					case "snapshot":
						_, queryErr = db.QueryEmbeddingForDB("nornic").EmbedQuery(context.Background(), "held query")
					case "chunks":
						_, _, queryErr = db.EmbedQueryChunks(context.Background(), "held query")
					case "per_database":
						_, queryErr = db.EmbedQueryForDB(context.Background(), "other", "held query")
					default:
						_, queryErr = db.EmbedQuery(context.Background(), "held query")
					}
					queryDone <- queryErr
				}()
				select {
				case <-e.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("query did not enter provider")
				}
				closeDone := make(chan error, 1)
				go func() { closeDone <- db.Close() }()
				select {
				case <-db.LifetimeContext().Done():
				case <-time.After(5 * time.Second):
					t.Fatal("Close did not cancel DB lifetime")
				}
				// A correct implementation may wait for this caller or cancel it.
				// Current code invokes Close immediately, with active == 1.
				closedEarly := false
				var closeErr error
				select {
				case <-e.closeCalled:
				case closeErr = <-closeDone:
					closedEarly = true
				case <-time.After(5 * time.Second):
					// Permit a fixed implementation to wait for the query.
				}
				allowQuery()
				select {
				case <-queryDone:
				case <-time.After(5 * time.Second):
					t.Fatal("query deadlocked during shutdown")
				}
				if closedEarly {
					require.NoError(t, closeErr)
				} else {
					select {
					case err := <-closeDone:
						require.NoError(t, err)
					case <-time.After(5 * time.Second):
						t.Fatal("DB.Close deadlocked")
					}
				}
				require.Zero(t, e.unsafeCloses.Load(), "provider released while a foreground embedding was in flight")
			})
		}
	}
}

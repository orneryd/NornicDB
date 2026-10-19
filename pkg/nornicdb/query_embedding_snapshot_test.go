package nornicdb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func queryEmbeddingTestExecutor(t *testing.T, provider cypher.QueryEmbedder) *cypher.StorageExecutor {
	t.Helper()
	store := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = store.Close() })
	executor := cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "snapshot"))
	executor.SetEmbedder(provider)
	return executor
}

// Deliberately implements only the shared query interface, without the document
// worker's batch/model/backend methods. Protocol adapters may publish these too.
type snapshotQueryProvider struct {
	name   string
	vector []float32
	err    error
}

func (p *snapshotQueryProvider) Embed(_ context.Context, text string) ([]float32, error) {
	if text != p.name {
		return nil, fmt.Errorf("%s cannot embed chunk from %s", p.name, text)
	}
	return append([]float32(nil), p.vector...), p.err
}

func (p *snapshotQueryProvider) ChunkText(string, int, int) ([]string, error) {
	return []string{p.name, p.name}, nil
}

func TestQueryEmbeddingSnapshotKeepsStartupAbsence(t *testing.T) {
	db := &DB{cypherExecutor: queryEmbeddingTestExecutor(t, nil)}
	before := db.QueryEmbeddingForDB("tenant")
	ready := &snapshotQueryProvider{name: "ready", vector: []float32{1, 0}}
	db.cypherExecutor.SetEmbedder(ready)
	chunks, err := before.ChunkQuery(context.Background(), "query")
	require.NoError(t, err)
	require.Equal(t, []string{"query"}, chunks)
	vec, err := before.EmbedQuery(context.Background(), "query")
	require.NoError(t, err)
	require.Nil(t, vec)
	chunks, vectors, err := db.QueryEmbeddingForDB("tenant").EmbedQueryChunks(context.Background(), "query")
	require.NoError(t, err)
	require.Equal(t, []string{"ready", "ready"}, chunks)
	require.Equal(t, [][]float32{{1, 0}, {1, 0}}, vectors)
	ready.err = errors.New("embedding failed")
	_, _, err = db.EmbedQueryChunks(context.Background(), "query")
	require.ErrorContains(t, err, "embedding failed")
}

func TestQueryEmbeddingRequestContextKeepsProvider(t *testing.T) {
	db := &DB{cypherExecutor: queryEmbeddingTestExecutor(t, nil)}
	ctx := db.WithQueryEmbeddingSnapshot(context.Background())
	ready := &snapshotQueryProvider{name: "ready", vector: []float32{1, 0}}
	db.cypherExecutor.SetEmbedder(ready)
	// Nested calls and timeout/cancellation wrappers must preserve captured nil.
	nested, cancel := context.WithCancel(ctx)
	defer cancel()
	require.Same(t, nested, db.WithQueryEmbeddingSnapshot(nested))
	vec, err := db.EmbedQuery(nested, "query")
	require.NoError(t, err)
	require.Nil(t, vec)
	chunks, err := db.ChunkQuery(nested, "query")
	require.NoError(t, err)
	require.Equal(t, []string{"query"}, chunks)
	_, vectors, err := db.EmbedQueryChunks(nested, "query")
	require.NoError(t, err)
	require.Nil(t, vectors)

	ctx = db.WithQueryEmbeddingSnapshot(context.Background())
	replacement := &snapshotQueryProvider{name: "replacement", vector: []float32{0, 1}}
	db.cypherExecutor.SetEmbedder(replacement)
	chunks, vectors, err = db.EmbedQueryChunks(db.WithQueryEmbeddingSnapshot(ctx), "query")
	require.NoError(t, err)
	require.Equal(t, []string{"ready", "ready"}, chunks)
	require.Equal(t, [][]float32{{1, 0}, {1, 0}}, vectors)
	vec, err = db.EmbedQuery(context.Background(), "query")
	require.NoError(t, err)
	require.Equal(t, replacement.vector, vec)
	// A different DB cannot consume this DB's request binding.
	other := &DB{cypherExecutor: queryEmbeddingTestExecutor(t, replacement)}
	vec, err = other.EmbedQuery(ctx, "query")
	require.NoError(t, err)
	require.Equal(t, replacement.vector, vec)
	vec, err = other.EmbedQuery(other.WithQueryEmbeddingSnapshot(ctx), "query")
	require.NoError(t, err)
	require.Equal(t, replacement.vector, vec)
}

func TestQueryEmbeddingSnapshotResolvesDatabaseOnce(t *testing.T) {
	for _, mode := range []string{"default", "override", "resolver error", "nil config", "factory error"} {
		t.Run(mode, func(t *testing.T) {
			old := &snapshotQueryProvider{name: "old", vector: []float32{1, 0}}
			replacement := &snapshotQueryProvider{name: "new", vector: []float32{0, 1}}
			db := &DB{cypherExecutor: queryEmbeddingTestExecutor(t, old)}
			db.SetDbConfigResolver(func(string) (int, float64, string) { return 3, 0, "" })
			cfg := &embed.Config{Provider: "ollama", Model: "database-model", Dimensions: 2}
			key := embedConfigKey(cfg)
			special := &coverageQueryEmbedder{chunks: []string{"special"}, embed: []float32{0.5, 0.5}}
			db.embedderRegistry = map[string]embed.Embedder{key: special}
			if mode == "default" {
				db.defaultEmbedKey = key
			}
			if mode == "factory error" {
				db.embedderRegistry = nil
				db.embedderFactory = func(context.Context, *embed.Config) (embed.Embedder, error) { return nil, errors.New("unavailable") }
			}
			var resolutions atomic.Int64
			db.SetEmbedConfigForDB(func(name string) (*embed.Config, error) {
				require.Equal(t, "tenant", name)
				resolutions.Add(1)
				// Even publication during config resolution cannot replace the
				// global fallback captured at this request's start.
				db.cypherExecutor.SetEmbedder(replacement)
				if mode == "resolver error" {
					return nil, errors.New("unavailable")
				}
				if mode == "nil config" {
					return nil, nil
				}
				return cfg, nil
			})
			snapshot := db.QueryEmbeddingForDB("tenant")
			chunks, err := snapshot.ChunkQuery(context.Background(), "query")
			require.NoError(t, err)
			want := old.vector
			if mode == "override" {
				require.Equal(t, []string{"special"}, chunks)
				want = special.embed
			} else {
				require.Equal(t, []string{"old", "old"}, chunks)
			}
			for _, chunk := range chunks {
				vec, err := snapshot.EmbedQuery(context.Background(), chunk)
				require.NoError(t, err)
				require.Equal(t, want, vec)
			}
			// Preserve the existing registry-backed single-vector policy;
			// the multi-vector API still checks every index dimension.
			_, _, err = snapshot.EmbedQueryChunks(context.Background(), "query")
			require.ErrorIs(t, err, ErrQueryEmbeddingDimensionMismatch)
			require.Equal(t, int64(1), resolutions.Load())
		})
	}
}

func TestQueryEmbeddingSnapshotConcurrentPublication(t *testing.T) {
	providers := []*snapshotQueryProvider{{name: "a", vector: []float32{1, 0}}, {name: "b", vector: []float32{0, 1}}}
	db := &DB{cypherExecutor: queryEmbeddingTestExecutor(t, providers[0])}
	var wg sync.WaitGroup
	errors := make(chan error, 4)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			db.cypherExecutor.SetEmbedder(providers[i%2])
		}
	}()
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				snapshot := db.QueryEmbeddingForDB("tenant")
				chunks, err := snapshot.ChunkQuery(context.Background(), "query")
				if err != nil {
					errors <- err
					return
				}
				for _, chunk := range chunks {
					if _, err := snapshot.EmbedQuery(context.Background(), chunk); err != nil {
						errors <- err
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
}

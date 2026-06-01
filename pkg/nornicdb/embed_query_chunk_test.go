package nornicdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/stretchr/testify/require"
)

type chunkingTestEmbedder struct {
	mu sync.Mutex

	dims int

	embedCalls     int
	embedBatchCall int
	maxTextLen     int
	maxTokens      int
}

func (e *chunkingTestEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	e.mu.Lock()
	e.embedCalls++
	if len(text) > e.maxTextLen {
		e.maxTextLen = len(text)
	}
	if tok := mustCountTestTokens(text); tok > e.maxTokens {
		e.maxTokens = tok
	}
	dims := e.dims
	e.mu.Unlock()

	// Simulate a tokenizer limit.
	if mustCountTestTokens(text) > 512 {
		return nil, fmt.Errorf("simulated tokenizer overflow for tokens=%d", mustCountTestTokens(text))
	}
	if dims <= 0 {
		dims = 4
	}
	vec := make([]float32, dims)
	vec[0] = 1
	return vec, nil
}

func (e *chunkingTestEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	e.mu.Lock()
	e.embedBatchCall++
	for _, t := range texts {
		if len(t) > e.maxTextLen {
			e.maxTextLen = len(t)
		}
		if tok := mustCountTestTokens(t); tok > e.maxTokens {
			e.maxTokens = tok
		}
	}
	dims := e.dims
	e.mu.Unlock()

	if dims <= 0 {
		dims = 4
	}
	out := make([][]float32, len(texts))
	for i, t := range texts {
		if mustCountTestTokens(t) > 512 {
			return nil, fmt.Errorf("chunk too long: tokens=%d", mustCountTestTokens(t))
		}
		vec := make([]float32, dims)
		vec[0] = 1
		out[i] = vec
	}
	return out, nil
}

func (e *chunkingTestEmbedder) Dimensions() int { return e.dims }
func (e *chunkingTestEmbedder) Model() string   { return "chunking-test-embedder" }
func (e *chunkingTestEmbedder) Backend() string { return "cpu" } // Plan 04-05 D-06
func (e *chunkingTestEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return chunkTestText(text, maxTokens, overlap)
}

type scriptedBatchEmbedder struct {
	embedVec  []float32
	batchVecs [][]float32
	batchErr  error
}

func (e *scriptedBatchEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	if e.embedVec == nil {
		return nil, nil
	}
	out := make([]float32, len(e.embedVec))
	copy(out, e.embedVec)
	return out, nil
}

func (e *scriptedBatchEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(e.batchVecs))
	for i := range e.batchVecs {
		if e.batchVecs[i] == nil {
			continue
		}
		out[i] = make([]float32, len(e.batchVecs[i]))
		copy(out[i], e.batchVecs[i])
	}
	return out, e.batchErr
}

func (e *scriptedBatchEmbedder) Dimensions() int { return 0 }
func (e *scriptedBatchEmbedder) Model() string   { return "scripted-batch" }
func (e *scriptedBatchEmbedder) Backend() string { return "cpu" } // Plan 04-05 D-06
func (e *scriptedBatchEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return chunkTestText(text, maxTokens, overlap)
}

func loadLargeDocQuery(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "docs", "features", "gpu-acceleration.md")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	query := string(data)
	require.Greater(t, mustCountTestTokens(query), 512)
	return query
}

func TestDB_EmbedQuery_ShortQuery_UsesEmbed(t *testing.T) {
	emb := &chunkingTestEmbedder{dims: 4}
	db := &DB{embedQueue: &EmbedQueue{embedder: emb}}

	vec, err := db.EmbedQuery(context.Background(), "hello world")
	require.NoError(t, err)
	require.Len(t, vec, 4)

	emb.mu.Lock()
	embedCalls := emb.embedCalls
	batchCalls := emb.embedBatchCall
	emb.mu.Unlock()

	require.Equal(t, 1, embedCalls)
	require.Equal(t, 0, batchCalls)
}

func TestDB_EmbedQuery_LongQuery_UsesFirstChunkForCompatibility(t *testing.T) {
	emb := &chunkingTestEmbedder{dims: 4}
	db := &DB{embedQueue: &EmbedQueue{embedder: emb}}

	longQuery := loadLargeDocQuery(t)
	vec, err := db.EmbedQuery(context.Background(), longQuery)
	require.NoError(t, err)
	require.Len(t, vec, 4)

	emb.mu.Lock()
	embedCalls := emb.embedCalls
	batchCalls := emb.embedBatchCall
	maxLen := emb.maxTextLen
	maxTokens := emb.maxTokens
	emb.mu.Unlock()

	require.Equal(t, 0, embedCalls, "expected long query path to avoid single-text embedding")
	require.Equal(t, 1, batchCalls, "expected long query path to batch-embed chunks once")
	require.Greater(t, maxLen, 0)
	require.LessOrEqual(t, maxTokens, 512, "expected all embedded chunks to be <= 512 tokens")
}

func TestDB_EmbedQueryChunks_LongQuery_ReturnsChunkEmbeddings(t *testing.T) {
	emb := &deterministicChunkEmbedder{
		dims: 4,
		chunks: []string{
			"first chunk",
			"second chunk",
		},
	}
	db := &DB{embedQueue: &EmbedQueue{embedder: emb}}

	chunks, embs, err := db.EmbedQueryChunks(context.Background(), "long query")
	require.NoError(t, err)
	require.Equal(t, []string{"first chunk", "second chunk"}, chunks)
	require.Len(t, embs, len(chunks))
	for _, vec := range embs {
		require.Len(t, vec, 4)
	}

	emb.mu.Lock()
	batchCalls := append([][]string(nil), emb.batchCalls...)
	emb.mu.Unlock()

	require.Len(t, batchCalls, 1, "expected multi-chunk query path to batch-embed chunks once")
	require.Equal(t, chunks, batchCalls[0])
}

func TestDB_EmbedQueryForDB_NoResolver_ReturnsSameAsEmbedQuery(t *testing.T) {
	emb := &chunkingTestEmbedder{dims: 8}
	db := &DB{embedQueue: &EmbedQueue{embedder: emb}}

	vec, err := db.EmbedQueryForDB(context.Background(), "mydb", "hello")
	require.NoError(t, err)
	require.Len(t, vec, 8)
}

func TestDB_EmbedQueryForDB_ResolverMatchesDims_Success(t *testing.T) {
	emb := &chunkingTestEmbedder{dims: 8}
	db := &DB{
		embedQueue: &EmbedQueue{embedder: emb},
		dbConfigResolver: func(dbName string) (embeddingDims int, searchMinSimilarity float64, bm25Engine string) {
			return 8, 0.5, ""
		},
	}

	vec, err := db.EmbedQueryForDB(context.Background(), "mydb", "hello")
	require.NoError(t, err)
	require.Len(t, vec, 8)
}

func TestDB_EmbedQueryForDB_ResolverMismatchDims_ReturnsErrQueryEmbeddingDimensionMismatch(t *testing.T) {
	emb := &chunkingTestEmbedder{dims: 8}
	db := &DB{
		embedQueue: &EmbedQueue{embedder: emb},
		dbConfigResolver: func(dbName string) (embeddingDims int, searchMinSimilarity float64, bm25Engine string) {
			return 768, 0.5, "" // index is 768-d, query will be 8-d from global embedder
		},
	}

	vec, err := db.EmbedQueryForDB(context.Background(), "mydb", "hello")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrQueryEmbeddingDimensionMismatch))
	require.Nil(t, vec)
	require.Contains(t, err.Error(), "index dims 768")
	require.Contains(t, err.Error(), "query dims 8")
}

func TestDB_EmbedQueryWithEmbedder_EdgeBranches(t *testing.T) {
	t.Run("nil embedder returns nil without error", func(t *testing.T) {
		db := &DB{}
		vec, err := db.embedQueryWithEmbedder(context.Background(), nil, "hello")
		require.NoError(t, err)
		require.Nil(t, vec)
	})

	t.Run("batch empty with error returns error", func(t *testing.T) {
		db := &DB{}
		emb := &scriptedBatchEmbedder{
			batchVecs: nil,
			batchErr:  errors.New("batch failed"),
		}
		vec, err := db.embedQueryWithEmbedder(context.Background(), emb, loadLargeDocQuery(t))
		require.Error(t, err)
		require.Contains(t, err.Error(), "batch failed")
		require.Nil(t, vec)
	})

	t.Run("batch empty without error returns nil", func(t *testing.T) {
		db := &DB{}
		emb := &scriptedBatchEmbedder{
			batchVecs: nil,
			batchErr:  nil,
		}
		vec, err := db.embedQueryWithEmbedder(context.Background(), emb, loadLargeDocQuery(t))
		require.NoError(t, err)
		require.Nil(t, vec)
	})

	t.Run("batch returns first valid chunk for single-vector compatibility", func(t *testing.T) {
		db := &DB{}
		emb := &scriptedBatchEmbedder{
			batchVecs: [][]float32{
				{1, 0, 0},
				{},     // empty skipped
				{1, 0}, // dimension mismatch skipped
				{0, 1, 0},
			},
		}
		vec, err := db.embedQueryWithEmbedder(context.Background(), emb, loadLargeDocQuery(t))
		require.NoError(t, err)
		require.Len(t, vec, 3)
		require.InDelta(t, 1.0, vec[0], 0.0001)
		require.InDelta(t, 0.0, vec[1], 0.0001)
		require.InDelta(t, 0.0, vec[2], 0.0001)
	})
}

func TestDB_EmbedQueryForDB_ResolverUnsetDimsOrNoVector(t *testing.T) {
	t.Run("resolver <= 0 dims bypasses mismatch check", func(t *testing.T) {
		emb := &chunkingTestEmbedder{dims: 5}
		db := &DB{
			embedQueue: &EmbedQueue{embedder: emb},
			dbConfigResolver: func(dbName string) (embeddingDims int, searchMinSimilarity float64, bm25Engine string) {
				return 0, 0.5, ""
			},
		}
		vec, err := db.EmbedQueryForDB(context.Background(), "dbx", "hello")
		require.NoError(t, err)
		require.Len(t, vec, 5)
	})

	t.Run("no embedder returns nil even when resolver exists", func(t *testing.T) {
		db := &DB{
			dbConfigResolver: func(dbName string) (embeddingDims int, searchMinSimilarity float64, bm25Engine string) {
				return 10, 0.5, ""
			},
		}
		vec, err := db.EmbedQueryForDB(context.Background(), "dbx", "hello")
		require.NoError(t, err)
		require.Nil(t, vec)
	})

	t.Run("embedConfigForDB enabled with nil queue falls back to global nil", func(t *testing.T) {
		db := &DB{
			embedConfigForDB: func(dbName string) (*embed.Config, error) {
				return &embed.Config{Provider: "local", Model: "m", Dimensions: 4}, nil
			},
		}
		vec, err := db.EmbedQueryForDB(context.Background(), "dbx", "hello")
		require.NoError(t, err)
		require.Nil(t, vec)
	})
}

func TestEmbedConfigKey_LocalZeroAndAutoLayers_AreEquivalent(t *testing.T) {
	cfgZero := &embed.Config{
		Provider:   "local",
		Model:      "bge-m3",
		Dimensions: 1024,
		ModelsDir:  "models",
		GPULayers:  0,
	}
	cfgAuto := &embed.Config{
		Provider:   "local",
		Model:      "bge-m3",
		Dimensions: 1024,
		ModelsDir:  "models",
		GPULayers:  -1,
	}
	require.Equal(t, embedConfigKey(cfgZero), embedConfigKey(cfgAuto))
}

func TestDB_GetOrCreateEmbedderForDB_LocalEquivalentConfig_AliasesDefaultEmbedder(t *testing.T) {
	defaultEmbedder := &chunkingTestEmbedder{dims: 1024}
	defaultCfg := &embed.Config{
		Provider:   "local",
		Model:      "bge-m3",
		Dimensions: 1024,
		ModelsDir:  "models",
		GPULayers:  0,
	}
	db := &DB{
		embedQueue: &EmbedQueue{embedder: defaultEmbedder},
		embedderRegistry: map[string]embed.Embedder{
			embedConfigKey(defaultCfg): defaultEmbedder,
		},
		defaultEmbedKey: embedConfigKey(defaultCfg),
	}
	db.embedConfigForDB = func(dbName string) (*embed.Config, error) {
		return &embed.Config{
			Provider:   "local",
			Model:      "bge-m3",
			Dimensions: 1024,
			ModelsDir:  "models",
			// Equivalent local default expressed as -1 instead of 0.
			GPULayers: -1,
		}, nil
	}

	got, err := db.getOrCreateEmbedderForDB("translations")
	require.NoError(t, err)
	require.Same(t, defaultEmbedder, got)

	aliasKey := embedConfigKey(&embed.Config{
		Provider:   "local",
		Model:      "bge-m3",
		Dimensions: 1024,
		ModelsDir:  "models",
		GPULayers:  -1,
	})

	db.embedderRegistryMu.RLock()
	aliased := db.embedderRegistry[aliasKey]
	db.embedderRegistryMu.RUnlock()
	require.Same(t, defaultEmbedder, aliased)
}

type factoryTestEmbedder struct {
	dims int
}

func (e *factoryTestEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	vec := make([]float32, e.dims)
	if e.dims > 0 {
		vec[0] = 1
	}
	return vec, nil
}
func (e *factoryTestEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i], _ = e.Embed(ctx, texts[i])
	}
	return out, nil
}
func (e *factoryTestEmbedder) Dimensions() int { return e.dims }
func (e *factoryTestEmbedder) Model() string   { return "factory-test" }
func (e *factoryTestEmbedder) Backend() string { return "cpu" } // Plan 04-05 D-06
func (e *factoryTestEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return chunkTestText(text, maxTokens, overlap)
}

func TestDB_GetOrCreateEmbedderForDB_SingleFlightAndNoStatsBlocking(t *testing.T) {
	fallback := &chunkingTestEmbedder{dims: 8}
	db := &DB{
		embedQueue: &EmbedQueue{embedder: fallback},
	}
	db.embedConfigForDB = func(dbName string) (*embed.Config, error) {
		return &embed.Config{
			Provider:   "openai",
			Model:      "test-model",
			Dimensions: 8,
			APIURL:     "https://example.invalid",
			APIKey:     "k",
		}, nil
	}

	var createCalls atomic.Int64
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	db.embedderFactory = func(cfg *embed.Config) (embed.Embedder, error) {
		createCalls.Add(1)
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return &factoryTestEmbedder{dims: cfg.Dimensions}, nil
	}

	const workers = 6
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() {
			_, err := db.getOrCreateEmbedderForDB("translations")
			errCh <- err
		}()
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("embedder factory did not start")
	}

	// While embedder creation is in-flight, stats paths should remain responsive.
	statsDone := make(chan struct{}, 1)
	go func() {
		_ = db.EmbeddingCountCached()
		_ = db.PendingEmbeddingsCount()
		statsDone <- struct{}{}
	}()
	select {
	case <-statsDone:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("stats calls blocked while embedder creation in-flight")
	}

	close(release)

	for i := 0; i < workers; i++ {
		require.NoError(t, <-errCh)
	}
	require.Equal(t, int64(1), createCalls.Load(), "expected single-flight embedder creation")
}

func TestDB_GetOrCreateEmbedderForDB_FallbackBranches(t *testing.T) {
	t.Run("no queue configured returns nil embedder without error", func(t *testing.T) {
		db := &DB{}
		got, err := db.getOrCreateEmbedderForDB("tenant")
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("nil resolver returns active queue embedder", func(t *testing.T) {
		fallback := &chunkingTestEmbedder{dims: 6}
		db := &DB{embedQueue: &EmbedQueue{embedder: fallback}}
		got, err := db.getOrCreateEmbedderForDB("tenant")
		require.NoError(t, err)
		require.Same(t, fallback, got)
	})

	t.Run("queue configured with nil embedder returns nil without error", func(t *testing.T) {
		db := &DB{embedQueue: &EmbedQueue{embedder: nil}}
		got, err := db.getOrCreateEmbedderForDB("tenant")
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("resolver error falls back to active queue embedder", func(t *testing.T) {
		fallback := &chunkingTestEmbedder{dims: 6}
		db := &DB{
			embedQueue: &EmbedQueue{embedder: fallback},
			embedConfigForDB: func(dbName string) (*embed.Config, error) {
				return nil, errors.New("resolver-failed")
			},
		}
		got, err := db.getOrCreateEmbedderForDB("tenant")
		require.NoError(t, err)
		require.Same(t, fallback, got)
	})

	t.Run("resolver returns nil config and falls back to active queue embedder", func(t *testing.T) {
		fallback := &chunkingTestEmbedder{dims: 6}
		db := &DB{
			embedQueue: &EmbedQueue{embedder: fallback},
			embedConfigForDB: func(dbName string) (*embed.Config, error) {
				return nil, nil
			},
		}
		got, err := db.getOrCreateEmbedderForDB("tenant")
		require.NoError(t, err)
		require.Same(t, fallback, got)
	})

	t.Run("uses default embedder when resolved key matches default and explicit key missing", func(t *testing.T) {
		fallback := &chunkingTestEmbedder{dims: 5}
		defaultCfg := &embed.Config{Provider: "local", Model: "bge-m3", Dimensions: 5, GPULayers: -1}
		defaultKey := embedConfigKey(defaultCfg)
		db := &DB{
			embedQueue: &EmbedQueue{embedder: fallback},
			embedConfigForDB: func(dbName string) (*embed.Config, error) {
				// Equivalent config that resolves to the same key.
				return &embed.Config{Provider: "local", Model: "bge-m3", Dimensions: 5, GPULayers: 0}, nil
			},
			defaultEmbedKey: defaultKey,
			embedderRegistry: map[string]embed.Embedder{
				defaultKey: fallback,
			},
		}
		got, err := db.getOrCreateEmbedderForDB("tenant")
		require.NoError(t, err)
		require.Same(t, fallback, got)
	})

	t.Run("factory failure falls back to active queue embedder", func(t *testing.T) {
		fallback := &chunkingTestEmbedder{dims: 8}
		db := &DB{
			embedQueue: &EmbedQueue{embedder: fallback},
			embedConfigForDB: func(dbName string) (*embed.Config, error) {
				return &embed.Config{
					Provider:   "openai",
					Model:      "test-model",
					Dimensions: 8,
					APIURL:     "https://example.invalid",
					APIKey:     "k",
				}, nil
			},
			embedderFactory: func(cfg *embed.Config) (embed.Embedder, error) {
				return nil, errors.New("factory failed")
			},
		}

		got, err := db.getOrCreateEmbedderForDB("tenant")
		require.NoError(t, err)
		require.Same(t, fallback, got)

		key := embedConfigKey(&embed.Config{
			Provider:   "openai",
			Model:      "test-model",
			Dimensions: 8,
			APIURL:     "https://example.invalid",
			APIKey:     "k",
		})
		db.embedderRegistryMu.RLock()
		_, exists := db.embedderRegistry[key]
		db.embedderRegistryMu.RUnlock()
		require.False(t, exists)
	})

	t.Run("single-flight waiter falls back when creation fails", func(t *testing.T) {
		fallback := &chunkingTestEmbedder{dims: 8}
		db := &DB{
			embedQueue: &EmbedQueue{embedder: fallback},
			embedConfigForDB: func(dbName string) (*embed.Config, error) {
				return &embed.Config{
					Provider:   "openai",
					Model:      "singleflight-failure",
					Dimensions: 8,
					APIURL:     "https://example.invalid",
					APIKey:     "k",
				}, nil
			},
		}

		started := make(chan struct{}, 1)
		release := make(chan struct{})
		var createCalls atomic.Int64
		db.embedderFactory = func(cfg *embed.Config) (embed.Embedder, error) {
			createCalls.Add(1)
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return nil, errors.New("factory failed")
		}

		type callResult struct {
			embedder embed.Embedder
			err      error
		}
		resCh1 := make(chan callResult, 1)
		resCh2 := make(chan callResult, 1)
		go func() {
			e, err := db.getOrCreateEmbedderForDB("tenant_a")
			resCh1 <- callResult{embedder: e, err: err}
		}()
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("factory did not start")
		}
		go func() {
			e, err := db.getOrCreateEmbedderForDB("tenant_a")
			resCh2 <- callResult{embedder: e, err: err}
		}()

		close(release)
		r1 := <-resCh1
		r2 := <-resCh2
		require.NoError(t, r1.err)
		require.NoError(t, r2.err)
		require.Same(t, fallback, r1.embedder)
		require.Same(t, fallback, r2.embedder)
		require.GreaterOrEqual(t, createCalls.Load(), int64(1))
		require.LessOrEqual(t, createCalls.Load(), int64(2))
	})

	t.Run("single-flight waiter returns embedder when creator populates registry", func(t *testing.T) {
		fallback := &chunkingTestEmbedder{dims: 8}
		target := &factoryTestEmbedder{dims: 8}
		keyCfg := &embed.Config{
			Provider:   "openai",
			Model:      "wait-success",
			Dimensions: 8,
			APIURL:     "https://example.invalid",
			APIKey:     "k",
		}
		key := embedConfigKey(keyCfg)

		db := &DB{
			embedQueue: &EmbedQueue{embedder: fallback},
			embedConfigForDB: func(dbName string) (*embed.Config, error) {
				return keyCfg, nil
			},
			embedderCreate: map[string]chan struct{}{
				key: make(chan struct{}),
			},
			embedderRegistry: map[string]embed.Embedder{},
		}

		done := make(chan struct{})
		go func() {
			time.Sleep(10 * time.Millisecond)
			db.embedderRegistryMu.Lock()
			db.embedderRegistry[key] = target
			db.embedderRegistryMu.Unlock()
			db.embedderCreateMu.Lock()
			close(db.embedderCreate[key])
			db.embedderCreateMu.Unlock()
			close(done)
		}()

		got, err := db.getOrCreateEmbedderForDB("tenant_wait_success")
		require.NoError(t, err)
		require.Same(t, target, got)
		<-done
	})

	t.Run("factory returns nil embedder and falls back", func(t *testing.T) {
		fallback := &chunkingTestEmbedder{dims: 8}
		db := &DB{
			embedQueue: &EmbedQueue{embedder: fallback},
			embedConfigForDB: func(dbName string) (*embed.Config, error) {
				return &embed.Config{
					Provider:   "openai",
					Model:      "nil-embedder",
					Dimensions: 8,
					APIURL:     "https://example.invalid",
					APIKey:     "k",
				}, nil
			},
			embedderFactory: func(cfg *embed.Config) (embed.Embedder, error) {
				return nil, nil
			},
		}

		got, err := db.getOrCreateEmbedderForDB("tenant_nil_embedder")
		require.NoError(t, err)
		require.Same(t, fallback, got)
	})
}

package nornicdb

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/search"
)

// embedQueryChunksWithEmbedder runs query chunking and embeds each chunk.
// Callers that can score multiple query vectors should use this shape and keep
// the best result across chunks instead of collapsing the query into a centroid.
func (db *DB) embedQueryChunksWithEmbedder(ctx context.Context, emb cypher.QueryEmbedder, query string) ([]string, [][]float32, error) {
	if emb == nil {
		return nil, nil, nil
	}
	ctx, finish, err := db.beginProviderUse(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer finish()
	chunks, err := db.chunkQueryWithEmbedder(ctx, emb, query)
	if err != nil {
		return nil, nil, err
	}
	if chunks, err = search.LimitQueryChunks(chunks); err != nil {
		return nil, nil, err
	}
	if len(chunks) == 0 {
		return nil, nil, nil
	}
	if len(chunks) == 1 {
		var vec []float32
		var err error
		if typed, ok := emb.(embed.TypedEmbedder); ok {
			vec, err = typed.EmbedWithInputType(ctx, chunks[0], embed.InputTypeQuery)
		} else {
			vec, err = emb.Embed(ctx, chunks[0])
		}
		if err != nil {
			return chunks, nil, err
		}
		if err := embed.CheckEmbeddings([][]float32{vec}, 1, 0); err != nil {
			return chunks, nil, err
		}
		return chunks, [][]float32{vec}, nil
	}
	var embs [][]float32
	if typed, ok := emb.(embed.TypedEmbedder); ok {
		embs, err = typed.EmbedBatchWithInputType(ctx, chunks, embed.InputTypeQuery)
	} else if batch, ok := emb.(interface {
		EmbedBatch(context.Context, []string) ([][]float32, error)
	}); ok {
		embs, err = batch.EmbedBatch(ctx, chunks)
	} else {
		embs = make([][]float32, len(chunks))
		for i, chunk := range chunks {
			embs[i], err = emb.Embed(ctx, chunk)
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		return chunks, nil, err
	}
	// One non-empty vector per chunk, or the query's embedding failed; the
	// dimension is checked per database (validateQueryEmbeddingDimensions).
	if err := embed.CheckEmbeddings(embs, len(chunks), 0); err != nil {
		return chunks, nil, err
	}
	return chunks, embs, nil
}

// embedQueryWithEmbedder returns a single query embedding for compatibility-only callers.
// Multi-chunk queries are intentionally not averaged; callers that need full
// recall should use embedQueryChunksWithEmbedder and score best-of chunks.
func (db *DB) embedQueryWithEmbedder(ctx context.Context, emb cypher.QueryEmbedder, query string) ([]float32, error) {
	_, embs, err := db.embedQueryChunksWithEmbedder(ctx, emb, query)
	if err != nil || len(embs) == 0 {
		return nil, err
	}
	for _, v := range embs {
		if len(v) > 0 {
			return v, nil
		}
	}
	return nil, nil
}

func (db *DB) validateQueryEmbeddingDimensions(dbName string, vec []float32) error {
	db.dbConfigResolverMu.RLock()
	resolver := db.dbConfigResolver
	db.dbConfigResolverMu.RUnlock()
	if resolver == nil || len(vec) == 0 {
		return nil
	}
	resolvedDims, _ := resolver(dbName)
	if resolvedDims <= 0 {
		return nil
	}
	if len(vec) != resolvedDims {
		return localizedError(localization.NornicDBCoreQueryDimensionMismatchForDatabase(dbName, resolvedDims, len(vec)), ErrQueryEmbeddingDimensionMismatch)
	}
	return nil
}

func (db *DB) validateQueryEmbeddingBatchDimensions(dbName string, embs [][]float32) error {
	for _, vec := range embs {
		if err := db.validateQueryEmbeddingDimensions(dbName, vec); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) embedQueryChunksForDBWithEmbedder(ctx context.Context, dbName string, emb cypher.QueryEmbedder, query string) ([]string, [][]float32, error) {
	chunks, embs, err := db.embedQueryChunksWithEmbedder(ctx, emb, query)
	if err != nil {
		return nil, nil, err
	}
	if err := db.validateQueryEmbeddingBatchDimensions(dbName, embs); err != nil {
		return nil, nil, err
	}
	return chunks, embs, nil
}

// EmbedQueryChunks returns embedder-safe query chunks and one embedding per chunk.
// This is the preferred API for search paths that should compare best-of chunks.
func (db *DB) EmbedQueryChunks(ctx context.Context, query string) ([]string, [][]float32, error) {
	emb := db.queryEmbedderForRequest(ctx)
	if emb == nil {
		return nil, nil, nil
	}
	return db.embedQueryChunksWithEmbedder(ctx, emb, query)
}

// EmbedQueryChunksForDB returns per-chunk query embeddings using the embedder configured for a database.
func (db *DB) EmbedQueryChunksForDB(ctx context.Context, dbName string, query string) ([]string, [][]float32, error) {
	return db.QueryEmbeddingForDB(dbName).EmbedQueryChunks(ctx, query)
}

func (db *DB) chunkQueryWithEmbedder(ctx context.Context, emb cypher.QueryEmbedder, query string) ([]string, error) {
	if emb == nil {
		return nil, nil
	}
	_, finish, err := db.beginProviderUse(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	const (
		queryChunkSize    = 512
		queryChunkOverlap = 50
	)
	// Every chunk: the search that uses them keeps the first
	// NORNICDB_SEARCH_QUERY_MAX_CHUNKS and reports how many it searched
	// (search.LimitQueryChunks).
	return emb.ChunkText(query, queryChunkSize, queryChunkOverlap)
}

// EmbedQuery generates an embedding for a search query.
// Returns nil if embeddings are not enabled or embedder is not yet set (e.g. still loading).
func (db *DB) EmbedQuery(ctx context.Context, query string) ([]float32, error) {
	emb := db.queryEmbedderForRequest(ctx)
	if emb == nil {
		return nil, nil // Not an error - just no embedding available
	}
	return db.embedQueryWithEmbedder(ctx, emb, query)
}

// ChunkQuery splits a search query using the configured global embedder.
func (db *DB) ChunkQuery(ctx context.Context, query string) ([]string, error) {
	emb := db.queryEmbedderForRequest(ctx)
	if emb == nil {
		return []string{query}, nil
	}
	return db.chunkQueryWithEmbedder(ctx, emb, query)
}

// globalEmbedder reads the shared query publication with one atomic load. The
// executor is initialized by Open before the DB is exposed and never replaced.
func (db *DB) globalEmbedder() cypher.QueryEmbedder {
	if db.cypherExecutor == nil {
		return nil
	}
	return db.cypherExecutor.GetEmbedder()
}

type queryEmbeddingRequestKey struct{}

type queryEmbeddingRequestContext struct {
	context.Context
	db       *DB
	embedder cypher.QueryEmbedder
}

func (c *queryEmbeddingRequestContext) Value(key any) any {
	if _, ok := key.(queryEmbeddingRequestKey); ok {
		return c
	}
	return c.Context.Value(key)
}

// WithQueryEmbeddingSnapshot binds the current global query provider to a text
// request. EmbedQuery, ChunkQuery and EmbedQueryChunks reuse it, including nil
// during startup. Nested requests on this DB keep the outer snapshot. Call only
// when text must be embedded; ID and numeric-vector operations need no binding.
func (db *DB) WithQueryEmbeddingSnapshot(ctx context.Context) context.Context {
	if snapshot, ok := ctx.Value(queryEmbeddingRequestKey{}).(*queryEmbeddingRequestContext); ok && snapshot.db == db {
		return ctx
	}
	return &queryEmbeddingRequestContext{Context: ctx, db: db, embedder: db.globalEmbedder()}
}

func (db *DB) queryEmbedderForRequest(ctx context.Context) cypher.QueryEmbedder {
	if snapshot, ok := ctx.Value(queryEmbeddingRequestKey{}).(*queryEmbeddingRequestContext); ok && snapshot.db == db {
		return snapshot.embedder
	}
	return db.globalEmbedder()
}

// EmbedQueryForDB generates an embedding for a search query using the embedder for the given database.
// When SetEmbedConfigForDB is set, the per-DB embedder registry is used so the query vector matches
// the index for that database. Otherwise it uses the global embedder and validates dimensions match
// (returns ErrQueryEmbeddingDimensionMismatch if not).
func (db *DB) EmbedQueryForDB(ctx context.Context, dbName string, query string) ([]float32, error) {
	return db.QueryEmbeddingForDB(dbName).EmbedQuery(ctx, query)
}

// ChunkQueryForDB splits a search query using the embedder configured for the given database.
func (db *DB) ChunkQueryForDB(ctx context.Context, dbName string, query string) ([]string, error) {
	return db.QueryEmbeddingForDB(dbName).ChunkQuery(ctx, query)
}

// QueryEmbeddingSnapshot binds chunking and embedding to one provider for a
// native search request. A captured nil remains unavailable until that request
// ends; the next request observes readiness. It retains no provider history.
type QueryEmbeddingSnapshot struct {
	db       *DB
	database string
	embedder cypher.QueryEmbedder

	// The single-vector API historically checks dimensions only without a
	// model-config resolver. The batch API always validates every vector.
	validateSingleDimensions bool
}

// QueryEmbeddingForDB resolves the query provider once. Capture the returned
// value at the request boundary and use its methods for every query chunk.
// Global/default models use the shared publication; explicit per-database
// models continue using the model registry shared with document embedding.
func (db *DB) QueryEmbeddingForDB(dbName string) QueryEmbeddingSnapshot {
	snapshot := QueryEmbeddingSnapshot{db: db, database: dbName, embedder: db.globalEmbedder(), validateSingleDimensions: true}
	if snapshot.embedder == nil {
		return snapshot
	}
	db.mu.RLock()
	resolve := db.embedConfigForDB
	db.mu.RUnlock()
	if resolve == nil {
		return snapshot
	}
	snapshot.validateSingleDimensions = false
	cfg, err := resolve(dbName)
	if err != nil || cfg == nil {
		return snapshot
	}
	key := embedConfigKey(cfg)
	db.embedderRegistryMu.RLock()
	isDefault := key == db.defaultEmbedKey
	db.embedderRegistryMu.RUnlock()
	if !isDefault {
		if provider := db.embedderForConfig(cfg); provider != nil {
			snapshot.embedder = provider
		}
	}
	return snapshot
}

// ChunkQuery splits text using this request's provider, or leaves it intact
// while embedding is unavailable.
func (s QueryEmbeddingSnapshot) ChunkQuery(ctx context.Context, query string) ([]string, error) {
	if s.embedder == nil {
		return []string{query}, nil
	}
	return s.db.chunkQueryWithEmbedder(ctx, s.embedder, query)
}

// EmbedQuery embeds text with this request's provider, preserving the database's
// single-vector dimension-check policy. Use EmbedQueryChunks to validate all chunks.
func (s QueryEmbeddingSnapshot) EmbedQuery(ctx context.Context, query string) ([]float32, error) {
	vec, err := s.db.embedQueryWithEmbedder(ctx, s.embedder, query)
	if err != nil {
		return nil, err
	}
	if s.validateSingleDimensions {
		if err := s.db.validateQueryEmbeddingDimensions(s.database, vec); err != nil {
			return nil, err
		}
	}
	return vec, nil
}

// EmbedQueryChunks embeds all chunks with this request's provider and checks
// their dimensions against the selected database's index.
func (s QueryEmbeddingSnapshot) EmbedQueryChunks(ctx context.Context, query string) ([]string, [][]float32, error) {
	return s.db.embedQueryChunksForDBWithEmbedder(ctx, s.database, s.embedder, query)
}

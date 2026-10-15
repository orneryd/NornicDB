package nornicdb

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/kms"
	"github.com/orneryd/nornicdb/pkg/retention"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type coverageQueryEmbedder struct {
	chunks   []string
	chunkErr error
	embed    []float32
	embedErr error
	batch    [][]float32
	batchErr error
}

func (e *coverageQueryEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	return e.embed, e.embedErr
}

func (e *coverageQueryEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	return e.batch, e.batchErr
}

func (e *coverageQueryEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return e.chunks, e.chunkErr
}

func (e *coverageQueryEmbedder) Dimensions() int { return 3 }
func (e *coverageQueryEmbedder) Model() string   { return "coverage" }
func (e *coverageQueryEmbedder) Backend() string { return "cpu" }

var _ embed.Embedder = (*coverageQueryEmbedder)(nil)

func TestDBCachingAccessorAndNoopBranches(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	queue := NewEmbedQueue(nil, engine, &EmbedQueueConfig{DeferWorkerStart: true})
	t.Cleanup(func() { queue.Close() })

	db := &DB{
		storage:    engine,
		config:     &nornicConfig.Config{},
		embedQueue: queue,
		buildCtx:   context.Background(),
	}

	require.Equal(t, queue, db.GetEmbedQueue())
	require.Nil(t, db.GetAccessFlusher())
	require.Nil(t, db.GetReplicator())
	require.Nil(t, db.GetRetentionManager())

	called := false
	db.SetRetentionAuditCallback(func(action, recordID, category string) { called = true })
	require.NotNil(t, db.onRetentionAction)
	db.onRetentionAction("delete", "n1", "doc")
	require.True(t, called)

	shouldYield := func() bool { return true }
	db.SetEmbedQueueShouldYield(shouldYield)
	require.NotNil(t, db.embedQueueYieldFn)
	queue.mu.Lock()
	gotYield := queue.shouldYield
	queue.mu.Unlock()
	require.NotNil(t, gotYield)
	require.True(t, gotYield())

	db.RunRetentionSweep(context.Background())
	db.startRetentionSweep(context.Background())
	require.Zero(t, (*EmbedQueue)(nil).QueueLen())
}

func TestDBRetentionPoliciesPathBranches(t *testing.T) {
	require.Equal(t, "retention-policies.json", (*DB)(nil).retentionPoliciesPath())
	require.Equal(t, "retention-policies.json", (&DB{}).retentionPoliciesPath())

	db := &DB{config: &nornicConfig.Config{}}
	require.Equal(t, "retention-policies.json", db.retentionPoliciesPath())

	db.config.Database.DataDir = t.TempDir()
	require.Equal(t, filepath.Join(db.config.Database.DataDir, "retention-policies.json"), db.retentionPoliciesPath())

	db.config.Retention.PoliciesFile = "custom-retention.json"
	require.Equal(t, "custom-retention.json", db.retentionPoliciesPath())
}

func TestDBCollectSubjectRetentionRecords(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	_, err := engine.CreateNode(&storage.Node{ID: "nornic:n1", Labels: []string{"Doc"}, Properties: map[string]interface{}{"owner_id": "alice", "category": "note"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "nornic:n2", Labels: []string{"Doc"}, Properties: map[string]interface{}{"owner_id": "bob"}})
	require.NoError(t, err)

	db := &DB{storage: engine, config: &nornicConfig.Config{}}
	records, err := db.CollectSubjectRetentionRecords(context.Background(), "alice")
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, "nornic:n1", records[0].ID)
	require.Equal(t, "alice", records[0].SubjectID)
	require.Equal(t, retention.CategoryUser, records[0].Category)

	records, err = db.CollectSubjectRetentionRecords(context.Background(), "missing")
	require.NoError(t, err)
	require.Empty(t, records)
}

func TestDBSearchServicePendingAndIgnoreBranches(t *testing.T) {
	entry := &dbSearchService{}
	require.False(t, entry.hasPending())
	require.Nil(t, (*dbSearchService)(nil).drainPending())
	require.False(t, (*dbSearchService)(nil).hasPending())
	require.Equal(t, searchMutationDebounceDelay, (*dbSearchService)(nil).flushDelay())

	entry.pendingFlushDelay = time.Millisecond
	require.Equal(t, time.Millisecond, entry.flushDelay())
	entry.queueIndex(nil)
	entry.queueRemove("")
	require.False(t, entry.hasPending())

	node := &storage.Node{ID: "n1", Labels: []string{"Doc"}, Properties: map[string]any{"title": "old"}}
	entry.queueIndex(node)
	node.Properties["title"] = "mutated"
	entry.queueRemove("n2")
	require.True(t, entry.hasPending())
	ops := entry.drainPending()
	require.Len(t, ops, 2)
	require.Equal(t, "old", ops["n1"].node.Properties["title"])
	require.True(t, ops["n2"].remove)
	require.False(t, entry.hasPending())
	require.Nil(t, entry.drainPending())

	entry.buildDone = make(chan struct{})
	entry.closeBuildDone()
	entry.closeBuildDone()
	(*dbSearchService)(nil).closeBuildDone()

	db := &DB{}
	require.False(t, db.shouldIgnoreSearchIndexingError(nil))
	require.True(t, db.shouldIgnoreSearchIndexingError(context.Canceled))
	require.True(t, db.shouldIgnoreSearchIndexingError(storage.ErrStorageClosed))
	require.True(t, db.shouldIgnoreSearchIndexingError(ErrClosed))
	require.False(t, db.shouldIgnoreSearchIndexingError(errors.New("boom")))
	db.closed = true
	require.True(t, db.shouldIgnoreSearchIndexingError(errors.New("boom")))

	dbName, local, ok := splitQualifiedID("tenant:n1")
	require.True(t, ok)
	require.Equal(t, "tenant", dbName)
	require.Equal(t, "n1", local)
	for _, id := range []string{"", "plain", ":local", "tenant:"} {
		_, _, ok = splitQualifiedID(id)
		require.False(t, ok, id)
	}
}

func TestDBQueryEmbeddingBranchHelpers(t *testing.T) {
	db := &DB{}
	vec, err := db.embedQueryWithEmbedder(context.Background(), nil, "hello")
	require.NoError(t, err)
	require.Nil(t, vec)

	chunkErr := errors.New("chunk failed")
	_, err = db.embedQueryWithEmbedder(context.Background(), &coverageQueryEmbedder{chunkErr: chunkErr}, "hello")
	require.ErrorIs(t, err, chunkErr)

	emb := &coverageQueryEmbedder{chunks: []string{"one"}, embed: []float32{1, 2, 3}}
	vec, err = db.embedQueryWithEmbedder(context.Background(), emb, "one")
	require.NoError(t, err)
	require.Equal(t, []float32{1, 2, 3}, vec)

	// A provider answer that isn't one non-empty vector per chunk is a failed
	// embedding.
	emb = &coverageQueryEmbedder{chunks: []string{"a", "b", "c"}, batch: [][]float32{{1, 0, 0}, {}, {0, 1, 0}, {1, 2}}}
	_, err = db.embedQueryWithEmbedder(context.Background(), emb, "many")
	var unusable *embed.UnusableEmbeddingError
	require.ErrorAs(t, err, &unusable)
	emb = &coverageQueryEmbedder{chunks: []string{"a", "b", "c"}, batch: [][]float32{{1, 0, 0}, {}, {0, 1, 0}}}
	_, _, err = db.embedQueryChunksWithEmbedder(context.Background(), emb, "many")
	require.ErrorContains(t, err, "empty vector for input 1")
	emb = &coverageQueryEmbedder{chunks: []string{"a", "b", "c"}, batch: [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}}}
	chunks, embs, err := db.embedQueryChunksWithEmbedder(context.Background(), emb, "many")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, chunks)
	require.Equal(t, [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}}, embs)

	batchErr := errors.New("batch failed")
	emb = &coverageQueryEmbedder{chunks: []string{"a", "b"}, batchErr: batchErr}
	vec, err = db.embedQueryWithEmbedder(context.Background(), emb, "many")
	require.ErrorIs(t, err, batchErr)
	require.Nil(t, vec)

	emb = &coverageQueryEmbedder{chunks: []string{"a", "b"}, batch: [][]float32{{}, {}}}
	vec, err = db.embedQueryWithEmbedder(context.Background(), emb, "many")
	require.ErrorContains(t, err, "empty vector for input 0")
	require.Nil(t, vec)

	manyChunks := make([]string, 40)
	for i := range manyChunks {
		manyChunks[i] = "chunk"
	}
	// Chunking returns every chunk; embedding keeps the first
	// NORNICDB_SEARCH_QUERY_MAX_CHUNKS (32 by default).
	gotChunks, err := db.chunkQueryWithEmbedder(context.Background(), &coverageQueryEmbedder{chunks: manyChunks}, "query")
	require.NoError(t, err)
	require.Len(t, gotChunks, 40)
	manyBatch := make([][]float32, 32)
	for i := range manyBatch {
		manyBatch[i] = []float32{1, 0, 0}
	}
	gotChunks, _, err = db.embedQueryChunksWithEmbedder(context.Background(), &coverageQueryEmbedder{chunks: manyChunks, batch: manyBatch}, "query")
	require.NoError(t, err)
	require.Len(t, gotChunks, 32)

	gotChunks, err = db.ChunkQuery(context.Background(), "plain")
	require.NoError(t, err)
	require.Equal(t, []string{"plain"}, gotChunks)
}

func TestDBSubjectPropertyAndProviderKeyHelpers(t *testing.T) {
	require.Equal(t, []string{"owner_id"}, (*DB)(nil).subjectIdentifierProperties())
	require.Equal(t, []string{"owner_id"}, (*DB)(nil).subjectPseudonymizeProperties())
	require.Equal(t, []string{"email", "name", "username", "ip_address"}, (*DB)(nil).subjectRedactProperties())
	require.False(t, (*DB)(nil).nodeMatchesSubject(nil, "alice"))
	require.False(t, (*DB)(nil).nodeMatchesSubject(&storage.Node{Properties: map[string]any{"owner_id": "alice"}}, ""))

	db := &DB{config: &nornicConfig.Config{}}
	db.config.Compliance.SubjectIdentifierProperties = []string{"subject", "owner_id"}
	db.config.Compliance.SubjectPseudonymizeProperties = []string{"subject"}
	db.config.Compliance.SubjectRedactProperties = []string{"email"}
	node := &storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{"subject": "alice", "owner_id": "ignored", "email": "a@example.com", "keep": "yes"}, ChunkEmbeddings: [][]float32{{1, 2}}}
	require.True(t, db.nodeMatchesSubject(node, "alice"))
	anon := db.anonymizedNodeCopy(node, "anon-1")
	require.Equal(t, "anon-1", anon.Properties["subject"])
	require.Equal(t, "a@example.com", node.Properties["email"])
	require.Equal(t, "yes", anon.Properties["keep"])
	anon.ChunkEmbeddings[0][0] = 99
	require.Equal(t, float32(1), node.ChunkEmbeddings[0][0])

	valid := strings.Repeat("a", 32)
	raw, err := decodeProviderMasterKey(valid)
	require.NoError(t, err)
	require.Equal(t, []byte(valid), raw)
	b64 := base64.StdEncoding.EncodeToString([]byte(valid))
	raw, err = decodeProviderMasterKey(b64)
	require.NoError(t, err)
	require.Equal(t, []byte(valid), raw)
	hexKey := hex.EncodeToString([]byte(valid))
	raw, err = decodeProviderMasterKey(hexKey)
	require.NoError(t, err)
	require.Equal(t, []byte(valid), raw)
	_, err = decodeProviderMasterKey("")
	require.Error(t, err)
	_, err = decodeProviderMasterKey("short")
	require.Error(t, err)

	require.False(t, rotationDue(nil, persistedProviderDEK{}))
	cfg := &nornicConfig.Config{}
	cfg.Database.EncryptionRotationEnabled = true
	cfg.Database.EncryptionRotationInterval = time.Hour
	require.True(t, rotationDue(cfg, persistedProviderDEK{}))
	require.True(t, rotationDue(cfg, persistedProviderDEK{CreatedAtRFC33: "not-a-time"}))
	require.True(t, rotationDue(cfg, persistedProviderDEK{CreatedAtRFC33: time.Now().Add(-2 * time.Hour).Format(time.RFC3339Nano)}))
	require.False(t, rotationDue(cfg, persistedProviderDEK{CreatedAtRFC33: time.Now().Format(time.RFC3339Nano)}))
	cfg.Database.EncryptionRotationInterval = 0
	require.False(t, rotationDue(cfg, persistedProviderDEK{CreatedAtRFC33: ""}))

	metadataPath := filepath.Join(t.TempDir(), "db.kms_dek.json")
	dataKey := &kms.DataKey{Plaintext: []byte(valid), Ciphertext: []byte("wrapped"), Algorithm: "AES-256-GCM", Version: 2, CreatedAt: time.Unix(10, 0).UTC(), KeyURI: "local://key"}
	require.NoError(t, persistProviderDEK(metadataPath, "local", dataKey))
	rawMetadata, err := os.ReadFile(metadataPath)
	require.NoError(t, err)
	var metadata persistedProviderDEK
	require.NoError(t, json.Unmarshal(rawMetadata, &metadata))
	require.Equal(t, "local", metadata.Provider)
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte("wrapped")), metadata.CiphertextB64)
}

func TestDBPublicQueryEmbeddingAndChunkingBranches(t *testing.T) {
	defaultEmbedder := &coverageQueryEmbedder{chunks: []string{"default-a", "default-b"}, embed: []float32{1, 0, 0}, batch: [][]float32{{1, 0, 0}, {1, 0, 0}}}
	db := &DB{cypherExecutor: queryEmbeddingTestExecutor(t, defaultEmbedder)}

	chunks, err := db.ChunkQuery(context.Background(), "query")
	require.NoError(t, err)
	require.Equal(t, []string{"default-a", "default-b"}, chunks)

	chunks, err = db.ChunkQueryForDB(context.Background(), "tenant", "query")
	require.NoError(t, err)
	require.Equal(t, []string{"default-a", "default-b"}, chunks)

	vec, err := db.EmbedQuery(context.Background(), "query")
	require.NoError(t, err)
	require.Equal(t, []float32{1, 0, 0}, vec)
	chunks, embs, err := db.EmbedQueryChunks(context.Background(), "query")
	require.NoError(t, err)
	require.Equal(t, []string{"default-a", "default-b"}, chunks)
	require.Equal(t, [][]float32{{1, 0, 0}, {1, 0, 0}}, embs)

	cfg := &embed.Config{Provider: "ollama", Model: "special", Dimensions: 3}
	specialEmbedder := &coverageQueryEmbedder{chunks: []string{"special"}, embed: []float32{0, 1, 0}}
	db.SetEmbedConfigForDB(func(dbName string) (*embed.Config, error) {
		require.Equal(t, "tenant", dbName)
		return cfg, nil
	})
	db.embedderRegistry = map[string]embed.Embedder{embedConfigKey(cfg): specialEmbedder}

	chunks, err = db.ChunkQueryForDB(context.Background(), "tenant", "query")
	require.NoError(t, err)
	require.Equal(t, []string{"special"}, chunks)
	vec, err = db.EmbedQueryForDB(context.Background(), "tenant", "query")
	require.NoError(t, err)
	require.Equal(t, []float32{0, 1, 0}, vec)

	db = &DB{cypherExecutor: queryEmbeddingTestExecutor(t, defaultEmbedder)}
	db.SetDbConfigResolver(func(dbName string) (int, float64, string) { return 99, 0, "" })
	vec, err = db.EmbedQueryForDB(context.Background(), "tenant", "query")
	require.ErrorIs(t, err, ErrQueryEmbeddingDimensionMismatch)
	require.Nil(t, vec)

	db.SetEmbedConfigForDB(func(dbName string) (*embed.Config, error) { return nil, errors.New("ignored") })
	chunks, err = db.ChunkQueryForDB(context.Background(), "tenant", "query")
	require.NoError(t, err)
	require.Equal(t, []string{"default-a", "default-b"}, chunks)

	db = &DB{}
	chunks, err = db.ChunkQueryForDB(context.Background(), "tenant", "query")
	require.NoError(t, err)
	require.Equal(t, []string{"query"}, chunks)
}

func TestDBSearchStatusAndDropStateBranches(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	cfg := DefaultConfig()
	db := &DB{
		storage:        storage.NewNamespacedEngine(base, "tenant"),
		baseStorage:    base,
		config:         cfg,
		searchServices: make(map[string]*dbSearchService),
	}

	db.SetDbSearchFlagsResolver(func(dbName string) (bool, bool, string, string) {
		require.Equal(t, "tenant", dbName)
		return true, false, "lazy", "startup"
	})
	status := db.GetDatabaseSearchStatus("tenant")
	require.False(t, status.Ready)
	require.False(t, status.Initialized)
	require.True(t, status.BM25Enabled)
	require.False(t, status.VectorEnabled)
	require.True(t, status.LazyTriggerNeeded)
	require.Equal(t, "not_initialized", status.Phase)
	require.Equal(t, int64(-1), status.ETASeconds)

	db.SetDbSearchFlagsResolver(func(dbName string) (bool, bool, string, string) { return false, false, "lazy", "lazy" })
	status = db.GetDatabaseSearchStatus("tenant")
	require.False(t, status.BM25Enabled)
	require.False(t, status.VectorEnabled)
	require.False(t, status.LazyTriggerNeeded)

	svc := search.NewServiceWithDimensions(base, 3)
	svc.MarkReadyDisabled()
	db.searchServices["tenant"] = &dbSearchService{dbName: "tenant", svc: svc}
	status = db.GetDatabaseSearchStatus("tenant")
	require.True(t, status.Initialized)
	require.True(t, status.Ready)
	require.Equal(t, svc.CurrentStrategy(), status.Strategy)

	dataDir := t.TempDir()
	db.config.Database.DataDir = dataDir
	searchDir := filepath.Join(dataDir, "search", "tenant")
	require.NoError(t, os.MkdirAll(searchDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(searchDir, "marker"), []byte("x"), 0o600))
	db.DropSearchServiceState("tenant")
	require.NoDirExists(t, searchDir)
	require.NotContains(t, db.searchServices, "tenant")

	db.config = nil
	db.searchServices["tenant"] = &dbSearchService{dbName: "tenant", svc: svc}
	db.DropSearchServiceState("tenant")
	require.NotContains(t, db.searchServices, "tenant")
}

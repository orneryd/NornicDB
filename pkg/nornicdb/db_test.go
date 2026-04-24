package nornicdb

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/decay"
	"github.com/orneryd/nornicdb/pkg/inference"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failingCloseEngine struct {
	storage.Engine
	closeErr error
}

func (f *failingCloseEngine) Close() error {
	return f.closeErr
}

type restoreFailEngine struct {
	storage.Engine
	createNodeErr error
	updateNodeErr error
	createEdgeErr error
	updateEdgeErr error
}

type allNodesErrorEngine struct {
	storage.Engine
	allNodesErr error
}

type temporalMaintenanceTestEngine struct {
	storage.Engine
	rebuildCalls      int
	pruneCalls        int
	rebuildErr        error
	pruneErr          error
	pruneResult       int64
	lastPrune         storage.TemporalPruneOptions
	mvccRebuildCalls  int
	mvccPruneCalls    int
	mvccRebuildErr    error
	mvccPruneErr      error
	mvccPruneResult   int64
	lastMVCCPrune     storage.MVCCPruneOptions
	lifecycleStatus   map[string]interface{}
	lifecycleErr      error
	lifecyclePruneCtx context.Context
	lifecyclePaused   int
	lifecycleResumed  int
}

func (e *allNodesErrorEngine) AllNodes() ([]*storage.Node, error) {
	if e.allNodesErr != nil {
		return nil, e.allNodesErr
	}
	return e.Engine.AllNodes()
}

func (e *temporalMaintenanceTestEngine) RebuildTemporalIndexes(ctx context.Context) error {
	e.rebuildCalls++
	return e.rebuildErr
}

func (e *temporalMaintenanceTestEngine) PruneTemporalHistory(ctx context.Context, opts storage.TemporalPruneOptions) (int64, error) {
	e.pruneCalls++
	e.lastPrune = opts
	return e.pruneResult, e.pruneErr
}

func (e *temporalMaintenanceTestEngine) RebuildMVCCHeads(ctx context.Context) error {
	e.mvccRebuildCalls++
	return e.mvccRebuildErr
}

func (e *temporalMaintenanceTestEngine) PruneMVCCVersions(ctx context.Context, opts storage.MVCCPruneOptions) (int64, error) {
	e.mvccPruneCalls++
	e.lastMVCCPrune = opts
	return e.mvccPruneResult, e.mvccPruneErr
}

func (e *temporalMaintenanceTestEngine) RegisterSnapshotReader(info storage.SnapshotReaderInfo) func() {
	_ = info
	return func() {}
}

func (e *temporalMaintenanceTestEngine) LifecycleStatus() map[string]interface{} {
	if e.lifecycleStatus == nil {
		return map[string]interface{}{"enabled": false}
	}
	status := make(map[string]interface{}, len(e.lifecycleStatus))
	for key, value := range e.lifecycleStatus {
		status[key] = value
	}
	return status
}

func (e *temporalMaintenanceTestEngine) TriggerPruneNow(ctx context.Context) error {
	e.lifecyclePruneCtx = ctx
	return e.lifecycleErr
}

func (e *temporalMaintenanceTestEngine) PauseLifecycle() {
	e.lifecyclePaused++
}

func (e *temporalMaintenanceTestEngine) ResumeLifecycle() {
	e.lifecycleResumed++
}

type closeErrReplicator struct {
	shutdownErr error
}

func (r *closeErrReplicator) Start(ctx context.Context) error { return nil }
func (r *closeErrReplicator) Apply(cmd *replication.Command, timeout time.Duration) error {
	return nil
}
func (r *closeErrReplicator) ApplyBatch(cmds []*replication.Command, timeout time.Duration) error {
	return nil
}
func (r *closeErrReplicator) IsLeader() bool                          { return true }
func (r *closeErrReplicator) LeaderAddr() string                      { return "" }
func (r *closeErrReplicator) LeaderID() string                        { return "" }
func (r *closeErrReplicator) Health() *replication.HealthStatus       { return nil }
func (r *closeErrReplicator) WaitForLeader(ctx context.Context) error { return nil }
func (r *closeErrReplicator) Shutdown() error                         { return r.shutdownErr }
func (r *closeErrReplicator) Mode() replication.ReplicationMode       { return replication.ModeStandalone }
func (r *closeErrReplicator) NodeID() string                          { return "n1" }

type closeErrTransport struct {
	closeErr error
}

func (t *closeErrTransport) Connect(ctx context.Context, addr string) (replication.PeerConnection, error) {
	return nil, nil
}
func (t *closeErrTransport) Listen(ctx context.Context, addr string, handler replication.ConnectionHandler) error {
	return nil
}
func (t *closeErrTransport) Close() error { return t.closeErr }

type nilSchemaEngine struct {
	storage.Engine
}

func (e *nilSchemaEngine) GetSchema() *storage.SchemaManager {
	return nil
}

func (e *restoreFailEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if e.createNodeErr != nil {
		return "", e.createNodeErr
	}
	return e.Engine.CreateNode(node)
}

func (e *restoreFailEngine) UpdateNode(node *storage.Node) error {
	if e.updateNodeErr != nil {
		return e.updateNodeErr
	}
	return e.Engine.UpdateNode(node)
}

func (e *restoreFailEngine) CreateEdge(edge *storage.Edge) error {
	if e.createEdgeErr != nil {
		return e.createEdgeErr
	}
	return e.Engine.CreateEdge(edge)
}

func (e *restoreFailEngine) UpdateEdge(edge *storage.Edge) error {
	if e.updateEdgeErr != nil {
		return e.updateEdgeErr
	}
	return e.Engine.UpdateEdge(edge)
}

func TestMemoryTierHalfLives(t *testing.T) {
	// Verify our decay constants match expected half-lives

	episodicHL := decay.HalfLife(decay.TierEpisodic)
	assert.InDelta(t, 7.0, episodicHL, 0.5, "Episodic should have ~7 day half-life")

	semanticHL := decay.HalfLife(decay.TierSemantic)
	assert.InDelta(t, 69.0, semanticHL, 2.0, "Semantic should have ~69 day half-life")

	proceduralHL := decay.HalfLife(decay.TierProcedural)
	assert.InDelta(t, 693.0, proceduralHL, 20.0, "Procedural should have ~693 day half-life")
}

func TestOpen(t *testing.T) {
	t.Run("with nil config uses defaults", func(t *testing.T) {
		tmpDir := t.TempDir() // Auto-cleanup after test
		db, err := Open(tmpDir, nil)
		require.NoError(t, err)
		require.NotNil(t, db)
		defer db.Close()

		assert.Equal(t, tmpDir, db.config.Database.DataDir)
		assert.False(t, db.config.Memory.DecayEnabled)
		assert.True(t, db.config.Memory.AutoLinksEnabled)
		assert.NotNil(t, db.storage)
		assert.Nil(t, db.decay)
		inferEngine, err := db.GetOrCreateInferenceService(db.defaultDatabaseName(), db.storage)
		require.NoError(t, err)
		assert.NotNil(t, inferEngine)
	})

	t.Run("with custom config", func(t *testing.T) {
		tmpDir := t.TempDir() // Auto-cleanup after test
		config := &Config{
			Memory: nornicConfig.MemoryConfig{
				DecayEnabled:     false,
				AutoLinksEnabled: false,
			},
		}
		db, err := Open(tmpDir, config)
		require.NoError(t, err)
		require.NotNil(t, db)
		defer db.Close()

		assert.Equal(t, tmpDir, db.config.Database.DataDir)
		assert.Nil(t, db.decay)
		inferEngine, err := db.GetOrCreateInferenceService(db.defaultDatabaseName(), db.storage)
		require.NoError(t, err)
		assert.Nil(t, inferEngine)
	})

	t.Run("returns error for invalid storage serializer", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Database.StorageSerializer = "invalid-serializer"
		_, err := Open(t.TempDir(), cfg)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported storage serializer")
	})

	t.Run("opens with msgpack storage serializer", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Database.StorageSerializer = "msgpack"
		db, err := Open(t.TempDir(), cfg)
		require.NoError(t, err)
		require.NotNil(t, db)
		require.NoError(t, db.Close())
	})

	t.Run("returns error when encryption enabled without password", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Database.EncryptionEnabled = true
		cfg.Database.EncryptionPassword = ""
		_, err := Open(t.TempDir(), cfg)
		require.Error(t, err)
		require.Contains(t, err.Error(), "encryption is enabled but no password was provided")
	})

	t.Run("creates and reuses encryption salt", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultConfig()
		cfg.Database.EncryptionEnabled = true
		cfg.Database.EncryptionPassword = "test-password"

		db, err := Open(dir, cfg)
		require.NoError(t, err)
		require.True(t, db.encryptionEnabled)
		require.NoError(t, db.Close())

		saltPath := filepath.Join(dir, "db.salt")
		firstSalt, err := os.ReadFile(saltPath)
		require.NoError(t, err)
		require.Len(t, firstSalt, 32)

		db2, err := Open(dir, cfg)
		require.NoError(t, err)
		require.True(t, db2.encryptionEnabled)
		require.NoError(t, db2.Close())

		secondSalt, err := os.ReadFile(saltPath)
		require.NoError(t, err)
		require.Equal(t, firstSalt, secondSalt)
	})

	t.Run("returns encryption error for wrong password on encrypted database", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultConfig()
		cfg.Database.EncryptionEnabled = true
		cfg.Database.EncryptionPassword = "correct-password"
		db, err := Open(dir, cfg)
		require.NoError(t, err)
		require.NoError(t, db.Close())

		wrong := DefaultConfig()
		wrong.Database.EncryptionEnabled = true
		wrong.Database.EncryptionPassword = "wrong-password"
		_, err = Open(dir, wrong)
		require.Error(t, err)
		require.True(t,
			strings.Contains(err.Error(), "Encryption key mismatch") || strings.Contains(err.Error(), "ENCRYPTION ERROR"),
			"unexpected error: %v", err,
		)
	})

	t.Run("replaces malformed existing encryption salt", func(t *testing.T) {
		dir := t.TempDir()
		saltPath := filepath.Join(dir, "db.salt")
		require.NoError(t, os.WriteFile(saltPath, []byte("short"), 0600))

		cfg := DefaultConfig()
		cfg.Database.EncryptionEnabled = true
		cfg.Database.EncryptionPassword = "correct-password"
		db, err := Open(dir, cfg)
		require.NoError(t, err)
		require.NoError(t, db.Close())

		salt, err := os.ReadFile(saltPath)
		require.NoError(t, err)
		require.Len(t, salt, 32)
		require.NotEqual(t, []byte("short"), salt)
	})

	t.Run("returns encryption-required error when encrypted database opened without encryption config", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultConfig()
		cfg.Database.EncryptionEnabled = true
		cfg.Database.EncryptionPassword = "correct-password"
		db, err := Open(dir, cfg)
		require.NoError(t, err)
		require.NoError(t, db.Close())

		_, err = Open(dir, DefaultConfig())
		require.Error(t, err)
		require.True(t,
			strings.Contains(err.Error(), "Encryption key mismatch") ||
				strings.Contains(err.Error(), "appears to be encrypted but no password was provided"),
			"unexpected error: %v", err,
		)
	})

	t.Run("explicit auto-recover env still skips when no recoverable artifacts", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultConfig()
		cfg.Database.EncryptionEnabled = true
		cfg.Database.EncryptionPassword = "correct-password"
		db, err := Open(dir, cfg)
		require.NoError(t, err)
		require.NoError(t, db.Close())

		t.Setenv("NORNICDB_AUTO_RECOVER_ON_CORRUPTION", "1")
		_, err = Open(dir, DefaultConfig())
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to open persistent storage")
	})

	t.Run("explicit auto-recover path is attempted when recoverable artifacts exist", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultConfig()
		cfg.Database.EncryptionEnabled = true
		cfg.Database.EncryptionPassword = "correct-password"
		db, err := Open(dir, cfg)
		require.NoError(t, err)
		require.NoError(t, db.Close())

		// Make artifacts discoverable so auto-recover branch is eligible.
		walDir := filepath.Join(dir, "wal")
		require.NoError(t, os.MkdirAll(walDir, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(walDir, "wal.log"), []byte("not-a-valid-wal"), 0644))

		t.Setenv("NORNICDB_AUTO_RECOVER_ON_CORRUPTION", "1")
		_, err = Open(dir, DefaultConfig())
		require.Error(t, err)
		require.Contains(t, err.Error(), "auto-recovery failed")
	})

	t.Run("returns persistent open error when dataDir is a file", func(t *testing.T) {
		filePath := filepath.Join(t.TempDir(), "not-a-dir")
		require.NoError(t, os.WriteFile(filePath, []byte("x"), 0600))

		_, err := Open(filePath, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to open persistent storage")
	})

	t.Run("returns error when encryption cannot create data directory", func(t *testing.T) {
		filePath := filepath.Join(t.TempDir(), "blocked")
		require.NoError(t, os.WriteFile(filePath, []byte("x"), 0600))

		cfg := DefaultConfig()
		cfg.Database.EncryptionEnabled = true
		cfg.Database.EncryptionPassword = "pw"
		_, err := Open(filePath, cfg)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to create data directory")
	})
}

func TestClose(t *testing.T) {
	t.Run("closes successfully", func(t *testing.T) {
		tmpDir := t.TempDir()
		db, err := Open(tmpDir, nil)
		require.NoError(t, err)

		err = db.Close()
		assert.NoError(t, err)
		assert.True(t, db.closed)
	})

	t.Run("close is idempotent", func(t *testing.T) {
		tmpDir := t.TempDir()
		db, err := Open(tmpDir, nil)
		require.NoError(t, err)

		err = db.Close()
		assert.NoError(t, err)

		// Second close should also succeed
		err = db.Close()
		assert.NoError(t, err)
	})

	t.Run("closeInternal aggregates close errors and cancels build context", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Database.PersistSearchIndexes = true
		cfg.Database.DataDir = t.TempDir()

		searchEngine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test")
		svc := search.NewServiceWithDimensions(searchEngine, 3)

		ctx, cancel := context.WithCancel(context.Background())
		db := &DB{
			config: cfg,
			baseStorage: &failingCloseEngine{
				Engine:   storage.NewMemoryEngine(),
				closeErr: errors.New("base close failed"),
			},
			searchServices: map[string]*dbSearchService{
				"test": {dbName: "test", svc: svc},
			},
			embedQueue: NewEmbedWorker(nil, searchEngine, &EmbedWorkerConfig{
				DeferWorkerStart: true,
			}),
			clusterTicker:     time.NewTicker(time.Hour),
			clusterTickerStop: make(chan struct{}),
			buildCtx:          ctx,
			buildCancel:       cancel,
		}
		t.Cleanup(func() {
			// closeInternal already calls Close() on this queue; this is just defensive.
			if db.embedQueue != nil {
				db.embedQueue.Close()
			}
		})

		err := db.closeInternal()
		require.Error(t, err)
		require.Contains(t, err.Error(), "base close failed")

		select {
		case <-ctx.Done():
		default:
			t.Fatal("build context should be canceled by closeInternal")
		}
		require.Nil(t, db.clusterTicker)
		require.Nil(t, db.clusterTickerStop)
	})

	t.Run("closeInternal aggregates replication shutdown errors", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		db := &DB{
			config: DefaultConfig(),
			replicator: &closeErrReplicator{
				shutdownErr: errors.New("replicator shutdown failed"),
			},
			replicationTrans: &closeErrTransport{
				closeErr: errors.New("transport close failed"),
			},
			buildCtx:    ctx,
			buildCancel: cancel,
		}

		err := db.closeInternal()
		require.Error(t, err)
		require.Contains(t, err.Error(), "replicator shutdown failed")
		require.Contains(t, err.Error(), "transport close failed")
	})
}

func TestMaybeEnableReplication(t *testing.T) {
	t.Run("standalone mode returns base storage unchanged", func(t *testing.T) {
		t.Setenv("NORNICDB_CLUSTER_MODE", string(replication.ModeStandalone))
		base := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })

		db := &DB{config: DefaultConfig()}
		got, err := db.maybeEnableReplication(base)
		require.NoError(t, err)
		require.Same(t, base, got)
	})

	t.Run("returns adapter creation error for invalid cluster data dir", func(t *testing.T) {
		t.Setenv("NORNICDB_CLUSTER_MODE", string(replication.ModeHAStandby))
		filePath := filepath.Join(t.TempDir(), "not-a-dir")
		require.NoError(t, os.WriteFile(filePath, []byte("x"), 0600))
		t.Setenv("NORNICDB_CLUSTER_DATA_DIR", filePath)

		base := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })

		db := &DB{config: DefaultConfig()}
		got, err := db.maybeEnableReplication(base)
		require.Error(t, err)
		require.Contains(t, err.Error(), "replication: create storage adapter")
		require.Nil(t, got)
	})
}

func TestStore(t *testing.T) {
	ctx := context.Background()

	t.Run("stores memory with defaults", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{
			Content: "Test content",
			Title:   "Test Title",
		}

		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)
		require.NotNil(t, stored)

		assert.NotEmpty(t, stored.ID)
		assert.Equal(t, "Test content", stored.Content)
		assert.Equal(t, "Test Title", stored.Title)
		assert.Equal(t, TierSemantic, stored.Tier)
		assert.Equal(t, 1.0, stored.DecayScore)
		assert.False(t, stored.CreatedAt.IsZero())
		assert.False(t, stored.LastAccessed.IsZero())
		assert.Equal(t, int64(0), stored.AccessCount)
	})

	t.Run("stores memory with explicit tier", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{
			Content: "Important skill",
			Tier:    TierProcedural,
		}

		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)
		assert.Equal(t, TierProcedural, stored.Tier)
	})

	t.Run("stores memory with tags and properties", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{
			Content:    "Tagged content",
			Tags:       []string{"tag1", "tag2"},
			Source:     "test-source",
			Properties: map[string]any{"custom": "value"},
		}

		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)
		assert.Equal(t, []string{"tag1", "tag2"}, stored.Tags)
		assert.Equal(t, "test-source", stored.Source)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		mem := &Memory{Content: "Test"}
		_, err = db.Store(ctx, mem)
		assert.ErrorIs(t, err, ErrClosed)
	})

	t.Run("returns error for nil memory", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Store(ctx, nil)
		assert.ErrorIs(t, err, ErrInvalidInput)
	})

	t.Run("stores with embeddings", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{
			Content:         "Memory with embedding",
			ChunkEmbeddings: [][]float32{{1.0, 0.0, 0.0}},
		}
		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)
		assert.Equal(t, [][]float32{{1.0, 0.0, 0.0}}, stored.ChunkEmbeddings)
	})
}

func TestRecall(t *testing.T) {
	ctx := context.Background()

	t.Run("recalls stored memory", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{
			Content: "Recallable content",
			Title:   "Recallable",
		}

		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)

		recalled, err := db.Recall(ctx, stored.ID)
		require.NoError(t, err)
		require.NotNil(t, recalled)

		assert.Equal(t, stored.ID, recalled.ID)
		assert.Equal(t, "Recallable content", recalled.Content)
		assert.Equal(t, int64(1), recalled.AccessCount)
	})

	t.Run("reinforces memory on recall", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{Content: "Reinforced content"}
		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)

		originalAccess := stored.LastAccessed
		time.Sleep(10 * time.Millisecond) // Small delay

		recalled, err := db.Recall(ctx, stored.ID)
		require.NoError(t, err)
		assert.Equal(t, int64(1), recalled.AccessCount)
		assert.True(t, recalled.LastAccessed.After(originalAccess) || recalled.LastAccessed.Equal(originalAccess))
	})

	t.Run("returns error for non-existent ID", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Recall(ctx, "non-existent-id")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns error for empty ID", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Recall(ctx, "")
		assert.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.Recall(ctx, "any-id")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestRemember(t *testing.T) {
	ctx := context.Background()

	t.Run("finds similar memories", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Store memories with embeddings
		mem1 := &Memory{
			Content:         "Dog running in park",
			ChunkEmbeddings: [][]float32{{1.0, 0.0, 0.0, 0.0}},
		}
		mem2 := &Memory{
			Content:         "Cat sleeping on couch",
			ChunkEmbeddings: [][]float32{{0.0, 1.0, 0.0, 0.0}},
		}
		mem3 := &Memory{
			Content:         "Puppy playing fetch",
			ChunkEmbeddings: [][]float32{{0.9, 0.1, 0.0, 0.0}},
		}

		_, err = db.Store(ctx, mem1)
		require.NoError(t, err)
		_, err = db.Store(ctx, mem2)
		require.NoError(t, err)
		_, err = db.Store(ctx, mem3)
		require.NoError(t, err)

		// Search for dog-like content
		query := []float32{0.95, 0.05, 0.0, 0.0}
		results, err := db.Remember(ctx, query, 2)
		require.NoError(t, err)
		require.Len(t, results, 2)

		// Top results should be dog-related
		contents := make([]string, len(results))
		for i, r := range results {
			contents[i] = r.Content
		}
		assert.Contains(t, contents, "Dog running in park")
		assert.Contains(t, contents, "Puppy playing fetch")
	})

	t.Run("respects limit", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Store multiple memories
		for i := 0; i < 5; i++ {
			mem := &Memory{
				Content:         "Memory content",
				ChunkEmbeddings: [][]float32{{0.5, 0.5, 0.0, 0.0}},
			}
			_, err = db.Store(ctx, mem)
			require.NoError(t, err)
		}

		results, err := db.Remember(ctx, []float32{0.5, 0.5, 0.0, 0.0}, 2)
		require.NoError(t, err)
		assert.Len(t, results, 2)
	})

	t.Run("returns empty for no matches", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Store memory without embedding
		mem := &Memory{Content: "No embedding"}
		_, err = db.Store(ctx, mem)
		require.NoError(t, err)

		results, err := db.Remember(ctx, []float32{1.0, 0.0}, 10)
		require.NoError(t, err)
		assert.Empty(t, results)
	})

	t.Run("returns error for empty embedding", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Remember(ctx, []float32{}, 10)
		assert.ErrorIs(t, err, ErrInvalidInput)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.Remember(ctx, []float32{1.0}, 10)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestLink(t *testing.T) {
	ctx := context.Background()

	t.Run("creates link between memories", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem1 := &Memory{Content: "Source"}
		mem2 := &Memory{Content: "Target"}

		stored1, err := db.Store(ctx, mem1)
		require.NoError(t, err)
		stored2, err := db.Store(ctx, mem2)
		require.NoError(t, err)

		edge, err := db.Link(ctx, stored1.ID, stored2.ID, "KNOWS", 0.9)
		require.NoError(t, err)
		require.NotNil(t, edge)

		assert.NotEmpty(t, edge.ID)
		assert.Equal(t, stored1.ID, edge.SourceID)
		assert.Equal(t, stored2.ID, edge.TargetID)
		assert.Equal(t, "KNOWS", edge.Type)
		assert.Equal(t, 0.9, edge.Confidence)
		assert.False(t, edge.AutoGenerated)
	})

	t.Run("uses default edge type", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem1, _ := db.Store(ctx, &Memory{Content: "A"})
		mem2, _ := db.Store(ctx, &Memory{Content: "B"})

		edge, err := db.Link(ctx, mem1.ID, mem2.ID, "", 0.5)
		require.NoError(t, err)
		assert.Equal(t, "RELATES_TO", edge.Type)
	})

	t.Run("normalizes confidence", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem1, _ := db.Store(ctx, &Memory{Content: "A"})
		mem2, _ := db.Store(ctx, &Memory{Content: "B"})

		edge, err := db.Link(ctx, mem1.ID, mem2.ID, "TEST", 0) // Invalid confidence
		require.NoError(t, err)
		assert.Equal(t, 1.0, edge.Confidence)

		mem3, _ := db.Store(ctx, &Memory{Content: "C"})
		edge2, err := db.Link(ctx, mem1.ID, mem3.ID, "TEST", 1.5) // Out of range
		require.NoError(t, err)
		assert.Equal(t, 1.0, edge2.Confidence)
	})

	t.Run("returns error for non-existent source", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem2, _ := db.Store(ctx, &Memory{Content: "Target"})

		_, err = db.Link(ctx, "non-existent", mem2.ID, "TEST", 1.0)
		assert.Error(t, err)
	})

	t.Run("returns error for non-existent target", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem1, _ := db.Store(ctx, &Memory{Content: "Source"})

		_, err = db.Link(ctx, mem1.ID, "non-existent", "TEST", 1.0)
		assert.Error(t, err)
	})

	t.Run("returns error for empty IDs", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Link(ctx, "", "target", "TEST", 1.0)
		assert.ErrorIs(t, err, ErrInvalidID)

		_, err = db.Link(ctx, "source", "", "TEST", 1.0)
		assert.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.Link(ctx, "a", "b", "TEST", 1.0)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestNeighbors(t *testing.T) {
	ctx := context.Background()

	t.Run("finds direct neighbors", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		memA, _ := db.Store(ctx, &Memory{Content: "A"})
		memB, _ := db.Store(ctx, &Memory{Content: "B"})
		memC, _ := db.Store(ctx, &Memory{Content: "C"})

		// A -> B, A -> C
		_, err = db.Link(ctx, memA.ID, memB.ID, "KNOWS", 1.0)
		require.NoError(t, err)
		_, err = db.Link(ctx, memA.ID, memC.ID, "KNOWS", 1.0)
		require.NoError(t, err)

		neighbors, err := db.Neighbors(ctx, memA.ID, 1, "")
		require.NoError(t, err)
		require.Len(t, neighbors, 2)

		contents := []string{neighbors[0].Content, neighbors[1].Content}
		assert.Contains(t, contents, "B")
		assert.Contains(t, contents, "C")
	})

	t.Run("finds incoming neighbors", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		memA, _ := db.Store(ctx, &Memory{Content: "A"})
		memB, _ := db.Store(ctx, &Memory{Content: "B"})

		// A -> B (so B has incoming from A)
		_, err = db.Link(ctx, memA.ID, memB.ID, "POINTS_TO", 1.0)
		require.NoError(t, err)

		neighbors, err := db.Neighbors(ctx, memB.ID, 1, "")
		require.NoError(t, err)
		require.Len(t, neighbors, 1)
		assert.Equal(t, "A", neighbors[0].Content)
	})

	t.Run("filters by edge type", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		memA, _ := db.Store(ctx, &Memory{Content: "A"})
		memB, _ := db.Store(ctx, &Memory{Content: "B"})
		memC, _ := db.Store(ctx, &Memory{Content: "C"})

		_, _ = db.Link(ctx, memA.ID, memB.ID, "KNOWS", 1.0)
		_, _ = db.Link(ctx, memA.ID, memC.ID, "LIKES", 1.0)

		neighbors, err := db.Neighbors(ctx, memA.ID, 1, "KNOWS")
		require.NoError(t, err)
		require.Len(t, neighbors, 1)
		assert.Equal(t, "B", neighbors[0].Content)
	})

	t.Run("traverses multiple hops", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		memA, _ := db.Store(ctx, &Memory{Content: "A"})
		memB, _ := db.Store(ctx, &Memory{Content: "B"})
		memC, _ := db.Store(ctx, &Memory{Content: "C"})

		// A -> B -> C
		_, _ = db.Link(ctx, memA.ID, memB.ID, "NEXT", 1.0)
		_, _ = db.Link(ctx, memB.ID, memC.ID, "NEXT", 1.0)

		// Depth 1 from A: just B
		neighbors1, err := db.Neighbors(ctx, memA.ID, 1, "")
		require.NoError(t, err)
		require.Len(t, neighbors1, 1)

		// Depth 2 from A: B and C
		neighbors2, err := db.Neighbors(ctx, memA.ID, 2, "")
		require.NoError(t, err)
		require.Len(t, neighbors2, 2)
	})

	t.Run("returns empty for isolated node", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		memA, _ := db.Store(ctx, &Memory{Content: "A"})

		neighbors, err := db.Neighbors(ctx, memA.ID, 1, "")
		require.NoError(t, err)
		assert.Empty(t, neighbors)
	})

	t.Run("returns error for empty ID", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Neighbors(ctx, "", 1, "")
		assert.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.Neighbors(ctx, "any-id", 1, "")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestForget(t *testing.T) {
	ctx := context.Background()

	t.Run("forgets memory", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem, _ := db.Store(ctx, &Memory{Content: "To forget"})

		err = db.Forget(ctx, mem.ID)
		require.NoError(t, err)

		// Should no longer be recallable
		_, err = db.Recall(ctx, mem.ID)
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("cleans up edges", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		memA, _ := db.Store(ctx, &Memory{Content: "A"})
		memB, _ := db.Store(ctx, &Memory{Content: "B"})
		_, _ = db.Link(ctx, memA.ID, memB.ID, "TEST", 1.0)

		// Forget A
		err = db.Forget(ctx, memA.ID)
		require.NoError(t, err)

		// B should have no neighbors now
		neighbors, err := db.Neighbors(ctx, memB.ID, 1, "")
		require.NoError(t, err)
		assert.Empty(t, neighbors)
	})

	t.Run("returns error for non-existent ID", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		err = db.Forget(ctx, "non-existent")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns error for empty ID", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		err = db.Forget(ctx, "")
		assert.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		err = db.Forget(ctx, "any-id")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestCypher(t *testing.T) {
	ctx := context.Background()

	t.Run("returns empty results for now", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		results, err := db.Cypher(ctx, "MATCH (n) RETURN n", nil)
		require.NoError(t, err)
		assert.NotNil(t, results)
		assert.Empty(t, results)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.Cypher(ctx, "MATCH (n) RETURN n", nil)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	assert.Equal(t, "./data", config.Database.DataDir)
	assert.Equal(t, "local", config.Memory.EmbeddingProvider)
	assert.Equal(t, "http://localhost:11434", config.Memory.EmbeddingAPIURL)
	assert.Equal(t, "bge-m3", config.Memory.EmbeddingModel)
	assert.Equal(t, 1024, config.Memory.EmbeddingDimensions)
	assert.False(t, config.Memory.DecayEnabled)
	assert.Equal(t, time.Hour, config.Memory.DecayInterval)
	assert.Equal(t, 0.05, config.Memory.ArchiveThreshold)
	assert.True(t, config.Memory.AutoLinksEnabled)
	assert.Equal(t, 0.82, config.Memory.AutoLinksSimilarityThreshold)
	assert.Equal(t, 7687, config.Server.BoltPort)
	assert.Equal(t, 7474, config.Server.HTTPPort)
}

func TestGenerateID(t *testing.T) {
	// Test that IDs are unique
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := generateID("test")
		// IDs are now UUIDs (prefix parameter is ignored for backward compatibility)
		assert.False(t, ids[id], "ID should be unique")
		// Verify it's a valid UUID format (8-4-4-4-12 hex digits)
		assert.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`, id, "ID should be a valid UUID")
		ids[id] = true
	}
}

func TestMemoryToNode(t *testing.T) {
	mem := &Memory{
		ID:              "test-123",
		Content:         "Test content",
		Title:           "Test Title",
		Tier:            TierProcedural,
		DecayScore:      0.8,
		CreatedAt:       time.Now(),
		LastAccessed:    time.Now(),
		AccessCount:     5,
		ChunkEmbeddings: [][]float32{{1.0, 2.0, 3.0}},
		Tags:            []string{"tag1", "tag2"},
		Source:          "test-source",
		Properties:      map[string]any{"custom": "value"},
	}

	node := memoryToNode(mem)

	assert.Equal(t, "test-123", string(node.ID))
	assert.Equal(t, []string{"Memory"}, node.Labels)
	assert.Equal(t, "Test content", node.Properties["content"])
	assert.Equal(t, "Test Title", node.Properties["title"])
	assert.Equal(t, "PROCEDURAL", node.Properties["tier"])
	assert.Equal(t, [][]float32{{1.0, 2.0, 3.0}}, node.ChunkEmbeddings)
	assert.Equal(t, "test-source", node.Properties["source"])
	assert.Equal(t, "value", node.Properties["custom"])
}

func TestNodeToMemory(t *testing.T) {
	now := time.Now()
	node := &storage.Node{
		ID:              "node-123",
		Labels:          []string{"Memory"},
		CreatedAt:       now,
		ChunkEmbeddings: [][]float32{{1.0, 2.0}},
		Properties: map[string]any{
			"content":       "Node content",
			"title":         "Node Title",
			"tier":          "EPISODIC",
			"decay_score":   0.7,
			"last_accessed": now.Format(time.RFC3339),
			"access_count":  int64(3),
			"source":        "node-source",
			"tags":          []string{"a", "b"},
			"custom_prop":   "custom_value",
		},
	}

	mem := nodeToMemory(node)

	assert.Equal(t, "node-123", mem.ID)
	assert.Equal(t, "Node content", mem.Content)
	assert.Equal(t, "Node Title", mem.Title)
	assert.Equal(t, TierEpisodic, mem.Tier)
	assert.Equal(t, 0.7, mem.DecayScore)
	assert.Equal(t, [][]float32{{1.0, 2.0}}, mem.ChunkEmbeddings)
	assert.Equal(t, "node-source", mem.Source)
	assert.Equal(t, []string{"a", "b"}, mem.Tags)
	assert.Equal(t, "custom_value", mem.Properties["custom_prop"])
}

func TestStorageEdgeToEdge(t *testing.T) {
	now := time.Now()
	se := &storage.Edge{
		ID:            "edge-1",
		StartNode:     "node-a",
		EndNode:       "node-b",
		Type:          "RELATES_TO",
		Confidence:    0.91,
		AutoGenerated: true,
		CreatedAt:     now,
		Properties: map[string]interface{}{
			"reason": "semantic overlap",
			"weight": 3,
		},
	}

	edge := storageEdgeToEdge(se)
	require.NotNil(t, edge)
	assert.Equal(t, "edge-1", edge.ID)
	assert.Equal(t, "node-a", edge.SourceID)
	assert.Equal(t, "node-b", edge.TargetID)
	assert.Equal(t, "RELATES_TO", edge.Type)
	assert.Equal(t, 0.91, edge.Confidence)
	assert.True(t, edge.AutoGenerated)
	assert.Equal(t, now, edge.CreatedAt)
	assert.Equal(t, "semantic overlap", edge.Reason)
	assert.Equal(t, 3, edge.Properties["weight"])

	// Missing/non-string reason should keep Reason empty.
	se2 := &storage.Edge{
		ID:         "edge-2",
		StartNode:  "x",
		EndNode:    "y",
		Type:       "LINKS",
		Properties: map[string]interface{}{"reason": 123},
	}
	edge2 := storageEdgeToEdge(se2)
	require.NotNil(t, edge2)
	assert.Equal(t, "", edge2.Reason)
}

func TestCosineSimilarity(t *testing.T) {
	t.Run("identical vectors", func(t *testing.T) {
		a := []float32{1.0, 0.0, 0.0}
		b := []float32{1.0, 0.0, 0.0}
		sim := vector.CosineSimilarity(a, b)
		assert.InDelta(t, 1.0, sim, 0.001)
	})

	t.Run("orthogonal vectors", func(t *testing.T) {
		a := []float32{1.0, 0.0, 0.0}
		b := []float32{0.0, 1.0, 0.0}
		sim := vector.CosineSimilarity(a, b)
		assert.InDelta(t, 0.0, sim, 0.001)
	})

	t.Run("opposite vectors", func(t *testing.T) {
		a := []float32{1.0, 0.0, 0.0}
		b := []float32{-1.0, 0.0, 0.0}
		sim := vector.CosineSimilarity(a, b)
		assert.InDelta(t, -1.0, sim, 0.001)
	})

	t.Run("similar vectors", func(t *testing.T) {
		a := []float32{1.0, 0.0, 0.0}
		b := []float32{0.9, 0.1, 0.0}
		sim := vector.CosineSimilarity(a, b)
		assert.Greater(t, sim, 0.9)
	})

	t.Run("different lengths returns 0", func(t *testing.T) {
		a := []float32{1.0, 0.0}
		b := []float32{1.0, 0.0, 0.0}
		sim := vector.CosineSimilarity(a, b)
		assert.Equal(t, 0.0, sim)
	})

	t.Run("empty vectors returns 0", func(t *testing.T) {
		sim := vector.CosineSimilarity([]float32{}, []float32{})
		assert.Equal(t, 0.0, sim)
	})

	t.Run("zero vectors returns 0", func(t *testing.T) {
		a := []float32{0.0, 0.0}
		b := []float32{0.0, 0.0}
		sim := vector.CosineSimilarity(a, b)
		assert.Equal(t, 0.0, sim)
	})
}

func TestSqrt(t *testing.T) {
	// Tests now use math.Sqrt (standard library) instead of custom implementation
	t.Run("positive values", func(t *testing.T) {
		assert.InDelta(t, 2.0, math.Sqrt(4.0), 0.001)
		assert.InDelta(t, 3.0, math.Sqrt(9.0), 0.001)
		assert.InDelta(t, 1.414, math.Sqrt(2.0), 0.01)
	})

	t.Run("zero", func(t *testing.T) {
		assert.Equal(t, 0.0, math.Sqrt(0.0))
	})

	t.Run("negative returns NaN", func(t *testing.T) {
		// math.Sqrt returns NaN for negative values (standard behavior)
		assert.True(t, math.IsNaN(math.Sqrt(-1.0)))
	})
}

func TestDecayIntegration(t *testing.T) {
	ctx := context.Background()

	t.Run("decay affects recall score", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{
			Content: "Decaying memory",
			Tier:    TierEpisodic,
		}

		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)
		assert.Equal(t, 1.0, stored.DecayScore)

		// Multiple recalls should reinforce the memory
		for i := 0; i < 3; i++ {
			_, err = db.Recall(ctx, stored.ID)
			require.NoError(t, err)
		}

		recalled, err := db.Recall(ctx, stored.ID)
		require.NoError(t, err)
		assert.Equal(t, int64(4), recalled.AccessCount)
	})
}

func TestTierPromotionIntegration(t *testing.T) {
	ctx := context.Background()

	t.Run("episodic memory promotes to semantic on frequent access", func(t *testing.T) {
		config := &Config{
			Memory: nornicConfig.MemoryConfig{
				DecayEnabled:     true,
				DecayInterval:    30 * time.Minute,
				ArchiveThreshold: 0.01,
			},
		}
		db, err := Open(t.TempDir(), config)
		require.NoError(t, err)
		defer db.Close()

		// Create episodic memory
		mem := &Memory{
			Content: "Frequently accessed episodic memory",
			Tier:    TierEpisodic,
		}

		// Store with creation time in the past to meet age requirement
		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)
		assert.Equal(t, TierEpisodic, stored.Tier)

		// Manually set CreatedAt to meet age requirement (hack for testing)
		// In real usage, time passes naturally
		node, err := db.storage.GetNode(storage.NodeID(stored.ID))
		require.NoError(t, err)
		node.CreatedAt = time.Now().Add(-2 * 24 * time.Hour) // Set directly on node
		err = db.storage.UpdateNode(node)
		require.NoError(t, err)

		// Recall multiple times to meet default access threshold (10)
		for i := 0; i < 10; i++ {
			recalled, err := db.Recall(ctx, stored.ID)
			require.NoError(t, err)
			assert.NotEmpty(t, recalled.Tier)
		}
	})

	t.Run("semantic memory promotes to procedural on very frequent access", func(t *testing.T) {
		config := &Config{
			Memory: nornicConfig.MemoryConfig{
				DecayEnabled:     true,
				DecayInterval:    30 * time.Minute,
				ArchiveThreshold: 0.01,
			},
		}
		db, err := Open(t.TempDir(), config)
		require.NoError(t, err)
		defer db.Close()

		// Create semantic memory
		mem := &Memory{
			Content: "Very frequently accessed semantic memory",
			Tier:    TierSemantic,
		}

		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)
		assert.Equal(t, TierSemantic, stored.Tier)

		// Manually set CreatedAt to meet age requirement
		node, err := db.storage.GetNode(storage.NodeID(stored.ID))
		require.NoError(t, err)
		node.CreatedAt = time.Now().Add(-3 * 24 * time.Hour) // Set directly on node
		err = db.storage.UpdateNode(node)
		require.NoError(t, err)

		// Recall multiple times to meet default procedural threshold (50)
		for i := 0; i < 50; i++ {
			recalled, err := db.Recall(ctx, stored.ID)
			require.NoError(t, err)
			assert.NotEmpty(t, recalled.Tier)
		}
	})

	t.Run("promotion disabled does not change tier", func(t *testing.T) {
		config := &Config{
			Memory: nornicConfig.MemoryConfig{
				DecayEnabled: true,
			},
		}
		db, err := Open(t.TempDir(), config)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{
			Content: "Memory with promotion disabled",
			Tier:    TierEpisodic,
		}

		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)

		// Set age requirement
		node, err := db.storage.GetNode(storage.NodeID(stored.ID))
		require.NoError(t, err)
		node.Properties["_createdAt"] = time.Now().Add(-2 * 24 * time.Hour).Format(time.RFC3339)
		err = db.storage.UpdateNode(node)
		require.NoError(t, err)

		// Recall many times
		for i := 0; i < 20; i++ {
			recalled, err := db.Recall(ctx, stored.ID)
			require.NoError(t, err)
			// Should remain episodic
			assert.Equal(t, TierEpisodic, recalled.Tier, "Should not promote when disabled")
		}
	})

	t.Run("procedural tier does not promote further", func(t *testing.T) {
		config := &Config{
			Memory: nornicConfig.MemoryConfig{
				DecayEnabled: true,
			},
		}
		db, err := Open(t.TempDir(), config)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{
			Content: "Procedural memory",
			Tier:    TierProcedural,
		}

		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)
		assert.Equal(t, TierProcedural, stored.Tier)

		// Recall many times
		for i := 0; i < 100; i++ {
			recalled, err := db.Recall(ctx, stored.ID)
			require.NoError(t, err)
			// Should remain procedural (highest tier)
			assert.Equal(t, TierProcedural, recalled.Tier, "Procedural is highest tier")
		}
	})

	t.Run("promotion updates decay score", func(t *testing.T) {
		config := &Config{
			Memory: nornicConfig.MemoryConfig{
				DecayEnabled: true,
			},
		}
		db, err := Open(t.TempDir(), config)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{
			Content: "Memory for decay score test",
			Tier:    TierEpisodic,
		}

		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)

		// Set age requirement
		node, err := db.storage.GetNode(storage.NodeID(stored.ID))
		require.NoError(t, err)
		node.CreatedAt = time.Now().Add(-2 * 24 * time.Hour) // Set directly on node
		err = db.storage.UpdateNode(node)
		require.NoError(t, err)

		// Recall to trigger promotion
		for i := 0; i < 12; i++ {
			recalled, err := db.Recall(ctx, stored.ID)
			require.NoError(t, err)

			if recalled.Tier == TierSemantic {
				// After promotion, decay score should be recalculated with new tier
				// Semantic tier has slower decay, so score may improve
				assert.Greater(t, recalled.DecayScore, 0.0, "Decay score should be valid")
			}
		}

		// Final recall should be promoted from episodic and have valid score.
		final, err := db.Recall(ctx, stored.ID)
		require.NoError(t, err)
		assert.NotEmpty(t, final.Tier)
		assert.Greater(t, final.DecayScore, 0.0)
		assert.LessOrEqual(t, final.DecayScore, 1.0)
	})
}

func TestWithoutDecay(t *testing.T) {
	ctx := context.Background()

	t.Run("works without decay manager", func(t *testing.T) {
		config := &Config{
			Memory: nornicConfig.MemoryConfig{
				DecayEnabled:     false,
				AutoLinksEnabled: false,
			},
		}
		db, err := Open(t.TempDir(), config)
		require.NoError(t, err)
		defer db.Close()

		mem := &Memory{Content: "No decay"}
		stored, err := db.Store(ctx, mem)
		require.NoError(t, err)

		recalled, err := db.Recall(ctx, stored.ID)
		require.NoError(t, err)
		assert.Equal(t, stored.Content, recalled.Content)
		assert.Equal(t, int64(1), recalled.AccessCount)
	})
}

// =============================================================================
// HTTP Server Interface Tests (Stats, ExecuteCypher, Node/Edge CRUD)
// =============================================================================

func TestStats(t *testing.T) {
	ctx := context.Background()

	t.Run("returns initial stats", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		stats := db.Stats()
		assert.GreaterOrEqual(t, stats.NodeCount, int64(0))
		assert.GreaterOrEqual(t, stats.EdgeCount, int64(0))
	})

	t.Run("updates after storing", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		initialStats := db.Stats()

		_, err = db.Store(ctx, &Memory{Content: "stats test"})
		require.NoError(t, err)

		stats := db.Stats()
		assert.GreaterOrEqual(t, stats.NodeCount, initialStats.NodeCount)
	})
}

func TestExecuteCypher(t *testing.T) {
	ctx := context.Background()

	t.Run("executes match query", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		result, err := db.ExecuteCypher(ctx, "MATCH (n) RETURN n LIMIT 10", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.NotNil(t, result.Columns)
	})

	t.Run("executes with parameters", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		params := map[string]interface{}{"name": "test"}
		result, err := db.ExecuteCypher(ctx, "MATCH (n) WHERE n.name = $name RETURN n", params)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.ExecuteCypher(ctx, "MATCH (n) RETURN n", nil)
		assert.ErrorIs(t, err, ErrClosed)
	})

	t.Run("creates and queries nodes", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create a node via Cypher
		_, err = db.ExecuteCypher(ctx, "CREATE (n:TestPerson {name: 'TestAlice'}) RETURN n", nil)
		require.NoError(t, err)

		// Query it back
		result, err := db.ExecuteCypher(ctx, "MATCH (n:TestPerson) RETURN n.name", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})
}

func TestBootstrapCanonicalSchema(t *testing.T) {
	t.Run("enforces canonical constraints", func(t *testing.T) {
		tmpDir := t.TempDir()
		db, err := Open(tmpDir, nil)
		require.NoError(t, err)
		defer db.Close()

		ctx := context.Background()
		require.NoError(t, db.BootstrapCanonicalSchema(ctx))
		require.NoError(t, db.BootstrapCanonicalSchema(ctx), "bootstrap should be idempotent")

		_, err = db.ExecuteCypher(ctx, "CREATE (:Memory {tier: 'SEMANTIC'})", nil)
		require.Error(t, err)

		_, err = db.ExecuteCypher(ctx, "CREATE (:Memory {id: 'mem-1', content: 'hello', tier: 'SEMANTIC', decay_score: 1.0, last_accessed: '2024-01-01T00:00:00Z', access_count: 'bad'})", nil)
		require.Error(t, err)

		_, err = db.ExecuteCypher(ctx, "CREATE (:Memory {id: 'mem-1', content: 'hello', tier: 'SEMANTIC', decay_score: 1.0, last_accessed: '2024-01-01T00:00:00Z', access_count: 0})", nil)
		require.NoError(t, err)

		_, err = db.ExecuteCypher(ctx, "CREATE (:Memory {id: 'mem-1', content: 'hello', tier: 'SEMANTIC', decay_score: 1.0, last_accessed: '2024-01-01T00:00:00Z', access_count: 0})", nil)
		require.Error(t, err)
	})

	t.Run("returns ErrClosed for closed database", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		require.NoError(t, db.Close())

		err = db.BootstrapCanonicalSchema(context.Background())
		require.ErrorIs(t, err, ErrClosed)
	})

	t.Run("returns error when schema manager is unavailable", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })

		db := &DB{
			config:  DefaultConfig(),
			storage: &nilSchemaEngine{Engine: base},
		}
		err := db.BootstrapCanonicalSchema(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "schema manager not initialized")
	})

	t.Run("fails when existing data violates canonical required constraints", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Existing invalid Memory node (missing required canonical properties) should fail bootstrap validation.
		_, err = db.ExecuteCypher(context.Background(), "CREATE (:Memory {id: 'bad-before-bootstrap'})", nil)
		require.NoError(t, err)

		err = db.BootstrapCanonicalSchema(context.Background())
		require.Error(t, err)
	})
}

// =============================================================================
// Node CRUD Tests
// =============================================================================

func TestListNodes(t *testing.T) {
	ctx := context.Background()

	t.Run("lists all nodes", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create some nodes
		db.CreateNode(ctx, []string{"TestListPerson"}, map[string]interface{}{"name": "Alice"})
		db.CreateNode(ctx, []string{"TestListPerson"}, map[string]interface{}{"name": "Bob"})

		nodes, err := db.ListNodes(ctx, "TestListPerson", 100, 0)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(nodes), 2)
	})

	t.Run("filters by label", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		db.CreateNode(ctx, []string{"FilterTestPerson"}, map[string]interface{}{"name": "Alice"})
		db.CreateNode(ctx, []string{"FilterTestItem"}, map[string]interface{}{"name": "Book"})

		nodes, err := db.ListNodes(ctx, "FilterTestPerson", 100, 0)
		require.NoError(t, err)
		// All returned nodes should have FilterTestPerson label
		for _, n := range nodes {
			found := false
			for _, l := range n.Labels {
				if l == "FilterTestPerson" {
					found = true
					break
				}
			}
			assert.True(t, found, "all nodes should have FilterTestPerson label")
		}
	})

	t.Run("respects limit", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		for i := 0; i < 10; i++ {
			db.CreateNode(ctx, []string{"LimitTest"}, map[string]interface{}{"i": i})
		}

		nodes, err := db.ListNodes(ctx, "LimitTest", 3, 0)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(nodes), 3)
	})

	t.Run("respects offset", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		for i := 0; i < 5; i++ {
			db.CreateNode(ctx, []string{"OffsetTest"}, map[string]interface{}{"i": i})
		}

		nodes, err := db.ListNodes(ctx, "OffsetTest", 100, 2)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(nodes), 0)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.ListNodes(ctx, "", 100, 0)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestGetNode(t *testing.T) {
	ctx := context.Background()

	t.Run("gets existing node", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		created, err := db.CreateNode(ctx, []string{"GetNodeTest"}, map[string]interface{}{"name": "TestGetNode"})
		require.NoError(t, err)

		node, err := db.GetNode(ctx, created.ID)
		require.NoError(t, err)
		assert.Equal(t, created.ID, node.ID)
	})

	t.Run("returns error for non-existent node", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.GetNode(ctx, "nonexistent-node-id-xyz")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.GetNode(ctx, "test-id")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestCreateNode(t *testing.T) {
	ctx := context.Background()

	t.Run("creates node with labels", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		node, err := db.CreateNode(ctx, []string{"CreateTest", "Employee"}, map[string]interface{}{"name": "CreateAlice"})
		require.NoError(t, err)
		assert.NotEmpty(t, node.ID)
		assert.Len(t, node.Labels, 2)
	})

	t.Run("creates node with properties", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		props := map[string]interface{}{
			"name":  "PropAlice",
			"age":   30,
			"email": "alice@example.com",
		}
		node, err := db.CreateNode(ctx, []string{"PropTest"}, props)
		require.NoError(t, err)
		assert.Equal(t, "PropAlice", node.Properties["name"])
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.CreateNode(ctx, []string{"Test"}, nil)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestUpdateNode(t *testing.T) {
	ctx := context.Background()

	t.Run("updates existing node", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		created, err := db.CreateNode(ctx, []string{"UpdateTest"}, map[string]interface{}{"name": "OriginalName"})
		require.NoError(t, err)

		updated, err := db.UpdateNode(ctx, created.ID, map[string]interface{}{"name": "UpdatedName", "age": 30})
		require.NoError(t, err)
		assert.Equal(t, "UpdatedName", updated.Properties["name"])
		assert.Equal(t, 30, updated.Properties["age"])
	})

	t.Run("returns error for non-existent node", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.UpdateNode(ctx, "nonexistent-update-id", map[string]interface{}{"name": "test"})
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.UpdateNode(ctx, "test-id", nil)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestDeleteNode(t *testing.T) {
	ctx := context.Background()

	t.Run("deletes existing node", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		created, err := db.CreateNode(ctx, []string{"DeleteTest"}, map[string]interface{}{"name": "ToDelete"})
		require.NoError(t, err)

		err = db.DeleteNode(ctx, created.ID)
		require.NoError(t, err)

		// Verify it's deleted
		_, err = db.GetNode(ctx, created.ID)
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		err = db.DeleteNode(ctx, "test-id")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

// =============================================================================
// Edge CRUD Tests
// =============================================================================

func TestListEdges(t *testing.T) {
	ctx := context.Background()

	t.Run("lists all edges", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		n1, _ := db.CreateNode(ctx, []string{"EdgeListTest"}, map[string]interface{}{"name": "EdgeAlice"})
		n2, _ := db.CreateNode(ctx, []string{"EdgeListTest"}, map[string]interface{}{"name": "EdgeBob"})

		db.CreateEdge(ctx, n1.ID, n2.ID, "TEST_KNOWS", nil)

		edges, err := db.ListEdges(ctx, "TEST_KNOWS", 100, 0)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(edges), 1)
	})

	t.Run("filters by type", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		n1, _ := db.CreateNode(ctx, []string{"EdgeFilterTest"}, nil)
		n2, _ := db.CreateNode(ctx, []string{"EdgeFilterTest"}, nil)

		db.CreateEdge(ctx, n1.ID, n2.ID, "FILTER_TYPE_A", nil)
		db.CreateEdge(ctx, n1.ID, n2.ID, "FILTER_TYPE_B", nil)

		edges, err := db.ListEdges(ctx, "FILTER_TYPE_A", 100, 0)
		require.NoError(t, err)
		for _, e := range edges {
			assert.Equal(t, "FILTER_TYPE_A", e.Type)
		}
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.ListEdges(ctx, "", 100, 0)
		assert.ErrorIs(t, err, ErrClosed)
	})

	t.Run("applies offset and limit", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		n1, _ := db.CreateNode(ctx, []string{"EdgePageTest"}, nil)
		n2, _ := db.CreateNode(ctx, []string{"EdgePageTest"}, nil)

		for i := 0; i < 4; i++ {
			_, _ = db.CreateEdge(ctx, n1.ID, n2.ID, "PAGE_TYPE", map[string]interface{}{"i": i})
		}

		edges, err := db.ListEdges(ctx, "PAGE_TYPE", 2, 1)
		require.NoError(t, err)
		require.Len(t, edges, 2)
	})
}

func TestGetEdge(t *testing.T) {
	ctx := context.Background()

	t.Run("gets existing edge", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		n1, _ := db.CreateNode(ctx, []string{"GetEdgeTest"}, nil)
		n2, _ := db.CreateNode(ctx, []string{"GetEdgeTest"}, nil)
		created, err := db.CreateEdge(ctx, n1.ID, n2.ID, "GET_EDGE_TEST", nil)
		require.NoError(t, err)

		edge, err := db.GetEdge(ctx, created.ID)
		require.NoError(t, err)
		assert.Equal(t, created.ID, edge.ID)
		assert.Equal(t, "GET_EDGE_TEST", edge.Type)
	})

	t.Run("returns error for non-existent edge", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.GetEdge(ctx, "nonexistent-edge-id")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.GetEdge(ctx, "test-id")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestCreateEdge(t *testing.T) {
	ctx := context.Background()

	t.Run("creates edge between nodes", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		n1, _ := db.CreateNode(ctx, []string{"CreateEdgeTest"}, map[string]interface{}{"name": "EdgeAlice"})
		n2, _ := db.CreateNode(ctx, []string{"CreateEdgeTest"}, map[string]interface{}{"name": "EdgeBob"})

		edge, err := db.CreateEdge(ctx, n1.ID, n2.ID, "CREATE_EDGE_TEST", map[string]interface{}{"since": 2020})
		require.NoError(t, err)
		assert.NotEmpty(t, edge.ID)
		assert.Equal(t, n1.ID, edge.Source)
		assert.Equal(t, n2.ID, edge.Target)
	})

	t.Run("returns error for non-existent source", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		n2, _ := db.CreateNode(ctx, []string{"EdgeSrcTest"}, nil)

		_, err = db.CreateEdge(ctx, "nonexistent-source", n2.ID, "TEST", nil)
		assert.Error(t, err)
	})

	t.Run("returns error for non-existent target", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		n1, _ := db.CreateNode(ctx, []string{"EdgeTgtTest"}, nil)

		_, err = db.CreateEdge(ctx, n1.ID, "nonexistent-target", "TEST", nil)
		assert.Error(t, err)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.CreateEdge(ctx, "a", "b", "KNOWS", nil)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestDeleteEdge(t *testing.T) {
	ctx := context.Background()

	t.Run("deletes existing edge", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		n1, _ := db.CreateNode(ctx, []string{"DeleteEdgeTest"}, nil)
		n2, _ := db.CreateNode(ctx, []string{"DeleteEdgeTest"}, nil)
		created, err := db.CreateEdge(ctx, n1.ID, n2.ID, "TO_DELETE", nil)
		require.NoError(t, err)

		err = db.DeleteEdge(ctx, created.ID)
		require.NoError(t, err)

		// Verify it's deleted
		_, err = db.GetEdge(ctx, created.ID)
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		err = db.DeleteEdge(ctx, "test-id")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

// =============================================================================
// Search Tests
// =============================================================================

func TestSearch(t *testing.T) {
	ctx := context.Background()

	t.Run("finds matching nodes", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		db.CreateNode(ctx, []string{"SearchTest"}, map[string]interface{}{"name": "SearchableAlice", "bio": "Software engineer"})
		db.CreateNode(ctx, []string{"SearchTest"}, map[string]interface{}{"name": "SearchableBob", "bio": "Product manager"})

		results, err := db.Search(ctx, "searchable", nil, 10)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(results), 1)
	})

	t.Run("filters by labels", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		db.CreateNode(ctx, []string{"SearchLabelPerson"}, map[string]interface{}{"desc": "labeltest expert"})
		db.CreateNode(ctx, []string{"SearchLabelCompany"}, map[string]interface{}{"desc": "labeltest company"})

		results, err := db.Search(ctx, "labeltest", []string{"SearchLabelPerson"}, 10)
		require.NoError(t, err)
		// Results should only contain SearchLabelPerson nodes
		for _, r := range results {
			found := false
			for _, l := range r.Node.Labels {
				if l == "SearchLabelPerson" {
					found = true
					break
				}
			}
			assert.True(t, found, "result should have SearchLabelPerson label")
		}
	})

	t.Run("case insensitive", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		db.CreateNode(ctx, []string{"CaseTest"}, map[string]interface{}{"text": "UniqueSearchTerm123"})

		results, err := db.Search(ctx, "uniquesearchterm123", nil, 10)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(results), 1)
	})

	t.Run("respects limit", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		for i := 0; i < 10; i++ {
			db.CreateNode(ctx, []string{"LimitSearchTest"}, map[string]interface{}{"text": "limitsearchcontent"})
		}

		results, err := db.Search(ctx, "limitsearchcontent", []string{"LimitSearchTest"}, 3)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(results), 3)
	})

	t.Run("returns empty for no matches", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		results, err := db.Search(ctx, "xyznonexistent123456", nil, 10)
		require.NoError(t, err)
		assert.Empty(t, results)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.Search(ctx, "test", nil, 10)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestFindSimilar(t *testing.T) {
	ctx := context.Background()

	t.Run("finds similar nodes by embedding", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create nodes with embeddings via Store
		mem1, _ := db.Store(ctx, &Memory{
			Content:         "similar test memory 1",
			ChunkEmbeddings: [][]float32{{1.0, 0.0, 0.0}},
		})
		db.Store(ctx, &Memory{
			Content:         "similar test memory 2",
			ChunkEmbeddings: [][]float32{{0.9, 0.1, 0.0}}, // Similar
		})

		results, err := db.FindSimilar(ctx, mem1.ID, 10)
		require.NoError(t, err)
		// May or may not find similar depending on other data in testdata
		assert.NotNil(t, results)
	})

	t.Run("maintains top-k order and replacement logic", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		root, err := db.Store(ctx, &Memory{
			Content:         "root",
			ChunkEmbeddings: [][]float32{{1.0, 0.0}},
		})
		require.NoError(t, err)

		low, err := db.Store(ctx, &Memory{
			Content:         "low",
			ChunkEmbeddings: [][]float32{{0.2, 0.8}},
		})
		require.NoError(t, err)

		high, err := db.Store(ctx, &Memory{
			Content:         "high",
			ChunkEmbeddings: [][]float32{{0.98, 0.02}},
		})
		require.NoError(t, err)

		_, err = db.CreateNode(ctx, []string{"NoEmbed"}, map[string]interface{}{"name": "ignored"})
		require.NoError(t, err)

		results, err := db.FindSimilar(ctx, root.ID, 1)
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, high.ID, results[0].Node.ID)
		require.NotEqual(t, low.ID, results[0].Node.ID)
	})

	t.Run("returns error for non-existent node", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.FindSimilar(ctx, "nonexistent-similar-id", 10)
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns error for node without embedding", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create node without embedding
		node, _ := db.CreateNode(ctx, []string{"NoEmbedTest"}, map[string]interface{}{"name": "no embedding"})

		_, err = db.FindSimilar(ctx, node.ID, 10)
		assert.Error(t, err)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.FindSimilar(ctx, "test-id", 10)
		assert.ErrorIs(t, err, ErrClosed)
	})

	t.Run("returns error for non-positive limit", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem, err := db.Store(ctx, &Memory{
			Content:         "has embedding",
			Tier:            TierSemantic,
			ChunkEmbeddings: [][]float32{{1, 0}},
		})
		require.NoError(t, err)

		_, err = db.FindSimilar(ctx, mem.ID, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "limit must be greater than 0")
	})

	t.Run("propagates context cancellation from streaming", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		mem1, _ := db.Store(ctx, &Memory{
			Content:         "cancel stream root",
			ChunkEmbeddings: [][]float32{{1.0, 0.0, 0.0}},
		})
		for i := 0; i < 32; i++ {
			_, _ = db.Store(ctx, &Memory{
				Content:         "cancel stream candidate",
				ChunkEmbeddings: [][]float32{{0.9, 0.1, 0.0}},
			})
		}

		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()
		_, err = db.FindSimilar(cancelCtx, mem1.ID, 5)
		require.Error(t, err)
		require.Contains(t, err.Error(), "context canceled")
	})

	t.Run("propagates streaming storage errors", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		ns := storage.NewNamespacedEngine(base, "test")
		_, err := ns.CreateNode(&storage.Node{
			ID:              "root",
			Labels:          []string{"Doc"},
			ChunkEmbeddings: [][]float32{{1.0, 0.0}},
			Properties:      map[string]interface{}{"content": "root"},
		})
		require.NoError(t, err)

		db := &DB{
			config:  DefaultConfig(),
			storage: &allNodesErrorEngine{Engine: ns, allNodesErr: errors.New("all nodes boom")},
		}

		_, err = db.FindSimilar(context.Background(), "root", 5)
		require.Error(t, err)
		require.Contains(t, err.Error(), "all nodes boom")
	})
}

func TestSearchAndHybridSearch_RetryWhileIndexBuilding(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	ns := storage.NewNamespacedEngine(base, "nornic")
	svc := search.NewService(ns) // Intentionally not pre-built.

	db := &DB{
		config:            DefaultConfig(),
		storage:           ns,
		baseStorage:       base,
		searchServices:    map[string]*dbSearchService{},
		inferenceServices: map[string]*inference.Engine{},
		buildCtx:          context.Background(),
	}
	db.searchServices["nornic"] = &dbSearchService{
		dbName:    "nornic",
		engine:    ns,
		svc:       svc,
		buildDone: make(chan struct{}),
	}

	results, err := db.Search(ctx, "missing", nil, 10)
	require.NoError(t, err)
	require.NotNil(t, results)

	hybrid, err := db.HybridSearch(ctx, "missing", nil, nil, 10)
	require.NoError(t, err)
	require.NotNil(t, hybrid)
}

func TestRemoveNodeFromSearchIndexes_WaitsForBuildCompletion(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	ns := storage.NewNamespacedEngine(base, "nornic")
	svc := search.NewServiceWithDimensions(ns, 3)
	require.NoError(t, svc.IndexNode(&storage.Node{
		ID:              storage.NodeID("wait-delete"),
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"content": "wait for build removal"},
		ChunkEmbeddings: [][]float32{{1, 2, 3}},
	}))

	entry := &dbSearchService{
		dbName:    "nornic",
		engine:    ns,
		svc:       svc,
		buildDone: make(chan struct{}),
	}
	entry.buildOnce.Do(func() {})

	db := &DB{
		config:            DefaultConfig(),
		storage:           ns,
		baseStorage:       base,
		searchServices:    map[string]*dbSearchService{"nornic": entry},
		inferenceServices: map[string]*inference.Engine{},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- db.removeNodeFromSearchIndexes(context.Background(), "nornic", ns, storage.NodeID("wait-delete"))
	}()

	select {
	case err := <-errCh:
		t.Fatalf("removeNodeFromSearchIndexes returned before build completion: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(entry.buildDone)
	require.NoError(t, <-errCh)
	require.Equal(t, 0, svc.EmbeddingCount())
}

func TestHybridSearch_InvalidUnnamespacedStoragePanics(t *testing.T) {
	db := &DB{
		config:            DefaultConfig(),
		searchServices:    map[string]*dbSearchService{},
		inferenceServices: map[string]*inference.Engine{},
	}

	require.Panics(t, func() {
		_, _ = db.HybridSearch(context.Background(), "query", []float32{0.1, 0.2}, nil, 5)
	})
}

func TestHybridSearch_ErrorBranches(t *testing.T) {
	t.Run("returns explicit error when cached service is nil", func(t *testing.T) {
		db, err := Open("", DefaultConfig())
		require.NoError(t, err)
		defer db.Close()

		dbName := db.defaultDatabaseName()
		db.searchServicesMu.Lock()
		db.searchServices[dbName] = &dbSearchService{dbName: dbName, svc: nil}
		db.searchServicesMu.Unlock()

		_, err = db.HybridSearch(context.Background(), "query", []float32{0.1, 0.2}, nil, 5)
		require.Error(t, err)
		require.Contains(t, err.Error(), "search service not initialized")
	})

	t.Run("handles canceled context without panic", func(t *testing.T) {
		db, err := Open("", DefaultConfig())
		require.NoError(t, err)
		defer db.Close()

		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()
		results, err := db.HybridSearch(canceledCtx, "missing", make([]float32, 1024), nil, 10)
		require.NoError(t, err)
		require.NotNil(t, results)
	})
}

// =============================================================================
// Schema Tests
// =============================================================================

func TestGetLabels(t *testing.T) {
	ctx := context.Background()

	t.Run("returns labels", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		db.CreateNode(ctx, []string{"LabelTestA"}, nil)
		db.CreateNode(ctx, []string{"LabelTestB"}, nil)

		labels, err := db.GetLabels(ctx)
		require.NoError(t, err)
		assert.NotNil(t, labels)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.GetLabels(ctx)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestGetRelationshipTypes(t *testing.T) {
	ctx := context.Background()

	t.Run("returns relationship types", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		n1, _ := db.CreateNode(ctx, []string{"RelTypeTest"}, nil)
		n2, _ := db.CreateNode(ctx, []string{"RelTypeTest"}, nil)

		db.CreateEdge(ctx, n1.ID, n2.ID, "REL_TYPE_A", nil)
		db.CreateEdge(ctx, n1.ID, n2.ID, "REL_TYPE_B", nil)

		types, err := db.GetRelationshipTypes(ctx)
		require.NoError(t, err)
		assert.NotNil(t, types)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.GetRelationshipTypes(ctx)
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestGetIndexes(t *testing.T) {
	ctx := context.Background()

	t.Run("returns indexes", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		indexes, err := db.GetIndexes(ctx)
		require.NoError(t, err)
		assert.NotNil(t, indexes)
	})
}

func TestCreateIndex(t *testing.T) {
	ctx := context.Background()

	t.Run("creates index without error", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		err = db.CreateIndex(ctx, "Person", "name", "btree")
		require.NoError(t, err)
	})
}

// =============================================================================
// Backup Tests
// =============================================================================

func TestBackup(t *testing.T) {
	ctx := context.Background()

	t.Run("backup succeeds", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Use a temp directory for backup output
		backupPath := filepath.Join(t.TempDir(), "backup-test")
		err = db.Backup(ctx, backupPath)
		require.NoError(t, err)
	})
}

func TestRestore(t *testing.T) {
	ctx := context.Background()

	t.Run("restore from backup", func(t *testing.T) {
		// Create source database with data
		sourceDB, err := Open(t.TempDir(), nil)
		require.NoError(t, err)

		// Create some nodes
		node1, err := sourceDB.CreateNode(ctx, []string{"Person"}, map[string]interface{}{
			"name": "Alice",
			"age":  30,
		})
		require.NoError(t, err)

		node2, err := sourceDB.CreateNode(ctx, []string{"Person"}, map[string]interface{}{
			"name": "Bob",
			"age":  25,
		})
		require.NoError(t, err)

		// Create edge
		_, err = sourceDB.CreateEdge(ctx, node1.ID, node2.ID, "KNOWS", map[string]interface{}{
			"since": "2024",
		})
		require.NoError(t, err)

		// Backup
		backupPath := filepath.Join(t.TempDir(), "backup.json")
		err = sourceDB.Backup(ctx, backupPath)
		require.NoError(t, err)
		sourceDB.Close()

		// Create new database and restore
		targetDB, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer targetDB.Close()

		err = targetDB.Restore(ctx, backupPath)
		require.NoError(t, err)

		// Verify nodes were restored
		restored1, err := targetDB.GetNode(ctx, node1.ID)
		require.NoError(t, err)
		assert.Equal(t, "Alice", restored1.Properties["name"])

		restored2, err := targetDB.GetNode(ctx, node2.ID)
		require.NoError(t, err)
		assert.Equal(t, "Bob", restored2.Properties["name"])
	})

	t.Run("restore with empty backup", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create empty backup
		backupPath := filepath.Join(t.TempDir(), "empty-backup.json")
		err = db.Backup(ctx, backupPath)
		require.NoError(t, err)

		// Restore should succeed with empty data
		err = db.Restore(ctx, backupPath)
		require.NoError(t, err)
	})

	t.Run("returns error when file not found", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		err = db.Restore(ctx, "/nonexistent/backup.json")
		assert.Error(t, err)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		err = db.Restore(ctx, "/any/path.json")
		assert.ErrorIs(t, err, ErrClosed)
	})

	t.Run("returns parse error for invalid backup json", func(t *testing.T) {
		base := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
		t.Cleanup(func() { _ = base.Close() })
		db := &DB{storage: base, config: DefaultConfig()}

		p := filepath.Join(t.TempDir(), "bad.json")
		require.NoError(t, os.WriteFile(p, []byte("{not-json"), 0644))
		err := db.Restore(ctx, p)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to parse backup")
	})

	t.Run("returns node restore error when create and update fail", func(t *testing.T) {
		base := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
		t.Cleanup(func() { _ = base.Close() })
		db := &DB{
			storage: &restoreFailEngine{
				Engine:        base,
				createNodeErr: errors.New("create node failed"),
				updateNodeErr: errors.New("update node failed"),
			},
			config: DefaultConfig(),
		}

		p := filepath.Join(t.TempDir(), "backup-node-fail.json")
		require.NoError(t, os.WriteFile(p, []byte(`{"version":"1.0","created_at":"2026-03-10T00:00:00Z","nodes":[{"id":"n1","labels":["L"],"properties":{}}],"edges":[]}`), 0644))

		err := db.Restore(ctx, p)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to restore node n1")
	})

	t.Run("returns edge restore error when create and update fail", func(t *testing.T) {
		base := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
		t.Cleanup(func() { _ = base.Close() })
		db := &DB{
			storage: &restoreFailEngine{
				Engine:        base,
				createEdgeErr: errors.New("create edge failed"),
				updateEdgeErr: errors.New("update edge failed"),
			},
			config: DefaultConfig(),
		}

		p := filepath.Join(t.TempDir(), "backup-edge-fail.json")
		require.NoError(t, os.WriteFile(p, []byte(`{"version":"1.0","created_at":"2026-03-10T00:00:00Z","nodes":[],"edges":[{"id":"e1","source":"n1","target":"n2","type":"REL","properties":{}}]}`), 0644))

		err := db.Restore(ctx, p)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to restore edge e1")
	})

	t.Run("updates existing nodes and edges when ids already exist", func(t *testing.T) {
		base := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
		t.Cleanup(func() { _ = base.Close() })
		db := &DB{
			storage:        base,
			config:         DefaultConfig(),
			searchServices: make(map[string]*dbSearchService),
		}

		_, err := base.CreateNode(&storage.Node{
			ID:     "n1",
			Labels: []string{"Person"},
			Properties: map[string]any{
				"name": "OldAlice",
			},
		})
		require.NoError(t, err)
		_, err = base.CreateNode(&storage.Node{
			ID:     "n2",
			Labels: []string{"Person"},
			Properties: map[string]any{
				"name": "Bob",
			},
		})
		require.NoError(t, err)
		require.NoError(t, base.CreateEdge(&storage.Edge{
			ID:        "e1",
			StartNode: "n1",
			EndNode:   "n2",
			Type:      "KNOWS",
			Properties: map[string]any{
				"since": "2020",
			},
		}))

		p := filepath.Join(t.TempDir(), "backup-update-existing.json")
		require.NoError(t, os.WriteFile(p, []byte(`{
			"version":"1.0",
			"created_at":"2026-03-10T00:00:00Z",
			"nodes":[
				{"id":"n1","labels":["Person"],"properties":{"name":"NewAlice"}},
				{"id":"n2","labels":["Person"],"properties":{"name":"Bob"}}
			],
			"edges":[
				{"id":"e1","startNode":"n1","endNode":"n2","type":"KNOWS","properties":{"since":"2026"}}
			]
		}`), 0644))

		require.NoError(t, db.Restore(ctx, p))

		n1, err := base.GetNode("n1")
		require.NoError(t, err)
		require.Equal(t, "NewAlice", n1.Properties["name"])

		e1, err := base.GetEdge("e1")
		require.NoError(t, err)
		require.Equal(t, "2026", e1.Properties["since"])
	})

	t.Run("rebuilds mvcc heads after restore", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = baseEngine.Close() })
		maint := &temporalMaintenanceTestEngine{Engine: baseEngine}
		db := &DB{
			storage:        storage.NewNamespacedEngine(maint, "nornic"),
			baseStorage:    maint,
			config:         DefaultConfig(),
			searchServices: make(map[string]*dbSearchService),
		}

		p := filepath.Join(t.TempDir(), "backup-mvcc-restore.json")
		require.NoError(t, os.WriteFile(p, []byte(`{
			"version":"1.0",
			"created_at":"2026-03-10T00:00:00Z",
			"nodes":[{"id":"n1","labels":["Person"],"properties":{"name":"Alice"}}],
			"edges":[]
		}`), 0644))

		require.NoError(t, db.Restore(ctx, p))
		require.Equal(t, 1, maint.mvccRebuildCalls)
	})
}

func TestDB_TemporalMaintenance(t *testing.T) {
	ctx := context.Background()

	t.Run("delegates rebuild and prune to base storage", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = baseEngine.Close() })
		maint := &temporalMaintenanceTestEngine{
			Engine:      baseEngine,
			pruneResult: 3,
		}
		db := &DB{
			storage:     storage.NewNamespacedEngine(maint, "nornic"),
			baseStorage: maint,
			config:      DefaultConfig(),
		}

		require.NoError(t, db.RebuildTemporalIndexes(ctx))
		deleted, err := db.PruneTemporalHistory(ctx, storage.TemporalPruneOptions{
			MaxVersionsPerKey: 2,
			MinRetentionAge:   24 * time.Hour,
		})
		require.NoError(t, err)
		require.Equal(t, 1, maint.rebuildCalls)
		require.Equal(t, 1, maint.pruneCalls)
		require.Equal(t, int64(3), deleted)
		require.Equal(t, 2, maint.lastPrune.MaxVersionsPerKey)
		require.Equal(t, 24*time.Hour, maint.lastPrune.MinRetentionAge)
	})

	t.Run("returns zero when maintenance is unsupported", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = baseEngine.Close() })
		db := &DB{
			storage:     storage.NewNamespacedEngine(baseEngine, "nornic"),
			baseStorage: baseEngine,
			config:      DefaultConfig(),
		}

		require.NoError(t, db.RebuildTemporalIndexes(ctx))
		deleted, err := db.PruneTemporalHistory(ctx, storage.TemporalPruneOptions{MaxVersionsPerKey: 1})
		require.NoError(t, err)
		require.Zero(t, deleted)
	})

	t.Run("returns ErrClosed", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = baseEngine.Close() })
		maint := &temporalMaintenanceTestEngine{Engine: baseEngine}
		db := &DB{
			storage:     storage.NewNamespacedEngine(maint, "nornic"),
			baseStorage: maint,
			config:      DefaultConfig(),
			closed:      true,
		}

		require.ErrorIs(t, db.RebuildTemporalIndexes(ctx), ErrClosed)
		_, err := db.PruneTemporalHistory(ctx, storage.TemporalPruneOptions{MaxVersionsPerKey: 1})
		require.ErrorIs(t, err, ErrClosed)
	})

	t.Run("lifecycle admin methods delegate to base storage", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = baseEngine.Close() })
		maint := &temporalMaintenanceTestEngine{
			Engine:          baseEngine,
			lifecycleStatus: map[string]interface{}{"enabled": true, "paused": false, "pressure_band": storage.PressureNormal},
		}
		db := &DB{
			storage:     storage.NewNamespacedEngine(maint, "nornic"),
			baseStorage: maint,
			config:      DefaultConfig(),
		}

		status, err := db.LifecycleStatus()
		require.NoError(t, err)
		require.Equal(t, true, status["enabled"])
		require.Equal(t, storage.PressureNormal, status["pressure_band"])

		require.NoError(t, db.TriggerLifecyclePrune(ctx))
		require.Equal(t, ctx, maint.lifecyclePruneCtx)
		require.NoError(t, db.PauseLifecycle())
		require.NoError(t, db.ResumeLifecycle())
		require.Equal(t, 1, maint.lifecyclePaused)
		require.Equal(t, 1, maint.lifecycleResumed)
	})

	t.Run("lifecycle admin methods fallback and closed handling", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = baseEngine.Close() })
		db := &DB{
			storage:     storage.NewNamespacedEngine(baseEngine, "nornic"),
			baseStorage: baseEngine,
			config:      DefaultConfig(),
		}

		status, err := db.LifecycleStatus()
		require.NoError(t, err)
		require.Equal(t, false, status["enabled"])
		require.NoError(t, db.TriggerLifecyclePrune(ctx))
		require.NoError(t, db.PauseLifecycle())
		require.NoError(t, db.ResumeLifecycle())

		db.closed = true
		_, err = db.LifecycleStatus()
		require.ErrorIs(t, err, ErrClosed)
		require.ErrorIs(t, db.TriggerLifecyclePrune(ctx), ErrClosed)
		require.ErrorIs(t, db.PauseLifecycle(), ErrClosed)
		require.ErrorIs(t, db.ResumeLifecycle(), ErrClosed)
	})
}

func TestDB_MVCCMaintenance(t *testing.T) {
	ctx := context.Background()

	t.Run("delegates rebuild and prune to base storage", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = baseEngine.Close() })
		maint := &temporalMaintenanceTestEngine{
			Engine:          baseEngine,
			mvccPruneResult: 4,
		}
		db := &DB{
			storage:     storage.NewNamespacedEngine(maint, "nornic"),
			baseStorage: maint,
			config:      DefaultConfig(),
		}

		require.NoError(t, db.RebuildMVCCHeads(ctx))
		deleted, err := db.PruneMVCCVersions(ctx, storage.MVCCPruneOptions{
			MaxVersionsPerKey: 2,
			MinRetentionAge:   12 * time.Hour,
		})
		require.NoError(t, err)
		require.Equal(t, 1, maint.mvccRebuildCalls)
		require.Equal(t, 1, maint.mvccPruneCalls)
		require.Equal(t, int64(4), deleted)
		require.Equal(t, 2, maint.lastMVCCPrune.MaxVersionsPerKey)
		require.Equal(t, 12*time.Hour, maint.lastMVCCPrune.MinRetentionAge)
	})

	t.Run("returns zero when maintenance is unsupported", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = baseEngine.Close() })
		db := &DB{
			storage:     storage.NewNamespacedEngine(baseEngine, "nornic"),
			baseStorage: baseEngine,
			config:      DefaultConfig(),
		}

		require.NoError(t, db.RebuildMVCCHeads(ctx))
		deleted, err := db.PruneMVCCVersions(ctx, storage.MVCCPruneOptions{MaxVersionsPerKey: 1})
		require.NoError(t, err)
		require.Zero(t, deleted)
	})

	t.Run("returns ErrClosed", func(t *testing.T) {
		baseEngine := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = baseEngine.Close() })
		maint := &temporalMaintenanceTestEngine{Engine: baseEngine}
		db := &DB{
			storage:     storage.NewNamespacedEngine(maint, "nornic"),
			baseStorage: maint,
			config:      DefaultConfig(),
			closed:      true,
		}

		require.ErrorIs(t, db.RebuildMVCCHeads(ctx), ErrClosed)
		_, err := db.PruneMVCCVersions(ctx, storage.MVCCPruneOptions{MaxVersionsPerKey: 1})
		require.ErrorIs(t, err, ErrClosed)
	})
}

// =============================================================================
// GDPR Compliance Tests
// =============================================================================

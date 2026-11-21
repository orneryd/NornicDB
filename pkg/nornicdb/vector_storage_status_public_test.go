package nornicdb

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// vectorStorageStatusJSON is the encoded form of the vector-storage section an
// operator reads from the existing database search status. Decoding it checks
// the transport contract rather than only the in-process struct.
type vectorStorageStatusJSON struct {
	VectorStorage struct {
		Configured    string `json:"configured"`
		Active        string `json:"active"`
		Reason        string `json:"reason"`
		ResidentBytes int64  `json:"resident_bytes"`
		LastRebuild   string `json:"last_rebuild_reason"`
	} `json:"vector_storage"`
}

// TestVectorStorageStatusViaDatabaseSearchStatus fails the real database
// search service's disk vector load for an existing service and reads the
// outcome through the public consumer. The fixture keeps the operator's
// explicit disk selection, so the database status must report the failed
// backing and its reason, and the encoded status must carry the same values
// instead of a successful disk or memory admission.
func TestVectorStorageStatusViaDatabaseSearchStatus(t *testing.T) {
	cfg := DefaultConfig()
	// Hold the startup warmup so this test's build is the database lifetime
	// owner's build, not a concurrent boot build of an empty database.
	cfg.DeferSearchWarmup = true
	cfg.Database.AsyncWritesEnabled = false
	cfg.Database.DataDir = t.TempDir()
	cfg.Database.PersistSearchIndexes = true
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.EmbeddingDimensions = 4
	db, err := Open(cfg.Database.DataDir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	// A database the startup warmup has not resolved, so this service is
	// constructed with the operator's explicit disk selection.
	dbName := "nor352status"
	engine := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), dbName)
	db.SetDbSearchOptionsResolver(func(name string) search.ServiceOptions {
		options := search.DefaultServiceOptions(name)
		if name == dbName {
			options.VectorStorageMode = "disk"
		}
		return options
	})
	svc, err := db.GetOrCreateSearchService(dbName, engine)
	require.NoError(t, err)
	_, _, _, _, configuredStorage := svc.IndexCapacityPolicy()
	require.Equal(t, "disk", configuredStorage)

	// The database's persistence owner names this generation; the configured
	// vector path is <state>/vectors. Save a valid payload through the public
	// store writer, then replace its committed metadata with a directory: a
	// real temporary-file read failure that is operational I/O, not derived
	// content the canonical rebuild policy may reject and rebuild.
	stateDir, err := db.activeSearchStateDir(dbName)
	require.NoError(t, err)
	base := filepath.Join(stateDir, "vectors")
	saved, err := search.NewVectorFileStore(base, 4)
	require.NoError(t, err)
	require.NoError(t, saved.Add("doc", []float32{1, 0, 0, 0}))
	require.NoError(t, saved.Save())
	require.NoError(t, saved.Close())
	require.NoError(t, os.Remove(base+".meta"))
	require.NoError(t, os.Mkdir(base+".meta", 0o700))

	_, err = db.EnsureSearchIndexes(t.Context(), dbName, engine, SearchIndexBuildWait)
	require.Error(t, err, "explicit disk must not acknowledge a memory replacement")
	require.Contains(t, err.Error(), "explicit vector disk storage")

	status := db.GetDatabaseSearchStatus(dbName)
	require.True(t, status.Initialized)
	require.Equal(t, "disk", status.VectorStorage.Configured)
	assert.Equal(t, "error", status.VectorStorage.Active,
		"explicit disk must report the failed backing instead of memory")
	assert.NotEmpty(t, status.VectorStorage.Reason)

	encoded, err := json.Marshal(status)
	require.NoError(t, err)
	var decoded vectorStorageStatusJSON
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Equal(t, "disk", decoded.VectorStorage.Configured)
	assert.Equal(t, "error", decoded.VectorStorage.Active)
	require.Equal(t, status.VectorStorage.Reason, decoded.VectorStorage.Reason)
	assert.NotEmpty(t, decoded.VectorStorage.Reason)
	require.Equal(t, status.VectorStorage.ResidentBytes, decoded.VectorStorage.ResidentBytes)
}

// TestBuildObstructedDiskVectorStatusViaDatabaseSearchStatus obstructs the
// parent of an explicit-disk vector destination with a regular file, so the
// build's population-artifact preparation cannot even create its private
// directory. The build must return that operational error, and the operator
// status must identify the failed disk configuration instead of reporting an
// unqualified, reasonless memory index that looks like a successful backing.
func TestBuildObstructedDiskVectorStatusViaDatabaseSearchStatus(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DeferSearchWarmup = true
	cfg.Database.AsyncWritesEnabled = false
	cfg.Database.DataDir = t.TempDir()
	cfg.Database.PersistSearchIndexes = true
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.EmbeddingDimensions = 4
	db, err := Open(cfg.Database.DataDir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	dbName := "nor352buildobstruction"
	engine := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), dbName)
	db.SetDbSearchOptionsResolver(func(name string) search.ServiceOptions {
		options := search.DefaultServiceOptions(name)
		if name == dbName {
			options.VectorStorageMode = "disk"
		}
		return options
	})
	svc, err := db.GetOrCreateSearchService(dbName, engine)
	require.NoError(t, err)
	_, _, _, _, configuredStorage := svc.IndexCapacityPolicy()
	require.Equal(t, "disk", configuredStorage)

	// A regular file where the destination's parent must be fails
	// deterministically even as root, without chmod assumptions.
	blocked := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blocked, []byte("path obstruction"), 0o600))
	svc.SetVectorIndexPath(filepath.Join(blocked, "vectors"))
	_, err = engine.CreateNode(t.Context(), &storage.Node{ID: "doc", Labels: []string{"Doc"},
		Properties: map[string]any{"embedding": []float32{1, 0, 0, 0}}})
	require.NoError(t, err)

	_, err = db.EnsureSearchIndexes(t.Context(), dbName, engine, SearchIndexBuildWait)
	require.Error(t, err, "the obstructed explicit-disk destination must fail the build")
	require.Contains(t, err.Error(), "not-a-directory",
		"the obstruction must surface as the operational filesystem error")

	status := db.GetDatabaseSearchStatus(dbName)
	require.True(t, status.Initialized)
	require.Equal(t, "failed", status.Phase, "the failed build must stay visible")
	require.NotEmpty(t, status.Error, "the operational build error must stay visible")
	require.Equal(t, "disk", status.VectorStorage.Configured)
	assert.Equal(t, "error", status.VectorStorage.Active,
		"explicit disk must report the failed backing, not an unqualified memory index")
	assert.NotEmpty(t, status.VectorStorage.Reason,
		"the failed disk configuration must stay operator-visible")

	encoded, err := json.Marshal(status)
	require.NoError(t, err)
	var decoded vectorStorageStatusJSON
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Equal(t, "disk", decoded.VectorStorage.Configured)
	assert.Equal(t, "error", decoded.VectorStorage.Active)
	require.Equal(t, status.VectorStorage.Reason, decoded.VectorStorage.Reason)
	assert.NotEmpty(t, decoded.VectorStorage.Reason)
}

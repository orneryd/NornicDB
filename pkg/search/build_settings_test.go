package search

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSearchBuildSettingsPath(t *testing.T) {
	p := searchBuildSettingsPath("/tmp/a/bm25", "", "")
	require.Equal(t, filepath.Join("/tmp/a", "build_settings"), p)

	p = searchBuildSettingsPath("", "/tmp/a/vectors", "")
	require.Equal(t, filepath.Join("/tmp/a", "build_settings"), p)

	p = searchBuildSettingsPath("", "", "/tmp/a/hnsw")
	require.Equal(t, filepath.Join("/tmp/a", "build_settings"), p)

	p = searchBuildSettingsPath("", "", "")
	require.Empty(t, p)
}

func TestSearchBuildSettingsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "build_settings")

	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 384)
	snap := svc.currentSearchBuildSettings()

	require.NoError(t, saveSearchBuildSettings(path, snap))
	got, err := loadSearchBuildSettings(path)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, snap.FormatVersion, got.FormatVersion)
	require.Equal(t, snap.BM25, got.BM25)
	require.Equal(t, snap.Vector, got.Vector)
	require.Equal(t, snap.HNSW, got.HNSW)
	require.Equal(t, snap.Routing, got.Routing)
}

func TestComposeRoutingBuildSettings_DoesNotEncodeSeedMode(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_ROUTING_MODE", "hybrid")
	t.Setenv("NORNICDB_KMEANS_MAX_ITERATIONS", "9")
	t.Setenv("NORNICDB_KMEANS_SEED_MODE", "none")

	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	routing := svc.composeRoutingBuildSettings()

	require.NotEmpty(t, routing)
	require.True(t, strings.Contains(routing, "kmeans_max_iter=9"))
	require.False(t, strings.Contains(routing, "kmeans_seed="))
}

func TestComposeHNSWBuildSettingsIncludesLexicalSeedMode(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	require.Contains(t, svc.composeHNSWBuildSettings(), "gpu_build=true")
	require.Contains(t, svc.composeHNSWBuildSettings(), "lexical_seeding=true")

	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_HNSW_LEXICAL_SEED_ENABLED", "false")
	require.Contains(t, svc.composeHNSWBuildSettings(), "gpu_build=false")
	require.Contains(t, svc.composeHNSWBuildSettings(), "lexical_seeding=false")
}

func TestComposeStrategyBuildSettings_CompressedIncludesFingerprint(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "compressed")
	t.Setenv("NORNICDB_VECTOR_IVF_LISTS", "64")
	t.Setenv("NORNICDB_VECTOR_PQ_SEGMENTS", "8")
	t.Setenv("NORNICDB_VECTOR_PQ_BITS", "4")

	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 64)
	strategy := svc.composeStrategyBuildSettings()
	require.True(t, strings.Contains(strategy, "quality=compressed"))
	require.True(t, strings.Contains(strategy, "lists=64"))
	require.True(t, strings.Contains(strategy, "segments=8"))
}

func TestLoadAndSaveSearchBuildSettings_EdgeBranches(t *testing.T) {
	t.Run("load with empty path returns nil", func(t *testing.T) {
		got, err := loadSearchBuildSettings("")
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("load with invalid msgpack returns nil without error", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "build_settings")
		require.NoError(t, os.WriteFile(path, []byte("not-msgpack"), 0o644))
		got, err := loadSearchBuildSettings(path)
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("load missing file returns nil without error", func(t *testing.T) {
		got, err := loadSearchBuildSettings(filepath.Join(t.TempDir(), "missing_build_settings"))
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("load with wrong format version returns nil", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "build_settings")
		require.NoError(t, saveSearchBuildSettings(path, searchBuildSettingsSnapshot{
			FormatVersion: searchBuildSettingsFormatVersion + 1,
			BM25:          "x",
			Vector:        "y",
			HNSW:          "z",
		}))
		got, err := loadSearchBuildSettings(path)
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("save with empty path is noop", func(t *testing.T) {
		err := saveSearchBuildSettings("", searchBuildSettingsSnapshot{FormatVersion: searchBuildSettingsFormatVersion})
		require.NoError(t, err)
	})

	t.Run("save returns parent creation error", func(t *testing.T) {
		dir := t.TempDir()
		parentFile := filepath.Join(dir, "not-a-dir")
		require.NoError(t, os.WriteFile(parentFile, []byte("x"), 0o644))

		err := saveSearchBuildSettings(filepath.Join(parentFile, "build_settings"), searchBuildSettingsSnapshot{FormatVersion: searchBuildSettingsFormatVersion})
		require.Error(t, err)
	})

	t.Run("save returns create error when target path is a directory", func(t *testing.T) {
		targetDir := filepath.Join(t.TempDir(), "build_settings")
		require.NoError(t, os.MkdirAll(targetDir, 0o755))

		err := saveSearchBuildSettings(targetDir, searchBuildSettingsSnapshot{FormatVersion: searchBuildSettingsFormatVersion})
		require.Error(t, err)
	})
}

func TestComposeRoutingBuildSettings_ClampsAndDefaults(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)

	t.Setenv("NORNICDB_KMEANS_MAX_ITERATIONS", "1")
	t.Setenv("NORNICDB_VECTOR_ROUTING_MODE", "")
	routing := svc.composeRoutingBuildSettings()
	require.Contains(t, routing, "mode=hybrid")
	require.Contains(t, routing, "kmeans_max_iter=5")

	t.Setenv("NORNICDB_KMEANS_MAX_ITERATIONS", "1000")
	t.Setenv("NORNICDB_VECTOR_ROUTING_MODE", "  lexical  ")
	routing = svc.composeRoutingBuildSettings()
	require.Contains(t, routing, "mode=lexical")
	require.Contains(t, routing, "kmeans_max_iter=500")
}

func TestBuildSettings_CurrentBM25AndPersistBranches(t *testing.T) {
	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 384)

	// Explicit v1 engine should use v1 format marker.
	svc.bm25Engine = BM25EngineV1
	require.Equal(t, fulltextIndexFormatVersion, svc.currentBM25FormatVersion())

	// Explicit v2 engine should use v2 marker.
	svc.bm25Engine = BM25EngineV2
	require.Equal(t, bm25V2FormatVersion, svc.currentBM25FormatVersion())

	// Empty paths return early.
	svc.persistSearchBuildSettings("", "", "")

	// Persist writes build settings metadata next to the selected index path.
	base := filepath.Join(t.TempDir(), "indexes")
	fulltext := filepath.Join(base, "bm25")
	svc.persistSearchBuildSettings(fulltext, "", "")
	_, err := os.Stat(filepath.Join(base, "build_settings"))
	require.NoError(t, err)

	// Error branch: parent path is not a directory; persist should swallow error.
	badParent := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(badParent, []byte("x"), 0o644))
	svc.persistSearchBuildSettings(filepath.Join(badParent, "bm25"), "", "")
}

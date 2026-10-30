package search

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
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

	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	svc := NewService(engine, 384, nil)
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

// An index built with different format settings must be rebuilt from storage.
func TestSearchBuildSettingsRebuildsMismatchedFulltextCache(t *testing.T) {
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	_, err := engine.CreateNode(&storage.Node{ID: "nornic:current", Properties: map[string]any{"text": "freshlyindexed"}})
	require.NoError(t, err)
	svc := NewService(engine, 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	svc.SetIndexFlags(true, false)
	path := filepath.Join(t.TempDir(), "bm25.v2")
	svc.SetFulltextIndexPath(path)
	svc.SetPersistenceEnabled(true)

	stale := NewFulltextIndexV2WithAnalyzer(nil)
	stale.Index("nornic:stale", "obsoleteonly", nil, nil, false)
	require.NoError(t, stale.Save(path))
	settings := svc.currentSearchBuildSettings()
	settings.BM25 = strings.Replace(settings.BM25, "format="+bm25V2FormatVersion, "format=other", 1)
	require.NoError(t, saveSearchBuildSettings(searchBuildSettingsPath(path, "", ""), settings))

	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.Len(t, svc.fulltext().Search("freshlyindexed", 10), 1)
	require.Empty(t, svc.fulltext().Search("obsoleteonly", 10))
}

func TestComposeHNSWBuildSettingsTracksGraphConfiguration(t *testing.T) {
	svc := NewService(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), 2, nil)
	require.Contains(t, svc.composeHNSWBuildSettings(), "gpu_build=true")

	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	require.Contains(t, svc.composeHNSWBuildSettings(), "gpu_build=false")
	require.NotContains(t, svc.composeHNSWBuildSettings(), "lexical_seeding")
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

func TestComposeBuildSettingsFingerprints(t *testing.T) {
	type step struct {
		env      [][2]string
		strategy bool
		notEmpty bool
		contains []string
		excludes []string
	}
	cases := []struct {
		name   string
		dims   int
		before [][2]string
		steps  []step
	}{
		{
			name: "routing excludes seed mode",
			dims: 2,
			before: [][2]string{
				{"NORNICDB_VECTOR_ROUTING_MODE", "hybrid"},
				{"NORNICDB_KMEANS_MAX_ITERATIONS", "9"},
				{"NORNICDB_KMEANS_SEED_MODE", "none"},
			},
			steps: []step{{notEmpty: true, contains: []string{"kmeans_max_iter=9"}, excludes: []string{"kmeans_seed="}}},
		},
		{
			name: "compressed strategy",
			dims: 64,
			before: [][2]string{
				{"NORNICDB_VECTOR_ANN_QUALITY", "compressed"},
				{"NORNICDB_VECTOR_IVF_LISTS", "64"},
				{"NORNICDB_VECTOR_PQ_SEGMENTS", "8"},
				{"NORNICDB_VECTOR_PQ_BITS", "4"},
			},
			steps: []step{{strategy: true, contains: []string{"quality=compressed", "lists=64", "segments=8"}}},
		},
		{
			name: "routing clamps and defaults",
			dims: 2,
			steps: []step{
				{env: [][2]string{{"NORNICDB_KMEANS_MAX_ITERATIONS", "1"}, {"NORNICDB_VECTOR_ROUTING_MODE", ""}},
					contains: []string{"mode=hybrid", "kmeans_max_iter=5"}},
				{env: [][2]string{{"NORNICDB_KMEANS_MAX_ITERATIONS", "1000"}, {"NORNICDB_VECTOR_ROUTING_MODE", "  lexical  "}},
					contains: []string{"mode=lexical", "kmeans_max_iter=500"}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, setting := range tc.before {
				t.Setenv(setting[0], setting[1])
			}
			svc := NewService(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), tc.dims, nil)
			for _, next := range tc.steps {
				for _, setting := range next.env {
					t.Setenv(setting[0], setting[1])
				}
				var got string
				if next.strategy {
					got = svc.composeStrategyBuildSettings()
				} else {
					got = svc.composeRoutingBuildSettings()
				}
				if next.notEmpty {
					require.NotEmpty(t, got)
				}
				for _, value := range next.contains {
					require.Contains(t, got, value)
				}
				for _, value := range next.excludes {
					require.NotContains(t, got, value)
				}
			}
		})
	}
}

func TestBuildSettings_BM25FormatAndPersistBranches(t *testing.T) {
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	svc := NewService(engine, 384, nil)

	// The single fulltext implementation stamps its own format marker.
	require.Contains(t, svc.composeBM25BuildSettings(), "format="+bm25V2FormatVersion)

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

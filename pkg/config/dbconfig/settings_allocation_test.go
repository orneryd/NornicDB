package dbconfig

import (
	"slices"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// settingLookupSink and resolveSink keep benchmark results observable without
// letting the compiler elide the work under test. Package-level globals are
// intentional: the benchmarks below are stable across runs.
var (
	settingLookupSink SettingDefinition
	resolveSink       *ResolvedDbConfig
)

// TestLookupSettingAllocationBudget pins the metadata-only allocation budget of
// LookupSetting: a plain number key allocates nothing, and an enum key allocates
// at most its fresh ValidValues copy so callers keep defensive ownership.
func TestLookupSettingAllocationBudget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		budget float64
	}{
		{"db.nornic.query_cache.max_entries", 0},
		{"NORNICDB_QUERY_CACHE_SIZE", 0},
		{"db.nornic.search.bm25.warming", 1},
	} {
		require.LessOrEqual(t, testing.AllocsPerRun(200, func() {
			settingLookupSink, _ = LookupSetting(tc.name)
		}), tc.budget, tc.name)
	}
}

// TestLookupSettingOwnsValidValuesCopy mutates the returned definition's
// ValidValues and requires the registry to be untouched and later lookups to
// hand back the restored copy.
func TestLookupSettingOwnsValidValuesCopy(t *testing.T) {
	name := "db.nornic.search.bm25.warming"
	owner, ok := LookupSetting(name)
	require.True(t, ok)
	want := append([]string(nil), owner.ValidValues...)
	require.NotEmpty(t, want)

	owner.ValidValues[0] = "mutated"
	require.Equal(t, want, registryValidValues(t, name))
	fresh, ok := LookupSetting(name)
	require.True(t, ok)
	require.Equal(t, want, fresh.ValidValues)
	require.NotSame(t, &owner.ValidValues[0], &fresh.ValidValues[0])
}

func registryValidValues(t *testing.T, name string) []string {
	t.Helper()
	for _, definition := range settingsRegistry {
		if definition.Name == name {
			return slices.Clone(definition.ValidValues)
		}
	}
	t.Fatalf("setting %s missing from settingsRegistry", name)
	return nil
}

// BenchmarkResolveWithCLIAndDatabaseOverrides measures a full resolve with both
// override sources fixed before the timer starts.
func BenchmarkResolveWithCLIAndDatabaseOverrides(b *testing.B) {
	global := config.LoadDefaults()
	global.CLIOverrides = map[string]string{
		"NORNICDB_SEARCH_BM25_ENABLED":  "false",
		"NORNICDB_EMBEDDING_DIMENSIONS": "1024",
	}
	overrides := map[string]string{
		"db.nornic.query_cache.max_entries":       "500",
		"db.nornic.search_result_cache.ttl":       "2m",
		"db.nornic.embedding.dimensions":          "768",
		"db.nornic.memory.storage.mode":           "low",
		"db.nornic.unknown.not.allowed":           "ignored",
		"db.nornic.memory.index.metadata.max":     "3m",
		"db.nornic.index.vector.storage":          "disk",
		"db.nornic.search.bm25.warming":           "lazy",
		"db.nornic.search.vector.warming":         "lazy",
		"db.nornic.search.min.similarity":         "0.5",
		"db.nornic.search.bm25.stemmer":           "none",
		"db.memory.transaction.total.max":         "2m",
		"db.nornic.max.entries.not.registered.at": "1",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		resolveSink = Resolve(global, overrides)
	}
}

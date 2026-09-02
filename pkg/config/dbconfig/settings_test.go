package dbconfig

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestParseByteSize(t *testing.T) {
	tests := map[string]int64{
		"0":    0,
		"512":  512,
		"1k":   1 << 10,
		"2M":   2 << 20,
		"3 g ": 3 << 30,
		"1t":   1 << 40,
	}
	for input, expected := range tests {
		actual, err := ParseByteSize(input)
		require.NoError(t, err, input)
		require.Equal(t, expected, actual, input)
	}

	for _, input := range []string{"", "-1", "1kb", "1.5m", "overflow999999999999999999999t"} {
		_, err := ParseByteSize(input)
		require.Error(t, err, input)
	}
}

func TestSettingDefinitionsActivationAndScope(t *testing.T) {
	physical, ok := LookupSetting("db.nornic.memory.storage.mode")
	require.True(t, ok)
	require.Equal(t, ScopePhysicalEngine, physical.Scope)
	require.False(t, physical.Dynamic)
	require.Equal(t, RestartProcess, physical.RestartLevel)
	require.False(t, IsAllowedKey(physical.Name))

	for _, name := range []string{
		"db.nornic.memory.index.bm25.max",
		"db.nornic.memory.index.vector.max",
		"db.nornic.memory.index.metadata.max",
		"db.nornic.index.bm25.storage",
		"db.nornic.index.vector.storage",
	} {
		definition, found := LookupSetting(name)
		require.True(t, found, name)
		require.True(t, IsAllowedKey(name), name)
		require.Equal(t, ScopeDatabase, definition.Scope, name)
		require.False(t, definition.Dynamic, name)
		require.Equal(t, RestartProcess, definition.RestartLevel, name)
	}

	queryCache, ok := LookupSetting("db.nornic.query_cache.max_entries")
	require.True(t, ok)
	require.False(t, queryCache.Dynamic)
	require.Equal(t, RestartProcess, queryCache.RestartLevel)
	require.Equal(t, ScopeDatabase, queryCache.Scope)
	require.Equal(t, "NORNICDB_QUERY_CACHE_SIZE", queryCache.EnvironmentVariable)
}

func TestSettingDefinitionsDynamicMatchesRestartLevel(t *testing.T) {
	for _, definition := range Settings() {
		require.Equalf(t, definition.RestartLevel == RestartNone, definition.Dynamic,
			"%s has contradictory activation metadata", definition.Name)
		require.Equalf(t, definition.Dynamic, definition.HotReload != HotReloadNone,
			"%s must name a hot-reload applicator", definition.Name)
	}
}

func TestNormalizeSettingValue(t *testing.T) {
	tests := map[string]struct {
		name string
		want string
	}{
		"binary size": {"db.memory.transaction.total.max", "2097152"},
		"enum":        {"db.nornic.memory.storage.mode", "low"},
		"boolean":     {"NORNICDB_SEARCH_VECTOR_ENABLED", "true"},
	}

	inputs := map[string]string{
		"binary size": "2m",
		"enum":        " LOW ",
		"boolean":     "1",
	}
	for label, test := range tests {
		actual, err := NormalizeSettingValue(test.name, inputs[label])
		require.NoError(t, err)
		require.Equal(t, test.want, actual)
	}

	_, err := NormalizeSettingValue("db.nornic.memory.storage.mode", "tiny")
	require.Error(t, err)

	for _, value := range []string{"1.5", "NaN", "+Inf"} {
		_, err = NormalizeSettingValue("db.nornic.embedding.dimensions", value)
		require.Error(t, err, value)
	}
	for _, value := range []string{"NaN", "+Inf"} {
		_, err = NormalizeSettingValue("db.nornic.search.min.similarity", value)
		require.Error(t, err, value)
	}
}

func TestResolveSearchResultCachePolicy(t *testing.T) {
	resolved := Resolve(config.LoadDefaults(), map[string]string{
		"db.nornic.search_result_cache.max_entries": "0",
		"db.nornic.search_result_cache.ttl":         "2m",
	})
	require.Equal(t, 0, resolved.SearchResultCacheMaxEntries)
	require.Equal(t, 2*time.Minute, resolved.SearchResultCacheTTL)
}

func TestResolveIndexCapacityPolicy(t *testing.T) {
	resolved := Resolve(config.LoadDefaults(), map[string]string{
		"db.nornic.memory.index.bm25.max":     "1m",
		"db.nornic.memory.index.vector.max":   "2m",
		"db.nornic.memory.index.metadata.max": "3m",
		"db.nornic.index.bm25.storage":        "memory",
		"db.nornic.index.vector.storage":      "disk",
	})

	require.Equal(t, int64(1<<20), resolved.BM25MemoryMaxBytes)
	require.Equal(t, int64(2<<20), resolved.VectorMemoryMaxBytes)
	require.Equal(t, int64(3<<20), resolved.MetadataMemoryMaxBytes)
	require.Equal(t, "memory", resolved.BM25StorageMode)
	require.Equal(t, "disk", resolved.VectorStorageMode)
}

func TestQueryCacheTTLRequiresProcessRestart(t *testing.T) {
	definition, ok := LookupSetting("db.nornic.query_cache.ttl")
	require.True(t, ok)
	require.Equal(t, "number", definition.Type)
	require.Equal(t, "300000", definition.DefaultValue)
	require.Contains(t, definition.Description, "milliseconds")
	require.False(t, definition.Dynamic)
	require.Equal(t, RestartProcess, definition.RestartLevel)
	require.Equal(t, HotReloadNone, definition.HotReload)
}

func TestQueryCacheTTLAcceptsOnlyIntegerMilliseconds(t *testing.T) {
	value, err := NormalizeSettingValue("db.nornic.query_cache.ttl", "1800000")
	require.NoError(t, err)
	require.Equal(t, "1800000", value)

	for _, invalid := range []string{"30m", "2s", "1.5", "-1"} {
		_, err := NormalizeSettingValue("db.nornic.query_cache.ttl", invalid)
		require.Error(t, err, invalid)
	}
}

func TestEveryEnvironmentAlternativeHasExplicitCanonicalSetting(t *testing.T) {
	seen := make(map[string]string)
	for _, definition := range Settings() {
		if definition.EnvironmentVariable == "" {
			continue
		}
		canonical := CanonicalSettingName(definition.EnvironmentVariable)
		require.NotEqual(t, definition.EnvironmentVariable, canonical)
		require.Contains(t, canonical, ".")
		require.NotContains(t, canonical, "NORNICDB_")
		if previous, exists := seen[canonical]; exists {
			t.Fatalf("environment variables %s and %s map to duplicate canonical setting %s", previous, definition.EnvironmentVariable, canonical)
		}
		seen[canonical] = definition.EnvironmentVariable
		lookup, ok := LookupSetting(definition.EnvironmentVariable)
		require.True(t, ok, definition.EnvironmentVariable)
		require.Equal(t, canonical, lookup.Name)
		require.Equal(t, definition.EnvironmentVariable, lookup.EnvironmentVariable)
	}
}

func TestResolveCanonicalDatabaseSettingWinsEnvironmentAlternativeAndCLI(t *testing.T) {
	global := config.LoadDefaults()
	global.Memory.SearchBM25Enabled = true
	global.CLIOverrides = map[string]string{"NORNICDB_SEARCH_BM25_ENABLED": "false"}
	resolved := Resolve(global, map[string]string{
		"NORNICDB_SEARCH_BM25_ENABLED":  "false",
		"db.nornic.search.bm25.enabled": "true",
	})
	require.True(t, resolved.BM25Enabled)
	require.Equal(t, "true", resolved.Effective["db.nornic.search.bm25.enabled"])
	_, hasEnvironmentName := resolved.Effective["NORNICDB_SEARCH_BM25_ENABLED"]
	require.False(t, hasEnvironmentName)
}

func TestCanonicalSettingNameDoesNotInferUnknownEnvironmentVariable(t *testing.T) {
	require.Equal(t, "NORNICDB_NOT_A_REGISTERED_SETTING", CanonicalSettingName("NORNICDB_NOT_A_REGISTERED_SETTING"))
}

func TestDeprecatedNeo4jSettingNameIsNotAcceptedAsAlias(t *testing.T) {
	require.Equal(t, "server.db.query_cache_size", CanonicalSettingName("server.db.query_cache_size"))
	_, ok := LookupSetting("server.db.query_cache_size")
	require.False(t, ok)
}

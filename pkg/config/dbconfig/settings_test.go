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

	indexBudget, ok := LookupSetting("db.nornic.memory.index.vector.max")
	require.True(t, ok)
	require.Equal(t, ScopeDatabaseIndex, indexBudget.Scope)
	require.False(t, indexBudget.Dynamic)
	require.Equal(t, RestartDatabase, indexBudget.RestartLevel)
	require.True(t, IsAllowedKey(indexBudget.Name))

	queryCache, ok := LookupSetting("server.memory.query_cache.per_db_cache_num_entries")
	require.True(t, ok)
	require.True(t, queryCache.Dynamic)
	require.Equal(t, RestartNone, queryCache.RestartLevel)
}

func TestNormalizeSettingValue(t *testing.T) {
	tests := map[string]struct {
		name string
		want string
	}{
		"binary size": {"db.nornic.memory.index.bm25.max", "2097152"},
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
}

func TestResolveSearchResultCachePolicy(t *testing.T) {
	resolved := Resolve(config.LoadDefaults(), map[string]string{
		"db.nornic.search_result_cache.max_entries": "0",
		"db.nornic.query_cache.ttl":                 "2m",
	})
	require.Equal(t, 0, resolved.SearchResultCacheMaxEntries)
	require.Equal(t, 2*time.Minute, resolved.SearchResultCacheTTL)
}

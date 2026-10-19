package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Each validator caches only its own acceptance of a statement text: under
// NORNICDB_PARSER=antlr a repeated valid statement skips the ANTLR parse
// (#911), a text the grammar rejects is never cached as valid for it, and
// the Nornic validator's acceptance doesn't count for ANTLR.
func TestSyntaxValidationCachePerValidator(t *testing.T) {
	previous := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previous) })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	config.SetParserType(config.ParserTypeANTLR)
	valid := "MATCH (n:CacheProbe) RETURN n.id"
	require.NoError(t, exec.validateSyntax(context.Background(), valid, false))
	require.True(t, exec.hasCachedValidSyntax(valid, syntaxValidANTLR))
	require.False(t, exec.hasCachedValidSyntax(valid, syntaxValidNornic))
	require.NoError(t, exec.validateSyntax(context.Background(), valid, false), "served from the cache")

	broken := "MATCH (n:CacheProbe RETURN n"
	for range 2 {
		require.Error(t, exec.validateSyntax(context.Background(), broken, false))
	}
	require.False(t, exec.hasCachedValidSyntax(broken, syntaxValidANTLR))

	extension := "SHOW LIMITS"
	require.NoError(t, exec.validateSyntax(context.Background(), extension, false), "a NornicDB extension, checked by the Nornic validator")
	require.True(t, exec.hasCachedValidSyntax(extension, syntaxValidANTLR))

	config.SetParserType(config.ParserTypeNornic)
	other := "MATCH (n:CacheProbe) RETURN count(n)"
	require.NoError(t, exec.validateSyntax(context.Background(), other, false))
	require.True(t, exec.hasCachedValidSyntax(other, syntaxValidNornic))
	require.False(t, exec.hasCachedValidSyntax(other, syntaxValidANTLR))
	// The Nornic acceptance of a text ANTLR accepted adds to its entry.
	require.NoError(t, exec.validateSyntax(context.Background(), valid, false))
	require.True(t, exec.hasCachedValidSyntax(valid, syntaxValidNornic|syntaxValidANTLR))
	require.False(t, exec.hasCachedValidSyntax("", syntaxValidNornic))
	exec.markCachedValidSyntax("", syntaxValidNornic)

	// Each shard keeps at most its share of the limit, and marking a known
	// text for another validator adds no entry.
	cache := exec.ensureSyntaxValidationCache()
	cache.max = 3 * boundedCacheShardCount
	entries := func() int {
		total := 0
		for i := range cache.shards {
			require.LessOrEqual(t, len(cache.shards[i].cache), 3)
			total += len(cache.shards[i].cache)
		}
		return total
	}
	for i := range 3 {
		exec.markCachedValidSyntax(fmt.Sprintf("RETURN %d", i), syntaxValidNornic)
	}
	before := entries()
	exec.markCachedValidSyntax("RETURN 2", syntaxValidANTLR)
	require.Equal(t, before, entries())
	require.True(t, exec.hasCachedValidSyntax("RETURN 7", syntaxValidNornic), "a literal variant, through the canonical form")
	require.True(t, exec.hasCachedValidSyntax("RETURN 7", syntaxValidANTLR), "RETURN 2 was marked for ANTLR, canonical form included")
}

// Under ANTLR, a NornicDB extension statement the Nornic validator rejects
// is an error and isn't cached as valid.
func TestSyntaxValidationCacheRejectedExtension(t *testing.T) {
	previous := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previous) })
	config.SetParserType(config.ParserTypeANTLR)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	rejected := "SHOW LIMITS YIELD name WHERE name IN ['a',]"
	require.True(t, isNornicExtensionStatement(rejected))
	require.Error(t, exec.validateSyntax(context.Background(), rejected, false))
	require.False(t, exec.hasCachedValidSyntax(rejected, syntaxValidANTLR))
}

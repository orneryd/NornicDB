package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Statement acceptance has a shared fact on either parser. The grammar fact
// stays separate: an extension is not a grammar acceptance.
func TestSyntaxValidationCachePerValidator(t *testing.T) {
	previous := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previous) })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	config.SetParserType(config.ParserTypeANTLR)
	valid := "MATCH (n:CacheProbe) RETURN n.id"
	require.NoError(t, exec.validateStatementSyntax(context.Background(), valid, false))
	require.True(t, exec.hasCachedValidSyntax(valid, syntaxStatementAccepted))
	require.True(t, exec.hasCachedValidSyntax(valid, syntaxGrammarAccepted))
	require.NoError(t, exec.validateStatementSyntax(context.Background(), valid, false), "served from the cache")

	broken := "MATCH (n:CacheProbe RETURN n"
	for range 2 {
		require.Error(t, exec.validateStatementSyntax(context.Background(), broken, false))
	}
	require.False(t, exec.hasCachedValidSyntax(broken, syntaxStatementAccepted))
	require.False(t, exec.hasCachedValidSyntax(broken, syntaxGrammarAccepted))

	extension := "SHOW LIMITS"
	require.NoError(t, exec.validateStatementSyntax(context.Background(), extension, false), "a NornicDB extension, checked by the Nornic validator")
	require.True(t, exec.hasCachedValidSyntax(extension, syntaxStatementAccepted))
	require.False(t, exec.hasCachedValidSyntax(extension, syntaxGrammarAccepted))

	config.SetParserType(config.ParserTypeNornic)
	other := "MATCH (n:CacheProbe) RETURN count(n)"
	require.NoError(t, exec.validateStatementSyntax(context.Background(), other, false))
	require.True(t, exec.hasCachedValidSyntax(other, syntaxStatementAccepted))
	require.True(t, exec.hasCachedValidSyntax(other, syntaxGrammarAccepted))
	// Switching parsers can consume the same complete acceptance fact.
	require.NoError(t, exec.validateStatementSyntax(context.Background(), valid, false))
	require.True(t, exec.hasCachedValidSyntax(valid, syntaxStatementAccepted))
	require.False(t, exec.hasCachedValidSyntax("", syntaxStatementAccepted))
	exec.markCachedValidSyntax("", syntaxStatementAccepted)

	// Each shard keeps at most its share of the limit, and marking a known
	// text with another fact adds no entry.
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
		exec.markCachedValidSyntax(fmt.Sprintf("RETURN %d", i), syntaxStatementAccepted)
	}
	before := entries()
	exec.markCachedValidSyntax("RETURN 2", syntaxGrammarAccepted)
	require.Equal(t, before, entries())
	require.True(t, exec.hasCachedValidSyntax("RETURN 7", syntaxStatementAccepted), "a literal variant, through the canonical form")
	require.True(t, exec.hasCachedValidSyntax("RETURN 2", syntaxGrammarAccepted), "RETURN 2 was marked with the exact-text grammar fact")
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
	require.Error(t, exec.validateStatementSyntax(context.Background(), rejected, false))
	require.False(t, exec.hasCachedValidSyntax(rejected, syntaxStatementAccepted))
}

func TestSyntaxValidationCanceledStatementCacheHit(t *testing.T) {
	restoreParserTypeAfter(t)
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		config.SetParserType(parser)
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		const query = "RETURN 13 AS syntax_cached_value"
		require.NoError(t, exec.validateStatementSyntax(t.Context(), query, false))
		before := exec.cachedSyntaxVerdict(query)
		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, exec.validateStatementSyntax(canceled, query, false), context.Canceled)
		require.Equal(t, before, exec.cachedSyntaxVerdict(query))
		const uncached = "RETURN 'uncached syntax value' AS fresh_value"
		require.ErrorIs(t, exec.validateStatementSyntax(canceled, uncached, false), context.Canceled)
		require.Zero(t, exec.cachedSyntaxVerdict(uncached))
		require.NoError(t, exec.validateStatementSyntax(t.Context(), query, false))
		result, err := exec.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"syntax_cached_value"}, result.Columns)
		require.Equal(t, [][]interface{}{{int64(13)}}, result.Rows)
	}
}

func TestSyntaxValidationStatementRejectionIsNeverAccepted(t *testing.T) {
	restoreParserTypeAfter(t)
	config.SetParserType(config.ParserTypeANTLR)
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	const query = "RETURN 1 @"
	for range 2 {
		err := exec.validateStatementSyntax(t.Context(), query, false)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", errorStatusCode(err))
		require.False(t, exec.hasCachedValidSyntax(query, syntaxStatementAccepted))
		require.False(t, exec.hasCachedValidSyntax(query, syntaxGrammarAccepted))
	}
}

func TestSyntaxValidationNativeCoreDoesNotBorrowStatementGrammar(t *testing.T) {
	restoreParserTypeAfter(t)
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		config.SetParserType(parser)
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		const query = "SET n.status = 'active' RETURN n"
		// NornicDB's start rule answers before the grammar on either parser, as
		// the default parser always did. Neither verdict is cached or borrowed.
		err := exec.validateStatementSyntax(t.Context(), query, false)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", errorStatusCode(err))
		require.ErrorContains(t, err, "query must start with a valid clause")
		require.False(t, exec.hasCachedValidSyntax(query, syntaxStatementAccepted))
	}
}

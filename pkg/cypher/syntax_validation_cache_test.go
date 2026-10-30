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
	require.NoError(t, exec.validateSyntax(context.Background(), originalBraceView(valid), false, syntaxStatement))
	require.True(t, exec.hasCachedValidSyntax(valid, syntaxValidANTLR))
	require.False(t, exec.hasCachedValidSyntax(valid, syntaxValidNornic))
	require.NoError(t, exec.validateSyntax(context.Background(), originalBraceView(valid), false, syntaxStatement), "served from the cache")

	broken := "MATCH (n:CacheProbe RETURN n"
	for range 2 {
		require.Error(t, exec.validateSyntax(context.Background(), originalBraceView(broken), false, syntaxStatement))
	}
	require.False(t, exec.hasCachedValidSyntax(broken, syntaxValidANTLR))

	extension := "SHOW LIMITS"
	require.NoError(t, exec.validateSyntax(context.Background(), originalBraceView(extension), false, syntaxStatement), "a NornicDB extension, checked by the Nornic validator")
	require.True(t, exec.hasCachedValidSyntax(extension, syntaxValidANTLR))

	config.SetParserType(config.ParserTypeNornic)
	other := "MATCH (n:CacheProbe) RETURN count(n)"
	require.NoError(t, exec.validateSyntax(context.Background(), originalBraceView(other), false, syntaxStatement))
	require.True(t, exec.hasCachedValidSyntax(other, syntaxValidNornic))
	require.False(t, exec.hasCachedValidSyntax(other, syntaxValidANTLR))
	// The Nornic acceptance of a text ANTLR accepted adds to its entry.
	require.NoError(t, exec.validateSyntax(context.Background(), originalBraceView(valid), false, syntaxStatement))
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
	require.True(t, isNornicExtensionStatement(originalBraceView(rejected)))
	require.Error(t, exec.validateSyntax(context.Background(), originalBraceView(rejected), false, syntaxStatement))
	require.False(t, exec.hasCachedValidSyntax(rejected, syntaxValidANTLR))
}

// Isolated native measurements intentionally exclude statement-wide Unicode
// admission. Keep that slice independent of the configured runtime parser.
func TestSyntaxValidationNativeCoreScope(t *testing.T) {
	restoreParserTypeAfter(t)
	const query = "RETURN −1"
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		config.SetParserType(parser)
		core := NewStorageExecutor(newTestMemoryEngine(t))
		require.NoError(t, core.validateSyntax(t.Context(), originalBraceView(query), false, syntaxNativeCore))
		require.True(t, core.hasCachedValidSyntax(query, syntaxValidNornic))
		require.False(t, core.hasCachedValidSyntax(query, syntaxValidANTLR))
		statement := NewStorageExecutor(newTestMemoryEngine(t))
		err := statement.validateSyntax(t.Context(), originalBraceView(query), false, syntaxStatement)
		requireSemanticDetail(t, err, "InvalidUnicodeCharacter")
		require.False(t, statement.hasCachedValidSyntax(query, activeSyntaxValidator()))
	}
}

func TestSyntaxValidationCanceledStatementCacheHit(t *testing.T) {
	restoreParserTypeAfter(t)
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		config.SetParserType(parser)
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		const query = "RETURN 13 AS syntax_cached_value"
		require.NoError(t, exec.validateSyntax(t.Context(), originalBraceView(query), false, syntaxStatement))
		before := exec.cachedSyntaxVerdict(query)
		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, exec.validateSyntax(canceled, originalBraceView(query), false, syntaxStatement), context.Canceled)
		require.Equal(t, before, exec.cachedSyntaxVerdict(query))
		const uncached = "RETURN 'uncached syntax value' AS fresh_value"
		require.ErrorIs(t, exec.validateSyntax(canceled, originalBraceView(uncached), false, syntaxStatement), context.Canceled)
		require.Zero(t, exec.cachedSyntaxVerdict(uncached))
		require.NoError(t, exec.validateSyntax(t.Context(), originalBraceView(query), false, syntaxStatement))
		result, err := exec.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"syntax_cached_value"}, result.Columns)
		require.Equal(t, [][]interface{}{{int64(13)}}, result.Rows)
	}
}

func TestSyntaxValidationANTLRRejectionMarksOnlyNativeAcceptance(t *testing.T) {
	restoreParserTypeAfter(t)
	config.SetParserType(config.ParserTypeANTLR)
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	const query = "RETURN 1 @"
	for range 2 {
		err := exec.validateSyntax(t.Context(), originalBraceView(query), false, syntaxStatement)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", errorStatusCode(err))
		require.True(t, exec.hasCachedValidSyntax(query, syntaxValidNornic))
		require.False(t, exec.hasCachedValidSyntax(query, syntaxValidANTLR))
		require.False(t, exec.hasCachedValidSyntax(query, syntaxGrammarAccepted))
	}
}

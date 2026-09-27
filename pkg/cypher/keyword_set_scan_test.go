package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFirstKeywordIndexFromDefault: one scan for a keyword set finds the
// earliest keyword by the rules keywordIndex applies to each one alone.
func TestFirstKeywordIndexFromDefault(t *testing.T) {
	for _, text := range []string{
		"p RETURN p",
		"p, q MATCH (n) RETURN n",
		"p OPTIONAL MATCH (n) RETURN n",
		"p WHERE p.x > 1 RETURN p",
		"p DETACH DELETE p",
		"'MATCH' AS s RETURN s",
		"[x IN l WHERE x > 1] AS y RETURN y",
		"p /* RETURN */ SET p.a = 1",
		"1 AS set RETURN set",
		"p",
		"",
	} {
		want := -1
		for _, keyword := range withImportClauseKeywords {
			if index := keywordIndex(text, keyword); index >= 0 && (want < 0 || index < want) {
				want = index
			}
		}
		require.Equal(t, want, firstKeywordIndexFromDefault(text, 0, withImportClauseKeywords...), text)
	}
	require.Equal(t, -1, firstKeywordIndexFromDefault("RETURN 1", 0))
	require.Equal(t, 9, firstKeywordIndexFromDefault("RETURN 1 RETURN 2", 1, "RETURN"))
}

// TestParseLeadingWithImportsKeywordAtStart: a clause keyword at the start of
// the import list is part of it; the list ends at the next one.
func TestParseLeadingWithImportsKeywordAtStart(t *testing.T) {
	vars, body, hasWith, err := parseLeadingWithImports("WITH p RETURN p")
	require.NoError(t, err)
	require.True(t, hasWith)
	require.Equal(t, []string{"p"}, vars)
	require.Equal(t, "RETURN p", body)

	_, body, hasWith, err = parseLeadingWithImports("with p MATCH (n) RETURN n")
	require.NoError(t, err)
	require.True(t, hasWith)
	require.Equal(t, "MATCH (n) RETURN n", body)

	_, _, hasWith, err = parseLeadingWithImports("WITHIN RETURN 1")
	require.NoError(t, err)
	require.False(t, hasWith)
}

// TestCachedPipelineClausesVariants: one cached parse serves both splits; a
// text with a CALL of its own is unsupported without procedure calls,
// whichever split is asked for first.
func TestCachedPipelineClausesVariants(t *testing.T) {
	const withCall = "CALL db.labels() YIELD label RETURN label /* variants */"
	clauses, ok := splitPipelineClausesAllowingProcedureCalls(withCall)
	require.True(t, ok)
	require.Len(t, clauses, 2)
	_, ok = splitPipelineClauses(withCall)
	require.False(t, ok)

	const withCallFirst = "CALL db.labels() YIELD label RETURN label /* variants, other order */"
	_, ok = splitPipelineClauses(withCallFirst)
	require.False(t, ok)
	_, ok = splitPipelineClausesAllowingProcedureCalls(withCallFirst)
	require.True(t, ok)

	const plain = "MATCH (n) WITH n RETURN n /* variants */"
	first, ok := splitPipelineClauses(plain)
	require.True(t, ok)
	second, ok := splitPipelineClausesAllowingProcedureCalls(plain)
	require.True(t, ok)
	require.Equal(t, first, second)
}

// TestFindKeywordIndexInContextNonASCII: the position is the keyword's in the
// text as written, whatever characters come before it.
func TestFindKeywordIndexInContextNonASCII(t *testing.T) {
	text := "['ıı', 'ß'] AS x"
	require.Equal(t, len("['ıı', 'ß'] "), findKeywordIndexInContext(text, "AS"))
	list, alias, ok := splitUnwindBody(text)
	require.True(t, ok)
	require.Equal(t, "['ıı', 'ß']", list)
	require.Equal(t, "x", alias)
	require.Equal(t, 2, findKeywordIndexInContext("n as m", "AS"))
}

// TestStartsWithKeywordsMatchesScan: startsWithKeywords answers what
// findMultiWordKeywordIndex(...) == 0 answers, for statements that start
// with the words, with them later, as names, or after whitespace, comments
// or strings.
func TestStartsWithKeywordsMatchesScan(t *testing.T) {
	pairs := [][2]string{
		{"CREATE", "DATABASE"}, {"CREATE", "COMPOSITE DATABASE"}, {"DROP", "ALIAS"},
		{"SHOW", "DATABASES"}, {"SHOW", "CURRENT USER"}, {"ALTER", "DATABASE"},
		{"OPTIONAL", "MATCH"}, {"WITH", "x"}, {"SET", "n"}, {"DETACH", "DELETE"},
	}
	statements := []string{
		"CREATE DATABASE foo", "create   database foo", "CREATE\n\tDATABASE foo", "CREATE DATABASEX",
		"CREATE COMPOSITE DATABASE c", "CREATE COMPOSITE  DATABASE c", " CREATE DATABASE foo",
		"/* c */ CREATE DATABASE foo", "MATCH (n) CREATE DATABASE", "CREATE (n) RETURN n",
		"DROP ALIAS a FOR DATABASE b", "DROP ALIASES", "SHOW DATABASES", "SHOW DATABASES YIELD name",
		"SHOW CURRENT USER", "SHOW CURRENT  USER", "SHOW CURRENTUSER", "ALTER DATABASE d SET ACCESS READ ONLY",
		"OPTIONAL MATCH (n) RETURN n", "OPTIONAL  MATCH (n)", "optional match (n)", "OPTIONALMATCH",
		"WITH x RETURN x", "WITH x+1 AS y RETURN y", "SET n.a = 1", "SET n", "SET + n",
		"DETACH DELETE n", "detach  delete n", "'CREATE DATABASE x'", "`CREATE` DATABASE",
		"CREATE", "CREATE ", "", "CREATE DATABASE", "SHOW", "SHOW DATABASES,", "CREATE: DATABASE",
		"CREATE.DATABASE", "SET.n", "WITH\nx", "SHOW DATABASES//c", "ıı CREATE DATABASE x",
	}
	for _, statement := range statements {
		for _, pair := range pairs {
			want := findMultiWordKeywordIndex(statement, pair[0], pair[1]) == 0
			require.Equal(t, want, startsWithKeywords(statement, pair[0], pair[1]), "%q %v", statement, pair)
		}
	}
}

package antlr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Every word after a USE graph reference has exactly one owner: the shared
// grammar's query clauses, or Neo4j's remaining routing vocabulary.
func TestUseFollowerWordsHaveOneOwner(t *testing.T) {
	starts := queryClauseStarts()
	for _, word := range []string{"USE", "MATCH", "OPTIONAL", "UNWIND", "LET", "FILTER", "FOR", "CALL",
		"CREATE", "MERGE", "DETACH", "DELETE", "SET", "REMOVE", "FOREACH"} {
		require.Contains(t, starts, word)
	}
	seen := map[string]bool{}
	for _, list := range [][]string{useClauseFollowers[:], useAdministrationFollowers[:]} {
		for _, word := range list {
			require.NotContains(t, starts, word, "%s is owned by the grammar", word)
			require.False(t, seen[word], "%s is listed twice", word)
			seen[word] = true
			require.True(t, IsUseClauseFollower(word, false))
		}
	}
	for _, word := range useClauseFollowers {
		require.True(t, IsUseClauseFollower(word, true))
	}
	for _, word := range useAdministrationFollowers {
		require.False(t, IsUseClauseFollower(word, true), "%s is not a subquery follower", word)
	}
	require.False(t, IsUseClauseFollower("ALL", false))
	require.False(t, IsUseClauseFollower("GRANT", true))
}

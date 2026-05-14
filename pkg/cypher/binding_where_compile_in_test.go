package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCypherWhereIN_LiteralListMembership exercises the WHERE ... IN
// predicate compiler — specifically the makeCompiledBindingMembership
// path which builds an O(1) lookup set for comparable items and a
// fallback list for non-comparables (slices, maps). Without this
// regression test the predicate compiler's literal-list branch had
// no end-to-end coverage.
func TestCypherWhereIN_LiteralListMembership(t *testing.T) {
	e := setupTestExecutor(t)

	for i, name := range []string{"alice", "bob", "carol", "dave"} {
		createTestNode(t, e, "u-"+string(rune('1'+i)), []string{"User"},
			map[string]interface{}{"name": name, "age": int64(20 + i*5)})
	}

	t.Run("string IN literal list", func(t *testing.T) {
		result, err := e.Execute(context.Background(),
			"MATCH (u:User) WHERE u.name IN ['alice', 'carol'] RETURN u.name AS name", nil)
		require.NoError(t, err)
		gotNames := map[string]bool{}
		for _, row := range result.Rows {
			gotNames[row[0].(string)] = true
		}
		require.Equal(t, map[string]bool{"alice": true, "carol": true}, gotNames)
	})

	t.Run("integer IN literal list", func(t *testing.T) {
		result, err := e.Execute(context.Background(),
			"MATCH (u:User) WHERE u.age IN [20, 30] RETURN u.name AS name ORDER BY u.name", nil)
		require.NoError(t, err)
		gotNames := []string{}
		for _, row := range result.Rows {
			gotNames = append(gotNames, row[0].(string))
		}
		require.ElementsMatch(t, []string{"alice", "carol"}, gotNames)
	})

	t.Run("NOT IN excludes matches", func(t *testing.T) {
		result, err := e.Execute(context.Background(),
			"MATCH (u:User) WHERE u.name NOT IN ['alice', 'bob'] RETURN u.name AS name", nil)
		require.NoError(t, err)
		gotNames := []string{}
		for _, row := range result.Rows {
			gotNames = append(gotNames, row[0].(string))
		}
		require.ElementsMatch(t, []string{"carol", "dave"}, gotNames)
	})

	t.Run("empty list matches nothing", func(t *testing.T) {
		result, err := e.Execute(context.Background(),
			"MATCH (u:User) WHERE u.name IN [] RETURN u.name AS name", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 0)
	})

	t.Run("missing property is treated as not-in", func(t *testing.T) {
		// nodes have name; "missing" prop returns nil → IN check fails.
		result, err := e.Execute(context.Background(),
			"MATCH (u:User) WHERE u.missing IN ['alice'] RETURN u.name AS name", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 0)
	})
}

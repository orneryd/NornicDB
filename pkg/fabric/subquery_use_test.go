package fabric

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseAcceptedSubqueryUseClause(t *testing.T) {
	for _, tc := range []struct {
		name, body, graph, remaining string
		found                        bool
	}{
		{"leading", "USE neo4j WITH x RETURN x", "neo4j", "WITH x RETURN x", true},
		{"importing", "WITH x USE neo4j RETURN x", "neo4j", "WITH x RETURN x", true},
		{"quoted imports", "WITH `use`, x USE `db-name` RETURN x", "db-name", "WITH `use`, x RETURN x", true},
		{"no use", " WITH x RETURN x ", "", " WITH x RETURN x ", false},
		{"name prefix", "WITH a USEא MATCH (n) RETURN n", "", "WITH a USEא MATCH (n) RETURN n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			use, remaining, found, err := ParseAcceptedSubqueryUseClause(t.Context(), tc.body, nil)
			require.NoError(t, err)
			require.Equal(t, tc.found, found)
			require.Equal(t, tc.graph, use.Name)
			require.Equal(t, tc.remaining, remaining)
		})
	}
	t.Run("dynamic", func(t *testing.T) {
		use, remaining, found, err := ParseAcceptedSubqueryUseClause(t.Context(), "WITH g USE graph.byName(g) RETURN g", nil)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "graph.byName", use.Function)
		require.Equal(t, []string{"g"}, use.Args)
		require.Equal(t, "WITH g RETURN g", remaining)
	})
	t.Run("syntax error", func(t *testing.T) {
		_, _, _, err := ParseAcceptedSubqueryUseClause(t.Context(), "WITH x USE", nil)
		var syntaxErr *UseSyntaxError
		require.ErrorAs(t, err, &syntaxErr)
	})
	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, _, _, err := ParseAcceptedSubqueryUseClause(ctx, "WITH x RETURN x", nil)
		require.ErrorIs(t, err, context.Canceled)
	})
}

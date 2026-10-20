package fabric

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNativeDeclarationProjectionIdentity(t *testing.T) {
	for _, sample := range []struct{ syntax, name string }{
		{"plain", "plain"}, {"`quoted`", "quoted"}, {"`name,part`", "name,part"}, {"`name``part`", "name`part"}, {"```edge```", "`edge`"},
	} {
		for _, query := range []string{
			"CALL db.labels() YIELD label AS " + sample.syntax + " RETURN *",
			"WITH 7 AS value CALL db.labels() YIELD `label` AS " + sample.syntax + " RETURN *",
			"RETURN 7 AS " + sample.syntax,
			"WITH 7 AS value CALL db.labels() YIELD label AS " + sample.syntax + " RETURN " + sample.syntax + " UNION ALL RETURN 8 AS " + sample.syntax,
		} {
			q := ReadQueryStructure(query)
			require.NoError(t, q.Err, query)
			require.Equal(t, []string{sample.name}, q.DeclaredColumns(), query)
		}
		projection := ProjectionSyntax("'AS, fake' /* AS discarded */ AS " + sample.syntax)
		require.NotNil(t, projection)
		require.Len(t, projection.Items, 1)
		require.Equal(t, sample.name, projection.Items[0].Column)
		require.Equal(t, sample.name, projection.Items[0].Alias)
		require.True(t, projection.Items[0].HasAlias)
		require.Greater(t, projection.Items[0].AliasStart, 0)
	}
	q := ReadQueryStructure("CALL () { CALL db.labels() YIELD label AS hidden RETURN * } RETURN *")
	require.NoError(t, q.Err)
	require.Nil(t, q.YieldClause())
	require.Nil(t, q.DeclaredColumns())
}

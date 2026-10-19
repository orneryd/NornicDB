package fabric

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestFabricGrammarRuleOwnership(t *testing.T) {
	for _, name := range []string{"union", "use", "return", "with", "create", "call", "not", "all", "distinct"} {
		text := "WITH 7 AS " + name + " WITH DISTINCT " + name + " CALL { WITH " + name + " USE comp.one RETURN " + name + " AS k } RETURN k"
		q := ReadQueryStructure(text)
		require.NoError(t, q.Err, text)
		require.Len(t, q.Clauses, 4, text)
		require.Equal(t, "WITH", q.Clauses[1].Kind)
		require.Equal(t, name, q.Clauses[1].Projection.Items[0].Name)
		require.Equal(t, "CALL", q.Clauses[2].Kind)
		require.Len(t, q.Clauses[2].Nested.Clauses, 3)
		require.False(t, q.Writes())
	}
	q := ReadQueryStructure("RETURN v ORDER /* UNION CALL RETURN */ BY v DESC LIMIT 1")
	require.NoError(t, q.Err)
	require.Equal(t, "1", q.Clauses[0].Projection.Limit)
	require.Len(t, q.Clauses[0].Projection.Order, 1)
	require.True(t, q.Clauses[0].Projection.Order[0].Desc)
	q = ReadQueryStructure("CALL { USE comp.one RETURN 1 AS k } IN /* USE */ TRANSACTIONS OF 10 ROWS RETURN k")
	require.NoError(t, q.Err)
	require.True(t, q.Clauses[0].InTransactions)
}

func TestFabricStructureRenderingSourceSpans(t *testing.T) {
	for _, text := range []string{"  WITH 7 AS union RETURN coalesce(union,0) /* CALL */", "RETURN 'USE UNION' AS k ORDER\n/* WITH */ BY k", "NODETACH DELETE n"} {
		q := ReadQueryStructure(text)
		require.NoError(t, q.Err)
		var out strings.Builder
		offset := 0
		for _, edit := range q.LocalEdits {
			require.GreaterOrEqual(t, edit.Start, offset)
			out.WriteString(text[offset:edit.Start])
			out.WriteString(edit.Text)
			offset = edit.End
		}
		out.WriteString(text[offset:])
		require.Equal(t, q.LocalText, out.String(), "structural rendering and its source map agree")
	}
}

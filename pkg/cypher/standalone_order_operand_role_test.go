package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStandaloneOrderRetainsSortOperandRole(t *testing.T) {
	for _, name := range []string{"call", "match", "finish"} {
		projection := "WITH 7 AS " + name + " RETURN " + name + " ORDER BY " + name + " DESC SKIP 0 LIMIT 1"
		got, rewrite := desugarStandaloneOrderClauses(projection)
		require.Equal(t, projection, got, name)
		require.Nil(t, rewrite, name)
		standalone := "UNWIND [7] AS " + name + " ORDER BY " + name + " DESC SKIP 0 LIMIT 1 RETURN " + name
		got, rewrite = desugarStandaloneOrderClauses(standalone)
		require.Equal(t, "UNWIND [7] AS "+name+" WITH * ORDER BY "+name+" DESC SKIP 0 LIMIT 1 RETURN "+name, got, name)
		require.NotNil(t, rewrite, name)
		require.Len(t, rewrite.Edits, 1, name)
		require.Equal(t, strings.Index(standalone, "ORDER"), rewrite.Edits[0].Original.Start, name)
		require.Equal(t, rewrite.Edits[0].Original.Start, rewrite.Edits[0].Original.End, name)
	}
	query := "WITH 7 AS call RETURN call ORDER BY call DESC OFFSET 0 LIMIT 1"
	got, rewrite := desugarStandaloneOrderClauses(query)
	require.Equal(t, "WITH 7 AS call RETURN call ORDER BY call DESC SKIP 0 LIMIT 1", got)
	require.NotNil(t, rewrite)
	require.Len(t, rewrite.Edits, 1)
	offset := strings.Index(query, "OFFSET")
	require.Equal(t, offset, rewrite.Edits[0].Original.Start)
	require.Equal(t, offset+len("OFFSET"), rewrite.Edits[0].Original.End)
	require.Equal(t, "SKIP", got[rewrite.Edits[0].Accepted.Start:rewrite.Edits[0].Accepted.End])
	const noEdit = "WITH 7 AS call RETURN call ORDER BY call DESC SKIP 0 LIMIT 1"
	allocs := testing.AllocsPerRun(100, func() {
		if got, rewrite := desugarStandaloneOrderClauses(noEdit); got != noEdit || rewrite != nil {
			panic("unexpected sort rewrite")
		}
	})
	require.Zero(t, allocs)
}

package fabric

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFoundationUseCommentExtents(t *testing.T) {
	for _, ending := range []string{"\r", "\n", "\r\n"} {
		for _, graph := range []string{"nornic", "graph.byName($db)"} {
			for _, subquery := range []bool{false, true} {
				query := "USE " + graph + " // comment" + ending + "RETURN 1"
				reference, rest, found, err := ReadUseClause(t.Context(), query, subquery, cyphertext.RawSource)
				require.NoError(t, err, query)
				require.True(t, found)
				require.Equal(t, len("USE "+graph), reference.End)
				require.Equal(t, "// comment"+ending+"RETURN 1", rest)
				use, parsedRest, parsed, err := ParseUseClause(t.Context(), query, subquery, cyphertext.RawSource)
				require.NoError(t, err)
				require.True(t, parsed)
				require.Equal(t, reference.UseClause, use)
				require.Equal(t, rest, parsedRest)
				if strings.Contains(graph, "(") {
					require.Equal(t, []string{"$db"}, use.Args)
				} else {
					require.Equal(t, "nornic", use.Name)
				}
			}
		}
	}
	require.Equal(t, len(" /* unfinished"), skipSpacesAndComments(" /* unfinished", 0))
}

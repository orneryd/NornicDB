package fabric

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/stretchr/testify/require"
)

func TestUseAdmissionSharesReadingStatementGrammar(t *testing.T) {
	for _, body := range []string{
		"MATCH (n) RETURN n", "OPTIONAL MATCH (n) RETURN n",
		"UNWIND [1] AS value RETURN value", "LET value = 1 RETURN value",
		"FILTER true RETURN 1", "FOR value IN [1] RETURN value",
		"CALL db.labels() YIELD label RETURN label",
		"CALL () { RETURN 1 AS value } RETURN value",
		"CREATE (n) RETURN n", "MERGE (n) RETURN n", "SET n.value = 1",
		"FOREACH (value IN [1] | CREATE ())",
	} {
		for _, graph := range []string{"target", "graph", "GRAPH target"} {
			if graph == "graph" && strings.HasPrefix(body, "OPTIONAL ") {
				// Two clause words after a bare graph: GRAPH is the keyword
				// (useGraphKeyword), asserted separately below.
				continue
			}
			query := "USE " + graph + " " + body
			for _, subquery := range []bool{false, true} {
				statement := query
				if subquery {
					statement = "CALL () { " + query + " } RETURN *"
				}
				_, err := cypherantlr.ParseSyntax(t.Context(), statement, cypherantlr.StatementEntry, false, cyphertext.RawSource)
				require.NoError(t, err, statement)
				reference, remaining, found, err := ReadUseClause(t.Context(), query, subquery, cyphertext.RawSource)
				require.NoError(t, err, statement)
				require.True(t, found)
				require.Equal(t, body, remaining)
				require.Equal(t, body, query[reference.Remainder.Start:reference.Remainder.End])
			}
		}
	}
	reference, remaining, found, err := ReadUseClause(t.Context(), "USE graph OPTIONAL MATCH (n) RETURN n", false, cyphertext.RawSource)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "OPTIONAL", reference.Name)
	require.Equal(t, "MATCH (n) RETURN n", remaining)
}

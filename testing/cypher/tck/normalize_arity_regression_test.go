package tck

import (
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNormalizeArityBoltRegression(t *testing.T) {
	const prefix = "MATCH p = (n:Q {id: 1})-[r:R]->(o:Q) WITH n, r, p, o, 1 AS i, 2.5 AS f, 'ab' AS s, [1, 2, 3] AS l, {a: 1, b: 'x'} AS m, null AS z, true AS t, date('2020-01-02') AS d, duration('P1D') AS du "
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, mode := range []TransactionMode{AutocommitMode, ExplicitTransactionMode} {
				t.Run(string(mode), func(t *testing.T) {
					driver, shutdown := startConformanceServer(t)
					defer shutdown()
					backend := newDifferentialBackend(t, driver, "nornic", mode)
					for _, query := range []string{"RETURN normalize() AS v", prefix + "RETURN normalize() AS v", prefix + "WITH normalize() AS v RETURN v"} {
						t.Run(query, func(t *testing.T) {
							_, err := backend.Execute(t.Context(), query, nil)
							var diagnostic *neo4j.Neo4jError
							require.ErrorAs(t, err, &diagnostic)
							require.Equal(t, "Neo.ClientError.Statement.SyntaxError", diagnostic.Code)
							require.Contains(t, diagnostic.Msg, "Insufficient parameters for function 'normalize'")
						})
					}
				})
			}
		})
	}
}

func TestIssue256ColonMapKeyBoltRegression(t *testing.T) {
	for _, mode := range []TransactionMode{AutocommitMode, ExplicitTransactionMode} {
		t.Run(string(mode), func(t *testing.T) {
			driver, shutdown := startConformanceServer(t)
			defer shutdown()
			backend := newDifferentialBackend(t, driver, "nornic", mode)
			require.NoError(t, backend.Reset(t.Context()))
			// Issue 256's quoted map key is invalid Cypher in pinned Neo4j.
			// Both public Bolt routes now report the same compile-time syntax
			// error, so these routes no longer belong in the mismatch ratchet.
			_, err := backend.Execute(t.Context(), "MERGE (n:Test { id: 0 }) SET n.my_property = {'key:key': 'value'}", nil)
			var diagnostic *neo4j.Neo4jError
			require.ErrorAs(t, err, &diagnostic)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", diagnostic.Code)
			var classified *QueryError
			require.ErrorAs(t, err, &classified)
			require.Equal(t, "compile time", classified.Phase)
		})
	}
}

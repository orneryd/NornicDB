package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestExplainKeyConstraintValidatesPropertyReferences(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, requirement := range []string{"(m.a, m.b)", "(n.a, 7)"} {
				for _, prefix := range []string{"", "EXPLAIN "} {
					t.Run(prefix+requirement, func(t *testing.T) {
						exec, store := newTestExecutor(t)
						_, err := exec.Execute(t.Context(), prefix+"CREATE CONSTRAINT invalid_key FOR (n:Review) REQUIRE "+requirement+" IS NODE KEY", nil)
						require.Error(t, err, "planning must validate the same property references as execution")
						require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError")
						require.Empty(t, store.GetSchema().GetAllConstraints(), "failed planning and execution must not install a constraint")
					})
				}
			}
			for _, statement := range []string{
				"EXPLAIN CREATE CONSTRAINT node_key FOR (n:Review) REQUIRE (n.a, n.b) IS NODE KEY",
				"EXPLAIN CREATE CONSTRAINT relationship_key FOR ()-[r:Review]-() REQUIRE (r.a, r.b) IS RELATIONSHIP KEY",
				"EXPLAIN CREATE CONSTRAINT unique_key FOR (n:Review) REQUIRE (n.a, n.b) IS UNIQUE",
			} {
				t.Run(statement, func(t *testing.T) {
					exec, store := newTestExecutor(t)
					_, err := exec.Execute(t.Context(), statement, nil)
					require.NoError(t, err)
					require.Empty(t, store.GetSchema().GetAllConstraints(), "successful EXPLAIN must not install a constraint")
				})
			}
		})
	}
}

package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestFabricUseTriviaPrefixRunsPrefixStatementText is the public control for a
// planner prefix that client-supplied trivia leads. A statement's leading
// whitespace and comments belong to no clause, so they are not the text ahead
// of a mid-statement USE or CALL { } block: the fragment that runs in the
// current graph is the statement text from its first clause, and a prefix of
// only trivia is no fragment at all.
//
// Each query states the same result bare, at both parser settings and both
// normalization settings, so the trivia variants are compared with the healthy
// statement they comment, not with an absolutized shape.
//
// Fail-first evidence, before the correction (normalization disabled, both
// parsers): the comment-only prefix ran as "//c RETURN *" or "/*c*/ RETURN *",
// which the fragment executor rejected as an unsupported query type or as
// RETURN * without a variable in scope, and the comment-led prefix before a
// mid-statement USE ran as "//c\nWITH 1 AS x RETURN *", which the fragment
// executor rejected as the query type "//C".
func TestFabricUseTriviaPrefixRunsPrefixStatementText(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		for _, normalized := range []bool{true, false} {
			t.Run(parser.name+map[bool]string{true: "/normalized", false: "/raw"}[normalized], func(t *testing.T) {
				t.Cleanup(parser.setup())
				t.Cleanup(config.WithCypherQueryNormalizationDisabled())
				config.SetCypherQueryNormalizationEnabled(normalized)

				for _, query := range []struct {
					name    string
					cypher  string
					columns []string
					rows    [][]interface{}
				}{
					{
						name:    "nested use bare",
						cypher:  "CALL { USE " + indexDropFabricGraphA + " RETURN 1 AS v } RETURN v",
						columns: []string{"v"},
						rows:    [][]interface{}{{int64(1)}},
					},
					{
						name:    "nested use line comment",
						cypher:  "//c\nCALL { USE " + indexDropFabricGraphA + " RETURN 1 AS v } RETURN v",
						columns: []string{"v"},
						rows:    [][]interface{}{{int64(1)}},
					},
					{
						name:    "nested use block comment",
						cypher:  "/*c*/ CALL { USE " + indexDropFabricGraphA + " RETURN 1 AS v } RETURN v",
						columns: []string{"v"},
						rows:    [][]interface{}{{int64(1)}},
					},
					{
						name:    "leading with bare",
						cypher:  "WITH 1 AS x USE " + indexDropFabricGraphA + " RETURN x",
						columns: []string{"x"},
						rows:    [][]interface{}{{int64(1)}},
					},
					{
						name:    "leading with line comment",
						cypher:  "//c\nWITH 1 AS x USE " + indexDropFabricGraphA + " RETURN x",
						columns: []string{"x"},
						rows:    [][]interface{}{{int64(1)}},
					},
					{
						name:    "leading with block comment",
						cypher:  "/*c*/ WITH 1 AS x USE " + indexDropFabricGraphA + " RETURN x",
						columns: []string{"x"},
						rows:    [][]interface{}{{int64(1)}},
					},
				} {
					t.Run(query.name, func(t *testing.T) {
						fixture := newIndexDropFabricFixture(t)
						session := newCompositeTestExecutor(t, fixture.mgr, indexDropFabricComposite, fixture.root.dbManager)
						// The statement routes to its composite constituent, so
						// the row proves the prefix fragment and the USE target
						// ran as one Fabric statement.
						require.True(t, session.shouldUseFabricPlanner(fixture.ctx, query.cypher))
						result, err := session.Execute(fixture.ctx, query.cypher, nil)
						require.NoError(t, err)
						require.Equal(t, query.columns, result.Columns)
						require.Equal(t, query.rows, result.Rows)
					})
				}
			})
		}
	}
}
